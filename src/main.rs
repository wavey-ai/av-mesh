mod control;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use control::{
    packetize_control_message, reassemble_unsigned_control_packets, MeshControlEvent,
    MeshControlMessage,
};
use http::{Method, Request, StatusCode};
use playlists::chunk_cache::ChunkCache;
use playlists::mesh::{CacheMesh, CacheMeshConfig, CacheMeshHandle};
use playlists::Options as CacheOptions;
use rist_core_pure::{packet::rtcp::NackMode, time::ntp_now, ReceivedPayload};
use rist_mio_pure::{MainMioReceiver, SimpleMioReceiver};
use serde::Serialize;
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::{
    net::UdpSocket,
    sync::{watch, RwLock},
    time::{interval, sleep, MissedTickBehavior},
};
use tracing::{debug, info, warn};
use web_service::{
    load_default_tls_base64, load_tls_base64_from_paths, H2H3Server, HandlerResponse,
    HandlerResult, Router, Server, ServerBuilder, ServerError, StreamWriter,
};

const DEFAULT_STREAM_ID: u64 = 1;
const DEFAULT_FLOW_ID: u32 = 0x7273_7401;
const MAX_RIST_DRAIN_PER_TICK: usize = 128;
const PART_WAIT_MS: u64 = 3_000;
const RIST_POLL_MS: u64 = 1;
const RTCP_INTERVAL_MS: u64 = 20;

#[derive(Debug, Clone, Parser)]
#[command(name = "av-mesh", about = "Run a local AV mesh node")]
struct Args {
    #[arg(long, default_value = "uk")]
    region: String,

    #[arg(long)]
    node_id: Option<String>,

    #[arg(long, default_value = "127.0.0.1:9101")]
    mesh_bind: SocketAddr,

    #[arg(long = "peer")]
    peers: Vec<SocketAddr>,

    #[arg(long, default_value = "127.0.0.1:10001")]
    ingest_bind: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:7000")]
    rist_bind: SocketAddr,

    #[arg(long, value_enum, default_value = "main")]
    rist_profile: RistProfile,

    #[arg(long, value_parser = parse_u32_auto, default_value = "0x72737401")]
    flow_id: u32,

    #[arg(long, default_value_t = 9444)]
    http_port: u16,

    #[arg(long)]
    cert: Option<PathBuf>,

    #[arg(long)]
    key: Option<PathBuf>,

    #[arg(long, default_value_t = DEFAULT_STREAM_ID)]
    stream_id: u64,

    #[arg(long, default_value_t = 500)]
    part_ms: u64,

    #[arg(long, default_value_t = 4)]
    parts_per_segment: usize,

    #[arg(long, default_value_t = 24)]
    window_parts: usize,

    #[arg(long, default_value_t = 2048)]
    slot_kb: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RistProfile {
    Simple,
    Main,
}

impl RistProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::Simple => "simple",
            Self::Main => "main",
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "av_mesh=info,playlists=info,web_service=info".into()),
        )
        .init();

    let args = Args::parse().normalized()?;
    let node_id = args.node_id.clone().unwrap_or_else(|| args.region.clone());
    let cache = LiveTsCache::new(
        args.stream_id,
        Duration::from_millis(args.part_ms),
        args.parts_per_segment,
        args.window_parts,
        args.slot_kb,
    )
    .await;

    let mesh_config = CacheMeshConfig::new(node_id.clone(), args.region.clone(), args.mesh_bind)
        .with_peers(args.peers.clone());
    let mesh_handle = Arc::new(
        CacheMesh::new(Arc::clone(&cache.chunk_cache), mesh_config)
            .start()
            .await
            .context("failed to start cache mesh")?,
    );

    let control_packets = packetize_control_message(&MeshControlMessage {
        node_id: node_id.clone(),
        region: args.region.clone(),
        event: MeshControlEvent::NodeStarted {
            mesh_addr: mesh_handle.local_addr().to_string(),
        },
    })?;
    let control_echo = reassemble_unsigned_control_packets(&control_packets)?;
    info!(
        packets = control_packets.len(),
        event = ?control_echo.event,
        "mesh control message packetized"
    );

    let (ingest_shutdown_tx, ingest_shutdown_rx) = watch::channel(());

    let udp_socket = UdpSocket::bind(args.ingest_bind)
        .await
        .with_context(|| format!("failed to bind UDP ingest on {}", args.ingest_bind))?;
    info!(bind = %udp_socket.local_addr()?, "UDP contributor ingest listening");
    let udp_ingest_task = tokio::spawn(run_udp_ingest(
        udp_socket,
        Arc::clone(&cache),
        ingest_shutdown_rx.clone(),
    ));

    let rist_config = RistIngestConfig {
        bind: args.rist_bind,
        profile: args.rist_profile,
        flow_id: args.flow_id,
    };
    let rist_receiver =
        RistReceiver::bind(rist_config.profile, rist_config.bind, rist_config.flow_id)
            .with_context(|| format!("failed to bind RIST ingest on {}", rist_config.bind))?;
    info!(
        bind = %rist_config.bind,
        profile = rist_config.profile.as_str(),
        flow_id = format_args!("0x{:08x}", rist_config.flow_id),
        "RIST contributor ingest listening"
    );
    let rist_ingest_task = tokio::spawn(run_rist_ingest(
        rist_receiver,
        rist_config,
        Arc::clone(&cache),
        ingest_shutdown_rx,
    ));

    let (cert, key) = load_tls(&args)?;
    let router = Box::new(AppRouter::new(Arc::clone(&cache), Arc::clone(&mesh_handle)));
    let server = H2H3Server::builder()
        .with_tls(cert, key)
        .with_port(args.http_port)
        .enable_h2(true)
        .enable_h3(false)
        .enable_websocket(false)
        .with_router(router)
        .build()?;
    let handle = server.start().await?;
    let _ = handle.ready_rx.await;

    println!("node:    {} ({})", node_id, args.region);
    println!("mesh:    {}", mesh_handle.local_addr());
    println!("udp:     udp://{}", args.ingest_bind);
    println!(
        "rist:    rist://127.0.0.1:{} profile={} flow_id=0x{:08x}",
        args.rist_bind.port(),
        args.rist_profile.as_str(),
        args.flow_id
    );
    println!(
        "hls:     https://127.0.0.1:{}/live/stream.m3u8",
        args.http_port
    );
    println!("health:  https://127.0.0.1:{}/up", args.http_port);

    tokio::signal::ctrl_c().await?;
    mesh_handle.shutdown();
    let _ = ingest_shutdown_tx.send(());
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
    let _ = udp_ingest_task.await;
    let _ = rist_ingest_task.await;
    Ok(())
}

impl Args {
    fn normalized(mut self) -> Result<Self> {
        if self.part_ms < 100 {
            bail!("--part-ms must be at least 100");
        }
        self.parts_per_segment = self.parts_per_segment.max(1);
        self.window_parts = self.window_parts.max(self.parts_per_segment * 3).max(6);
        self.slot_kb = self.slot_kb.max(64);
        Ok(self)
    }
}

fn parse_u32_auto(value: &str) -> std::result::Result<u32, String> {
    let trimmed = value.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        u32::from_str_radix(hex, 16).map_err(|err| err.to_string())
    } else {
        trimmed.parse::<u32>().map_err(|err| err.to_string())
    }
}

fn load_tls(args: &Args) -> Result<(String, String)> {
    match (&args.cert, &args.key) {
        (Some(cert), Some(key)) => load_tls_base64_from_paths(cert, key).with_context(|| {
            format!(
                "failed to load TLS files {} and {}",
                cert.display(),
                key.display()
            )
        }),
        (None, None) => {
            load_default_tls_base64().context("failed to load default TLS files from web-services")
        }
        _ => bail!("--cert and --key must be provided together"),
    }
}

async fn run_udp_ingest(
    socket: UdpSocket,
    cache: Arc<LiveTsCache>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let mut buf = vec![0u8; 65_536];
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                cache.rotate_if_due(true).await?;
                info!("UDP contributor ingest shutting down");
                return Ok(());
            }
            received = socket.recv_from(&mut buf) => {
                let (len, peer) = received?;
                if len == 0 {
                    continue;
                }
                if let Err(error) = cache.push_payload(&buf[..len]).await {
                    warn!(peer = %peer, error = %error, "failed to cache UDP contributor payload");
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RistIngestConfig {
    bind: SocketAddr,
    profile: RistProfile,
    flow_id: u32,
}

enum RistReceiver {
    Simple(SimpleMioReceiver),
    Main(MainMioReceiver),
}

impl RistReceiver {
    fn bind(profile: RistProfile, addr: SocketAddr, flow_id: u32) -> io::Result<Self> {
        match profile {
            RistProfile::Simple => {
                SimpleMioReceiver::bind(addr, flow_id, "av-mesh", NackMode::Range).map(Self::Simple)
            }
            RistProfile::Main => {
                MainMioReceiver::bind(addr, flow_id, "av-mesh", NackMode::Range).map(Self::Main)
            }
        }
    }

    fn try_recv_payload(
        &mut self,
        buf: &mut [u8],
    ) -> io::Result<Option<(SocketAddr, ReceivedPayload)>> {
        match self {
            Self::Simple(receiver) => receiver.try_recv_payload(buf),
            Self::Main(receiver) => receiver.try_recv_payload(buf),
        }
    }

    fn poll_rtcp_and_send(&mut self, now: Instant, now_ntp: u64) -> io::Result<()> {
        match self {
            Self::Simple(receiver) => receiver.poll_rtcp_and_send(now, now_ntp).map(|_| ()),
            Self::Main(receiver) => receiver.poll_rtcp_and_send(now, now_ntp).map(|_| ()),
        }
    }
}

async fn run_rist_ingest(
    mut receiver: RistReceiver,
    config: RistIngestConfig,
    cache: Arc<LiveTsCache>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let mut buf = vec![0u8; 65_536];
    let mut poll = interval(Duration::from_millis(RIST_POLL_MS));
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_rtcp = Instant::now();

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                cache.rotate_if_due(true).await?;
                info!("RIST contributor ingest shutting down");
                return Ok(());
            }
            _ = poll.tick() => {
                for _ in 0..MAX_RIST_DRAIN_PER_TICK {
                    match receiver.try_recv_payload(&mut buf) {
                        Ok(Some((peer, payload))) => {
                            if payload.duplicate {
                                continue;
                            }
                            if payload.recovered {
                                debug!(peer = %peer, "RIST payload recovered by protocol repair");
                            }
                            if let Err(error) = cache.push_payload(&payload.payload).await {
                                warn!(peer = %peer, error = %error, "failed to cache RIST payload");
                            }
                        }
                        Ok(None) => break,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                        Err(error) => {
                            warn!(bind = %config.bind, error = %error, "RIST receive failed");
                            break;
                        }
                    }
                }

                cache.rotate_if_due(false).await?;

                let now = Instant::now();
                if now.duration_since(last_rtcp) >= Duration::from_millis(RTCP_INTERVAL_MS) {
                    if let Err(error) = receiver.poll_rtcp_and_send(now, ntp_now()) {
                        if error.kind() != io::ErrorKind::WouldBlock {
                            debug!(error = %error, "RIST RTCP poll failed");
                        }
                    }
                    last_rtcp = now;
                }
            }
        }
    }
}

struct LiveTsCache {
    chunk_cache: Arc<ChunkCache>,
    stream_id: u64,
    part_target: Duration,
    parts_per_segment: usize,
    window_parts: usize,
    max_part_bytes: usize,
    state: RwLock<LiveState>,
}

impl LiveTsCache {
    async fn new(
        stream_id: u64,
        part_target: Duration,
        parts_per_segment: usize,
        window_parts: usize,
        slot_kb: usize,
    ) -> Arc<Self> {
        let mut options = CacheOptions::default();
        options.num_playlists = 16;
        options.max_segments = 1;
        options.max_parts_per_segment = window_parts.saturating_mul(4).max(32);
        options.buffer_size_kb = slot_kb;
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let _ = chunk_cache.get_or_create_stream_idx(stream_id).await;
        Arc::new(Self {
            chunk_cache,
            stream_id,
            part_target,
            parts_per_segment,
            window_parts,
            max_part_bytes: slot_kb * 1024,
            state: RwLock::new(LiveState::new()),
        })
    }

    async fn push_payload(&self, payload: &[u8]) -> Result<()> {
        let now = Instant::now();
        let now_ms = now_unix_ms();
        let finalized = {
            let mut state = self.state.write().await;
            state.datagrams_received += 1;
            state.last_ingest_unix_ms = Some(now_ms);
            state.bytes_received += payload.len() as u64;

            if state.current.is_empty() {
                state.current_started = now;
                state.current_started_unix_ms = now_ms;
            }
            state.current.extend_from_slice(payload);

            if now.duration_since(state.current_started) >= self.part_target
                || state.current.len() >= self.max_part_bytes
            {
                state.take_current(now, now_ms)
            } else {
                None
            }
        };

        if let Some(part) = finalized {
            self.commit_part(part).await?;
        }
        Ok(())
    }

    async fn rotate_if_due(&self, force: bool) -> Result<()> {
        let now = Instant::now();
        let now_ms = now_unix_ms();
        let finalized = {
            let mut state = self.state.write().await;
            if force || now.duration_since(state.current_started) >= self.part_target {
                state.take_current(now, now_ms)
            } else {
                None
            }
        };

        if let Some(part) = finalized {
            self.commit_part(part).await?;
        }
        Ok(())
    }

    async fn commit_part(&self, part: PendingPart) -> Result<()> {
        self.chunk_cache
            .add_for_stream_id(self.stream_id, part.seq as usize, Bytes::from(part.data))
            .await
            .map_err(|err| anyhow!("chunk cache write failed: {err}"))?;

        let mut state = self.state.write().await;
        state.last_committed_seq = Some(part.seq);
        state.last_committed_unix_ms = Some(part.committed_unix_ms);
        state.last_committed_bytes = Some(part.bytes);
        state.last_committed_duration_ms = Some(part.duration_ms);
        Ok(())
    }

    async fn playlist(&self) -> String {
        let Some((stream_idx, last)) = self.stream_position().await else {
            return self.empty_playlist(0);
        };
        let first = last.saturating_sub(self.window_parts.saturating_sub(1));
        let mut available = Vec::new();
        for seq in first..=last {
            if let Some((bytes, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                if hash != 0 || !bytes.is_empty() {
                    available.push(seq as u64);
                }
            }
        }
        if available.is_empty() {
            return self.empty_playlist(last);
        }

        let first_available = *available.first().unwrap();
        let media_sequence = first_available / self.parts_per_segment as u64;
        let next_part = available.last().copied().unwrap_or(0) + 1;
        let part_target = self.part_target.as_secs_f64();
        let target_duration = (part_target * self.parts_per_segment as f64)
            .ceil()
            .max(1.0) as u64;

        let mut groups: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for seq in available {
            groups
                .entry(seq / self.parts_per_segment as u64)
                .or_default()
                .push(seq);
        }

        let mut out = String::new();
        out.push_str("#EXTM3U\n");
        out.push_str("#EXT-X-VERSION:9\n");
        out.push_str(&format!("#EXT-X-TARGETDURATION:{target_duration}\n"));
        out.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{media_sequence}\n"));
        out.push_str(&format!("#EXT-X-PART-INF:PART-TARGET={part_target:.3}\n"));
        out.push_str(&format!(
            "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:.3},HOLD-BACK={:.3}\n",
            part_target * 3.0,
            (part_target * self.parts_per_segment as f64 * 2.0).max(3.0)
        ));

        for (segment, group) in groups {
            let mut duration = 0.0;
            for seq in &group {
                duration += part_target;
                out.push_str(&format!(
                    "#EXT-X-PART:DURATION={part_target:.3},URI=\"part{seq}.ts\"\n"
                ));
            }
            if group.len() == self.parts_per_segment {
                out.push_str(&format!("#EXTINF:{duration:.3},\n"));
                out.push_str(&format!("seg{segment}.ts\n"));
            }
        }

        out.push_str(&format!(
            "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part{next_part}.ts\"\n"
        ));
        out
    }

    fn empty_playlist(&self, next_part: usize) -> String {
        let part_target = self.part_target.as_secs_f64();
        let target_duration = (part_target * self.parts_per_segment as f64)
            .ceil()
            .max(1.0) as u64;
        format!(
            "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:{target_duration}\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PART-INF:PART-TARGET={part_target:.3}\n#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:.3},HOLD-BACK={:.3}\n#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part{next_part}.ts\"\n",
            part_target * 3.0,
            (part_target * self.parts_per_segment as f64 * 2.0).max(3.0)
        )
    }

    async fn stream_position(&self) -> Option<(usize, usize)> {
        let stream_idx = self.chunk_cache.get_stream_idx(self.stream_id).await?;
        let last = self.chunk_cache.last(stream_idx)?;
        Some((stream_idx, last))
    }

    async fn get_part_blocking(&self, seq: u64) -> Option<(Bytes, u64)> {
        let deadline = Instant::now() + Duration::from_millis(PART_WAIT_MS);
        loop {
            if let Some((bytes, hash)) = self
                .chunk_cache
                .get_for_stream_id(self.stream_id, seq as usize)
                .await
            {
                if hash != 0 || !bytes.is_empty() {
                    return Some((bytes, hash));
                }
            }
            let Some((_, last)) = self.stream_position().await else {
                return None;
            };
            if seq as usize > last || Instant::now() >= deadline {
                return None;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn get_segment(&self, segment: u64) -> Option<Bytes> {
        let first_part = segment.checked_mul(self.parts_per_segment as u64)?;
        let mut out = Vec::new();
        for offset in 0..self.parts_per_segment {
            let seq = first_part + offset as u64;
            let (bytes, _) = self.get_part_blocking(seq).await?;
            out.extend_from_slice(&bytes);
        }
        Some(Bytes::from(out))
    }

    async fn stats(&self, mesh: &CacheMeshHandle) -> StatsSnapshot {
        let now_ms = now_unix_ms();
        let state = self.state.read().await;
        let datagrams_received = state.datagrams_received;
        let bytes_received = state.bytes_received;
        let current_part_bytes = state.current.len();
        let latest_local_part = state.last_committed_seq;
        let latest_local_part_bytes = state.last_committed_bytes;
        let latest_local_part_duration_ms = state.last_committed_duration_ms;
        let latest_local_part_age_ms = state
            .last_committed_unix_ms
            .map(|last| now_ms.saturating_sub(last));
        let last_ingest_age_ms = state
            .last_ingest_unix_ms
            .map(|last| now_ms.saturating_sub(last));
        drop(state);

        let latest_mesh_part = match self.stream_position().await {
            Some((stream_idx, last)) => {
                let mut latest = None;
                for seq in (0..=last).rev().take(self.window_parts) {
                    if let Some((bytes, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                        if hash != 0 || !bytes.is_empty() {
                            latest = Some(seq as u64);
                            break;
                        }
                    }
                }
                latest
            }
            None => None,
        };

        StatsSnapshot {
            stream_id: self.stream_id,
            part_target_ms: self.part_target.as_millis() as u64,
            parts_per_segment: self.parts_per_segment,
            window_parts: self.window_parts,
            datagrams_received,
            bytes_received,
            current_part_bytes,
            latest_local_part,
            latest_local_part_bytes,
            latest_local_part_duration_ms,
            latest_mesh_part,
            mesh_peers: mesh
                .peers()
                .await
                .into_iter()
                .map(|addr| addr.to_string())
                .collect(),
            latest_local_part_age_ms,
            last_ingest_age_ms,
        }
    }
}

struct LiveState {
    current: Vec<u8>,
    current_started: Instant,
    current_started_unix_ms: u64,
    next_seq: u64,
    datagrams_received: u64,
    bytes_received: u64,
    last_ingest_unix_ms: Option<u64>,
    last_committed_seq: Option<u64>,
    last_committed_unix_ms: Option<u64>,
    last_committed_bytes: Option<usize>,
    last_committed_duration_ms: Option<u64>,
}

impl LiveState {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            current: Vec::new(),
            current_started: now,
            current_started_unix_ms: now_unix_ms(),
            next_seq: 0,
            datagrams_received: 0,
            bytes_received: 0,
            last_ingest_unix_ms: None,
            last_committed_seq: None,
            last_committed_unix_ms: None,
            last_committed_bytes: None,
            last_committed_duration_ms: None,
        }
    }

    fn take_current(&mut self, now: Instant, now_ms: u64) -> Option<PendingPart> {
        if self.current.is_empty() {
            self.current_started = now;
            self.current_started_unix_ms = now_ms;
            return None;
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        let data = std::mem::take(&mut self.current);
        let duration_ms = now
            .duration_since(self.current_started)
            .as_millis()
            .max(1)
            .min(u128::from(u64::MAX)) as u64;
        let part = PendingPart {
            seq,
            bytes: data.len(),
            duration_ms,
            committed_unix_ms: now_ms,
            data,
        };
        self.current_started = now;
        self.current_started_unix_ms = now_ms;
        Some(part)
    }
}

struct PendingPart {
    seq: u64,
    bytes: usize,
    duration_ms: u64,
    committed_unix_ms: u64,
    data: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct StatsSnapshot {
    stream_id: u64,
    part_target_ms: u64,
    parts_per_segment: usize,
    window_parts: usize,
    datagrams_received: u64,
    bytes_received: u64,
    current_part_bytes: usize,
    latest_local_part: Option<u64>,
    latest_local_part_bytes: Option<usize>,
    latest_local_part_duration_ms: Option<u64>,
    latest_mesh_part: Option<u64>,
    mesh_peers: Vec<String>,
    latest_local_part_age_ms: Option<u64>,
    last_ingest_age_ms: Option<u64>,
}

struct AppRouter {
    cache: Arc<LiveTsCache>,
    mesh: Arc<CacheMeshHandle>,
}

impl AppRouter {
    fn new(cache: Arc<LiveTsCache>, mesh: Arc<CacheMeshHandle>) -> Self {
        Self { cache, mesh }
    }
}

#[async_trait]
impl Router for AppRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        if req.method() == Method::OPTIONS {
            return Ok(response(StatusCode::NO_CONTENT, None, None));
        }
        if req.method() != Method::GET && req.method() != Method::HEAD {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED, None, None));
        }

        let path = req.uri().path();
        match path {
            "/" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(
                    b"av-mesh node\n\nHLS: /live/stream.m3u8\nHealth: /up\nStats: /api/stats\n",
                )),
                Some("text/plain; charset=utf-8"),
            )),
            "/up" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(b"OK")),
                Some("text/plain"),
            )),
            "/live/stream.m3u8" => {
                let playlist = self.cache.playlist().await;
                Ok(response(
                    StatusCode::OK,
                    Some(Bytes::from(playlist)),
                    Some("application/vnd.apple.mpegurl"),
                )
                .with_no_store())
            }
            "/api/stats" => {
                let json = serde_json::to_vec(&self.cache.stats(&self.mesh).await)
                    .map_err(|err| ServerError::Handler(Box::new(err)))?;
                Ok(response(
                    StatusCode::OK,
                    Some(Bytes::from(json)),
                    Some("application/json"),
                )
                .with_no_store())
            }
            _ => {
                if let Some(seq) = parse_part_path(path) {
                    if let Some((bytes, hash)) = self.cache.get_part_blocking(seq).await {
                        return Ok(response(StatusCode::OK, Some(bytes), Some("video/mp2t"))
                            .with_etag(hash));
                    }
                    return Ok(response(StatusCode::NOT_FOUND, None, None));
                }

                if let Some(segment) = parse_segment_path(path) {
                    if let Some(bytes) = self.cache.get_segment(segment).await {
                        return Ok(response(StatusCode::OK, Some(bytes), Some("video/mp2t")));
                    }
                    return Ok(response(StatusCode::NOT_FOUND, None, None));
                }

                Ok(response(StatusCode::NOT_FOUND, None, None))
            }
        }
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _req: Request<()>,
        _stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config("no streaming endpoints".into()))
    }

    fn webtransport_handler(&self) -> Option<&dyn web_service::WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn web_service::WebSocketHandler> {
        None
    }
}

trait ResponseExt {
    fn with_no_store(self) -> Self;
    fn with_etag(self, etag: u64) -> Self;
}

impl ResponseExt for HandlerResponse {
    fn with_no_store(mut self) -> Self {
        self.headers
            .push(("cache-control".into(), "no-store, max-age=0".into()));
        self
    }

    fn with_etag(mut self, etag: u64) -> Self {
        self.etag = Some(etag);
        self
    }
}

fn response(
    status: StatusCode,
    body: Option<Bytes>,
    content_type: Option<&'static str>,
) -> HandlerResponse {
    HandlerResponse {
        status,
        body,
        content_type: content_type.map(str::to_string),
        headers: vec![
            ("access-control-allow-origin".into(), "*".into()),
            (
                "access-control-allow-methods".into(),
                "GET, HEAD, OPTIONS".into(),
            ),
        ],
        etag: None,
    }
}

fn parse_part_path(path: &str) -> Option<u64> {
    path.strip_prefix("/live/part")?
        .strip_suffix(".ts")?
        .parse()
        .ok()
}

fn parse_segment_path(path: &str) -> Option<u64> {
    path.strip_prefix("/live/seg")?
        .strip_suffix(".ts")?
        .parse()
        .ok()
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn playlist_uses_replicated_cache_parts() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        cache
            .chunk_cache
            .add_for_stream_id(1, 0, Bytes::from_static(b"part0"))
            .await
            .unwrap();
        cache
            .chunk_cache
            .add_for_stream_id(1, 1, Bytes::from_static(b"part1"))
            .await
            .unwrap();

        let playlist = cache.playlist().await;
        assert!(playlist.contains("part0.ts"));
        assert!(playlist.contains("part1.ts"));
        assert!(playlist.contains("seg0.ts"));
    }

    #[test]
    fn parses_live_paths() {
        assert_eq!(parse_part_path("/live/part42.ts"), Some(42));
        assert_eq!(parse_segment_path("/live/seg7.ts"), Some(7));
        assert_eq!(parse_part_path("/live/seg7.ts"), None);
    }

    #[test]
    fn parses_decimal_and_hex_flow_ids() {
        assert_eq!(parse_u32_auto("0x72737401").unwrap(), DEFAULT_FLOW_ID);
        assert_eq!(parse_u32_auto("1920168961").unwrap(), DEFAULT_FLOW_ID);
    }

    #[tokio::test]
    async fn rist_ingest_writes_cache_parts() {
        use tokio::time::timeout;

        let bind = unused_loopback_addr();
        let cache = LiveTsCache::new(1, Duration::from_millis(100), 2, 6, 64).await;
        let receiver = RistReceiver::bind(RistProfile::Main, bind, DEFAULT_FLOW_ID).unwrap();
        let config = RistIngestConfig {
            bind,
            profile: RistProfile::Main,
            flow_id: DEFAULT_FLOW_ID,
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_rist_ingest(
            receiver,
            config,
            Arc::clone(&cache),
            shutdown_rx,
        ));

        send_rist_payloads(bind, 4).await;

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) = cache.get_part_blocking(0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert!(!bytes.is_empty());
        assert!(cache.playlist().await.contains("part0.ts"));

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rist_ingest_replicates_to_peer_cache() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let rist_addr = unused_loopback_addr();

        let cache_a = LiveTsCache::new(1, Duration::from_millis(100), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(100), 2, 6, 64).await;

        let mesh_a = CacheMesh::new(
            Arc::clone(&cache_a.chunk_cache),
            CacheMeshConfig::new("uk-test", "uk", mesh_a_addr).with_peer(mesh_b_addr),
        )
        .start()
        .await
        .unwrap();
        let mesh_b = CacheMesh::new(
            Arc::clone(&cache_b.chunk_cache),
            CacheMeshConfig::new("us-test", "us", mesh_b_addr).with_peer(mesh_a_addr),
        )
        .start()
        .await
        .unwrap();

        let receiver = RistReceiver::bind(RistProfile::Main, rist_addr, DEFAULT_FLOW_ID).unwrap();
        let config = RistIngestConfig {
            bind: rist_addr,
            profile: RistProfile::Main,
            flow_id: DEFAULT_FLOW_ID,
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_rist_ingest(
            receiver,
            config,
            Arc::clone(&cache_a),
            shutdown_rx,
        ));

        send_rist_payloads(rist_addr, 4).await;

        let bytes = timeout(Duration::from_secs(5), async {
            loop {
                if let Some((bytes, hash)) = cache_b.chunk_cache.get_for_stream_id(1, 0).await {
                    if hash != 0 || !bytes.is_empty() {
                        break bytes;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert!(!bytes.is_empty());
        assert!(cache_b.playlist().await.contains("part0.ts"));

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    fn unused_loopback_addr() -> SocketAddr {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        drop(socket);
        addr
    }

    async fn send_rist_payloads(peer: SocketAddr, count: usize) {
        use rist_core_pure::time::ntp_now;
        use rist_mio_pure::MainMioSender;
        use std::net::{Ipv4Addr, SocketAddrV4};

        let local = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
        let mut sender = MainMioSender::connect(local, peer, DEFAULT_FLOW_ID, 8192).unwrap();
        let mut feedback_buf = vec![0u8; 65_536];
        let payload = vec![0x47; 1316];

        for _ in 0..count {
            loop {
                match sender.send_payload(&payload, ntp_now(), Instant::now()) {
                    Ok(_) => break,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        drive_rist_feedback(&mut sender, &mut feedback_buf);
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("RIST send failed: {error}"),
                }
            }
            drive_rist_feedback(&mut sender, &mut feedback_buf);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn drive_rist_feedback(sender: &mut rist_mio_pure::MainMioSender, buf: &mut [u8]) {
        for _ in 0..32 {
            match sender.try_recv_feedback_and_retransmit(buf) {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("RIST feedback failed: {error}"),
            }
        }
    }
}
