mod control;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use av_mesh::replication::{
    DemandSignal, MeshNode, ReplicaPlacement, ReplicaReason, ReplicationPolicy, StreamInfo,
};
use av_mesh::udp_fec::UdpFecReceiver;
use bytes::{BufMut, Bytes, BytesMut};
use clap::Parser;
use control::{
    packetize_control_message, reassemble_unsigned_control_packets, MeshControlEvent,
    MeshControlMessage,
};
use futures_util::{SinkExt, StreamExt};
use h3_webtransport::server::{AcceptedBi, WebTransportSession};
use http::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
#[cfg(feature = "linode-provisioner")]
use linode::{regions::REGIONS as LINODE_REGIONS, LinodeClient};
use playlists::chunk_cache::ChunkCache;
use playlists::mesh::{CacheMesh, CacheMeshConfig, CacheMeshHandle};
use playlists::Options as CacheOptions;
use raptorq_datagram_fec::{
    decode_serialized_media_access_unit, DecodedMediaFrame, MediaCodec, MediaFecDecoder,
    MediaFragmentHeader, MediaFrameMetadata, DATAGRAM_MAGIC, MEDIA_FRAME_HEADER_LEN,
};
use raptorq_fec_transport::{split_stream_id_prefix, FecDatagramDecoder, STREAM_ID_PREFIX_LEN};
use serde::{de, Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex as StdMutex,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tcp_changes::{
    Client as TcpChangesClient, Message as TcpChangesMessage, Payload as TcpChangesPayload,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::{
    net::UdpSocket,
    sync::{mpsc, watch, RwLock},
    time::{interval, sleep, MissedTickBehavior},
};
use tokio_tungstenite::{tungstenite::Message as WebSocketMessage, WebSocketStream};
use tracing::{debug, info, warn};
use web_service::{
    load_default_tls_base64, load_tls_base64_from_paths, read_length_prefixed_frame,
    write_length_prefixed_frame, BodyStream, H2H3Server, HandlerResponse, HandlerResult,
    RawTcpHandler, Router, Server, ServerBuilder, ServerError, StreamWriter, WebSocketHandler,
    WebTransportHandler,
};

const DEFAULT_STREAM_ID: u64 = 1;
const PART_WAIT_MS: u64 = 3_000;
const REPLICA_REQUEST_MIN_INTERVAL_MS: u64 = 1_000;
const MESH_EVENTS_PATH: &str = "/api/mesh/events";
const MESH_WEBSOCKET_PATH: &str = "/ws/mesh";
const DASHBOARD_DIST_ENV: &str = "AV_MESH_DASHBOARD_DIST";
const MEDIA_ACCESS_UNIT_CONTENT_TYPE: &str = "application/vnd.wavey.media-access-unit";
const LIVE_FMP4_CONTENT_TYPE: &str = "video/mp4";
const LIVE_TS_CONTENT_TYPE: &str = "video/mp2t";
const MESH_FMP4_SLOT_MAGIC: &[u8; 8] = b"AVFMP4S1";
const MESH_FMP4_SLOT_HEADER_LEN: usize = 16;
const WAVEY_GOOSE_ASSET_PATH: &str = "/assets/wavey-goose.png";
const WAVEY_GOOSE_PNG: &[u8] = include_bytes!("../assets/wavey-goose.png");
const TELEMETRY_TAG: [u8; 4] = *b"AVMT";
const CONTROL_TAG: [u8; 4] = *b"AVMC";
const DEFAULT_TELEMETRY_STALE_MS: u64 = 30_000;
const RAW_MESH_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const MESH_STORAGE_WARN_PCT: u64 = 85;
const MESH_STORAGE_ERROR_PCT: u64 = 95;
const MESH_MIN_STALE_INGEST_ALERT_MS: u64 = 5_000;
const MESH_ACTIVITY_LIMIT: usize = 64;
const EDGE_RECENT_RESPONSE_LIMIT: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveMediaKind {
    Fmp4,
    Ts,
}

impl LiveMediaKind {
    fn extension(self) -> &'static str {
        match self {
            Self::Fmp4 => "mp4",
            Self::Ts => "ts",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::Fmp4 => LIVE_FMP4_CONTENT_TYPE,
            Self::Ts => LIVE_TS_CONTENT_TYPE,
        }
    }
}

enum LiveSlotPayload {
    Fmp4 { init: Option<Bytes>, media: Bytes },
    Opaque(Bytes),
}

impl LiveSlotPayload {
    fn decode(payload: Bytes) -> Self {
        if payload.len() < MESH_FMP4_SLOT_HEADER_LEN {
            return Self::Opaque(payload);
        }
        if !payload.starts_with(MESH_FMP4_SLOT_MAGIC) {
            return Self::Opaque(payload);
        }

        let init_len = u32::from_be_bytes(payload[8..12].try_into().unwrap()) as usize;
        let media_len = u32::from_be_bytes(payload[12..16].try_into().unwrap()) as usize;
        let Some(init_end) = MESH_FMP4_SLOT_HEADER_LEN.checked_add(init_len) else {
            return Self::Opaque(payload);
        };
        let Some(media_end) = init_end.checked_add(media_len) else {
            return Self::Opaque(payload);
        };
        if media_end != payload.len() {
            return Self::Opaque(payload);
        }

        let init = (init_len > 0).then(|| payload.slice(MESH_FMP4_SLOT_HEADER_LEN..init_end));
        let media = payload.slice(init_end..media_end);
        Self::Fmp4 { init, media }
    }

    fn media_kind(&self) -> LiveMediaKind {
        match self {
            Self::Fmp4 { .. } => LiveMediaKind::Fmp4,
            Self::Opaque(_) => LiveMediaKind::Ts,
        }
    }

    fn init(&self) -> Option<Bytes> {
        match self {
            Self::Fmp4 { init, .. } => init.clone(),
            Self::Opaque(_) => None,
        }
    }

    fn media(&self) -> Bytes {
        match self {
            Self::Fmp4 { media, .. } => media.clone(),
            Self::Opaque(payload) => payload.clone(),
        }
    }

    fn has_media(&self) -> bool {
        match self {
            Self::Fmp4 { media, .. } => !media.is_empty(),
            Self::Opaque(payload) => !payload.is_empty(),
        }
    }
}

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

    #[cfg(feature = "private-subnet-discovery")]
    #[arg(long)]
    private_subnet_discovery: bool,

    #[cfg(feature = "private-subnet-discovery")]
    #[arg(long, default_value_t = 12345)]
    private_discovery_broadcast_port: u16,

    #[cfg(feature = "private-subnet-discovery")]
    #[arg(long)]
    private_discovery_mesh_port: Option<u16>,

    #[arg(long, default_value = "127.0.0.1:12001")]
    fec_bind: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:12101")]
    media_fec_bind: SocketAddr,

    #[arg(long, default_value_t = 9444)]
    http_port: u16,

    #[arg(long)]
    playback_base_url: Option<String>,

    #[arg(long)]
    edge_websocket: bool,

    #[arg(long)]
    edge_webtransport: bool,

    #[arg(long)]
    raw_tcp_port: Option<u16>,

    #[arg(long)]
    raw_tcp_tls: bool,

    #[arg(long)]
    cert: Option<PathBuf>,

    #[arg(long)]
    key: Option<PathBuf>,

    #[arg(long, default_value_t = DEFAULT_STREAM_ID)]
    stream_id: u64,

    #[arg(long, env = "AV_LL_HLS_PART_MS", default_value_t = 50)]
    part_ms: u64,

    #[arg(long, default_value_t = 4)]
    parts_per_segment: usize,

    #[arg(long, default_value_t = 24)]
    window_parts: usize,

    #[arg(long, default_value_t = 2048)]
    slot_kb: usize,

    #[arg(long, default_value = "eu")]
    continent: String,

    #[arg(long, default_value_t = 51.5074)]
    latitude: f64,

    #[arg(long, default_value_t = -0.1278)]
    longitude: f64,

    #[arg(long, default_value_t = 100_000_000_000)]
    storage_bytes: u64,

    #[arg(long, default_value_t = 10_000_000_000)]
    egress_capacity_bps: u64,

    #[arg(long, default_value_t = 0)]
    baseline_per_region: usize,

    #[arg(long, default_value_t = 1)]
    baseline_per_continent: usize,

    #[arg(long, default_value_t = 300.0)]
    min_mirror_distance_km: f64,

    #[arg(long)]
    telemetry_bind: Option<SocketAddr>,

    #[arg(long = "telemetry-peer")]
    telemetry_peers: Vec<SocketAddr>,

    #[arg(long, default_value = "local.wavey.ai")]
    telemetry_dns_name: String,

    #[arg(long, default_value_t = Ipv4Addr::LOCALHOST)]
    telemetry_private_ipv4: Ipv4Addr,

    #[arg(long, default_value_t = 1000)]
    telemetry_interval_ms: u64,

    #[arg(long, default_value_t = DEFAULT_TELEMETRY_STALE_MS)]
    telemetry_stale_ms: u64,

    #[arg(long, default_value_t = 1000)]
    replication_plan_interval_ms: u64,

    #[arg(long)]
    provision_command: Option<String>,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long)]
    linode_provision: bool,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long)]
    linode_image_id: Option<String>,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long)]
    linode_instance_type: Option<String>,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long)]
    linode_domain_id: Option<u64>,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long, default_value = "av-mesh")]
    linode_vlan_tag: String,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long, default_value = "LINODE_API_TOKEN")]
    linode_token_env: String,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long, default_value = "LINODE_PUB_KEY")]
    linode_pub_key_env: String,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long = "linode-region-map", value_parser = parse_key_value)]
    linode_region_maps: Vec<(String, String)>,

    #[arg(long, default_value_t = 30_000)]
    provision_timeout_ms: u64,
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
    let node_profile = MeshNode {
        node_id: node_id.clone(),
        region: args.region.clone(),
        continent: args.continent.clone(),
        latitude: args.latitude,
        longitude: args.longitude,
        total_storage_bytes: args.storage_bytes,
        used_storage_bytes: 0,
        egress_capacity_bps: args.egress_capacity_bps,
        contributor_streams: 0,
        active_streams: 0,
        draining: false,
    };
    let replication_policy = ReplicationPolicy {
        baseline_per_region: args.baseline_per_region,
        baseline_per_continent: args.baseline_per_continent,
        min_mirror_distance_km: args.min_mirror_distance_km,
        ..ReplicationPolicy::default()
    };
    let control_plane = ControlPlane::default();
    let control_dispatch = ControlDispatch::default();
    let demand_tracker = DemandTracker::default();
    let lifecycle = NodeLifecycle::default();
    let telemetry_aggregator = TelemetryAggregator::new(args.telemetry_stale_ms);
    let telemetry_peer_monitor = TelemetryPeerMonitor::new(&args.telemetry_peers);
    let playback_base_url = args
        .playback_base_url
        .as_deref()
        .map(normalize_playback_base_url);
    let edge_load = EdgeLoad::default();
    let provision_executor = {
        let executor = ProvisionExecutor::new(
            args.provision_command.clone(),
            Duration::from_millis(args.provision_timeout_ms),
        );
        #[cfg(feature = "linode-provisioner")]
        let executor = executor.with_linode(args.linode_provision_config());
        executor
    };
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

    #[cfg(feature = "private-subnet-discovery")]
    let private_subnet_discovery = if args.private_subnet_discovery {
        Some(
            start_private_subnet_discovery(
                args.private_discovery_broadcast_port,
                args.private_discovery_mesh_port
                    .unwrap_or_else(|| args.mesh_bind.port()),
                Arc::clone(&mesh_handle),
                ingest_shutdown_rx.clone(),
            )
            .await?,
        )
    } else {
        None
    };

    let fec_socket = UdpSocket::bind(args.fec_bind)
        .await
        .with_context(|| format!("failed to bind UDP-FEC ingest on {}", args.fec_bind))?;
    info!(bind = %fec_socket.local_addr()?, "UDP-FEC mesh byte ingest listening");
    let fec_ingest_task = tokio::spawn(run_udp_fec_ingest(
        fec_socket,
        Arc::clone(&cache),
        ingest_shutdown_rx.clone(),
    ));

    let media_fec_socket = UdpSocket::bind(args.media_fec_bind)
        .await
        .with_context(|| {
            format!(
                "failed to bind media UDP-FEC ingest on {}",
                args.media_fec_bind
            )
        })?;
    info!(
        bind = %media_fec_socket.local_addr()?,
        "media UDP-FEC access-unit ingest listening"
    );
    let media_fec_ingest_task = tokio::spawn(run_udp_media_fec_ingest(
        media_fec_socket,
        Arc::clone(&cache),
        ingest_shutdown_rx.clone(),
    ));

    let (cert, key) = load_tls(&args)?;
    let telemetry_runtime = if let Some(bind) = args.telemetry_bind {
        Some(
            start_telemetry_feed(
                bind,
                args.telemetry_private_ipv4,
                cert.clone(),
                key.clone(),
                args.telemetry_interval_ms,
                Arc::clone(&cache),
                Arc::clone(&mesh_handle),
                node_profile.clone(),
                replication_policy.clone(),
                control_plane.clone(),
                lifecycle.clone(),
                control_dispatch.clone(),
                playback_base_url.clone(),
                edge_load.clone(),
                ingest_shutdown_rx.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    let router = AppRouter::new(
        Arc::clone(&cache),
        Arc::clone(&mesh_handle),
        node_profile.clone(),
        replication_policy.clone(),
        control_plane.clone(),
        control_dispatch.clone(),
        telemetry_aggregator.clone(),
        demand_tracker.clone(),
        lifecycle.clone(),
        playback_base_url.clone(),
        edge_load.clone(),
        provision_executor.clone(),
        telemetry_peer_monitor.clone(),
    );
    let telemetry_collector_tasks = start_telemetry_collectors(
        args.telemetry_peers.clone(),
        args.telemetry_dns_name.clone(),
        cert.clone(),
        router.clone(),
        telemetry_peer_monitor.clone(),
        ingest_shutdown_rx.clone(),
    );
    let replication_planner_task = tokio::spawn(run_replication_planner(
        router.clone(),
        Duration::from_millis(args.replication_plan_interval_ms),
        ingest_shutdown_rx.clone(),
    ));
    let mut server_builder = H2H3Server::builder()
        .with_tls(cert, key)
        .with_port(args.http_port)
        .enable_h2(true)
        .enable_h3(args.edge_webtransport)
        .enable_webtransport(args.edge_webtransport)
        .enable_websocket(args.edge_websocket);
    if let Some(raw_tcp_port) = args.raw_tcp_port {
        server_builder = server_builder
            .enable_raw_tcp(true)
            .with_raw_tcp_port(raw_tcp_port)
            .with_raw_tcp_tls(args.raw_tcp_tls)
            .with_raw_tcp_handler(Box::new(router.clone()));
    }
    let server = server_builder.with_router(Box::new(router)).build()?;
    let handle = server.start().await?;
    let _ = handle.ready_rx.await;

    println!("node:    {} ({})", node_id, args.region);
    println!("mesh:    {}", mesh_handle.local_addr());
    println!("fec:     udp+fec://{}", args.fec_bind);
    println!("media:   udp+media-fec://{}", args.media_fec_bind);
    println!(
        "hls:     https://127.0.0.1:{}/live/{}/stream.m3u8",
        args.http_port, args.stream_id
    );
    println!(
        "hls-default: https://127.0.0.1:{}/live/stream.m3u8",
        args.http_port
    );
    println!("mesh-ui: https://127.0.0.1:{}/mesh", args.http_port);
    if args.edge_websocket {
        println!(
            "edge-ws: wss://127.0.0.1:{}{}",
            args.http_port, MESH_WEBSOCKET_PATH
        );
    }
    if args.edge_webtransport {
        println!(
            "edge-webtransport: https://127.0.0.1:{} (HTTP/3 WebTransport)",
            args.http_port
        );
    }
    if let Some(runtime) = &telemetry_runtime {
        println!("telemetry: tcp+tls://{}", runtime.local_addr);
    }
    if !args.telemetry_peers.is_empty() {
        println!("telemetry-peers: {}", args.telemetry_peers.len());
    }
    if let Some(raw_tcp_port) = args.raw_tcp_port {
        println!(
            "raw-tcp: {}://0.0.0.0:{}",
            if args.raw_tcp_tls { "tls+tcp" } else { "tcp" },
            raw_tcp_port
        );
    }
    #[cfg(feature = "private-subnet-discovery")]
    if args.private_subnet_discovery {
        println!(
            "private-discovery: udp-broadcast://0.0.0.0:{} mesh-port={}",
            args.private_discovery_broadcast_port,
            args.private_discovery_mesh_port
                .unwrap_or_else(|| args.mesh_bind.port())
        );
    }
    println!("health:  https://127.0.0.1:{}/up", args.http_port);

    tokio::signal::ctrl_c().await?;
    mesh_handle.shutdown();
    let _ = ingest_shutdown_tx.send(());
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
    let _ = fec_ingest_task.await;
    let _ = media_fec_ingest_task.await;
    let _ = replication_planner_task.await;
    if let Some(runtime) = telemetry_runtime {
        let _ = runtime.shutdown_tx.send(());
        let _ = runtime.finished_rx.await;
        let _ = runtime.publisher_task.await;
    }
    #[cfg(feature = "private-subnet-discovery")]
    if let Some(runtime) = private_subnet_discovery {
        let _ = runtime.shutdown_tx.send(());
        let _ = runtime.task.await;
    }
    for task in telemetry_collector_tasks {
        let _ = task.await;
    }
    Ok(())
}

impl Args {
    fn normalized(mut self) -> Result<Self> {
        if self.part_ms == 0 {
            bail!("--part-ms must be at least 1");
        }
        self.parts_per_segment = self.parts_per_segment.max(1);
        self.window_parts = self.window_parts.max(self.parts_per_segment * 3).max(6);
        self.slot_kb = self.slot_kb.max(64);
        self.storage_bytes = self.storage_bytes.max((self.slot_kb as u64) * 1024);
        self.egress_capacity_bps = self.egress_capacity_bps.max(1);
        self.telemetry_interval_ms = self.telemetry_interval_ms.max(100);
        self.replication_plan_interval_ms = self.replication_plan_interval_ms.max(100);
        self.provision_timeout_ms = self.provision_timeout_ms.max(100);
        #[cfg(feature = "linode-provisioner")]
        self.validate_linode_provision()?;
        Ok(self)
    }

    #[cfg(feature = "linode-provisioner")]
    fn validate_linode_provision(&self) -> Result<()> {
        if !self.linode_provision {
            return Ok(());
        }
        let mut missing = Vec::new();
        if self.linode_image_id.is_none() {
            missing.push("--linode-image-id");
        }
        if self.linode_instance_type.is_none() {
            missing.push("--linode-instance-type");
        }
        if self.linode_domain_id.is_none() {
            missing.push("--linode-domain-id");
        }
        if missing.is_empty() {
            Ok(())
        } else {
            bail!("--linode-provision requires {}", missing.join(", "))
        }
    }

    #[cfg(feature = "linode-provisioner")]
    fn linode_provision_config(&self) -> Option<LinodeProvisionConfig> {
        if !self.linode_provision {
            return None;
        }
        Some(LinodeProvisionConfig {
            token_env: self.linode_token_env.clone(),
            pub_key_env: self.linode_pub_key_env.clone(),
            image_id: self.linode_image_id.clone().unwrap_or_default(),
            instance_type: self.linode_instance_type.clone().unwrap_or_default(),
            domain_id: self.linode_domain_id.unwrap_or_default(),
            vlan_tag: self.linode_vlan_tag.clone(),
            region_map: self.linode_region_maps.iter().cloned().collect(),
        })
    }
}

#[cfg(feature = "linode-provisioner")]
fn parse_key_value(value: &str) -> std::result::Result<(String, String), String> {
    let (key, val) = value
        .split_once('=')
        .ok_or_else(|| "expected KEY=VALUE".to_string())?;
    let key = key.trim();
    let val = val.trim();
    if key.is_empty() || val.is_empty() {
        return Err("expected non-empty KEY=VALUE".to_string());
    }
    Ok((key.to_string(), val.to_string()))
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

#[cfg(feature = "private-subnet-discovery")]
struct PrivateSubnetDiscoveryRuntime {
    shutdown_tx: watch::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "private-subnet-discovery")]
async fn start_private_subnet_discovery(
    broadcast_port: u16,
    mesh_port: u16,
    mesh: Arc<CacheMeshHandle>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<PrivateSubnetDiscoveryRuntime> {
    let (up_rx, _finished_rx, discovery_shutdown_tx, nodes) =
        discovery::vlan::discover(broadcast_port)
            .await
            .map_err(|err| anyhow!("private subnet discovery failed to start: {err}"))?;
    let _ = up_rx.await;

    let mut node_rx = nodes.rx();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => return,
                discovered = node_rx.recv() => {
                    match discovered {
                        Ok(node) if node.is_self() => {}
                        Ok(node) => {
                            let peer = node.addr(mesh_port);
                            if mesh.add_peer(peer).await {
                                info!(
                                    peer = %peer,
                                    tag = ?node.tag(),
                                    seq = ?node.seq(),
                                    "private subnet discovery added cache mesh peer"
                                );
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            debug!(skipped, "private subnet discovery receiver lagged");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        }
    });

    Ok(PrivateSubnetDiscoveryRuntime {
        shutdown_tx: discovery_shutdown_tx,
        task,
    })
}

async fn run_udp_fec_ingest(
    socket: UdpSocket,
    cache: Arc<LiveTsCache>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let mut receiver = UdpFecReceiver::new();
    let mut buf = vec![0u8; 65_536];
    let mut rotate = interval(Duration::from_millis(10));
    rotate.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                cache.rotate_if_due(true).await?;
                info!("UDP-FEC mesh byte ingest shutting down");
                return Ok(());
            }
            _ = rotate.tick() => {
                cache.rotate_if_due(false).await?;
            }
            received = socket.recv_from(&mut buf) => {
                let (len, peer) = received?;
                debug!(
                    peer = %peer,
                    datagram_bytes = len,
                    "UDP-FEC mesh datagram received"
                );
                if let Some(decoded) = receiver.push_payload(peer, &buf[..len]) {
                    let payload_bytes = decoded.payload.len();
                    if let Some(stream_id) = decoded.stream_id {
                        match cache
                            .commit_stream_payload(stream_id, decoded.payload)
                            .await
                        {
                            Ok(sequence) => {
                                debug!(
                                    peer = %peer,
                                    stream_id,
                                    sequence,
                                    payload_bytes,
                                    "cached stream-prefixed UDP-FEC mesh byte payload"
                                );
                            }
                            Err(error) => {
                                warn!(peer = %peer, stream_id, error = %error, "failed to cache stream-prefixed UDP-FEC mesh byte payload");
                            }
                        }
                    } else if let Err(error) = cache.push_payload(&decoded.payload).await {
                        warn!(peer = %peer, error = %error, "failed to cache UDP-FEC mesh byte payload");
                    } else {
                        debug!(
                            peer = %peer,
                            payload_bytes,
                            "cached UDP-FEC mesh byte payload"
                        );
                    }
                } else {
                    debug!(
                        peer = %peer,
                        datagram_bytes = len,
                        "UDP-FEC mesh datagram buffered awaiting repair/source symbols"
                    );
                }
            }
        }
    }
}

async fn run_udp_media_fec_ingest(
    socket: UdpSocket,
    cache: Arc<LiveTsCache>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let mut decoder = MediaFecDecoder::new();
    let mut buf = vec![0u8; 65_536];

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("media UDP-FEC access-unit ingest shutting down");
                return Ok(());
            }
            received = socket.recv_from(&mut buf) => {
                let (len, peer) = received?;
                debug!(
                    peer = %peer,
                    datagram_bytes = len,
                    "media UDP-FEC datagram received"
                );
                match decoder.push_datagram(&buf[..len]) {
                    Ok(Some(frame)) => {
                        let stream_id = frame.metadata.stream_id;
                        let sequence = frame.metadata.sequence;
                        let payload_bytes = frame.payload.len();
                        if let Err(error) = cache
                            .add_media_access_unit(frame.metadata, Bytes::from(frame.payload))
                            .await
                        {
                            warn!(
                                peer = %peer,
                                stream_id,
                                sequence,
                                error = %error,
                                "failed to cache media UDP-FEC access unit"
                            );
                        } else {
                            debug!(
                                peer = %peer,
                                stream_id,
                                sequence,
                                payload_bytes,
                                "cached media UDP-FEC access unit"
                            );
                        }
                    }
                    Ok(None) => {
                        debug!(
                            peer = %peer,
                            datagram_bytes = len,
                            "media UDP-FEC datagram buffered awaiting complete access unit"
                        );
                    }
                    Err(error) => {
                        warn!(
                            peer = %peer,
                            error = %error,
                            "failed to decode media UDP-FEC access unit"
                        );
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct CachedMediaAccessUnit {
    metadata: MediaFrameMetadata,
    payload_bytes: usize,
    serialized: Bytes,
    hash: u64,
}

fn codec_name(codec: MediaCodec) -> &'static str {
    match codec {
        MediaCodec::Unknown => "unknown",
        MediaCodec::H264 => "h264",
        MediaCodec::Opus => "opus",
        MediaCodec::Aac => "aac",
        MediaCodec::Data => "data",
    }
}

struct WebTransportMediaDecoder {
    unprefixed: MediaFecDecoder,
    prefixed_by_stream: HashMap<u64, MediaFecDecoder>,
}

impl WebTransportMediaDecoder {
    fn new() -> Self {
        Self {
            unprefixed: MediaFecDecoder::new(),
            prefixed_by_stream: HashMap::new(),
        }
    }

    fn push_datagram(
        &mut self,
        datagram: &[u8],
    ) -> std::result::Result<Option<DecodedMediaFrame>, String> {
        if datagram.len() == STREAM_ID_PREFIX_LEN && split_stream_id_prefix(datagram).is_some() {
            return Ok(None);
        }

        if let Some((stream_id, payload)) = split_stream_id_prefix(datagram) {
            if payload.starts_with(&DATAGRAM_MAGIC) {
                let decoder = self
                    .prefixed_by_stream
                    .entry(stream_id)
                    .or_insert_with(MediaFecDecoder::new);
                let transport = FecDatagramDecoder::webtransport_with_stream_prefix(stream_id);
                let decoded = transport
                    .push_media_datagram(decoder, datagram)
                    .map_err(|error| error.to_string())?;
                if let Some(frame) = decoded.as_ref() {
                    let metadata_stream_id = frame.metadata.stream_id;
                    if metadata_stream_id != stream_id {
                        return Err(format!(
                            "WebTransport stream prefix {stream_id} does not match media stream id {metadata_stream_id}"
                        ));
                    }
                }
                return Ok(decoded);
            }
        }

        self.unprefixed
            .push_datagram(datagram)
            .map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone, Serialize)]
struct MediaAccessUnitResponse {
    stream_id: u64,
    stream_id_text: String,
    sequence: u64,
    pts_ms: u64,
    dts_ms: Option<u64>,
    duration_ms: u32,
    codec: &'static str,
    flags: u16,
    payload_bytes: usize,
    stored_bytes: usize,
}

impl MediaAccessUnitResponse {
    fn from_cached(unit: &CachedMediaAccessUnit) -> Self {
        Self {
            stream_id: unit.metadata.stream_id,
            stream_id_text: stream_id_text(unit.metadata.stream_id),
            sequence: unit.metadata.sequence,
            pts_ms: unit.metadata.pts_ms,
            dts_ms: unit.metadata.dts_ms,
            duration_ms: unit.metadata.duration_ms,
            codec: codec_name(unit.metadata.codec),
            flags: unit.metadata.flags.bits(),
            payload_bytes: unit.payload_bytes,
            stored_bytes: unit.serialized.len(),
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
        debug!(
            stream_id = self.stream_id,
            sequence = part.seq,
            bytes = part.bytes,
            duration_ms = part.duration_ms,
            "committed mesh byte part"
        );
        Ok(())
    }

    async fn commit_stream_payload(&self, stream_id: u64, payload: Bytes) -> Result<u64> {
        let bytes = payload.len();
        let decoded = LiveSlotPayload::decode(payload.clone());
        let media_bytes = decoded.media().len();
        let media_kind = decoded.media_kind();
        let init = decoded.init();
        let now_ms = now_unix_ms();
        let seq = {
            let mut state = self.state.write().await;
            state.datagrams_received = state.datagrams_received.saturating_add(1);
            state.bytes_received = state.bytes_received.saturating_add(bytes as u64);
            state.last_ingest_unix_ms = Some(now_ms);
            state.stream_media_kinds.insert(stream_id, media_kind);
            if let Some(init) = init {
                state.stream_inits.insert(stream_id, init);
            }
            state.next_stream_seq(stream_id)
        };
        let slot_id = usize::try_from(seq).context("stream slot sequence too large")?;
        self.chunk_cache
            .add_for_stream_id(stream_id, slot_id, payload)
            .await
            .map_err(|err| anyhow!("stream-prefixed chunk cache write failed: {err}"))?;

        let mut state = self.state.write().await;
        state.last_committed_seq = Some(seq);
        state.last_committed_unix_ms = Some(now_ms);
        state.last_committed_bytes = Some(media_bytes);
        state.last_committed_duration_ms = None;
        debug!(
            stream_id,
            sequence = seq,
            slot_id,
            bytes,
            media_bytes,
            media_kind = ?media_kind,
            "committed stream-prefixed mesh payload"
        );
        Ok(seq)
    }

    async fn add_media_access_unit(
        &self,
        metadata: MediaFrameMetadata,
        payload: Bytes,
    ) -> Result<CachedMediaAccessUnit> {
        let stream_id = metadata.stream_id;
        let slot_id =
            usize::try_from(metadata.sequence).context("media access-unit sequence too large")?;
        if payload.len() > u32::MAX as usize {
            bail!(
                "media access-unit too large: {} bytes exceeds u32::MAX",
                payload.len()
            );
        }

        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: payload.len() as u32,
            fragment_offset: 0,
        };
        let mut serialized = Vec::with_capacity(MEDIA_FRAME_HEADER_LEN + payload.len());
        serialized.resize(MEDIA_FRAME_HEADER_LEN, 0);
        header
            .encode(&mut serialized[..MEDIA_FRAME_HEADER_LEN])
            .map_err(|err| anyhow!("media access-unit header encode failed: {err}"))?;
        serialized.extend_from_slice(&payload);
        let serialized = Bytes::from(serialized);

        self.chunk_cache
            .add_for_stream_id(stream_id, slot_id, serialized.clone())
            .await
            .map_err(|err| anyhow!("media access-unit cache write failed: {err}"))?;

        {
            let mut state = self.state.write().await;
            state.datagrams_received = state.datagrams_received.saturating_add(1);
            state.last_ingest_unix_ms = Some(now_unix_ms());
            state.bytes_received = state.bytes_received.saturating_add(payload.len() as u64);
        }

        debug!(
            stream_id,
            sequence = metadata.sequence,
            slot_id,
            payload_bytes = payload.len(),
            serialized_bytes = serialized.len(),
            codec = ?metadata.codec,
            keyframe = metadata.flags.is_keyframe(),
            "committed media access unit"
        );

        Ok(CachedMediaAccessUnit {
            metadata,
            payload_bytes: payload.len(),
            serialized,
            hash: 0,
        })
    }

    async fn get_media_access_unit(
        &self,
        stream_id: u64,
        sequence: u64,
    ) -> Option<CachedMediaAccessUnit> {
        let slot_id = usize::try_from(sequence).ok()?;
        let (serialized, hash) = self
            .chunk_cache
            .get_for_stream_id(stream_id, slot_id)
            .await?;
        if serialized.len() < MEDIA_FRAME_HEADER_LEN {
            return None;
        }
        let header = MediaFragmentHeader::decode(&serialized[..MEDIA_FRAME_HEADER_LEN]).ok()?;
        if header.metadata.stream_id != stream_id
            || header.metadata.sequence != sequence
            || header.fragment_index != 0
            || header.fragment_count != 1
            || header.fragment_offset != 0
        {
            return None;
        }
        let payload_bytes = serialized.len().checked_sub(MEDIA_FRAME_HEADER_LEN)?;
        if header.access_unit_len as usize != payload_bytes {
            return None;
        }
        Some(CachedMediaAccessUnit {
            metadata: header.metadata,
            payload_bytes,
            serialized,
            hash,
        })
    }

    async fn playlist(&self) -> String {
        self.playlist_for_stream_id(self.stream_id).await
    }

    async fn playlist_for_stream_id(&self, stream_id: u64) -> String {
        let Some((stream_idx, last)) = self.stream_position_for_id(stream_id).await else {
            let media_kind = self
                .media_kind_hint(stream_id)
                .await
                .unwrap_or(LiveMediaKind::Fmp4);
            let include_map = media_kind == LiveMediaKind::Fmp4
                && self.get_init_for_stream_id(stream_id).await.is_some();
            return self.empty_playlist(0, media_kind, include_map);
        };
        let first = last.saturating_sub(self.window_parts.saturating_sub(1));
        let mut available = Vec::new();
        let mut saw_fmp4 = false;
        let mut saw_ts = false;
        let mut discovered_init = None;
        for seq in first..=last {
            if let Some((bytes, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                let slot = LiveSlotPayload::decode(bytes);
                if hash != 0 || slot.has_media() {
                    match slot.media_kind() {
                        LiveMediaKind::Fmp4 => saw_fmp4 = true,
                        LiveMediaKind::Ts => saw_ts = true,
                    }
                    if let Some(init) = slot.init() {
                        discovered_init = Some(init);
                    }
                    available.push(seq as u64);
                }
            }
        }
        if available.is_empty() {
            let media_kind = self
                .media_kind_hint(stream_id)
                .await
                .unwrap_or(LiveMediaKind::Fmp4);
            let include_map = media_kind == LiveMediaKind::Fmp4
                && self.get_init_for_stream_id(stream_id).await.is_some();
            return self.empty_playlist(last, media_kind, include_map);
        }
        if let Some(init) = discovered_init {
            self.remember_stream_init(stream_id, init).await;
        }
        let media_kind = if saw_fmp4 {
            LiveMediaKind::Fmp4
        } else if saw_ts {
            LiveMediaKind::Ts
        } else {
            self.media_kind_hint(stream_id)
                .await
                .unwrap_or(LiveMediaKind::Fmp4)
        };
        self.remember_media_kind(stream_id, media_kind).await;
        let init = if media_kind == LiveMediaKind::Fmp4 {
            self.get_init_for_stream_id(stream_id).await
        } else {
            None
        };

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
        if media_kind == LiveMediaKind::Fmp4 {
            if init.is_some() {
                out.push_str("#EXT-X-MAP:URI=\"init.mp4\"\n");
            } else {
                warn!(
                    stream_id,
                    "fMP4 live playlist has media fragments but no init segment"
                );
            }
        }
        out.push_str(&format!("#EXT-X-PART-INF:PART-TARGET={part_target:.3}\n"));
        out.push_str(&format!(
            "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:.3},HOLD-BACK={:.3}\n",
            part_target * 3.0,
            (part_target * self.parts_per_segment as f64 * 2.0).max(3.0)
        ));

        let extension = media_kind.extension();
        for (segment, group) in groups {
            let mut duration = 0.0;
            for seq in &group {
                duration += part_target;
                out.push_str(&format!(
                    "#EXT-X-PART:DURATION={part_target:.3},URI=\"part{seq}.{extension}\"\n"
                ));
            }
            if group.len() == self.parts_per_segment {
                out.push_str(&format!("#EXTINF:{duration:.3},\n"));
                out.push_str(&format!("seg{segment}.{extension}\n"));
            }
        }

        out.push_str(&format!(
            "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part{next_part}.{extension}\"\n"
        ));
        out
    }

    fn empty_playlist(
        &self,
        next_part: usize,
        media_kind: LiveMediaKind,
        include_map: bool,
    ) -> String {
        let part_target = self.part_target.as_secs_f64();
        let target_duration = (part_target * self.parts_per_segment as f64)
            .ceil()
            .max(1.0) as u64;
        let extension = media_kind.extension();
        let map = if include_map {
            "#EXT-X-MAP:URI=\"init.mp4\"\n"
        } else {
            ""
        };
        format!(
            "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:{target_duration}\n#EXT-X-MEDIA-SEQUENCE:0\n{map}#EXT-X-PART-INF:PART-TARGET={part_target:.3}\n#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:.3},HOLD-BACK={:.3}\n#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part{next_part}.{extension}\"\n",
            part_target * 3.0,
            (part_target * self.parts_per_segment as f64 * 2.0).max(3.0)
        )
    }

    async fn remember_media_kind(&self, stream_id: u64, media_kind: LiveMediaKind) {
        let mut state = self.state.write().await;
        state.stream_media_kinds.insert(stream_id, media_kind);
    }

    async fn media_kind_hint(&self, stream_id: u64) -> Option<LiveMediaKind> {
        let state = self.state.read().await;
        state.stream_media_kinds.get(&stream_id).copied()
    }

    async fn remember_stream_init(&self, stream_id: u64, init: Bytes) {
        let mut state = self.state.write().await;
        state.stream_inits.insert(stream_id, init);
    }

    async fn get_init_for_stream_id(&self, stream_id: u64) -> Option<Bytes> {
        {
            let state = self.state.read().await;
            if let Some(init) = state.stream_inits.get(&stream_id) {
                return Some(init.clone());
            }
        }

        let (stream_idx, last) = self.stream_position_for_id(stream_id).await?;
        let first = self.chunk_cache.retained_start(last);
        for seq in (first..=last).rev() {
            let Some((bytes, _hash)) = self.chunk_cache.get(stream_idx, seq).await else {
                continue;
            };
            let slot = LiveSlotPayload::decode(bytes);
            if let Some(init) = slot.init() {
                self.remember_stream_init(stream_id, init.clone()).await;
                return Some(init);
            }
        }
        None
    }

    async fn stream_position(&self) -> Option<(usize, usize)> {
        self.stream_position_for_id(self.stream_id).await
    }

    async fn stream_position_for_id(&self, stream_id: u64) -> Option<(usize, usize)> {
        let stream_idx = self.chunk_cache.get_stream_idx(stream_id).await?;
        let last = self.chunk_cache.last(stream_idx)?;
        Some((stream_idx, last))
    }

    async fn get_part_for_stream_id(&self, stream_id: u64, seq: u64) -> Option<(Bytes, u64)> {
        let (bytes, hash) = self
            .chunk_cache
            .get_for_stream_id(stream_id, seq as usize)
            .await?;
        let slot = LiveSlotPayload::decode(bytes);
        if hash != 0 || slot.has_media() {
            if let Some(init) = slot.init() {
                self.remember_stream_init(stream_id, init).await;
            }
            self.remember_media_kind(stream_id, slot.media_kind()).await;
            return Some((slot.media(), hash));
        }
        None
    }

    async fn next_part_after_for_stream_id(
        &self,
        stream_id: u64,
        after: Option<u64>,
    ) -> Option<(u64, Bytes, u64)> {
        let (stream_idx, last) = self.stream_position_for_id(stream_id).await?;
        if let Some(after) = after {
            let start = after.checked_add(1)?;
            if start as usize > last {
                return None;
            }
            for seq in start as usize..=last {
                if let Some((bytes, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                    let slot = LiveSlotPayload::decode(bytes);
                    if hash != 0 || slot.has_media() {
                        if let Some(init) = slot.init() {
                            self.remember_stream_init(stream_id, init).await;
                        }
                        self.remember_media_kind(stream_id, slot.media_kind()).await;
                        return Some((seq as u64, slot.media(), hash));
                    }
                }
            }
            return None;
        }

        let first = last.saturating_sub(self.window_parts.saturating_sub(1));
        for seq in (first..=last).rev() {
            if let Some((bytes, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                let slot = LiveSlotPayload::decode(bytes);
                if hash != 0 || slot.has_media() {
                    if let Some(init) = slot.init() {
                        self.remember_stream_init(stream_id, init).await;
                    }
                    self.remember_media_kind(stream_id, slot.media_kind()).await;
                    return Some((seq as u64, slot.media(), hash));
                }
            }
        }
        None
    }

    async fn get_part_blocking(&self, seq: u64) -> Option<(Bytes, u64)> {
        self.get_part_blocking_for_stream_id(self.stream_id, seq)
            .await
    }

    async fn get_part_blocking_for_stream_id(
        &self,
        stream_id: u64,
        seq: u64,
    ) -> Option<(Bytes, u64)> {
        let deadline = Instant::now() + Duration::from_millis(PART_WAIT_MS);
        loop {
            if let Some((bytes, hash)) = self.get_part_for_stream_id(stream_id, seq).await {
                return Some((bytes, hash));
            }
            let Some((_, last)) = self.stream_position_for_id(stream_id).await else {
                return None;
            };
            if seq as usize > last || Instant::now() >= deadline {
                return None;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn get_segment(&self, segment: u64) -> Option<Bytes> {
        self.get_segment_for_stream_id(self.stream_id, segment)
            .await
    }

    async fn get_segment_for_stream_id(&self, stream_id: u64, segment: u64) -> Option<Bytes> {
        let first_part = segment.checked_mul(self.parts_per_segment as u64)?;
        let mut out = Vec::new();
        for offset in 0..self.parts_per_segment {
            let seq = first_part + offset as u64;
            let (bytes, _) = self.get_part_blocking_for_stream_id(stream_id, seq).await?;
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
            stream_id_text: stream_id_text(self.stream_id),
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

    async fn estimated_storage_bytes(&self) -> u64 {
        let mut bytes = 0u64;
        for stream_id in self.active_stream_ids().await {
            bytes = bytes.saturating_add(self.estimated_storage_bytes_for_stream(stream_id).await);
        }
        bytes
    }

    async fn estimated_storage_bytes_for_stream(&self, stream_id: u64) -> u64 {
        let Some(stream_idx) = self.chunk_cache.get_stream_idx(stream_id).await else {
            return 0;
        };
        let Some(last) = self.chunk_cache.last(stream_idx) else {
            return 0;
        };
        self.estimated_storage_bytes_for_idx(stream_idx, last).await
    }

    async fn estimated_storage_bytes_for_idx(&self, stream_idx: usize, last: usize) -> u64 {
        let first = last.saturating_sub(self.window_parts.saturating_sub(1));
        let mut bytes = 0u64;
        for seq in first..=last {
            if let Some((payload, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                if hash != 0 || !payload.is_empty() {
                    bytes = bytes.saturating_add(payload.len() as u64);
                }
            }
        }
        bytes
    }

    async fn active_stream_ids(&self) -> Vec<u64> {
        let mut stream_ids = Vec::new();
        for (stream_id, stream_idx) in self.chunk_cache.stream_ids().await {
            let Some(last) = self.chunk_cache.last(stream_idx) else {
                continue;
            };
            let mut active = false;
            for seq in (0..=last).rev().take(self.window_parts) {
                if let Some((payload, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                    if hash != 0 || !payload.is_empty() {
                        active = true;
                        break;
                    }
                }
            }
            if active {
                stream_ids.push(stream_id);
            }
        }
        stream_ids.sort_unstable();
        stream_ids.dedup();
        stream_ids
    }

    async fn stream_telemetry(
        &self,
        node_id: &str,
        default_stats: &StatsSnapshot,
    ) -> Vec<StreamTelemetry> {
        let mut streams = Vec::new();
        for (stream_id, stream_idx) in self.chunk_cache.stream_ids().await {
            let Some(last) = self.chunk_cache.last(stream_idx) else {
                continue;
            };
            let first = last.saturating_sub(self.window_parts.saturating_sub(1));
            let mut latest_part = None;
            let mut latest_part_bytes = None;
            let mut bytes_received = 0u64;
            let mut datagrams_received = 0u64;

            for seq in first..=last {
                if let Some((payload, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                    if hash != 0 || !payload.is_empty() {
                        latest_part = Some(seq as u64);
                        latest_part_bytes = Some(payload.len());
                        bytes_received = bytes_received.saturating_add(payload.len() as u64);
                        datagrams_received = datagrams_received.saturating_add(1);
                    }
                }
            }

            let Some(latest_part) = latest_part else {
                continue;
            };
            let is_default_stream = stream_id == self.stream_id;
            streams.push(StreamTelemetry {
                node_id: node_id.to_string(),
                stream_id,
                stream_id_text: stream_id_text(stream_id),
                latest_local_part: if is_default_stream {
                    default_stats.latest_local_part
                } else {
                    None
                },
                latest_local_part_bytes: if is_default_stream {
                    default_stats.latest_local_part_bytes.or(latest_part_bytes)
                } else {
                    latest_part_bytes
                },
                latest_local_part_duration_ms: if is_default_stream {
                    default_stats.latest_local_part_duration_ms
                } else {
                    None
                },
                latest_local_part_age_ms: if is_default_stream {
                    default_stats.latest_local_part_age_ms
                } else {
                    None
                },
                latest_mesh_part: Some(latest_part),
                bytes_received: if is_default_stream {
                    default_stats.bytes_received.max(bytes_received)
                } else {
                    bytes_received
                },
                datagrams_received: if is_default_stream {
                    default_stats.datagrams_received.max(datagrams_received)
                } else {
                    datagrams_received
                },
                last_ingest_age_ms: if is_default_stream {
                    default_stats.last_ingest_age_ms
                } else {
                    None
                },
                stale_threshold_ms: Some(stream_stale_threshold_ms(
                    default_stats.part_target_ms,
                    default_stats.window_parts,
                )),
            });
        }
        streams.sort_by_key(|stream| stream.stream_id);
        streams
    }

    async fn replica_request_from_slot(&self, stream_id: u64) -> usize {
        let Some(stream_idx) = self.chunk_cache.get_stream_idx(stream_id).await else {
            return 0;
        };
        let Some(last) = self.chunk_cache.last(stream_idx) else {
            return 0;
        };
        for seq in (0..=last).rev().take(self.window_parts) {
            if let Some((bytes, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                if hash != 0 || !bytes.is_empty() {
                    return seq.saturating_add(1);
                }
            }
        }
        0
    }

    async fn mesh_snapshot(
        &self,
        mesh: &CacheMeshHandle,
        mut node: MeshNode,
        policy: ReplicationPolicy,
        control: &ControlPlane,
    ) -> MeshSnapshot {
        let stats = self.stats(mesh).await;
        let streams = self.stream_telemetry(&node.node_id, &stats).await;
        node.used_storage_bytes = self
            .estimated_storage_bytes()
            .await
            .min(node.total_storage_bytes);
        node.active_streams = streams.len() as u64;
        node.contributor_streams = streams
            .iter()
            .filter(|stream| stream.latest_local_part.is_some())
            .count() as u64;

        MeshSnapshot {
            updated_unix_ms: now_unix_ms(),
            node,
            mesh_addr: Some(mesh.local_addr().to_string()),
            edge_service: None,
            peers: stats
                .mesh_peers
                .iter()
                .map(|addr| PeerSnapshot {
                    addr: addr.clone(),
                    state: "discovered".into(),
                })
                .collect(),
            stream: stats,
            streams,
            replication_policy: policy,
            recent_commands: control.recent().await,
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
    stream_next_seq: HashMap<u64, u64>,
    stream_inits: HashMap<u64, Bytes>,
    stream_media_kinds: HashMap<u64, LiveMediaKind>,
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
            stream_next_seq: HashMap::new(),
            stream_inits: HashMap::new(),
            stream_media_kinds: HashMap::new(),
        }
    }

    fn next_stream_seq(&mut self, stream_id: u64) -> u64 {
        let next = self.stream_next_seq.entry(stream_id).or_insert(0);
        let seq = *next;
        *next = next.saturating_add(1);
        seq
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatsSnapshot {
    stream_id: u64,
    #[serde(default)]
    stream_id_text: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MeshSnapshot {
    updated_unix_ms: u64,
    node: MeshNode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mesh_addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    edge_service: Option<EdgeServiceSnapshot>,
    peers: Vec<PeerSnapshot>,
    stream: StatsSnapshot,
    #[serde(default)]
    streams: Vec<StreamTelemetry>,
    replication_policy: ReplicationPolicy,
    recent_commands: Vec<ControlCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeerSnapshot {
    addr: String,
    state: String,
}

#[derive(Debug, Clone, Serialize)]
struct MeshApiSnapshot {
    updated_unix_ms: u64,
    node: MeshNode,
    peers: Vec<PeerSnapshot>,
    stream: StatsSnapshot,
    replication_policy: ReplicationPolicy,
    recent_commands: Vec<ControlCommand>,
    planned_replicas: Vec<ReplicaPlacementSnapshot>,
    aggregate: AggregateMetrics,
    alerts: Vec<MeshAlert>,
    activity: Vec<MeshActivity>,
    telemetry: TelemetryHealthSnapshot,
    orchestration: OrchestrationStatus,
    nodes: Vec<MeshNode>,
    edge_services: Vec<EdgeServiceSnapshot>,
    connections: Vec<ConnectionSnapshot>,
    streams: Vec<StreamTelemetry>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct TelemetryPeerStatus {
    peer: String,
    state: String,
    connect_attempts: u64,
    disconnects: u64,
    payloads: u64,
    bytes: u64,
    last_connected_unix_ms: Option<u64>,
    last_payload_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct ReplicaPlacementSnapshot {
    stream_id: u64,
    stream_id_text: String,
    target_node_id: String,
    reason: ReplicaReason,
    reason_text: String,
    score: f64,
}

impl From<ReplicaPlacement> for ReplicaPlacementSnapshot {
    fn from(placement: ReplicaPlacement) -> Self {
        let reason_text = replica_reason_text(&placement.reason);
        Self {
            stream_id: placement.stream_id,
            stream_id_text: stream_id_text(placement.stream_id),
            target_node_id: placement.target_node_id,
            reason: placement.reason,
            reason_text,
            score: placement.score,
        }
    }
}

fn replica_reason_text(reason: &ReplicaReason) -> String {
    match reason {
        ReplicaReason::BaselineRegion { region } => format!("baseline region {region}"),
        ReplicaReason::BaselineContinent { continent } => {
            format!("baseline continent {continent}")
        }
        ReplicaReason::DemandRegion { region } => format!("demand region {region}"),
        ReplicaReason::DemandContinent { continent } => format!("demand continent {continent}"),
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct AggregateMetrics {
    node_count: usize,
    connection_count: usize,
    total_storage_bytes: u64,
    used_storage_bytes: u64,
    total_egress_capacity_bps: u64,
    contributor_streams: u64,
    active_streams: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct TelemetryHealthSnapshot {
    stale_after_ms: u64,
    fresh_remote_count: usize,
    stale_remote_count: usize,
    stale_nodes: Vec<TelemetryNodeHealth>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TelemetryNodeHealth {
    node_id: String,
    region: String,
    updated_unix_ms: u64,
    age_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct OrchestrationStatus {
    control_dispatch_ready: bool,
    provision: ProvisionStatus,
    telemetry_peers: Vec<TelemetryPeerStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct ProvisionStatus {
    enabled: bool,
    backends: Vec<String>,
    timeout_ms: u64,
    backend_statuses: Vec<ProvisionBackendStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct ProvisionBackendStatus {
    name: String,
    state: &'static str,
    details: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct MeshAlert {
    level: &'static str,
    code: &'static str,
    message: String,
    count: u64,
    last_seen_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_id_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct MeshActivity {
    level: &'static str,
    code: String,
    message: String,
    count: u64,
    seen_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_id_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ConnectionSnapshot {
    source_node_id: String,
    target_addr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_node_id: Option<String>,
    state: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct EdgeServiceSnapshot {
    node_id: String,
    region: String,
    continent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    playback_base_url: Option<String>,
    active_readers: u64,
    requests_served: u64,
    bytes_served: u64,
    llhls_tail_requests: u64,
    #[serde(default)]
    responses_total: u64,
    #[serde(default)]
    response_errors: u64,
    #[serde(default)]
    response_not_found: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_response_unix_ms: Option<u64>,
    #[serde(default)]
    recent_responses: Vec<EdgeResponseSnapshot>,
    draining: bool,
}

impl EdgeServiceSnapshot {
    fn from_node(
        node: &MeshNode,
        playback_base_url: Option<String>,
        load: EdgeLoadSnapshot,
    ) -> Self {
        Self {
            node_id: node.node_id.clone(),
            region: node.region.clone(),
            continent: node.continent.clone(),
            playback_base_url,
            active_readers: load.active_readers,
            requests_served: load.requests_served,
            bytes_served: load.bytes_served,
            llhls_tail_requests: load.llhls_tail_requests,
            responses_total: load.responses_total,
            response_errors: load.response_errors,
            response_not_found: load.response_not_found,
            last_response_unix_ms: load.last_response_unix_ms,
            recent_responses: load.recent_responses,
            draining: node.draining,
        }
    }

    fn fallback_for_node(node: &MeshNode) -> Self {
        Self::from_node(node, None, EdgeLoadSnapshot::default())
    }
}

#[derive(Debug, Clone, Default)]
struct EdgeLoadSnapshot {
    active_readers: u64,
    requests_served: u64,
    bytes_served: u64,
    llhls_tail_requests: u64,
    responses_total: u64,
    response_errors: u64,
    response_not_found: u64,
    last_response_unix_ms: Option<u64>,
    recent_responses: Vec<EdgeResponseSnapshot>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct EdgeResponseSnapshot {
    unix_ms: u64,
    method: String,
    path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    status: u16,
    bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct EdgeLoad {
    inner: Arc<EdgeLoadInner>,
}

#[derive(Debug, Default)]
struct EdgeLoadInner {
    active_readers: AtomicU64,
    requests_served: AtomicU64,
    bytes_served: AtomicU64,
    llhls_tail_requests: AtomicU64,
    responses_total: AtomicU64,
    response_errors: AtomicU64,
    response_not_found: AtomicU64,
    last_response_unix_ms: AtomicU64,
    recent_responses: StdMutex<VecDeque<EdgeResponseSnapshot>>,
}

impl EdgeLoad {
    fn begin_read(&self, llhls_tail: bool) -> EdgeReadGuard {
        self.inner.active_readers.fetch_add(1, Ordering::Relaxed);
        self.inner.requests_served.fetch_add(1, Ordering::Relaxed);
        if llhls_tail {
            self.inner
                .llhls_tail_requests
                .fetch_add(1, Ordering::Relaxed);
        }
        EdgeReadGuard {
            load: self.clone(),
            finished: false,
        }
    }

    fn snapshot(&self, node: &MeshNode, playback_base_url: Option<String>) -> EdgeServiceSnapshot {
        let recent_responses = self
            .inner
            .recent_responses
            .lock()
            .map(|responses| responses.iter().cloned().collect())
            .unwrap_or_default();
        let last_response_unix_ms = match self.inner.last_response_unix_ms.load(Ordering::Relaxed) {
            0 => None,
            value => Some(value),
        };
        EdgeServiceSnapshot::from_node(
            node,
            playback_base_url,
            EdgeLoadSnapshot {
                active_readers: self.inner.active_readers.load(Ordering::Relaxed),
                requests_served: self.inner.requests_served.load(Ordering::Relaxed),
                bytes_served: self.inner.bytes_served.load(Ordering::Relaxed),
                llhls_tail_requests: self.inner.llhls_tail_requests.load(Ordering::Relaxed),
                responses_total: self.inner.responses_total.load(Ordering::Relaxed),
                response_errors: self.inner.response_errors.load(Ordering::Relaxed),
                response_not_found: self.inner.response_not_found.load(Ordering::Relaxed),
                last_response_unix_ms,
                recent_responses,
            },
        )
    }

    fn record_response(
        &self,
        method: &Method,
        path: &str,
        query: Option<&str>,
        response: &HandlerResponse,
    ) {
        let unix_ms = now_unix_ms();
        let status = response.status.as_u16();
        let bytes = response
            .body
            .as_ref()
            .map(|body| body.len() as u64)
            .unwrap_or(0);
        self.inner.responses_total.fetch_add(1, Ordering::Relaxed);
        if response.status.is_client_error() || response.status.is_server_error() {
            self.inner.response_errors.fetch_add(1, Ordering::Relaxed);
        }
        if response.status == StatusCode::NOT_FOUND {
            self.inner
                .response_not_found
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner
            .last_response_unix_ms
            .store(unix_ms, Ordering::Relaxed);

        if let Ok(mut responses) = self.inner.recent_responses.lock() {
            responses.push_front(EdgeResponseSnapshot {
                unix_ms,
                method: method.as_str().into(),
                path: path.into(),
                query: query.map(ToOwned::to_owned),
                status,
                bytes,
                content_type: response.content_type.clone(),
            });
            while responses.len() > EDGE_RECENT_RESPONSE_LIMIT {
                responses.pop_back();
            }
        }
    }
}

struct EdgeReadGuard {
    load: EdgeLoad,
    finished: bool,
}

impl EdgeReadGuard {
    fn finish(mut self, bytes_served: usize) {
        self.load
            .inner
            .bytes_served
            .fetch_add(bytes_served as u64, Ordering::Relaxed);
        self.finished = true;
    }
}

impl Drop for EdgeReadGuard {
    fn drop(&mut self) {
        self.load
            .inner
            .active_readers
            .fetch_sub(1, Ordering::Relaxed);
        if !self.finished {
            self.load.inner.bytes_served.fetch_add(0, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamTelemetry {
    node_id: String,
    stream_id: u64,
    #[serde(default)]
    stream_id_text: String,
    latest_local_part: Option<u64>,
    latest_local_part_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_local_part_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_local_part_age_ms: Option<u64>,
    latest_mesh_part: Option<u64>,
    bytes_received: u64,
    datagrams_received: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_ingest_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stale_threshold_ms: Option<u64>,
}

impl StreamTelemetry {
    fn from_stats(node_id: String, stats: &StatsSnapshot) -> Self {
        Self {
            node_id,
            stream_id: stats.stream_id,
            stream_id_text: stream_id_text(stats.stream_id),
            latest_local_part: stats.latest_local_part,
            latest_local_part_bytes: stats.latest_local_part_bytes,
            latest_local_part_duration_ms: stats.latest_local_part_duration_ms,
            latest_local_part_age_ms: stats.latest_local_part_age_ms,
            latest_mesh_part: stats.latest_mesh_part,
            bytes_received: stats.bytes_received,
            datagrams_received: stats.datagrams_received,
            last_ingest_age_ms: stats.last_ingest_age_ms,
            stale_threshold_ms: Some(stream_stale_threshold_ms(
                stats.part_target_ms,
                stats.window_parts,
            )),
        }
    }

    fn active(&self) -> bool {
        self.latest_local_part.is_some() || self.latest_mesh_part.is_some()
    }

    fn stale(&self) -> bool {
        self.active()
            && self.last_ingest_age_ms.is_some_and(|age_ms| {
                age_ms
                    > self
                        .stale_threshold_ms
                        .unwrap_or(MESH_MIN_STALE_INGEST_ALERT_MS)
            })
    }
}

fn stream_stale_threshold_ms(part_target_ms: u64, window_parts: usize) -> u64 {
    part_target_ms
        .saturating_mul(window_parts as u64)
        .max(MESH_MIN_STALE_INGEST_ALERT_MS)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MeshProtocolRequest {
    Snapshot,
    ProvisionNode {
        node_id: Option<String>,
        region: Option<String>,
    },
    CloseNode {
        node_id: Option<String>,
        region: Option<String>,
    },
    WarmStream {
        #[serde(default, deserialize_with = "deserialize_optional_u64_from_any")]
        stream_id: Option<u64>,
        region: Option<String>,
    },
}

#[derive(Debug, Serialize)]
struct MeshProtocolResponse {
    ok: bool,
    response_type: &'static str,
    snapshot: Option<MeshApiSnapshot>,
    command: Option<ControlCommand>,
    media_access_unit: Option<MediaAccessUnitResponse>,
    error: Option<String>,
}

impl MeshProtocolResponse {
    fn snapshot(snapshot: MeshApiSnapshot) -> Self {
        Self {
            ok: true,
            response_type: "snapshot",
            snapshot: Some(snapshot),
            command: None,
            media_access_unit: None,
            error: None,
        }
    }

    fn command(command: ControlCommand) -> Self {
        Self {
            ok: true,
            response_type: "command",
            snapshot: None,
            command: Some(command),
            media_access_unit: None,
            error: None,
        }
    }

    fn media_access_unit(unit: MediaAccessUnitResponse) -> Self {
        Self {
            ok: true,
            response_type: "media_access_unit",
            snapshot: None,
            command: None,
            media_access_unit: Some(unit),
            error: None,
        }
    }

    fn error(error: impl Into<String>) -> Self {
        Self {
            ok: false,
            response_type: "error",
            snapshot: None,
            command: None,
            media_access_unit: None,
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlEnvelope {
    id: u64,
    origin_node_id: String,
    kind: ControlKind,
    request: ControlRequest,
    #[serde(default)]
    target_node_ids: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct NodeLifecycle {
    draining: Arc<RwLock<bool>>,
}

impl NodeLifecycle {
    async fn is_draining(&self) -> bool {
        *self.draining.read().await
    }

    async fn set_draining(&self, draining: bool) {
        *self.draining.write().await = draining;
    }
}

#[derive(Debug, Clone, Default)]
struct DemandTracker {
    last_replica_request_unix_ms: Arc<RwLock<HashMap<u64, u64>>>,
}

impl DemandTracker {
    async fn should_request_replica(&self, stream_id: u64, now_ms: u64) -> bool {
        let mut requests = self.last_replica_request_unix_ms.write().await;
        let should_request = requests
            .get(&stream_id)
            .map(|last| now_ms.saturating_sub(*last) >= REPLICA_REQUEST_MIN_INTERVAL_MS)
            .unwrap_or(true);
        if should_request {
            requests.insert(stream_id, now_ms);
        }
        should_request
    }
}

#[derive(Debug, Clone)]
struct TelemetryAggregator {
    snapshots: Arc<RwLock<HashMap<String, MeshSnapshot>>>,
    stale_nodes: Arc<RwLock<Vec<TelemetryNodeHealth>>>,
    stale_after_ms: u64,
}

impl Default for TelemetryAggregator {
    fn default() -> Self {
        Self::new(DEFAULT_TELEMETRY_STALE_MS)
    }
}

impl TelemetryAggregator {
    fn new(stale_after_ms: u64) -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            stale_nodes: Arc::new(RwLock::new(Vec::new())),
            stale_after_ms,
        }
    }

    async fn ingest_payload(&self, payload: TcpChangesPayload) -> Result<bool> {
        if payload.tag != TELEMETRY_TAG {
            return Ok(false);
        }
        let snapshot: MeshSnapshot = serde_json::from_slice(&payload.val)
            .context("failed to decode AVMT mesh telemetry payload")?;
        self.ingest_snapshot(snapshot).await;
        Ok(true)
    }

    async fn ingest_snapshot(&self, snapshot: MeshSnapshot) {
        let node_id = snapshot.node.node_id.clone();
        self.snapshots
            .write()
            .await
            .insert(node_id.clone(), snapshot);
        self.stale_nodes
            .write()
            .await
            .retain(|node| node.node_id != node_id);
    }

    #[cfg(test)]
    async fn snapshot(&self, local: MeshSnapshot) -> MeshApiSnapshot {
        let (snapshots, telemetry) = self.snapshots_with_local(local.clone()).await;
        MeshApiSnapshot::from_snapshots(
            local,
            snapshots,
            telemetry,
            Vec::new(),
            OrchestrationStatus::default(),
        )
    }

    async fn snapshots_with_local(
        &self,
        local: MeshSnapshot,
    ) -> (Vec<MeshSnapshot>, TelemetryHealthSnapshot) {
        let now_ms = now_unix_ms();
        let mut snapshots = self.snapshots.write().await;
        let stale_nodes = snapshots
            .values()
            .filter(|snapshot| self.is_stale(snapshot, now_ms))
            .map(|snapshot| TelemetryNodeHealth::from_snapshot(snapshot, now_ms))
            .collect::<Vec<_>>();
        snapshots.retain(|_, snapshot| !self.is_stale(snapshot, now_ms));
        let fresh_remote_count = snapshots.len();
        if !stale_nodes.is_empty() {
            self.remember_stale_nodes(stale_nodes).await;
        }
        let remembered_stale_nodes = self.stale_nodes.read().await.clone();
        let telemetry = TelemetryHealthSnapshot {
            stale_after_ms: self.stale_after_ms,
            fresh_remote_count,
            stale_remote_count: remembered_stale_nodes.len(),
            stale_nodes: remembered_stale_nodes,
        };

        let mut snapshots_with_local = snapshots.clone();
        snapshots_with_local.insert(local.node.node_id.clone(), local);
        let mut snapshots_with_local = snapshots_with_local.into_values().collect::<Vec<_>>();
        snapshots_with_local.sort_by(|left, right| left.node.node_id.cmp(&right.node.node_id));
        (snapshots_with_local, telemetry)
    }

    fn is_stale(&self, snapshot: &MeshSnapshot, now_ms: u64) -> bool {
        self.stale_after_ms > 0
            && now_ms.saturating_sub(snapshot.updated_unix_ms) > self.stale_after_ms
    }

    async fn remember_stale_nodes(&self, stale_nodes: Vec<TelemetryNodeHealth>) {
        let mut remembered = self.stale_nodes.write().await;
        for stale_node in stale_nodes {
            if let Some(existing) = remembered
                .iter_mut()
                .find(|node| node.node_id == stale_node.node_id)
            {
                *existing = stale_node;
            } else {
                remembered.push(stale_node);
            }
        }
        remembered.sort_by(|left, right| {
            right
                .age_ms
                .cmp(&left.age_ms)
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
        remembered.truncate(32);
    }
}

impl TelemetryNodeHealth {
    fn from_snapshot(snapshot: &MeshSnapshot, now_ms: u64) -> Self {
        Self {
            node_id: snapshot.node.node_id.clone(),
            region: snapshot.node.region.clone(),
            updated_unix_ms: snapshot.updated_unix_ms,
            age_ms: now_ms.saturating_sub(snapshot.updated_unix_ms),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct TelemetryPeerMonitor {
    peers: Arc<RwLock<HashMap<String, TelemetryPeerStatus>>>,
}

impl TelemetryPeerMonitor {
    fn new(peers: &[SocketAddr]) -> Self {
        let peers = peers
            .iter()
            .map(|peer| {
                let peer = peer.to_string();
                (
                    peer.clone(),
                    TelemetryPeerStatus {
                        peer,
                        state: "configured".into(),
                        ..TelemetryPeerStatus::default()
                    },
                )
            })
            .collect();
        Self {
            peers: Arc::new(RwLock::new(peers)),
        }
    }

    async fn record_connecting(&self, peer: SocketAddr) {
        let mut peers = self.peers.write().await;
        let status = peers
            .entry(peer.to_string())
            .or_insert_with(|| TelemetryPeerStatus {
                peer: peer.to_string(),
                state: "configured".into(),
                ..TelemetryPeerStatus::default()
            });
        status.state = "connecting".into();
        status.connect_attempts = status.connect_attempts.saturating_add(1);
    }

    async fn record_connected(&self, peer: SocketAddr) {
        let mut peers = self.peers.write().await;
        let status = peers
            .entry(peer.to_string())
            .or_insert_with(|| TelemetryPeerStatus {
                peer: peer.to_string(),
                ..TelemetryPeerStatus::default()
            });
        status.state = "connected".into();
        status.last_connected_unix_ms = Some(now_unix_ms());
        status.last_error = None;
    }

    async fn record_payload(&self, peer: SocketAddr, bytes: usize) {
        let mut peers = self.peers.write().await;
        let status = peers
            .entry(peer.to_string())
            .or_insert_with(|| TelemetryPeerStatus {
                peer: peer.to_string(),
                ..TelemetryPeerStatus::default()
            });
        status.payloads = status.payloads.saturating_add(1);
        status.bytes = status.bytes.saturating_add(bytes as u64);
        status.last_payload_unix_ms = Some(now_unix_ms());
    }

    async fn record_disconnected(&self, peer: SocketAddr, error: Option<String>) {
        let mut peers = self.peers.write().await;
        let status = peers
            .entry(peer.to_string())
            .or_insert_with(|| TelemetryPeerStatus {
                peer: peer.to_string(),
                ..TelemetryPeerStatus::default()
            });
        status.state = if error.is_some() {
            "error".into()
        } else {
            "disconnected".into()
        };
        status.disconnects = status.disconnects.saturating_add(1);
        status.last_error = error;
    }

    async fn snapshot(&self) -> Vec<TelemetryPeerStatus> {
        let mut peers = self
            .peers
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        peers.sort_by(|left, right| left.peer.cmp(&right.peer));
        peers
    }
}

impl MeshSnapshot {
    fn with_edge_service(mut self, edge_service: EdgeServiceSnapshot) -> Self {
        self.edge_service = Some(edge_service);
        self
    }
}

impl MeshApiSnapshot {
    fn from_snapshots(
        local: MeshSnapshot,
        snapshots: Vec<MeshSnapshot>,
        telemetry: TelemetryHealthSnapshot,
        planned_replicas: Vec<ReplicaPlacement>,
        orchestration: OrchestrationStatus,
    ) -> Self {
        let mut aggregate = AggregateMetrics::default();
        let mut nodes = Vec::with_capacity(snapshots.len());
        let mut edge_services = Vec::with_capacity(snapshots.len());
        let mut connections = Vec::new();
        let mut streams = Vec::with_capacity(snapshots.len());
        let mut peer_addr_to_node_id = HashMap::with_capacity(snapshots.len() * 2);

        for snapshot in &snapshots {
            peer_addr_to_node_id
                .insert(snapshot.node.node_id.clone(), snapshot.node.node_id.clone());
            if let Some(mesh_addr) = &snapshot.mesh_addr {
                peer_addr_to_node_id.insert(mesh_addr.clone(), snapshot.node.node_id.clone());
            }
        }

        for snapshot in snapshots {
            aggregate.node_count += 1;
            aggregate.total_storage_bytes = aggregate
                .total_storage_bytes
                .saturating_add(snapshot.node.total_storage_bytes);
            aggregate.used_storage_bytes = aggregate
                .used_storage_bytes
                .saturating_add(snapshot.node.used_storage_bytes);
            aggregate.total_egress_capacity_bps = aggregate
                .total_egress_capacity_bps
                .saturating_add(snapshot.node.egress_capacity_bps);
            aggregate.contributor_streams = aggregate
                .contributor_streams
                .saturating_add(snapshot.node.contributor_streams);
            aggregate.active_streams = aggregate
                .active_streams
                .saturating_add(snapshot.node.active_streams);

            connections.extend(snapshot.peers.iter().map(|peer| ConnectionSnapshot {
                source_node_id: snapshot.node.node_id.clone(),
                target_addr: peer.addr.clone(),
                target_node_id: peer_addr_to_node_id.get(&peer.addr).cloned(),
                state: peer.state.clone(),
            }));
            if snapshot.streams.is_empty() {
                streams.push(StreamTelemetry::from_stats(
                    snapshot.node.node_id.clone(),
                    &snapshot.stream,
                ));
            } else {
                streams.extend(snapshot.streams.iter().cloned());
            }
            edge_services.push(
                snapshot
                    .edge_service
                    .clone()
                    .unwrap_or_else(|| EdgeServiceSnapshot::fallback_for_node(&snapshot.node)),
            );
            nodes.push(snapshot.node);
        }

        connections.sort_by(|left, right| {
            left.source_node_id
                .cmp(&right.source_node_id)
                .then_with(|| left.target_addr.cmp(&right.target_addr))
                .then_with(|| left.target_node_id.cmp(&right.target_node_id))
        });
        connections.dedup();
        aggregate.connection_count = connections.len();

        let planned_replicas = planned_replicas
            .into_iter()
            .map(ReplicaPlacementSnapshot::from)
            .collect::<Vec<_>>();
        let recent_commands = local.recent_commands;
        let alerts = derive_mesh_alerts(
            &aggregate,
            &nodes,
            &edge_services,
            &connections,
            &local.stream,
            &local.node.node_id,
            &streams,
            &recent_commands,
            &telemetry,
            &orchestration.provision,
            &orchestration.telemetry_peers,
        );
        let activity = derive_mesh_activity(&aggregate, &alerts, &recent_commands);

        MeshApiSnapshot {
            updated_unix_ms: now_unix_ms(),
            node: local.node,
            peers: local.peers,
            stream: local.stream,
            replication_policy: local.replication_policy,
            recent_commands,
            planned_replicas,
            aggregate,
            alerts,
            activity,
            telemetry,
            orchestration,
            nodes,
            edge_services,
            connections,
            streams,
        }
    }
}

fn derive_mesh_alerts(
    aggregate: &AggregateMetrics,
    nodes: &[MeshNode],
    edge_services: &[EdgeServiceSnapshot],
    connections: &[ConnectionSnapshot],
    local_stream: &StatsSnapshot,
    local_node_id: &str,
    streams: &[StreamTelemetry],
    recent_commands: &[ControlCommand],
    telemetry: &TelemetryHealthSnapshot,
    provision: &ProvisionStatus,
    telemetry_peers: &[TelemetryPeerStatus],
) -> Vec<MeshAlert> {
    let now = now_unix_ms();
    let mut alerts = Vec::new();

    if aggregate.node_count <= 1 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "mesh_single_node",
            message:
                "Only one mesh node is visible; failover and regional routing are not available."
                    .into(),
            count: aggregate.node_count as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    } else if aggregate.connection_count == 0 {
        alerts.push(MeshAlert {
            level: "error",
            code: "mesh_no_links",
            message: "Multiple mesh nodes are visible, but no mesh links are currently reported."
                .into(),
            count: aggregate.node_count as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    let unknown_peers = connections
        .iter()
        .filter(|connection| connection.target_node_id.is_none())
        .count();
    if unknown_peers > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "mesh_unknown_peers",
            message: "Some mesh peer addresses do not resolve to known node ids.".into(),
            count: unknown_peers as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    let draining_nodes = nodes.iter().filter(|node| node.draining).count();
    if draining_nodes > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "nodes_draining",
            message: "One or more mesh nodes are marked draining.".into(),
            count: draining_nodes as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    let playback_missing = edge_services
        .iter()
        .filter(|edge| edge.playback_base_url.is_none())
        .count();
    if playback_missing > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "edge_playback_missing",
            message: "One or more nodes do not advertise a public LL-HLS playback base URL.".into(),
            count: playback_missing as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    let edge_response_errors = edge_services
        .iter()
        .map(|edge| edge.response_errors)
        .sum::<u64>();
    let edge_recent_errors = edge_services
        .iter()
        .flat_map(|edge| {
            edge.recent_responses
                .iter()
                .filter(|response| response.status >= 400)
                .map(move |response| (edge, response))
        })
        .max_by_key(|(_, response)| response.unix_ms);
    if edge_response_errors > 0 {
        let (node_id, path, status, last_seen) = edge_recent_errors
            .map(|(edge, response)| {
                (
                    Some(edge.node_id.clone()),
                    response.path.clone(),
                    response.status,
                    Some(response.unix_ms),
                )
            })
            .unwrap_or((None, "unknown edge path".into(), 0, Some(now)));
        alerts.push(MeshAlert {
            level: if status >= 500 { "error" } else { "warn" },
            code: "edge_response_errors",
            message: format!(
                "Edge playback/API paths have returned {edge_response_errors} non-success response(s); latest was HTTP {status} for {path}."
            ),
            count: edge_response_errors,
            last_seen_unix_ms: last_seen,
            node_id,
            stream_id_text: None,
        });
    }

    let storage_error_nodes = nodes
        .iter()
        .filter(|node| storage_percent(node) >= MESH_STORAGE_ERROR_PCT)
        .count();
    let storage_warn_nodes = nodes
        .iter()
        .filter(|node| storage_percent(node) >= MESH_STORAGE_WARN_PCT)
        .count();
    if storage_error_nodes > 0 {
        alerts.push(MeshAlert {
            level: "error",
            code: "storage_exhausted",
            message: format!(
                "{storage_error_nodes} node(s) are at or above {MESH_STORAGE_ERROR_PCT}% storage usage."
            ),
            count: storage_error_nodes as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    } else if storage_warn_nodes > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "storage_pressure",
            message: format!(
                "{storage_warn_nodes} node(s) are at or above {MESH_STORAGE_WARN_PCT}% storage usage."
            ),
            count: storage_warn_nodes as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    let mut failed_commands = 0u64;
    let mut skipped_commands = 0u64;
    let mut last_command_issue = None;
    for command in recent_commands {
        let status = command.status.to_ascii_lowercase();
        if status.contains("failed") || status.contains("timed out") || status.contains("error") {
            failed_commands = failed_commands.saturating_add(1);
            last_command_issue = Some(last_command_issue.unwrap_or(0).max(command.created_unix_ms));
        } else if status.contains("skipped") {
            skipped_commands = skipped_commands.saturating_add(1);
            last_command_issue = Some(last_command_issue.unwrap_or(0).max(command.created_unix_ms));
        }
    }
    if failed_commands > 0 {
        alerts.push(MeshAlert {
            level: "error",
            code: "control_failures",
            message: "One or more recent orchestration commands failed.".into(),
            count: failed_commands,
            last_seen_unix_ms: last_command_issue,
            node_id: None,
            stream_id_text: None,
        });
    } else if skipped_commands > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "control_skipped",
            message: "One or more recent orchestration commands were skipped.".into(),
            count: skipped_commands,
            last_seen_unix_ms: last_command_issue,
            node_id: None,
            stream_id_text: None,
        });
    }

    let unavailable_telemetry_peers = telemetry_peers
        .iter()
        .filter(|peer| peer.state != "connected")
        .count();
    if unavailable_telemetry_peers > 0 {
        let latest_error_peer = telemetry_peers
            .iter()
            .filter(|peer| peer.state != "connected")
            .max_by_key(|peer| {
                peer.last_payload_unix_ms
                    .or(peer.last_connected_unix_ms)
                    .unwrap_or(0)
            });
        let peer = latest_error_peer
            .map(|peer| peer.peer.as_str())
            .unwrap_or("unknown peer");
        let state = latest_error_peer
            .map(|peer| peer.state.as_str())
            .unwrap_or("unknown");
        alerts.push(MeshAlert {
            level: "warn",
            code: "telemetry_peer_unavailable",
            message: format!(
                "{unavailable_telemetry_peers} tcp-changes telemetry peer(s) are not connected; latest is {peer} ({state})."
            ),
            count: unavailable_telemetry_peers as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    if telemetry.stale_remote_count > 0 {
        let latest_stale_node = telemetry
            .stale_nodes
            .iter()
            .max_by_key(|node| node.updated_unix_ms);
        let node_id = latest_stale_node.map(|node| node.node_id.clone());
        let detail = latest_stale_node
            .map(|node| {
                format!(
                    "latest stale node {} in {} last updated {} ms ago",
                    node.node_id, node.region, node.age_ms
                )
            })
            .unwrap_or_else(|| "stale telemetry node details unavailable".into());
        alerts.push(MeshAlert {
            level: "warn",
            code: "telemetry_snapshot_stale",
            message: format!(
                "{} mesh telemetry snapshot(s) have aged out; {detail}.",
                telemetry.stale_remote_count
            ),
            count: telemetry.stale_remote_count as u64,
            last_seen_unix_ms: latest_stale_node.map(|node| node.updated_unix_ms),
            node_id,
            stream_id_text: None,
        });
    }

    let blocked_provision_backends = provision
        .backend_statuses
        .iter()
        .filter(|backend| backend.state != "ready")
        .count();
    if blocked_provision_backends > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "provision_backend_blocked",
            message: "One or more configured provision backends are not ready.".into(),
            count: blocked_provision_backends as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    if let Some(age_ms) = local_stream.last_ingest_age_ms {
        let stale_threshold_ms =
            stream_stale_threshold_ms(local_stream.part_target_ms, local_stream.window_parts);
        if local_stream.latest_local_part.is_some() && age_ms > stale_threshold_ms {
            alerts.push(MeshAlert {
                level: "warn",
                code: "local_ingest_stale",
                message: format!(
                    "Local stream {} has not ingested bytes for {} ms.",
                    local_stream.stream_id_text, age_ms
                ),
                count: 1,
                last_seen_unix_ms: now.checked_sub(age_ms),
                node_id: None,
                stream_id_text: Some(local_stream.stream_id_text.clone()),
            });
        }
    }

    let stale_streams = streams
        .iter()
        .filter(|stream| stream.node_id != local_node_id && stream.stale())
        .collect::<Vec<_>>();
    if let Some((stream, last_seen, age_ms)) = stale_streams
        .iter()
        .filter_map(|stream| {
            stream
                .last_ingest_age_ms
                .map(|age_ms| (*stream, now.saturating_sub(age_ms), age_ms))
        })
        .max_by_key(|(_, last_seen, _)| *last_seen)
    {
        alerts.push(MeshAlert {
            level: "warn",
            code: "mesh_stream_stale",
            message: format!(
                "{} mesh stream(s) are stale; latest stale stream {} on {} has not ingested bytes for {} ms.",
                stale_streams.len(),
                stream.stream_id_text,
                stream.node_id,
                age_ms
            ),
            count: stale_streams.len() as u64,
            last_seen_unix_ms: Some(last_seen),
            node_id: Some(stream.node_id.clone()),
            stream_id_text: Some(stream.stream_id_text.clone()),
        });
    }

    alerts
}

fn derive_mesh_activity(
    aggregate: &AggregateMetrics,
    alerts: &[MeshAlert],
    recent_commands: &[ControlCommand],
) -> Vec<MeshActivity> {
    let now = now_unix_ms();
    let mut activity = Vec::with_capacity(1 + alerts.len() + recent_commands.len());

    activity.push(MeshActivity {
        level: "info",
        code: "mesh_snapshot".into(),
        message: format!(
            "Mesh sees {} node(s), {} link(s), and {} active stream(s).",
            aggregate.node_count, aggregate.connection_count, aggregate.active_streams
        ),
        count: aggregate.node_count as u64,
        seen_unix_ms: now,
        node_id: None,
        stream_id_text: None,
    });

    activity.extend(alerts.iter().map(|alert| MeshActivity {
        level: alert.level,
        code: alert.code.into(),
        message: alert.message.clone(),
        count: alert.count,
        seen_unix_ms: alert.last_seen_unix_ms.unwrap_or(now),
        node_id: alert.node_id.clone(),
        stream_id_text: alert.stream_id_text.clone(),
    }));

    activity.extend(recent_commands.iter().map(|command| {
        let status = command.status.to_ascii_lowercase();
        let level = if status.contains("failed")
            || status.contains("timed out")
            || status.contains("error")
        {
            "error"
        } else if status.contains("skipped") {
            "warn"
        } else {
            "info"
        };
        MeshActivity {
            level,
            code: control_kind_code(command.kind).into(),
            message: format!(
                "{} command {}.",
                control_kind_label(command.kind),
                command.status
            ),
            count: 1,
            seen_unix_ms: command.created_unix_ms,
            node_id: command.node_id.clone(),
            stream_id_text: command.stream_id_text.clone(),
        }
    }));

    activity.sort_by(|left, right| {
        right
            .seen_unix_ms
            .cmp(&left.seen_unix_ms)
            .then_with(|| left.code.cmp(&right.code))
    });
    activity.truncate(MESH_ACTIVITY_LIMIT);
    activity
}

fn control_kind_code(kind: ControlKind) -> &'static str {
    match kind {
        ControlKind::ProvisionNode => "provision_node",
        ControlKind::CloseNode => "close_node",
        ControlKind::WarmStream => "warm_stream",
        ControlKind::ReplicaRequest => "replica_request",
    }
}

fn control_kind_label(kind: ControlKind) -> &'static str {
    match kind {
        ControlKind::ProvisionNode => "Provision node",
        ControlKind::CloseNode => "Close node",
        ControlKind::WarmStream => "Warm stream",
        ControlKind::ReplicaRequest => "Replica request",
    }
}

fn storage_percent(node: &MeshNode) -> u64 {
    if node.total_storage_bytes == 0 {
        return 0;
    }
    node.used_storage_bytes.saturating_mul(100) / node.total_storage_bytes
}

fn snapshot_stream_for_id(snapshot: &MeshSnapshot, stream_id: u64) -> Option<StreamTelemetry> {
    if snapshot.streams.is_empty() {
        if snapshot.stream.stream_id == stream_id {
            return Some(StreamTelemetry::from_stats(
                snapshot.node.node_id.clone(),
                &snapshot.stream,
            ));
        }
        return None;
    }

    snapshot
        .streams
        .iter()
        .find(|stream| stream.stream_id == stream_id)
        .cloned()
}

fn snapshot_stream_ids(snapshot: &MeshSnapshot) -> Vec<u64> {
    if snapshot.streams.is_empty() {
        if snapshot.stream.latest_local_part.is_some() || snapshot.stream.latest_mesh_part.is_some()
        {
            return vec![snapshot.stream.stream_id];
        }
        return Vec::new();
    }

    snapshot
        .streams
        .iter()
        .filter(|stream| stream.active())
        .map(|stream| stream.stream_id)
        .collect()
}

#[derive(Debug, Clone, Default)]
struct ControlPlane {
    commands: Arc<RwLock<Vec<ControlCommand>>>,
}

impl ControlPlane {
    async fn record(&self, kind: ControlKind, request: ControlRequest) -> ControlCommand {
        let target_text = control_request_target_text(&request);
        let command = ControlCommand {
            id: now_unix_ms(),
            kind,
            node_id: request.node_id,
            region: request.region,
            stream_id: request.stream_id,
            stream_id_text: request.stream_id.map(stream_id_text),
            target_text,
            created_unix_ms: now_unix_ms(),
            status: "accepted".into(),
        };
        let mut commands = self.commands.write().await;
        commands.push(command.clone());
        if commands.len() > 128 {
            let keep_from = commands.len() - 128;
            commands.drain(0..keep_from);
        }
        command
    }

    async fn recent(&self) -> Vec<ControlCommand> {
        self.commands
            .read()
            .await
            .iter()
            .rev()
            .take(16)
            .cloned()
            .collect()
    }

    async fn replace(&self, command: ControlCommand) {
        let mut commands = self.commands.write().await;
        if let Some(existing) = commands
            .iter_mut()
            .find(|existing| existing.id == command.id)
        {
            *existing = command;
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ControlDispatch {
    tx: Arc<RwLock<Option<mpsc::Sender<TcpChangesMessage>>>>,
}

impl ControlDispatch {
    async fn set_sender(&self, tx: mpsc::Sender<TcpChangesMessage>) {
        *self.tx.write().await = Some(tx);
    }

    async fn ready(&self) -> bool {
        self.tx.read().await.is_some()
    }

    async fn publish(&self, envelope: &ControlEnvelope) -> Result<bool> {
        let Some(tx) = self.tx.read().await.clone() else {
            return Ok(false);
        };
        let json = serde_json::to_vec(envelope).context("failed to encode control envelope")?;
        tx.send(TcpChangesMessage::new(CONTROL_TAG, vec![Bytes::from(json)]))
            .await
            .map_err(|_| anyhow!("tcp changes control feed is closed"))?;
        Ok(true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlRequest {
    node_id: Option<String>,
    region: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_u64_from_any")]
    stream_id: Option<u64>,
}

fn control_request_target_text(request: &ControlRequest) -> String {
    let mut parts = Vec::new();
    if let Some(node_id) = request.node_id.as_deref().filter(|value| !value.is_empty()) {
        parts.push(format!("node {node_id}"));
    }
    if let Some(region) = request.region.as_deref().filter(|value| !value.is_empty()) {
        parts.push(format!("region {region}"));
    }
    if let Some(stream_id) = request.stream_id {
        parts.push(format!("stream {}", stream_id_text(stream_id)));
    }
    if parts.is_empty() {
        "global".to_owned()
    } else {
        parts.join(" / ")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ControlKind {
    ProvisionNode,
    CloseNode,
    WarmStream,
    ReplicaRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlCommand {
    id: u64,
    kind: ControlKind,
    node_id: Option<String>,
    region: Option<String>,
    stream_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stream_id_text: Option<String>,
    target_text: String,
    created_unix_ms: u64,
    status: String,
}

#[derive(Debug, Clone)]
struct ProvisionExecutor {
    command: Option<String>,
    timeout: Duration,
    #[cfg(feature = "linode-provisioner")]
    linode: Option<LinodeProvisionConfig>,
}

impl ProvisionExecutor {
    fn new(command: Option<String>, timeout: Duration) -> Self {
        Self {
            command,
            timeout,
            #[cfg(feature = "linode-provisioner")]
            linode: None,
        }
    }

    #[cfg(feature = "linode-provisioner")]
    fn with_linode(mut self, linode: Option<LinodeProvisionConfig>) -> Self {
        self.linode = linode;
        self
    }

    #[cfg(test)]
    fn disabled() -> Self {
        Self {
            command: None,
            timeout: Duration::from_secs(30),
            #[cfg(feature = "linode-provisioner")]
            linode: None,
        }
    }

    fn status(&self) -> ProvisionStatus {
        let mut backends = Vec::new();
        let mut backend_statuses = Vec::new();
        if self.command.is_some() {
            backends.push("command".to_owned());
            backend_statuses.push(ProvisionBackendStatus {
                name: "command".to_owned(),
                state: "ready",
                details: vec!["shell command configured".to_owned()],
            });
        }
        #[cfg(feature = "linode-provisioner")]
        if let Some(config) = &self.linode {
            backends.push("linode".to_owned());
            backend_statuses.push(config.status());
        }
        ProvisionStatus {
            enabled: !backends.is_empty(),
            backends,
            timeout_ms: self.timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            backend_statuses,
        }
    }

    async fn run(
        &self,
        control_id: u64,
        local_node: &MeshNode,
        request: &ControlRequest,
    ) -> String {
        let mut statuses = Vec::new();
        #[cfg(feature = "linode-provisioner")]
        let mut linode_result = None;

        #[cfg(feature = "linode-provisioner")]
        if let Some(config) = &self.linode {
            let (status, result) = self.run_linode(config, local_node, request).await;
            let failed = status.contains(" failed:") || status.contains(" skipped:");
            statuses.push(status);
            linode_result = result;
            if failed {
                return statuses.join("; ");
            }
        }

        if let Some(script) = &self.command {
            statuses.push(
                self.run_command(
                    script,
                    control_id,
                    local_node,
                    request,
                    #[cfg(feature = "linode-provisioner")]
                    linode_result.as_ref(),
                )
                .await,
            );
        }

        if statuses.is_empty() {
            return "local provision skipped: no provision backend configured".into();
        }

        statuses.join("; ")
    }

    async fn run_command(
        &self,
        script: &str,
        control_id: u64,
        local_node: &MeshNode,
        request: &ControlRequest,
        #[cfg(feature = "linode-provisioner")] linode_result: Option<&linode::ScaleUpResult>,
    ) -> String {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(script)
            .env("AV_MESH_CONTROL_ID", control_id.to_string())
            .env("AV_MESH_LOCAL_NODE_ID", &local_node.node_id)
            .env("AV_MESH_LOCAL_REGION", &local_node.region)
            .env(
                "AV_MESH_PROVISION_NODE_ID",
                request.node_id.as_deref().unwrap_or(""),
            )
            .env(
                "AV_MESH_PROVISION_REGION",
                request.region.as_deref().unwrap_or(""),
            )
            .env(
                "AV_MESH_PROVISION_STREAM_ID",
                request
                    .stream_id
                    .map(|stream_id| stream_id.to_string())
                    .unwrap_or_default(),
            );

        #[cfg(feature = "linode-provisioner")]
        if let Some(result) = linode_result {
            command
                .env("AV_MESH_LINODE_INSTANCE_ID", result.instance_id.to_string())
                .env("AV_MESH_LINODE_LABEL", &result.label)
                .env("AV_MESH_LINODE_PUBLIC_IPV4", &result.public_ipv4)
                .env("AV_MESH_LINODE_PRIVATE_IPAM", &result.private_ipam_address)
                .env("AV_MESH_LINODE_VLAN_LABEL", &result.vlan_label)
                .env(
                    "AV_MESH_LINODE_DNS_NAME",
                    result.dns_name.as_deref().unwrap_or(""),
                )
                .env("AV_MESH_LINODE_REGION_CODE", &result.region_code)
                .env("AV_MESH_LINODE_REGION", &result.linode_region);
        }

        match tokio::time::timeout(self.timeout, command.output()).await {
            Err(_) => format!(
                "local provision failed: command timed out after {} ms",
                self.timeout.as_millis()
            ),
            Ok(Err(error)) => format!("local provision failed: command spawn failed: {error}"),
            Ok(Ok(output)) if output.status.success() => {
                let detail = command_output_detail(&output.stdout, &output.stderr);
                if detail.is_empty() {
                    format!("local provision executed: {}", output.status)
                } else {
                    format!("local provision executed: {}; {detail}", output.status)
                }
            }
            Ok(Ok(output)) => {
                let detail = command_output_detail(&output.stdout, &output.stderr);
                if detail.is_empty() {
                    format!("local provision failed: {}", output.status)
                } else {
                    format!("local provision failed: {}; {detail}", output.status)
                }
            }
        }
    }

    #[cfg(feature = "linode-provisioner")]
    async fn run_linode(
        &self,
        config: &LinodeProvisionConfig,
        local_node: &MeshNode,
        request: &ControlRequest,
    ) -> (String, Option<linode::ScaleUpResult>) {
        let requested_region = request.region.as_deref().unwrap_or(&local_node.region);
        let linode_region_code = config.resolve_region(requested_region);
        let Some(region_info) = LINODE_REGIONS.get(linode_region_code.as_str()) else {
            return (
                format!(
                    "local linode provision skipped: region {requested_region} resolved to unsupported Linode region {linode_region_code}"
                ),
                None,
            );
        };
        let token = match std::env::var(&config.token_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    format!(
                        "local linode provision skipped: missing {}",
                        config.token_env
                    ),
                    None,
                )
            }
        };
        let pub_key = match std::env::var(&config.pub_key_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    format!(
                        "local linode provision skipped: missing {}",
                        config.pub_key_env
                    ),
                    None,
                )
            }
        };
        let client = match LinodeClient::new(token, pub_key) {
            Ok(client) => client,
            Err(error) => {
                return (
                    format!("local linode provision failed: invalid client config: {error}"),
                    None,
                )
            }
        };

        match tokio::time::timeout(
            self.timeout,
            client.scale_up_one(
                &config.image_id,
                &config.instance_type,
                config.domain_id,
                region_info,
                &config.vlan_tag,
            ),
        )
        .await
        {
            Err(_) => (
                format!(
                    "local linode provision failed: timed out after {} ms",
                    self.timeout.as_millis()
                ),
                None,
            ),
            Ok(Err(error)) => (format!("local linode provision failed: {error}"), None),
            Ok(Ok(result)) => (
                format_linode_provision_result(requested_region, &result),
                Some(result),
            ),
        }
    }
}

#[cfg(feature = "linode-provisioner")]
#[derive(Debug, Clone)]
struct LinodeProvisionConfig {
    token_env: String,
    pub_key_env: String,
    image_id: String,
    instance_type: String,
    domain_id: u64,
    vlan_tag: String,
    region_map: BTreeMap<String, String>,
}

#[cfg(feature = "linode-provisioner")]
impl LinodeProvisionConfig {
    fn resolve_region(&self, mesh_region: &str) -> String {
        self.region_map
            .get(mesh_region)
            .cloned()
            .unwrap_or_else(|| mesh_region.to_string())
    }

    fn status(&self) -> ProvisionBackendStatus {
        let token_present = env_value_present(&self.token_env);
        let pub_key_present = env_value_present(&self.pub_key_env);
        let mut details = vec![
            format!(
                "token env {} {}",
                self.token_env,
                if token_present { "present" } else { "missing" }
            ),
            format!(
                "public key env {} {}",
                self.pub_key_env,
                if pub_key_present {
                    "present"
                } else {
                    "missing"
                }
            ),
            format!("image {}", self.image_id),
            format!("type {}", self.instance_type),
            format!("domain {}", self.domain_id),
            format!("private vlan {}", self.vlan_tag),
        ];
        if self.region_map.is_empty() {
            details.push("region map empty".to_owned());
        } else {
            details.push(format!("{} region map entries", self.region_map.len()));
        }

        ProvisionBackendStatus {
            name: "linode".to_owned(),
            state: if token_present && pub_key_present {
                "ready"
            } else {
                "blocked"
            },
            details,
        }
    }
}

#[cfg(feature = "linode-provisioner")]
fn format_linode_provision_result(
    requested_region: &str,
    result: &linode::ScaleUpResult,
) -> String {
    let dns = result.dns_name.as_deref().unwrap_or("none");
    format!(
        "local linode provisioned: requested_region={requested_region} linode_region={} instance_id={} label={} public_ipv4={} private_ipam={} vlan={} dns={dns}",
        result.linode_region,
        result.instance_id,
        result.label,
        result.public_ipv4,
        result.private_ipam_address,
        result.vlan_label
    )
}

fn command_output_detail(stdout: &[u8], stderr: &[u8]) -> String {
    let stdout = truncated_utf8(stdout);
    let stderr = truncated_utf8(stderr);
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => format!("stdout={stdout}"),
        (true, false) => format!("stderr={stderr}"),
        (false, false) => format!("stdout={stdout}; stderr={stderr}"),
    }
}

#[cfg(feature = "linode-provisioner")]
fn env_value_present(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.trim().is_empty())
}

fn truncated_utf8(bytes: &[u8]) -> String {
    const LIMIT: usize = 160;
    let mut text = String::from_utf8_lossy(&bytes[..bytes.len().min(LIMIT)])
        .trim()
        .replace('\n', "\\n");
    if bytes.len() > LIMIT {
        text.push_str("...");
    }
    text
}

struct TelemetryRuntime {
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<()>,
    finished_rx: tokio::sync::oneshot::Receiver<()>,
    publisher_task: tokio::task::JoinHandle<()>,
}

#[allow(clippy::too_many_arguments)]
async fn start_telemetry_feed(
    bind: SocketAddr,
    private_ipv4: Ipv4Addr,
    cert: String,
    key: String,
    interval_ms: u64,
    cache: Arc<LiveTsCache>,
    mesh: Arc<CacheMeshHandle>,
    node: MeshNode,
    policy: ReplicationPolicy,
    control: ControlPlane,
    lifecycle: NodeLifecycle,
    dispatch: ControlDispatch,
    playback_base_url: Option<String>,
    edge_load: EdgeLoad,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<TelemetryRuntime> {
    let server = tcp_changes::Server::new(cert, key, private_ipv4);
    let (up_rx, finished_rx, shutdown_tx, tx) = server
        .start(bind)
        .await
        .map_err(|err| anyhow!("failed to start tcp changes telemetry feed: {err}"))?;
    up_rx
        .await
        .map_err(|_| anyhow!("tcp changes telemetry feed failed before ready"))?;
    dispatch.set_sender(tx.clone()).await;

    let publisher_task = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(interval_ms));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    return;
                }
                _ = ticker.tick() => {
                    let mut node = node.clone();
                    node.draining = lifecycle.is_draining().await;
                    let snapshot = cache
                        .mesh_snapshot(&mesh, node, policy.clone(), &control)
                        .await;
                    let edge_service =
                        edge_load.snapshot(&snapshot.node, playback_base_url.clone());
                    let snapshot = snapshot.with_edge_service(edge_service);
                    match serde_json::to_vec(&snapshot) {
                        Ok(json) => {
                            debug!(
                                node_id = %snapshot.node.node_id,
                                active_streams = snapshot.node.active_streams,
                                stream_telemetry = snapshot.streams.len(),
                                peers = snapshot.peers.len(),
                                bytes = json.len(),
                                "publishing mesh telemetry snapshot"
                            );
                            let message = TcpChangesMessage::new(TELEMETRY_TAG, vec![Bytes::from(json)]);
                            if tx.send(message).await.is_err() {
                                return;
                            }
                        }
                        Err(error) => warn!(error = %error, "failed to encode mesh telemetry snapshot"),
                    }
                }
            }
        }
    });

    Ok(TelemetryRuntime {
        local_addr: bind,
        shutdown_tx,
        finished_rx,
        publisher_task,
    })
}

fn start_telemetry_collectors(
    peers: Vec<SocketAddr>,
    dns_name: String,
    ca_cert: String,
    router: AppRouter,
    telemetry_peers: TelemetryPeerMonitor,
    shutdown_rx: watch::Receiver<()>,
) -> Vec<tokio::task::JoinHandle<()>> {
    peers
        .into_iter()
        .map(|peer| {
            let dns_name = dns_name.clone();
            let ca_cert = ca_cert.clone();
            let router = router.clone();
            let telemetry_peers = telemetry_peers.clone();
            let mut shutdown_rx = shutdown_rx.clone();
            tokio::spawn(async move {
                loop {
                    let connect_shutdown_rx = shutdown_rx.clone();
                    tokio::select! {
                        _ = shutdown_rx.changed() => return,
                        result = connect_telemetry_peer(peer, &dns_name, &ca_cert, router.clone(), telemetry_peers.clone(), connect_shutdown_rx) => {
                            match result {
                                Ok(()) => {
                                    telemetry_peers.record_disconnected(peer, None).await;
                                    info!(peer = %peer, "telemetry peer collector disconnected");
                                }
                                Err(error) => {
                                    let error_text = error.to_string();
                                    telemetry_peers.record_disconnected(peer, Some(error_text.clone())).await;
                                    warn!(peer = %peer, error = %error_text, "telemetry peer collector disconnected");
                                }
                            }
                            tokio::select! {
                                _ = shutdown_rx.changed() => return,
                                _ = sleep(Duration::from_secs(1)) => {}
                            }
                        }
                    }
                }
            })
        })
        .collect()
}

async fn connect_telemetry_peer(
    peer: SocketAddr,
    dns_name: &str,
    ca_cert: &str,
    router: AppRouter,
    telemetry_peers: TelemetryPeerMonitor,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    telemetry_peers.record_connecting(peer).await;
    let client = TcpChangesClient::new(dns_name.to_string(), peer, ca_cert.to_string());
    let (up_rx, _fin_rx, client_shutdown, mut rx) = client
        .start("HELLO")
        .await
        .map_err(|err| anyhow!("failed to connect tcp changes telemetry peer {peer}: {err}"))?;
    up_rx
        .await
        .map_err(|_| anyhow!("tcp changes telemetry peer {peer} ended before ready"))?;
    telemetry_peers.record_connected(peer).await;
    info!(peer = %peer, "telemetry peer collector connected");

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                let _ = client_shutdown.send(());
                return Ok(());
            }
            payload = rx.recv() => {
                let Some(payload) = payload else {
                    return Ok(());
                };
                debug!(
                    peer = %peer,
                    tag = ?payload.tag,
                    bytes = payload.val.len(),
                    "received tcp-changes telemetry payload"
                );
                telemetry_peers.record_payload(peer, payload.val.len()).await;
                if let Err(error) = router.ingest_tcp_changes_payload(payload).await {
                    warn!(peer = %peer, error = %error, "failed to ingest tcp changes payload");
                }
            }
        }
    }
}

#[derive(Clone)]
struct AppRouter {
    cache: Arc<LiveTsCache>,
    mesh: Arc<CacheMeshHandle>,
    node: MeshNode,
    replication_policy: ReplicationPolicy,
    control: ControlPlane,
    dispatch: ControlDispatch,
    telemetry: TelemetryAggregator,
    demand: DemandTracker,
    lifecycle: NodeLifecycle,
    playback_base_url: Option<String>,
    edge_load: EdgeLoad,
    provision: ProvisionExecutor,
    telemetry_peers: TelemetryPeerMonitor,
}

impl AppRouter {
    fn new(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        node: MeshNode,
        replication_policy: ReplicationPolicy,
        control: ControlPlane,
        dispatch: ControlDispatch,
        telemetry: TelemetryAggregator,
        demand: DemandTracker,
        lifecycle: NodeLifecycle,
        playback_base_url: Option<String>,
        edge_load: EdgeLoad,
        provision: ProvisionExecutor,
        telemetry_peers: TelemetryPeerMonitor,
    ) -> Self {
        Self {
            cache,
            mesh,
            node,
            replication_policy,
            control,
            dispatch,
            telemetry,
            demand,
            lifecycle,
            playback_base_url,
            edge_load,
            provision,
            telemetry_peers,
        }
    }

    async fn local_mesh_snapshot(&self) -> MeshSnapshot {
        let mut node = self.node.clone();
        node.draining = self.lifecycle.is_draining().await;
        let snapshot = self
            .cache
            .mesh_snapshot(
                &self.mesh,
                node.clone(),
                self.replication_policy.clone(),
                &self.control,
            )
            .await;
        snapshot.with_edge_service(
            self.edge_load
                .snapshot(&node, self.playback_base_url.clone()),
        )
    }

    async fn mesh_api_snapshot(&self) -> MeshApiSnapshot {
        let local = self.local_mesh_snapshot().await;
        let (snapshots, telemetry) = self.telemetry.snapshots_with_local(local.clone()).await;
        let planned_replicas = self
            .plan_all_active_replicas_from_snapshots(&snapshots)
            .await;
        MeshApiSnapshot::from_snapshots(
            local,
            snapshots,
            telemetry,
            planned_replicas,
            self.orchestration_status().await,
        )
    }

    fn record_edge_response(
        &self,
        method: &Method,
        path: &str,
        query: Option<&str>,
        response: HandlerResponse,
    ) -> HandlerResponse {
        if path == "/live/stream.m3u8" || path.starts_with("/live/") {
            self.edge_load
                .record_response(method, path, query, &response);
        }
        response
    }

    async fn orchestration_status(&self) -> OrchestrationStatus {
        OrchestrationStatus {
            control_dispatch_ready: self.dispatch.ready().await,
            provision: self.provision.status(),
            telemetry_peers: self.telemetry_peers.snapshot().await,
        }
    }

    async fn mesh_protocol_response_from_bytes(&self, bytes: &[u8]) -> MeshProtocolResponse {
        match self.parse_mesh_protocol_request(bytes) {
            Ok(request) => self.handle_mesh_protocol_request(request).await,
            Err(error) => MeshProtocolResponse::error(error),
        }
    }

    fn parse_mesh_protocol_request(
        &self,
        bytes: &[u8],
    ) -> std::result::Result<MeshProtocolRequest, String> {
        let text = std::str::from_utf8(bytes).map_err(|err| err.to_string())?;
        if text.trim().eq_ignore_ascii_case("snapshot") {
            return Ok(MeshProtocolRequest::Snapshot);
        }
        serde_json::from_str(text).map_err(|err| err.to_string())
    }

    async fn handle_mesh_protocol_request(
        &self,
        request: MeshProtocolRequest,
    ) -> MeshProtocolResponse {
        match request {
            MeshProtocolRequest::Snapshot => {
                MeshProtocolResponse::snapshot(self.mesh_api_snapshot().await)
            }
            MeshProtocolRequest::ProvisionNode { node_id, region } => {
                let command = self
                    .execute_control(
                        ControlKind::ProvisionNode,
                        ControlRequest {
                            node_id,
                            region,
                            stream_id: None,
                        },
                    )
                    .await;
                MeshProtocolResponse::command(command)
            }
            MeshProtocolRequest::CloseNode { node_id, region } => {
                let command = self
                    .execute_control(
                        ControlKind::CloseNode,
                        ControlRequest {
                            node_id,
                            region,
                            stream_id: None,
                        },
                    )
                    .await;
                MeshProtocolResponse::command(command)
            }
            MeshProtocolRequest::WarmStream { stream_id, region } => {
                let command = self
                    .execute_control(
                        ControlKind::WarmStream,
                        ControlRequest {
                            node_id: None,
                            region,
                            stream_id,
                        },
                    )
                    .await;
                MeshProtocolResponse::command(command)
            }
        }
    }

    async fn mesh_protocol_response_json(&self, bytes: &[u8]) -> HandlerResult<Bytes> {
        let response = self.mesh_protocol_response_from_bytes(bytes).await;
        serde_json::to_vec(&response)
            .map(Bytes::from)
            .map_err(|err| ServerError::Handler(Box::new(err)))
    }

    async fn binary_mesh_response_from_bytes(&self, bytes: Bytes) -> HandlerResult<Bytes> {
        if bytes.is_empty() {
            return serde_json::to_vec(&MeshProtocolResponse::snapshot(
                self.mesh_api_snapshot().await,
            ))
            .map(Bytes::from)
            .map_err(|err| ServerError::Handler(Box::new(err)));
        }

        if let Some(unit) = self
            .ingest_serialized_media_access_unit(bytes.clone())
            .await
            .map_err(ServerError::Config)?
        {
            return serde_json::to_vec(&MeshProtocolResponse::media_access_unit(
                MediaAccessUnitResponse::from_cached(&unit),
            ))
            .map(Bytes::from)
            .map_err(|err| ServerError::Handler(Box::new(err)));
        }

        self.mesh_protocol_response_json(&bytes).await
    }

    async fn webtransport_response_from_bytes(&self, bytes: Bytes) -> HandlerResult<Bytes> {
        self.binary_mesh_response_from_bytes(bytes).await
    }

    async fn ingest_serialized_media_access_unit(
        &self,
        bytes: Bytes,
    ) -> std::result::Result<Option<CachedMediaAccessUnit>, String> {
        let Some(unit) = decode_serialized_media_access_unit(bytes)? else {
            return Ok(None);
        };
        self.cache
            .add_media_access_unit(unit.metadata, unit.payload)
            .await
            .map(Some)
            .map_err(|err| err.to_string())
    }

    async fn ingest_tcp_changes_payload(&self, payload: TcpChangesPayload) -> Result<bool> {
        match payload.tag {
            TELEMETRY_TAG => self.telemetry.ingest_payload(payload).await,
            CONTROL_TAG => self.ingest_control_payload(payload.val).await,
            _ => Ok(false),
        }
    }

    async fn ingest_control_payload(&self, bytes: Bytes) -> Result<bool> {
        let envelope: ControlEnvelope =
            serde_json::from_slice(&bytes).context("failed to decode AVMC control payload")?;
        if envelope.origin_node_id == self.node.node_id {
            return Ok(false);
        }
        if !self.control_targets_local(&envelope.request, &envelope.target_node_ids) {
            return Ok(false);
        }

        let mut command = self
            .execute_control_internal_with_targets(
                envelope.kind,
                envelope.request,
                false,
                envelope.target_node_ids,
            )
            .await;
        command.status = format!(
            "received from {} command {}; {}",
            envelope.origin_node_id, envelope.id, command.status
        );
        self.control.replace(command).await;
        Ok(true)
    }

    async fn execute_control(&self, kind: ControlKind, request: ControlRequest) -> ControlCommand {
        self.execute_control_internal(kind, request, true).await
    }

    async fn execute_control_internal(
        &self,
        kind: ControlKind,
        request: ControlRequest,
        dispatch: bool,
    ) -> ControlCommand {
        let target_node_ids = if dispatch {
            self.selected_control_target_node_ids(kind, &request).await
        } else {
            Vec::new()
        };
        self.execute_control_internal_with_targets(kind, request, dispatch, target_node_ids)
            .await
    }

    async fn execute_control_internal_with_targets(
        &self,
        kind: ControlKind,
        request: ControlRequest,
        dispatch: bool,
        target_node_ids: Vec<String>,
    ) -> ControlCommand {
        let mut command = self.control.record(kind, request.clone()).await;
        let mut statuses = vec![
            self.apply_control_locally(kind, &request, dispatch, command.id, &target_node_ids)
                .await,
        ];

        if dispatch {
            let envelope = ControlEnvelope {
                id: command.id,
                origin_node_id: self.node.node_id.clone(),
                kind,
                request,
                target_node_ids: target_node_ids.clone(),
            };
            let status = match self.dispatch.publish(&envelope).await {
                Ok(true) if target_node_ids.is_empty() => "published AVMC control".to_string(),
                Ok(true) => format!("published AVMC control to {}", target_node_ids.join(",")),
                Ok(false) => "no tcp-changes control publisher".to_string(),
                Err(error) => format!("control publish failed: {error}"),
            };
            statuses.push(status);
        }

        command.status = statuses.join("; ");
        self.control.replace(command.clone()).await;
        command
    }

    async fn apply_control_locally(
        &self,
        kind: ControlKind,
        request: &ControlRequest,
        originated_locally: bool,
        control_id: u64,
        target_node_ids: &[String],
    ) -> String {
        if kind != ControlKind::ProvisionNode
            && !self.control_targets_local(request, target_node_ids)
        {
            return "local skipped: target does not match".into();
        }

        match kind {
            ControlKind::WarmStream => {
                let Some(stream_id) = request.stream_id else {
                    return "local warm skipped: missing stream id".into();
                };
                let _ = self
                    .cache
                    .chunk_cache
                    .get_or_create_stream_idx(stream_id)
                    .await;
                self.request_replica_for_stream(
                    stream_id,
                    "warm-stream",
                    request.region.as_deref(),
                )
                .await;
                "local warm requested".into()
            }
            ControlKind::CloseNode => {
                self.lifecycle.set_draining(true).await;
                "local close requested: draining".into()
            }
            ControlKind::ProvisionNode => {
                if !originated_locally {
                    return "remote provision noted: executor only runs at command origin".into();
                }
                self.provision.run(control_id, &self.node, request).await
            }
            ControlKind::ReplicaRequest => "local replica request command ignored".into(),
        }
    }

    fn control_targets_local(&self, request: &ControlRequest, target_node_ids: &[String]) -> bool {
        if !target_node_ids.is_empty() {
            return target_node_ids
                .iter()
                .any(|node_id| node_id == &self.node.node_id);
        }
        if let Some(node_id) = &request.node_id {
            return node_id == &self.node.node_id;
        }
        if let Some(region) = &request.region {
            return region == &self.node.region;
        }
        true
    }

    async fn selected_control_target_node_ids(
        &self,
        kind: ControlKind,
        request: &ControlRequest,
    ) -> Vec<String> {
        if kind == ControlKind::ProvisionNode {
            return Vec::new();
        }
        if let Some(node_id) = &request.node_id {
            return vec![node_id.clone()];
        }
        let Some(region) = &request.region else {
            return Vec::new();
        };

        let local = self.local_mesh_snapshot().await;
        let (snapshots, _) = self.telemetry.snapshots_with_local(local).await;
        let mut node_ids = snapshots
            .into_iter()
            .filter(|snapshot| snapshot.node.region == *region)
            .map(|snapshot| snapshot.node.node_id)
            .collect::<Vec<_>>();
        node_ids.sort();
        node_ids.dedup();
        node_ids
    }

    async fn mesh_sse_event(&self) -> HandlerResult<Bytes> {
        let snapshot = self.mesh_api_snapshot().await;
        let json =
            serde_json::to_vec(&snapshot).map_err(|err| ServerError::Handler(Box::new(err)))?;
        let mut event = BytesMut::new();
        event.put_slice(b"event: mesh\n");
        event.put_slice(b"data: ");
        event.put_slice(&json);
        event.put_slice(b"\n\n");
        Ok(event.freeze())
    }

    fn plan_replicas_from_snapshots(
        &self,
        stream_id: u64,
        local_bytes: u64,
        snapshots: &[MeshSnapshot],
        demand: &[DemandSignal],
    ) -> Vec<ReplicaPlacement> {
        let nodes = snapshots
            .iter()
            .map(|snapshot| snapshot.node.clone())
            .collect::<Vec<_>>();
        let mut existing_replicas = snapshots
            .iter()
            .filter(|snapshot| {
                snapshot_stream_for_id(snapshot, stream_id)
                    .map(|stream| stream.active())
                    .unwrap_or(false)
            })
            .map(|snapshot| snapshot.node.node_id.clone())
            .collect::<HashSet<_>>();

        if local_bytes > 0 {
            existing_replicas.insert(self.node.node_id.clone());
        }

        let telemetry_bytes = snapshots
            .iter()
            .filter_map(|snapshot| snapshot_stream_for_id(snapshot, stream_id))
            .filter_map(|stream| stream.latest_local_part_bytes)
            .map(|bytes| bytes as u64)
            .max()
            .unwrap_or(0)
            .saturating_mul(self.cache.window_parts as u64);
        let stream = StreamInfo {
            stream_id,
            bytes: local_bytes.max(telemetry_bytes).max(1),
            contributor_node_id: snapshots
                .iter()
                .find(|snapshot| {
                    snapshot_stream_for_id(snapshot, stream_id)
                        .map(|stream| stream.latest_local_part.is_some())
                        .unwrap_or(false)
                })
                .map(|snapshot| snapshot.node.node_id.clone()),
            active: !existing_replicas.is_empty(),
        };
        if !stream.active && demand.is_empty() {
            return Vec::new();
        }

        self.replication_policy
            .plan_replicas(&stream, &nodes, &existing_replicas, demand)
    }

    async fn plan_all_active_replicas_from_snapshots(
        &self,
        snapshots: &[MeshSnapshot],
    ) -> Vec<ReplicaPlacement> {
        let mut stream_ids = self.cache.active_stream_ids().await;
        for snapshot in snapshots {
            stream_ids.extend(snapshot_stream_ids(snapshot));
        }
        stream_ids.sort_unstable();
        stream_ids.dedup();

        let mut planned = Vec::new();
        for stream_id in stream_ids {
            let local_bytes = self
                .cache
                .estimated_storage_bytes_for_stream(stream_id)
                .await;
            planned.extend(self.plan_replicas_from_snapshots(
                stream_id,
                local_bytes,
                snapshots,
                &[],
            ));
        }
        planned
    }

    async fn request_replica_for_stream(
        &self,
        stream_id: u64,
        reason: &'static str,
        demand_region: Option<&str>,
    ) {
        let now_ms = now_unix_ms();
        if !self.demand.should_request_replica(stream_id, now_ms).await {
            return;
        }

        let local = self.local_mesh_snapshot().await;
        let (snapshots, _) = self.telemetry.snapshots_with_local(local).await;
        let region = demand_region.unwrap_or(&self.node.region);
        let continent = snapshots
            .iter()
            .find(|snapshot| snapshot.node.region == region)
            .map(|snapshot| snapshot.node.continent.as_str())
            .unwrap_or(&self.node.continent);
        let demand = [DemandSignal {
            stream_id,
            requester_node_id: self.node.node_id.clone(),
            region: region.to_string(),
            continent: continent.to_string(),
            active_readers: self.replication_policy.demand_active_readers.max(1),
            reads_per_sec: self.replication_policy.demand_reads_per_sec.max(1.0),
            observed_unix_ms: now_ms,
        }];
        let local_bytes = self
            .cache
            .estimated_storage_bytes_for_stream(stream_id)
            .await;
        let planned_replicas =
            self.plan_replicas_from_snapshots(stream_id, local_bytes, &snapshots, &demand);
        let from_slot = self.cache.replica_request_from_slot(stream_id).await;
        match self.mesh.request_replica(stream_id, from_slot).await {
            Ok(peer_count) => {
                let mut command = self
                    .control
                    .record(
                        ControlKind::ReplicaRequest,
                        ControlRequest {
                            node_id: Some(self.node.node_id.clone()),
                            region: Some(region.to_string()),
                            stream_id: Some(stream_id),
                        },
                    )
                    .await;
                command.status =
                    format_replica_request_status(reason, peer_count, &planned_replicas);
                self.control.replace(command).await;
            }
            Err(error) => {
                warn!(
                    stream_id,
                    from_slot,
                    error = %error,
                    "failed to request mesh replica"
                );
            }
        }
    }

    async fn known_active_stream_ids(&self) -> Vec<u64> {
        let local = self.local_mesh_snapshot().await;
        let (snapshots, _) = self.telemetry.snapshots_with_local(local).await;
        let mut stream_ids = self.cache.active_stream_ids().await;
        for snapshot in snapshots {
            stream_ids.extend(snapshot_stream_ids(&snapshot));
        }
        stream_ids.sort_unstable();
        stream_ids.dedup();
        stream_ids
    }

    async fn request_planned_local_replicas(&self, reason: &'static str) -> Vec<u64> {
        let mut requested = Vec::new();
        for stream_id in self.known_active_stream_ids().await {
            if self.request_planned_local_replica(stream_id, reason).await {
                requested.push(stream_id);
            }
        }
        requested
    }

    async fn request_planned_local_replica(&self, stream_id: u64, reason: &'static str) -> bool {
        let local = self.local_mesh_snapshot().await;
        let (snapshots, _) = self.telemetry.snapshots_with_local(local).await;
        let local_bytes = self
            .cache
            .estimated_storage_bytes_for_stream(stream_id)
            .await;
        let planned_replicas =
            self.plan_replicas_from_snapshots(stream_id, local_bytes, &snapshots, &[]);
        if !planned_replicas
            .iter()
            .any(|placement| placement.target_node_id == self.node.node_id)
        {
            return false;
        }

        let now_ms = now_unix_ms();
        if !self.demand.should_request_replica(stream_id, now_ms).await {
            return false;
        }

        let from_slot = self.cache.replica_request_from_slot(stream_id).await;
        match self.mesh.request_replica(stream_id, from_slot).await {
            Ok(peer_count) => {
                if peer_count > 0 {
                    let mut command = self
                        .control
                        .record(
                            ControlKind::ReplicaRequest,
                            ControlRequest {
                                node_id: Some(self.node.node_id.clone()),
                                region: Some(self.node.region.clone()),
                                stream_id: Some(stream_id),
                            },
                        )
                        .await;
                    command.status =
                        format_replica_request_status(reason, peer_count, &planned_replicas);
                    self.control.replace(command).await;
                }
                true
            }
            Err(error) => {
                warn!(
                    stream_id,
                    from_slot,
                    error = %error,
                    "failed to request planned mesh replica"
                );
                false
            }
        }
    }
}

async fn run_replication_planner(
    router: AppRouter,
    plan_interval: Duration,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut ticker = interval(plan_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = ticker.tick() => {
                let requested_streams = router
                    .request_planned_local_replicas("baseline-replication")
                    .await;
                for stream_id in requested_streams {
                    debug!(
                        node_id = router.node.node_id,
                        stream_id,
                        "baseline replication planner requested local replica"
                    );
                }
            }
        }
    }
}

#[async_trait]
impl Router for AppRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        let method = req.method().clone();
        let path_owned = req.uri().path().to_owned();
        let query_owned = req.uri().query().map(ToOwned::to_owned);
        let path = path_owned.as_str();
        let query = query_owned.as_deref();

        if req.method() == Method::OPTIONS {
            let response = response(StatusCode::NO_CONTENT, None, None);
            return Ok(self.record_edge_response(&method, path, query, response));
        }
        if req.method() != Method::GET && req.method() != Method::HEAD {
            let response = response(StatusCode::METHOD_NOT_ALLOWED, None, None);
            return Ok(self.record_edge_response(&method, path, query, response));
        }

        match path {
            "/" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(
                    b"av-mesh node\n\nMesh UI: /mesh\nHLS: /live/stream.m3u8\nHealth: /up\nStats: /api/stats\n",
                )),
                Some("text/plain; charset=utf-8"),
            )),
            "/mesh" => Ok(dashboard_dist_response(path).unwrap_or_else(|| {
                response(
                    StatusCode::OK,
                    Some(Bytes::from_static(MESH_DASHBOARD_HTML.as_bytes())),
                    Some("text/html; charset=utf-8"),
                )
                .with_no_store()
            })),
            WAVEY_GOOSE_ASSET_PATH => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(WAVEY_GOOSE_PNG)),
                Some("image/png"),
            )),
            "/up" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(b"OK")),
                Some("text/plain"),
            )),
            "/live/stream.m3u8" => {
                self.request_replica_for_stream(self.cache.stream_id, "playlist-demand", None)
                    .await;
                let playlist = self.cache.playlist().await;
                let response = response(
                    StatusCode::OK,
                    Some(Bytes::from(playlist)),
                    Some("application/vnd.apple.mpegurl"),
                )
                .with_no_store();
                Ok(self.record_edge_response(&method, path, query, response))
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
            "/api/mesh" => {
                let snapshot = self.mesh_api_snapshot().await;
                let json = serde_json::to_vec(&snapshot)
                    .map_err(|err| ServerError::Handler(Box::new(err)))?;
                Ok(response(
                    StatusCode::OK,
                    Some(Bytes::from(json)),
                    Some("application/json"),
                )
                .with_no_store())
            }
            _ => {
                if let Some(dashboard_asset) = dashboard_dist_response(path) {
                    return Ok(dashboard_asset);
                }

                if let Some(stream_id) = parse_stream_playlist_path(path) {
                    self.request_replica_for_stream(stream_id, "playlist-demand", None)
                        .await;
                    let playlist = self.cache.playlist_for_stream_id(stream_id).await;
                    let response = response(
                        StatusCode::OK,
                        Some(Bytes::from(playlist)),
                        Some("application/vnd.apple.mpegurl"),
                    )
                    .with_no_store();
                    return Ok(self.record_edge_response(&method, path, query, response));
                }

                if let Some(stream_id) = parse_llhls_tail_path(path) {
                    self.request_replica_for_stream(stream_id, "llhls-tail-demand", None)
                        .await;
                    let read = self.edge_load.begin_read(true);
                    let after = parse_query_u64(query, "after");
                    let Some((sequence, bytes, hash)) = self
                        .cache
                        .next_part_after_for_stream_id(stream_id, after)
                        .await
                    else {
                        read.finish(0);
                        let response = response(StatusCode::NO_CONTENT, None, None).with_no_store();
                        return Ok(self.record_edge_response(&method, path, query, response));
                    };
                    let bytes_len = bytes.len();
                    let media_kind = self
                        .cache
                        .media_kind_hint(stream_id)
                        .await
                        .unwrap_or(LiveMediaKind::Ts);
                    let mut tail_response =
                        response(StatusCode::OK, Some(bytes), Some(media_kind.content_type()))
                            .with_etag(hash)
                            .with_no_store();
                    tail_response
                        .headers
                        .push(("x-sequence".into(), sequence.to_string()));
                    tail_response
                        .headers
                        .push(("x-av-stream-id".into(), stream_id.to_string()));
                    read.finish(bytes_len);
                    return Ok(self.record_edge_response(&method, path, query, tail_response));
                }

                if let Some(stream_id) = parse_stream_init_path(path) {
                    self.request_replica_for_stream(stream_id, "playlist-init-demand", None)
                        .await;
                    if let Some(init) = self.cache.get_init_for_stream_id(stream_id).await {
                        let response = response(
                            StatusCode::OK,
                            Some(init),
                            Some(LiveMediaKind::Fmp4.content_type()),
                        )
                        .with_no_store();
                        return Ok(self.record_edge_response(&method, path, query, response));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
                    return Ok(self.record_edge_response(&method, path, query, response));
                }

                if let Some((stream_id, sequence)) = parse_media_unit_path(path) {
                    self.request_replica_for_stream(stream_id, "media-demand", None)
                        .await;
                    let Some(unit) = self.cache.get_media_access_unit(stream_id, sequence).await
                    else {
                        let response = response(StatusCode::NOT_FOUND, None, None);
                        return Ok(self.record_edge_response(&method, path, query, response));
                    };
                    let mut media_response = response(
                        StatusCode::OK,
                        Some(unit.serialized),
                        Some(MEDIA_ACCESS_UNIT_CONTENT_TYPE),
                    )
                    .with_etag(unit.hash)
                    .with_no_store();
                    media_response.headers.push((
                        "x-av-stream-id".into(),
                        unit.metadata.stream_id.to_string(),
                    ));
                    media_response
                        .headers
                        .push(("x-av-sequence".into(), unit.metadata.sequence.to_string()));
                    media_response
                        .headers
                        .push(("x-av-codec".into(), codec_name(unit.metadata.codec).into()));
                    media_response
                        .headers
                        .push(("x-av-pts-ms".into(), unit.metadata.pts_ms.to_string()));
                    media_response.headers.push((
                        "x-av-duration-ms".into(),
                        unit.metadata.duration_ms.to_string(),
                    ));
                    media_response
                        .headers
                        .push(("x-av-flags".into(), unit.metadata.flags.bits().to_string()));
                    return Ok(self.record_edge_response(&method, path, query, media_response));
                }

                if let Some((stream_id, seq, requested_kind)) = parse_stream_part_path(path) {
                    self.request_replica_for_stream(stream_id, "playlist-part-demand", None)
                        .await;
                    if let Some((bytes, hash)) =
                        self.cache.get_part_blocking_for_stream_id(stream_id, seq).await
                    {
                        let media_kind = self
                            .cache
                            .media_kind_hint(stream_id)
                            .await
                            .unwrap_or(requested_kind);
                        let response =
                            response(StatusCode::OK, Some(bytes), Some(media_kind.content_type()))
                                .with_etag(hash);
                        return Ok(self.record_edge_response(&method, path, query, response));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
                    return Ok(self.record_edge_response(&method, path, query, response));
                }

                if let Some((stream_id, segment, requested_kind)) = parse_stream_segment_path(path) {
                    self.request_replica_for_stream(stream_id, "playlist-segment-demand", None)
                        .await;
                    if let Some(bytes) = self.cache.get_segment_for_stream_id(stream_id, segment).await
                    {
                        let media_kind = self
                            .cache
                            .media_kind_hint(stream_id)
                            .await
                            .unwrap_or(requested_kind);
                        let response = response(
                            StatusCode::OK,
                            Some(bytes),
                            Some(media_kind.content_type()),
                        );
                        return Ok(self.record_edge_response(&method, path, query, response));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
                    return Ok(self.record_edge_response(&method, path, query, response));
                }

                if parse_init_path(path) {
                    self.request_replica_for_stream(
                        self.cache.stream_id,
                        "playlist-init-demand",
                        None,
                    )
                    .await;
                    if let Some(init) = self.cache.get_init_for_stream_id(self.cache.stream_id).await
                    {
                        let response = response(
                            StatusCode::OK,
                            Some(init),
                            Some(LiveMediaKind::Fmp4.content_type()),
                        )
                        .with_no_store();
                        return Ok(self.record_edge_response(&method, path, query, response));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
                    return Ok(self.record_edge_response(&method, path, query, response));
                }

                if let Some((seq, requested_kind)) = parse_part_path(path) {
                    if let Some((bytes, hash)) = self.cache.get_part_blocking(seq).await {
                        let media_kind = self
                            .cache
                            .media_kind_hint(self.cache.stream_id)
                            .await
                            .unwrap_or(requested_kind);
                        let response =
                            response(StatusCode::OK, Some(bytes), Some(media_kind.content_type()))
                                .with_etag(hash);
                        return Ok(self.record_edge_response(&method, path, query, response));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
                    return Ok(self.record_edge_response(&method, path, query, response));
                }

                if let Some((segment, requested_kind)) = parse_segment_path(path) {
                    if let Some(bytes) = self.cache.get_segment(segment).await {
                        let media_kind = self
                            .cache
                            .media_kind_hint(self.cache.stream_id)
                            .await
                            .unwrap_or(requested_kind);
                        let response = response(
                            StatusCode::OK,
                            Some(bytes),
                            Some(media_kind.content_type()),
                        );
                        return Ok(self.record_edge_response(&method, path, query, response));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
                    return Ok(self.record_edge_response(&method, path, query, response));
                }

                let response = response(StatusCode::NOT_FOUND, None, None);
                Ok(self.record_edge_response(&method, path, query, response))
            }
        }
    }

    async fn route_body(
        &self,
        req: Request<()>,
        mut body: BodyStream,
    ) -> HandlerResult<HandlerResponse> {
        let path = req.uri().path().to_string();
        if path.starts_with("/api/control/") {
            if req.method() != Method::POST {
                return Ok(response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    Some(Bytes::from_static(b"use POST for control commands\n")),
                    Some("text/plain"),
                ));
            }

            let request = read_control_request(&mut body).await?;
            let Some(kind) = control_kind_from_path(&path) else {
                return Ok(response(StatusCode::NOT_FOUND, None, None));
            };
            let command = self.execute_control(kind, request).await;
            let json =
                serde_json::to_vec(&command).map_err(|err| ServerError::Handler(Box::new(err)))?;
            return Ok(response(
                StatusCode::ACCEPTED,
                Some(Bytes::from(json)),
                Some("application/json"),
            )
            .with_no_store());
        }

        self.route(req).await
    }

    fn has_body_handler(&self, path: &str) -> bool {
        path.starts_with("/api/control/")
    }

    fn is_streaming(&self, path: &str) -> bool {
        path == MESH_EVENTS_PATH
    }

    async fn route_stream(
        &self,
        req: Request<()>,
        mut stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        if req.uri().path() != MESH_EVENTS_PATH {
            return Err(ServerError::Config("unsupported streaming path".into()));
        }

        let response = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-store, max-age=0")
            .body(())
            .map_err(ServerError::Http)?;
        stream_writer.send_response(response).await?;

        let mut ticker = interval(Duration::from_secs(1));
        loop {
            stream_writer
                .send_data(self.mesh_sse_event().await?)
                .await?;
            ticker.tick().await;
        }
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        Some(self)
    }

    fn websocket_handler(&self, path: &str) -> Option<&dyn WebSocketHandler> {
        if path == MESH_WEBSOCKET_PATH {
            Some(self)
        } else {
            None
        }
    }
}

#[async_trait]
impl WebSocketHandler for AppRouter {
    async fn handle_websocket(
        &self,
        req: Request<()>,
        mut stream: WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
    ) -> HandlerResult<()> {
        if req.uri().path() != MESH_WEBSOCKET_PATH {
            return Err(ServerError::Config("unsupported websocket path".into()));
        }

        let initial = serde_json::to_vec(&MeshProtocolResponse::snapshot(
            self.mesh_api_snapshot().await,
        ))
        .map_err(|err| ServerError::Handler(Box::new(err)))?;
        stream
            .send(WebSocketMessage::Text(
                String::from_utf8(initial)
                    .map_err(|err| ServerError::Handler(Box::new(err)))?
                    .into(),
            ))
            .await
            .map_err(|err| ServerError::Handler(Box::new(err)))?;

        while let Some(frame) = stream.next().await {
            match frame {
                Ok(WebSocketMessage::Text(text)) => {
                    let response = self.mesh_protocol_response_json(text.as_bytes()).await?;
                    stream
                        .send(WebSocketMessage::Text(
                            String::from_utf8(response.to_vec())
                                .map_err(|err| ServerError::Handler(Box::new(err)))?
                                .into(),
                        ))
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                }
                Ok(WebSocketMessage::Binary(bytes)) => {
                    let response = self.binary_mesh_response_from_bytes(bytes).await?;
                    stream
                        .send(WebSocketMessage::Binary(response))
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                }
                Ok(WebSocketMessage::Ping(bytes)) => {
                    stream
                        .send(WebSocketMessage::Pong(bytes))
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                }
                Ok(WebSocketMessage::Close(frame)) => {
                    let _ = stream.close(frame).await;
                    break;
                }
                Ok(_) => {}
                Err(err) => return Err(ServerError::Handler(Box::new(err))),
            }
        }

        Ok(())
    }

    fn can_handle(&self, path: &str) -> bool {
        path == MESH_WEBSOCKET_PATH
    }
}

#[async_trait]
impl WebTransportHandler for AppRouter {
    async fn handle_session(
        &self,
        session: WebTransportSession<h3_quinn::Connection, Bytes>,
    ) -> HandlerResult<()> {
        let mut datagram_reader = session.datagram_reader();
        let mut datagram_sender = session.datagram_sender();
        let mut media_decoder = WebTransportMediaDecoder::new();

        loop {
            tokio::select! {
                accepted = session.accept_bi() => {
                    let accepted = accepted
                        .map_err(|err| ServerError::Config(format!("accept WebTransport bidi: {err}")))?;
                    let Some(AcceptedBi::BidiStream(stream_session_id, mut stream)) = accepted else {
                        return Ok(());
                    };
                    if stream_session_id != session.session_id() {
                        return Err(ServerError::Config(
                            "WebTransport stream used the wrong session id".into(),
                        ));
                    }

                    let mut request = Vec::new();
                    stream
                        .read_to_end(&mut request)
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                    let response = self
                        .webtransport_response_from_bytes(Bytes::from(request))
                        .await?;
                    stream
                        .write_all(&response)
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                    stream
                        .flush()
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                    stream
                        .shutdown()
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                }
                datagram = datagram_reader.read_datagram() => {
                    let datagram = datagram
                        .map_err(|err| ServerError::Config(format!("read WebTransport datagram: {err}")))?;
                    let payload = datagram.into_payload();
                    match media_decoder.push_datagram(&payload) {
                        Ok(Some(frame)) => {
                            let unit = self
                                .cache
                                .add_media_access_unit(frame.metadata, Bytes::from(frame.payload))
                                .await
                                .map_err(|err| {
                                    ServerError::Config(format!("WebTransport media datagram ingest failed: {err}"))
                                })?;
                            let response = serde_json::to_vec(&MeshProtocolResponse::media_access_unit(
                                MediaAccessUnitResponse::from_cached(&unit),
                            ))
                            .map(Bytes::from)
                            .map_err(|err| ServerError::Handler(Box::new(err)))?;
                            datagram_sender
                                .send_datagram(response)
                                .map_err(|err| ServerError::Config(format!("send WebTransport media ack datagram: {err:?}")))?;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            let response = serde_json::to_vec(&MeshProtocolResponse::error(format!(
                                "WebTransport media datagram decode failed: {error}"
                            )))
                            .map(Bytes::from)
                            .map_err(|err| ServerError::Handler(Box::new(err)))?;
                            datagram_sender
                                .send_datagram(response)
                                .map_err(|err| ServerError::Config(format!("send WebTransport media error datagram: {err:?}")))?;
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl RawTcpHandler for AppRouter {
    async fn handle_stream(
        &self,
        mut stream: Box<dyn web_service::traits::RawStream>,
        _is_tls: bool,
    ) -> HandlerResult<()> {
        loop {
            let Some(frame) =
                read_length_prefixed_frame(&mut *stream, RAW_MESH_MAX_FRAME_BYTES).await?
            else {
                return Ok(());
            };
            let response = self.binary_mesh_response_from_bytes(frame).await?;
            write_length_prefixed_frame(&mut *stream, response.as_ref()).await?;
        }
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
                "GET, HEAD, POST, PUT, OPTIONS".into(),
            ),
        ],
        etag: None,
    }
}

fn dashboard_dist_response(path: &str) -> Option<HandlerResponse> {
    let dist_dir = std::env::var_os(DASHBOARD_DIST_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dashboard/dist"));
    dashboard_dist_response_from_dir(&dist_dir, path)
}

fn dashboard_dist_response_from_dir(dist_dir: &Path, path: &str) -> Option<HandlerResponse> {
    let relative_path = dashboard_dist_relative_path(path)?;
    let full_path = dist_dir.join(relative_path);
    let bytes = std::fs::read(full_path).ok()?;
    Some(
        response(
            StatusCode::OK,
            Some(Bytes::from(bytes)),
            dashboard_dist_content_type(relative_path),
        )
        .with_no_store(),
    )
}

fn dashboard_dist_relative_path(path: &str) -> Option<&str> {
    match path {
        "/mesh" | "/mesh/" => Some("index.html"),
        _ => {
            let candidate = path.strip_prefix('/')?;
            if candidate.is_empty()
                || candidate.contains('/')
                || candidate.contains("..")
                || !dashboard_dist_asset_extension_allowed(candidate)
            {
                return None;
            }
            Some(candidate)
        }
    }
}

fn dashboard_dist_asset_extension_allowed(path: &str) -> bool {
    path.ends_with(".js")
        || path.ends_with(".wasm")
        || path.ends_with(".css")
        || path.ends_with(".ico")
}

fn dashboard_dist_content_type(path: &str) -> Option<&'static str> {
    if path.ends_with(".html") {
        Some("text/html; charset=utf-8")
    } else if path.ends_with(".js") {
        Some("text/javascript; charset=utf-8")
    } else if path.ends_with(".wasm") {
        Some("application/wasm")
    } else if path.ends_with(".css") {
        Some("text/css; charset=utf-8")
    } else if path.ends_with(".ico") {
        Some("image/x-icon")
    } else {
        None
    }
}

fn stream_id_text(stream_id: u64) -> String {
    stream_id.to_string()
}

fn normalize_playback_base_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

fn parse_llhls_tail_path(path: &str) -> Option<u64> {
    let rest = path.strip_prefix("/live/")?;
    let stream_id = rest.strip_suffix("/tail")?;
    if stream_id.is_empty() || stream_id.contains('/') {
        return None;
    }
    stream_id.parse().ok()
}

fn parse_stream_playlist_path(path: &str) -> Option<u64> {
    let rest = path.strip_prefix("/live/")?;
    let stream_id = rest.strip_suffix("/stream.m3u8")?;
    if stream_id.is_empty() || stream_id.contains('/') {
        return None;
    }
    stream_id.parse().ok()
}

fn parse_stream_init_path(path: &str) -> Option<u64> {
    let rest = path.strip_prefix("/live/")?;
    let stream_id = rest.strip_suffix("/init.mp4")?;
    if stream_id.is_empty() || stream_id.contains('/') {
        return None;
    }
    stream_id.parse().ok()
}

fn parse_stream_part_path(path: &str) -> Option<(u64, u64, LiveMediaKind)> {
    let rest = path.strip_prefix("/live/")?;
    let (stream_id, part) = rest.split_once("/part")?;
    let (seq, media_kind) = strip_live_media_suffix(part)?;
    if stream_id.is_empty() || stream_id.contains('/') || seq.is_empty() || seq.contains('/') {
        return None;
    }
    Some((stream_id.parse().ok()?, seq.parse().ok()?, media_kind))
}

fn parse_stream_segment_path(path: &str) -> Option<(u64, u64, LiveMediaKind)> {
    let rest = path.strip_prefix("/live/")?;
    let (stream_id, segment) = rest.split_once("/seg")?;
    let (seq, media_kind) = strip_live_media_suffix(segment)?;
    if stream_id.is_empty() || stream_id.contains('/') || seq.is_empty() || seq.contains('/') {
        return None;
    }
    Some((stream_id.parse().ok()?, seq.parse().ok()?, media_kind))
}

fn parse_init_path(path: &str) -> bool {
    path == "/live/init.mp4"
}

fn parse_part_path(path: &str) -> Option<(u64, LiveMediaKind)> {
    let part = path.strip_prefix("/live/part")?;
    let (seq, media_kind) = strip_live_media_suffix(part)?;
    Some((seq.parse().ok()?, media_kind))
}

fn parse_segment_path(path: &str) -> Option<(u64, LiveMediaKind)> {
    let segment = path.strip_prefix("/live/seg")?;
    let (seq, media_kind) = strip_live_media_suffix(segment)?;
    Some((seq.parse().ok()?, media_kind))
}

fn strip_live_media_suffix(value: &str) -> Option<(&str, LiveMediaKind)> {
    value
        .strip_suffix(".mp4")
        .map(|seq| (seq, LiveMediaKind::Fmp4))
        .or_else(|| {
            value
                .strip_suffix(".ts")
                .map(|seq| (seq, LiveMediaKind::Ts))
        })
}

fn parse_media_unit_path(path: &str) -> Option<(u64, u64)> {
    let rest = path.strip_prefix("/media/")?;
    let (stream_id, sequence) = rest.split_once("/unit/")?;
    if stream_id.is_empty() || sequence.is_empty() || sequence.contains('/') {
        return None;
    }
    Some((stream_id.parse().ok()?, sequence.parse().ok()?))
}

fn query_value<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
    for part in query?.split('&') {
        let (part_key, value) = part.split_once('=').unwrap_or((part, ""));
        if part_key == key {
            return Some(value);
        }
    }
    None
}

fn parse_query_u64(query: Option<&str>, key: &str) -> Option<u64> {
    query_value(query, key)?.parse().ok()
}

async fn read_body_bytes(body: &mut BodyStream) -> HandlerResult<Bytes> {
    let mut bytes = BytesMut::new();
    while let Some(next) = body.next().await {
        bytes.extend_from_slice(&next?);
    }
    Ok(bytes.freeze())
}

async fn read_control_request(body: &mut BodyStream) -> HandlerResult<ControlRequest> {
    let bytes = read_body_bytes(body).await?;
    if bytes.is_empty() {
        return Ok(ControlRequest {
            node_id: None,
            region: None,
            stream_id: None,
        });
    }
    serde_json::from_slice(&bytes).map_err(|err| ServerError::Handler(Box::new(err)))
}

fn deserialize_optional_u64_from_any<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(value) = Option::<serde_json::Value>::deserialize(deserializer)? else {
        return Ok(None);
    };
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Number(number) => number
            .as_u64()
            .map(Some)
            .ok_or_else(|| de::Error::custom("stream_id must be an unsigned 64-bit integer")),
        serde_json::Value::String(text) => text
            .parse::<u64>()
            .map(Some)
            .map_err(|error| de::Error::custom(format!("invalid stream_id `{text}`: {error}"))),
        _ => Err(de::Error::custom(
            "stream_id must be an unsigned integer or decimal string",
        )),
    }
}

fn control_kind_from_path(path: &str) -> Option<ControlKind> {
    match path {
        "/api/control/provision-node" => Some(ControlKind::ProvisionNode),
        "/api/control/close-node" => Some(ControlKind::CloseNode),
        "/api/control/warm-stream" => Some(ControlKind::WarmStream),
        _ => None,
    }
}

fn format_replica_request_status(
    reason: &str,
    peer_count: usize,
    planned_replicas: &[ReplicaPlacement],
) -> String {
    if planned_replicas.is_empty() {
        return format!("{reason}: requested from {peer_count} peers; no eligible planned targets");
    }

    let mut targets = planned_replicas
        .iter()
        .take(5)
        .map(|placement| placement.target_node_id.as_str())
        .collect::<Vec<_>>()
        .join(",");
    if planned_replicas.len() > 5 {
        targets.push_str(",...");
    }
    format!(
        "{reason}: requested from {peer_count} peers; planned {} targets: {targets}",
        planned_replicas.len()
    )
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

const MESH_DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>av-mesh</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --line: #d9dee7;
      --text: #17202f;
      --muted: #647084;
      --blue: #2667ff;
      --green: #168a5b;
      --red: #b42318;
      --amber: #b7791f;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.4 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      height: 56px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 20px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
    }
    .brand-icon {
      width: 32px;
      height: 32px;
      image-rendering: pixelated;
      flex: 0 0 auto;
    }
    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 650;
      letter-spacing: 0;
    }
    main {
      max-width: 1360px;
      margin: 0 auto;
      padding: 18px;
      display: grid;
      grid-template-columns: minmax(360px, 1.45fr) minmax(320px, 0.85fr);
      gap: 16px;
    }
    section, aside {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }
    .section-head {
      height: 44px;
      padding: 0 14px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid var(--line);
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
    }
    #map {
      display: block;
      width: 100%;
      height: 430px;
      background: #fbfcfe;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(120px, 1fr));
      gap: 1px;
      background: var(--line);
      border-top: 1px solid var(--line);
    }
    .metric {
      background: var(--panel);
      padding: 14px;
      min-height: 86px;
    }
    .metric span {
      display: block;
      color: var(--muted);
      font-size: 12px;
    }
    .metric strong {
      display: block;
      margin-top: 6px;
      font-size: 22px;
      font-weight: 680;
      letter-spacing: 0;
      overflow-wrap: anywhere;
    }
    .bar {
      height: 8px;
      background: #e9edf4;
      border-radius: 4px;
      overflow: hidden;
      margin-top: 10px;
    }
    .bar div {
      height: 100%;
      background: var(--green);
      width: 0;
    }
    .side {
      display: grid;
      gap: 16px;
    }
    .wide {
      grid-column: 1 / -1;
    }
    .rows {
      display: grid;
      gap: 1px;
      background: var(--line);
    }
    .row {
      background: var(--panel);
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 12px;
      padding: 12px 14px;
      align-items: center;
    }
    .row small {
      display: block;
      color: var(--muted);
      margin-top: 2px;
    }
    .node-row {
      grid-template-columns: minmax(160px, 1.3fr) minmax(110px, 0.75fr) minmax(110px, 0.8fr) minmax(110px, 0.7fr) minmax(100px, 0.7fr);
    }
    .connection-row {
      grid-template-columns: minmax(180px, 1.2fr) minmax(180px, 1fr) auto;
    }
    .pill {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 8px;
      font-size: 12px;
      color: var(--muted);
      white-space: nowrap;
    }
    form {
      display: grid;
      grid-template-columns: 1fr 1fr auto;
      gap: 8px;
      padding: 14px;
      border-top: 1px solid var(--line);
    }
    input {
      min-width: 0;
      height: 34px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 0 9px;
      font: inherit;
      background: #fff;
    }
    button {
      height: 34px;
      border: 1px solid #1f54d9;
      background: var(--blue);
      color: #fff;
      border-radius: 6px;
      padding: 0 12px;
      font: inherit;
      font-weight: 600;
      cursor: pointer;
    }
    button.secondary {
      background: #fff;
      color: var(--text);
      border-color: var(--line);
    }
    @media (max-width: 900px) {
      main { grid-template-columns: 1fr; padding: 12px; }
      .metrics { grid-template-columns: repeat(2, minmax(130px, 1fr)); }
      form { grid-template-columns: 1fr; }
      .node-row, .connection-row { grid-template-columns: 1fr; }
      #map { height: 340px; }
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <img class="brand-icon" src="/assets/wavey-goose.png" width="32" height="32" alt="Wavey goose">
      <h1>av-mesh</h1>
    </div>
    <div id="updated" class="pill">loading</div>
  </header>
  <main>
    <section>
      <div class="section-head"><span>Topology</span><span id="nodeLabel"></span></div>
      <canvas id="map" width="900" height="430"></canvas>
      <div class="metrics">
        <div class="metric"><span>Capacity</span><strong id="capacity">0%</strong><div class="bar"><div id="capacityBar"></div></div></div>
        <div class="metric"><span>Throughput</span><strong id="throughput">0 bps</strong></div>
        <div class="metric"><span>Contributor Streams</span><strong id="contributors">0</strong></div>
        <div class="metric"><span>Active Streams</span><strong id="active">0</strong></div>
      </div>
    </section>
    <div class="side">
      <aside>
        <div class="section-head"><span>Streams</span><span id="streamId"></span></div>
        <div class="rows" id="streamRows"></div>
      </aside>
      <aside>
        <div class="section-head"><span>Controls</span><span id="commandStatus"></span></div>
        <form data-action="provision-node">
          <input name="node_id" placeholder="node id">
          <input name="region" placeholder="region">
          <button type="submit">Provision</button>
        </form>
        <form data-action="close-node">
          <input name="node_id" placeholder="node id">
          <input name="region" placeholder="region">
          <button class="secondary" type="submit">Close</button>
        </form>
        <form data-action="warm-stream">
          <input name="stream_id" placeholder="stream id">
          <input name="region" placeholder="region">
          <button type="submit">Warm</button>
        </form>
        <div class="rows" id="commands"></div>
      </aside>
    </div>
    <section class="wide">
      <div class="section-head"><span>Nodes</span><span id="nodeCount">0 nodes</span></div>
      <div class="rows" id="nodeRows"></div>
    </section>
    <section class="wide">
      <div class="section-head"><span>Connections</span><span id="connectionCount">0 connections</span></div>
      <div class="rows" id="connectionRows"></div>
    </section>
  </main>
  <script>
    const state = { snapshot: null, previousThroughput: null, events: false };
    const number = new Intl.NumberFormat();
    const fmtBytes = value => {
      if (!value) return '0 B';
      const units = ['B', 'KB', 'MB', 'GB', 'TB'];
      let n = value;
      let i = 0;
      while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
      return `${n.toFixed(n >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
    };
    const fmtBps = value => {
      if (!value) return '0 bps';
      const units = ['bps', 'Kbps', 'Mbps', 'Gbps', 'Tbps'];
      let n = value;
      let i = 0;
      while (n >= 1000 && i < units.length - 1) { n /= 1000; i++; }
      return `${n.toFixed(n >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
    };
    const storagePct = node => {
      const total = node.total_storage_bytes || 1;
      return Math.min(100, Math.round(((node.used_storage_bytes || 0) / total) * 100));
    };
    const streamIdText = item => item?.stream_id_text || (item?.stream_id === undefined ? '' : String(item.stream_id));
    function streamBytes(snapshot) {
      const streams = snapshot.streams && snapshot.streams.length ? snapshot.streams : [snapshot.stream];
      return streams.reduce((total, stream) => total + (stream.bytes_received || 0), 0);
    }
    function observedThroughput(snapshot) {
      const updated = snapshot.updated_unix_ms || Date.now();
      const bytes = streamBytes(snapshot);
      let bps = 0;
      if (state.previousThroughput && updated > state.previousThroughput.updated && bytes >= state.previousThroughput.bytes) {
        bps = ((bytes - state.previousThroughput.bytes) * 8000) / (updated - state.previousThroughput.updated);
      }
      state.previousThroughput = { updated, bytes };
      return bps;
    }
    async function load() {
      const res = await fetch('/api/mesh', { cache: 'no-store' });
      if (!res.ok) throw new Error(`mesh api ${res.status}`);
      state.snapshot = await res.json();
      render();
    }
    function render() {
      const s = state.snapshot;
      const node = s.node;
      const aggregate = s.aggregate || {};
      const nodes = s.nodes && s.nodes.length ? s.nodes : [node];
      const streams = s.streams || [];
      const streamIds = [...new Set(streams.map(streamIdText).filter(Boolean))].sort((a, b) => a.localeCompare(b));
      const used = aggregate.used_storage_bytes ?? node.used_storage_bytes ?? 0;
      const total = aggregate.total_storage_bytes ?? node.total_storage_bytes ?? 1;
      const pct = Math.min(100, Math.round((used / total) * 100));
      const ingressBps = observedThroughput(s);
      const egressCapacity = aggregate.total_egress_capacity_bps ?? node.egress_capacity_bps ?? 0;
      document.getElementById('updated').textContent = new Date(s.updated_unix_ms).toLocaleTimeString();
      document.getElementById('nodeLabel').textContent = `${nodes.length} nodes / ${node.region} / ${node.continent}`;
      document.getElementById('capacity').textContent = `${pct}%`;
      document.getElementById('capacityBar').style.width = `${pct}%`;
      document.getElementById('throughput').textContent = `${fmtBps(ingressBps)} / ${fmtBps(egressCapacity)} cap`;
      document.getElementById('contributors').textContent = aggregate.contributor_streams ?? node.contributor_streams ?? 0;
      document.getElementById('active').textContent = aggregate.active_streams ?? node.active_streams ?? 0;
      document.getElementById('streamId').textContent = `${streamIds.length || 1} streams`;
      document.getElementById('streamRows').innerHTML = [
        row('Stream ids', streamIds.length || 1, streamIds.slice(0, 8).join(', ') || streamIdText(s.stream)),
        row('Latest local part', s.stream.latest_local_part ?? 'none', fmtBytes(s.stream.latest_local_part_bytes || 0)),
        row('Latest mesh part', s.stream.latest_mesh_part ?? 'none', `${s.stream.mesh_peers.length} peers`),
        row('Nodes', aggregate.node_count ?? nodes.length, `${aggregate.connection_count ?? 0} connections`),
        row('Ingest', fmtBytes(s.stream.bytes_received), `${s.stream.datagrams_received} datagrams`),
        row('Policy', `${s.replication_policy.baseline_per_continent}/continent`, `${s.replication_policy.baseline_per_region}/region`),
        row('Replica plan', (s.planned_replicas || []).length, (s.planned_replicas || []).slice(0, 3).map(p => `${streamIdText(p)}:${p.target_node_id}`).join(', ') || 'none')
      ].join('');
      renderNodes(nodes);
      renderConnections(s.connections || []);
      document.getElementById('commands').innerHTML = (s.recent_commands || []).map(cmd =>
        row(cmd.kind, cmd.node_id || cmd.region || `stream ${streamIdText(cmd)}`, cmd.status)
      ).join('') || row('No commands', 'ready', '');
      drawMap(s);
    }
    function row(label, value, sub) {
      return `<div class="row"><div><strong>${escapeHtml(String(label))}</strong><small>${escapeHtml(String(sub || ''))}</small></div><span class="pill">${escapeHtml(String(value))}</span></div>`;
    }
    function renderNodes(nodes) {
      document.getElementById('nodeCount').textContent = `${nodes.length} nodes`;
      document.getElementById('nodeRows').innerHTML = nodes.map(node => {
        const nodeState = node.draining ? 'draining' : `${node.region} / ${node.continent}`;
        return `<div class="row node-row">
          <div><strong>${escapeHtml(node.node_id)}</strong><small>${escapeHtml(nodeState)}</small></div>
          <span class="pill">${storagePct(node)}% storage</span>
          <span class="pill">${fmtBps(node.egress_capacity_bps || 0)}</span>
          <span class="pill">${number.format(node.contributor_streams || 0)} contributors</span>
          <span class="pill">${number.format(node.active_streams || 0)} active</span>
        </div>`;
      }).join('') || row('No nodes', 'waiting for telemetry', '');
    }
    function renderConnections(connections) {
      document.getElementById('connectionCount').textContent = `${connections.length} connections`;
      document.getElementById('connectionRows').innerHTML = connections.map(conn => {
        const target = conn.target_node_id || conn.target_addr;
        return `<div class="row connection-row">
          <div><strong>${escapeHtml(conn.source_node_id)}</strong><small>source</small></div>
          <div><strong>${escapeHtml(target)}</strong><small>${escapeHtml(conn.target_addr)}</small></div>
          <span class="pill">${escapeHtml(conn.state)}</span>
        </div>`;
      }).join('') || row('No connections', 'waiting for peer gossip', '');
    }
    function drawMap(s) {
      const canvas = document.getElementById('map');
      const rect = canvas.getBoundingClientRect();
      const scale = window.devicePixelRatio || 1;
      canvas.width = Math.max(1, Math.floor(rect.width * scale));
      canvas.height = Math.max(1, Math.floor(rect.height * scale));
      const ctx = canvas.getContext('2d');
      ctx.scale(scale, scale);
      const w = rect.width;
      const h = rect.height;
      ctx.clearRect(0, 0, w, h);
      ctx.strokeStyle = '#e2e7ef';
      ctx.lineWidth = 1;
      for (let x = 80; x < w; x += 120) { line(ctx, x, 0, x, h); }
      for (let y = 70; y < h; y += 90) { line(ctx, 0, y, w, y); }
      const cx = w / 2;
      const cy = h / 2;
      const nodes = s.nodes && s.nodes.length ? s.nodes : [s.node];
      const positions = new Map();
      nodes.forEach((node, i) => {
        positions.set(node.node_id, nodePoint(node, i, nodes.length, w, h, cx, cy));
      });
      (s.connections || []).forEach(conn => {
        const from = positions.get(conn.source_node_id);
        if (!from) return;
        const target = positions.get(conn.target_node_id) || positions.get(conn.target_addr) || hashPoint(conn.target_addr, w, h);
        ctx.strokeStyle = conn.target_node_id ? '#7e98c0' : '#c3ccd9';
        ctx.lineWidth = 1.5;
        line(ctx, from.x, from.y, target.x, target.y);
      });
      nodes.forEach(node => {
        const pos = positions.get(node.node_id);
        const local = node.node_id === s.node.node_id;
        dot(ctx, pos.x, pos.y, local ? 13 : 9, local ? '#2667ff' : '#ffffff', local ? '#173a91' : '#2667ff');
        label(ctx, node.node_id, pos.x + 14, pos.y + 5, '#17202f');
      });
    }
    function line(ctx, x1, y1, x2, y2) { ctx.beginPath(); ctx.moveTo(x1, y1); ctx.lineTo(x2, y2); ctx.stroke(); }
    function dot(ctx, x, y, r, fill, stroke) { ctx.beginPath(); ctx.arc(x, y, r, 0, Math.PI * 2); ctx.fillStyle = fill; ctx.fill(); ctx.strokeStyle = stroke; ctx.lineWidth = 2; ctx.stroke(); }
    function label(ctx, text, x, y, color) { ctx.fillStyle = color; ctx.font = '12px system-ui, sans-serif'; ctx.fillText(text, x, y); }
    function nodePoint(node, i, count, w, h, cx, cy) {
      const lat = Number(node.latitude);
      const lon = Number(node.longitude);
      if (Number.isFinite(lat) && Number.isFinite(lon)) {
        const pad = 36;
        const x = pad + ((lon + 180) / 360) * Math.max(1, w - pad * 2);
        const y = pad + ((90 - lat) / 180) * Math.max(1, h - pad * 2);
        return { x, y };
      }
      const angle = count === 1 ? 0 : (Math.PI * 2 * i) / count;
      const radius = count === 1 ? 0 : Math.min(w, h) * 0.34;
      return { x: cx + Math.cos(angle) * radius, y: cy + Math.sin(angle) * radius };
    }
    function hashPoint(text, w, h) {
      let hash = 0;
      for (let i = 0; i < text.length; i++) hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0;
      const x = 40 + (Math.abs(hash) % Math.max(1, Math.floor(w - 80)));
      const y = 40 + (Math.abs(hash >> 8) % Math.max(1, Math.floor(h - 80)));
      return { x, y };
    }
    function escapeHtml(value) {
      return value.replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    }
    document.querySelectorAll('form[data-action]').forEach(form => {
      form.addEventListener('submit', async event => {
        event.preventDefault();
        const data = new FormData(form);
        const body = {};
        for (const [key, value] of data.entries()) {
          if (!value) continue;
          body[key] = key === 'stream_id' ? String(value).trim() : value;
        }
        const action = form.getAttribute('data-action');
        const status = document.getElementById('commandStatus');
        try {
          status.textContent = 'sending';
          const res = await fetch(`/api/control/${action}`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(body)
          });
          const command = await res.json().catch(() => null);
          if (!res.ok) throw new Error(command?.error || `control ${res.status}`);
          status.textContent = command?.status || 'accepted';
          form.reset();
          await load();
        } catch (err) {
          status.textContent = err.message;
        }
      });
    });
    function connectEvents() {
      if (!('EventSource' in window)) return false;
      const source = new EventSource('/api/mesh/events');
      source.addEventListener('mesh', event => {
        state.events = true;
        state.snapshot = JSON.parse(event.data);
        render();
      });
      source.onerror = () => {
        document.getElementById('updated').textContent = state.snapshot ? 'reconnecting' : 'loading';
      };
      return true;
    }
    const eventBacked = connectEvents();
    load().catch(err => { document.getElementById('updated').textContent = err.message; });
    setInterval(() => {
      if (!eventBacked || !state.events) load().catch(() => {});
    }, 1000);
    if (eventBacked) setInterval(() => load().catch(() => {}), 10000);
  </script>
</body>
</html>"#;

#[cfg(test)]
mod tests {
    use super::*;
    use raptorq_datagram_fec::MediaFrameFlags;
    use std::{
        io::{self, Cursor},
        pin::Pin,
        sync::Mutex,
        task::{Context as TaskContext, Poll},
    };
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    fn encode_test_fmp4_slot(init: Option<&[u8]>, media: &[u8]) -> Bytes {
        let init = init.unwrap_or_default();
        let mut out = Vec::with_capacity(MESH_FMP4_SLOT_HEADER_LEN + init.len() + media.len());
        out.extend_from_slice(MESH_FMP4_SLOT_MAGIC);
        out.extend_from_slice(&(init.len() as u32).to_be_bytes());
        out.extend_from_slice(&(media.len() as u32).to_be_bytes());
        out.extend_from_slice(init);
        out.extend_from_slice(media);
        Bytes::from(out)
    }

    fn deterministic_video_payload(len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| {
                let mixed = (index as u32)
                    .wrapping_mul(1_103_515_245)
                    .wrapping_add(12_345);
                (mixed >> 16) as u8
            })
            .collect()
    }

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

        cache
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"stream77-part0"))
            .await
            .unwrap();
        let playlist = cache.playlist_for_stream_id(77).await;
        assert!(playlist.contains("part0.ts"));
        assert!(playlist.contains("#EXT-X-PRELOAD-HINT"));
    }

    #[tokio::test]
    async fn fmp4_stream_slots_emit_mp4_playlist_and_media() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        cache
            .commit_stream_payload(77, encode_test_fmp4_slot(Some(b"ftypmoov"), b"moofmdat-a"))
            .await
            .unwrap();
        cache
            .commit_stream_payload(77, encode_test_fmp4_slot(None, b"moofmdat-b"))
            .await
            .unwrap();

        let playlist = cache.playlist_for_stream_id(77).await;
        assert!(playlist.contains("#EXT-X-MAP:URI=\"init.mp4\""));
        assert!(playlist.contains("part0.mp4"));
        assert!(playlist.contains("part1.mp4"));
        assert!(playlist.contains("seg0.mp4"));
        assert!(!playlist.contains("AVFMP4S1"));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/init.mp4")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.content_type.as_deref(), Some("video/mp4"));
        assert_eq!(response.body.unwrap(), Bytes::from_static(b"ftypmoov"));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/part0.mp4")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.content_type.as_deref(), Some("video/mp4"));
        assert_eq!(response.body.unwrap(), Bytes::from_static(b"moofmdat-a"));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/seg0.mp4")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.content_type.as_deref(), Some("video/mp4"));
        assert_eq!(
            response.body.unwrap(),
            Bytes::from_static(b"moofmdat-amoofmdat-b")
        );

        mesh.shutdown();
    }

    #[test]
    fn parses_live_paths() {
        assert_eq!(parse_stream_playlist_path("/live/77/stream.m3u8"), Some(77));
        assert_eq!(parse_stream_init_path("/live/77/init.mp4"), Some(77));
        assert_eq!(
            parse_stream_part_path("/live/77/part42.ts"),
            Some((77, 42, LiveMediaKind::Ts))
        );
        assert_eq!(
            parse_stream_part_path("/live/77/part42.mp4"),
            Some((77, 42, LiveMediaKind::Fmp4))
        );
        assert_eq!(
            parse_stream_segment_path("/live/77/seg7.ts"),
            Some((77, 7, LiveMediaKind::Ts))
        );
        assert_eq!(
            parse_stream_segment_path("/live/77/seg7.mp4"),
            Some((77, 7, LiveMediaKind::Fmp4))
        );
        assert_eq!(parse_stream_playlist_path("/live/stream.m3u8"), None);
        assert_eq!(parse_stream_playlist_path("/live/77/part42.ts"), None);
        assert_eq!(parse_stream_part_path("/live/part42.ts"), None);
        assert_eq!(parse_stream_segment_path("/live/seg7.ts"), None);
        assert_eq!(
            parse_part_path("/live/part42.ts"),
            Some((42, LiveMediaKind::Ts))
        );
        assert_eq!(
            parse_part_path("/live/part42.mp4"),
            Some((42, LiveMediaKind::Fmp4))
        );
        assert_eq!(
            parse_segment_path("/live/seg7.ts"),
            Some((7, LiveMediaKind::Ts))
        );
        assert_eq!(
            parse_segment_path("/live/seg7.mp4"),
            Some((7, LiveMediaKind::Fmp4))
        );
        assert_eq!(parse_part_path("/live/seg7.ts"), None);
        assert_eq!(parse_llhls_tail_path("/live/77/tail"), Some(77));
        assert_eq!(parse_llhls_tail_path("/live/not-number/tail"), None);
        assert_eq!(
            parse_query_u64(Some("mode=part&after=41"), "after"),
            Some(41)
        );
        assert_eq!(
            normalize_playback_base_url("https://node/live/"),
            "https://node/live"
        );
    }

    #[test]
    fn default_args_keep_edge_protocols_opt_in() {
        let args = Args::try_parse_from(["av-mesh"])
            .unwrap()
            .normalized()
            .unwrap();

        assert!(!args.edge_websocket);
        assert!(!args.edge_webtransport);
        assert!(args.raw_tcp_port.is_none());
        assert_eq!(args.telemetry_dns_name, "local.wavey.ai");
    }

    #[test]
    fn parses_edge_protocol_opt_ins() {
        let args = Args::try_parse_from([
            "av-mesh",
            "--edge-websocket",
            "--edge-webtransport",
            "--raw-tcp-port",
            "19000",
        ])
        .unwrap()
        .normalized()
        .unwrap();

        assert!(args.edge_websocket);
        assert!(args.edge_webtransport);
        assert_eq!(args.raw_tcp_port, Some(19000));
    }

    #[cfg(feature = "linode-provisioner")]
    #[test]
    fn parses_linode_provision_flags_and_region_maps() {
        let args = Args::try_parse_from([
            "av-mesh",
            "--linode-provision",
            "--linode-image-id",
            "linode/arch",
            "--linode-instance-type",
            "g6-dedicated-2",
            "--linode-domain-id",
            "2958920",
            "--linode-vlan-tag",
            "avmesh",
            "--linode-token-env",
            "TEST_LINODE_TOKEN",
            "--linode-pub-key-env",
            "TEST_LINODE_PUB_KEY",
            "--linode-region-map",
            "uk=gb-lon",
            "--linode-region-map",
            "us=us-east",
        ])
        .unwrap()
        .normalized()
        .unwrap();
        let config = args.linode_provision_config().unwrap();

        assert_eq!(config.image_id, "linode/arch");
        assert_eq!(config.instance_type, "g6-dedicated-2");
        assert_eq!(config.domain_id, 2_958_920);
        assert_eq!(config.vlan_tag, "avmesh");
        assert_eq!(config.token_env, "TEST_LINODE_TOKEN");
        assert_eq!(config.pub_key_env, "TEST_LINODE_PUB_KEY");
        assert_eq!(config.resolve_region("uk"), "gb-lon");
        assert_eq!(config.resolve_region("us"), "us-east");
        assert_eq!(config.resolve_region("jp-osa"), "jp-osa");
    }

    #[cfg(feature = "linode-provisioner")]
    #[test]
    fn linode_provision_requires_provider_config() {
        let error = Args::try_parse_from(["av-mesh", "--linode-provision"])
            .unwrap()
            .normalized()
            .unwrap_err()
            .to_string();

        assert!(error.contains("--linode-image-id"));
        assert!(error.contains("--linode-instance-type"));
        assert!(error.contains("--linode-domain-id"));
    }

    fn serialized_media_access_unit_for_tests(
        metadata: MediaFrameMetadata,
        payload: &'static [u8],
    ) -> Bytes {
        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: payload.len() as u32,
            fragment_offset: 0,
        };
        let mut bytes = vec![0; MEDIA_FRAME_HEADER_LEN];
        header.encode(&mut bytes[..]).unwrap();
        bytes.extend_from_slice(payload);
        Bytes::from(bytes)
    }

    fn push_raw_mesh_frame(frames: &mut Vec<u8>, payload: &[u8]) {
        let len = u32::try_from(payload.len()).unwrap();
        frames.extend_from_slice(&len.to_be_bytes());
        frames.extend_from_slice(payload);
    }

    fn pop_raw_mesh_frame(frames: &mut &[u8]) -> Bytes {
        let (len_buf, rest) = frames.split_at(4);
        *frames = rest;
        let len_buf = <[u8; 4]>::try_from(len_buf).unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let (payload, rest) = frames.split_at(len);
        *frames = rest;
        Bytes::copy_from_slice(payload)
    }

    struct MemoryRawStream {
        read: Cursor<Vec<u8>>,
        written: Arc<Mutex<Vec<u8>>>,
    }

    impl MemoryRawStream {
        fn new(read: Vec<u8>) -> (Self, Arc<Mutex<Vec<u8>>>) {
            let written = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    read: Cursor::new(read),
                    written: Arc::clone(&written),
                },
                written,
            )
        }
    }

    impl AsyncRead for MemoryRawStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let pos = self.read.position() as usize;
            let source_len = self.read.get_ref().len();
            if pos >= source_len {
                return Poll::Ready(Ok(()));
            }

            let to_copy = buf.remaining().min(source_len - pos);
            buf.put_slice(&self.read.get_ref()[pos..pos + to_copy]);
            self.read.set_position((pos + to_copy) as u64);
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for MemoryRawStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.written.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn http_ingest_is_not_served_by_mesh_node() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/ingest")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::NOT_FOUND);
        assert!(!router.has_body_handler("/ingest"));

        let req = Request::builder()
            .method(Method::POST)
            .uri("/ingest")
            .body(())
            .unwrap();
        let body: BodyStream = Box::pin(futures_util::stream::iter(vec![Ok(Bytes::from_static(
            b"http-body-owned-by-av-contrib",
        ))]));
        let response = router.route_body(req, body).await.unwrap();
        assert_eq!(response.status, StatusCode::METHOD_NOT_ALLOWED);
        assert!(cache.get_part_blocking(0).await.is_none());
        mesh.shutdown();
    }

    #[tokio::test]
    async fn http_media_access_unit_ingest_is_not_served_by_mesh_node() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let stream_id = u64::from(u32::MAX) + 55;
        let rejected_metadata = MediaFrameMetadata {
            duration_ms: 33,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(stream_id, 7, 1234, MediaCodec::H264)
        };
        let req = Request::builder()
            .method(Method::GET)
            .uri("/media/access-unit")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::NOT_FOUND);
        assert!(!router.has_body_handler("/media/access-unit"));

        let req = Request::builder()
            .method(Method::POST)
            .uri("/media/access-unit")
            .body(())
            .unwrap();
        let body: BodyStream = Box::pin(futures_util::stream::iter(vec![Ok(
            serialized_media_access_unit_for_tests(rejected_metadata, b"h264-access-unit"),
        )]));

        let response = router.route_body(req, body).await.unwrap();

        assert_eq!(response.status, StatusCode::METHOD_NOT_ALLOWED);
        assert!(cache.get_media_access_unit(stream_id, 7).await.is_none());

        let metadata = MediaFrameMetadata {
            duration_ms: 33,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(stream_id, 7, 1234, MediaCodec::H264)
        };
        let cached = cache
            .add_media_access_unit(metadata, Bytes::from_static(b"h264-access-unit"))
            .await
            .unwrap();
        assert_eq!(cached.metadata.codec, MediaCodec::H264);
        assert!(cached.metadata.flags.is_keyframe());
        assert_eq!(cached.payload_bytes, b"h264-access-unit".len());

        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("/media/{stream_id}/unit/7"))
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.content_type.as_deref(),
            Some(MEDIA_ACCESS_UNIT_CONTENT_TYPE)
        );
        let body = response.body.unwrap();
        let header = MediaFragmentHeader::decode(&body[..MEDIA_FRAME_HEADER_LEN]).unwrap();
        assert_eq!(header.metadata.stream_id, stream_id);
        assert_eq!(header.metadata.sequence, 7);
        assert_eq!(header.metadata.codec, MediaCodec::H264);
        assert_eq!(&body[MEDIA_FRAME_HEADER_LEN..], b"h264-access-unit");
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_returns_node_capacity_and_stream_counts() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        cache.push_payload(b"mesh-ui-part").await.unwrap();
        cache.rotate_if_due(true).await.unwrap();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/mesh")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();
        let body = response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(json["node"]["node_id"], "test-node");
        assert_eq!(json["stream"]["stream_id"], 1);
        assert_eq!(json["stream"]["stream_id_text"], "1");
        assert!(json["streams"]
            .as_array()
            .unwrap()
            .iter()
            .any(|stream| stream["stream_id"] == 1 && stream["stream_id_text"] == "1"));
        assert_eq!(json["node"]["active_streams"], 1);
        assert_eq!(json["edge_services"][0]["node_id"], "test-node");
        assert_eq!(
            json["edge_services"][0]["playback_base_url"],
            "https://test-node.local/live"
        );
        assert_eq!(json["edge_services"][0]["active_readers"], 0);
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_ui_serves_topology_inventory_and_operator_controls() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let req = Request::builder()
            .method(Method::GET)
            .uri("/mesh")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.content_type.as_deref(),
            Some("text/html; charset=utf-8")
        );
        let body = String::from_utf8(response.body.unwrap().to_vec()).unwrap();
        if body.contains("av mission control") {
            for expected in ["av mission control", "type=\"module\"", "av-mesh-dashboard"] {
                assert!(
                    body.contains(expected),
                    "Leptos dashboard missing expected fragment: {expected}"
                );
            }
        } else {
            for expected in [
                "id=\"map\"",
                "id=\"nodeRows\"",
                "id=\"connectionRows\"",
                "id=\"capacity\"",
                "id=\"throughput\"",
                "id=\"contributors\"",
                "id=\"active\"",
                "class=\"brand-icon\"",
                "src=\"/assets/wavey-goose.png\"",
                "data-action=\"provision-node\"",
                "data-action=\"close-node\"",
                "data-action=\"warm-stream\"",
                "new EventSource('/api/mesh/events')",
                "renderNodes(nodes)",
                "renderConnections(s.connections || [])",
            ] {
                assert!(
                    body.contains(expected),
                    "legacy dashboard missing expected fragment: {expected}"
                );
            }
        }

        let icon_req = Request::builder()
            .method(Method::GET)
            .uri("/assets/wavey-goose.png")
            .body(())
            .unwrap();
        let icon_response = router.route(icon_req).await.unwrap();
        assert_eq!(icon_response.status, StatusCode::OK);
        assert_eq!(icon_response.content_type.as_deref(), Some("image/png"));
        let icon = icon_response.body.unwrap();
        assert!(icon.starts_with(b"\x89PNG\r\n\x1a\n"));
        mesh.shutdown();
    }

    #[test]
    fn dashboard_dist_response_serves_leptos_assets_when_present() {
        let temp_dir =
            std::env::temp_dir().join(format!("av-mesh-dashboard-dist-test-{}", now_unix_ms()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        std::fs::write(
            temp_dir.join("index.html"),
            r#"<html><body><script type="module" src="/app.js"></script></body></html>"#,
        )
        .unwrap();
        std::fs::write(temp_dir.join("app.js"), "export default {};").unwrap();
        std::fs::write(temp_dir.join("app_bg.wasm"), b"\0asm").unwrap();

        let index = dashboard_dist_response_from_dir(&temp_dir, "/mesh").unwrap();
        assert_eq!(index.status, StatusCode::OK);
        assert_eq!(
            index.content_type.as_deref(),
            Some("text/html; charset=utf-8")
        );
        assert!(String::from_utf8(index.body.unwrap().to_vec())
            .unwrap()
            .contains("type=\"module\""));
        assert!(index
            .headers
            .iter()
            .any(|(name, value)| name == "cache-control" && value.contains("no-store")));

        let js = dashboard_dist_response_from_dir(&temp_dir, "/app.js").unwrap();
        assert_eq!(
            js.content_type.as_deref(),
            Some("text/javascript; charset=utf-8")
        );
        let wasm = dashboard_dist_response_from_dir(&temp_dir, "/app_bg.wasm").unwrap();
        assert_eq!(wasm.content_type.as_deref(), Some("application/wasm"));
        assert!(dashboard_dist_response_from_dir(&temp_dir, "/live/stream.m3u8").is_none());

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn telemetry_aggregator_consumes_avmt_payloads_from_global_nodes() {
        let aggregator = TelemetryAggregator::default();
        let local = telemetry_snapshot_for_tests(
            "uk-1",
            "uk",
            "eu",
            51.5,
            -0.1,
            vec![PeerSnapshot {
                addr: "us-1".into(),
                state: "discovered".into(),
            }],
            1,
        );
        let us = telemetry_snapshot_for_tests(
            "us-1",
            "us-east",
            "na",
            37.4,
            -78.6,
            vec![PeerSnapshot {
                addr: "uk-1".into(),
                state: "discovered".into(),
            }],
            2,
        );
        let apac = telemetry_snapshot_for_tests(
            "jp-1",
            "jp-east",
            "apac",
            35.6,
            139.6,
            vec![PeerSnapshot {
                addr: "uk-1".into(),
                state: "discovered".into(),
            }],
            3,
        );

        assert!(!aggregator
            .ingest_payload(TcpChangesPayload {
                tag: *b"myip",
                val: Bytes::from_static(b"ignored"),
            })
            .await
            .unwrap());
        for snapshot in [us, apac] {
            let payload = TcpChangesPayload {
                tag: TELEMETRY_TAG,
                val: Bytes::from(serde_json::to_vec(&snapshot).unwrap()),
            };
            assert!(aggregator.ingest_payload(payload).await.unwrap());
        }

        let aggregate = aggregator.snapshot(local).await;

        assert_eq!(aggregate.aggregate.node_count, 3);
        assert_eq!(aggregate.aggregate.connection_count, 3);
        assert_eq!(aggregate.aggregate.active_streams, 3);
        assert!(aggregate.nodes.iter().any(|node| node.node_id == "jp-1"));
        assert!(aggregate
            .connections
            .iter()
            .any(|connection| connection.source_node_id == "us-1"
                && connection.target_addr == "uk-1"
                && connection.target_node_id.as_deref() == Some("uk-1")));
    }

    #[tokio::test]
    async fn telemetry_aggregator_resolves_peer_addresses_to_node_ids() {
        let aggregator = TelemetryAggregator::default();
        let mut local = telemetry_snapshot_for_tests(
            "uk-1",
            "uk",
            "eu",
            51.5,
            -0.1,
            vec![PeerSnapshot {
                addr: "10.0.0.2:9100".into(),
                state: "discovered".into(),
            }],
            1,
        );
        local.mesh_addr = Some("10.0.0.1:9100".into());
        let mut us = telemetry_snapshot_for_tests(
            "us-1",
            "us-east",
            "na",
            37.4,
            -78.6,
            vec![PeerSnapshot {
                addr: "10.0.0.1:9100".into(),
                state: "discovered".into(),
            }],
            1,
        );
        us.mesh_addr = Some("10.0.0.2:9100".into());
        aggregator.ingest_snapshot(us).await;

        let aggregate = aggregator.snapshot(local).await;
        let connection = aggregate
            .connections
            .iter()
            .find(|connection| connection.source_node_id == "uk-1")
            .expect("missing uk connection");

        assert_eq!(connection.target_addr, "10.0.0.2:9100");
        assert_eq!(connection.target_node_id.as_deref(), Some("us-1"));
    }

    #[tokio::test]
    async fn stale_telemetry_is_pruned_from_api_and_control_targets() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::new(1_000);
        let now_ms = now_unix_ms();
        let mut stale = telemetry_snapshot_for_tests(
            "jp-edge-old",
            "jp-east",
            "apac",
            35.6,
            139.6,
            Vec::new(),
            7,
        );
        stale.updated_unix_ms = now_ms.saturating_sub(2_000);
        let mut live = telemetry_snapshot_for_tests(
            "jp-edge-live",
            "jp-east",
            "apac",
            35.7,
            139.7,
            Vec::new(),
            7,
        );
        live.updated_unix_ms = now_ms;
        telemetry.ingest_snapshot(stale).await;
        telemetry.ingest_snapshot(live).await;

        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let snapshot = router.mesh_api_snapshot().await;
        let targets = router
            .selected_control_target_node_ids(
                ControlKind::WarmStream,
                &ControlRequest {
                    node_id: None,
                    region: Some("jp-east".into()),
                    stream_id: Some(7),
                },
            )
            .await;

        assert_eq!(snapshot.aggregate.node_count, 2);
        assert_eq!(snapshot.telemetry.fresh_remote_count, 1);
        assert_eq!(snapshot.telemetry.stale_remote_count, 1);
        assert_eq!(snapshot.telemetry.stale_nodes[0].node_id, "jp-edge-old");
        assert_eq!(snapshot.telemetry.stale_nodes[0].region, "jp-east");
        assert!(snapshot.telemetry.stale_nodes[0].age_ms >= 1_000);
        assert!(snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "telemetry_snapshot_stale"
                && alert.node_id.as_deref() == Some("jp-edge-old")));
        assert!(snapshot
            .nodes
            .iter()
            .any(|node| node.node_id == "test-node"));
        assert!(snapshot
            .nodes
            .iter()
            .any(|node| node.node_id == "jp-edge-live"));
        assert!(!snapshot
            .nodes
            .iter()
            .any(|node| node.node_id == "jp-edge-old"));
        assert_eq!(targets, vec!["jp-edge-live"]);
        assert!(!router
            .telemetry
            .snapshots
            .read()
            .await
            .contains_key("jp-edge-old"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_includes_remote_telemetry_nodes() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        telemetry
            .ingest_snapshot(telemetry_snapshot_for_tests(
                "us-1",
                "us-east",
                "na",
                37.4,
                -78.6,
                Vec::new(),
                5,
            ))
            .await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/mesh")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();
        let body = response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(json["aggregate"]["node_count"], 2);
        assert!(json["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|node| node["node_id"] == "us-1"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_reports_operational_alerts() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        let mut remote = telemetry_snapshot_for_tests(
            "full-node",
            "us-east",
            "na",
            37.4,
            -78.6,
            vec![PeerSnapshot {
                addr: "10.0.0.9:29101".into(),
                state: "discovered".into(),
            }],
            5,
        );
        remote.node.used_storage_bytes = 960_000;
        telemetry.ingest_snapshot(remote).await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);

        router
            .execute_control(
                ControlKind::ProvisionNode,
                ControlRequest {
                    node_id: Some("new-node".into()),
                    region: Some("us-east".into()),
                    stream_id: None,
                },
            )
            .await;

        let response = router
            .route(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/mesh")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let alert_codes = json["alerts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|alert| alert["code"].as_str().unwrap())
            .collect::<HashSet<_>>();
        let activity_codes = json["activity"]
            .as_array()
            .unwrap()
            .iter()
            .map(|activity| activity["code"].as_str().unwrap())
            .collect::<HashSet<_>>();

        assert_eq!(response.status, StatusCode::OK);
        assert!(alert_codes.contains("mesh_unknown_peers"));
        assert!(alert_codes.contains("edge_playback_missing"));
        assert!(alert_codes.contains("storage_exhausted"));
        assert!(alert_codes.contains("control_skipped"));
        assert!(activity_codes.contains("mesh_snapshot"));
        assert!(activity_codes.contains("storage_exhausted"));
        assert!(activity_codes.contains("provision_node"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_reports_telemetry_peer_data_hose_status() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let peer = unused_tcp_loopback_addr();
        let monitor = TelemetryPeerMonitor::new(&[peer]);
        monitor.record_connecting(peer).await;
        monitor
            .record_disconnected(peer, Some("dial failed".into()))
            .await;
        let router = app_router_for_tests_with_telemetry_monitor(
            Arc::clone(&cache),
            Arc::clone(&mesh),
            monitor.clone(),
        );

        let snapshot = router.mesh_api_snapshot().await;
        assert_eq!(snapshot.orchestration.telemetry_peers.len(), 1);
        let peer_status = &snapshot.orchestration.telemetry_peers[0];
        assert_eq!(peer_status.peer, peer.to_string());
        assert_eq!(peer_status.state, "error");
        assert_eq!(peer_status.connect_attempts, 1);
        assert_eq!(peer_status.disconnects, 1);
        assert_eq!(peer_status.last_error.as_deref(), Some("dial failed"));
        assert!(snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "telemetry_peer_unavailable"));

        monitor.record_connecting(peer).await;
        monitor.record_connected(peer).await;
        monitor.record_payload(peer, 512).await;
        let snapshot = router.mesh_api_snapshot().await;
        let peer_status = &snapshot.orchestration.telemetry_peers[0];
        assert_eq!(peer_status.state, "connected");
        assert_eq!(peer_status.connect_attempts, 2);
        assert_eq!(peer_status.payloads, 1);
        assert_eq!(peer_status.bytes, 512);
        assert!(!snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "telemetry_peer_unavailable"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_reports_edge_response_errors() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        let missing_response = router
            .route(
                Request::builder()
                    .method(Method::GET)
                    .uri("/live/77/init.mp4")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_response.status, StatusCode::NOT_FOUND);

        let api_response = router
            .route(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/mesh")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = api_response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let edge = json["edge_services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|edge| edge["node_id"] == "test-node")
            .unwrap();
        let alert_codes = json["alerts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|alert| alert["code"].as_str().unwrap())
            .collect::<HashSet<_>>();

        assert_eq!(api_response.status, StatusCode::OK);
        assert_eq!(edge["responses_total"], 1);
        assert_eq!(edge["response_errors"], 1);
        assert_eq!(edge["response_not_found"], 1);
        assert_eq!(edge["recent_responses"][0]["path"], "/live/77/init.mp4");
        assert_eq!(edge["recent_responses"][0]["status"], 404);
        assert!(alert_codes.contains("edge_response_errors"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_reports_orchestration_status() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let disabled_router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let disabled = disabled_router.mesh_api_snapshot().await;
        assert!(!disabled.orchestration.control_dispatch_ready);
        assert!(!disabled.orchestration.provision.enabled);
        assert!(disabled.orchestration.provision.backends.is_empty());
        assert!(disabled.orchestration.provision.backend_statuses.is_empty());

        let provision = ProvisionExecutor::new(
            Some("printf provision-ready".into()),
            Duration::from_millis(1_500),
        );
        let router =
            app_router_for_tests_with_provision(Arc::clone(&cache), Arc::clone(&mesh), provision);
        let (tx, _rx) = mpsc::channel(1);
        router.dispatch.set_sender(tx).await;
        let enabled = router.mesh_api_snapshot().await;

        assert!(enabled.orchestration.control_dispatch_ready);
        assert!(enabled.orchestration.provision.enabled);
        assert_eq!(enabled.orchestration.provision.backends, vec!["command"]);
        assert_eq!(enabled.orchestration.provision.timeout_ms, 1_500);
        assert_eq!(enabled.orchestration.provision.backend_statuses.len(), 1);
        assert_eq!(
            enabled.orchestration.provision.backend_statuses[0].name,
            "command"
        );
        assert_eq!(
            enabled.orchestration.provision.backend_statuses[0].state,
            "ready"
        );
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_includes_baseline_replica_plan_from_telemetry() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        telemetry
            .ingest_snapshot(telemetry_snapshot_for_tests(
                "us-1",
                "us-east",
                "na",
                37.4,
                -78.6,
                Vec::new(),
                1,
            ))
            .await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/mesh")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();
        let body = response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(response.status, StatusCode::OK);
        let planned = json["planned_replicas"].as_array().unwrap();
        assert!(planned.iter().any(|placement| placement["stream_id"] == 1
            && placement["stream_id_text"] == "1"
            && placement["target_node_id"] == "test-node"
            && placement["reason_text"] == "baseline continent test-continent"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_includes_non_default_stream_replica_plan_from_telemetry() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        let mut remote =
            telemetry_snapshot_for_tests("us-1", "us-east", "na", 37.4, -78.6, Vec::new(), 1);
        remote.streams = vec![StreamTelemetry {
            node_id: "us-1".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(4),
            latest_local_part_bytes: Some(2048),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(250),
            latest_mesh_part: Some(4),
            bytes_received: 8192,
            datagrams_received: 4,
            last_ingest_age_ms: Some(250),
            stale_threshold_ms: Some(5_000),
        }];
        telemetry.ingest_snapshot(remote).await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/mesh")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();
        let body = response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(response.status, StatusCode::OK);
        let planned = json["planned_replicas"].as_array().unwrap();
        assert!(planned.iter().any(|placement| {
            placement["stream_id"] == 77
                && placement["stream_id_text"] == "77"
                && placement["target_node_id"] == "test-node"
                && placement["reason_text"] == "baseline continent test-continent"
        }));
        assert!(json["streams"].as_array().unwrap().iter().any(|stream| {
            stream["node_id"] == "us-1"
                && stream["stream_id"] == 77
                && stream["stream_id_text"] == "77"
        }));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_alerts_on_stale_remote_streams() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        let mut remote =
            telemetry_snapshot_for_tests("us-1", "us-east", "na", 37.4, -78.6, Vec::new(), 1);
        remote.streams = vec![StreamTelemetry {
            node_id: "us-1".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(4),
            latest_local_part_bytes: Some(2048),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(6_000),
            latest_mesh_part: Some(4),
            bytes_received: 8192,
            datagrams_received: 4,
            last_ingest_age_ms: Some(6_000),
            stale_threshold_ms: Some(5_000),
        }];
        telemetry.ingest_snapshot(remote).await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let snapshot = router.mesh_api_snapshot().await;

        assert!(snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "mesh_stream_stale"
                && alert.node_id.as_deref() == Some("us-1")
                && alert.stream_id_text.as_deref() == Some("77")));
        assert!(snapshot.streams.iter().any(|stream| {
            stream.node_id == "us-1"
                && stream.stream_id == 77
                && stream.last_ingest_age_ms == Some(6_000)
                && stream.stale_threshold_ms == Some(5_000)
        }));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_protocol_snapshot_and_sse_expose_mesh_state() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        assert!(router.is_streaming(MESH_EVENTS_PATH));

        let response = router.mesh_protocol_response_from_bytes(b"snapshot").await;
        assert!(response.ok);
        assert!(response.snapshot.is_some());

        let event = router.mesh_sse_event().await.unwrap();
        let event = std::str::from_utf8(&event).unwrap();
        assert!(event.starts_with("event: mesh\n"));
        assert!(event.contains("\"node_id\":\"test-node\""));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_protocol_warm_stream_executes_control_request() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        let response = router
            .mesh_protocol_response_from_bytes(
                br#"{"type":"warm_stream","stream_id":88,"region":"test-region"}"#,
            )
            .await;

        assert!(response.ok);
        assert!(cache.chunk_cache.get_stream_idx(88).await.is_some());
        assert_eq!(response.command.unwrap().kind, ControlKind::WarmStream);
        assert!(router
            .control
            .recent()
            .await
            .iter()
            .any(|command| command.kind == ControlKind::ReplicaRequest));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn raw_tcp_mesh_protocol_handles_snapshot_and_media_frames() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let mut input = Vec::new();
        push_raw_mesh_frame(&mut input, b"snapshot");

        let metadata = MediaFrameMetadata {
            duration_ms: 25,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(93, 4, 800, MediaCodec::Opus)
        };
        let request = serialized_media_access_unit_for_tests(metadata, b"raw-tcp-opus-frame");
        push_raw_mesh_frame(&mut input, &request);

        let (stream, written) = MemoryRawStream::new(input);
        tokio::time::timeout(
            Duration::from_secs(2),
            router.handle_stream(Box::new(stream), false),
        )
        .await
        .unwrap()
        .unwrap();
        let output = written.lock().unwrap().clone();
        let mut output = output.as_slice();

        let response = pop_raw_mesh_frame(&mut output);
        let json: serde_json::Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(json["response_type"], "snapshot");
        assert_eq!(json["snapshot"]["node"]["node_id"], "test-node");

        let response = pop_raw_mesh_frame(&mut output);
        let json: serde_json::Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(json["response_type"], "media_access_unit");
        assert_eq!(json["media_access_unit"]["stream_id"], 93);
        assert_eq!(json["media_access_unit"]["sequence"], 4);
        let unit = cache.get_media_access_unit(93, 4).await.unwrap();
        assert_eq!(
            unit.serialized.slice(MEDIA_FRAME_HEADER_LEN..),
            Bytes::from_static(b"raw-tcp-opus-frame")
        );
        assert!(output.is_empty());
        mesh.shutdown();
    }

    #[tokio::test]
    async fn webtransport_binary_media_request_ingests_access_unit() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let metadata = MediaFrameMetadata {
            duration_ms: 20,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(91, 2, 400, MediaCodec::Opus)
        };
        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: b"webtransport-opus-frame".len() as u32,
            fragment_offset: 0,
        };
        let mut request = vec![0; MEDIA_FRAME_HEADER_LEN];
        header.encode(&mut request[..]).unwrap();
        request.extend_from_slice(b"webtransport-opus-frame");

        let response = router
            .webtransport_response_from_bytes(Bytes::from(request))
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&response).unwrap();

        assert_eq!(json["response_type"], "media_access_unit");
        assert_eq!(json["media_access_unit"]["stream_id"], 91);
        assert_eq!(json["media_access_unit"]["sequence"], 2);
        let unit = cache.get_media_access_unit(91, 2).await.unwrap();
        assert_eq!(unit.metadata.codec, MediaCodec::Opus);
        assert_eq!(
            unit.serialized.slice(MEDIA_FRAME_HEADER_LEN..),
            Bytes::from_static(b"webtransport-opus-frame")
        );
        mesh.shutdown();
    }

    #[test]
    fn webtransport_media_decoder_accepts_stream_prefixed_raptorq_datagrams() {
        use raptorq_datagram_fec::{MediaFecEncoder, MediaFrame};
        use raptorq_fec_transport::{webtransport_subscription_datagram, FecDatagramEncoder};

        let mut metadata = MediaFrameMetadata::new(77, 9, 1200, MediaCodec::H264);
        metadata.duration_ms = 33;
        metadata.flags = MediaFrameFlags::keyframe();

        let transport = FecDatagramEncoder::webtransport_with_stream_prefix(77);
        let mut media_encoder = MediaFecEncoder::default();
        let encoded = transport
            .encode_media_frame(
                &mut media_encoder,
                MediaFrame {
                    metadata,
                    payload: b"prefixed-h264-access-unit",
                },
            )
            .unwrap();

        let mut decoder = WebTransportMediaDecoder::new();
        assert!(decoder
            .push_datagram(&webtransport_subscription_datagram(77))
            .unwrap()
            .is_none());

        let mut decoded = None;
        for datagram in encoded.datagrams {
            if let Some(frame) = decoder.push_datagram(&datagram).unwrap() {
                decoded = Some(frame);
            }
        }

        let frame = decoded.unwrap();
        assert_eq!(frame.metadata.stream_id, 77);
        assert_eq!(frame.metadata.sequence, 9);
        assert_eq!(frame.metadata.codec, MediaCodec::H264);
        assert!(frame.metadata.flags.is_keyframe());
        assert_eq!(frame.payload, b"prefixed-h264-access-unit");
    }

    #[tokio::test]
    async fn websocket_binary_media_request_ingests_access_unit() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let metadata = MediaFrameMetadata {
            duration_ms: 33,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(92, 3, 600, MediaCodec::Opus)
        };
        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: b"websocket-opus-frame".len() as u32,
            fragment_offset: 0,
        };
        let mut request = vec![0; MEDIA_FRAME_HEADER_LEN];
        header.encode(&mut request[..]).unwrap();
        request.extend_from_slice(b"websocket-opus-frame");

        let response = router
            .binary_mesh_response_from_bytes(Bytes::from(request))
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&response).unwrap();

        assert_eq!(json["response_type"], "media_access_unit");
        assert_eq!(json["media_access_unit"]["stream_id"], 92);
        assert_eq!(json["media_access_unit"]["sequence"], 3);
        let unit = cache.get_media_access_unit(92, 3).await.unwrap();
        assert_eq!(unit.metadata.codec, MediaCodec::Opus);
        assert_eq!(
            unit.serialized.slice(MEDIA_FRAME_HEADER_LEN..),
            Bytes::from_static(b"websocket-opus-frame")
        );
        mesh.shutdown();
    }

    #[tokio::test]
    async fn local_control_publishes_avmc_when_feed_is_available() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let (tx, mut rx) = mpsc::channel(1);
        router.dispatch.set_sender(tx).await;

        let command = router
            .execute_control(
                ControlKind::WarmStream,
                ControlRequest {
                    node_id: None,
                    region: Some("test-region".into()),
                    stream_id: Some(91),
                },
            )
            .await;

        assert!(command.status.contains("published AVMC control"));
        assert_eq!(command.target_text, "region test-region / stream 91");
        let command_json = serde_json::to_value(&command).unwrap();
        assert_eq!(
            command_json["target_text"],
            "region test-region / stream 91"
        );
        assert!(command_json["created_unix_ms"].as_u64().unwrap() > 0);
        let message = rx.recv().await.unwrap();
        assert_eq!(message.tag(), CONTROL_TAG);
        let envelope: ControlEnvelope = serde_json::from_slice(&message.data()[0]).unwrap();
        assert_eq!(envelope.id, command.id);
        assert_eq!(envelope.origin_node_id, "test-node");
        assert_eq!(envelope.kind, ControlKind::WarmStream);
        mesh.shutdown();
    }

    #[tokio::test]
    async fn tcp_changes_feed_carries_avmt_and_avmc_between_nodes() {
        use tokio::time::timeout;

        let (cert, key) = tls_pair_for_tests();
        let source_cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let collector_cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let source_mesh = mesh_handle_for_tests(Arc::clone(&source_cache.chunk_cache)).await;
        let collector_mesh = mesh_handle_for_tests(Arc::clone(&collector_cache.chunk_cache)).await;

        let source_router = app_router_for_tests_with_node(
            Arc::clone(&source_cache),
            Arc::clone(&source_mesh),
            mesh_node_for_tests("eu-source", "eu-west", "eu", 51.5, -0.1),
        );
        let collector_router = app_router_for_tests_with_node(
            Arc::clone(&collector_cache),
            Arc::clone(&collector_mesh),
            mesh_node_for_tests("jp-edge", "jp-east", "apac", 35.7, 139.7),
        );

        let telemetry_bind = unused_tcp_loopback_addr();
        let (publisher_shutdown_tx, publisher_shutdown_rx) = watch::channel(());
        let telemetry_runtime = start_telemetry_feed(
            telemetry_bind,
            Ipv4Addr::LOCALHOST,
            cert.clone(),
            key,
            50,
            Arc::clone(&source_cache),
            Arc::clone(&source_mesh),
            source_router.node.clone(),
            source_router.replication_policy.clone(),
            source_router.control.clone(),
            source_router.lifecycle.clone(),
            source_router.dispatch.clone(),
            source_router.playback_base_url.clone(),
            source_router.edge_load.clone(),
            publisher_shutdown_rx,
        )
        .await
        .unwrap();

        let (collector_shutdown_tx, collector_shutdown_rx) = watch::channel(());
        let mut collector_task = {
            let collector_router = collector_router.clone();
            let cert = cert.clone();
            tokio::spawn(async move {
                connect_telemetry_peer(
                    telemetry_bind,
                    "local.wavey.ai",
                    &cert,
                    collector_router,
                    TelemetryPeerMonitor::new(&[telemetry_bind]),
                    collector_shutdown_rx,
                )
                .await
            })
        };

        timeout(Duration::from_secs(3), async {
            loop {
                if collector_task.is_finished() {
                    let result = (&mut collector_task).await;
                    panic!("telemetry collector exited early: {result:?}");
                }
                let snapshot = collector_router.mesh_api_snapshot().await;
                if snapshot
                    .nodes
                    .iter()
                    .any(|node| node.node_id == "eu-source")
                {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();

        let command = source_router
            .execute_control(
                ControlKind::WarmStream,
                ControlRequest {
                    node_id: None,
                    region: Some("jp-east".into()),
                    stream_id: Some(77),
                },
            )
            .await;
        assert!(command.status.contains("published AVMC control"));

        timeout(Duration::from_secs(3), async {
            loop {
                if collector_cache
                    .chunk_cache
                    .get_stream_idx(77)
                    .await
                    .is_some()
                {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();

        assert!(collector_router
            .control
            .recent()
            .await
            .iter()
            .any(|command| {
                command.kind == ControlKind::WarmStream
                    && command.status.contains("received from eu-source")
            }));

        let _ = collector_shutdown_tx.send(());
        let _ = publisher_shutdown_tx.send(());
        let _ = telemetry_runtime.shutdown_tx.send(());
        timeout(Duration::from_secs(2), &mut collector_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), telemetry_runtime.finished_rx)
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), telemetry_runtime.publisher_task)
            .await
            .unwrap()
            .unwrap();
        source_mesh.shutdown();
        collector_mesh.shutdown();
    }

    #[tokio::test]
    async fn provision_node_control_runs_configured_executor() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests_with_provision(
            Arc::clone(&cache),
            Arc::clone(&mesh),
            ProvisionExecutor::new(
                Some(
                    "test \"$AV_MESH_PROVISION_REGION\" = eu-test && test \"$AV_MESH_PROVISION_NODE_ID\" = eu-test-2 && printf provision-ok"
                        .into(),
                ),
                Duration::from_secs(1),
            ),
        );

        let command = router
            .execute_control(
                ControlKind::ProvisionNode,
                ControlRequest {
                    node_id: Some("eu-test-2".into()),
                    region: Some("eu-test".into()),
                    stream_id: None,
                },
            )
            .await;

        assert!(command.status.contains("local provision executed"));
        assert!(command.status.contains("provision-ok"));
        mesh.shutdown();
    }

    #[cfg(feature = "linode-provisioner")]
    #[tokio::test]
    async fn linode_provision_reports_missing_env_without_network() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let provision = ProvisionExecutor::new(None, Duration::from_secs(1)).with_linode(Some(
            LinodeProvisionConfig {
                token_env: "AV_MESH_TEST_MISSING_LINODE_TOKEN".into(),
                pub_key_env: "AV_MESH_TEST_MISSING_LINODE_PUB_KEY".into(),
                image_id: "linode/arch".into(),
                instance_type: "g6-dedicated-2".into(),
                domain_id: 2_958_920,
                vlan_tag: "avmesh".into(),
                region_map: BTreeMap::from([("uk".into(), "gb-lon".into())]),
            },
        ));
        let router =
            app_router_for_tests_with_provision(Arc::clone(&cache), Arc::clone(&mesh), provision);

        let status = router.mesh_api_snapshot().await.orchestration.provision;
        assert!(status.enabled);
        assert_eq!(status.backends, vec!["linode"]);
        assert_eq!(status.backend_statuses.len(), 1);
        assert_eq!(status.backend_statuses[0].name, "linode");
        assert_eq!(status.backend_statuses[0].state, "blocked");
        assert!(status.backend_statuses[0]
            .details
            .iter()
            .any(|detail| detail.contains("AV_MESH_TEST_MISSING_LINODE_TOKEN missing")));
        assert!(router
            .mesh_api_snapshot()
            .await
            .alerts
            .iter()
            .any(|alert| alert.code == "provision_backend_blocked"));

        let command = router
            .execute_control(
                ControlKind::ProvisionNode,
                ControlRequest {
                    node_id: Some("uk-test-2".into()),
                    region: Some("uk".into()),
                    stream_id: None,
                },
            )
            .await;

        assert!(command
            .status
            .contains("local linode provision skipped: missing AV_MESH_TEST_MISSING_LINODE_TOKEN"));
        mesh.shutdown();
    }

    #[cfg(feature = "linode-provisioner")]
    #[test]
    fn linode_provision_status_reports_private_subnet_details() {
        let result = linode::ScaleUpResult {
            instance_id: 42,
            label: "gb-lon-test".into(),
            public_ipv4: "203.0.113.10".into(),
            private_ipam_address: "10.0.0.5/24".into(),
            vlan_label: "avmesh".into(),
            dns_name: Some("avmesh-gb-lon-1".into()),
            region_code: "gb-lon".into(),
            linode_region: "gb-lon".into(),
        };

        let status = format_linode_provision_result("uk", &result);

        assert!(status.contains("requested_region=uk"));
        assert!(status.contains("instance_id=42"));
        assert!(status.contains("private_ipam=10.0.0.5/24"));
        assert!(status.contains("vlan=avmesh"));
        assert!(status.contains("dns=avmesh-gb-lon-1"));
    }

    #[tokio::test]
    async fn warm_stream_control_dispatches_selected_nodes_from_telemetry() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        telemetry
            .ingest_snapshot(telemetry_snapshot_for_tests(
                "jp-edge-1",
                "jp-east",
                "apac",
                35.6,
                139.6,
                Vec::new(),
                77,
            ))
            .await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let (tx, mut rx) = mpsc::channel(1);
        router.dispatch.set_sender(tx).await;

        let command = router
            .execute_control(
                ControlKind::WarmStream,
                ControlRequest {
                    node_id: None,
                    region: Some("jp-east".into()),
                    stream_id: Some(77),
                },
            )
            .await;

        assert!(command
            .status
            .contains("published AVMC control to jp-edge-1"));
        let message = rx.recv().await.unwrap();
        let envelope: ControlEnvelope = serde_json::from_slice(&message.data()[0]).unwrap();
        assert_eq!(envelope.target_node_ids, vec!["jp-edge-1"]);
        assert_eq!(envelope.request.region.as_deref(), Some("jp-east"));
        assert!(cache.chunk_cache.get_stream_idx(77).await.is_none());
        mesh.shutdown();
    }

    #[tokio::test]
    async fn local_global_distribution_scenario_uses_avmt_and_avmc() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let policy = ReplicationPolicy {
            baseline_per_region: 1,
            baseline_per_continent: 1,
            min_mirror_distance_km: 700.0,
            max_new_replicas_per_plan: 16,
            ..ReplicationPolicy::default()
        };
        let router = app_router_for_tests_with_policy_and_telemetry(
            Arc::clone(&cache),
            Arc::clone(&mesh),
            policy,
            TelemetryAggregator::default(),
        );

        let mut eu_source =
            telemetry_snapshot_for_tests("eu-source", "eu-west", "eu", 51.5, -0.1, Vec::new(), 1);
        eu_source.mesh_addr = Some("10.0.0.10:9100".into());
        eu_source.peers = vec![PeerSnapshot {
            addr: "10.0.0.22:9100".into(),
            state: "discovered".into(),
        }];
        eu_source.streams = vec![StreamTelemetry {
            node_id: "eu-source".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(8),
            latest_local_part_bytes: Some(16_384),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(250),
            latest_mesh_part: Some(8),
            bytes_received: 262_144,
            datagrams_received: 128,
            last_ingest_age_ms: Some(250),
            stale_threshold_ms: Some(5_000),
        }];

        let mut na_edge =
            telemetry_snapshot_for_tests("na-edge", "us-east", "na", 37.4, -78.6, Vec::new(), 1);
        na_edge.mesh_addr = Some("10.0.0.20:9100".into());

        let mut jp_full = telemetry_snapshot_for_tests(
            "jp-full",
            "jp-east",
            "apac",
            35.60,
            139.60,
            Vec::new(),
            1,
        );
        jp_full.mesh_addr = Some("10.0.0.21:9100".into());
        jp_full.node.used_storage_bytes = jp_full.node.total_storage_bytes.saturating_sub(1_000);

        let mut jp_healthy = telemetry_snapshot_for_tests(
            "jp-healthy",
            "jp-east",
            "apac",
            35.70,
            139.75,
            vec![PeerSnapshot {
                addr: "10.0.0.10:9100".into(),
                state: "discovered".into(),
            }],
            1,
        );
        jp_healthy.mesh_addr = Some("10.0.0.22:9100".into());
        jp_healthy.node.used_storage_bytes = 50_000;
        jp_healthy.node.egress_capacity_bps = 20_000_000_000;

        for snapshot in [eu_source, na_edge, jp_full, jp_healthy] {
            assert!(router
                .ingest_tcp_changes_payload(TcpChangesPayload {
                    tag: TELEMETRY_TAG,
                    val: Bytes::from(serde_json::to_vec(&snapshot).unwrap()),
                })
                .await
                .unwrap());
        }

        let snapshot = router.mesh_api_snapshot().await;
        assert_eq!(snapshot.aggregate.node_count, 5);
        assert!(snapshot.connections.iter().any(|connection| {
            connection.source_node_id == "jp-healthy"
                && connection.target_addr == "10.0.0.10:9100"
                && connection.target_node_id.as_deref() == Some("eu-source")
        }));

        let stream_77_targets = snapshot
            .planned_replicas
            .iter()
            .filter(|placement| placement.stream_id == 77)
            .map(|placement| placement.target_node_id.as_str())
            .collect::<HashSet<_>>();
        assert!(stream_77_targets.contains("jp-healthy"));
        assert!(!stream_77_targets.contains("jp-full"));

        let (tx, mut rx) = mpsc::channel(1);
        router.dispatch.set_sender(tx).await;
        let command = router
            .execute_control(
                ControlKind::WarmStream,
                ControlRequest {
                    node_id: None,
                    region: Some("jp-east".into()),
                    stream_id: Some(77),
                },
            )
            .await;
        assert!(command.status.contains("published AVMC control to"));

        let message = rx.recv().await.unwrap();
        assert_eq!(message.tag(), CONTROL_TAG);
        let envelope: ControlEnvelope = serde_json::from_slice(&message.data()[0]).unwrap();
        assert_eq!(envelope.kind, ControlKind::WarmStream);
        assert_eq!(envelope.request.stream_id, Some(77));
        assert_eq!(envelope.request.region.as_deref(), Some("jp-east"));
        assert_eq!(envelope.target_node_ids, vec!["jp-full", "jp-healthy"]);
        assert!(cache.chunk_cache.get_stream_idx(77).await.is_none());
        mesh.shutdown();
    }

    #[tokio::test]
    async fn remote_avmc_warm_stream_runs_on_matching_region() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let envelope = ControlEnvelope {
            id: 12,
            origin_node_id: "central".into(),
            kind: ControlKind::WarmStream,
            request: ControlRequest {
                node_id: None,
                region: Some("test-region".into()),
                stream_id: Some(92),
            },
            target_node_ids: Vec::new(),
        };
        let payload = TcpChangesPayload {
            tag: CONTROL_TAG,
            val: Bytes::from(serde_json::to_vec(&envelope).unwrap()),
        };

        assert!(router.ingest_tcp_changes_payload(payload).await.unwrap());
        assert!(cache.chunk_cache.get_stream_idx(92).await.is_some());
        assert!(router.control.recent().await.iter().any(|command| {
            command.kind == ControlKind::WarmStream && command.status.contains("central")
        }));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn remote_avmc_ignores_non_matching_targets() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let envelope = ControlEnvelope {
            id: 13,
            origin_node_id: "central".into(),
            kind: ControlKind::WarmStream,
            request: ControlRequest {
                node_id: Some("other-node".into()),
                region: Some("test-region".into()),
                stream_id: Some(93),
            },
            target_node_ids: Vec::new(),
        };
        let payload = TcpChangesPayload {
            tag: CONTROL_TAG,
            val: Bytes::from(serde_json::to_vec(&envelope).unwrap()),
        };

        assert!(!router.ingest_tcp_changes_payload(payload).await.unwrap());
        assert!(cache.chunk_cache.get_stream_idx(93).await.is_none());
        mesh.shutdown();
    }

    #[tokio::test]
    async fn close_node_control_marks_local_node_draining() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        let command = router
            .execute_control(
                ControlKind::CloseNode,
                ControlRequest {
                    node_id: Some("test-node".into()),
                    region: None,
                    stream_id: None,
                },
            )
            .await;

        assert!(command.status.contains("draining"));
        assert!(router.mesh_api_snapshot().await.node.draining);
        mesh.shutdown();
    }

    #[tokio::test]
    async fn warm_stream_control_creates_stream_and_records_command() {
        const SNOWFLAKE_STREAM_ID: u64 = 9_007_199_254_741_993;

        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/control/warm-stream")
            .body(())
            .unwrap();
        let body: BodyStream = Box::pin(futures_util::stream::iter(vec![Ok(Bytes::from_static(
            br#"{"stream_id":"9007199254741993","region":"test-region"}"#,
        ))]));

        let response = router.route_body(req, body).await.unwrap();

        assert_eq!(response.status, StatusCode::ACCEPTED);
        assert!(cache
            .chunk_cache
            .get_stream_idx(SNOWFLAKE_STREAM_ID)
            .await
            .is_some());
        let commands = router.control.recent().await;
        assert_eq!(commands.len(), 2);
        assert!(commands
            .iter()
            .any(|command| command.kind == ControlKind::WarmStream
                && command.stream_id == Some(SNOWFLAKE_STREAM_ID)
                && command.stream_id_text.as_deref() == Some("9007199254741993")));
        assert!(commands.iter().any(|command| {
            command.kind == ControlKind::ReplicaRequest
                && command.stream_id == Some(SNOWFLAKE_STREAM_ID)
                && command.stream_id_text.as_deref() == Some("9007199254741993")
        }));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn warm_stream_control_dispatches_regional_target() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let (tx, mut rx) = mpsc::channel(1);
        router.dispatch.set_sender(tx).await;
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/control/warm-stream")
            .body(())
            .unwrap();
        let body: BodyStream = Box::pin(futures_util::stream::iter(vec![Ok(Bytes::from_static(
            br#"{"stream_id":77,"region":"jp-east"}"#,
        ))]));

        let response = router.route_body(req, body).await.unwrap();

        assert_eq!(response.status, StatusCode::ACCEPTED);
        let commands = router.control.recent().await;
        assert!(commands
            .iter()
            .any(|command| command.kind == ControlKind::WarmStream
                && command.status.contains("published AVMC control")));
        assert!(cache.chunk_cache.get_stream_idx(77).await.is_none());
        let message = rx.recv().await.unwrap();
        assert_eq!(message.tag(), CONTROL_TAG);
        let envelope: ControlEnvelope = serde_json::from_slice(&message.data()[0]).unwrap();
        assert_eq!(envelope.kind, ControlKind::WarmStream);
        assert_eq!(envelope.request.region.as_deref(), Some("jp-east"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn baseline_planner_fetches_when_local_node_is_selected_replica() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-baseline", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("test-node", "test-region", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a.push_payload(b"baseline-part").await.unwrap();
        cache_a.rotate_if_due(true).await.unwrap();

        let telemetry = TelemetryAggregator::default();
        telemetry
            .ingest_snapshot(telemetry_snapshot_for_tests(
                "uk-baseline",
                "uk",
                "eu",
                51.5,
                -0.1,
                Vec::new(),
                1,
            ))
            .await;
        let router = app_router_for_tests_with_telemetry(
            Arc::clone(&cache_b),
            Arc::clone(&mesh_b),
            telemetry,
        );

        assert!(
            router
                .request_planned_local_replica(1, "baseline-replication")
                .await
        );

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) = cache_b.get_part_blocking(0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"baseline-part"));
        assert!(router.control.recent().await.iter().any(|command| {
            command.kind == ControlKind::ReplicaRequest
                && command.status.contains("baseline-replication")
        }));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn baseline_planner_fetches_non_default_stream_from_telemetry() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-baseline", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("test-node", "test-region", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"baseline-stream-77"))
            .await
            .unwrap();

        let telemetry = TelemetryAggregator::default();
        let mut remote =
            telemetry_snapshot_for_tests("uk-baseline", "uk", "eu", 51.5, -0.1, Vec::new(), 1);
        remote.streams = vec![StreamTelemetry {
            node_id: "uk-baseline".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(0),
            latest_local_part_bytes: Some(b"baseline-stream-77".len()),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(250),
            latest_mesh_part: Some(0),
            bytes_received: b"baseline-stream-77".len() as u64,
            datagrams_received: 1,
            last_ingest_age_ms: Some(250),
            stale_threshold_ms: Some(5_000),
        }];
        telemetry.ingest_snapshot(remote).await;

        let router = app_router_for_tests_with_telemetry(
            Arc::clone(&cache_b),
            Arc::clone(&mesh_b),
            telemetry,
        );

        let requested = router
            .request_planned_local_replicas("baseline-replication")
            .await;
        assert_eq!(requested, vec![77]);

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) = cache_b.chunk_cache.get_for_stream_id(77, 0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"baseline-stream-77"));
        assert!(router.control.recent().await.iter().any(|command| {
            command.kind == ControlKind::ReplicaRequest && command.stream_id == Some(77)
        }));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn mesh_byte_slots_replicate_to_peer_cache() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mesh_a = CacheMesh::new(
            Arc::clone(&cache_a.chunk_cache),
            CacheMeshConfig::new("uk-http-test", "uk", mesh_a_addr).with_peer(mesh_b_addr),
        )
        .start()
        .await
        .unwrap();
        let mesh_b = CacheMesh::new(
            Arc::clone(&cache_b.chunk_cache),
            CacheMeshConfig::new("us-http-test", "us", mesh_b_addr).with_peer(mesh_a_addr),
        )
        .start()
        .await
        .unwrap();

        cache_a
            .chunk_cache
            .add_for_stream_id(1, 0, Bytes::from_static(b"mesh-byte-part"))
            .await
            .unwrap();

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

        assert_eq!(bytes, Bytes::from_static(b"mesh-byte-part"));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn media_access_unit_demand_replicates_non_ts_stream_slot() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-media", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("us-media", "us", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = Arc::new(
            CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
                .start()
                .await
                .unwrap(),
        );
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );
        let router_b = app_router_for_tests(Arc::clone(&cache_b), Arc::clone(&mesh_b));

        let metadata = MediaFrameMetadata {
            duration_ms: 20,
            ..MediaFrameMetadata::new(88, 0, 900, MediaCodec::Opus)
        };
        cache_a
            .add_media_access_unit(metadata, Bytes::from_static(b"opus-frame"))
            .await
            .unwrap();

        let req = Request::builder()
            .method(Method::GET)
            .uri("/media/88/unit/0")
            .body(())
            .unwrap();
        let response = router_b.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::NOT_FOUND);

        let unit = timeout(Duration::from_secs(3), async {
            loop {
                if let Some(unit) = cache_b.get_media_access_unit(88, 0).await {
                    break unit;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(unit.metadata.codec, MediaCodec::Opus);
        assert_eq!(&unit.serialized[MEDIA_FRAME_HEADER_LEN..], b"opus-frame");
        let req = Request::builder()
            .method(Method::GET)
            .uri("/media/88/unit/0")
            .body(())
            .unwrap();
        let response = router_b.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.body.unwrap().slice(MEDIA_FRAME_HEADER_LEN..),
            Bytes::from_static(b"opus-frame")
        );
        assert!(router_b
            .control
            .recent()
            .await
            .iter()
            .any(|command| command.kind == ControlKind::ReplicaRequest));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn playlist_demand_requests_mesh_replica() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-demand", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("jp-demand", "jp", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a.push_payload(b"playlist-demand-part").await.unwrap();
        cache_a.rotate_if_due(true).await.unwrap();

        let router = app_router_for_tests(Arc::clone(&cache_b), Arc::clone(&mesh_b));
        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/stream.m3u8")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) = cache_b.get_part_blocking(0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"playlist-demand-part"));
        assert!(router
            .control
            .recent()
            .await
            .iter()
            .any(|command| command.kind == ControlKind::ReplicaRequest));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn stream_specific_playlist_can_read_playlist_id_from_any_mesh_node() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-playlist", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("jp-playlist", "jp", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"playlist-77-part0"))
            .await
            .unwrap();
        cache_a
            .chunk_cache
            .add_for_stream_id(77, 1, Bytes::from_static(b"playlist-77-part1"))
            .await
            .unwrap();

        let router = app_router_for_tests(Arc::clone(&cache_b), Arc::clone(&mesh_b));
        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/stream.m3u8")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);

        timeout(Duration::from_secs(3), async {
            loop {
                if cache_b.chunk_cache.get_for_stream_id(77, 1).await.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/stream.m3u8")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        let playlist = String::from_utf8(response.body.unwrap().to_vec()).unwrap();
        assert!(playlist.contains("part0.ts"));
        assert!(playlist.contains("part1.ts"));
        assert!(playlist.contains("seg0.ts"));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/part0.ts")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.body.unwrap(),
            Bytes::from_static(b"playlist-77-part0")
        );

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/seg0.ts")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.body.unwrap(),
            Bytes::from_static(b"playlist-77-part0playlist-77-part1")
        );

        assert!(router.control.recent().await.iter().any(|command| {
            command.kind == ControlKind::ReplicaRequest && command.stream_id == Some(77)
        }));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn llhls_tail_can_read_playlist_id_from_any_mesh_node() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-llhls", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("jp-llhls", "jp", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"llhls-stream-77"))
            .await
            .unwrap();

        let router = app_router_for_tests(Arc::clone(&cache_b), Arc::clone(&mesh_b));
        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/tail?mode=part")
            .body(())
            .unwrap();
        let first_response = router.route(req).await.unwrap();
        assert!(
            first_response.status == StatusCode::NO_CONTENT
                || first_response.status == StatusCode::OK
        );

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) = cache_b.chunk_cache.get_for_stream_id(77, 0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"llhls-stream-77"));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/tail?mode=part")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.body.unwrap(),
            Bytes::from_static(b"llhls-stream-77")
        );
        assert_eq!(
            response
                .headers
                .iter()
                .find(|(name, _)| name == "x-sequence")
                .map(|(_, value)| value.as_str()),
            Some("0")
        );
        assert_eq!(
            response
                .headers
                .iter()
                .find(|(name, _)| name == "x-av-stream-id")
                .map(|(_, value)| value.as_str()),
            Some("77")
        );

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/tail?mode=part&after=0")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::NO_CONTENT);

        let snapshot = router.mesh_api_snapshot().await;
        let edge = snapshot
            .edge_services
            .iter()
            .find(|service| service.node_id == "test-node")
            .unwrap();
        assert!(edge.llhls_tail_requests >= 3);
        assert!(edge.requests_served >= 3);
        assert!(edge.bytes_served >= b"llhls-stream-77".len() as u64);
        assert!(router.control.recent().await.iter().any(|command| {
            command.kind == ControlKind::ReplicaRequest && command.stream_id == Some(77)
        }));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn warm_stream_control_requests_mesh_replica() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-warm", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("jp-warm", "jp", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"warm-stream-part"))
            .await
            .unwrap();

        let router = app_router_for_tests(Arc::clone(&cache_b), Arc::clone(&mesh_b));
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/control/warm-stream")
            .body(())
            .unwrap();
        let body: BodyStream = Box::pin(futures_util::stream::iter(vec![Ok(Bytes::from_static(
            br#"{"stream_id":77,"region":"test-region"}"#,
        ))]));

        let response = router.route_body(req, body).await.unwrap();
        assert_eq!(response.status, StatusCode::ACCEPTED);

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, hash)) = cache_b.chunk_cache.get_for_stream_id(77, 0).await {
                    if hash != 0 || !bytes.is_empty() {
                        break bytes;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"warm-stream-part"));
        let commands = router.control.recent().await;
        assert!(commands
            .iter()
            .any(|command| command.kind == ControlKind::WarmStream));
        assert!(commands
            .iter()
            .any(|command| command.kind == ControlKind::ReplicaRequest));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn udp_fec_ingest_writes_cache_parts() {
        use av_mesh::udp_fec::UdpFecSender;
        use tokio::time::timeout;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_fec_ingest(socket, Arc::clone(&cache), shutdown_rx));
        let mut sender = UdpFecSender::new(bind).await.unwrap();

        sender.send(b"fec-part-0").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();

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

        assert_eq!(bytes, Bytes::from_static(b"fec-part-0"));
    }

    #[tokio::test]
    async fn udp_fec_ingest_writes_stream_prefixed_slots() {
        use raptorq_fec_transport::FecDatagramEncoder;
        use tokio::time::timeout;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_fec_ingest(socket, Arc::clone(&cache), shutdown_rx));
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stream_id = 77;
        let mut encoder = FecDatagramEncoder::webtransport_with_stream_prefix(stream_id);

        for datagram in encoder.encode_payload(b"prefixed-fmp4-or-bytes").unwrap() {
            sender.send_to(&datagram, bind).await.unwrap();
        }

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) = cache.get_part_for_stream_id(stream_id, 0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"prefixed-fmp4-or-bytes"));
        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn media_udp_fec_ingest_writes_access_unit_stream() {
        use raptorq_datagram_fec::{MediaFecEncoder, MediaFrame};
        use tokio::time::timeout;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_media_fec_ingest(
            socket,
            Arc::clone(&cache),
            shutdown_rx,
        ));

        let mut metadata = MediaFrameMetadata::new(66, 3, 777, MediaCodec::H264);
        metadata.duration_ms = 33;
        metadata.flags = MediaFrameFlags::keyframe();
        let mut encoder = MediaFecEncoder::default();
        let encoded = encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: b"fec-h264-access-unit",
            })
            .unwrap();

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for datagram in encoded.datagrams {
            sender.send_to(&datagram, bind).await.unwrap();
        }

        let unit = timeout(Duration::from_secs(3), async {
            loop {
                if let Some(unit) = cache.get_media_access_unit(66, 3).await {
                    break unit;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(unit.metadata.codec, MediaCodec::H264);
        assert!(unit.metadata.flags.is_keyframe());
        assert_eq!(unit.metadata.pts_ms, 777);
        assert_eq!(unit.metadata.duration_ms, 33);
        assert_eq!(
            unit.serialized.slice(MEDIA_FRAME_HEADER_LEN..),
            Bytes::from_static(b"fec-h264-access-unit")
        );

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn media_udp_fec_ingest_recovers_video_access_units_with_lost_datagrams() {
        use raptorq_datagram_fec::{MediaFecEncoder, MediaFrame, NetworkMetrics};
        use tokio::time::timeout;

        struct LossyFrame {
            sequence: u64,
            pts_ms: u64,
            payload_len: usize,
            flags: MediaFrameFlags,
            dropped_indexes: &'static [usize],
        }

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_media_fec_ingest(
            socket,
            Arc::clone(&cache),
            shutdown_rx,
        ));

        let mut encoder = MediaFecEncoder::default();
        encoder
            .controller_mut()
            .update_network_metrics(NetworkMetrics {
                loss_fraction: 0.08,
                rtt_ms: 70.0,
                jitter_ms: 25.0,
                queue_delay_ms: 20.0,
                available_bitrate_bps: Some(8_000_000),
            });
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let frames = [
            LossyFrame {
                sequence: 10,
                pts_ms: 1_000,
                payload_len: 40_000,
                flags: MediaFrameFlags::keyframe(),
                dropped_indexes: &[3, 4, 5, 6, 7, 8, 9, 10],
            },
            LossyFrame {
                sequence: 11,
                pts_ms: 1_016,
                payload_len: 18_000,
                flags: MediaFrameFlags::default(),
                dropped_indexes: &[2, 8, 12],
            },
            LossyFrame {
                sequence: 12,
                pts_ms: 1_032,
                payload_len: 18_000,
                flags: MediaFrameFlags::default(),
                dropped_indexes: &[1, 5],
            },
        ];

        for frame in &frames {
            let payload = deterministic_video_payload(frame.payload_len);
            let mut metadata =
                MediaFrameMetadata::new(66, frame.sequence, frame.pts_ms, MediaCodec::H264);
            metadata.duration_ms = 16;
            metadata.flags = frame.flags;
            let encoded = encoder
                .encode_frame(MediaFrame {
                    metadata,
                    payload: &payload,
                })
                .unwrap();
            assert!(
                encoded.decision.config.repair_symbols as usize >= frame.dropped_indexes.len(),
                "test loss must stay inside the repair budget for sequence {}",
                frame.sequence
            );

            for (index, datagram) in encoded.datagrams.iter().enumerate() {
                if frame.dropped_indexes.contains(&index) {
                    continue;
                }
                sender.send_to(datagram, bind).await.unwrap();
            }

            let unit = timeout(Duration::from_secs(3), async {
                loop {
                    if let Some(unit) = cache.get_media_access_unit(66, frame.sequence).await {
                        break unit;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();

            assert_eq!(unit.metadata.codec, MediaCodec::H264);
            assert_eq!(unit.metadata.sequence, frame.sequence);
            assert_eq!(unit.metadata.pts_ms, frame.pts_ms);
            assert_eq!(unit.metadata.duration_ms, 16);
            assert_eq!(
                &unit.serialized[MEDIA_FRAME_HEADER_LEN..],
                payload.as_slice()
            );
        }

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn media_udp_fec_ingest_recovers_multiblock_video_stream_with_reordered_loss() {
        use raptorq_datagram_fec::{
            DatagramFecHeader, EncodedMediaFrame, MediaFecEncoder, MediaFrame, NetworkMetrics,
        };
        use tokio::time::timeout;

        #[derive(Debug)]
        struct StreamFrame {
            sequence: u64,
            pts_ms: u64,
            payload_len: usize,
            flags: MediaFrameFlags,
            max_loss_per_block: usize,
        }

        #[derive(Debug)]
        struct ScheduledDatagram {
            ordinal: usize,
            delay_ms: u64,
            bytes: Vec<u8>,
        }

        fn bounded_source_loss(
            encoded: &EncodedMediaFrame,
            max_loss_per_block: usize,
        ) -> HashSet<usize> {
            let mut blocks = BTreeMap::<u32, (u16, Vec<usize>, usize)>::new();
            for (datagram_index, datagram) in encoded.datagrams.iter().enumerate() {
                let header =
                    DatagramFecHeader::decode(datagram).expect("decode FEC datagram header");
                let entry =
                    blocks
                        .entry(header.block_id)
                        .or_insert((header.source_symbols, Vec::new(), 0));
                assert_eq!(entry.0, header.source_symbols);
                if entry.2 < usize::from(header.source_symbols) {
                    entry.1.push(datagram_index);
                }
                entry.2 += 1;
            }

            let mut dropped = HashSet::new();
            assert_eq!(blocks.len(), usize::from(encoded.fragment_count));
            for (_block_id, (source_symbols, source_indices, datagram_count)) in blocks {
                let source_symbols = usize::from(source_symbols);
                let repair_symbols = datagram_count.saturating_sub(source_symbols);
                let drop_count = repair_symbols.min(max_loss_per_block);
                assert!(drop_count > 0, "test frame should have repair symbols");
                dropped.extend(source_indices.into_iter().take(drop_count));
            }
            dropped
        }

        fn reorder_delay_ms(ordinal: usize) -> u64 {
            match ordinal % 11 {
                0 => 8,
                3 => 5,
                7 => 2,
                _ => 0,
            }
        }

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 256).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_media_fec_ingest(
            socket,
            Arc::clone(&cache),
            shutdown_rx,
        ));

        let frames = [
            StreamFrame {
                sequence: 100,
                pts_ms: 2_000,
                payload_len: 96_000,
                flags: MediaFrameFlags::keyframe(),
                max_loss_per_block: 4,
            },
            StreamFrame {
                sequence: 101,
                pts_ms: 2_016,
                payload_len: 18_000,
                flags: MediaFrameFlags::default(),
                max_loss_per_block: 2,
            },
            StreamFrame {
                sequence: 102,
                pts_ms: 2_032,
                payload_len: 40_000,
                flags: MediaFrameFlags::keyframe(),
                max_loss_per_block: 3,
            },
            StreamFrame {
                sequence: 103,
                pts_ms: 2_048,
                payload_len: 9_000,
                flags: MediaFrameFlags::default(),
                max_loss_per_block: 1,
            },
        ];

        let mut encoder = MediaFecEncoder::default();
        encoder
            .controller_mut()
            .update_network_metrics(NetworkMetrics {
                loss_fraction: 0.08,
                rtt_ms: 70.0,
                jitter_ms: 25.0,
                queue_delay_ms: 20.0,
                available_bitrate_bps: Some(8_000_000),
            });
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut expected_payloads = BTreeMap::new();
        let mut scheduled = Vec::new();
        let mut ordinal = 0usize;
        let mut dropped_datagrams = 0usize;

        for frame in &frames {
            let payload = deterministic_video_payload(frame.payload_len);
            let mut metadata =
                MediaFrameMetadata::new(66, frame.sequence, frame.pts_ms, MediaCodec::H264);
            metadata.duration_ms = 16;
            metadata.flags = frame.flags;
            let encoded = encoder
                .encode_frame(MediaFrame {
                    metadata,
                    payload: &payload,
                })
                .unwrap();
            assert!(
                encoded.fragment_count > 1 || frame.payload_len < 64_000,
                "large access units should exercise multi-block frame reconstruction"
            );
            let dropped = bounded_source_loss(&encoded, frame.max_loss_per_block);
            dropped_datagrams += dropped.len();

            for (index, datagram) in encoded.datagrams.into_iter().enumerate() {
                if dropped.contains(&index) {
                    continue;
                }
                scheduled.push(ScheduledDatagram {
                    ordinal,
                    delay_ms: reorder_delay_ms(ordinal),
                    bytes: datagram,
                });
                ordinal += 1;
            }
            expected_payloads.insert(frame.sequence, payload);
        }

        assert!(
            dropped_datagrams >= 8,
            "test should exercise repeated datagram loss"
        );
        scheduled.sort_by_key(|datagram| (datagram.delay_ms, datagram.ordinal));
        for datagram in scheduled {
            if datagram.delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(datagram.delay_ms)).await;
            }
            sender.send_to(&datagram.bytes, bind).await.unwrap();
        }

        for frame in &frames {
            let unit = timeout(Duration::from_secs(3), async {
                loop {
                    if let Some(unit) = cache.get_media_access_unit(66, frame.sequence).await {
                        break unit;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let expected_payload = expected_payloads
                .get(&frame.sequence)
                .expect("expected payload for frame");

            assert_eq!(unit.metadata.codec, MediaCodec::H264);
            assert_eq!(unit.metadata.sequence, frame.sequence);
            assert_eq!(unit.metadata.pts_ms, frame.pts_ms);
            assert_eq!(unit.metadata.duration_ms, 16);
            assert_eq!(
                &unit.serialized[MEDIA_FRAME_HEADER_LEN..],
                expected_payload.as_slice()
            );
        }

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn raptorq_mesh_replicates_opaque_stream_slots() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(7, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(7, Duration::from_millis(500), 2, 6, 64).await;
        let mesh_a = CacheMesh::new(
            Arc::clone(&cache_a.chunk_cache),
            CacheMeshConfig::new("uk-opaque", "uk", mesh_a_addr).with_peer(mesh_b_addr),
        )
        .start()
        .await
        .unwrap();
        let mesh_b = CacheMesh::new(
            Arc::clone(&cache_b.chunk_cache),
            CacheMeshConfig::new("us-opaque", "us", mesh_b_addr).with_peer(mesh_a_addr),
        )
        .start()
        .await
        .unwrap();

        cache_a
            .chunk_cache
            .add_for_stream_id(7, 0, Bytes::from_static(b"raptorq-mesh-bytes-0"))
            .await
            .unwrap();

        let bytes = timeout(Duration::from_secs(5), async {
            loop {
                if let Some((bytes, _hash)) = cache_b.get_part_blocking(0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"raptorq-mesh-bytes-0"));
        assert!(cache_b.playlist().await.contains("part0.ts"));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    async fn mesh_handle_for_tests(cache: Arc<ChunkCache>) -> Arc<CacheMeshHandle> {
        let mesh = CacheMesh::new(
            cache,
            CacheMeshConfig::new("test-node", "test", unused_loopback_addr()),
        )
        .start()
        .await
        .unwrap();
        Arc::new(mesh)
    }

    fn app_router_for_tests(cache: Arc<LiveTsCache>, mesh: Arc<CacheMeshHandle>) -> AppRouter {
        app_router_for_tests_with_telemetry(cache, mesh, TelemetryAggregator::default())
    }

    fn app_router_for_tests_with_telemetry(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        telemetry: TelemetryAggregator,
    ) -> AppRouter {
        app_router_for_tests_with_policy_and_telemetry(
            cache,
            mesh,
            ReplicationPolicy::default(),
            telemetry,
        )
    }

    fn app_router_for_tests_with_policy_and_telemetry(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        replication_policy: ReplicationPolicy,
        telemetry: TelemetryAggregator,
    ) -> AppRouter {
        app_router_for_tests_with_policy_telemetry_and_provision(
            cache,
            mesh,
            replication_policy,
            telemetry,
            ProvisionExecutor::disabled(),
        )
    }

    fn app_router_for_tests_with_provision(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        provision: ProvisionExecutor,
    ) -> AppRouter {
        app_router_for_tests_with_policy_telemetry_and_provision(
            cache,
            mesh,
            ReplicationPolicy::default(),
            TelemetryAggregator::default(),
            provision,
        )
    }

    fn app_router_for_tests_with_node(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        node: MeshNode,
    ) -> AppRouter {
        app_router_for_tests_with_node_policy_telemetry_and_provision(
            cache,
            mesh,
            node,
            ReplicationPolicy::default(),
            TelemetryAggregator::default(),
            ProvisionExecutor::disabled(),
            TelemetryPeerMonitor::default(),
        )
    }

    fn app_router_for_tests_with_policy_telemetry_and_provision(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        replication_policy: ReplicationPolicy,
        telemetry: TelemetryAggregator,
        provision: ProvisionExecutor,
    ) -> AppRouter {
        app_router_for_tests_with_policy_telemetry_provision_and_monitor(
            cache,
            mesh,
            replication_policy,
            telemetry,
            provision,
            TelemetryPeerMonitor::default(),
        )
    }

    fn app_router_for_tests_with_telemetry_monitor(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        monitor: TelemetryPeerMonitor,
    ) -> AppRouter {
        app_router_for_tests_with_policy_telemetry_provision_and_monitor(
            cache,
            mesh,
            ReplicationPolicy::default(),
            TelemetryAggregator::default(),
            ProvisionExecutor::disabled(),
            monitor,
        )
    }

    fn app_router_for_tests_with_policy_telemetry_provision_and_monitor(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        replication_policy: ReplicationPolicy,
        telemetry: TelemetryAggregator,
        provision: ProvisionExecutor,
        monitor: TelemetryPeerMonitor,
    ) -> AppRouter {
        app_router_for_tests_with_node_policy_telemetry_and_provision(
            cache,
            mesh,
            mesh_node_for_tests("test-node", "test-region", "test-continent", 51.5, -0.1),
            replication_policy,
            telemetry,
            provision,
            monitor,
        )
    }

    fn app_router_for_tests_with_node_policy_telemetry_and_provision(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        node: MeshNode,
        replication_policy: ReplicationPolicy,
        telemetry: TelemetryAggregator,
        provision: ProvisionExecutor,
        monitor: TelemetryPeerMonitor,
    ) -> AppRouter {
        AppRouter::new(
            cache,
            mesh,
            node,
            replication_policy,
            ControlPlane::default(),
            ControlDispatch::default(),
            telemetry,
            DemandTracker::default(),
            NodeLifecycle::default(),
            Some("https://test-node.local/live".into()),
            EdgeLoad::default(),
            provision,
            monitor,
        )
    }

    fn mesh_node_for_tests(
        node_id: &str,
        region: &str,
        continent: &str,
        latitude: f64,
        longitude: f64,
    ) -> MeshNode {
        MeshNode {
            node_id: node_id.into(),
            region: region.into(),
            continent: continent.into(),
            latitude,
            longitude,
            total_storage_bytes: 1_000_000,
            used_storage_bytes: 0,
            egress_capacity_bps: 10_000_000_000,
            contributor_streams: 0,
            active_streams: 0,
            draining: false,
        }
    }

    fn telemetry_snapshot_for_tests(
        node_id: &str,
        region: &str,
        continent: &str,
        latitude: f64,
        longitude: f64,
        peers: Vec<PeerSnapshot>,
        stream_id: u64,
    ) -> MeshSnapshot {
        MeshSnapshot {
            updated_unix_ms: now_unix_ms(),
            node: MeshNode {
                node_id: node_id.into(),
                region: region.into(),
                continent: continent.into(),
                latitude,
                longitude,
                total_storage_bytes: 1_000_000,
                used_storage_bytes: 100_000,
                egress_capacity_bps: 10_000_000_000,
                contributor_streams: 1,
                active_streams: 1,
                draining: false,
            },
            mesh_addr: Some(node_id.into()),
            edge_service: None,
            peers,
            stream: StatsSnapshot {
                stream_id,
                stream_id_text: stream_id_text(stream_id),
                part_target_ms: 500,
                parts_per_segment: 2,
                window_parts: 6,
                datagrams_received: 10,
                bytes_received: 20_000,
                current_part_bytes: 0,
                latest_local_part: Some(1),
                latest_local_part_bytes: Some(1024),
                latest_local_part_duration_ms: Some(500),
                latest_mesh_part: Some(1),
                mesh_peers: Vec::new(),
                latest_local_part_age_ms: Some(10),
                last_ingest_age_ms: Some(10),
            },
            streams: Vec::new(),
            replication_policy: ReplicationPolicy::default(),
            recent_commands: Vec::new(),
        }
    }

    fn unused_loopback_addr() -> SocketAddr {
        static NEXT_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(24_000);

        loop {
            let port = NEXT_PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
            if std::net::UdpSocket::bind(addr).is_ok() {
                return addr;
            }
        }
    }

    fn unused_tcp_loopback_addr() -> SocketAddr {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.local_addr().unwrap()
    }

    fn tls_pair_for_tests() -> (String, String) {
        use base64::engine::general_purpose::STANDARD as base64_engine;
        use base64::Engine;

        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["local.wavey.ai".into()]).unwrap();
        (
            base64_engine.encode(cert.pem()),
            base64_engine.encode(key_pair.serialize_pem()),
        )
    }
}
