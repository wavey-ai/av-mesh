//! The compact client lane (low-latency wave 5, W1b): what a UDP listener
//! costs on the wire and at the edge, and that it loses nothing.
//!
//! The listener here does what `needletail-udp-client` does: it subscribes,
//! measures the loss of its path every 150 ms, asks for the repair (and the
//! compact plan) that the loss calls for, and asks for the parts it misses
//! again (`NACK`). Its path drops datagrams by a model of its own (random
//! loss, or Gilbert-Elliott bursts as netem's `gemodel` makes them) after
//! they are counted as received on the wire.
//!
//! Two edges: one fed by the fast plane (two forwarders' RelaySession
//! symbols, the fast client lane) and one fed by the cache mesh (two
//! distributors, cut-through and the cache-fed lane), as today's deploy
//! feeds them.
//!
//! `bytes_on_the_wire_per_listener` (ignored, a measurement) prints the
//! bytes per part and the bit rate of a listener, before (the RaptorQ lane
//! with the repair of before wave 5) and after (the compact lane), for 2.5 ms
//! Opus, 1 ms PCM and 4K, and the edge's processor time per listener. Run it
//! alone: `cargo test --release bytes_on_the_wire -- --ignored --nocapture
//! --test-threads 1`, with COMPACT_BENCH_LISTENERS, COMPACT_BENCH_SECONDS and
//! COMPACT_BENCH_FEED (fast, mesh or both).

use super::fast_lane::LaneEdge;
use super::fast_lease::loopback;
use super::*;
use av_mesh::client_compact::{CompactPlan, LegId};
use av_mesh::client_udp::{
    Control, ObjectKind as UdpObjectKind, ObjectReceiver, RepairPolicy, UNMEASURED_LOSS,
};
use media_object::{ClockConfidence, ClockTimestamp, StageTimestamp};
use playlists::mesh::{CacheMesh, CacheMeshConfig, CacheMeshRole as MeshRole, ReplicationMode};
use socket2::SockRef;
use std::sync::atomic::AtomicU64;
use tokio::time::timeout;

// ------------------------------------------------------------------ sources

/// What a contributor publishes, as av-contrib builds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Payload {
    /// 2.5 ms Opus at 256 kbit/s: an opaque part of a 16-byte SoundKit
    /// header and an 80-byte Opus packet.
    Opus,
    /// 1 ms PCM: an opaque part of a 16-byte header and 48 stereo 24-bit
    /// samples.
    Pcm,
    /// 1 ms Opus Custom at 320 kbit/s: an opaque part of a 16-byte SoundKit
    /// header and a 40-byte packet.
    Opus1,
    /// 4K at 25 Mbit/s and 60 frames a second: one fMP4 frame a part.
    Video4k,
}

impl Payload {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Opus => "Opus 2.5 ms 256 kbit/s",
            Self::Pcm => "PCM 1 ms 48 kHz 24-bit stereo",
            Self::Opus1 => "Opus 1 ms Custom 320 kbit/s",
            Self::Video4k => "4K 25 Mbit/s 60 fps",
        }
    }

    pub(super) fn part_us(self) -> u64 {
        match self {
            Self::Opus => 2_500,
            Self::Pcm | Self::Opus1 => 1_000,
            Self::Video4k => 16_667,
        }
    }

    /// The bytes of a part's media (the LL-HLS part).
    fn media_len(self) -> usize {
        match self {
            Self::Opus => 16 + 80,
            Self::Pcm => 16 + 288,
            Self::Opus1 => 16 + 40,
            Self::Video4k => 25_000_000 / 8 / 60,
        }
    }

    /// The codec's own rate, bits a second.
    fn codec_bits_per_second(self) -> f64 {
        match self {
            Self::Opus => 256_000.0,
            Self::Pcm => 48_000.0 * 2.0 * 24.0,
            Self::Opus1 => 320_000.0,
            Self::Video4k => 25_000_000.0,
        }
    }

    pub(super) fn video(self) -> bool {
        self == Self::Video4k
    }

    /// The canonical envelope of part `sequence`, as av-contrib's
    /// `build_opaque_media_object` / `build_fmp4_media_object` build it
    /// (deadline, packaged and published stamps, its metadata and HLS
    /// stamp), and the LL-HLS part it becomes.
    pub(super) fn envelope(self, stream_id: u64, sequence: u64) -> (Bytes, Vec<u8>) {
        let media: Vec<u8> = (0..self.media_len())
            .map(|index| ((index as u64 * 31 + sequence * 7) % 251) as u8)
            .collect();
        let now_ns = i64::try_from(now_unix_us() * 1_000).unwrap();
        let stamp = |ns: i64| {
            ClockTimestamp::new(
                ns,
                "system:realtime:av-contrib",
                ClockConfidence::estimated(1_000_000),
            )
            .unwrap()
        };
        let part_us = self.part_us();
        let parts_per_segment = 1_000_000 / part_us;
        let hls = [
            ("hls-msn", (sequence / parts_per_segment).to_string()),
            ("hls-part", (sequence % parts_per_segment).to_string()),
            ("hls-part-target-ms", part_us.div_ceil(1_000).to_string()),
            ("hls-part-target-us", part_us.to_string()),
            ("hls-target-duration-ms", "1000".to_string()),
            ("hls-target-duration-us", "1000000".to_string()),
        ];
        let (payload, track) = if self.video() {
            (
                encode_test_fmp4_slot(Some(&[7_u8; 900]), &media).to_vec(),
                "muxed-fmp4",
            )
        } else {
            (media.clone(), "opaque-audio")
        };
        let key = media_object::ObjectKey::for_payload(
            "default",
            stream_id.to_string(),
            track,
            0,
            0,
            sequence,
            1,
            &payload,
        )
        .unwrap();
        let mut builder = MediaObject::builder(key, ObjectKind::Media, payload)
            .with_deadline(stamp(now_ns + 5_000_000_000))
            .with_stage_timestamp(StageTimestamp::new(Stage::Packaged, stamp(now_ns - 50_000)))
            .with_stage_timestamp(StageTimestamp::new(Stage::Published, stamp(now_ns)));
        let duration_ms = part_us.div_ceil(1_000).to_string();
        let metadata: Vec<(&str, Vec<u8>)> = if self.video() {
            let init = media_object::ObjectKey::for_payload(
                "default",
                stream_id.to_string(),
                "muxed-fmp4-init",
                0,
                0,
                0,
                1,
                b"init",
            )
            .unwrap();
            builder = builder
                .with_keyframe(sequence % 60 == 0)
                .with_configuration_epoch(1)
                .with_dependency(init);
            vec![
                ("container", b"fmp4".to_vec()),
                ("duration-ms", duration_ms.into_bytes()),
                ("duration-us", part_us.to_string().into_bytes()),
                ("payload-format", b"fmp4-slot-v1".to_vec()),
                ("scheduler-class", b"video".to_vec()),
                ("track-composition", b"video".to_vec()),
                ("video-codec", b"avc1.640033".to_vec()),
                ("video-width", b"3840".to_vec()),
                ("video-height", b"2160".to_vec()),
            ]
        } else {
            vec![
                ("container", b"opaque".to_vec()),
                ("duration-ms", duration_ms.into_bytes()),
                ("duration-us", part_us.to_string().into_bytes()),
                ("payload-format", b"opaque-part-v1".to_vec()),
                ("scheduler-class", b"audio".to_vec()),
                ("track-composition", b"audio".to_vec()),
                ("content-type", b"application/octet-stream".to_vec()),
                ("file-extension", b"bin".to_vec()),
                (
                    "duration-samples",
                    (part_us * 48 / 1_000).to_string().into_bytes(),
                ),
                ("sample-rate", b"48000".to_vec()),
            ]
        };
        for (name, value) in metadata {
            builder = builder.with_metadata(name, value);
        }
        for (name, value) in hls {
            builder = builder.with_metadata(name, value.into_bytes());
        }
        let object = builder.build().unwrap();
        (Bytes::from(media_object::encode(&object).unwrap()), media)
    }

    /// The contributor's RelaySession symbols of an envelope: audio parts
    /// one symbol and one repair symbol, a 4K frame in 1,088-byte symbols
    /// with a tenth of repair.
    pub(super) fn symbols(self, envelope: &[u8]) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let deadline = now_unix_us() + 30_000_000;
        if self.video() {
            let sources = envelope.len().div_ceil(1_088) as u32;
            fast_test_symbols(
                envelope,
                relay_session::MediaPriority::VideoKey,
                1_088,
                400,
                sources.div_ceil(10),
                deadline,
            )
        } else {
            fast_test_symbols(
                envelope,
                relay_session::MediaPriority::Audio,
                1_088,
                400,
                1,
                deadline,
            )
        }
    }
}

// ------------------------------------------------------------------ the path

/// What a listener's path does to the datagrams that reach it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum PathLoss {
    Clean,
    /// Each datagram lost with this probability.
    Random(f64),
    /// Gilbert-Elliott, as netem's `gemodel p r`: a datagram moves the path
    /// to the bad state with probability `enter` and back with `leave`;
    /// every datagram in the bad state is lost.
    Bursts {
        enter: f64,
        leave: f64,
    },
}

impl PathLoss {
    fn name(self) -> String {
        match self {
            Self::Clean => "clean".into(),
            Self::Random(p) => format!("{:.0} % random", p * 100.0),
            Self::Bursts { enter, leave } => {
                format!("gemodel {:.0} % {:.0} %", enter * 100.0, leave * 100.0)
            }
        }
    }
}

struct Path {
    loss: PathLoss,
    state: u64,
    bad: bool,
}

impl Path {
    fn new(loss: PathLoss, seed: u64) -> Self {
        Self {
            loss,
            state: seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1,
            bad: false,
        }
    }

    fn uniform(&mut self) -> f64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        (self.state >> 11) as f64 / (1_u64 << 53) as f64
    }

    fn lost(&mut self) -> bool {
        match self.loss {
            PathLoss::Clean => false,
            PathLoss::Random(p) => self.uniform() < p,
            PathLoss::Bursts { enter, leave } => {
                let draw = self.uniform();
                self.bad = if self.bad {
                    draw >= leave
                } else {
                    draw < enter
                };
                self.bad
            }
        }
    }
}

// ------------------------------------------------------------------ the listener

/// How a listener asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    /// The RaptorQ lane with the repair of before wave 5 (a quarter of the
    /// source symbols and at least three), as needletail-udp-client asked
    /// for it: `--compact false --repair-permille 250 --min-repair 3`.
    Before,
    /// The compact lane, parts in the long form (11-byte header).
    Compact,
    /// needletail-udp-client's defaults now: the compact lane, parts in the
    /// short form (7-byte header, wave 7 item 9).
    CompactShort,
}

/// Shared with a running listener.
#[derive(Default)]
pub(super) struct Shared {
    /// Count the wire from now on (and stop counting when cleared).
    pub(super) measuring: AtomicBool,
    /// Bytes and datagrams of media counted, with their IP and UDP headers.
    wire_bytes: AtomicU64,
    datagrams: AtomicU64,
    /// The last part published: the listener stops once it has it and the
    /// edge has been quiet for a while.
    pub(super) last: AtomicU64,
    /// Publication to arrival of each part measured, in µs.
    latencies_us: std::sync::Mutex<Vec<u64>>,
    /// Each part measured: its number, publication and arrival (µs since
    /// the epoch), for the hop journal's breakdown.
    pub(super) arrivals: std::sync::Mutex<Vec<(u64, u64, u64)>>,
}

impl Shared {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            last: AtomicU64::new(u64::MAX),
            ..Self::default()
        })
    }
}

/// What a listener received.
#[derive(Debug, Default)]
pub(super) struct ListenerRun {
    pub(super) first: Option<u64>,
    /// Each part's deliveries and its bytes.
    pub(super) parts: BTreeMap<u64, (u32, Bytes)>,
    pub(super) gaps: u64,
    pub(super) compact_confirmed: bool,
    pub(super) plan: Option<CompactPlan>,
    pub(super) nack_parts: u64,
    pub(super) recovered_by_window: u64,
    pub(super) retransmits: u64,
    pub(super) copies_asked: u8,
    pub(super) lost_on_path: u64,
    pub(super) errors: u64,
    /// Media datagrams that reached the listener's socket, and their bytes
    /// with IP and UDP headers (before the path's loss).
    pub(super) datagrams: u64,
    pub(super) wire_bytes: u64,
    /// Publication to first arrival of each part, in µs.
    pub(super) latencies_us: Vec<u64>,
    /// Parts first delivered by a compact datagram that the edge marked as
    /// its fast lane's.
    pub(super) fast_lane_parts: u64,
}

impl ListenerRun {
    /// The `q` quantile of publication to arrival, in ms.
    pub(super) fn latency_ms(&self, q: f64) -> f64 {
        let mut values = self.latencies_us.clone();
        if values.is_empty() {
            return 0.0;
        }
        values.sort_unstable();
        let index = ((values.len() as f64 * q).ceil() as usize).clamp(1, values.len()) - 1;
        values[index] as f64 / 1_000.0
    }
}

/// A listener of `stream_id` at `edge` on a path with `loss`, until it has
/// the last part published and the edge has been quiet for 300 ms.
#[allow(clippy::too_many_arguments)]
pub(super) async fn listen(
    edge: SocketAddr,
    stream_id: u64,
    mode: Mode,
    fast_lane: bool,
    loss: PathLoss,
    seed: u64,
    shared: Arc<Shared>,
    subscribed: tokio::sync::oneshot::Sender<u64>,
) -> ListenerRun {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let _ = SockRef::from(&socket).set_recv_buffer_size(16 << 20);
    // As needletail-udp-client: the compact lane's repair while it asks
    // for it, the RaptorQ lane's (as before wave 5) on that lane alone.
    let compact = mode != Mode::Before;
    let short = mode == Mode::CompactShort;
    let mut base = RepairPolicy::base_for_lane(compact, None, None);
    let mut repair = RepairPolicy::for_loss(UNMEASURED_LOSS, base);
    let mut plan = CompactPlan::CLEAN.with_short_header(short);
    let subscribe = |token, repair, plan| {
        Control::Subscribe {
            stream_id,
            from_part: None,
            repair,
            token,
            fast_lane,
            compact: compact.then_some(plan),
            others: Vec::new(),
            resume: false,
        }
        .encode()
    };
    let mut buf = vec![0_u8; 65_536];
    socket
        .send_to(&subscribe(None, repair, plan), edge)
        .await
        .unwrap();
    let asked_at = Instant::now();
    let token = loop {
        let (len, _) = socket.recv_from(&mut buf).await.unwrap();
        if let Some(Control::Token { token, .. }) = Control::decode(&buf[..len]) {
            break token;
        }
    };
    let request_rtt = asked_at.elapsed();
    socket
        .send_to(&subscribe(Some(token), repair, plan), edge)
        .await
        .unwrap();
    let mut run = ListenerRun::default();
    let mut subscribed = Some(subscribed);
    let mut receiver = ObjectReceiver::new();
    receiver.compact.set_leg_rtt(LegId::FIRST, request_rtt);
    let mut path = Path::new(loss, seed);
    let mut estimate: Option<f64> = None;
    let mut smooth: Option<f64> = None;
    let mut burst: Option<f64> = None;
    let mut next_loss_check = Instant::now() + Duration::from_millis(150);
    let mut next_refresh = Instant::now() + Duration::from_secs(2);
    let mut quiet_since = Instant::now();
    loop {
        let now = Instant::now();
        let done = run
            .parts
            .last_key_value()
            .is_some_and(|(part, _)| *part >= shared.last.load(Ordering::Relaxed))
            && receiver.compact.holes() == 0;
        if (done && now.duration_since(quiet_since) >= Duration::from_millis(300))
            || now.duration_since(quiet_since) >= Duration::from_secs(5)
        {
            break;
        }
        if now >= next_loss_check {
            next_loss_check = now + Duration::from_millis(150);
            if let Some(loss) = receiver.take_loss(LegId::FIRST, repair) {
                let value = match estimate {
                    Some(previous) if loss < previous => previous * 0.8 + loss * 0.2,
                    _ => loss,
                };
                estimate = Some(value);
                let smoothed = av_mesh::client_compact::smoothed_loss(smooth, loss);
                smooth = Some(smoothed);
                if let Some(sample) = receiver.compact.take_burst(LegId::FIRST) {
                    burst = Some(burst.map_or(sample, |previous| previous * 0.7 + sample * 0.3));
                }
                let wanted = RepairPolicy::for_loss(value, base);
                let wanted_plan = CompactPlan::for_measured(smoothed, burst.unwrap_or(1.0))
                    .with_short_header(short);
                if wanted != repair || (compact && wanted_plan != plan) {
                    repair = wanted;
                    plan = wanted_plan;
                    run.copies_asked = run.copies_asked.max(plan.copies);
                    next_refresh = now;
                }
            }
        }
        if now >= next_refresh {
            next_refresh = now + Duration::from_secs(2);
            let _ = socket
                .send_to(&subscribe(Some(token), repair, plan), edge)
                .await;
        }
        if run.compact_confirmed {
            let parts = receiver.nacks_due(now).take(LegId::FIRST);
            if !parts.is_empty() {
                run.nack_parts += parts.len() as u64;
                let _ = socket
                    .send_to(
                        &Control::Nack {
                            stream_id,
                            token,
                            parts,
                        }
                        .encode(),
                        edge,
                    )
                    .await;
            }
        }
        let mut wake = next_loss_check.min(now + Duration::from_millis(50));
        if let Some(at) = receiver.compact.next_nack_at() {
            wake = wake.min(at);
        }
        let Ok(Ok((len, _))) = timeout(
            wake.saturating_duration_since(Instant::now()),
            socket.recv_from(&mut buf),
        )
        .await
        else {
            continue;
        };
        quiet_since = Instant::now();
        let datagram = &buf[..len];
        if let Some(control) = Control::decode(datagram) {
            if let Control::Subscribed {
                next_part, compact, ..
            } = control
            {
                if run.first.is_none() {
                    run.first = Some(next_part);
                    receiver.expect(LegId::FIRST, stream_id, next_part, compact);
                }
                run.compact_confirmed = compact;
                // An edge that does not serve the compact lane: the RaptorQ
                // lane's repair.
                let wanted = RepairPolicy::base_for_lane(compact, None, None);
                if wanted != base {
                    base = wanted;
                    repair = RepairPolicy::for_loss(estimate.unwrap_or(UNMEASURED_LOSS), base);
                    next_refresh = Instant::now();
                }
                if let Some(subscribed) = subscribed.take() {
                    let _ = subscribed.send(next_part);
                }
            }
            continue;
        }
        if shared.measuring.load(Ordering::Relaxed) {
            shared
                .wire_bytes
                .fetch_add((len + 28) as u64, Ordering::Relaxed);
            shared.datagrams.fetch_add(1, Ordering::Relaxed);
        }
        run.datagrams += 1;
        run.wire_bytes += (len + 28) as u64;
        if path.lost() {
            run.lost_on_path += 1;
            continue;
        }
        let objects = receiver.push_all(LegId::FIRST, datagram);
        let fast = std::mem::take(&mut receiver.last_compact_fast_lane);
        for object in objects {
            match object.kind {
                UdpObjectKind::Part => {
                    if !run.parts.contains_key(&object.part) && fast.contains(&object.part) {
                        run.fast_lane_parts += 1;
                    }
                    if object.published_unix_us > 0 && !run.parts.contains_key(&object.part) {
                        run.latencies_us
                            .push(now_unix_us().saturating_sub(object.published_unix_us));
                    }
                    if object.published_unix_us > 0
                        && !run.parts.contains_key(&object.part)
                        && shared.measuring.load(Ordering::Relaxed)
                    {
                        let arrived = now_unix_us();
                        shared
                            .latencies_us
                            .lock()
                            .unwrap()
                            .push(arrived.saturating_sub(object.published_unix_us));
                        shared.arrivals.lock().unwrap().push((
                            object.part,
                            object.published_unix_us,
                            arrived,
                        ));
                    }
                    let entry = run
                        .parts
                        .entry(object.part)
                        .or_insert((0, object.payload.clone()));
                    entry.0 += 1;
                }
                UdpObjectKind::Gap => run.gaps += 1,
                UdpObjectKind::Init => {}
            }
        }
    }
    let stats = receiver.compact.stats;
    run.plan = compact.then_some(plan);
    run.recovered_by_window = stats.recovered;
    run.retransmits = stats.retransmits;
    run.errors = receiver.errors;
    let _ = socket
        .send_to(&Control::Unsubscribe { stream_id, token }.encode(), edge)
        .await;
    run
}

/// Every part from the one the edge named to `last` arrived exactly once,
/// byte for byte the edge's LL-HLS part.
fn check(name: &str, run: &ListenerRun, served: &BTreeMap<u64, Bytes>, last: u64) {
    let first = run.first.unwrap_or_else(|| panic!("{name}: subscribed"));
    let mut missing = Vec::new();
    for part in first..=last {
        match run.parts.get(&part) {
            None => missing.push(part),
            Some((count, bytes)) => {
                assert_eq!(*count, 1, "{name}: part {part} arrived {count} times");
                assert_eq!(
                    Some(bytes),
                    served.get(&part),
                    "{name}: part {part} is the edge's LL-HLS part"
                );
            }
        }
    }
    assert!(
        missing.is_empty(),
        "{name}: {} parts never arrived (first {first}, last {last}): {:?}; {run:?}",
        missing.len(),
        &missing[..missing.len().min(20)]
    );
    assert_eq!(run.gaps, 0, "{name}: no gap");
}

// ------------------------------------------------------------------ the edges

/// An edge fed by two distributors over the cache mesh, with cut-through
/// and the cache-fed lane, as today's deploy feeds edges.
pub(super) struct MeshEdge {
    pub(super) cache: Arc<LiveTsCache>,
    pub(super) client_port: SocketAddr,
    pub(super) stats: Arc<edge_udp::EdgeUdpStats>,
    parents: Vec<(Arc<LiveTsCache>, playlists::mesh::CacheMeshHandle)>,
    shutdown_tx: watch::Sender<()>,
}

impl MeshEdge {
    /// The edge, and the next part to publish (the parents publish parts
    /// from 0 until the edge serves the stream).
    pub(super) async fn start(payload: Payload, stream_id: u64, compact: bool) -> (Self, u64) {
        let edge_addr = unused_loopback_addr();
        let mut parents = Vec::new();
        for name in ["dist-a", "dist-b"] {
            let addr = unused_loopback_addr();
            let cache =
                LiveTsCache::with_ring(1, Duration::from_micros(2_500), 400, 12_000, 64, 8, 0)
                    .await;
            let mesh = CacheMesh::new(
                Arc::clone(&cache.chunk_cache),
                CacheMeshConfig::new(name, "uk", addr)
                    .with_peer(edge_addr)
                    .with_role(MeshRole::Distributor)
                    .with_announce_interval(Duration::from_millis(100)),
            )
            .start()
            .await
            .unwrap();
            parents.push((addr, cache, mesh));
        }
        let parent_addrs: Vec<SocketAddr> = parents.iter().map(|(addr, ..)| *addr).collect();
        let cache =
            LiveTsCache::with_ring(1, Duration::from_micros(2_500), 400, 12_000, 64, 8, 0).await;
        // The edge's hop journal, as a node keeps one.
        let journal = Arc::new(HopJournal::new("edge-1", 1 << 18));
        cache.set_hop_journal(Arc::clone(&journal));
        let cut = Arc::new(edge_udp::CutThrough::default());
        let (applied_tx, applied_rx) = mpsc::unbounded_channel();
        let mesh = Arc::new(
            CacheMesh::new(
                Arc::clone(&cache.chunk_cache),
                CacheMeshConfig::new("edge-1", "uk", edge_addr)
                    .with_peers(parent_addrs)
                    .with_role(MeshRole::Edge)
                    .with_replication(ReplicationMode::Demand)
                    .with_announce_interval(Duration::from_millis(100))
                    .with_applied_tx(applied_tx)
                    .with_receive_observer(Arc::clone(&cut) as _),
            )
            .start()
            .await
            .unwrap(),
        );
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh))
            .with_playlist_demand(PlaylistDemand::new(PlaylistDemandConfig {
                enabled: true,
                lease: Duration::from_secs(5),
                idle: Duration::from_secs(30),
                warm_parts: 64,
                max_playlists: MAX_EDGE_PLAYLISTS,
            }))
            .with_parents_silent_after(Some(Duration::from_millis(1_500)));
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(run_mesh_applied_parts(
            Arc::clone(&cache),
            router.edge_load.clone(),
            applied_rx,
            None,
            shutdown_rx.clone(),
        ));
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let _ = SockRef::from(&socket).set_send_buffer_size(16 << 20);
        let sockets = edge_udp::EdgeSockets::single(socket).with_hop_journal(Some(journal));
        let client_port = sockets.for_stream(0).local_addr().unwrap();
        let stats: Arc<edge_udp::EdgeUdpStats> = Arc::default();
        let mut config = edge_udp::EdgeUdpConfig::with_defaults(1_200, 4_096);
        config.playout = Duration::from_secs(5);
        config.compact = compact;
        tokio::spawn(edge_udp::run_edge_udp(
            sockets,
            router.clone(),
            config,
            Arc::clone(&stats),
            vec![cut],
            shutdown_rx,
        ));
        // The parents push each new part to the edge as it is committed; a
        // reader's first request then subscribes the edge to the playlist.
        let mut next = 0_u64;
        timeout(Duration::from_secs(10), async {
            loop {
                let (envelope, _) = payload.envelope(stream_id, next);
                next += 1;
                for (_, cache, _) in &parents {
                    cache
                        .commit_stream_payload(stream_id, envelope.clone())
                        .await
                        .unwrap();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
                let warm =
                    get_for_tests(&router, &format!("/live/{stream_id}/stream.m3u8?sid=warm"))
                        .await;
                if warm.status == StatusCode::OK
                    && cache.stream_position_for_id(stream_id).await.is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("the edge serves the stream");
        (
            Self {
                cache,
                client_port,
                stats,
                parents: parents
                    .into_iter()
                    .map(|(_, cache, mesh)| (cache, mesh))
                    .collect(),
                shutdown_tx,
            },
            next,
        )
    }

    async fn publish(&self, stream_id: u64, envelope: &Bytes) {
        for (cache, _) in &self.parents {
            cache
                .commit_stream_payload(stream_id, envelope.clone())
                .await
                .unwrap();
        }
    }

    async fn stop(self) {
        let _ = self.shutdown_tx.send(());
        for (_, mesh) in self.parents {
            mesh.shutdown();
        }
    }
}

/// Which edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Feed {
    Fast,
    Mesh,
    /// The fast plane, its audio parts sent as the contributor sends them
    /// with --relay-compact-audio: compact RLC1 datagrams, and a whole part
    /// every 100 ms for the descriptor (wave 7 item 3).
    FastCompact,
}

pub(super) enum AnyEdge {
    Fast(LaneEdge),
    Mesh(MeshEdge),
    FastCompact(
        LaneEdge,
        std::sync::Mutex<playlists::compact_object::CompactSender>,
    ),
}

/// How often a compact feed sends a part whole.
const COMPACT_WHOLE_EVERY_US: u64 = 100_000;

impl AnyEdge {
    /// The edge, and the next part to publish.
    pub(super) async fn start(feed: Feed, payload: Payload, stream_id: u64, ip: u8) -> (Self, u64) {
        match feed {
            Feed::Fast => {
                let edge = LaneEdge::start(loopback(ip)).await;
                // The stream is in the cache before anyone subscribes.
                let (envelope, _) = payload.envelope(stream_id, 0);
                edge.send(payload.video(), &payload.symbols(&envelope), false)
                    .await;
                wait_for_part(&edge.cache, stream_id, 0).await;
                (Self::Fast(edge), 1)
            }
            Feed::Mesh => {
                let (edge, next) = MeshEdge::start(payload, stream_id, true).await;
                (Self::Mesh(edge), next)
            }
            Feed::FastCompact => {
                let (Self::Fast(edge), next) =
                    Box::pin(Self::start(Feed::Fast, payload, stream_id, ip)).await
                else {
                    unreachable!("a fast edge")
                };
                let sender = playlists::compact_object::CompactSender::new(COMPACT_WHOLE_EVERY_US);
                (Self::FastCompact(edge, std::sync::Mutex::new(sender)), next)
            }
        }
    }

    pub(super) fn client_port(&self) -> SocketAddr {
        match self {
            Self::Fast(edge) | Self::FastCompact(edge, _) => edge.client_port,
            Self::Mesh(edge) => edge.client_port,
        }
    }

    pub(super) fn cache(&self) -> &Arc<LiveTsCache> {
        match self {
            Self::Fast(edge) | Self::FastCompact(edge, _) => &edge.cache,
            Self::Mesh(edge) => &edge.cache,
        }
    }

    pub(super) fn stats(&self) -> &Arc<edge_udp::EdgeUdpStats> {
        match self {
            Self::Fast(edge) | Self::FastCompact(edge, _) => &edge.stats,
            Self::Mesh(edge) => &edge.stats,
        }
    }

    pub(super) async fn publish(&self, payload: Payload, stream_id: u64, sequence: u64) {
        let (envelope, _) = payload.envelope(stream_id, sequence);
        match self {
            Self::Fast(edge) => {
                edge.send(payload.video(), &payload.symbols(&envelope), false)
                    .await;
            }
            Self::Mesh(edge) => edge.publish(stream_id, &envelope).await,
            Self::FastCompact(edge, sender) => {
                let compact = crate::tests::fast_test_compact(
                    &envelope,
                    &mut sender.lock().unwrap(),
                    sequence * payload.part_us(),
                    now_unix_us() + 30_000_000,
                    1,
                );
                let symbols = compact.unwrap_or_else(|| payload.symbols(&envelope));
                edge.send(false, &symbols, false).await;
            }
        }
    }

    pub(super) async fn stop(self) {
        match self {
            Self::Fast(edge) | Self::FastCompact(edge, _) => edge.stop().await,
            Self::Mesh(edge) => edge.stop().await,
        }
    }
}

/// Publish parts `from..to` at the payload's pace (behind a slow host it
/// catches up, never more than a few parts at once).
pub(super) async fn publish_paced(
    edge: &AnyEdge,
    payload: Payload,
    stream_id: u64,
    from: u64,
    to: u64,
) {
    let start = Instant::now();
    for sequence in from..to {
        let due = start + Duration::from_micros(payload.part_us() * (sequence - from));
        tokio::time::sleep_until(due.into()).await;
        edge.publish(payload, stream_id, sequence).await;
    }
}

// ------------------------------------------------------------------ zero loss

/// Listeners of the compact lane on clean, 1 % and 5 % random and bursty
/// (gemodel 1 % 20 %) paths each get every part exactly once, byte for byte
/// the edge's LL-HLS part, from an edge of each feed: the copies, the window
/// repair and NACK make up the loss.
async fn compact_listeners_lose_nothing(feed: Feed, payload: Payload, ip: u8) {
    let stream_id = 4001;
    let (edge, first) = AnyEdge::start(feed, payload, stream_id, ip).await;
    let shared = Shared::new();
    let paths = [
        PathLoss::Clean,
        PathLoss::Random(0.01),
        PathLoss::Random(0.05),
        PathLoss::Bursts {
            enter: 0.01,
            leave: 0.2,
        },
    ];
    let mut listeners = Vec::new();
    // A listener of before the compact lane, beside them.
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        listeners.push((
            format!("{feed:?} clean, RaptorQ lane"),
            tokio::spawn(listen(
                edge.client_port(),
                stream_id,
                Mode::Before,
                true,
                PathLoss::Clean,
                0xbef0,
                Arc::clone(&shared),
                tx,
            )),
        ));
        timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
    }
    for (index, loss) in paths.into_iter().enumerate() {
        // The fast lane's edge also serves a listener without F1 (the
        // cache-fed lane) on the bursty path.
        let lanes: &[bool] = if feed != Feed::Mesh && index == 3 {
            &[true, false]
        } else {
            &[true]
        };
        for fast_lane in lanes {
            let (tx, rx) = tokio::sync::oneshot::channel();
            listeners.push((
                format!("{feed:?} {} F1={fast_lane}", loss.name()),
                tokio::spawn(listen(
                    edge.client_port(),
                    stream_id,
                    // The compact feed's listeners take the short form.
                    if feed == Feed::FastCompact {
                        Mode::CompactShort
                    } else {
                        Mode::Compact
                    },
                    *fast_lane,
                    loss,
                    0x5eed + index as u64,
                    Arc::clone(&shared),
                    tx,
                )),
            ));
            timeout(Duration::from_secs(5), rx)
                .await
                .expect("the edge answers")
                .unwrap();
        }
    }
    let parts = first + 2_000;
    publish_paced(&edge, payload, stream_id, first, parts).await;
    shared.last.store(parts - 1, Ordering::Relaxed);
    let mut runs = Vec::new();
    for (name, listener) in listeners {
        runs.push((name, listener.await.unwrap()));
    }
    let from = runs.iter().filter_map(|(_, run)| run.first).min().unwrap();
    let mut served = BTreeMap::new();
    for part in from..parts {
        served.insert(part, wait_for_part(edge.cache(), stream_id, part).await);
    }
    let mut report = Vec::new();
    let part_ms = payload.part_us() as f64 / 1_000.0;
    for (name, run) in runs {
        let received = (parts - run.first.unwrap_or(first)) as f64;
        let datagrams_per_part = run.datagrams as f64 / received;
        let bytes_per_part = run.wire_bytes as f64 / received;
        let (p50, p99) = (run.latency_ms(0.5), run.latency_ms(0.99));
        report.push(format!(
            "{name}: lost on path {}, NACKed {}, retransmitted {}, recovered by window {}, plan {:?}, \
             {datagrams_per_part:.3} datagrams and {bytes_per_part:.0} B a part, \
             publish->arrival p50 {p50:.2} p99 {p99:.2} ms",
            run.lost_on_path,
            run.nack_parts,
            run.retransmits,
            run.recovered_by_window,
            run.plan.map(|plan| plan.word())
        ));
        assert_eq!(
            run.compact_confirmed,
            !name.contains("RaptorQ"),
            "{name}: the edge serves the compact lane to those that ask"
        );
        check(&name, &run, &served, parts - 1);
        // The lane of the parts, from the edge's mark: a fast-lane edge
        // sends an F1 listener its parts from the class thread (all but
        // the few that the cache-fed lane sent first, and those that loss
        // took), and the cache-fed lane sends the rest unmarked.
        if run.compact_confirmed {
            if feed != Feed::Mesh && name.contains("F1=true") {
                assert!(
                    run.fast_lane_parts as f64 >= 0.8 * received,
                    "{name}: {} of {received} parts came marked as the fast lane's",
                    run.fast_lane_parts
                );
            } else {
                assert_eq!(run.fast_lane_parts, 0, "{name}: no part is marked fast");
            }
        }
        if !name.contains("clean") {
            assert!(run.lost_on_path > 0, "{name}: the path lost datagrams");
        }
        if !run.compact_confirmed {
            continue;
        }
        // The wire of a clean path: one datagram a part and a window's
        // repair (1/32), nothing more.
        if name.contains("clean") {
            let header = if feed == Feed::FastCompact {
                av_mesh::client_compact::SHORT_HEADER_LEN
            } else {
                av_mesh::client_compact::HEADER_LEN
            };
            let ceiling = (payload.media_len() + header + 28) as f64 * 1.06;
            assert!(
                datagrams_per_part <= 1.06 && bytes_per_part <= ceiling,
                "{name}: {datagrams_per_part:.3} datagrams and {bytes_per_part:.0} B a part \
                 (at most 1.06 and {ceiling:.0})"
            );
        }
        // On time: a part that its path lost comes with its copy, its
        // window's repair or a NACK's answer, not hundreds of ms late. A
        // burst can wait for the end of its window (16 parts).
        let bound_ms = if name.contains("gemodel") {
            16.0 * part_ms + 30.0
        } else {
            30.0
        };
        assert!(
            p99 <= bound_ms,
            "{name}: publish->arrival p99 {p99:.2} ms (at most {bound_ms:.1})"
        );
    }
    println!("{}", report.join("\n"));
    if feed == Feed::FastCompact {
        // The edge's LL-HLS parts are the contributor's parts byte for
        // byte, though most came as slim objects: the tee rebuilt their
        // envelopes from the descriptor.
        for (part, bytes) in &served {
            assert_eq!(
                bytes.as_ref(),
                payload.envelope(stream_id, *part).1.as_slice(),
                "part {part} is the contributor's part"
            );
        }
        let relay = edge.cache().relay_ingress_snapshot();
        assert!(
            relay.compact_decoded_objects >= parts - first - 2 * (parts - first) / 100 - 2,
            "{relay:?}: the parts but the whole ones came compact"
        );
        assert!(relay.compact_descriptors_learned > 0, "{relay:?}");
    }
    let stats = edge.stats();
    assert!(stats.compact_parts.load(Ordering::Relaxed) > 0);
    assert!(stats.compact_window_repair.load(Ordering::Relaxed) > 0);
    assert!(stats.nack_resent.load(Ordering::Relaxed) > 0);
    edge.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compact_listeners_of_the_fast_lane_lose_nothing_through_loss_and_bursts() {
    compact_listeners_lose_nothing(Feed::Fast, Payload::Pcm, 21).await;
}

/// An edge fed compact RLC1 datagrams (wave 7 item 3): its compact
/// listeners, its RaptorQ-lane listener and its LL-HLS parts lose nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compact_listeners_of_a_compact_fed_edge_lose_nothing_through_loss_and_bursts() {
    compact_listeners_lose_nothing(Feed::FastCompact, Payload::Opus1, 29).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compact_listeners_of_a_mesh_fed_edge_lose_nothing_through_loss_and_bursts() {
    compact_listeners_lose_nothing(Feed::Mesh, Payload::Opus, 22).await;
}

/// 4K parts do not fit one datagram: a compact listener gets them as RaptorQ
/// blocks with one repair symbol on a clean path, and a part that its loss
/// beats is asked for again and sent whole. Five seconds of 25 Mbit/s, on
/// clean, 1 % and bursty paths: every frame, and on time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compact_listener_of_4k_loses_nothing_at_one_percent() {
    let stream_id = 1;
    let payload = Payload::Video4k;
    let (edge, first) = AnyEdge::start(Feed::Fast, payload, stream_id, 23).await;
    let shared = Shared::new();
    let mut listeners = Vec::new();
    for (index, loss) in [
        PathLoss::Clean,
        PathLoss::Random(0.01),
        PathLoss::Bursts {
            enter: 0.01,
            leave: 0.2,
        },
    ]
    .into_iter()
    .enumerate()
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        listeners.push((
            format!("4K {}", loss.name()),
            tokio::spawn(listen(
                edge.client_port(),
                stream_id,
                Mode::Compact,
                true,
                loss,
                0x4b + index as u64,
                Arc::clone(&shared),
                tx,
            )),
        ));
        timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
    }
    let parts = 300_u64;
    publish_paced(&edge, payload, stream_id, first, parts).await;
    shared.last.store(parts - 1, Ordering::Relaxed);
    let mut runs = Vec::new();
    for (name, listener) in listeners {
        runs.push((name, listener.await.unwrap()));
    }
    let mut served = BTreeMap::new();
    for part in 0..parts {
        served.insert(part, wait_for_part(edge.cache(), stream_id, part).await);
    }
    for (name, run) in runs {
        check(&name, &run, &served, parts - 1);
        // On time: within two frames of its publication, but for the 1 %
        // that a NACK's round trip may take.
        let (p50, p99) = (run.latency_ms(0.5), run.latency_ms(0.99));
        let on_time = run
            .latencies_us
            .iter()
            .filter(|latency| **latency <= 2 * payload.part_us())
            .count() as f64
            / run.latencies_us.len().max(1) as f64;
        println!(
            "{name}: lost on path {}, NACKed {}, frames on time {:.1} %, p50 {p50:.2} p99 {p99:.2} ms",
            run.lost_on_path,
            run.nack_parts,
            on_time * 100.0
        );
        assert!(
            on_time >= 0.98,
            "{name}: {:.1} % of frames on time",
            on_time * 100.0
        );
        assert!(p99 <= 100.0, "{name}: p99 {p99:.2} ms");
    }
    edge.stop().await;
}

/// With --client-udp-compact false, a listener that asks for the compact
/// lane is told no (`OK` without `C`) and gets every part on the RaptorQ
/// lane, with the repair of before the compact lane (a quarter and at least
/// three symbols a part): on clean and 1 % paths nothing is lost, and on a
/// bursty one no more than wave 4's lane lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_without_the_compact_lane_serves_the_raptorq_lane() {
    let stream_id = 4001;
    let payload = Payload::Opus;
    let (edge, first) = MeshEdge::start(payload, stream_id, false).await;
    let edge = AnyEdge::Mesh(edge);
    let shared = Shared::new();
    let mut listeners = Vec::new();
    for (index, loss) in [
        PathLoss::Clean,
        PathLoss::Random(0.01),
        PathLoss::Bursts {
            enter: 0.01,
            leave: 0.2,
        },
    ]
    .into_iter()
    .enumerate()
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        listeners.push((
            format!("compact off, {}", loss.name()),
            tokio::spawn(listen(
                edge.client_port(),
                stream_id,
                Mode::Compact,
                true,
                loss,
                7 + index as u64,
                Arc::clone(&shared),
                tx,
            )),
        ));
        timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
    }
    let parts = first + 2_000;
    publish_paced(&edge, payload, stream_id, first, parts).await;
    shared.last.store(parts - 1, Ordering::Relaxed);
    let mut runs = Vec::new();
    for (name, listener) in listeners {
        runs.push((name, listener.await.unwrap()));
    }
    let from = runs.iter().filter_map(|(_, run)| run.first).min().unwrap();
    let mut served = BTreeMap::new();
    for part in from..parts {
        served.insert(part, wait_for_part(edge.cache(), stream_id, part).await);
    }
    for (name, run) in runs {
        assert!(!run.compact_confirmed, "{name}");
        let received = (parts - run.first.unwrap()) as f64;
        // The RaptorQ lane's repair: four datagrams or more a part.
        let datagrams_per_part = run.datagrams as f64 / received;
        assert!(
            datagrams_per_part >= 3.9,
            "{name}: {datagrams_per_part:.2} datagrams a part"
        );
        if name.contains("gemodel") {
            let missing = (run.first.unwrap()..parts)
                .filter(|part| !run.parts.contains_key(part))
                .count();
            println!(
                "{name}: lost on path {}, parts missing {missing} of {received}",
                run.lost_on_path
            );
            assert!(
                missing as f64 <= received * 0.01,
                "{name}: {missing} parts missing"
            );
        } else {
            check(&name, &run, &served, parts - 1);
        }
    }
    assert_eq!(edge.stats().compact_parts.load(Ordering::Relaxed), 0);
    edge.stop().await;
}

/// A part that the edge gave up as a gap (its hole outlived the repair
/// horizon) and that its cache gets a moment later reaches a compact
/// listener: the listener asks for the gap again, and the edge answers with
/// the part, not the gap it had kept.
async fn a_part_filled_after_its_gap_reaches_the_listener(fast_lane: bool, ip: u8) {
    let stream_id = 4002;
    let payload = Payload::Opus;
    let (edge, first) = AnyEdge::start(Feed::Fast, payload, stream_id, ip).await;
    edge.cache()
        .set_canonical_gap_hold(Duration::from_millis(DEFAULT_CANONICAL_GAP_HOLD_MS));
    edge.cache().set_repair_horizon(RepairHorizon::Fixed(10));
    let shared = Shared::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let listener = tokio::spawn(listen(
        edge.client_port(),
        stream_id,
        Mode::Compact,
        fast_lane,
        PathLoss::Clean,
        11,
        Arc::clone(&shared),
        tx,
    ));
    timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
    let parts = first + 200;
    let late = first + 100;
    let start = Instant::now();
    for sequence in first..parts {
        let due = start + Duration::from_micros(payload.part_us() * (sequence - first));
        tokio::time::sleep_until(due.into()).await;
        if sequence != late {
            edge.publish(payload, stream_id, sequence).await;
        }
        // The lost part comes 60 ms late: long after the 10 ms horizon.
        if sequence == late + 24 {
            edge.publish(payload, stream_id, late).await;
        }
    }
    shared.last.store(parts - 1, Ordering::Relaxed);
    let run = listener.await.unwrap();
    let name = format!("late part, F1={fast_lane}");
    assert!(run.compact_confirmed, "{name}");
    assert_eq!(
        run.gaps,
        1,
        "{name}: the part went as a gap first: late {late} came {:?}, edge gaps {} past playout {} nack resent {}, cache {:?}",
        run.parts.get(&late).map(|entry| entry.0),
        edge.stats().gaps.load(Ordering::Relaxed),
        edge.stats().holes_past_playout.load(Ordering::Relaxed),
        edge.stats().nack_resent.load(Ordering::Relaxed),
        edge.cache().canonical_gap_skip_snapshot()
    );
    let (count, bytes) = run
        .parts
        .get(&late)
        .unwrap_or_else(|| panic!("{name}: the late part never came: {run:?}"));
    assert_eq!(*count, 1, "{name}");
    assert_eq!(*bytes, wait_for_part(edge.cache(), stream_id, late).await);
    for part in run.first.unwrap()..parts {
        assert!(run.parts.contains_key(&part), "{name}: part {part}");
    }
    assert!(edge.stats().gaps.load(Ordering::Relaxed) >= 1);
    edge.stop().await;
}

/// A one-symbol part whose source symbol the edge never got (only the
/// contributor's repair symbol came) goes to a compact fast-lane listener
/// from the edge's decode, on the fast plane, not after the cache-fed lane
/// has worked its way to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_part_decoded_from_repair_alone_goes_to_compact_listeners_at_once() {
    let stream_id = 4003;
    let payload = Payload::Opus;
    let (edge, first) = AnyEdge::start(Feed::Fast, payload, stream_id, 26).await;
    let shared = Shared::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let listener = tokio::spawn(listen(
        edge.client_port(),
        stream_id,
        Mode::Compact,
        true,
        PathLoss::Clean,
        13,
        Arc::clone(&shared),
        tx,
    ));
    timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
    let AnyEdge::Fast(lane_edge) = &edge else {
        unreachable!()
    };
    let parts = first + 400;
    let start = Instant::now();
    let mut repair_only = 0;
    for sequence in first..parts {
        let due = start + Duration::from_micros(payload.part_us() * (sequence - first));
        tokio::time::sleep_until(due.into()).await;
        let (envelope, _) = payload.envelope(stream_id, sequence);
        let (sources, repairs) = payload.symbols(&envelope);
        // Every tenth part loses its source symbol upstream.
        if sequence % 10 == 5 {
            repair_only += 1;
            lane_edge.send(false, &(Vec::new(), repairs), false).await;
        } else {
            lane_edge.send(false, &(sources, repairs), false).await;
        }
    }
    shared.last.store(parts - 1, Ordering::Relaxed);
    let run = listener.await.unwrap();
    let mut served = BTreeMap::new();
    for part in run.first.unwrap()..parts {
        served.insert(part, wait_for_part(edge.cache(), stream_id, part).await);
    }
    check("repair alone", &run, &served, parts - 1);
    let snapshot = edge
        .stats()
        .fast_lane
        .get()
        .and_then(std::sync::Weak::upgrade)
        .expect("the fast lane")
        .snapshot_stats();
    assert!(
        snapshot.compact_top_ups >= repair_only - 2,
        "{} of {repair_only} parts decoded from repair went from the decode",
        snapshot.compact_top_ups
    );
    let p99 = run.latency_ms(0.99);
    assert!(p99 <= 30.0, "publish->arrival p99 {p99:.2} ms");
    edge.stop().await;
}

/// A window's repair goes from the lane that completes the window (here
/// the fast lane's class thread), not when the cache-fed lane reaches the
/// window's end: that lane waits at a hole upstream for the playout budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_repair_does_not_wait_for_the_cache_fed_lane() {
    let stream_id = 4004;
    let payload = Payload::Opus;
    let (edge, first) = AnyEdge::start(Feed::Fast, payload, stream_id, 27).await;
    let shared = Shared::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let listener = tokio::spawn(listen(
        edge.client_port(),
        stream_id,
        Mode::Compact,
        true,
        PathLoss::Clean,
        17,
        Arc::clone(&shared),
        tx,
    ));
    let from = timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
    // A part that never comes, early: the cache-fed lane holds there for
    // the playout budget (500 ms).
    let hole = from + 3;
    // Two whole windows of 32 after it, then a look before the hole's wait
    // is over (about 200 ms of parts).
    let window = u64::from(CompactPlan::CLEAN.window);
    let parts = (hole / window + 3) * window;
    let start = Instant::now();
    for sequence in first..parts {
        let due = start + Duration::from_micros(payload.part_us() * (sequence - first));
        tokio::time::sleep_until(due.into()).await;
        if sequence != hole {
            edge.publish(payload, stream_id, sequence).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let held_for = start.elapsed();
    let repair = edge.stats().compact_window_repair.load(Ordering::Relaxed);
    assert!(
        held_for < Duration::from_millis(450),
        "the run took {held_for:?}: the hole's wait is over"
    );
    assert!(
        repair >= 2,
        "{repair} window repair datagrams sent within {held_for:?} (two windows complete)"
    );
    // Each window's repair goes once, though the cache-fed lane passes it
    // later.
    shared.last.store(parts - 1, Ordering::Relaxed);
    let run = listener.await.unwrap();
    assert!(run.compact_confirmed);
    let windows = (parts - from).div_ceil(window) + 1;
    let repair = edge.stats().compact_window_repair.load(Ordering::Relaxed);
    assert!(
        repair <= windows,
        "{repair} window repair datagrams for at most {windows} windows"
    );
    edge.stop().await;
}

/// D3 review 1: a window whose parts reach the edge in one burst (several
/// in each receive batch, as after any queueing upstream) has its repair
/// from the fast lane at once, though the tee keeps the parts in the
/// compact ring later and on another thread. Each part the class thread
/// sends to a window-repair client goes to the repair thread with its
/// canonical datagram, in order, and is kept in the ring there before the
/// window it completes is read. The cache-fed lane holds at a hole for the
/// playout budget meanwhile, so it repairs nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_window_that_arrives_in_one_burst_is_repaired_by_the_fast_lane() {
    let stream_id = 4005;
    let payload = Payload::Opus;
    let (edge, first) = AnyEdge::start(Feed::Fast, payload, stream_id, 28).await;
    let AnyEdge::Fast(lane_edge) = &edge else {
        unreachable!("a fast edge")
    };
    let shared = Shared::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let listener = tokio::spawn(listen(
        edge.client_port(),
        stream_id,
        Mode::Compact,
        true,
        PathLoss::Clean,
        18,
        Arc::clone(&shared),
        tx,
    ));
    let from = timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
    let window = u64::from(CompactPlan::CLEAN.window);
    // A hole early on: the cache-fed lane waits there (500 ms).
    let hole = from + 3;
    let bursts_from = (hole / window + 1) * window;
    let bursts = 3;
    let start = Instant::now();
    for sequence in first..bursts_from {
        let due = start + Duration::from_micros(payload.part_us() * (sequence - first));
        tokio::time::sleep_until(due.into()).await;
        if sequence != hole {
            edge.publish(payload, stream_id, sequence).await;
        }
    }
    let batches_before = lane_edge.cache.relay_ingress_snapshot().fast_plane.audio_batches;
    let received_before = lane_edge.cache.relay_ingress_snapshot().fast_plane.audio_received;
    for burst in 0..bursts {
        // Each window's parts made first, then sent back to back.
        let parts: Vec<_> = (0..window)
            .map(|offset| {
                let (envelope, _) = payload.envelope(stream_id, bursts_from + burst * window + offset);
                payload.symbols(&envelope)
            })
            .collect();
        for symbols in &parts {
            lane_edge.send(false, symbols, false).await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let held_for = start.elapsed();
    let fast_plane = lane_edge.cache.relay_ingress_snapshot().fast_plane;
    let repair = edge.stats().compact_window_repair.load(Ordering::Relaxed);
    assert!(
        held_for < Duration::from_millis(450),
        "the run took {held_for:?}: the hole's wait is over"
    );
    let per_batch = (fast_plane.audio_received - received_before) as f64
        / (fast_plane.audio_batches - batches_before).max(1) as f64;
    assert!(
        per_batch > 1.5,
        "{per_batch:.2} datagrams a receive batch: the bursts were not batched"
    );
    assert!(
        repair >= bursts,
        "{repair} window repair datagrams within {held_for:?} for {bursts} windows sent in bursts"
    );
    // The edge's stop ends the listener (the hole is never filled).
    shared.last.store(bursts_from + bursts * window - 1, Ordering::Relaxed);
    edge.stop().await;
    let run = listener.await.unwrap();
    assert!(run.compact_confirmed);
}

/// Wave 8 S1: the class thread takes no lock that ordinary tasks hold. A
/// task that holds the subscriber table's lock (a SUB, a NACK storm, the
/// cache-fed lane) for 60 ms does not stop the fast lane: windows complete
/// meanwhile and their repair waits on the repair thread, not on the class
/// thread. With --client-udp-repair-thread false the class thread runs the
/// repair itself and stops at the first window that completes. Returns the
/// parts that the fast lane sent while the lock was held.
async fn the_fast_lane_goes_on_while_the_table_is_locked(repair_thread: bool, ip: u8) -> u64 {
    let stream_id = 4005;
    let payload = Payload::Opus1;
    let (edge, first) = AnyEdge::start(Feed::Fast, payload, stream_id, ip).await;
    let AnyEdge::Fast(lane_edge) = &edge else {
        unreachable!("a fast edge")
    };
    lane_edge.lane.set_repair_thread(repair_thread);
    let lane = Arc::clone(&lane_edge.lane);
    let shared = Shared::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let listener = tokio::spawn(listen(
        edge.client_port(),
        stream_id,
        Mode::Compact,
        true,
        PathLoss::Clean,
        17,
        Arc::clone(&shared),
        tx,
    ));
    let from = timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
    let table = lane.table_for_test().expect("armed");
    let window = u64::from(CompactPlan::CLEAN.window);
    // A few windows before the lock, then 60 ms of parts with the table
    // locked by another thread, then the rest.
    let parts = from + 6 * window;
    let lock_at = from + 2 * window;
    let lock_for = Duration::from_millis(60);
    let start = Instant::now();
    let mut during = 0_u64;
    // The lane's compact parts while the lock is still held (read by the
    // holder, before it lets go).
    let mut holder: Option<std::thread::JoinHandle<u64>> = None;
    let mut before = 0;
    for sequence in first..parts {
        let due = start + Duration::from_micros(payload.part_us() * (sequence - first));
        tokio::time::sleep_until(due.into()).await;
        if sequence == lock_at {
            before = lane.stats.compact_parts.load(Ordering::Relaxed);
            let table = Arc::clone(&table);
            let lane = Arc::clone(&lane);
            let (locked_tx, locked_rx) = std::sync::mpsc::channel();
            holder = Some(std::thread::spawn(move || {
                let _guard = table.lock().unwrap();
                locked_tx.send(()).unwrap();
                std::thread::sleep(lock_for);
                lane.stats.compact_parts.load(Ordering::Relaxed)
            }));
            locked_rx.recv().unwrap();
        }
        edge.publish(payload, stream_id, sequence).await;
        if holder.as_ref().is_some_and(|thread| thread.is_finished()) {
            during = holder.take().unwrap().join().unwrap() - before;
        }
    }
    if let Some(thread) = holder.take() {
        during = thread.join().unwrap() - before;
    }
    shared.last.store(parts - 1, Ordering::Relaxed);
    let run = listener.await.unwrap();
    assert!(run.compact_confirmed);
    if repair_thread {
        assert_eq!(lane.stats.class_repairs.load(Ordering::Relaxed), 0);
        assert!(lane.stats.repair_handoffs.load(Ordering::Relaxed) >= 4);
    } else {
        assert!(lane.stats.class_repairs.load(Ordering::Relaxed) >= 1);
    }
    edge.stop().await;
    eprintln!("repair thread {repair_thread}: {during} parts on the fast lane in the 60 ms lock");
    during
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_class_thread_does_not_wait_for_the_subscriber_table() {
    // About 60 parts of 1 ms go while the table is locked.
    let during = the_fast_lane_goes_on_while_the_table_is_locked(true, 51).await;
    assert!(during >= 40, "{during} parts went on the fast lane while the table was locked");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_the_repair_thread_the_class_thread_waits_for_the_table() {
    // The first window that completes under the lock stops the thread: a
    // window's parts go at most.
    let during = the_fast_lane_goes_on_while_the_table_is_locked(false, 52).await;
    assert!(
        during <= u64::from(CompactPlan::CLEAN.window) + 2,
        "{during} parts went on the fast lane while the table was locked"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_part_filled_after_its_gap_reaches_a_cache_fed_listener() {
    a_part_filled_after_its_gap_reaches_the_listener(false, 24).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_part_filled_after_its_gap_reaches_a_fast_lane_listener() {
    a_part_filled_after_its_gap_reaches_the_listener(true, 25).await;
}

#[test]
fn the_compact_flag_defaults_on_and_can_be_turned_off() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert!(args.client_udp_compact);
    let args = Args::try_parse_from(["av-mesh", "--client-udp-compact", "false"]).unwrap();
    assert!(!args.client_udp_compact);
}

#[test]
fn the_fast_lane_mark_defaults_on_and_can_be_turned_off() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert!(args.client_udp_compact_mark_fast_lane);
    let args =
        Args::try_parse_from(["av-mesh", "--client-udp-compact-mark-fast-lane", "false"]).unwrap();
    assert!(!args.client_udp_compact_mark_fast_lane);
}

// ------------------------------------------------------------------ the measurement

/// Processor time of this process's threads whose names start with one of
/// `prefixes`, in nanoseconds.
fn threads_cpu_ns(prefixes: &[&str]) -> u64 {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return 0;
    };
    let mut total = 0;
    for task in tasks.flatten() {
        let path = task.path();
        let Ok(comm) = std::fs::read_to_string(path.join("comm")) else {
            continue;
        };
        if !prefixes.iter().any(|prefix| comm.starts_with(prefix)) {
            continue;
        }
        if let Some(ns) = std::fs::read_to_string(path.join("schedstat"))
            .ok()
            .and_then(|stat| stat.split_whitespace().next()?.parse::<u64>().ok())
        {
            total += ns;
        }
    }
    total
}

struct Measured {
    parts: u64,
    wire_bytes: u64,
    datagrams: u64,
    edge_cpu: f64,
    /// Cut-through parts, and those completed from the other parent's
    /// block (sent whole again).
    cut_through: (u64, u64),
    /// Publication to arrival, p50 and p99, ms.
    latency_ms: (f64, f64),
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// One run: an edge of `feed`, `listeners` listeners of `mode` on clean
/// paths, the stream for `seconds`: the listeners' wire bytes and the
/// edge's processor time over the last `seconds - 1` seconds.
fn measure(feed: Feed, payload: Payload, mode: Mode, listeners: usize, seconds: u64) -> Measured {
    let runtime = |name: &str, workers: usize| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_name(name)
            .enable_all()
            .build()
            .unwrap()
    };
    let edge_rt = runtime("edge-rt", 2);
    let client_rt = runtime("client-rt", 4);
    let main_rt = runtime("pub-rt", 2);
    let stream_id = if payload.video() { 2 } else { 4001 };
    let (edge, first) = edge_rt
        .block_on(async { tokio::spawn(AnyEdge::start(feed, payload, stream_id, 30)).await })
        .unwrap();
    let edge = Arc::new(edge);
    let shared = Shared::new();
    let client_handle = client_rt.handle().clone();
    let measured = main_rt.block_on(async {
        let mut runs = Vec::new();
        for index in 0..listeners {
            let (tx, rx) = tokio::sync::oneshot::channel();
            runs.push(client_handle.spawn(listen(
                edge.client_port(),
                stream_id,
                mode,
                true,
                PathLoss::Clean,
                index as u64,
                Arc::clone(&shared),
                tx,
            )));
            timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
        }
        let per_second = 1_000_000 / payload.part_us();
        let total = first + per_second * seconds;
        let publisher = {
            let edge = Arc::clone(&edge);
            tokio::spawn(
                async move { publish_paced(&edge, payload, stream_id, first, total).await },
            )
        };
        // Settle for a second (the listeners measure their paths and ask for
        // their repair), then measure.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let head_before = edge
            .cache()
            .stream_position_for_id(stream_id)
            .await
            .map_or(0, |(_, last)| last as u64);
        let prefixes = ["edge-rt", "needletail-fast", "needletail-cut"];
        let cpu_before = threads_cpu_ns(&prefixes);
        shared.measuring.store(true, Ordering::Relaxed);
        let started = Instant::now();
        tokio::time::sleep(Duration::from_secs(seconds.saturating_sub(1).max(1))).await;
        shared.measuring.store(false, Ordering::Relaxed);
        let elapsed = started.elapsed().as_nanos() as f64;
        let cpu = threads_cpu_ns(&prefixes) - cpu_before;
        let head_after = edge
            .cache()
            .stream_position_for_id(stream_id)
            .await
            .map_or(0, |(_, last)| last as u64);
        publisher.await.unwrap();
        shared.last.store(total - 1, Ordering::Relaxed);
        for run in runs {
            let _ = timeout(Duration::from_secs(10), run).await;
        }
        let stats = edge.stats();
        let mut latencies = shared.latencies_us.lock().unwrap().clone();
        latencies.sort_unstable();
        let at = |q: f64| {
            latencies
                .get(((latencies.len().max(1) - 1) as f64 * q) as usize)
                .map_or(0.0, |us| *us as f64 / 1_000.0)
        };
        Measured {
            latency_ms: (at(0.5), at(0.99)),
            cut_through: (
                stats.cut_through_parts.load(Ordering::Relaxed),
                stats.cut_through_fallbacks.load(Ordering::Relaxed),
            ),
            parts: head_after - head_before,
            wire_bytes: shared.wire_bytes.load(Ordering::Relaxed),
            datagrams: shared.datagrams.load(Ordering::Relaxed),
            edge_cpu: cpu as f64 / elapsed,
        }
    });
    let edge = Arc::try_unwrap(edge)
        .ok()
        .expect("the edge is no longer shared");
    edge_rt.block_on(edge.stop());
    measured
}

#[test]
#[ignore = "a measurement: run it alone, in release"]
fn bytes_on_the_wire_per_listener() {
    let listeners = env_u64("COMPACT_BENCH_LISTENERS", 20) as usize;
    let seconds = env_u64("COMPACT_BENCH_SECONDS", 5);
    let feeds = match std::env::var("COMPACT_BENCH_FEED").as_deref() {
        Ok("mesh") => vec![Feed::Mesh],
        Ok("both") => vec![Feed::Fast, Feed::Mesh],
        _ => vec![Feed::Fast],
    };
    let payloads = match std::env::var("COMPACT_BENCH_PAYLOAD").as_deref() {
        Ok("opus") => vec![Payload::Opus],
        Ok("pcm") => vec![Payload::Pcm],
        Ok("4k") => vec![Payload::Video4k],
        _ => vec![Payload::Opus, Payload::Pcm, Payload::Video4k],
    };
    let mut lines = Vec::new();
    for feed in &feeds {
        for payload in &payloads {
            let idle = measure(*feed, *payload, Mode::Compact, 0, seconds);
            for mode in [Mode::Before, Mode::Compact] {
                let measured = measure(*feed, *payload, mode, listeners, seconds);
                let per_listener_part =
                    measured.wire_bytes as f64 / (measured.parts.max(1) * listeners as u64) as f64;
                let parts_per_second = 1e6 / payload.part_us() as f64;
                let bits = per_listener_part * 8.0 * parts_per_second;
                let datagrams =
                    measured.datagrams as f64 / (measured.parts.max(1) * listeners as u64) as f64;
                let cpu_per_listener = (measured.edge_cpu - idle.edge_cpu) / listeners as f64;
                let line = format!(
                    "{feed:?} {:<32} {:<7} {listeners} listeners: {:>9.0} B/part/listener \
                     ({:>5.2} datagrams), {:>8.3} Mbit/s a listener = {:>5.2}x the part bytes, \
                     {:>5.2}x the codec rate; edge {:>5.1} % of a CPU ({:>5.1} % idle), \
                     {:>6.3} % of a CPU a listener, {:>5} listeners at 100 % of one CPU; \
                     publish to arrival p50 {:.3} ms p99 {:.3} ms; \
                     cut-through parts {} (whole again {})",
                    payload.name(),
                    format!("{mode:?}"),
                    per_listener_part,
                    datagrams,
                    bits / 1e6,
                    per_listener_part / payload.media_len() as f64,
                    bits / payload.codec_bits_per_second(),
                    measured.edge_cpu * 100.0,
                    idle.edge_cpu * 100.0,
                    cpu_per_listener * 100.0,
                    if cpu_per_listener > 0.0 {
                        format!("{:.0}", (1.0 - idle.edge_cpu) / cpu_per_listener)
                    } else {
                        "-".into()
                    },
                    measured.latency_ms.0,
                    measured.latency_ms.1,
                    measured.cut_through.0,
                    measured.cut_through.1,
                );
                println!("{line}");
                lines.push(line);
            }
        }
    }
    println!("\n{}", lines.join("\n"));
}
