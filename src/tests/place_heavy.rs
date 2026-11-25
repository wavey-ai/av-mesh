//! Place240 in process: 4K at 247 Mbit/s (`PH_MBPS`) to a region of small
//! edges, as cmpd8 carried it, with the rules of place240 on or off.
//!
//! A region of two or three real edges (fed on the fast plane, their
//! client UDP sockets behind a proxy each at the address they advertise),
//! one entry point that answers `ASSIGN` from the region's
//! advertisements (each edge's own heavy advertisement,
//! `AppRouter::heavy_advert`), a publisher of 24 frames a second, each
//! stamped with its due time, and viewers that each run the director over
//! the session on a socket of their own, as `needletail-udp-client` does.
//!
//! The proxy carries everything between a viewer and its edge, and can
//! throttle what the edge sends to a fraction of the stream's rate: the
//! datagrams keep coming, later and later, as cmpd8's stalled send side
//! delivered them (fast-lane parts 0.5-1.1 s late, then the cache-fed lane
//! 3-5.7 s late, no silence for any rule to see). The publisher can hold
//! back its sends for a while and then send what it held: an upstream stall
//! that every edge sees.
//!
//! - (a) four viewers that join within 100 ms spread over the edges by
//!   their committed cost (2+1+1 on three edges);
//! - (b) the edge carrying two of them is throttled for 3 s: its viewers
//!   move (`late-parts`) and their hold is bounded; the others see nothing;
//! - (c) with the edges at budget the next heavy viewer is refused
//!   (`edge-busy`, counted), a heavy reader is refused (the entry's 429 and
//!   the edge's own cap), and an audio crowd joins and plays untouched;
//! - (d) an upstream stall that every edge sees moves each viewer at most
//!   once.
//!
//! Each runs with the rules off first (the "before", printed) and on (the
//! "after", printed and asserted). They take turns ([`SERIAL`]); the 247
//! Mbit/s stream is a load of its own, and on a busy box the loopback drops
//! datagrams (parts go missing at every edge, and viewers move for it), so
//! they are ignored by default: run them alone, in release:
//! `cargo test --release place_heavy -- --ignored --nocapture
//! --test-threads 1` (`PH_MBPS` sets the rate).

use super::compact_lane::Payload;
use super::fast_lane::LaneEdge;
use super::fast_lease::loopback;
use super::two_paths::recv_stamped;
use super::*;
use av_mesh::client_director::{Action, Director, DirectorConfig, LegKey, Replacement};
use av_mesh::client_quality::QualityConfig;
use av_mesh::client_session::{Received, SessionConfig, UdpSession};
use av_mesh::client_udp::ObjectKind as UdpObjectKind;
use std::sync::atomic::{AtomicBool, AtomicU64};
use media_object::{ClockConfidence, ClockTimestamp, StageTimestamp};
use std::sync::{Condvar, Mutex as StdMutex};

const STREAM: u64 = 2_401;
const AUDIO: u64 = 2_402;
const REGION: &str = "place-region";
const FPS: u64 = 24;
const SYMBOL: u16 = 1_088;
const ADVERTISE_EVERY: Duration = Duration::from_millis(200);
const ENTRY_OFFSET: u8 = 5;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn mbps() -> u64 {
    std::env::var("PH_MBPS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(247)
}

fn rate_bps() -> u64 {
    mbps() * 1_000_000
}

// ------------------------------------------------------------------ source

/// The canonical envelope of 4K frame `sequence` of `media_bytes`, as
/// av-contrib builds it, packaged and published at `published_us`.
fn frame_envelope(sequence: u64, media_bytes: usize, published_us: u64) -> Vec<u8> {
    let mut media = vec![0_u8; media_bytes];
    for (index, byte) in media.iter_mut().enumerate() {
        *byte = ((index as u64 * 31 + sequence * 7) % 251) as u8;
    }
    let ns = i64::try_from(published_us * 1_000).unwrap();
    let stamp = |ns: i64| {
        ClockTimestamp::new(
            ns,
            "system:realtime:av-contrib",
            ClockConfidence::estimated(1_000_000),
        )
        .unwrap()
    };
    let payload = encode_test_fmp4_slot(Some(b"ftyp-moov-avc1"), &media);
    let key = media_object::ObjectKey::for_payload(
        "default",
        STREAM.to_string(),
        "muxed-fmp4",
        0,
        0,
        sequence,
        1,
        &payload,
    )
    .unwrap();
    let init = media_object::ObjectKey::for_payload(
        "default",
        STREAM.to_string(),
        "muxed-fmp4-init",
        0,
        0,
        0,
        1,
        b"init",
    )
    .unwrap();
    let part_us = 1_000_000 / FPS;
    let mut builder = MediaObject::builder(key, ObjectKind::Media, payload.to_vec())
        .with_deadline(stamp(ns + 600_000_000_000))
        .with_stage_timestamp(StageTimestamp::new(Stage::Packaged, stamp(ns)))
        .with_stage_timestamp(StageTimestamp::new(Stage::Published, stamp(ns)))
        .with_keyframe(sequence % FPS == 0)
        .with_configuration_epoch(1)
        .with_dependency(init);
    for (name, value) in [
        ("container", "fmp4".to_string()),
        ("duration-ms", part_us.div_ceil(1_000).to_string()),
        ("duration-us", part_us.to_string()),
        ("payload-format", "fmp4-slot-v1".to_string()),
        ("scheduler-class", "video".to_string()),
        ("track-composition", "video".to_string()),
        ("video-codec", "avc1.640033".to_string()),
        ("hls-msn", (sequence / FPS).to_string()),
        ("hls-part", (sequence % FPS).to_string()),
        ("hls-part-target-ms", part_us.div_ceil(1_000).to_string()),
        ("hls-part-target-us", part_us.to_string()),
        ("hls-target-duration-ms", "1000".to_string()),
        ("hls-target-duration-us", "1000000".to_string()),
    ] {
        builder = builder.with_metadata(name, value.into_bytes());
    }
    media_object::encode(&builder.build().unwrap()).unwrap()
}

/// A frame's symbols: sources (forwarder 1) and a twentieth of repair
/// (forwarder 2), as the forwarders send a video stream.
type Symbols = (Vec<Vec<u8>>, Vec<Vec<u8>>);

fn frame_symbols(sequence: u64, published_us: u64) -> Symbols {
    let media_bytes = (rate_bps() / 8 / FPS) as usize;
    let envelope = frame_envelope(sequence, media_bytes, published_us);
    let sources = envelope.len().div_ceil(usize::from(SYMBOL)) as u32;
    fast_test_symbols(
        &envelope,
        if sequence % FPS == 0 {
            relay_session::MediaPriority::VideoKey
        } else {
            relay_session::MediaPriority::VideoDelta
        },
        SYMBOL,
        u16::try_from(sources.max(400)).unwrap(),
        sources.div_ceil(20),
        published_us + 600_000_000,
    )
}

/// The 4K publisher: frames made ahead on a few threads, each stamped with
/// its due time, sent at that time to every edge that runs (unless held).
struct Source {
    start_us: u64,
    start: Instant,
    /// Frames made, by sequence, and the next sequence a maker takes.
    made: Arc<(StdMutex<BTreeMap<u64, (u64, Symbols)>>, Condvar)>,
    /// Each frame's publication stamp, by sequence.
    stamps: Arc<StdMutex<Vec<u64>>>,
    next: Arc<AtomicU64>,
    /// The newest sequence sent.
    published: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    /// Sends held while this is in the future (Unix µs): an upstream stall.
    hold_until_us: Arc<AtomicU64>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl Source {
    fn interval_us() -> u64 {
        1_000_000 / FPS
    }

    fn due_us(start_us: u64, sequence: u64) -> u64 {
        start_us + sequence * Self::interval_us()
    }

    fn start(edges: Arc<StdMutex<Vec<Option<Arc<LaneEdge>>>>>) -> Self {
        let start_us = now_unix_us() + 300_000;
        let start = Instant::now() + Duration::from_millis(300);
        let made: Arc<(StdMutex<BTreeMap<u64, (u64, Symbols)>>, Condvar)> = Arc::default();
        let stamps: Arc<StdMutex<Vec<u64>>> = Arc::default();
        let next = Arc::new(AtomicU64::new(0));
        let published = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let hold_until_us = Arc::new(AtomicU64::new(0));
        let mut threads = Vec::new();
        for maker in 0..8 {
            let (made, next, running) = (Arc::clone(&made), Arc::clone(&next), Arc::clone(&running));
            threads.push(
                std::thread::Builder::new()
                    .name(format!("ph-maker-{maker}"))
                    .spawn(move || {
                        while running.load(Ordering::Relaxed) {
                            let sequence = next.fetch_add(1, Ordering::Relaxed);
                            let due = Self::due_us(start_us, sequence);
                            // At most 0.6 s ahead of its time.
                            while running.load(Ordering::Relaxed)
                                && now_unix_us() + 600_000 < due
                            {
                                std::thread::sleep(Duration::from_millis(5));
                            }
                            // Stamped when it is made, at its due time at the
                            // earliest: a source that cannot keep up on a busy
                            // box sends fewer frames, not late ones.
                            let stamp = due.max(now_unix_us());
                            let symbols = frame_symbols(sequence, stamp);
                            let (map, ready) = &*made;
                            map.lock().unwrap().insert(sequence, (stamp, symbols));
                            ready.notify_all();
                        }
                    })
                    .unwrap(),
            );
        }
        {
            let stamps = Arc::clone(&stamps);
            let (made, published, running, hold) = (
                Arc::clone(&made),
                Arc::clone(&published),
                Arc::clone(&running),
                Arc::clone(&hold_until_us),
            );
            threads.push(
                std::thread::Builder::new()
                    .name("ph-sender".into())
                    .spawn(move || {
                        let mut sequence = 0_u64;
                        while running.load(Ordering::Relaxed) {
                            let due = start + Duration::from_micros(sequence * Self::interval_us());
                            if let Some(wait) = due.checked_duration_since(Instant::now()) {
                                std::thread::sleep(wait);
                            }
                            while running.load(Ordering::Relaxed)
                                && now_unix_us() < hold.load(Ordering::Relaxed)
                            {
                                std::thread::sleep(Duration::from_millis(1));
                            }
                            let (stamp, symbols) = {
                                let (map, ready) = &*made;
                                let mut map = map.lock().unwrap();
                                loop {
                                    if let Some(symbols) = map.remove(&sequence) {
                                        break symbols;
                                    }
                                    if !running.load(Ordering::Relaxed) {
                                        return;
                                    }
                                    map = ready.wait_timeout(map, Duration::from_millis(50)).unwrap().0;
                                }
                            };
                            let running_edges: Vec<Arc<LaneEdge>> =
                                edges.lock().unwrap().iter().flatten().cloned().collect();
                            for edge in &running_edges {
                                for symbol in &symbols.0 {
                                    send_from(edge.forwarder_fd(0), symbol, edge.relay_port(0, true));
                                }
                                for symbol in &symbols.1 {
                                    send_from(edge.forwarder_fd(1), symbol, edge.relay_port(1, true));
                                }
                            }
                            stamps.lock().unwrap().push(stamp);
                            published.store(sequence, Ordering::Relaxed);
                            sequence += 1;
                        }
                    })
                    .unwrap(),
            );
        }
        Self {
            start_us,
            start,
            made,
            stamps,
            next,
            published,
            running,
            hold_until_us,
            threads,
        }
    }

    fn published_us(&self, sequence: u64) -> u64 {
        self.stamps
            .lock()
            .unwrap()
            .get(sequence as usize)
            .copied()
            .unwrap_or_else(|| Self::due_us(self.start_us, sequence))
    }

    /// The rate sent so far, Mbit/s.
    fn sent_mbps(&self) -> f64 {
        let stamps = self.stamps.lock().unwrap();
        let (Some(first), Some(last)) = (stamps.first(), stamps.last()) else {
            return 0.0;
        };
        let seconds = (last - first) as f64 / 1e6;
        (stamps.len() as f64 - 1.0) * (rate_bps() / FPS) as f64 / seconds.max(0.001) / 1e6
    }

    /// Hold every send for `hold` from now, then send what was held.
    fn stall(&self, hold: Duration) {
        self.hold_until_us
            .store(now_unix_us() + hold.as_micros() as u64, Ordering::Relaxed);
    }

    fn stop(mut self) {
        self.running.store(false, Ordering::Relaxed);
        self.made.1.notify_all();
        let _ = (&self.next, self.start);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

/// An audio stream (2.5 ms Opus) to every edge from both forwarders.
fn audio_publisher(
    edges: Arc<StdMutex<Vec<Option<Arc<LaneEdge>>>>>,
    running: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("ph-audio".into())
        .spawn(move || {
            let payload = Payload::Opus;
            let start = Instant::now();
            let mut sequence = 0_u64;
            while running.load(Ordering::Relaxed) {
                let due = start + Duration::from_micros(payload.part_us() * sequence);
                if let Some(wait) = due.checked_duration_since(Instant::now()) {
                    std::thread::sleep(wait);
                }
                let (envelope, _) = payload.envelope(AUDIO, sequence);
                let (sources, repairs) = payload.symbols(&envelope);
                let running_edges: Vec<Arc<LaneEdge>> =
                    edges.lock().unwrap().iter().flatten().cloned().collect();
                for edge in &running_edges {
                    for symbol in &sources {
                        send_from(edge.forwarder_fd(0), symbol, edge.relay_port(0, false));
                        send_from(edge.forwarder_fd(1), symbol, edge.relay_port(1, false));
                    }
                    for symbol in &repairs {
                        send_from(edge.forwarder_fd(1), symbol, edge.relay_port(1, false));
                    }
                }
                sequence += 1;
            }
        })
        .unwrap()
}

fn send_from(fd: std::os::fd::RawFd, datagram: &[u8], to: SocketAddr) {
    let SocketAddr::V4(to) = to else {
        unreachable!("loopback")
    };
    let address = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: to.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from(*to.ip()).to_be(),
        },
        sin_zero: [0; 8],
    };
    // SAFETY: a valid fd, a buffer and an address that live for the call.
    unsafe {
        libc::sendto(
            fd,
            datagram.as_ptr().cast(),
            datagram.len(),
            0,
            std::ptr::from_ref(&address).cast(),
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        );
    }
}

// ------------------------------------------------------------------- proxy

/// Between viewers and an edge, at the address the edge advertises: what
/// the edge sends can be throttled to a share of the stream's rate (its
/// send side stalled: the datagrams come later and later, none lost).
struct Proxy {
    /// Thousandths of the stream's rate the edge's sends are held to (0:
    /// not held).
    throttle: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
}

impl Proxy {
    fn start(front: SocketAddr, edge: SocketAddr) -> Self {
        let throttle = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let socket = std::net::UdpSocket::bind(front).unwrap();
        let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(8 << 20);
        let _ = socket2::SockRef::from(&socket).set_send_buffer_size(8 << 20);
        socket
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let (throttle_in, running_in) = (Arc::clone(&throttle), Arc::clone(&running));
        std::thread::Builder::new()
            .name("ph-proxy".into())
            .spawn(move || {
                let mut upstreams: HashMap<SocketAddr, std::net::UdpSocket> = HashMap::new();
                let mut buf = vec![0_u8; 65_536];
                while running_in.load(Ordering::Relaxed) {
                    let Ok((len, client)) = socket.recv_from(&mut buf) else {
                        continue;
                    };
                    let upstream = upstreams.entry(client).or_insert_with(|| {
                        let upstream = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
                        let _ = socket2::SockRef::from(&upstream).set_recv_buffer_size(16 << 20);
                        // Not connected: the edge sends a video stream from
                        // its video socket, another port.
                        upstream.set_nonblocking(true).unwrap();
                        let down = upstream.try_clone().unwrap();
                        let front = socket.try_clone().unwrap();
                        let (throttle, running) = (Arc::clone(&throttle_in), Arc::clone(&running_in));
                        std::thread::Builder::new()
                            .name("ph-proxy-down".into())
                            .spawn(move || downstream(down, front, client, throttle, running))
                            .unwrap();
                        upstream
                    });
                    let _ = upstream.send_to(&buf[..len], edge);
                }
            })
            .unwrap();
        Self { throttle, running }
    }

    fn throttle(&self, permille: u64) {
        self.throttle.store(permille, Ordering::Relaxed);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

/// One viewer's way back: the edge's datagrams to it, through the queue
/// that the throttle drains at its share of the stream's rate.
fn downstream(
    upstream: std::net::UdpSocket,
    front: std::net::UdpSocket,
    client: SocketAddr,
    throttle: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
) {
    let mut queue: VecDeque<Vec<u8>> = VecDeque::new();
    let mut buf = vec![0_u8; 65_536];
    let mut credit = 0.0_f64;
    let mut last = Instant::now();
    while running.load(Ordering::Relaxed) {
        let mut got = false;
        for _ in 0..512 {
            match upstream.recv_from(&mut buf) {
                Ok((len, _)) => {
                    queue.push_back(buf[..len].to_vec());
                    got = true;
                }
                Err(_) => break,
            }
        }
        let permille = throttle.load(Ordering::Relaxed);
        let now = Instant::now();
        if permille == 0 {
            while let Some(datagram) = queue.pop_front() {
                let _ = front.send_to(&datagram, client);
            }
            credit = 0.0;
        } else {
            let rate = rate_bps() as f64 / 8.0 * permille as f64 / 1_000.0;
            credit = (credit + now.duration_since(last).as_secs_f64() * rate).min(256_000.0);
            while queue.front().is_some_and(|datagram| datagram.len() as f64 <= credit) {
                let datagram = queue.pop_front().unwrap();
                credit -= datagram.len() as f64;
                let _ = front.send_to(&datagram, client);
            }
        }
        last = now;
        if !got {
            std::thread::sleep(Duration::from_micros(150));
        }
    }
}

// ------------------------------------------------------------------ region

/// The rules on (`true`) or off (the "before": no heavy session, no
/// client rule).
fn heavy_config(rules: bool, egress_max_bps: u64) -> heavy::HeavyConfig {
    heavy::HeavyConfig {
        session_bps: if rules {
            heavy::DEFAULT_HEAVY_SESSION_BPS
        } else {
            u64::MAX
        },
        cpus: 2,
        egress_max_bps,
        distress: heavy::DistressConfig {
            // The test box's other work is not this edge's.
            core_busy_permille: 0,
            ..heavy::DistressConfig::default()
        },
        ..heavy::HeavyConfig::default()
    }
}

struct Slot {
    node: String,
    front: SocketAddr,
    proxy: Proxy,
}

struct Region {
    port: u16,
    slots: Vec<Slot>,
    edges: Arc<StdMutex<Vec<Option<Arc<LaneEdge>>>>>,
    entry: AppRouter,
    entry_mesh: Arc<CacheMeshHandle>,
    entry_url: String,
    entry_shutdown: watch::Sender<()>,
    entry_task: tokio::task::JoinHandle<anyhow::Result<()>>,
    source: Option<Source>,
    audio_running: Arc<AtomicBool>,
    audio: Option<std::thread::JoinHandle<()>>,
    advertiser: tokio::task::JoinHandle<()>,
}

fn region_port(base: u8) -> u16 {
    'search: for _ in 0..64 {
        let probe = std::net::UdpSocket::bind((loopback(base), 0)).unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        for offset in 0..=ENTRY_OFFSET + 1 {
            if std::net::UdpSocket::bind((loopback(base + offset), port)).is_err() {
                continue 'search;
            }
        }
        return port;
    }
    panic!("no port free at 127.0.0.{base}+");
}

fn edge_snapshot(node: &str, front: SocketAddr, edge: &LaneEdge, head: u64) -> MeshSnapshot {
    let mut snapshot =
        telemetry_snapshot_for_tests(node, REGION, "test-continent", 51.6, -0.2, Vec::new(), STREAM);
    let mut service = EdgeServiceSnapshot::from_node(
        &snapshot.node,
        Some(format!("https://{}:19444/live", front.ip())),
        "edge",
        EdgeLoadSnapshot::default(),
    );
    let legs = edge.stats.legs().map_or(0, |legs| legs.subscribers);
    service.load_permille = 30 + 5 * legs as u32;
    service.udp_subscribers = Some(legs);
    service.playlists = [(STREAM, rate_bps()), (AUDIO, 300_000)]
        .into_iter()
        .map(|(stream_id, bps)| PlaylistHolding {
            request_rate: None,
            stream_id,
            head_part: head,
            bitrate_bps: Some(bps),
            sessions: 0,
            video: false,
        })
        .collect();
    // The edge's own heavy advertisement, as its `local_mesh_snapshot`
    // makes it.
    service.heavy = edge
        .router
        .heavy_advert([(STREAM, Some(rate_bps())), (AUDIO, Some(300_000))].into_iter());
    snapshot.edge_service = Some(service);
    snapshot
}

impl Region {
    async fn start(base: u8, edges: usize, rules: bool, egress_max_bps: u64, audio: bool) -> Self {
        let port = region_port(base);
        let config = heavy_config(rules, egress_max_bps);
        let mut slots = Vec::new();
        let mut running = Vec::new();
        for index in 0..edges {
            let ip = loopback(base + index as u8);
            let node = format!("edge-{}", index + 1);
            let real = {
                let probe = std::net::UdpSocket::bind((ip, 0)).unwrap();
                probe.local_addr().unwrap()
            };
            let name = node.clone();
            let edge = LaneEdge::start_full(
                ip,
                real,
                Duration::ZERO,
                64,
                true,
                // The deploy's tee rings: LaneEdge's 8,192 entries hold no
                // 4K frame of these rates (hls_beside_4k).
                |class| class.tee_capacity = fast_plane::relay::DEFAULT_TEE_CAPACITY,
                move |router| {
                    let mut router = router.with_heavy(config);
                    router.node = mesh_node_for_tests(&name, REGION, "test-continent", 51.6, -0.2);
                    router
                },
            )
            .await;
            edge.router
                .edge_load
                .egress
                .set_playlist_bps(STREAM, rate_bps());
            edge.router.edge_load.egress.set_playlist_bps(AUDIO, 300_000);
            let front = SocketAddr::from((ip, port));
            let proxy = Proxy::start(front, edge.client_port);
            slots.push(Slot { node, front, proxy });
            running.push(Some(Arc::new(edge)));
        }
        let edges = Arc::new(StdMutex::new(running));
        let ip = loopback(base + ENTRY_OFFSET);
        let cache = LiveTsCache::new(STREAM, Duration::from_millis(50), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let mut entry = app_router_for_tests_with_telemetry(
            cache,
            Arc::clone(&mesh),
            TelemetryAggregator::default(),
        )
        .with_client_udp_port(Some(port))
        .with_heavy(config);
        entry.cache_mesh_role = CacheMeshRole::Distributor;
        entry.node = mesh_node_for_tests("entry", REGION, "test-continent", 51.6, -0.2);
        let socket = UdpSocket::bind((ip, port)).await.unwrap();
        let mut udp_config = edge_udp::EdgeUdpConfig::with_defaults(1_200, 16);
        udp_config.assign = true;
        udp_config.assign_confirms_per_source = 4_096;
        let (entry_shutdown, shutdown_rx) = watch::channel(());
        let entry_task = tokio::spawn(edge_udp::run_edge_udp(
            edge_udp::EdgeSockets::single(socket),
            entry.clone(),
            udp_config,
            Arc::clone(&entry.edge_udp_stats),
            Vec::new(),
            shutdown_rx,
        ));
        let source = Source::start(Arc::clone(&edges));
        let audio_running = Arc::new(AtomicBool::new(audio));
        let audio = audio.then(|| audio_publisher(Arc::clone(&edges), Arc::clone(&audio_running)));
        let advertiser = {
            let (edges, entry, published) = (
                Arc::clone(&edges),
                entry.clone(),
                Arc::clone(&source.published),
            );
            let fronts: Vec<(String, SocketAddr)> =
                slots.iter().map(|slot| (slot.node.clone(), slot.front)).collect();
            tokio::spawn(async move {
                loop {
                    let head = published.load(Ordering::Relaxed);
                    let snapshots: Vec<MeshSnapshot> = {
                        let edges = edges.lock().unwrap();
                        fronts
                            .iter()
                            .zip(edges.iter())
                            .filter_map(|((node, front), edge)| {
                                Some(edge_snapshot(node, *front, edge.as_ref()?, head))
                            })
                            .collect()
                    };
                    for snapshot in snapshots {
                        entry.telemetry.ingest_snapshot(snapshot).await;
                    }
                    tokio::time::sleep(ADVERTISE_EVERY).await;
                }
            })
        };
        let region = Self {
            port,
            slots,
            edges,
            entry,
            entry_mesh: mesh,
            entry_url: format!("https://{ip}:19444"),
            entry_shutdown,
            entry_task,
            source: Some(source),
            audio_running,
            audio,
            advertiser,
        };
        let published = Arc::clone(&region.source.as_ref().unwrap().published);
        until("the stream runs at every edge", Duration::from_secs(10), || {
            published.load(Ordering::Relaxed) > 2 * FPS
        })
        .await;
        tokio::time::sleep(ADVERTISE_EVERY * 3).await;
        region
    }

    fn edge(&self, index: usize) -> Arc<LaneEdge> {
        Arc::clone(self.edges.lock().unwrap()[index].as_ref().unwrap())
    }

    fn legs_at(&self, index: usize, stream: u64) -> u64 {
        self.edge(index)
            .stats
            .subscribers_by_stream_and_lane()
            .get(&stream)
            .map_or(0, |(fast, other)| fast + other)
    }

    /// What the region and the viewers are doing (a failure's message).
    async fn describe(&self, crowd: &Crowd) {
        let mut lines = Vec::new();
        for index in 0..self.slots.len() {
            let edge = self.edge(index);
            lines.push(format!(
                "edge-{}: plane {:?} head {:?} legs {:?} refusals {} subs {}",
                index + 1,
                {
                    let plane = edge.cache.relay_ingress_snapshot().fast_plane;
                    (plane.video_received, plane.not_admitted, plane.tee_dropped)
                },
                edge.head(STREAM).await,
                edge.stats.subscribers_by_stream_and_lane(),
                edge.stats.refusals.load(Ordering::Relaxed),
                edge.stats.subscriptions.load(Ordering::Relaxed),
            ));
        }
        let stats = &self.entry.edge_udp_stats;
        lines.push(format!(
            "entry: assignments {} dropped {} confirms {} published {}",
            stats.assignments.load(Ordering::Relaxed),
            stats.assignments_dropped.load(Ordering::Relaxed),
            stats.assignment_confirms.load(Ordering::Relaxed),
            self.source().published.load(Ordering::Relaxed),
        ));
        lines.push(format!("viewers: {:?}", crowd.nodes()));
        eprintln!("{}", lines.join("\n"));
    }

    fn source(&self) -> &Source {
        self.source.as_ref().unwrap()
    }

    async fn shutdown(mut self) {
        self.advertiser.abort();
        if let Some(source) = self.source.take() {
            let _ = tokio::task::spawn_blocking(move || source.stop()).await;
        }
        self.audio_running.store(false, Ordering::Relaxed);
        if let Some(audio) = self.audio.take() {
            let _ = tokio::task::spawn_blocking(move || audio.join()).await;
        }
        let edges: Vec<Arc<LaneEdge>> = self.edges.lock().unwrap().iter_mut().filter_map(Option::take).collect();
        for edge in edges {
            let mut edge = edge;
            loop {
                match Arc::try_unwrap(edge) {
                    Ok(edge) => {
                        edge.kill().await;
                        break;
                    }
                    Err(shared) => {
                        edge = shared;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
        }
        let _ = self.entry_shutdown.send(());
        let _ = (&mut self.entry_task).await;
        self.entry_mesh.shutdown();
        drop(self.slots);
    }
}

async fn until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(limit, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}: not within {limit:?}"));
}

// ---------------------------------------------------------------- viewers

/// What a viewer received and what its director did.
#[derive(Debug, Default)]
struct Run {
    sid: String,
    /// Each part's first arrival: kernel receive time (Unix µs) and the
    /// node of the leg that brought it.
    parts: BTreeMap<u64, (u64, String)>,
    replacements: Vec<Replacement>,
    stats: av_mesh::client_director::DirectorStats,
    /// The node of its first live leg, and every node it held.
    first_node: Option<String>,
    refused: Vec<String>,
    degraded_late: u64,
    degraded_missing: u64,
}

#[derive(Debug, Default)]
struct Live {
    node: StdMutex<Option<String>>,
    stop: AtomicBool,
}

async fn view(
    port: u16,
    entry: String,
    sid: String,
    stream: u64,
    rules: bool,
    live: Arc<Live>,
) -> Run {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(16 << 20);
    {
        use std::os::fd::AsRawFd;
        let on: libc::c_int = 1;
        // SAFETY: a valid fd and an int option of its size.
        unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_TIMESTAMPNS,
                (&on as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    let config = SessionConfig {
        quality: QualityConfig {
            enabled: rules,
            ..QualityConfig::default()
        },
        ..SessionConfig::new(stream)
    };
    let mut session = UdpSession::new(config, Instant::now());
    let mut director_config = DirectorConfig::new(stream, sid.clone(), vec![entry]);
    director_config.paths = 2;
    director_config.spare = true;
    director_config.assignment_udp_port = port;
    director_config.udp_port = Some(port);
    director_config.nonce_seed = sid.bytes().fold(0, |hash, byte| hash * 31 + u64::from(byte));
    let mut director = Director::new(director_config, Instant::now());
    let mut run = Run {
        sid,
        ..Run::default()
    };
    let mut outbox: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
    let mut actions: Vec<Action> = Vec::new();
    let mut buf = vec![0_u8; 65_536];
    let mut stopped_at: Option<Instant> = None;
    loop {
        let now = Instant::now();
        if live.stop.load(Ordering::Relaxed) {
            let since = *stopped_at.get_or_insert(now);
            if now.duration_since(since) >= Duration::from_millis(300) {
                break;
            }
        }
        session.queued = {
            use std::os::fd::AsRawFd;
            let mut queued: libc::c_int = 0;
            // SAFETY: FIONREAD writes one int to the pointer given.
            let result = unsafe { libc::ioctl(socket.as_raw_fd(), libc::FIONREAD, &mut queued) };
            result == 0 && queued > 0
        };
        session.poll_legs(now, &mut |to, datagram| outbox.push((to, datagram.to_vec())));
        director.poll(&mut session, now, &mut actions);
        for (to, datagram) in outbox.drain(..) {
            let _ = socket.send_to(&datagram, to).await;
        }
        for action in actions.drain(..) {
            if let Action::Send { to, datagram } = action {
                let _ = socket.send_to(&datagram, to).await;
            }
        }
        let held: Option<String> = director
            .legs()
            .iter()
            .find(|leg| leg.live && leg.leaving.is_none() && leg.hedge_for.is_none())
            .map(|leg| leg.node.clone());
        if run.first_node.is_none() {
            run.first_node.clone_from(&held);
        }
        *live.node.lock().unwrap() = held;
        let wake = session
            .next_wake()
            .min(director.next_wake())
            .min(now + Duration::from_millis(20));
        let Ok(Ok((len, peer, kernel_us))) = tokio::time::timeout(
            wake.saturating_duration_since(now),
            recv_stamped(&socket, &mut buf),
        )
        .await
        else {
            continue;
        };
        let datagram = &buf[..len];
        let now = Instant::now();
        if director.on_datagram(&mut session, peer, datagram, now, &mut actions) {
            continue;
        }
        if let Some(av_mesh::client_udp::Control::Refused { reason, .. }) =
            av_mesh::client_udp::Control::decode(datagram)
        {
            run.refused.push(reason);
        }
        let outcome = session.on_datagram(peer, datagram, now, &mut |to, datagram| {
            outbox.push((to, datagram.to_vec()))
        });
        match outcome {
            Received::Media => {}
            Received::Subscribed { .. } if session.has_objects() => {}
            _ => continue,
        }
        let node = director
            .legs()
            .iter()
            .find(|leg| leg.addr == peer)
            .map_or_else(String::new, |leg| leg.node.clone());
        while let Some(object) = session.next_object() {
            if object.kind == UdpObjectKind::Part {
                run.parts.entry(object.part).or_insert((kernel_us, node.clone()));
            }
        }
    }
    director.leave(&mut session, Instant::now(), &mut actions);
    for action in actions.drain(..) {
        if let Action::Send { to, datagram } = action {
            let _ = socket.send_to(&datagram, to).await;
        }
    }
    run.replacements = director.replacements.clone();
    run.stats = director.stats.clone();
    run.degraded_late = session.degraded_late;
    run.degraded_missing = session.degraded_missing;
    run
}

struct Crowd {
    lives: Vec<Arc<Live>>,
    tasks: Vec<tokio::task::JoinHandle<Run>>,
}

impl Crowd {
    /// Viewers `sids` of `stream`, each `spacing` after the last.
    async fn join(region: &Region, sids: &[String], stream: u64, rules: bool, spacing: Duration) -> Self {
        let mut lives = Vec::new();
        let mut tasks = Vec::new();
        for sid in sids {
            let live = Arc::new(Live::default());
            tasks.push(tokio::spawn(view(
                region.port,
                region.entry_url.clone(),
                sid.clone(),
                stream,
                rules,
                Arc::clone(&live),
            )));
            lives.push(live);
            tokio::time::sleep(spacing).await;
        }
        Self { lives, tasks }
    }

    fn nodes(&self) -> Vec<Option<String>> {
        self.lives.iter().map(|live| live.node.lock().unwrap().clone()).collect()
    }

    async fn finish(self) -> Vec<Run> {
        for live in &self.lives {
            live.stop.store(true, Ordering::Relaxed);
        }
        let mut runs = Vec::new();
        for task in self.tasks {
            runs.push(task.await.unwrap());
        }
        runs
    }
}

/// A viewer's figures over the window `from..to` (Unix µs): its longest
/// gap between first arrivals, its worst lateness past the run's least
/// (arrival less the part's publication), parts missing, and the place240
/// moves it made.
#[derive(Debug)]
struct Figures {
    sid: String,
    first: Option<String>,
    longest_gap_ms: f64,
    worst_late_ms: f64,
    missing: u64,
    moves: Vec<(String, String, String, f64)>,
}

fn figures(run: &Run, source: &Source, from_us: u64, to_us: u64, fault_us: u64) -> Figures {
    let lateness = |part: u64, at: u64| at as i64 - source.published_us(part) as i64;
    let least = run
        .parts
        .iter()
        .map(|(part, (at, _))| lateness(*part, *at))
        .min()
        .unwrap_or(0);
    let window: Vec<(u64, u64)> = run
        .parts
        .iter()
        .filter(|(part, _)| {
            let published = source.published_us(**part);
            published >= from_us && published <= to_us
        })
        .map(|(part, (at, _))| (*part, *at))
        .collect();
    let mut longest_gap = 0_u64;
    let mut last: Option<u64> = None;
    let mut by_arrival: Vec<u64> = window.iter().map(|(_, at)| *at).collect();
    by_arrival.sort_unstable();
    for at in by_arrival {
        if let Some(last) = last {
            longest_gap = longest_gap.max(at.saturating_sub(last));
        }
        last = Some(at);
    }
    let worst = window
        .iter()
        .map(|(part, at)| lateness(*part, *at) - least)
        .max()
        .unwrap_or(0);
    let (low, high) = (
        window.first().map_or(0, |(part, _)| *part),
        window.last().map_or(0, |(part, _)| *part),
    );
    let missing = (low..=high).filter(|part| !run.parts.contains_key(part)).count() as u64;
    let unix = |at: Instant| {
        now_unix_us().saturating_sub(Instant::now().saturating_duration_since(at).as_micros() as u64)
    };
    let moves = run
        .replacements
        .iter()
        .filter(|replacement| {
            replacement.reason == "late-parts"
                || replacement.reason == "missing-parts"
                || replacement.reason.starts_with("distress-")
        })
        .map(|replacement| {
            (
                replacement.from.clone(),
                replacement.to.clone(),
                replacement.reason.clone(),
                (unix(replacement.at) as f64 - fault_us as f64) / 1_000.0,
            )
        })
        .collect();
    Figures {
        sid: run.sid.clone(),
        first: run.first_node.clone(),
        longest_gap_ms: longest_gap as f64 / 1_000.0,
        worst_late_ms: worst as f64 / 1_000.0,
        missing,
        moves,
    }
}

fn counts(nodes: &[Option<String>], edges: usize) -> Vec<usize> {
    (1..=edges)
        .map(|index| {
            nodes
                .iter()
                .filter(|node| node.as_deref() == Some(format!("edge-{index}").as_str()))
                .count()
        })
        .collect()
}

// ----------------------------------------------------------------- scenes

/// (a) and (b): four viewers join within 100 ms on three edges; the edge
/// with the most is throttled to 35 % of the stream's rate for 3 s.
async fn join_and_throttle(base: u8, rules: bool) -> (Vec<usize>, usize, Vec<Figures>) {
    let region = Region::start(base, 3, rules, heavy::HeavyConfig::default().egress_max_bps, false).await;
    let sids: Vec<String> = (1..=4).map(|index| format!("viewer-{index}")).collect();
    let crowd = Crowd::join(&region, &sids, STREAM, rules, Duration::from_millis(25)).await;
    for _ in 0..30 {
        if crowd.nodes().iter().all(Option::is_some) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        region.describe(&crowd).await;
    }
    until("every viewer plays", Duration::from_secs(15), || {
        crowd.nodes().iter().all(Option::is_some)
    })
    .await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let placed = counts(&crowd.nodes(), 3);
    let stalled = (0..3).max_by_key(|index| (placed[*index], 3 - index)).unwrap();
    let fault_us = now_unix_us();
    region.slots[stalled].proxy.throttle(350);
    tokio::time::sleep(Duration::from_secs(3)).await;
    region.slots[stalled].proxy.throttle(0);
    tokio::time::sleep(Duration::from_secs(8)).await;
    let runs = crowd.finish().await;
    eprintln!("  source sent {:.0} Mbit/s", region.source().sent_mbps());
    let window = (fault_us.saturating_sub(2_000_000), fault_us + 9_000_000);
    let figures = runs
        .iter()
        .map(|run| figures(run, region.source(), window.0, window.1, fault_us))
        .collect();
    region.shutdown().await;
    (placed, stalled, figures)
}

/// (a) The four viewers of a 247 Mbit/s stream that join within 100 ms
/// spread 2+1+1 over three 2-vCPU edges by committed cost (cmpd8 b: both
/// of a region's viewers and a reader on one edge, the other healthy).
/// (b) The edge with two of them has its send side throttled for 3 s: its
/// viewers move (`late-parts`) within about 1.5 s and hold under about
/// 2 s, where without the rules they hold the stall and seconds more;
/// the viewers of the other edges see nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "247 Mbit/s in process: run alone, in release, on a quiet box"]
async fn heavy_viewers_spread_and_move_off_a_stalled_edge() {
    let _turn = SERIAL.lock().await;
    let (placed_before, stalled_before, before) = join_and_throttle(110, false).await;
    eprintln!("place240 (a, b) before ({} Mbit/s): placed {placed_before:?}, edge-{} throttled", mbps(), stalled_before + 1);
    for figure in &before {
        eprintln!("  {figure:?}");
    }
    let (placed, stalled, after) = join_and_throttle(120, true).await;
    eprintln!("place240 (a, b) after: placed {placed:?}, edge-{} throttled", stalled + 1);
    for figure in &after {
        eprintln!("  {figure:?}");
    }
    let mut sorted = placed.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, vec![1, 1, 2], "spread by committed cost");
    let throttled = format!("edge-{}", stalled + 1);
    for figure in &after {
        if figure.first.as_deref() == Some(throttled.as_str()) {
            assert_eq!(figure.moves.len(), 1, "{figure:?}");
            let (from, to, reason, at_ms) = &figure.moves[0];
            assert_eq!((from.as_str(), reason.as_str()), (throttled.as_str(), "late-parts"));
            assert_ne!(to, &throttled);
            assert!(*at_ms < 2_000.0, "moved {at_ms} ms after the stall began: {figure:?}");
            assert!(figure.worst_late_ms < 2_000.0, "{figure:?}");
            // 0 to 1 part was lost at the hand-over in the runs so far
            // (open: see place240.md).
            assert!(figure.missing <= 2, "{figure:?}");
        } else {
            assert!(figure.moves.is_empty(), "{figure:?}");
            assert!(figure.worst_late_ms < 500.0, "{figure:?}");
        }
    }
    // Before: the stalled edge's viewers held the stall and more.
    let throttled = format!("edge-{}", stalled_before + 1);
    assert!(before
        .iter()
        .filter(|figure| figure.first.as_deref() == Some(throttled.as_str()))
        .all(|figure| figure.moves.is_empty() && figure.worst_late_ms > 3_000.0));
}

/// (d) An upstream stall that every edge sees (the publisher holds its
/// sends 2.5 s, then sends them): each viewer moves at most once.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "247 Mbit/s in process: run alone, in release, on a quiet box"]
async fn an_upstream_stall_moves_each_viewer_at_most_once() {
    let _turn = SERIAL.lock().await;
    let region = Region::start(130, 3, true, heavy::HeavyConfig::default().egress_max_bps, false).await;
    let sids: Vec<String> = (1..=3).map(|index| format!("up-{index}")).collect();
    let crowd = Crowd::join(&region, &sids, STREAM, true, Duration::from_millis(25)).await;
    until("every viewer plays", Duration::from_secs(15), || {
        crowd.nodes().iter().all(Option::is_some)
    })
    .await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let fault_us = now_unix_us();
    region.source().stall(Duration::from_millis(2_500));
    tokio::time::sleep(Duration::from_secs(14)).await;
    let runs = crowd.finish().await;
    let figures: Vec<Figures> = runs
        .iter()
        .map(|run| figures(run, region.source(), fault_us - 2_000_000, fault_us + 13_000_000, fault_us))
        .collect();
    eprintln!("place240 (d) upstream stall of 2.5 s:");
    for (figure, run) in figures.iter().zip(&runs) {
        eprintln!("  {figure:?} quality {:?}", run.stats.quality);
    }
    region.shutdown().await;
    for (figure, run) in figures.iter().zip(&runs) {
        assert!(figure.moves.len() <= 1, "{figure:?}");
        assert!(run.stats.quality.moves() <= 1, "{:?}", run.stats.quality);
    }
}

/// (c) Two 2-vCPU edges whose heavy egress is capped at 300 Mbit/s (one
/// 247 Mbit/s viewer each): two viewers fill the region; the third is
/// refused `edge-busy` (the edge's own cap, counted) and never served; a
/// heavy reader is refused at the entry (429) and by an edge's own cap;
/// an audio crowd of twelve two-path listeners joins and plays whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "247 Mbit/s in process: run alone, in release, on a quiet box"]
async fn at_budget_the_next_heavy_session_is_refused_and_audio_is_untouched() {
    let _turn = SERIAL.lock().await;
    for rules in [false, true] {
        let region = Region::start(if rules { 150 } else { 140 }, 2, rules, 300_000_000, true).await;
        let sids: Vec<String> = (1..=2).map(|index| format!("full-{index}")).collect();
        let crowd = Crowd::join(&region, &sids, STREAM, rules, Duration::from_millis(25)).await;
        until("both viewers play", Duration::from_secs(15), || {
            crowd.nodes().iter().all(Option::is_some)
        })
        .await;
        tokio::time::sleep(ADVERTISE_EVERY * 3).await;
        let third = Crowd::join(&region, &["full-3".to_owned()], STREAM, rules, Duration::ZERO).await;
        let audio_sids: Vec<String> = (1..=12).map(|index| format!("pcm-{index}")).collect();
        let audio = Crowd::join(&region, &audio_sids, AUDIO, rules, Duration::from_millis(10)).await;
        tokio::time::sleep(Duration::from_secs(6)).await;
        // A heavy reader asks the entry, and an edge's own cap.
        let reader = ReaderRequest {
            udp: false,
            stream_id: STREAM,
            session: Some("reader-1"),
            ..ReaderRequest::default()
        };
        let (http, _) = region.entry.assign_reader(reader, true).await;
        let edge = region.edge(0);
        edge.router.edge_load.egress.admit_playback(PlaybackRequest {
            session_id: Some("reader-2"),
            stream_id: Some(STREAM),
            draining: false,
        });
        let edge_reader = edge.router.heavy_admits_reader(
            Some(STREAM),
            Some("reader-2"),
            PlaybackAdmission::Admitted,
        );
        // The region's shutdown waits for every hold on its edges.
        drop(edge);
        let heavy_legs: Vec<u64> = (0..2).map(|index| region.legs_at(index, STREAM)).collect();
        let audio_legs: Vec<u64> = (0..2).map(|index| region.legs_at(index, AUDIO)).collect();
        let refused_at_edges: u64 = (0..2)
            .map(|index| {
                let stats = &region.edge(index).router.heavy.stats;
                stats.refused_bytes.load(Ordering::Relaxed) + stats.refused_cpu.load(Ordering::Relaxed)
            })
            .sum();
        let third_runs = third.finish().await;
        let viewers = crowd.finish().await;
        let audio_runs = audio.finish().await;
        let entry_udp = region.entry.heavy.stats.entry_refused_udp.load(Ordering::Relaxed);
        let entry_http = region.entry.heavy.stats.entry_refused_http.load(Ordering::Relaxed);
        eprintln!(
            "place240 (c) rules {rules}: heavy legs {heavy_legs:?}, audio legs {audio_legs:?}, third viewer refused {:?} parts {}, entry refused udp {entry_udp} http {entry_http}, edges refused {refused_at_edges}, reader at the entry {:?}, at the edge {edge_reader:?}",
            third_runs[0].refused,
            third_runs[0].parts.len(),
            http.as_ref().map(|answer| answer.caches.len()),
        );
        let audio_moves: usize = audio_runs.iter().map(|run| run.replacements.len()).sum();
        let audio_parts: Vec<usize> = audio_runs.iter().map(|run| run.parts.len()).collect();
        eprintln!("  audio: moves {audio_moves}, parts {audio_parts:?}");
        let audio_missing: Vec<u64> = audio_runs
            .iter()
            .map(|run| {
                let (Some(first), Some(last)) = (run.parts.keys().next(), run.parts.keys().last())
                else {
                    return u64::MAX;
                };
                (first + 1..*last).filter(|part| !run.parts.contains_key(part)).count() as u64
            })
            .collect();
        region.shutdown().await;
        if rules {
            assert_eq!(heavy_legs, vec![1, 1], "one viewer an edge");
            assert!(third_runs[0].refused.iter().any(|reason| reason == "edge-busy"));
            assert!(third_runs[0].parts.is_empty(), "the third viewer is never served");
            assert!(refused_at_edges >= 1);
            assert!(entry_udp >= 1);
            assert!(matches!(http, Err(AssignmentError::HeavyFull)));
            assert_eq!(entry_http, 1);
            assert_eq!(edge_reader, PlaybackAdmission::Rejected);
            assert!(viewers.iter().all(|run| run.replacements.is_empty()));
            assert_eq!(audio_legs.iter().sum::<u64>(), 24, "every audio listener on two legs");
            assert!(audio_runs.iter().all(|run| run.refused.is_empty()));
            assert!(audio_missing.iter().all(|missing| *missing == 0), "{audio_missing:?}");
        }
    }
}
