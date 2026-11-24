//! The fast client lane (low-latency wave 4, B8 and B9): a client decodes
//! the contributor's own symbols, forwarded by the edge as they arrive, into
//! the part that the edge serves over LL-HLS.

use super::fast_lease::{bind_edge_class_ports, edge_dispatch, loopback, wait_until};
use super::*;
use av_mesh::client_udp::{
    rewrap_fast_vec, Control, ObjectKind as UdpObjectKind, ObjectReceiver, RepairPolicy,
};
use fast_plane::edge_clients::EdgeFastLane;
use fast_plane::relay::RelayClassConfig;
use fast_plane::view::RlsView;
use media_object::{ClockConfidence, ClockTimestamp, StageTimestamp};
use playlists::mesh::{CacheMesh, CacheMeshConfig, CacheMeshRole as MeshRole};
use std::sync::atomic::AtomicU64;
use tokio::time::timeout;

const OFFSET: u16 = 1_000;

/// The packagings that av-contrib publishes, as it builds them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Packaging {
    /// 2.5 ms Opus: an fMP4 slot without init.
    Opus,
    /// 1 ms PCM: an opaque part.
    Pcm,
    /// One 4K frame: an fMP4 slot with its init.
    Fmp4Video,
    /// Any other opaque part.
    Opaque,
    /// A part without a container (TS).
    Ts,
}

fn published_now() -> StageTimestamp {
    StageTimestamp::new(
        Stage::Published,
        ClockTimestamp::new(
            i64::try_from(now_unix_us() * 1_000).unwrap(),
            "test",
            ClockConfidence::estimated(1_000),
        )
        .unwrap(),
    )
}

/// The media bytes of a part of `len` bytes.
fn media_bytes(sequence: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| ((index as u64 * 31 + sequence * 7) % 251) as u8)
        .collect()
}

/// A canonical envelope as av-contrib builds it for `packaging`, and the
/// LL-HLS part bytes it should become.
fn packaged_envelope(
    packaging: Packaging,
    stream_id: u64,
    epoch: u64,
    sequence: u64,
    len: usize,
) -> (Bytes, Vec<u8>) {
    let media = media_bytes(sequence, len);
    let (payload, track, metadata): (Vec<u8>, &str, Vec<(&str, &[u8])>) = match packaging {
        Packaging::Opus => (
            encode_test_fmp4_slot(None, &media).to_vec(),
            "fmp4-media",
            vec![
                ("container", b"fmp4"),
                ("payload-format", b"fmp4-slot-v1"),
                ("audio-codec", b"opus"),
                ("track-composition", b"audio"),
                ("duration-us", b"2500"),
            ],
        ),
        Packaging::Fmp4Video => (
            encode_test_fmp4_slot(Some(b"ftyp-moov-avc1"), &media).to_vec(),
            "fmp4-media",
            vec![
                ("container", b"fmp4"),
                ("payload-format", b"fmp4-slot-v1"),
                ("video-codec", b"avc1.640033"),
                ("track-composition", b"video"),
            ],
        ),
        Packaging::Pcm | Packaging::Opaque => (
            media.clone(),
            "opaque-audio",
            vec![
                ("container", b"opaque"),
                ("payload-format", b"opaque-part-v1"),
                ("content-type", b"application/octet-stream"),
                ("file-extension", b"bin"),
                (
                    "duration-us",
                    if packaging == Packaging::Pcm {
                        b"1000"
                    } else {
                        b"5000"
                    },
                ),
            ],
        ),
        Packaging::Ts => (media.clone(), "ts", vec![]),
    };
    let key = media_object::ObjectKey::for_payload(
        "default",
        stream_id.to_string(),
        track,
        epoch,
        0,
        sequence,
        1,
        &payload,
    )
    .unwrap();
    let mut builder = MediaObject::builder(key, ObjectKind::Media, payload)
        .with_keyframe(true)
        .with_stage_timestamp(published_now());
    for (name, value) in metadata {
        builder = builder.with_metadata(name, value.to_vec());
    }
    let object = builder.build().unwrap();
    (Bytes::from(media_object::encode(&object).unwrap()), media)
}

/// The RQD2 datagram of an RLS1 symbol, rewrapped for the fast lane.
fn fast_datagram(symbol: &[u8], part: u64) -> Vec<u8> {
    let view = RlsView::parse(symbol, relay_session::RelayLimits::default()).unwrap();
    rewrap_fast_vec(view.fec, part).unwrap()
}

/// B8: the contributor's symbols of an object, rewrapped to the fast lane
/// with some of them lost, decode at the client to the bytes of the part
/// that the tee committed (as LL-HLS serves it), for every packaging.
#[tokio::test]
async fn a_contributor_object_decodes_on_the_fast_lane_to_the_part_the_tee_committed() {
    let deadline = now_unix_us() + 10_000_000;
    for (index, (packaging, len)) in [
        (Packaging::Opus, 60),
        (Packaging::Pcm, 288),
        (Packaging::Fmp4Video, 30_000),
        (Packaging::Opaque, 1_440),
        (Packaging::Ts, 2_000),
    ]
    .into_iter()
    .enumerate()
    {
        let stream_id = 9_000 + index as u64;
        let sequence = 41;
        let (envelope, media) = packaged_envelope(packaging, stream_id, 0, sequence, len);
        // The tee's commit.
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 64, 64).await;
        commit_relay_object(
            &cache,
            media_object::decode(&envelope).unwrap(),
            envelope.to_vec(),
        )
        .await
        .unwrap();
        let (slot, _) = cache
            .chunk_cache
            .get_for_stream_id(stream_id, sequence as usize)
            .await
            .unwrap();
        let (served, _) = cache
            .get_part_for_stream_id(stream_id, sequence)
            .await
            .unwrap();
        assert_eq!(served.as_ref(), media.as_slice(), "{packaging:?}");
        let priority = if packaging == Packaging::Fmp4Video {
            relay_session::MediaPriority::VideoKey
        } else {
            relay_session::MediaPriority::Audio
        };
        let (sources, repairs) = fast_test_symbols(&envelope, priority, 1_088, 400, 3, deadline);
        assert!(
            repairs.len() >= 2,
            "{packaging:?}: the contributor sends repair"
        );
        // The first source and the first repair symbol are lost.
        let mut receiver = ObjectReceiver::new();
        let mut decoded = None;
        for symbol in sources.iter().skip(1).chain(repairs.iter().skip(1)) {
            let datagram = fast_datagram(symbol, sequence);
            assert!(
                datagram.len() <= 36 + 1_088,
                "a last-mile datagram fits 1,124 bytes"
            );
            if let Some(object) = receiver.push(av_mesh::client_compact::LegId::FIRST, &datagram) {
                assert!(decoded.is_none(), "{packaging:?}: one object");
                assert_eq!(receiver.last_fast_envelope_bytes, Some(envelope.len()));
                decoded = Some(object);
            }
        }
        let decoded = decoded.unwrap_or_else(|| panic!("{packaging:?}: the part decodes"));
        assert_eq!(decoded.kind, UdpObjectKind::Part);
        assert_eq!(decoded.stream_id, stream_id);
        assert_eq!(decoded.part, sequence);
        assert_eq!(
            decoded.payload, served,
            "{packaging:?}: the LL-HLS part's bytes"
        );
        assert_eq!(
            Some(decoded.payload.clone()),
            av_mesh::client_udp::slot_media(&slot, stream_id).map(|(media, _)| media),
            "{packaging:?}: slot_media of the committed slot"
        );
        assert!(decoded.published_unix_us > 0);
        assert_eq!(receiver.fast_parts, 1);
        assert_eq!(receiver.fast_envelope_bytes, envelope.len() as u64);
        assert_eq!(receiver.recovered_objects, 1);
    }
}

/// B8: a Soundkit bundle is reboxed at the commit, so its part is not the
/// envelope's payload: a client refuses it on the fast lane.
#[tokio::test]
async fn a_soundkit_bundle_is_refused_on_the_fast_lane() {
    let payload = b"NTV2PART-a-bundle".to_vec();
    let key =
        media_object::ObjectKey::for_payload("default", "4001", "audio", 0, 0, 3, 1, &payload)
            .unwrap();
    let object = MediaObject::builder(key, ObjectKind::Media, payload)
        .with_metadata("payload-format", b"soundkit-v2-bundle".to_vec())
        .build()
        .unwrap();
    let envelope = media_object::encode(&object).unwrap();
    let (sources, _) = fast_test_symbols(
        &envelope,
        relay_session::MediaPriority::Audio,
        1_088,
        400,
        0,
        now_unix_us() + 10_000_000,
    );
    let mut receiver = ObjectReceiver::new();
    for symbol in &sources {
        assert!(receiver
            .push(
                av_mesh::client_compact::LegId::FIRST,
                &fast_datagram(symbol, 3)
            )
            .is_none());
    }
    assert_eq!(receiver.errors, 1);
    assert_eq!(receiver.fast_parts, 0);
}

/// An edge fed by two forwarders on the fast plane, with its client UDP
/// port and the fast lane.
pub(super) struct LaneEdge {
    pub(super) cache: Arc<LiveTsCache>,
    pub(super) lane: Arc<EdgeFastLane>,
    pub(super) client_port: SocketAddr,
    relay_ports: [SocketAddr; 2],
    forwarders: [UdpSocket; 2],
    pub(super) stats: Arc<edge_udp::EdgeUdpStats>,
    holes: Option<Arc<edge_holes::HoleTrigger>>,
    shutdown_tx: watch::Sender<()>,
    ingest: tokio::task::JoinHandle<Result<()>>,
    udp: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl LaneEdge {
    pub(super) async fn start(ip: Ipv4Addr) -> Self {
        Self::start_with(ip, Duration::ZERO).await
    }

    /// With holes found by part distance (`hole_distance` not zero).
    async fn start_with(ip: Ipv4Addr, hole_distance: Duration) -> Self {
        Self::start_with_capacity(ip, hole_distance, 64, true).await
    }

    /// An edge that takes up to `max_subscribers` UDP clients, its fast lane
    /// sending from the client sockets' send-only twins (`send_twins`) or
    /// from the sockets themselves.
    pub(super) async fn start_with_capacity(
        ip: Ipv4Addr,
        hole_distance: Duration,
        max_subscribers: usize,
        send_twins: bool,
    ) -> Self {
        let (ports, sockets) = bind_edge_class_ports(ip);
        let forwarders = [
            UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap(),
            UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap(),
        ];
        // Slots of the node's default --slot-kb: the merged cache refuses a
        // part larger than its slot, and a 4K frame is hundreds of kB.
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 2_048, 2_048).await;
        // The edge's hop journal, as a node keeps one (`--hop-journal-capacity`).
        let journal = Arc::new(HopJournal::new("edge-lane", 1 << 18));
        cache.set_hop_journal(Arc::clone(&journal));
        let lane = EdgeFastLane::new(0);
        let mesh = Arc::new(
            CacheMesh::new(
                Arc::clone(&cache.chunk_cache),
                CacheMeshConfig::new("edge-lane", "uk", unused_loopback_addr())
                    .with_role(MeshRole::Edge),
            )
            .start()
            .await
            .unwrap(),
        );
        let holes =
            edge_holes::HoleTrigger::new(Arc::clone(&cache), Arc::clone(&mesh), hole_distance);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let ingest = tokio::spawn(fast_plane::relay::run_relay_class_ingest(
            sockets,
            Arc::clone(&cache),
            shutdown_rx.clone(),
            RelayIngestRuntime {
                dispatch: edge_dispatch(
                    forwarders[0].local_addr().unwrap(),
                    forwarders[1].local_addr().unwrap(),
                ),
                secondary_socket: None,
                class_twins: Vec::new(),
                forwarder: None,
                audio_epochs: None,
                failover_controller: None,
                failover_heartbeat: Duration::from_millis(100),
                link_telemetry: None,
            },
            RelayClassConfig {
                port_offset: OFFSET,
                cpus: [None, None],
                realtime: false,
                tee_capacity: 8_192,
                edge_clients: Some(Arc::clone(&lane)),
                holes: holes.clone(),
                ..Default::default()
            },
        ));
        wait_until("the edge's threads start", || {
            cache.relay_ingress_snapshot().fast_plane.threads == 2
        })
        .await;
        let router = app_router_for_tests(Arc::clone(&cache), mesh);
        let client_sockets = bind_edge_class_sockets_with(
            "127.0.0.1:0".parse().unwrap(),
            4 << 20,
            cache.stream_classes(),
            send_twins,
        )
        .unwrap()
        .with_fast_lane(Some(Arc::clone(&lane)))
        .with_hop_journal(Some(journal));
        let client_port = client_sockets
            .class_socket(fast_plane::TrafficClass::Audio)
            .local_addr()
            .unwrap();
        let stats: Arc<edge_udp::EdgeUdpStats> = Arc::default();
        let mut config = edge_udp::EdgeUdpConfig::with_defaults(1_200, max_subscribers);
        // A host stall must not make the cache-fed lane skip a part it owes.
        config.playout = Duration::from_secs(5);
        let udp = tokio::spawn(edge_udp::run_edge_udp(
            client_sockets,
            router,
            config,
            Arc::clone(&stats),
            Vec::new(),
            shutdown_rx,
        ));
        wait_until("the lane is armed", || lane.is_armed()).await;
        Self {
            cache,
            lane,
            client_port,
            relay_ports: [
                SocketAddr::from((ip, ports[0])),
                SocketAddr::from((ip, ports[1])),
            ],
            forwarders,
            stats,
            holes,
            shutdown_tx,
            ingest,
            udp,
        }
    }

    /// The forwarders send an object's symbols: F1 its sources (except
    /// when `lost_at_1`) and its repair, F2 its sources, as both forward an
    /// audio stream in full (and F2, a video stream's repair).
    pub(super) async fn send(
        &self,
        video: bool,
        (sources, repairs): &(Vec<Vec<u8>>, Vec<Vec<u8>>),
        lost_at_1: bool,
    ) {
        let port = |index: usize| {
            let mut port = self.relay_ports[index];
            if video {
                port.set_port(port.port() + OFFSET);
            }
            port
        };
        for symbol in sources {
            if !lost_at_1 {
                self.forwarders[0].send_to(symbol, port(0)).await.unwrap();
            }
            if !video {
                self.forwarders[1].send_to(symbol, port(1)).await.unwrap();
            }
        }
        for symbol in repairs {
            self.forwarders[1].send_to(symbol, port(1)).await.unwrap();
        }
    }

    /// Forwarder `index`'s socket, for a sender thread of its own.
    pub(super) fn forwarder_fd(&self, index: usize) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.forwarders[index].as_raw_fd()
    }

    pub(super) fn forwarder_socket(&self, index: usize) -> &UdpSocket {
        &self.forwarders[index]
    }

    /// The edge's RLS1 port that forwarder `index` sends a class to.
    pub(super) fn relay_port(&self, index: usize, video: bool) -> SocketAddr {
        let mut port = self.relay_ports[index];
        if video {
            port.set_port(port.port() + OFFSET);
        }
        port
    }

    pub(super) async fn head(&self, stream_id: u64) -> Option<u64> {
        self.cache
            .stream_position_for_id(stream_id)
            .await
            .map(|(_, last)| last as u64)
    }

    pub(super) async fn stop(self) {
        let _ = self.shutdown_tx.send(());
        self.ingest.await.unwrap().unwrap();
        self.udp.await.unwrap().unwrap();
    }
}

/// What one client received.
#[derive(Debug, Default)]
struct ClientRun {
    /// The part the edge named in its OK.
    first: Option<u64>,
    /// Each part's decodes (to count a part received twice), its bytes and
    /// whether it came on the fast lane.
    parts: BTreeMap<u64, (u32, Bytes, bool)>,
    gaps: u64,
    largest_media_datagram: usize,
    receiver_errors: u64,
}

/// Datagram loss of a client's path, deterministic.
struct Loss {
    state: u64,
    threshold: u64,
}

impl Loss {
    fn new(seed: u64, loss: f64) -> Self {
        Self {
            state: seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1,
            threshold: (loss * u32::MAX as f64) as u64,
        }
    }

    fn lost(&mut self) -> bool {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        (self.state & u64::from(u32::MAX)) < self.threshold
    }
}

/// A fast-lane client of `stream_id` at `edge`, from `from_part`, on a path
/// that loses `loss` of the media datagrams, until it has part `last` and
/// nothing more arrives for a while.
async fn lane_client(
    edge: SocketAddr,
    stream_id: u64,
    from_part: Option<u64>,
    loss: f64,
    last: Arc<AtomicU64>,
    subscribed: tokio::sync::oneshot::Sender<u64>,
) -> ClientRun {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let _ = SockRef::from(&socket).set_recv_buffer_size(8 << 20);
    let repair = if loss > 0.0 {
        RepairPolicy::for_loss(loss, RepairPolicy::default())
    } else {
        RepairPolicy::default()
    };
    let subscribe = |token| {
        Control::Subscribe {
            stream_id,
            from_part,
            repair,
            token,
            fast_lane: true,
            compact: None,
            others: Vec::new(),
            resume: false,
        }
        .encode()
    };
    let mut buf = vec![0_u8; 65_536];
    socket.send_to(&subscribe(None), edge).await.unwrap();
    let token = loop {
        let (len, _) = socket.recv_from(&mut buf).await.unwrap();
        if let Some(Control::Token { token, .. }) = Control::decode(&buf[..len]) {
            break token;
        }
    };
    socket.send_to(&subscribe(Some(token)), edge).await.unwrap();
    let mut run = ClientRun::default();
    let mut subscribed = Some(subscribed);
    let mut receiver = ObjectReceiver::new();
    let mut lose = Loss::new(
        stream_id ^ from_part.unwrap_or(7) ^ (loss * 1e6) as u64,
        loss,
    );
    loop {
        let wait = if run
            .parts
            .last_key_value()
            .is_some_and(|(part, _)| *part >= last.load(Ordering::Relaxed))
        {
            Duration::from_millis(300)
        } else {
            Duration::from_secs(5)
        };
        let Ok(Ok((len, _))) = timeout(wait, socket.recv_from(&mut buf)).await else {
            break;
        };
        let datagram = &buf[..len];
        if let Some(control) = Control::decode(datagram) {
            if let Control::Subscribed { next_part, .. } = control {
                run.first.get_or_insert(next_part);
                if let Some(subscribed) = subscribed.take() {
                    let _ = subscribed.send(next_part);
                }
            }
            continue;
        }
        if !datagram.starts_with(&raptorq_datagram_fec::DATAGRAM_MAGIC) {
            continue;
        }
        if datagram[11] >> 6 == 3 {
            run.largest_media_datagram = run.largest_media_datagram.max(len);
        }
        if lose.lost() {
            continue;
        }
        let Some(object) = receiver.push(av_mesh::client_compact::LegId::FIRST, datagram) else {
            continue;
        };
        let fast = receiver.last_fast_envelope_bytes.is_some();
        match object.kind {
            UdpObjectKind::Part => {
                let entry =
                    run.parts
                        .entry(object.part)
                        .or_insert((0, object.payload.clone(), fast));
                entry.0 += 1;
            }
            UdpObjectKind::Gap => run.gaps += 1,
            UdpObjectKind::Init => {}
        }
    }
    run.receiver_errors = receiver.errors;
    let _ = socket
        .send_to(&Control::Unsubscribe { stream_id, token }.encode(), edge)
        .await;
    run
}

/// Check a client's run: every part from the one the edge named to the
/// last arrived exactly once, byte for byte the edge's LL-HLS part, and the
/// live parts on the fast lane.
fn check_run(
    name: &str,
    run: &ClientRun,
    served: &BTreeMap<u64, Bytes>,
    last: u64,
    live_from: u64,
) {
    let first = run.first.unwrap_or_else(|| panic!("{name}: subscribed"));
    for part in first..=last {
        let Some((count, bytes, _)) = run.parts.get(&part) else {
            panic!("{name}: part {part} never arrived (first {first}, last {last})");
        };
        assert_eq!(*count, 1, "{name}: part {part} arrived {count} times");
        assert_eq!(
            Some(bytes),
            served.get(&part),
            "{name}: part {part} is the edge's LL-HLS part"
        );
    }
    assert_eq!(run.gaps, 0, "{name}: no gap");
    assert_eq!(run.receiver_errors, 0, "{name}: no decode error");
    // Parts published after the client joined (with one part of slack for
    // the snapshot) came on the fast lane.
    let live: Vec<u64> = (live_from + 1..=last).collect();
    let fast = live
        .iter()
        .filter(|part| run.parts.get(part).is_some_and(|(_, _, fast)| *fast))
        .count();
    assert!(
        fast + 1 >= live.len(),
        "{name}: {fast} of {} live parts on the fast lane",
        live.len()
    );
}

/// B9: an edge fed by two forwarders sends 1 ms PCM parts to clients on the
/// fast lane, joining at head-20, head-1 and the head, each on a path with
/// 0, 1 or 5 % loss. Every part arrives exactly once, byte for byte the
/// edge's LL-HLS part; the live parts come on the fast lane and the older
/// ones from the cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_sends_each_part_once_to_fast_lane_clients_joining_anywhere() {
    let edge = LaneEdge::start(loopback(5)).await;
    let stream_id = 4001;
    let deadline = now_unix_us() + 30_000_000;
    let mut served = BTreeMap::new();
    let last = Arc::new(AtomicU64::new(u64::MAX));
    let publish = |sequence: u64| {
        let (envelope, _) = packaged_envelope(Packaging::Pcm, stream_id, 0, sequence, 288);
        fast_test_symbols(
            &envelope,
            relay_session::MediaPriority::Audio,
            1_088,
            400,
            0,
            deadline,
        )
    };
    let mut clients = Vec::new();
    let mut joined = Vec::new();
    let parts = 160_u64;
    for sequence in 0..parts {
        if sequence == 40 {
            let head = edge.head(stream_id).await.unwrap();
            for (join, from_part) in [
                ("head-20", Some(head - 20)),
                ("head-1", Some(head - 1)),
                ("head", None),
            ] {
                for loss in [0.0, 0.01, 0.05] {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    clients.push((
                        format!("{join} at {:.0} % loss", loss * 100.0),
                        tokio::spawn(lane_client(
                            edge.client_port,
                            stream_id,
                            from_part,
                            loss,
                            Arc::clone(&last),
                            tx,
                        )),
                    ));
                    joined.push(rx);
                }
            }
            // Every client is in before the stream goes on.
            for rx in joined.drain(..) {
                timeout(Duration::from_secs(5), rx)
                    .await
                    .expect("the edge answers")
                    .unwrap();
            }
        }
        edge.send(false, &publish(sequence), sequence % 20 == 7)
            .await;
        served.insert(
            sequence,
            wait_for_part(&edge.cache, stream_id, sequence).await,
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    last.store(parts - 1, Ordering::Relaxed);
    let live_from = 40;
    for (name, client) in clients {
        let run = client.await.unwrap();
        check_run(&name, &run, &served, parts - 1, live_from);
    }
    let lane = edge.lane.snapshot_stats();
    assert!(lane.parts > 0 && lane.datagrams > 0, "{lane:?}");
    // Phase AM item 8: the lane counts its bytes (the edge's load reads
    // them), at least a few dozen a datagram.
    assert!(lane.bytes >= lane.datagrams * 32, "{lane:?}");
    assert!(
        edge.stats.egress_wire_bytes()
            >= lane.bytes + lane.datagrams * edge_udp::UDP_IP_HEADER_BYTES,
        "the edge's wire bytes hold the fast lane's: {lane:?}"
    );
    assert!(
        lane.top_up_datagrams > 0,
        "the edge tops the clients' repair up: {lane:?}"
    );
    assert_eq!(lane.would_block, 0, "{lane:?}");
    // The cache-fed lane sent the older parts and none of the live ones.
    assert!(edge.stats.parts.load(Ordering::Relaxed) > 0);
    edge.stop().await;
}

/// B9, 4K: one-frame fMP4 parts of a video stream (sources from the primary
/// forwarder, repair from the secondary) reach three fast-lane clients, one
/// on a 1 % path, whole, once each, in last-mile datagrams of at most 1,128
/// bytes, sent as UDP GSO messages (each client's symbols of a batch
/// together).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_4k_stream_rides_the_fast_lane_in_datagrams_that_fit_the_last_mile() {
    let edge = LaneEdge::start(loopback(6)).await;
    let stream_id = 1;
    let deadline = now_unix_us() + 30_000_000;
    let mut served = BTreeMap::new();
    let last = Arc::new(AtomicU64::new(u64::MAX));
    let publish = |sequence: u64| {
        let (envelope, _) = packaged_envelope(Packaging::Fmp4Video, stream_id, 0, sequence, 40_000);
        fast_test_symbols(
            &envelope,
            relay_session::MediaPriority::VideoKey,
            1_088,
            400,
            4,
            deadline,
        )
    };
    let parts = 40_u64;
    let mut clients = Vec::new();
    for sequence in 0..parts {
        if sequence == 5 {
            for loss in [0.01, 0.0, 0.0] {
                let (tx, rx) = tokio::sync::oneshot::channel();
                clients.push(tokio::spawn(lane_client(
                    edge.client_port,
                    stream_id,
                    None,
                    loss,
                    Arc::clone(&last),
                    tx,
                )));
                timeout(Duration::from_secs(5), rx)
                    .await
                    .expect("the edge answers")
                    .unwrap();
            }
        }
        edge.send(true, &publish(sequence), false).await;
        served.insert(
            sequence,
            wait_for_part(&edge.cache, stream_id, sequence).await,
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    last.store(parts - 1, Ordering::Relaxed);
    for (index, client) in clients.into_iter().enumerate() {
        let run = client.await.unwrap();
        check_run(&format!("4K client {index}"), &run, &served, parts - 1, 5);
        assert!(run.largest_media_datagram > 1_000);
        assert!(
            run.largest_media_datagram <= 1_128,
            "a last-mile 4K datagram is {} bytes",
            run.largest_media_datagram
        );
    }
    let lane = edge.lane.snapshot_stats();
    assert!(
        lane.gso_messages > 0 && lane.gso_fallbacks == 0,
        "video leaves the edge's fast lane as GSO messages: {lane:?}"
    );
    assert_eq!(
        edge.cache.stream_classes().class(stream_id),
        fast_plane::TrafficClass::Video
    );
    edge.stop().await;
}

/// B9: with --client-udp-fast-lane false, a client that asks for F1 gets
/// the cache-fed lane alone; the flag defaults on, as does the client's.
#[test]
fn the_fast_lane_flags_default_on_and_can_be_turned_off() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert!(args.client_udp_fast_lane);
    assert_eq!(args.client_udp_edge_rank, 0);
    let args = Args::try_parse_from([
        "av-mesh",
        "--client-udp-fast-lane",
        "false",
        "--client-udp-edge-rank",
        "3",
    ])
    .unwrap();
    assert!(!args.client_udp_fast_lane);
    assert_eq!(args.client_udp_edge_rank, 3);
}

/// B10: an edge fed by the fast plane finds a hole by part distance. Part
/// 10 of a 1 ms stream never arrives; when part 25 (15 ms later) arrives
/// the tee asks the parents for part 10, once, with no timer: before part
/// 25 nothing is asked, and parts that arrived are never asked for. The
/// request is lost (the edge has no parent): when part 40 (twice the
/// distance) arrives, part 10 is asked again, of both parents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_asks_for_a_hole_when_the_part_the_distance_later_arrives() {
    let edge = LaneEdge::start_with(loopback(7), Duration::from_millis(15)).await;
    let holes = edge.holes.clone().expect("the trigger is on");
    let stream_id = 4001;
    let deadline = now_unix_us() + 30_000_000;
    for sequence in 0..48_u64 {
        if sequence != 10 {
            let (envelope, _) = packaged_envelope(Packaging::Pcm, stream_id, 0, sequence, 288);
            let symbols = fast_test_symbols(
                &envelope,
                relay_session::MediaPriority::Audio,
                1_088,
                400,
                0,
                deadline,
            );
            edge.send(false, &symbols, false).await;
            wait_for_part(&edge.cache, stream_id, sequence).await;
            // The tee looks for holes after it commits.
            let looked = timeout(Duration::from_secs(1), async {
                let expected = u64::from(sequence >= 25) + u64::from(sequence >= 40);
                while holes.requests.load(Ordering::Relaxed) < expected {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await;
            assert!(looked.is_ok(), "part {sequence}");
        }
        let (requests, asked) = (
            holes.requests.load(Ordering::Relaxed),
            holes.holes.load(Ordering::Relaxed),
        );
        if sequence < 25 {
            assert_eq!(
                (requests, asked),
                (0, 0),
                "nothing asked at part {sequence}"
            );
        } else if sequence < 40 {
            assert_eq!(
                (requests, asked),
                (1, 1),
                "part 10 asked once, at part {sequence}"
            );
        } else {
            assert_eq!(
                (requests, asked, holes.rechecks.load(Ordering::Relaxed)),
                (2, 2, 1),
                "part 10 asked again, of both parents, at part {sequence}"
            );
        }
    }
    edge.stop().await;
}
