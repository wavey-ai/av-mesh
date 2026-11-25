//! Two contributors that publish the same parts into one ingress (A 2.7,
//! AM3): the class thread forwards every symbol once, compares the bytes of
//! every copy, and drops what is stale on its stream's own timeline.
//!
//! Each test drives the audio class thread itself, on loopback sockets: two
//! senders, as `contrib-lon` on the primary lane and `contrib-lon-b` on the
//! secondary, both admitted as promoted sessions, and two children. The tee
//! is not run (its channel is read where a test needs it), so what is shown
//! is the first hop's merge alone. The spec places this file at
//! `tests/two_contributors.rs`; the class thread lives in the binary, which
//! an integration test cannot reach, so it is a module of the relay here.

use super::*;
use crate::{RelayForwardEndpoint, RelayForwardRole};
use av_mesh::relay_ingress::PeerAdmission;
use playlists::compact_object::{self as compact, CompactHeader};
use relay_session::ParentPath;
use std::net::{Ipv4Addr, UdpSocket as StdSocket};
use std::thread::JoinHandle;

const GENERATION: u64 = 7;
const SUBSCRIPTION: u64 = 19;
/// The source session of the test streams, in µs.
const EPOCH: u64 = 1_763_924_000_001_000;
const BUDGET: u64 = DEFAULT_MEDIA_DEADLINE_US;

/// The envelope of one opaque audio part of the test session.
fn envelope(stream: u64, object: u64, version: u32, payload: &[u8]) -> Vec<u8> {
    envelope_of(stream, EPOCH, object, version, payload)
}

/// The envelope of one opaque audio part of session `epoch`.
fn envelope_of(stream: u64, epoch: u64, object: u64, version: u32, payload: &[u8]) -> Vec<u8> {
    let key = media_object::ObjectKey::for_payload(
        "default",
        stream.to_string(),
        "opaque-audio",
        epoch,
        0,
        object,
        version,
        payload,
    )
    .unwrap();
    let object =
        media_object::MediaObject::builder(key, media_object::ObjectKind::Media, payload.to_vec())
            .with_metadata("container", b"opaque".to_vec())
            .with_metadata("payload-format", b"opaque-part-v1".to_vec())
            .with_metadata("content-type", b"application/octet-stream".to_vec())
            .with_metadata("file-extension", b"bin".to_vec())
            .build()
            .unwrap();
    media_object::encode(&object).unwrap()
}

/// A one-symbol audio part as a contributor sends it on the compact lane
/// (RLC1): the source (copy 0) and `copies` repair copies, their packet
/// sequences from the part number (A 2.2).
fn compact_part(
    stream: u64,
    object: u64,
    version: u32,
    payload: &[u8],
    deadline_us: u64,
    copies: u32,
) -> Vec<Vec<u8>> {
    let envelope = envelope(stream, object, version, payload);
    let decoded = media_object::decode(&envelope).unwrap();
    let descriptor = compact::descriptor_id(&decoded).expect("an opaque part has a descriptor");
    let slim = compact::encode_slim(descriptor, &decoded).expect("an opaque part goes compact");
    let stream = stream.to_string();
    let header = CompactHeader {
        generation: GENERATION,
        subscription_id: SUBSCRIPTION,
        deadline_us,
        role: 0,
        kind: 3,
        path: 0,
        tenant: "default",
        stream: &stream,
        track: "opaque-audio",
        epoch: EPOCH,
        group: 0,
        object,
        version,
        packet_sequence: object as u32,
        esi: 0,
    };
    (0..=copies)
        .map(|copy| {
            let header = if copy == 0 {
                header
            } else {
                CompactHeader {
                    role: 1,
                    path: 1,
                    esi: copy,
                    packet_sequence: (object as u32).wrapping_add(copy),
                    ..header
                }
            };
            compact::encode_datagram(&header, &slim).unwrap()
        })
        .collect()
}

/// A part of several source symbols and one repair symbol (RLS1), as FLAC
/// at 5 ms goes.
fn whole_part(stream: u64, object: u64, deadline_us: u64) -> Vec<Vec<u8>> {
    let payload: Vec<u8> = (0..1_000).map(|index| (index * 7 + object) as u8).collect();
    let symbols = rls1_part(&envelope(stream, object, 1, &payload), deadline_us);
    assert!(symbols.len() > 2, "a part of several symbols");
    symbols
}

/// The RLS1 symbols of an envelope, sources then one repair symbol.
fn rls1_part(envelope: &[u8], deadline_us: u64) -> Vec<Vec<u8>> {
    use relay_session::{
        encode_datagram, AdaptiveFecController, AdaptiveFecPolicy, CongestionConfig, MediaDeadline,
        MediaPriority, RaptorQObjectEncoder, SubscriptionId, TopologyGeneration,
    };
    let object = media_object::decode(envelope).unwrap();
    let mut encoder = RaptorQObjectEncoder::new(
        AdaptiveFecController::new(
            AdaptiveFecPolicy {
                min_repair_symbols: 1,
                max_repair_symbols: 1,
                min_repair_ratio: 0.0,
                max_repair_ratio: 0.0,
                max_source_symbols: 48,
                symbol_size: 400,
                ..AdaptiveFecPolicy::default()
            },
            CongestionConfig::default(),
        ),
        RelayLimits::default(),
    )
    .unwrap();
    let encoded = encoder
        .encode_object(
            &object,
            TopologyGeneration::new(GENERATION).unwrap(),
            SubscriptionId::new(SUBSCRIPTION).unwrap(),
            MediaDeadline::from_micros(deadline_us),
            MediaPriority::Audio,
        )
        .unwrap();
    encoded
        .source_symbols
        .iter()
        .chain(&encoded.repair_symbols)
        .map(|symbol| encode_datagram(symbol, RelayLimits::default()).unwrap())
        .collect()
}

/// The part numbers' payloads: patterned, one per number.
fn payload(object: u64) -> Vec<u8> {
    (0..48)
        .map(|index| (object as u8).wrapping_add(index))
        .collect()
}

/// The deadline of part `object` of a stream of 1 ms parts.
fn deadline(object: u64) -> u64 {
    EPOCH + 1_000_000 + object * 1_000 + BUDGET
}

fn bound_socket() -> StdSocket {
    let socket = StdSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(4 << 20);
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    socket
}

/// One ingress: the audio class thread of a first forwarder.
struct Ingress {
    /// `contrib-lon` (primary lane) and `contrib-lon-b` (secondary lane).
    senders: [StdSocket; 2],
    /// The thread's two receive sockets, by lane.
    lanes: [SocketAddr; 2],
    children: [StdSocket; 2],
    stats: Arc<FastPlaneStats>,
    epochs: Arc<ValidatedEpochs>,
    /// What the failover controller reads of the parents' arrivals.
    failover: Arc<FailoverInputs>,
    /// What `GET /api/node` reports of each parent's feed (C CR4).
    feeds: Arc<ParentFeeds>,
    tee: TeeReceiver<TeeMeta>,
    stop: Arc<AtomicBool>,
    thread: Option<ClassThread>,
    running: Option<JoinHandle<()>>,
    /// Makes the feed entries the thread asks for, as the tee does.
    feeder: Option<JoinHandle<()>>,
}

impl Ingress {
    /// Bound and ready; the thread runs from `start`.
    async fn bind() -> Self {
        let senders = [bound_socket(), bound_socket()];
        let children = [bound_socket(), bound_socket()];
        let endpoints: Vec<RelayForwardEndpoint> = children
            .iter()
            .map(|child| RelayForwardEndpoint {
                bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                target: child.local_addr().unwrap(),
                role: RelayForwardRole::All,
            })
            .collect();
        let forwarder = crate::RelayDownstreamForwarder::bind_split(&endpoints, None)
            .await
            .unwrap()
            .expect("two children");
        forwarder.configure_buffers(4 << 20);
        let sockets = bind_class_sockets(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
            0,
            4 << 20,
        )
        .unwrap();
        let lane = |path| {
            sockets
                .iter()
                .find(|socket| socket.path == path)
                .map(|socket| socket.socket.local_addr().unwrap())
                .unwrap()
        };
        let lanes = [
            lane(RelayIngressParentPath::Primary),
            lane(RelayIngressParentPath::Secondary),
        ];
        // Both contributors' sessions are promoted (A 2.7).
        let admissions: HashMap<SocketAddr, PeerAdmission> = senders
            .iter()
            .map(|sender| {
                (
                    sender.local_addr().unwrap(),
                    PeerAdmission {
                        generation: GENERATION,
                        subscription_id: SUBSCRIPTION,
                        parent_path: ParentPath::PromotedSecondary,
                    },
                )
            })
            .collect();
        let stats = Arc::new(FastPlaneStats::default());
        let epochs = Arc::new(ValidatedEpochs::new(64));
        let failover = Arc::new(FailoverInputs::new());
        let feeds = Arc::new(ParentFeeds::default());
        let stop = Arc::new(AtomicBool::new(false));
        let (tx, tee) = tee_ring(65_536);
        let thread = ClassThread {
            class: TrafficClass::Audio,
            sockets: sockets
                .into_iter()
                .map(|socket| (socket.path, socket.socket))
                .collect(),
            admissions: Arc::new(admissions),
            limits: RelayLimits::default(),
            forwarder: Some(forwarder),
            link_telemetry: None,
            tee: Some(tx),
            stats: Arc::clone(&stats),
            failover: Arc::clone(&failover),
            stop: Arc::clone(&stop),
            cpu: None,
            realtime: false,
            video_gso: false,
            lease: None,
            edge_clients: None,
            burst_copy_ms: 0,
            loss_report: false,
            demand: None,
            demand_peers: Arc::from(Vec::new()),
            lease_descriptors: false,
            video_gro: false,
            media_deadline_us: BUDGET,
            epochs: Arc::clone(&epochs),
            feeds: Arc::clone(&feeds),
            send_defer: None,
            test_alloc_counter: None,
        };
        Self {
            senders,
            lanes,
            children,
            stats,
            epochs,
            failover,
            feeds,
            tee,
            stop,
            thread: Some(thread),
            running: None,
            feeder: None,
        }
    }

    async fn start() -> Self {
        let mut ingress = Self::bind().await;
        ingress.run();
        ingress
    }

    fn run(&mut self) {
        let thread = self.thread.take().expect("started once");
        self.running = Some(std::thread::spawn(move || thread.run()));
        // The tee's part in the feeds: the entries the thread asks for.
        let (feeds, stop) = (Arc::clone(&self.feeds), Arc::clone(&self.stop));
        self.feeder = Some(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                feeds.make_requested();
                std::thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    /// Contributor `sender` (0: primary lane, 1: secondary) sends `datagrams`.
    fn send(&self, sender: usize, datagrams: &[Vec<u8>]) {
        for datagram in datagrams {
            self.senders[sender]
                .send_to(datagram, self.lanes[sender])
                .unwrap();
        }
    }

    /// The next `count` datagrams that `child` receives, in order.
    fn receive(&self, child: usize, count: usize) -> Vec<Vec<u8>> {
        let mut buf = [0_u8; 2_048];
        (0..count)
            .map(|index| {
                let (len, _) = self.children[child]
                    .recv_from(&mut buf)
                    .unwrap_or_else(|error| panic!("child {child}: {index} of {count}: {error}"));
                buf[..len].to_vec()
            })
            .collect()
    }

    /// Each child receives exactly `expected`, each once, and nothing more.
    fn assert_each_child_has_once(&self, expected: &[Vec<u8>]) {
        let mut expected = expected.to_vec();
        expected.sort();
        for child in 0..2 {
            let mut received = self.receive(child, expected.len());
            received.sort();
            assert_eq!(received, expected, "child {child}");
        }
        self.assert_nothing_more();
    }

    fn assert_nothing_more(&self) {
        let mut buf = [0_u8; 2_048];
        for child in &self.children {
            child
                .set_read_timeout(Some(Duration::from_millis(100)))
                .unwrap();
            assert!(
                child.recv_from(&mut buf).is_err(),
                "a child received a datagram twice"
            );
            child
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
        }
    }

    /// The counters once the thread has taken `datagrams` in all (both
    /// lanes, any verdict).
    fn settled(&self, datagrams: u64) -> FastPlaneSnapshot {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let snapshot = self.stats.snapshot();
            if snapshot.audio_received >= datagrams || Instant::now() >= deadline {
                return snapshot;
            }
            std::thread::yield_now();
        }
    }

    /// The datagrams the thread handed its tee so far.
    fn teed(&mut self) -> usize {
        let mut count = 0;
        while self.tee.peek().is_some() {
            self.tee.release();
            count += 1;
        }
        count
    }
}

impl Drop for Ingress {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(running) = self.running.take() {
            let _ = running.join();
        }
        if let Some(feeder) = self.feeder.take() {
            let _ = feeder.join();
        }
    }
}

/// Parts `0..count` of `stream`, one copy each, as both contributors send them.
fn parts(stream: u64, count: u64) -> Vec<Vec<Vec<u8>>> {
    (0..count)
        .map(|object| compact_part(stream, object, 1, &payload(object), deadline(object), 1))
        .collect()
}

#[tokio::test]
async fn two_senders_of_equal_datagrams_have_every_symbol_forwarded_once() {
    let ingress = Ingress::start().await;
    ingress.epochs.set(3001, EPOCH);
    let mut sent = parts(3001, 200);
    // And a part of several symbols with its repair.
    sent.push(whole_part(3001, 200, deadline(200)));
    for part in &sent {
        ingress.send(0, part);
        ingress.send(1, part);
    }
    let unique: Vec<Vec<u8>> = sent.concat();
    let count = unique.len() as u64;
    ingress.assert_each_child_has_once(&unique);
    let snapshot = ingress.settled(2 * count);
    let [primary, secondary] = snapshot.parent_lanes;
    assert_eq!(primary.lane, ParentLane::Primary);
    assert_eq!(secondary.lane, ParentLane::Secondary);
    assert_eq!((primary.received, secondary.received), (count, count));
    assert_eq!(primary.first_arrivals + secondary.first_arrivals, count);
    assert_eq!(primary.duplicates + secondary.duplicates, count);
    assert_eq!(snapshot.duplicates, count);
    assert_eq!(
        (primary.duplicate_mismatch, secondary.duplicate_mismatch),
        (0, 0)
    );
    assert_eq!(snapshot.media_clock, MediaClockSnapshot::default());
    assert_eq!(snapshot.not_admitted, 0);
}

#[tokio::test]
async fn either_sender_stopped_at_any_part_loses_nothing() {
    let ingress = Ingress::start().await;
    let mut stream = 4100;
    for stopped in [0, 1] {
        for stop_at in [0, 1, 99, 199] {
            stream += 1;
            ingress.epochs.set(stream, EPOCH);
            let sent = parts(stream, 200);
            for (object, part) in sent.iter().enumerate() {
                for sender in 0..2 {
                    if sender != stopped || (object as u64) < stop_at {
                        ingress.send(sender, part);
                    }
                }
            }
            ingress.assert_each_child_has_once(&sent.concat());
        }
    }
    let snapshot = ingress.stats.snapshot();
    let [primary, secondary] = snapshot.parent_lanes;
    assert_eq!(primary.duplicate_mismatch + secondary.duplicate_mismatch, 0);
    assert_eq!(snapshot.media_clock, MediaClockSnapshot::default());
}

/// The second copy of each part is sent once the first has reached the
/// children, so which lane came first is known.
#[tokio::test]
async fn a_late_joining_sender_changes_nothing_and_its_copies_show_their_lag() {
    let ingress = Ingress::start().await;
    ingress.epochs.set(3002, EPOCH);
    let sent = parts(3002, 200);
    for (object, part) in sent.iter().enumerate() {
        ingress.send(1, part);
        for child in 0..2 {
            assert_eq!(&ingress.receive(child, 2), part);
        }
        // contrib-lon returns at part 120.
        if object >= 120 {
            ingress.send(0, part);
        }
    }
    ingress.assert_nothing_more();
    let snapshot = ingress.settled(400 + 160);
    let [primary, secondary] = snapshot.parent_lanes;
    assert_eq!((secondary.first_arrivals, secondary.duplicates), (400, 0));
    assert_eq!((primary.first_arrivals, primary.duplicates), (0, 160));
    assert_eq!(primary.duplicate_mismatch, 0);
    // Every copy came after its first copy had been forwarded.
    assert!(primary.duplicate_lag_max_us > 0, "{primary:?}");
}

#[tokio::test]
async fn a_copy_with_other_bytes_under_one_key_counts_a_mismatch_on_the_later_lane() {
    let ingress = Ingress::start().await;
    ingress.epochs.set(3003, EPOCH);
    let first = compact_part(3003, 7, 1, &payload(7), deadline(7), 0);
    let mut changed_payload = payload(7);
    changed_payload[20] ^= 1;
    let other = compact_part(3003, 7, 1, &changed_payload, deadline(7), 0);
    assert_ne!(first, other);
    // The same key: the dedup cannot tell them apart.
    let key = |datagram: &[u8]| {
        let view = RlsView::parse(datagram, RelayLimits::default()).unwrap();
        (
            view.stream.to_owned(),
            view.epoch,
            view.object,
            view.version,
            view.block_id,
            view.packet_sequence,
        )
    };
    assert_eq!(key(&first[0]), key(&other[0]));

    ingress.send(0, &first);
    for child in 0..2 {
        assert_eq!(ingress.receive(child, 1), first);
    }
    ingress.send(1, &other);
    // The equal copy afterwards is equal: the first copy stays the one
    // that copies are compared with.
    ingress.send(1, &first);
    ingress.assert_nothing_more();
    let snapshot = ingress.settled(3);
    let [primary, secondary] = snapshot.parent_lanes;
    assert_eq!((primary.first_arrivals, primary.duplicate_mismatch), (1, 0));
    assert_eq!((secondary.duplicates, secondary.duplicate_mismatch), (2, 1));
}

#[tokio::test]
async fn different_object_versions_are_both_forwarded() {
    let ingress = Ingress::start().await;
    ingress.epochs.set(3004, EPOCH);
    let mut sent = Vec::new();
    for object in 0..20 {
        let one = compact_part(3004, object, 1, &payload(object), deadline(object), 1);
        let two = compact_part(3004, object, 2, &payload(object), deadline(object), 1);
        ingress.send(0, &one);
        ingress.send(1, &two);
        sent.extend(one);
        sent.extend(two);
    }
    ingress.assert_each_child_has_once(&sent);
    let snapshot = ingress.settled(80);
    assert_eq!(snapshot.duplicates, 0);
    assert_eq!(
        snapshot.parent_lanes[0].first_arrivals + snapshot.parent_lanes[1].first_arrivals,
        80
    );
}

/// contrib-lon was frozen and resumes with 10,000 datagrams of old parts
/// queued on its lane, while contrib-lon-b's live parts arrive on the other.
/// Everything is queued before the thread first reads, so the turns are
/// known: the thread takes 4 batches (256 datagrams) of a socket a turn.
///
/// - turn 1: 256 live parts on the primary lane (the stream's clock is
///   made; it follows the validated epoch at the next turn), and their
///   copies on the secondary;
/// - turn 2: two live parts (the first starts the head, the second
///   confirms it), then 254 old ones (stale); the secondary's live parts;
/// - turn 3 on: the marker part (live) behind them, then the rest of the
///   burst.
///
/// So the secondary's live parts reach the children before the marker:
/// they waited for at most 4 batches of the burst. Before the change the
/// thread emptied the primary socket first, and the marker came before
/// them, behind all 10,000.
#[tokio::test]
async fn a_burst_of_old_datagrams_on_one_lane_is_stale_and_does_not_hold_the_other_back() {
    const STREAM: u64 = 3005;
    const BURST: u64 = 10_000;
    let mut ingress = Ingress::bind().await;
    ingress.epochs.set(STREAM, EPOCH);
    // The stream is at part 20,000 now; the burst is of parts a second and
    // more older.
    let live = |object: u64| compact_part(STREAM, object, 1, &payload(object), deadline(object), 0);
    let stale = |index: u64| {
        let object = 10_000 + index % 8_000;
        compact_part(STREAM, object, 1, &payload(object), deadline(object), 0)
    };
    let first_turn: Vec<Vec<u8>> = (20_000..20_256).flat_map(live).collect();
    let head: Vec<Vec<u8>> = (20_256..20_258).flat_map(live).collect();
    let marker = live(20_258);
    let probes: Vec<Vec<u8>> = (20_259..20_267).flat_map(live).collect();
    let mut primary = first_turn.clone();
    primary.extend(head.iter().cloned());
    primary.extend((0..254).flat_map(stale));
    primary.extend(marker.iter().cloned());
    primary.extend((254..BURST).flat_map(stale));
    let mut secondary = first_turn.clone();
    secondary.extend(probes.iter().cloned());
    ingress.send(0, &primary);
    ingress.send(1, &secondary);
    ingress.run();

    let received = ingress.receive(0, 256 + head.len() + probes.len() + 1);
    let mut other_child = ingress.receive(1, received.len());
    let mut in_order = received.clone();
    other_child.sort();
    in_order.sort();
    assert_eq!(other_child, in_order);
    let position = |datagram: &Vec<u8>| received.iter().position(|got| got == datagram);
    let marker_at = position(&marker[0]).expect("the marker is forwarded");
    for probe in &probes {
        let probe_at = position(probe).expect("every live part is forwarded");
        assert!(
            probe_at < marker_at,
            "a live part of the other lane waited behind the burst"
        );
    }
    // The receive queue may have dropped some of the burst's tail: a live
    // part sent after it carries the drop count.
    // Once the queue is empty a live part carries the queue's drop count.
    let mut taken = 0;
    loop {
        std::thread::sleep(Duration::from_millis(50));
        let now = ingress.stats.snapshot().audio_received;
        if now == taken {
            break;
        }
        taken = now;
    }
    let tail = live(20_267);
    ingress.send(0, &tail);
    assert_eq!(ingress.receive(0, 1), tail);
    assert_eq!(ingress.receive(1, 1), tail);
    let judged = |snapshot: &FastPlaneSnapshot| {
        snapshot.media_clock.stale_media + snapshot.parent_lanes[0].kernel_dropped
    };
    let snapshot = ingress.stats.snapshot();
    assert_eq!(judged(&snapshot), BURST, "{:?}", snapshot.media_clock);
    assert!(snapshot.media_clock.stale_media >= 254);
    assert_eq!(snapshot.media_clock.stale_epoch, 0);
    // Nothing stale reached a child or the tee: the tee has the live parts
    // (the secondary's copies of the first turn are duplicates).
    ingress.assert_nothing_more();
    assert_eq!(ingress.teed(), 256 + head.len() + 1 + probes.len() + 1);
    // The stale datagrams make their lane look no more live: a burst alone
    // leaves the primary's last source arrival where the last live part
    // put it (A 4, "does not stamp the parent feeds").
    let stamped = ingress.failover.primary_source.load(Ordering::Relaxed);
    assert_ne!(stamped, 0, "the live parts stamped the primary");
    // The same holds for the parent feeds of `GET /api/node` (C CR4,
    // stamped after the media clock's verdict): the entry's last source
    // arrival, as now minus its age, does not move. Both senders share the
    // loopback address, so they are one parent (`two_senders_of_one_address_
    // are_one_parent`), and only the stale sends below reach it.
    let last_source = |feeds: &ParentFeeds| {
        let now_us = now_unix_us();
        let feed = feeds
            .snapshot(now_us)
            .into_iter()
            .find(|feed| feed.stream_id == Some(STREAM))
            .expect("the live parts made a feed of the stream");
        (
            now_us.saturating_sub(feed.source_age_ms.expect("a source arrived") * 1_000),
            feed.head_object,
        )
    };
    let (fed_at, fed_head) = last_source(&ingress.feeds);
    std::thread::sleep(Duration::from_millis(5));
    for index in 0..100 {
        ingress.send(0, &stale(index));
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while judged(&ingress.stats.snapshot()) < BURST + 100 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    let after = ingress.stats.snapshot();
    assert_eq!(judged(&after), BURST + 100, "{:?}", after.media_clock);
    assert_eq!(ingress.failover.primary_source.load(Ordering::Relaxed), stamped);
    let (fed_at_after, fed_head_after) = last_source(&ingress.feeds);
    assert_eq!(fed_head_after, fed_head, "a stale burst moved the feed's head");
    // Ages are whole milliseconds: the same arrival reads within 1 ms.
    assert!(
        fed_at_after.abs_diff(fed_at) <= 1_000,
        "a stale burst stamped the parent feed: {fed_at} then {fed_at_after}"
    );
    ingress.assert_nothing_more();
    assert_eq!(ingress.teed(), 0);
    // What a live part of the other lane waited at most: one turn of the
    // thread. Printed, not asserted (a loaded host stretches any turn): the
    // order above is the proof that it is one turn and not the burst.
    eprintln!(
        "{BURST} old datagrams ({} dropped by the receive queue) in {} receive batches: the longest turn \
         {} µs, turns over 1 ms {}",
        snapshot.parent_lanes[0].kernel_dropped,
        snapshot.audio_batches,
        snapshot.audio_loop_busy_max_us,
        snapshot.audio_loop_busy_over_1ms
    );
}

#[tokio::test]
async fn a_part_of_an_ended_session_is_stale_epoch_and_is_not_forwarded() {
    let ingress = Ingress::start().await;
    // The cache took up a later session of the stream.
    ingress.epochs.set(3006, EPOCH + 1);
    let opening = compact_part(3006, 1, 1, &payload(1), deadline(1), 0);
    // The stream's clock is made on its first datagram and follows the
    // cache's epoch at the next turn: until then nothing is judged.
    ingress.send(0, &opening);
    assert_eq!(ingress.receive(0, 1), opening);
    assert_eq!(ingress.receive(1, 1), opening);
    let old = parts(3006, 50);
    for part in &old {
        ingress.send(0, part);
    }
    ingress.assert_nothing_more();
    let snapshot = ingress.settled(1 + 100);
    assert_eq!(snapshot.media_clock.stale_epoch, 100);
    assert_eq!(snapshot.parent_lanes[0].first_arrivals, 1);
}

/// Wait up to 3 s for `done`.
async fn until(done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !done() {
        assert!(Instant::now() < deadline, "timed out");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// The epoch that the node's cache takes up on a commit reaches the class
/// threads' clocks (through `on_source_epoch`), with the tee running.
#[tokio::test]
async fn the_epoch_that_the_cache_takes_up_reaches_the_class_threads() {
    use relay_session::{
        CarrierIdentity, CarrierKind, NodeId, SubscriptionId, TopologyGeneration, TrustMode,
    };
    const STREAM: u64 = 3007;
    let sender = bound_socket();
    let mut dispatch = crate::empty_relay_udp_dispatch();
    dispatch
        .bind_controlled_peer(
            sender.local_addr().unwrap(),
            crate::ControlledRelayParentSession::new(
                1,
                CarrierIdentity {
                    local: NodeId::new("lon-dist-a").unwrap(),
                    peer: NodeId::new("contrib").unwrap(),
                    kind: CarrierKind::PrivateUdp,
                    trust_mode: TrustMode::ControlledPrivateNetwork,
                },
                TopologyGeneration::new(GENERATION).unwrap(),
                SubscriptionId::new(SUBSCRIPTION).unwrap(),
                ParentPath::Primary,
            )
            .unwrap(),
        )
        .unwrap();
    let sockets =
        bind_class_sockets(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), None, 0, 4 << 20).unwrap();
    let lane = sockets[0].socket.local_addr().unwrap();
    let cache = crate::LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 512).await;
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let task = tokio::spawn(run_relay_class_ingest(
        sockets,
        Arc::clone(&cache),
        shutdown_rx,
        RelayIngestRuntime {
            dispatch,
            secondary_socket: None,
            class_twins: Vec::new(),
            forwarder: None,
            audio_epochs: None,
            failover_controller: None,
            failover_heartbeat: Duration::from_millis(100),
            link_telemetry: None,
        },
        RelayClassConfig {
            port_offset: 0,
            realtime: false,
            ..RelayClassConfig::default()
        },
    ));
    let stats = || cache.relay_ingress_snapshot().fast_plane;
    until(|| stats().threads == 1).await;
    // Deadlines near the wall clock: the tee's receiver (AM4) judges them
    // on the stream's own timeline, and these are fresh on either.
    let deadline_us = now_unix_us() + 5_000_000;
    let session = |epoch: u64, object: u64| {
        rls1_part(
            &envelope_of(STREAM, epoch, object, 1, &payload(object)),
            deadline_us,
        )
    };
    for datagram in session(EPOCH, 0) {
        sender.send_to(&datagram, lane).unwrap();
    }
    let committed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if cache.get_part_for_stream_id(STREAM, 0).await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    assert!(committed.is_ok(), "the tee commits the part");
    // A part of the session before: stale at the class thread.
    for datagram in session(EPOCH - 1, 1) {
        sender.send_to(&datagram, lane).unwrap();
    }
    until(|| stats().media_clock.stale_epoch == 1).await;
    assert_eq!(stats().parent_lanes[0].first_arrivals, 1);
    let _ = shutdown_tx.send(());
    task.await.unwrap().unwrap();
}

/// A stream's last whole part, kept for a lease child that is new to the
/// stream (`LeaseDescriptors`), ages on this node's clock from its receipt
/// (A 2.5, "local holds age on the local clock"): its wire deadline is a
/// point on the stream's timeline, and here it is long past.
#[test]
fn a_kept_whole_part_ages_from_its_receipt_not_its_wire_deadline() {
    use crate::fast_plane::lease::LeaseChildConfig;
    use playlists::mesh::ServedSubscription;
    use std::os::fd::AsRawFd;
    const STREAM: u64 = 3010;
    const RECEIPT_US: u64 = 1_763_924_000_000_000;
    // Two lease children on one port of two loopback addresses.
    let ips = [Ipv4Addr::new(127, 0, 0, 2), Ipv4Addr::new(127, 0, 0, 3)];
    let (port, children) = (0..128)
        .find_map(|_| {
            let first = StdSocket::bind((ips[0], 0)).ok()?;
            let port = first.local_addr().ok()?.port();
            port.checked_add(1_000)?;
            let second = StdSocket::bind((ips[1], port)).ok()?;
            Some((port, [first, second]))
        })
        .expect("a port free on both addresses");
    for child in &children {
        child
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
    }
    let lease = LeaseChildren::bind(
        LeaseChildConfig {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            child_ports: [port, port ^ 1],
            class_port_offset: 1_000,
            primary_children: ips.iter().map(|ip| (*ip).into()).collect(),
            max_children: 16,
            ranked: false,
            warm_hold_us: DEFAULT_MEDIA_DEADLINE_US,
        },
        1 << 20,
    )
    .unwrap();
    let served = |ip: Ipv4Addr| ServedSubscription {
        child: SocketAddr::from((ip, 9_101)),
        stream_id: STREAM,
        backfill_parts_pending: 0,
        rank: None,
    };
    let sender = StdSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let stats = FastPlaneStats::default();
    let mut descriptors = LeaseDescriptors::new();
    let mut observe = |datagram: &[u8], now_us: u64| {
        let view = RlsView::parse(datagram, RelayLimits::default()).unwrap();
        descriptors.observe(
            sender.as_raw_fd(),
            &lease.load(),
            TrafficClass::Audio,
            &view,
            datagram,
            now_us,
            &stats,
        );
    };
    // A whole one-symbol part whose wire deadline passed long ago (a source
    // clock behind this node's), kept before the stream has a child.
    let past = 1;
    let whole = rls1_part(&envelope(STREAM, 0, 1, &payload(0)), past).remove(0);
    let view = RlsView::parse(&whole, RelayLimits::default()).unwrap();
    assert!(view.slim.is_none() && view.source_symbols == 1);
    observe(&whole, RECEIPT_US);
    let compact_of = |object: u64| compact_part(STREAM, object, 1, &payload(object), past, 0).remove(0);
    // A child that comes 900 ms after the receipt gets it before the next
    // compact part.
    lease.rebuild(&[served(ips[0])]);
    observe(&compact_of(1), RECEIPT_US + 900_000);
    let mut buf = [0_u8; 2_048];
    let (len, _) = children[0].recv_from(&mut buf).expect("the kept part");
    assert_eq!(&buf[..len], whole.as_slice());
    assert_eq!(stats.lease_descriptors.load(Ordering::Relaxed), 1);
    // One that comes a second after it does not.
    lease.rebuild(&[served(ips[0]), served(ips[1])]);
    observe(&compact_of(2), RECEIPT_US + DESCRIPTOR_HOLD_US);
    assert!(children[1].recv_from(&mut buf).is_err(), "a part held past its second");
    assert!(children[0].recv_from(&mut buf).is_err(), "the first child had it");
    assert_eq!(stats.lease_descriptors.load(Ordering::Relaxed), 1);
}
