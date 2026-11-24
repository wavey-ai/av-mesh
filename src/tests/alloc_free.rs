//! The audio class thread allocates and frees nothing once it runs (stall
//! follow-up to wave 9): a measured run on Linode showed its ingest stopping
//! for 0.3 to 2 s around a listener's arrival, and the leading in-process
//! candidate was the allocator's lock reached through the tee's copy of
//! every datagram, with other threads freeing those copies into the class
//! thread's arena. The test binary's allocator counts per thread
//! (`crate::alloc_count`); the class thread installs the counter at the top
//! of its loop, after everything it uses is made.

use super::compact_lane::{listen, ListenerRun, Mode, PathLoss, Payload, Shared};
use super::fast_lane::LaneEdge;
use super::fast_lease::loopback;
use super::*;
use fast_plane::lease::{LeaseChildConfig, LeaseChildren};
use playlists::mesh::ServedSubscription;
use std::net::IpAddr;
use std::os::fd::AsRawFd;
use tokio::time::timeout;

/// The process's memory is no longer collapsed into huge pages from now
/// (`PR_SET_THP_DISABLE`): khugepaged's collapse of a range unmaps it for a
/// moment, and a thread that touches it then takes a fault that no code of
/// the thread caused (this box runs THP `always`). What is left to count is
/// the thread's own first touches.
fn no_huge_page_collapse() {
    // SAFETY: prctl with integer arguments; a refusal leaves THP as it was.
    unsafe { libc::prctl(libc::PR_SET_THP_DISABLE, 1, 0, 0, 0) };
}

fn served(child: Ipv4Addr, stream_id: u64) -> ServedSubscription {
    ServedSubscription {
        child: SocketAddr::from((child, 9_101)),
        stream_id,
        backfill_parts_pending: 0,
        rank: None,
    }
}

/// A compact listener of `stream_id`, and the first part it was sent.
async fn listener(
    edge: &LaneEdge,
    stream_id: u64,
    seed: u64,
) -> (Arc<Shared>, tokio::task::JoinHandle<ListenerRun>, u64) {
    let shared = Shared::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(listen(
        edge.client_port,
        stream_id,
        Mode::Compact,
        true,
        PathLoss::Clean,
        seed,
        Arc::clone(&shared),
        tx,
    ));
    let first = timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
    (shared, task, first)
}

/// An edge whose audio thread also feeds lease children (to cover both in
/// one thread): compact listeners join two streams, the lease children of
/// one change, and a second stream arrives, all while the thread runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_audio_class_thread_allocates_and_frees_nothing_once_it_runs() {
    no_huge_page_collapse();
    let counter = crate::alloc_count::AllocCounter::leaked();
    let children = [loopback(72), loopback(73)];
    let lease = LeaseChildren::bind(
        LeaseChildConfig {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            child_ports: [unused_loopback_addr().port(), unused_loopback_addr().port()],
            class_port_offset: 0,
            primary_children: vec![IpAddr::V4(children[0])],
            max_children: 16,
            ranked: true,
            warm_hold_us: 1_000_000,
        },
        4 << 20,
    )
    .unwrap();
    let (stream_a, stream_b) = (4_101_u64, 4_102_u64);
    lease.rebuild(&[served(children[0], stream_a)]);
    let edge = LaneEdge::start_configured(loopback(71), Duration::ZERO, 64, true, |config| {
        config.lease = Some(Arc::clone(&lease));
        config.test_alloc_counter = Some(counter);
    })
    .await;
    let payload = Payload::Opus1;
    let publish = |stream_id: u64, sequence: u64| {
        let (envelope, _) = payload.envelope(stream_id, sequence);
        payload.symbols(&envelope)
    };
    edge.send(false, &publish(stream_a, 0), false).await;
    wait_for_part(&edge.cache, stream_a, 0).await;
    let (shared_a, listener_a, first_a) = listener(&edge, stream_a, 71).await;

    // 1 ms parts of stream A; at part 100 the lease children change (a
    // second child of A, and B leased before it exists); from part 150
    // stream B arrives, new to the thread; at part 200 a listener joins B,
    // at part 250 a second listener joins A.
    let parts = 400_u64;
    let b_from = 150_u64;
    let mut listener_b = None;
    let mut listener_a2 = None;
    let start = Instant::now();
    for sequence in 1..parts {
        tokio::time::sleep_until((start + Duration::from_millis(sequence)).into()).await;
        edge.send(false, &publish(stream_a, sequence), false).await;
        if sequence == 100 {
            lease.rebuild(&[
                served(children[0], stream_a),
                served(children[1], stream_a),
                served(children[0], stream_b),
            ]);
        }
        if sequence >= b_from {
            edge.send(false, &publish(stream_b, sequence - b_from), false)
                .await;
        }
        if sequence == 200 {
            listener_b = Some(listener(&edge, stream_b, 72).await);
        }
        if sequence == 250 {
            listener_a2 = Some(listener(&edge, stream_a, 73).await);
        }
    }
    let last_a = parts - 1;
    let last_b = parts - 1 - b_from;
    wait_for_part(&edge.cache, stream_b, last_b).await;
    // Every part the listeners take (they would hold on for a quiet edge;
    // the edge's stop ends them).
    let (shared_b, task_b, first_b) = listener_b.unwrap();
    let (shared_a2, task_a2, first_a2) = listener_a2.unwrap();
    let runs = [
        (&shared_a, first_a, last_a),
        (&shared_b, first_b, last_b),
        (&shared_a2, first_a2, last_a),
    ];
    for (shared, _, last) in runs {
        shared.last.store(last, Ordering::Relaxed);
    }
    // The paths ran: the lane's compact parts, the lease children, the
    // dedup of the second forwarder's copies, the tee's commits.
    let fast_plane = edge.cache.relay_ingress_snapshot().fast_plane;
    assert!(edge.lane.stats.compact_parts.load(Ordering::Relaxed) > 0);
    // Passes that made window repair: each part's job handed to the repair
    // thread through its ring (av-mesh-fix-3), none dropped, and the rows
    // sent.
    assert!(
        edge.lane.stats.repair_handoffs.load(Ordering::Relaxed) > 0,
        "the class thread handed window repair over"
    );
    assert_eq!(edge.lane.stats.repair_dropped.load(Ordering::Relaxed), 0);
    assert!(
        edge.router
            .edge_udp_stats
            .compact_window_repair
            .load(Ordering::Relaxed)
            > 0,
        "window repair rows were sent"
    );
    assert!(fast_plane.lease_children.audio_sent > 0, "{fast_plane:?}");
    assert!(fast_plane.duplicates > 0);
    assert_eq!(fast_plane.tee_dropped, 0);
    let feeds = edge.cache.parent_feeds().unwrap().snapshot(now_unix_us());
    assert!(
        feeds.iter().any(|feed| feed.stream_id == Some(stream_b)),
        "the new stream's feed entry was made (by the tee)"
    );
    // The thread's last passes of the run (and idle ones after them).
    tokio::time::sleep(Duration::from_millis(500)).await;
    let watched = Watched::read(counter);
    edge.stop().await;
    for (run, (_, first, last)) in [listener_a, task_b, task_a2].into_iter().zip(runs) {
        let run = run.await.unwrap();
        assert!(run.compact_confirmed, "{run:?}");
        assert!(
            (first..=last).all(|part| run.parts.contains_key(&part)),
            "every part from {first} to {last}"
        );
        assert!(
            run.fast_lane_parts > 0,
            "the class thread's lane carried parts"
        );
    }
    watched.assert_nothing(parts);
}

/// What the watched audio thread did once it ran.
#[derive(Debug)]
struct Watched {
    installed: bool,
    passes: u64,
    allocs: u64,
    frees: u64,
    faults: u64,
}

impl Watched {
    fn read(counter: &crate::alloc_count::AllocCounter) -> Self {
        Self {
            installed: counter.installed.load(Ordering::Acquire),
            passes: counter.passes.load(Ordering::Relaxed),
            allocs: counter.allocs.load(Ordering::Relaxed),
            frees: counter.frees.load(Ordering::Relaxed),
            faults: counter.faults(),
        }
    }

    /// The thread ran its loop (at least a pass for each of `parts`'
    /// batches, give or take) and allocated, freed and first touched
    /// nothing in it.
    fn assert_nothing(&self, parts: u64) {
        assert!(self.installed, "the class thread installed the counter");
        assert!(
            self.passes >= parts / 4,
            "{self:?}: the loop's passes were counted"
        );
        assert_eq!(
            (self.allocs, self.frees),
            (0, 0),
            "allocations and frees on the class thread once it ran \
             (NEEDLETAIL_ALLOC_TRACE=1 prints where)"
        );
        assert_eq!(
            self.faults, 0,
            "{self:?}: minor faults on the class thread once it ran"
        );
    }
}

/// Count what arrives at `socket` until it is quiet for 300 ms.
fn count_arrivals(socket: UdpSocket) -> tokio::task::JoinHandle<u64> {
    tokio::spawn(async move {
        let mut buf = [0_u8; 2_048];
        let mut count = 0;
        while let Ok(Ok(_)) = timeout(Duration::from_millis(300), socket.recv_from(&mut buf)).await
        {
            count += 1;
        }
        count
    })
}

/// A forwarder's audio thread (D3 review 3): two compiled children (one of
/// every symbol, one of sources), burst copies, loss reports to its
/// parents, and lease children, with both parents sending every symbol.
/// The lease children change and a new stream arrives while it runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarder_s_audio_thread_allocates_frees_and_faults_nothing_once_it_runs() {
    no_huge_page_collapse();
    let counter = crate::alloc_count::AllocCounter::leaked();
    let offset = 1_000;
    let (all_child, _all_video) = bind_class_pair(offset).await;
    let (source_child, _source_video) = bind_class_pair(offset).await;
    // A repair-only child: the audio thread keeps the sources held back from
    // it (its warm buffer, made whole with the path; av-mesh-fix-3).
    let (repair_child, _repair_video) = bind_class_pair(offset).await;
    let lease_ip = loopback(74);
    let lease_child = UdpSocket::bind((lease_ip, 0)).await.unwrap();
    let lease_port = lease_child.local_addr().unwrap().port();
    let lease = LeaseChildren::bind(
        LeaseChildConfig {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            child_ports: [lease_port, unused_loopback_addr().port()],
            class_port_offset: offset,
            primary_children: vec![IpAddr::V4(lease_ip)],
            max_children: 16,
            ranked: true,
            warm_hold_us: 1_000_000,
        },
        4 << 20,
    )
    .unwrap();
    let (stream_a, stream_b) = (4_201_u64, 4_202_u64);
    lease.rebuild(&[served(lease_ip, stream_a)]);
    // A promoted secondary takes sources as the primary does: both
    // parents send every symbol, and the second copy is a duplicate.
    let relay = start_fast_relay_with(
        ParentPath::PromotedSecondary,
        &[
            (all_child.local_addr().unwrap(), RelayForwardRole::All),
            (source_child.local_addr().unwrap(), RelayForwardRole::Source),
            (repair_child.local_addr().unwrap(), RelayForwardRole::Repair),
        ],
        fast_plane::relay::RelayClassConfig {
            port_offset: offset,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 8_192,
            burst_copy_ms: 5,
            loss_report: true,
            lease: Some(Arc::clone(&lease)),
            test_alloc_counter: Some(counter),
            ..Default::default()
        },
    )
    .await;
    // The repair-only child's sources are held back (warm), not sent as a
    // one-symbol object's repair lane would take them.
    relay
        .forwarder
        .as_ref()
        .expect("a forwarder")
        .repair_lane_single_symbol
        .store(false, Ordering::Relaxed);
    let arrivals = [
        count_arrivals(all_child),
        count_arrivals(source_child),
        count_arrivals(lease_child),
    ];
    let deadline = now_unix_us() + 30_000_000;
    let symbols = |stream_id: u64, sequence: u64| {
        let envelope =
            encode_test_canonical_opaque_object(stream_id, sequence, &[sequence as u8; 120]);
        let (mut sources, repairs) = fast_test_symbols(
            &envelope,
            relay_session::MediaPriority::Audio,
            400,
            48,
            1,
            deadline,
        );
        sources.extend(repairs);
        sources
    };
    // 1 ms parts of stream A from both parents; at part 100 the lease
    // children change (B leased before it exists); from part 150 stream B
    // arrives, new to the thread.
    let parts = 400_u64;
    let b_from = 150_u64;
    let start = Instant::now();
    for sequence in 0..parts {
        tokio::time::sleep_until((start + Duration::from_millis(sequence)).into()).await;
        let mut batch = symbols(stream_a, sequence);
        if sequence >= b_from {
            batch.extend(symbols(stream_b, sequence - b_from));
        }
        for symbol in &batch {
            relay
                .primary_sender
                .send_to(symbol, relay.primary_audio)
                .await
                .unwrap();
            relay
                .secondary_sender
                .send_to(symbol, relay.secondary_audio)
                .await
                .unwrap();
        }
        if sequence == 100 {
            lease.rebuild(&[served(lease_ip, stream_a), served(lease_ip, stream_b)]);
        }
    }
    wait_for_part(&relay.cache, stream_b, parts - 1 - b_from).await;
    // The burst copies' last ones, and idle passes after them.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let watched = Watched::read(counter);
    let stats = relay.stats();
    let forwarder = relay.forwarder.clone().expect("a forwarder");
    let warm_buffered = forwarder
        .warm_source_buffered_datagrams
        .load(Ordering::Relaxed)
        + forwarder.warm_source_evicted_datagrams.load(Ordering::Relaxed)
        + forwarder.warm_source_expired_datagrams.load(Ordering::Relaxed)
        + forwarder.warm_source_retired_datagrams.load(Ordering::Relaxed);
    let warm_refused = forwarder
        .warm_source_refused_datagrams
        .load(Ordering::Relaxed);
    drop(forwarder);
    relay.stop().await;
    let [all, sources, leased] = arrivals;
    let (all, sources, leased) = (
        all.await.unwrap(),
        sources.await.unwrap(),
        leased.await.unwrap(),
    );
    // The paths ran: both compiled children (the source child took no
    // repair), the burst copies, the lease child, the second parent's
    // duplicates, the tee's commits.
    let symbols_sent = (parts + (parts - b_from)) * 2;
    assert!(
        all >= symbols_sent,
        "{all} of {symbols_sent} symbols at the child of every symbol"
    );
    assert!(
        sources >= symbols_sent / 2 && sources < all,
        "{sources} at the source child"
    );
    assert!(leased > 0, "the lease child took its streams");
    assert!(stats.burst_copies > 0, "{stats:?}");
    assert!(stats.lease_children.audio_sent > 0, "{stats:?}");
    assert!(stats.duplicates > 0, "{stats:?}");
    assert_eq!(stats.tee_dropped, 0);
    assert!(warm_buffered > 0, "the repair-only child's sources were kept");
    assert_eq!(warm_refused, 0, "no warm source refused (lock or room)");
    watched.assert_nothing(parts);
}

// ------------------------------------------------------------------ video

/// 4K frames of `stream_id` (40 Mbit/s at 24 frames a second, the deploy's
/// 1,316-byte symbols, a twentieth of repair: about 170 symbols a part).
fn video_frames(stream_id: u64, count: usize) -> Vec<super::video_throughput::Frame> {
    super::video_throughput::make_frames_shaped(
        stream_id,
        0,
        count,
        40_000_000,
        super::video_throughput::FrameShape {
            fps: 24,
            symbol_bytes: super::video_tee::SYMBOL_BYTES,
            repair_per_sources: 20,
        },
    )
}

/// Send every datagram to `target` from `fd`, never waiting on a full
/// socket for long.
async fn send_all(fd: std::os::fd::RawFd, target: SocketAddr, datagrams: &[Vec<u8>]) {
    let mut sent = 0;
    while sent < datagrams.len() {
        match fast_plane::sys::send_batch(fd, datagrams.len() - sent, |index| {
            (&datagrams[sent + index][..], target)
        }) {
            Ok(0) | Err(_) => tokio::time::sleep(Duration::from_micros(200)).await,
            Ok(taken) => sent += taken,
        }
    }
}

/// A video socket at `ip` whose audio port (its port less `offset`) a
/// lease child config can name.
async fn video_child(ip: Ipv4Addr, offset: u16) -> (UdpSocket, u16) {
    loop {
        let socket = UdpSocket::bind((ip, 0)).await.unwrap();
        let port = socket.local_addr().unwrap().port();
        if port > offset {
            let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(8 << 20);
            return (socket, port - offset);
        }
    }
}

/// Count what arrives at `socket` until it is quiet for 300 ms (after a
/// first datagram, or 10 s).
fn count_video(socket: UdpSocket) -> tokio::task::JoinHandle<u64> {
    tokio::spawn(async move {
        let mut buf = vec![0_u8; 65_536];
        let mut count = 0;
        let mut quiet = Duration::from_secs(10);
        while let Ok(Ok(_)) = timeout(quiet, socket.recv_from(&mut buf)).await {
            count += 1;
            quiet = Duration::from_millis(300);
        }
        count
    })
}

/// A forwarder's video thread (fix4-video, task 3) allocates, frees and
/// first touches nothing once it runs: 4K from both parents (the second
/// copy a duplicate) to a compiled child of every symbol and a repair-only
/// one (its sources kept warm), to a primary lease child and, from the
/// twelfth part, a repair-only lease child (its sources kept warm); at the
/// 48th the secondary children are promoted and their warm rings replayed
/// (as the failover listeners do), and from the 36th a second stream
/// arrives, new to the thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarder_s_video_thread_allocates_frees_and_faults_nothing_once_it_runs() {
    no_huge_page_collapse();
    let counter = crate::alloc_count::AllocCounter::leaked();
    let offset = 1_000;
    let (all_child, all_video) = bind_class_pair(offset).await;
    let (repair_child, repair_video) = bind_class_pair(offset).await;
    for socket in [&all_video, &repair_video] {
        let _ = socket2::SockRef::from(socket).set_recv_buffer_size(8 << 20);
    }
    let (primary_ip, warm_ip) = (loopback(81), loopback(82));
    let (primary_lease, primary_port) = video_child(primary_ip, offset).await;
    let (warm_lease, warm_port) = video_child(warm_ip, offset).await;
    let lease = LeaseChildren::bind(
        LeaseChildConfig {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            child_ports: [primary_port, warm_port],
            class_port_offset: offset,
            primary_children: vec![IpAddr::V4(primary_ip)],
            max_children: 16,
            ranked: true,
            warm_hold_us: 1_000_000,
        },
        8 << 20,
    )
    .unwrap();
    let (stream_a, stream_b) = (1_u64, 2_u64);
    lease.rebuild(&[served(primary_ip, stream_a)]);
    let cache = LiveTsCache::new(1, Duration::from_millis(42), 2, 96, 2_048).await;
    let relay = start_fast_relay_in(
        ParentPath::PromotedSecondary,
        &[
            RelayForwardEndpoint {
                bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                target: all_child.local_addr().unwrap(),
                role: RelayForwardRole::All,
            },
            RelayForwardEndpoint {
                bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                target: repair_child.local_addr().unwrap(),
                role: RelayForwardRole::Repair,
            },
        ],
        fast_plane::relay::RelayClassConfig {
            port_offset: offset,
            cpus: [None, None],
            realtime: false,
            lease: Some(Arc::clone(&lease)),
            // The video thread measures its lanes against ledgers too
            // (4k-repair): the meters and their reports are in the count.
            loss_report: true,
            test_video_alloc_counter: Some(counter),
            ..Default::default()
        },
        cache,
    )
    .await;
    let forwarder = relay.forwarder.clone().expect("a forwarder");
    forwarder.configure_buffers(8 << 20);
    let arrivals = [
        count_video(all_video),
        count_video(repair_video),
        count_video(primary_lease),
        count_video(warm_lease),
    ];
    let (frames_a, frames_b) = (video_frames(stream_a, 72), video_frames(stream_b, 36));
    let (primary_fd, secondary_fd) = (
        relay.primary_sender.as_raw_fd(),
        relay.secondary_sender.as_raw_fd(),
    );
    let (primary_video, secondary_video) =
        (relay.primary_video.unwrap(), relay.secondary_video.unwrap());
    let repair_target = repair_child.local_addr().unwrap();
    let start = Instant::now();
    let interval = Duration::from_micros(41_667);
    let (mut primary_sent, mut secondary_sent) = (0_u64, 0_u64);
    for (index, frame) in frames_a.iter().enumerate() {
        tokio::time::sleep_until((start + interval * index as u32).into()).await;
        // The primary lane takes the sources, the promoted secondary all.
        send_all(primary_fd, primary_video, &frame.datagrams[..frame.sources]).await;
        send_all(secondary_fd, secondary_video, &frame.datagrams).await;
        primary_sent += frame.sources as u64;
        secondary_sent += frame.datagrams.len() as u64;
        if index >= 36 {
            send_all(secondary_fd, secondary_video, &frames_b[index - 36].datagrams).await;
            secondary_sent += frames_b[index - 36].datagrams.len() as u64;
        }
        // Each lane's ledger (RLL1), every few parts.
        if index % 6 == 5 {
            let ledger = fast_plane::loss_report::encode_ledger(primary_sent);
            send_all(primary_fd, primary_video, &[ledger.to_vec()]).await;
            let ledger = fast_plane::loss_report::encode_ledger(secondary_sent);
            send_all(secondary_fd, secondary_video, &[ledger.to_vec()]).await;
        }
        match index {
            12 => lease.rebuild(&[served(primary_ip, stream_a), served(warm_ip, stream_a)]),
            30 => lease.rebuild(&[
                served(primary_ip, stream_a),
                served(warm_ip, stream_a),
                served(primary_ip, stream_b),
                served(warm_ip, stream_b),
            ]),
            48 => {
                assert_eq!(
                    forwarder.apply_failover_mode(
                        repair_target,
                        FailoverForwardMode::SourceAndRepair,
                        false
                    ),
                    Some(true)
                );
                forwarder.replay_warm_sources(repair_target).await;
                assert_eq!(
                    lease.apply_failover(IpAddr::V4(warm_ip), FailoverForwardMode::SourceAndRepair),
                    Some(true)
                );
                let child = lease.child(IpAddr::V4(warm_ip)).expect("the warm child");
                crate::fast_control::replay_warm(&lease, &child).await;
            }
            _ => {}
        }
    }
    wait_for_part(&relay.cache, stream_b, 35).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let watched = Watched::read(counter);
    let stats = relay.stats();
    let snapshot = forwarder.snapshot();
    drop(forwarder);
    relay.stop().await;
    let [all, repair, primary, warm] = arrivals;
    let (all, repair, primary, warm) = (
        all.await.unwrap(),
        repair.await.unwrap(),
        primary.await.unwrap(),
        warm.await.unwrap(),
    );
    let symbols: u64 = frames_a
        .iter()
        .chain(&frames_b)
        .map(|frame| frame.datagrams.len() as u64)
        .sum();
    assert!(all >= symbols, "{all} of {symbols} at the child of every symbol");
    assert!(repair > 0 && primary > 0 && warm > 0, "{repair} {primary} {warm}");
    assert!(stats.duplicates > 0, "{stats:?}");
    assert_eq!(stats.tee_dropped, 0, "{stats:?}");
    assert!(stats.lease_children.video_sent > 0, "{stats:?}");
    assert!(stats.lease_children.warm_buffered > 0, "{stats:?}");
    assert!(stats.lease_children.warm_replayed > 0, "{stats:?}");
    assert_eq!(stats.lease_children.warm_refused, 0, "{stats:?}");
    assert!(snapshot.warm_source_replayed_datagrams > 0, "{snapshot:?}");
    assert_eq!(snapshot.warm_source_refused_datagrams, 0, "{snapshot:?}");
    assert!(stats.loss_reports > 0, "the video lanes were measured: {stats:?}");
    watched.assert_nothing(72);
}

/// An edge's video thread (fix4-video, task 3) allocates, frees and first
/// touches nothing once it runs: 4K to a fast-lane client of one stream,
/// then a second stream arrives (new to the thread) and a client joins it.
#[test]
fn an_edge_s_video_thread_allocates_frees_and_faults_nothing_once_it_runs() {
    no_huge_page_collapse();
    let counter = crate::alloc_count::AllocCounter::leaked();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let edge = rt.block_on(async {
        tokio::spawn(LaneEdge::start_configured(
            loopback(83),
            Duration::ZERO,
            64,
            true,
            move |config| config.test_video_alloc_counter = Some(counter),
        ))
        .await
        .unwrap()
    });
    let (stream_a, stream_b) = (1_u64, 2_u64);
    let (frames_a, frames_b) = (video_frames(stream_a, 72), video_frames(stream_b, 36));
    let fd = edge.forwarder_fd(0);
    let target = edge.relay_port(0, true);
    let _ = socket2::SockRef::from(edge.forwarder_socket(0)).set_send_buffer_size(8 << 20);
    let send = |frame: &super::video_throughput::Frame| {
        rt.block_on(send_all(fd, target, &frame.datagrams));
    };
    send(&frames_a[0]);
    rt.block_on(wait_for_part(&edge.cache, stream_a, 0));
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(true));
    let client = |stream_id: u64| {
        let (tx, rx) = std::sync::mpsc::channel();
        let (port, stop, measuring) = (edge.client_port, Arc::clone(&stop), Arc::clone(&measuring));
        let reader = std::thread::spawn(move || {
            super::video_throughput::run_client(port, stream_id, stop, measuring, tx)
        });
        let first = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the edge subscribes the client");
        (reader, first)
    };
    let (reader_a, first_a) = client(stream_a);
    let mut reader_b = None;
    let start = Instant::now();
    let interval = Duration::from_micros(41_667);
    for (index, frame) in frames_a.iter().enumerate().skip(1) {
        std::thread::sleep((start + interval * index as u32).saturating_duration_since(Instant::now()));
        send(frame);
        if index >= 36 {
            send(&frames_b[index - 36]);
        }
        if index == 48 {
            reader_b = Some(client(stream_b));
        }
    }
    rt.block_on(wait_for_part(&edge.cache, stream_b, 35));
    std::thread::sleep(Duration::from_millis(300));
    let watched = Watched::read(counter);
    stop.store(true, Ordering::Relaxed);
    let received_a = reader_a.join().unwrap();
    let (reader_b, first_b) = reader_b.unwrap();
    let received_b = reader_b.join().unwrap();
    let missing = |received: &super::video_throughput::Received, first: u64, last: u64| {
        (first..=last)
            .filter(|part| !received.complete.contains_key(part))
            .count()
    };
    let parts = |received: &super::video_throughput::Received| {
        let mut parts: Vec<u64> = received.complete.keys().copied().collect();
        parts.sort_unstable();
        parts
    };
    assert_eq!(
        missing(&received_a, first_a.max(1), 71),
        0,
        "stream A whole from {first_a}: {:?}",
        parts(&received_a)
    );
    assert_eq!(
        // A client that joins mid-stream may wait for a part or two
        // (the listener side's); the thread's work is what is measured.
        missing(&received_b, first_b + 2, 35),
        0,
        "stream B whole from {first_b}: {:?}",
        parts(&received_b)
    );
    let stats = edge.cache.relay_ingress_snapshot().fast_plane;
    assert_eq!(stats.tee_dropped, 0, "{stats:?}");
    rt.block_on(edge.stop());
    watched.assert_nothing(72);
}
