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
        assert!(self.installed, "the audio thread installed the counter");
        assert!(
            self.passes >= parts / 4,
            "{self:?}: the loop's passes were counted"
        );
        assert_eq!(
            (self.allocs, self.frees),
            (0, 0),
            "allocations and frees on the audio class thread once it ran \
             (NEEDLETAIL_ALLOC_TRACE=1 prints where)"
        );
        assert_eq!(
            self.faults, 0,
            "{self:?}: minor faults on the audio class thread once it ran"
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
