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
    let (allocs, frees) = (
        counter.allocs.load(Ordering::Relaxed),
        counter.frees.load(Ordering::Relaxed),
    );
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
    assert_eq!(
        (allocs, frees),
        (0, 0),
        "allocations and frees on the audio class thread once it ran \
         (NEEDLETAIL_ALLOC_TRACE=1 prints where)"
    );
}
