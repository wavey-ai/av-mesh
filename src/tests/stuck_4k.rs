//! Stuck4k in process: a 4K viewer on one leg of the RaptorQ lane, at an
//! edge whose source session restarts (the contributor restarts at every
//! rung of a sweep).
//!
//! On Linode (video-w10, rung 2160p320), syd-edge-2 held two late copies of
//! the previous session's parts above the new session's: the cache mesh
//! wrote the previous session's part 1233 70 ms after the new session's
//! first part had zeroed the stream (`activated canonical media source
//! epoch` 20:49:00.968, part 1233 written 20:49:01.040), and the copy was
//! never recorded but raised the cache's watermark. Both UDP viewers that
//! joined 6.9 s later were given 1233 as the head, and the edge then waited
//! for 1234, which the new session reached 44 s later; its keepalives kept
//! every silence rule of the client from firing.
//!
//! Here: an edge fed on the fast plane, an old session (parts 1220 to 1233)
//! and a new one (from 0), the old session's last part written into the
//! edge's cache after the new session started (as the cache mesh wrote
//! it), and a one-leg viewer (the real session, with a stand-in for the
//! director that asks again at a failed leg) subscribing before or after
//! that write, or already watching when the source restarts. In each, the
//! viewer gets every part of the new session from near its head on, once,
//! and nothing of the old session after it.

use super::fast_lane::{packaged_envelope, LaneEdge, Packaging};
use super::fast_lease::loopback;
use super::*;
use av_mesh::client_director::{LegEvent, LegKey, LegStart, SessionLegs, StartFrom};
use av_mesh::client_session::{SessionConfig, UdpSession};
use av_mesh::client_udp::ObjectKind as UdpObjectKind;
use std::sync::atomic::AtomicBool;

const STREAM: u64 = 7;
/// The two sessions' source epochs (syd-edge-2's, 240 and 320 Mbit/s).
const OLD_EPOCH: u64 = 1_795_553_276_846_835;
const NEW_EPOCH: u64 = 1_795_553_340_807_636;
/// 4K frames of the two sessions, told apart by their length.
const OLD_LEN: usize = 250_000;
const NEW_LEN: usize = 300_000;
const OLD_FIRST: u64 = 1_220;
const OLD_LAST: u64 = 1_233;
const NEW_LAST: u64 = 80;
const PART: Duration = Duration::from_micros(41_667);

async fn publish(edge: &LaneEdge, epoch: u64, sequence: u64, len: usize, deadline_us: u64) {
    let (envelope, _) = packaged_envelope(Packaging::Fmp4Video, STREAM, epoch, sequence, len);
    let symbols = fast_test_symbols(
        &envelope,
        relay_session::MediaPriority::VideoKey,
        1_088,
        400,
        4,
        deadline_us,
    );
    edge.send(true, &symbols, false).await;
    wait_for_part(&edge.cache, STREAM, sequence).await;
}

/// What the viewer took: each part's lengths as they came out.
#[derive(Debug, Default)]
struct Viewer {
    parts: Vec<(u64, usize)>,
    events: Vec<LegEvent>,
    stalled_legs: u64,
    refusing_legs: u64,
    raptorq_refused: av_mesh::client_udp::RaptorqRefused,
}

/// A one-leg viewer at `edge` until `stop` and 300 ms with nothing more (at
/// most 30 s). A failed leg is released and the edge asked again, as the
/// director does with a lone edge.
async fn view(
    edge: SocketAddr,
    fast_lane: bool,
    live: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) -> Viewer {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(16 << 20);
    let config = SessionConfig {
        fast_lane,
        ..SessionConfig::new(STREAM)
    };
    let mut session = UdpSession::new(config, Instant::now());
    let mut outbox: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
    let start = LegStart {
        from: StartFrom::AfterNewest,
        token: None,
        token_wait: Duration::from_millis(500),
    };
    let mut key = LegKey(1);
    session.add_leg(key, "edge", edge, start, Instant::now(), &mut |to, datagram| {
        outbox.push((to, datagram.to_vec()))
    });
    let mut run = Viewer::default();
    let mut buf = vec![0_u8; 65_536];
    let started = Instant::now();
    let mut last_part_at = Instant::now();
    loop {
        for (to, datagram) in outbox.drain(..) {
            let _ = socket.send_to(&datagram, to).await;
        }
        let now = Instant::now();
        if stop.load(Ordering::Relaxed) && now.duration_since(last_part_at) >= Duration::from_millis(300)
        {
            break;
        }
        assert!(now.duration_since(started) < Duration::from_secs(30), "{run:?}");
        session.poll_legs(now, &mut |to, datagram| outbox.push((to, datagram.to_vec())));
        while let Some(event) = session.next_event() {
            match &event {
                LegEvent::Live { .. } => live.store(true, Ordering::Relaxed),
                LegEvent::Failed { leg, .. } | LegEvent::Refused { leg, .. } => {
                    session.release_leg(*leg, now, &mut |to, datagram| {
                        outbox.push((to, datagram.to_vec()))
                    });
                    key = LegKey(key.0 + 1);
                    session.add_leg(key, "edge", edge, start, now, &mut |to, datagram| {
                        outbox.push((to, datagram.to_vec()))
                    });
                }
                _ => {}
            }
            run.events.push(event);
        }
        if !outbox.is_empty() {
            continue;
        }
        let wait = session
            .next_wake()
            .saturating_duration_since(now)
            .min(Duration::from_millis(20));
        let Ok(Ok((len, peer))) = tokio::time::timeout(wait, socket.recv_from(&mut buf)).await
        else {
            continue;
        };
        session.on_datagram(peer, &buf[..len], Instant::now(), &mut |to, datagram| {
            outbox.push((to, datagram.to_vec()))
        });
        while let Some(object) = session.next_object() {
            if object.kind == UdpObjectKind::Part {
                run.parts.push((object.part, object.payload.len()));
                last_part_at = Instant::now();
            }
        }
    }
    session.leave(&mut |to, datagram| outbox.push((to, datagram.to_vec())));
    for (to, datagram) in outbox.drain(..) {
        let _ = socket.send_to(&datagram, to).await;
    }
    run.stalled_legs = session.stalled_legs;
    run.refusing_legs = session.refusing_legs;
    run.raptorq_refused = session.receiver.raptorq_refused;
    run
}

/// When the old session's late copy is written, against the viewer's join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Order {
    /// The new session runs, the copy is written, the viewer joins
    /// (syd-edge-2's order).
    CopyThenJoin,
    /// The new session runs, the viewer joins, the copy is written.
    JoinThenCopy,
    /// The viewer watches the old session; the source restarts; the copy
    /// is written after the new session's third part.
    WatchingAtRestart,
}

async fn run_order(order: Order, fast_lane: bool, ip: u8) {
    let name = format!("{order:?}, fast lane {fast_lane}");
    let edge = LaneEdge::start(loopback(ip)).await;
    let deadline_us = now_unix_us() + 120_000_000;
    let live = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let join = |live: &Arc<AtomicBool>, stop: &Arc<AtomicBool>| {
        tokio::spawn(view(
            edge.client_port,
            fast_lane,
            Arc::clone(live),
            Arc::clone(stop),
        ))
    };
    let mut viewer = None;
    let paced = |sequence: u64, from: u64, start: Instant| {
        tokio::time::sleep_until((start + PART * (sequence - from) as u32).into())
    };
    // The old session.
    let start = Instant::now();
    for sequence in OLD_FIRST..=OLD_LAST {
        paced(sequence, OLD_FIRST, start).await;
        publish(&edge, OLD_EPOCH, sequence, OLD_LEN, deadline_us).await;
        if order == Order::WatchingAtRestart && sequence == OLD_FIRST + 2 {
            viewer = Some(join(&live, &stop));
        }
    }
    // The cache's copy of the old session's last part, as the cache mesh
    // wrote it.
    let (late_copy, _) = edge
        .cache
        .chunk_cache
        .get_for_stream_id(STREAM, OLD_LAST as usize)
        .await
        .expect("the old session's last part");
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The new session, counting from 0.
    let start = Instant::now();
    let copy_at = match order {
        Order::WatchingAtRestart => 3,
        _ => 10,
    };
    for sequence in 0..=NEW_LAST {
        paced(sequence, 0, start).await;
        publish(&edge, NEW_EPOCH, sequence, NEW_LEN, deadline_us).await;
        let write_copy = sequence == copy_at;
        if order == Order::JoinThenCopy && sequence == copy_at - 2 {
            viewer = Some(join(&live, &stop));
            tokio::time::timeout(Duration::from_secs(3), async {
                while !live.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{name}: the viewer is live"));
        }
        if write_copy {
            // The late copy, as the cache mesh wrote it on syd-edge-2. The
            // stream's epoch floor refuses it now (integration 5): no stale
            // part in the cache, and the watermark is the new session's.
            let refused = edge
                .cache
                .chunk_cache
                .add_for_stream_id(STREAM, OLD_LAST as usize, late_copy.clone())
                .await;
            assert!(refused.is_err(), "{name}: the late copy was written");
            assert_eq!(edge.head(STREAM).await, Some(sequence), "{name}");
            assert!(
                edge.cache
                    .chunk_cache
                    .get_for_stream_id(STREAM, OLD_LAST as usize)
                    .await
                    .is_none(),
                "{name}: a stale part in the cache"
            );
            if order == Order::CopyThenJoin {
                viewer = Some(join(&live, &stop));
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    let run = viewer.expect("a viewer").await.unwrap();
    let restarts = edge.stats.session_restarts.load(Ordering::Relaxed);
    edge.stop().await;

    // The new session, from its first part on: every part once.
    let first_new = run
        .parts
        .iter()
        .position(|(_, len)| *len == NEW_LEN)
        .unwrap_or_else(|| panic!("{name}: no part of the new session: {run:?}"));
    let new: Vec<u64> = run.parts[first_new..].iter().map(|(part, _)| *part).collect();
    assert!(
        run.parts[first_new..].iter().all(|(_, len)| *len == NEW_LEN),
        "{name}: the old session after the new: {:?}",
        run.parts
    );
    let from = new[0];
    assert_eq!(new, (from..=NEW_LAST).collect::<Vec<_>>(), "{name}: {run:?}");
    match order {
        Order::WatchingAtRestart => {
            // From the new session's first part or so (the cache-fed
            // lane's first new part is refused while the old session
            // holds), and the old session whole before it.
            assert!(from <= 3, "{name}: the new session from {from}");
            let old: Vec<u64> = run.parts[..first_new].iter().map(|(part, _)| *part).collect();
            assert_eq!(old.last(), Some(&OLD_LAST), "{name}: {old:?}");
            assert_eq!(restarts, 1, "{name}: the subscriber started again at the new head");
        }
        Order::CopyThenJoin | Order::JoinThenCopy => {
            // Never the old session's part as the head.
            assert_eq!(first_new, 0, "{name}: {:?}", &run.parts[..first_new]);
            assert!(from <= copy_at + 3, "{name}: joined at {from}");
        }
    }
    assert_eq!(run.stalled_legs, 0, "{name}: {run:?}");
    assert_eq!(run.refusing_legs, 0, "{name}: {run:?}");
    assert!(
        !run.events.iter().any(|event| matches!(event, LegEvent::Failed { .. })),
        "{name}: {:?}",
        run.events
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_4k_viewer_converges_on_a_restarted_session_whatever_the_order() {
    let mut ip = 250;
    for order in [
        Order::CopyThenJoin,
        Order::JoinThenCopy,
        Order::WatchingAtRestart,
    ] {
        for fast_lane in [true, false] {
            run_order(order, fast_lane, ip).await;
            ip += 1;
        }
    }
}

/// Integration 5: late copies of the old session at the numbers the new
/// session then reaches are refused on every cache write path (the stream's
/// epoch floor), never lift the watermark, and do not block the new
/// session's own parts at those numbers: these are committed and an LL-HLS
/// reader is served them (stuck4k's probe on syd-edge-2 was served the old
/// session's 1145-1150 and the new session's own were refused as identity
/// conflicts).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_new_session_takes_the_numbers_the_old_session_s_late_copies_held() {
    let edge = LaneEdge::start(loopback(244)).await;
    let deadline_us = now_unix_us() + 120_000_000;
    for sequence in 30..=45 {
        publish(&edge, OLD_EPOCH, sequence, OLD_LEN, deadline_us).await;
    }
    let mut late = Vec::new();
    for sequence in 40..=45_u64 {
        let (bytes, _) = edge
            .cache
            .chunk_cache
            .get_for_stream_id(STREAM, sequence as usize)
            .await
            .expect("an old part");
        late.push((sequence, bytes));
    }
    for sequence in 0..=50_u64 {
        publish(&edge, NEW_EPOCH, sequence, NEW_LEN, deadline_us).await;
        if sequence == 5 || sequence == 39 {
            for (part, bytes) in &late {
                let id = *part as usize;
                let added = edge.cache.chunk_cache.add_for_stream_id(STREAM, id, bytes.clone()).await;
                assert!(added.is_err(), "part {part} after new {sequence}: written");
                let put = edge
                    .cache
                    .chunk_cache
                    .put_if_absent_for_stream_id(STREAM, id, bytes.clone())
                    .await;
                assert_eq!(put, Ok(PutIfAbsentResult::Superseded), "part {part}");
                assert!(
                    edge.cache.chunk_cache.get_for_stream_id(STREAM, id).await.is_none(),
                    "part {part}: a stale part in the cache"
                );
            }
            assert_eq!(edge.head(STREAM).await, Some(sequence), "the watermark");
        }
    }
    // The new session's own parts at the old copies' numbers: committed,
    // and served to an LL-HLS reader as the new session's.
    for (part, _) in &late {
        let (media, _, _, _) = edge
            .cache
            .read_part_for_stream_id(STREAM, *part)
            .await
            .unwrap_or_else(|| panic!("part {part} of the new session"));
        assert_eq!(media.len(), NEW_LEN, "part {part}");
        assert!(wait_for_part(&edge.cache, STREAM, *part).await.len() >= NEW_LEN);
    }
    let playlist = edge.cache.playlist_for_stream_id(STREAM).await;
    // A valid playlist over the new session, from its first part.
    assert!(assert_valid_ll_hls(&playlist) > 0, "{playlist}");
    assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:0\n"), "{playlist}");
    assert_eq!(edge.head(STREAM).await, Some(50));
    edge.stop().await;
}

/// Integration 5b (part repair's epoch floor): an answer to an ask of the
/// old session (repair symbols that complete one of its parts, with the
/// sources the asker held) that arrives after the new session started is
/// decoded at the tee and refused: it is neither written nor lifts the
/// watermark, and the new session's own parts at those numbers follow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_answer_for_the_old_session_never_lifts_the_new_session_s_watermark() {
    let edge = LaneEdge::start(loopback(245)).await;
    let deadline_us = now_unix_us() + 120_000_000;
    for sequence in 30..=39 {
        publish(&edge, OLD_EPOCH, sequence, OLD_LEN, deadline_us).await;
    }
    for sequence in 0..=20_u64 {
        publish(&edge, NEW_EPOCH, sequence, NEW_LEN, deadline_us).await;
    }
    // The old session's parts 40-45, as an asker held half their sources
    // and its parent's answer completed them with repair.
    for sequence in 40..=45_u64 {
        let (envelope, _) = packaged_envelope(Packaging::Fmp4Video, STREAM, OLD_EPOCH, sequence, OLD_LEN);
        let (sources, repairs) = fast_test_symbols(
            &envelope,
            relay_session::MediaPriority::VideoKey,
            1_088,
            400,
            u32::try_from(OLD_LEN / 1_088 + 4).unwrap(),
            deadline_us,
        );
        let half = sources.len() / 2;
        edge.send(true, &(sources[..half].to_vec(), repairs), false).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(edge.head(STREAM).await, Some(20), "the watermark");
    for sequence in 40..=45_usize {
        assert!(
            edge.cache.chunk_cache.get_for_stream_id(STREAM, sequence).await.is_none(),
            "part {sequence}: the old session's answer was written"
        );
    }
    for sequence in 21..=45_u64 {
        publish(&edge, NEW_EPOCH, sequence, NEW_LEN, deadline_us).await;
    }
    for sequence in 40..=45_u64 {
        let (media, _, _, _) = edge
            .cache
            .read_part_for_stream_id(STREAM, sequence)
            .await
            .unwrap_or_else(|| panic!("part {sequence} of the new session"));
        assert_eq!(media.len(), NEW_LEN, "part {sequence}");
    }
    assert_eq!(edge.head(STREAM).await, Some(45));
    edge.stop().await;
}
