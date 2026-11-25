//! Joins at a source restart (local proof 3, step 2), in process.
//!
//! On the local lab, listeners that joined 0.8 s before a source session
//! started got the old session's last part and were then anchored at its
//! number: their first join re-created the edge's lapsed playlist demand,
//! the cache mesh's warm copy brought the old session's last parts, and
//! those counted as fresh because freshness was judged by their arrival at
//! the edge. The edge admitted them at that head, 20 s old by the source's
//! clock, and their subscription stayed at its number.
//!
//! Here: a distributor and an edge on the cache mesh with demand
//! replication (the edge holds no copy until a listener asks), an old
//! session that stopped 3.5 s before a new one starts, and listeners that
//! join from 2 s before to 2 s after the new session's start, on the
//! compact audio lane and on the RaptorQ lane (video parts larger than a
//! datagram), and an LL-HLS reader whose request for the old session's next
//! part is held when the new session starts. Each listener gets the new
//! session's parts within 300 ms of the new session's first part reaching
//! the edge (or of its join, after it), every one from there, and never a
//! part of the old session once it has one of the new; one that joined
//! before the start gets nothing of the old session at all.

use super::fast_lane::{packaged_envelope, Packaging};
use super::*;
use av_mesh::client_director::{LegEvent, LegKey, LegStart, SessionLegs, StartFrom};
use av_mesh::client_session::{SessionConfig, UdpSession};
use av_mesh::client_udp::ObjectKind as UdpObjectKind;
use playlists::mesh::{CacheMesh, CacheMeshConfig, CacheMeshRole as MeshRole, ReplicationMode};
use std::sync::atomic::AtomicBool;

const STREAM: u64 = 3_001;
/// The old session's source epoch, and its first part: its numbers are far
/// above the new session's, which count from 0 (the two are told apart by
/// number).
const OLD_EPOCH_AGO_US: u64 = 60_000_000;
const OLD_FIRST: u64 = 40_000;
/// How long before the new session the old one stopped.
const STOPPED_FOR: Duration = Duration::from_millis(3_500);
/// The longest a listener may wait, past the new session's first part at
/// the edge (or its join), for the new session.
const BOUND: Duration = Duration::from_millis(300);

/// A lane: what the contributor publishes.
#[derive(Debug, Clone, Copy)]
struct Lane {
    name: &'static str,
    packaging: Packaging,
    part: Duration,
    old_len: usize,
    new_len: usize,
}

const AUDIO: Lane = Lane {
    name: "compact audio (2.5 ms Opus)",
    packaging: Packaging::Opus,
    part: Duration::from_micros(2_500),
    old_len: 80,
    new_len: 96,
};

const VIDEO: Lane = Lane {
    name: "RaptorQ (24 fps video, 12 kB parts)",
    packaging: Packaging::Fmp4Video,
    part: Duration::from_micros(41_667),
    old_len: 10_000,
    new_len: 12_000,
};

/// A distributor and an edge on the cache mesh, demand replication, the
/// edge's client UDP.
struct JoinEdge {
    dist: Arc<LiveTsCache>,
    edge: Arc<LiveTsCache>,
    router: AppRouter,
    client_port: SocketAddr,
    stats: Arc<edge_udp::EdgeUdpStats>,
    _dist_mesh: playlists::mesh::CacheMeshHandle,
    shutdown_tx: watch::Sender<()>,
}

impl JoinEdge {
    async fn start() -> Self {
        let dist_addr = unused_loopback_addr();
        let edge_addr = unused_loopback_addr();
        let dist =
            LiveTsCache::with_ring(1, Duration::from_micros(2_500), 400, 12_000, 64, 8, 0).await;
        let edge =
            LiveTsCache::with_ring(1, Duration::from_micros(2_500), 400, 12_000, 64, 8, 0).await;
        let mut dist_config = CacheMeshConfig::new("dist-a", "uk", dist_addr)
            .with_peer(edge_addr)
            .with_same_region_only(true)
            .with_role(MeshRole::Distributor)
            .with_replication(ReplicationMode::Demand)
            .with_subscription_lease(Duration::from_millis(900));
        dist_config.sync_interval = Duration::from_millis(2);
        dist_config.announce_interval = Duration::from_millis(100);
        let dist_mesh = CacheMesh::new(Arc::clone(&dist.chunk_cache), dist_config)
            .start()
            .await
            .unwrap();
        let (applied_tx, applied_rx) = mpsc::unbounded_channel();
        let mut edge_config = CacheMeshConfig::new("edge-1", "uk", edge_addr)
            .with_peer(dist_addr)
            .with_same_region_only(true)
            .with_role(MeshRole::Edge)
            .with_replication(ReplicationMode::Demand)
            .with_subscription_lease(Duration::from_millis(900))
            .with_applied_tx(applied_tx);
        edge_config.sync_interval = Duration::from_millis(2);
        edge_config.announce_interval = Duration::from_millis(100);
        let edge_mesh = Arc::new(
            CacheMesh::new(Arc::clone(&edge.chunk_cache), edge_config)
                .start()
                .await
                .unwrap(),
        );
        // The lab's demand (cache_mesh_lease_ms 3000, idle 8000) shortened.
        let demand = PlaylistDemand::new(PlaylistDemandConfig {
            enabled: true,
            lease: Duration::from_millis(900),
            idle: Duration::from_millis(3_000),
            warm_parts: 64,
            max_playlists: MAX_EDGE_PLAYLISTS,
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let router = app_router_for_tests(Arc::clone(&edge), Arc::clone(&edge_mesh))
            .with_playlist_demand(demand.clone());
        tokio::spawn(run_mesh_applied_parts(
            Arc::clone(&edge),
            router.edge_load.clone(),
            applied_rx,
            None,
            shutdown_rx.clone(),
        ));
        tokio::spawn(run_playlist_demand(
            Arc::clone(&edge),
            Arc::clone(&edge_mesh),
            router.edge_load.clone(),
            demand,
            shutdown_rx.clone(),
        ));
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sockets = edge_udp::EdgeSockets::single(socket);
        let client_port = sockets.for_stream(0).local_addr().unwrap();
        let stats: Arc<edge_udp::EdgeUdpStats> = Arc::default();
        let mut config = edge_udp::EdgeUdpConfig::with_defaults(1_200, 4_096);
        config.playout = Duration::from_secs(5);
        tokio::spawn(edge_udp::run_edge_udp(
            sockets,
            router.clone(),
            config,
            Arc::clone(&stats),
            Vec::new(),
            shutdown_rx,
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while edge_mesh.parents_for(STREAM).await.is_empty()
                || dist_mesh.peers().await.is_empty()
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the edge knows its distributor");
        Self {
            dist,
            edge,
            router,
            client_port,
            stats,
            _dist_mesh: dist_mesh,
            shutdown_tx,
        }
    }

    async fn publish(&self, lane: Lane, epoch: u64, sequence: u64, len: usize) {
        let (envelope, _) = packaged_envelope(lane.packaging, STREAM, epoch, sequence, len);
        self.dist
            .commit_stream_payload(STREAM, envelope)
            .await
            .unwrap();
    }

    /// Publish parts `from..to` at the lane's pace from `start`.
    async fn publish_paced(&self, lane: Lane, epoch: u64, len: usize, from: u64, to: u64, start: Instant) {
        for sequence in from..to {
            tokio::time::sleep_until((start + lane.part * (sequence - from) as u32).into()).await;
            self.publish(lane, epoch, sequence, len).await;
        }
    }

    fn stop(self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// What a listener took: each part, its length and when it came.
#[derive(Debug, Default)]
struct Listener {
    joined_at: Option<Instant>,
    parts: Vec<(u64, usize, Instant)>,
    events: Vec<LegEvent>,
    stalled_legs: u64,
    refusing_legs: u64,
}

/// A listener with one leg at `edge` from now until `stop`, asking the edge
/// again at a failed leg as the director does with one edge.
async fn listen(edge: SocketAddr, stop: Arc<AtomicBool>) -> Listener {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(8 << 20);
    let mut session = UdpSession::new(SessionConfig::new(STREAM), Instant::now());
    let mut outbox: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
    let start = LegStart {
        from: StartFrom::AfterNewest,
        token: None,
        token_wait: Duration::from_millis(500),
    };
    let mut key = LegKey(1);
    let mut run = Listener {
        joined_at: Some(Instant::now()),
        ..Listener::default()
    };
    session.add_leg(key, "edge-1", edge, start, Instant::now(), &mut |to, datagram| {
        outbox.push((to, datagram.to_vec()))
    });
    let mut buf = vec![0_u8; 65_536];
    loop {
        for (to, datagram) in outbox.drain(..) {
            let _ = socket.send_to(&datagram, to).await;
        }
        let now = Instant::now();
        if stop.load(Ordering::Relaxed) {
            break;
        }
        session.poll_legs(now, &mut |to, datagram| outbox.push((to, datagram.to_vec())));
        while let Some(event) = session.next_event() {
            if let LegEvent::Failed { leg, .. } | LegEvent::Refused { leg, .. } = &event {
                session.release_leg(*leg, now, &mut |to, datagram| {
                    outbox.push((to, datagram.to_vec()))
                });
                key = LegKey(key.0 + 1);
                session.add_leg(key, "edge-1", edge, start, now, &mut |to, datagram| {
                    outbox.push((to, datagram.to_vec()))
                });
            }
            run.events.push(event);
        }
        if !outbox.is_empty() {
            continue;
        }
        let wait = session
            .next_wake()
            .saturating_duration_since(now)
            .min(Duration::from_millis(10));
        let Ok(Ok((len, peer))) = tokio::time::timeout(wait, socket.recv_from(&mut buf)).await
        else {
            continue;
        };
        session.on_datagram(peer, &buf[..len], Instant::now(), &mut |to, datagram| {
            outbox.push((to, datagram.to_vec()))
        });
        while let Some(object) = session.next_object() {
            if object.kind == UdpObjectKind::Part {
                run.parts.push((object.part, object.payload.len(), Instant::now()));
            }
        }
    }
    session.leave(&mut |to, datagram| outbox.push((to, datagram.to_vec())));
    for (to, datagram) in outbox.drain(..) {
        let _ = socket.send_to(&datagram, to).await;
    }
    run.stalled_legs = session.stalled_legs;
    run.refusing_legs = session.refusing_legs;
    run
}

/// One join at `offset_ms` from the new session's start: the verdict, or
/// why it failed.
async fn join_at(lane: Lane, offset_ms: i64) -> Result<String, String> {
    let edge = JoinEdge::start().await;
    let now_us = now_unix_us();
    let old_epoch = now_us - OLD_EPOCH_AGO_US;
    let new_epoch = now_us;
    // The old session: one second of parts, then nothing.
    let old_parts = (Duration::from_secs(1).as_micros() / lane.part.as_micros()) as u64;
    edge.publish_paced(lane, old_epoch, lane.old_len, OLD_FIRST, OLD_FIRST + old_parts, Instant::now())
        .await;
    let session_start = Instant::now() + STOPPED_FOR;
    let new_parts = (Duration::from_millis(3_400).as_micros() / lane.part.as_micros()) as u64;
    let stop = Arc::new(AtomicBool::new(false));
    // When the new session's first part is at the edge.
    let first_new_at = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let watch_first = {
        let edge_cache = Arc::clone(&edge.edge);
        let first_new_at = Arc::clone(&first_new_at);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if edge_cache
                    .client_head_for_id(STREAM)
                    .await
                    .is_some_and(|(_, epoch)| epoch == new_epoch)
                {
                    first_new_at.lock().unwrap().get_or_insert(Instant::now());
                    return;
                }
                tokio::time::sleep(Duration::from_micros(500)).await;
            }
        })
    };
    let join_time = if offset_ms >= 0 {
        session_start + Duration::from_millis(offset_ms as u64)
    } else {
        session_start - Duration::from_millis((-offset_ms) as u64)
    };
    let listener = {
        let stop = Arc::clone(&stop);
        let port = edge.client_port;
        tokio::spawn(async move {
            tokio::time::sleep_until(join_time.into()).await;
            listen(port, stop).await
        })
    };
    tokio::time::sleep_until(session_start.into()).await;
    edge.publish_paced(lane, new_epoch, lane.new_len, 0, new_parts, session_start)
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, Ordering::Relaxed);
    let run = listener.await.unwrap();
    let _ = watch_first.await;
    let waiting = edge.stats.admitted_waiting.load(Ordering::Relaxed);
    let restarts = edge.stats.session_restarts.load(Ordering::Relaxed);
    let _ = &edge.router;
    edge.stop();

    let name = format!("{} join {offset_ms:+} ms", lane.name);
    let first_new_at = (*first_new_at.lock().unwrap())
        .ok_or_else(|| format!("{name}: the new session never reached the edge"))?;
    let old = |part: u64| part >= OLD_FIRST;
    let first_new = run
        .parts
        .iter()
        .position(|(part, _, _)| !old(*part))
        .ok_or_else(|| format!("{name}: no part of the new session ({} parts)", run.parts.len()))?;
    let mut problems = Vec::new();
    if offset_ms < 0 && first_new > 0 {
        problems.push(format!(
            "a part of the old session at a join before the start: {:?}",
            run.parts[..first_new].iter().map(|(part, ..)| part).collect::<Vec<_>>()
        ));
    }
    if run.parts[first_new..].iter().any(|(part, _, _)| old(*part)) {
        problems.push("the old session after the new one".to_owned());
    }
    let from = first_new_at.max(run.joined_at.unwrap() + Duration::from_millis(20));
    let first_at = run.parts[first_new].2;
    let waited = first_at.saturating_duration_since(from);
    if waited > BOUND {
        problems.push(format!(
            "the new session's first part came {} ms after it reached the edge (or the join); events {:?}",
            waited.as_millis(),
            run.events
        ));
    }
    let mut longest = Duration::ZERO;
    for pair in run.parts[first_new..].windows(2) {
        longest = longest.max(pair[1].2.saturating_duration_since(pair[0].2));
    }
    if longest > BOUND {
        problems.push(format!("silent {} ms after the new session came", longest.as_millis()));
    }
    // Every part once, in any order (a part repaired by NACK comes after
    // later ones).
    let mut new: Vec<u64> = run.parts[first_new..].iter().map(|(part, ..)| *part).collect();
    new.sort_unstable();
    let start = new[0];
    let last = *new.iter().max().unwrap();
    if new != (start..=last).collect::<Vec<_>>() || last + 1 < new_parts {
        problems.push(format!(
            "the new session from {start} not whole to {}: {} parts, last {last}",
            new_parts - 1,
            new.len()
        ));
    }
    if run.stalled_legs + run.refusing_legs > 0 {
        problems.push(format!("legs failed: {:?}", run.events));
    }
    if !problems.is_empty() {
        return Err(format!(
            "{name}: first new part {} ms after it reached the edge (or the join): {}",
            waited.as_millis(),
            problems.join("; ")
        ));
    }
    Ok(format!(
        "{name}: first new part {} ms after it reached the edge (or the join), from part {start}; \
         admitted waiting {waiting}, restarted {restarts}",
        waited.as_millis()
    ))
}

/// Joins from 2 s before to 2 s after the new session's start, every
/// 100 ms, on the compact audio lane; a few of them on the RaptorQ lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joins_around_a_source_restart_reach_the_new_session_at_once() {
    let mut runs: Vec<(Lane, i64)> = (-20..=20).map(|step| (AUDIO, step * 100)).collect();
    runs.extend([-2_000, -800, -300, 0, 200, 1_000].map(|offset| (VIDEO, offset)));
    let mut results = Vec::new();
    for chunk in runs.chunks(8) {
        let mut set = tokio::task::JoinSet::new();
        for (lane, offset) in chunk.iter().copied() {
            set.spawn(async move { (lane.name, offset, join_at(lane, offset).await) });
        }
        while let Some(done) = set.join_next().await {
            results.push(done.unwrap());
        }
    }
    results.sort_by_key(|(name, offset, _)| (*name, *offset));
    let failed: Vec<&String> = results
        .iter()
        .filter_map(|(_, _, result)| result.as_ref().err())
        .collect();
    for (_, _, result) in &results {
        match result {
            Ok(line) | Err(line) => eprintln!("{line}"),
        }
    }
    assert!(failed.is_empty(), "{} of {} joins failed:\n{failed:#?}", failed.len(), results.len());
}

/// The numbers of the parts that a media playlist lists, and their file
/// extension.
fn listed_parts(playlist: &str) -> (Vec<u64>, String) {
    let mut parts = Vec::new();
    let mut extension = String::from("m4s");
    for line in playlist.lines().filter(|line| line.starts_with("#EXT-X-PART:")) {
        let Some(uri) = line.split("URI=\"").nth(1).and_then(|rest| rest.split('"').next()) else {
            continue;
        };
        let Some((number, ext)) = uri.strip_prefix("part").and_then(|rest| rest.split_once('.')) else {
            continue;
        };
        if let Ok(number) = number.parse() {
            parts.push(number);
            extension = ext.to_owned();
        }
    }
    (parts, extension)
}

/// An LL-HLS reader of the old session, still asking for its next part
/// when the new session starts. (A reader that joins a stopped stream is
/// given an empty playlist, which lists no part of a warm copy whose source
/// has stopped, and joins the next session when it is listed.) Its request
/// for the part after the old session's last is held; when the new session
/// reaches the edge, the held request is answered at once (404: the number
/// is of the old session), and the next playlist lists the new session,
/// numbered from 0, so far below the part the reader asked for that it
/// starts again at the new session's newest parts (`needletail-reader`'s
/// `restart_target`). Before, the request was held to its 3 s deadline, and
/// a reader asked for the old numbers until the new ones passed them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ll_hls_reader_held_at_an_old_number_is_answered_when_the_new_session_comes() {
    let lane = VIDEO;
    let edge = JoinEdge::start().await;
    let now_us = now_unix_us();
    let old_parts = 48_u64;
    let path = format!("/live/{STREAM}/stream.m3u8?sid=reader-1");
    let old_session = {
        let dist = Arc::clone(&edge.dist);
        tokio::spawn(async move {
            let started = Instant::now();
            for sequence in OLD_FIRST..OLD_FIRST + old_parts {
                tokio::time::sleep_until((started + lane.part * (sequence - OLD_FIRST) as u32).into())
                    .await;
                let (envelope, _) = packaged_envelope(
                    lane.packaging,
                    STREAM,
                    now_us - OLD_EPOCH_AGO_US,
                    sequence,
                    lane.old_len,
                );
                dist.commit_stream_payload(STREAM, envelope).await.unwrap();
            }
        })
    };
    // The reader joins the old session and reloads its playlist every
    // 250 ms, as a player does.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut playlist = body_text(get_for_tests(&edge.router, &path).await);
    old_session.await.unwrap();
    let session_start = Instant::now() + STOPPED_FOR;
    while Instant::now() + Duration::from_millis(800) < session_start {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let reloaded = body_text(get_for_tests(&edge.router, &path).await);
        if !listed_parts(&reloaded).0.is_empty() {
            playlist = reloaded;
        }
    }
    let (listed, extension) = listed_parts(&playlist);
    let newest = *listed
        .iter()
        .max()
        .unwrap_or_else(|| panic!("the reader read the old session:\n{playlist}"));
    assert_eq!(newest, OLD_FIRST + old_parts - 1, "{playlist}");
    // The reader asks for the part after the newest listed.
    let wanted = newest + 1;
    let held = {
        let router = edge.router.clone();
        let uri = format!("/live/{STREAM}/part{wanted}.{extension}?sid=reader-1");
        tokio::spawn(async move {
            let response = get_for_tests(&router, &uri).await;
            (response.status, Instant::now())
        })
    };
    let first_new_at = {
        let edge_cache = Arc::clone(&edge.edge);
        tokio::spawn(async move {
            loop {
                if edge_cache
                    .client_head_for_id(STREAM)
                    .await
                    .is_some_and(|(_, epoch)| epoch == now_us)
                {
                    return Instant::now();
                }
                tokio::time::sleep(Duration::from_micros(500)).await;
            }
        })
    };
    tokio::time::sleep_until(session_start.into()).await;
    edge.publish_paced(lane, now_us, lane.new_len, 0, 24, session_start).await;
    let first_new_at = first_new_at.await.unwrap();
    let (status, answered_at) = held.await.unwrap();
    let after = answered_at.saturating_duration_since(first_new_at);
    assert_ne!(status, StatusCode::OK, "the old number is not served from the new session");
    assert!(after <= BOUND, "the held request answered {} ms after the new session came", after.as_millis());
    // The playlist now lists the new session, far below the wanted part:
    // the reader starts again there.
    let playlist = body_text(get_for_tests(&edge.router, &path).await);
    let (listed, _) = listed_parts(&playlist);
    let newest_now = *listed.iter().max().expect("the new session is listed");
    let two_seconds = (Duration::from_secs(2).as_micros() / lane.part.as_micros()) as u64;
    assert!(
        newest_now + listed.len() as u64 + two_seconds < wanted,
        "the reader sees a new session: newest {newest_now}, {} listed, wanted {wanted}",
        listed.len()
    );
    edge.stop();
}
