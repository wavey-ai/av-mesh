//! The node's lifecycle in this process (C CR6, CR8): SIGTERM and SIGINT as
//! a graceful stop of an edge and of a forwarder, the drain reasons (a
//! start, an operator's drain kept across a restart, a stop), the start
//! that takes a node's place only once it is warm (an edge's warm copies, a
//! slot's start demand), and the demand loops' reaction to a parent that
//! changed with the make-before-break release of parents.

use super::fast_lane::{packaged_envelope, LaneEdge, Packaging};
use super::fast_lease::loopback;
use super::*;
use av_mesh::client_session::{Received, SessionConfig, UdpSession};
use lifecycle::{
    start_edge, start_forwarder, stop_edge, stop_forwarder, stop_signal, DrainState,
    LifecycleConfig, NodeLifecycle, STOP_HOLD,
};
use node_status::{DrainReason, NodeState};
use playlists::mesh::{CacheMesh, CacheMeshConfig};
use std::future::pending;
use std::net::IpAddr;

const STREAM: u64 = 4_301;

/// The tests that send this process a signal, one at a time: each listens
/// for its own.
static SIGNALS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn raise(signal: libc::c_int) {
    // SAFETY: kill(2) on this process with a signal that `stop_signal`
    // listens for; it has no memory effects.
    assert_eq!(unsafe { libc::kill(libc::getpid(), signal) }, 0);
}

/// A router of `role` with `lifecycle`, in region `uk`, whose status asks
/// a window of `warm_parts`.
fn router_with(
    name: &str,
    cache: Arc<LiveTsCache>,
    mesh: Arc<CacheMeshHandle>,
    role: CacheMeshRole,
    lifecycle: NodeLifecycle,
    warm_parts: u64,
) -> AppRouter {
    AppRouter::new(
        cache,
        1,
        mesh,
        broadcast::channel(AUDIO_EPOCH_BROADCAST_CAPACITY).0,
        MeshTransportConfigSnapshot::default(),
        mesh_node_for_tests(name, "uk", "europe", 51.5, -0.1),
        role,
        ReplicationPolicy::default(),
        ControlPlane::default(),
        ControlDispatch::default(),
        TelemetryAggregator::default(),
        DemandTracker::default(),
        lifecycle,
        None,
        DEFAULT_HLS_FAILOVER_BANDWIDTH_BPS,
        EdgeLoad::default(),
        EdgeLifecycleConfig::default(),
        ProvisionExecutor::disabled(),
        TelemetryPeerMonitor::default(),
        TelemetryFecMonitor::default(),
        PrivateDiscoveryStatus::default(),
        None,
    )
    .with_node_status(NodeStatusSource {
        warm_parts,
        ..NodeStatusSource::default()
    })
}

/// A cache-mesh node of the region `uk` with fast hellos and syncs.
fn mesh_config(
    name: &str,
    addr: SocketAddr,
    role: CacheMeshRole,
    lease: Duration,
) -> CacheMeshConfig {
    let mut config = CacheMeshConfig::new(name, "uk", addr)
        .with_same_region_only(true)
        .with_role(role)
        .with_replication(ReplicationMode::Demand)
        .with_subscription_lease(lease);
    config.sync_interval = Duration::from_millis(10);
    config.announce_interval = Duration::from_millis(100);
    config
}

async fn start_mesh(cache: &LiveTsCache, config: CacheMeshConfig) -> Arc<CacheMeshHandle> {
    Arc::new(
        CacheMesh::new(Arc::clone(&cache.chunk_cache), config)
            .start()
            .await
            .unwrap(),
    )
}

/// Wait until `done`, at most `within`.
async fn within(what: &str, within: Duration, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(within, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

/// A snapshot of another node of region `uk`: this node has a regional view
/// (E4) from it.
fn neighbour_snapshot() -> MeshSnapshot {
    telemetry_snapshot_for_tests("uk-neighbour", "uk", "europe", 51.5, -0.1, Vec::new(), 1)
}

fn control(router: &AppRouter) -> ControlRequest {
    ControlRequest {
        node_id: Some(router.node.node_id.clone()),
        region: None,
        stream_id: None,
    }
}

/// Part `sequence` of the stream, as the contributor's symbols.
fn symbols(sequence: u64, deadline_us: u64) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let (envelope, _) = packaged_envelope(Packaging::Pcm, STREAM, 0, sequence, 288);
    fast_test_symbols(
        &envelope,
        relay_session::MediaPriority::Audio,
        1_088,
        400,
        0,
        deadline_us,
    )
}

/// Feed `edges` the stream's parts from `first` every 2 ms until `stop`.
fn feed(
    edges: Vec<Arc<LaneEdge>>,
    first: u64,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<u64> {
    tokio::spawn(async move {
        let deadline_us = now_unix_us() + 120_000_000;
        let mut sequence = first;
        while !stop.load(Ordering::Relaxed) {
            let part = symbols(sequence, deadline_us);
            for edge in &edges {
                edge.send(false, &part, false).await;
            }
            sequence += 1;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        sequence
    })
}

/// What a listener saw.
#[derive(Debug, Default)]
struct Heard {
    /// Each `MOVE` (or `REFUSED`), when and from where.
    moves: Vec<(Instant, SocketAddr, String)>,
    /// Each first `OK` at an edge, when and where.
    subscribed: Vec<(Instant, SocketAddr)>,
    parts: u64,
}

/// A one-leg UDP listener on the session's one-edge API, as the wave 9
/// client: at a `MOVE` it begins at the next edge of `edges` (its `begin`
/// leaves the old one), or with one edge at that edge again 200 ms later;
/// with `follow` false it stays where it is told to leave. It leaves at
/// `stop`.
async fn listen(edges: Vec<SocketAddr>, follow: bool, stop: Arc<AtomicBool>) -> Heard {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut session = UdpSession::new(SessionConfig::new(STREAM), Instant::now());
    let mut outbox: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
    let mut heard = Heard::default();
    let mut at = 0_usize;
    let mut again: Option<Instant> = None;
    session.begin(edges[0], None, Instant::now(), &mut |to, datagram| {
        outbox.push((to, datagram.to_vec()))
    });
    let mut buf = vec![0_u8; 65_536];
    loop {
        for (to, datagram) in outbox.drain(..) {
            let _ = socket.send_to(&datagram, to).await;
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let now = Instant::now();
        if again.is_some_and(|due| now >= due) {
            again = None;
            session.begin(edges[at], None, now, &mut |to, datagram| {
                outbox.push((to, datagram.to_vec()))
            });
            continue;
        }
        if session
            .poll(now, &mut |to, datagram| {
                outbox.push((to, datagram.to_vec()))
            })
            .is_some()
            && follow
        {
            again = Some(now + Duration::from_millis(200));
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
        let outcome =
            session.on_datagram(peer, &buf[..len], Instant::now(), &mut |to, datagram| {
                outbox.push((to, datagram.to_vec()))
            });
        match outcome {
            Received::Move(reason) => {
                heard.moves.push((Instant::now(), peer, reason));
                if follow && edges.len() > 1 {
                    at = (at + 1) % edges.len();
                    session.begin(edges[at], None, Instant::now(), &mut |to, datagram| {
                        outbox.push((to, datagram.to_vec()))
                    });
                } else if follow {
                    again = Some(Instant::now() + Duration::from_millis(200));
                }
            }
            Received::Subscribed { first: true, .. } => {
                heard.subscribed.push((Instant::now(), peer))
            }
            _ => {}
        }
        while let Some(object) = session.next_object() {
            if object.kind == av_mesh::client_udp::ObjectKind::Part {
                heard.parts += 1;
            }
        }
    }
    session.leave(&mut |to, datagram| outbox.push((to, datagram.to_vec())));
    for (to, datagram) in outbox.drain(..) {
        let _ = socket.send_to(&datagram, to).await;
    }
    heard
}

/// An edge in this process, shared with the feeder.
async fn lane_edge(last: u8) -> Arc<LaneEdge> {
    Arc::new(LaneEdge::start(loopback(last)).await)
}

/// Stop an edge once the tasks that shared it have ended.
async fn stop_lane_edge(edge: Arc<LaneEdge>) {
    Arc::into_inner(edge)
        .expect("the feeder has ended")
        .stop()
        .await;
}

/// SIGTERM to an edge with a listener: the edge says it is stopping, its
/// listener is moved make-before-break to the other edge, and once it has
/// been empty for a second the edge releases its subscriptions and its stop
/// returns, well inside its bound; the edge then shuts down at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_drains_an_edge_then_exits_within_its_bound() {
    let _signals = SIGNALS.lock().await;
    let mut stop = stop_signal().unwrap();
    let edge_a = lane_edge(211).await;
    let edge_b = lane_edge(212).await;
    let feeding = Arc::new(AtomicBool::new(false));
    let feeder = feed(
        vec![Arc::clone(&edge_a), Arc::clone(&edge_b)],
        0,
        Arc::clone(&feeding),
    );
    let leave = Arc::new(AtomicBool::new(false));
    let listener = tokio::spawn(listen(
        vec![edge_a.client_port, edge_b.client_port],
        true,
        Arc::clone(&leave),
    ));
    within("the listener is at edge A", Duration::from_secs(5), || {
        edge_a.stats.live_subscribers() == Some(1)
    })
    .await;
    edge_a.router.mesh.subscribe(STREAM, 0, 0).await.unwrap();

    raise(libc::SIGTERM);
    let signal = tokio::time::timeout(Duration::from_secs(2), stop.recv())
        .await
        .expect("SIGTERM is a stop the node makes itself");
    assert_eq!(signal, "SIGTERM");
    let bound = Duration::from_millis(14_000);
    let signalled = Instant::now();
    let stopped = stop_edge(&edge_a.router, bound, pending()).await;
    assert!(stopped.drained && !stopped.skipped, "{stopped:?}");
    assert!(stopped.waited >= STOP_HOLD, "{stopped:?}");
    assert!(stopped.waited < bound, "{stopped:?}");
    assert_eq!(
        stopped.released, 1,
        "its subscription is released at its parents"
    );
    assert!(edge_a.router.mesh.subscribed_streams().is_empty());
    assert_eq!(
        edge_a.router.lifecycle.reasons().list(),
        vec![DrainReason::Stopping]
    );
    assert!(
        edge_a.router.mesh.is_draining(),
        "its hello says it is leaving"
    );
    assert_eq!(edge_a.router.node_status().await.state, NodeState::Stopping);
    // The listener moved to edge B, which serves it now.
    assert_eq!(edge_a.stats.live_subscribers(), Some(0));
    assert_eq!(edge_b.stats.live_subscribers(), Some(1));
    assert_eq!(edge_a.stats.moves_expired.load(Ordering::Relaxed), 0);

    leave.store(true, Ordering::Relaxed);
    let heard = listener.await.unwrap();
    assert_eq!(
        heard
            .moves
            .iter()
            .map(|(_, from, reason)| (*from, reason.as_str()))
            .collect::<Vec<_>>(),
        vec![(edge_a.client_port, "draining")],
        "{heard:?}"
    );
    assert_eq!(
        heard
            .subscribed
            .iter()
            .map(|(_, at)| *at)
            .collect::<Vec<_>>(),
        vec![edge_a.client_port, edge_b.client_port]
    );
    feeding.store(true, Ordering::Relaxed);
    feeder.await.unwrap();
    // The shutdown of today follows: the whole stop is inside the bound and
    // the unit's 2 s for the shutdown.
    tokio::time::timeout(Duration::from_secs(2), stop_lane_edge(edge_a))
        .await
        .expect("the edge shuts down at once after its stop");
    assert!(signalled.elapsed() < bound + Duration::from_secs(2));
    stop_lane_edge(edge_b).await;
}

/// A second signal during an edge's stop skips what is left of the wait: a
/// listener that never leaves would hold the edge for its grace (10 s), and
/// SIGINT after SIGTERM ends the wait at once. The subscriptions are still
/// released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_signal_skips_the_wait() {
    let _signals = SIGNALS.lock().await;
    let mut stop = stop_signal().unwrap();
    let edge = lane_edge(213).await;
    let feeding = Arc::new(AtomicBool::new(false));
    let feeder = feed(vec![Arc::clone(&edge)], 0, Arc::clone(&feeding));
    let leave = Arc::new(AtomicBool::new(false));
    let listener = tokio::spawn(listen(vec![edge.client_port], false, Arc::clone(&leave)));
    within(
        "the listener is at the edge",
        Duration::from_secs(5),
        || edge.stats.live_subscribers() == Some(1),
    )
    .await;
    edge.router.mesh.subscribe(STREAM, 0, 0).await.unwrap();

    raise(libc::SIGTERM);
    assert_eq!(stop.recv().await, "SIGTERM");
    let bound = Duration::from_millis(14_000);
    let second_at = Arc::new(StdMutex::new(None));
    let second = {
        let (stats, second_at) = (Arc::clone(&edge.stats), Arc::clone(&second_at));
        async move {
            // Once the listener has been told to move (and stays), a second
            // signal.
            while !stats.legs().is_some_and(|legs| legs.leaving == 1) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            *second_at.lock().unwrap() = Some(Instant::now());
            raise(libc::SIGINT);
            assert_eq!(stop.recv().await, "SIGINT");
        }
    };
    let stopped = stop_edge(&edge.router, bound, second).await;
    let second_at = second_at
        .lock()
        .unwrap()
        .expect("the second signal was sent");
    assert!(stopped.skipped && !stopped.drained, "{stopped:?}");
    // At once: well inside the listener's grace, which would otherwise
    // hold the edge (the margin is for a loaded machine).
    assert!(
        second_at.elapsed() < Duration::from_millis(1_500),
        "{stopped:?}"
    );
    assert!(stopped.waited < Duration::from_secs(4), "{stopped:?}");
    assert_eq!(stopped.released, 1);
    // The listener was told to leave and had not: the edge still served it.
    assert_eq!(edge.stats.live_subscribers(), Some(1));

    leave.store(true, Ordering::Relaxed);
    let heard = listener.await.unwrap();
    assert!(
        heard
            .moves
            .iter()
            .any(|(_, _, reason)| reason == "draining"),
        "{heard:?}"
    );
    feeding.store(true, Ordering::Relaxed);
    feeder.await.unwrap();
    stop_lane_edge(edge).await;
}

/// The drained proof waits for LL-HLS readers that were redirected and
/// still fetch the parts they were told about, and for a read in progress:
/// the stop returns a second after the last of them, not when the session
/// gauge reads 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_waits_for_moved_readers_that_still_fetch() {
    let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
    let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
    let router = app_router_for_tests(cache, mesh);
    let request = |draining| PlaybackRequest {
        session_id: Some("reader"),
        stream_id: Some(7),
        draining,
    };
    assert_eq!(
        router.edge_load.admit_playback(request(false)),
        PlaybackAdmission::Admitted
    );
    // The redirect at its reload: the session gauge no longer counts it.
    assert!(router.edge_load.egress.release_session("reader"));
    assert_eq!(router.edge_load.admission_snapshot().active_sessions, 0);
    let fetching = router.clone();
    let reader = tokio::spawn(async move {
        let started = Instant::now();
        let mut last_request = started;
        while started.elapsed() < Duration::from_millis(1_200) {
            last_request = Instant::now();
            assert_eq!(
                fetching.edge_load.admit_playback(request(true)),
                PlaybackAdmission::Existing
            );
            let read = fetching.edge_load.begin_read(false);
            tokio::time::sleep(Duration::from_millis(60)).await;
            drop(read);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // A last read, of a part it was told about, that takes 1.5 s.
        let read = fetching.edge_load.begin_read(false);
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        drop(read);
        (last_request, Instant::now())
    });
    let started = Instant::now();
    let stopped = stop_edge(&router, Duration::from_secs(8), pending()).await;
    let (last_request, last_read) = reader.await.unwrap();
    assert!(stopped.drained, "{stopped:?}");
    // The edge is empty once the moved session made no request for a
    // second (`hls_moved_active`) and no read is in progress, and it stops
    // a second after that, within a few polls.
    let empty = (last_request + Duration::from_secs(1)).max(last_read);
    let done = started + stopped.waited;
    assert!(
        done + Duration::from_millis(20) >= empty + STOP_HOLD,
        "{stopped:?}"
    );
    assert!(
        done <= empty + STOP_HOLD + Duration::from_secs(1),
        "{stopped:?}"
    );
    assert!(
        stopped.waited >= Duration::from_millis(3_500),
        "{stopped:?}"
    );
}

/// An edge that stops releases its playlist subscriptions: its parent drops
/// the lease child at once, not when the lease (5 s) lapses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_releases_its_leases_when_it_stops() {
    let lease = Duration::from_secs(5);
    let (parent_addr, edge_addr) = (unused_loopback_addr(), unused_loopback_addr());
    let parent_cache = canonical_gap_test_cache().await;
    commit_canonical_gap_test_part(&parent_cache, 7, 0).await;
    let parent = start_mesh(
        &parent_cache,
        mesh_config("dist-a", parent_addr, CacheMeshRole::Distributor, lease).with_peer(edge_addr),
    )
    .await;
    let edge_cache = canonical_gap_test_cache().await;
    let edge_mesh = start_mesh(
        &edge_cache,
        mesh_config("edge", edge_addr, CacheMeshRole::Edge, lease).with_peer(parent_addr),
    )
    .await;
    let router = app_router_for_tests(edge_cache, Arc::clone(&edge_mesh));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if edge_mesh.subscribe(7, 0, 0).await.unwrap().len() == 1
                && !parent.served_subscriptions().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the parent serves the edge a lease");

    let stopped = stop_edge(&router, Duration::from_secs(3), pending()).await;
    assert!(stopped.drained, "{stopped:?}");
    assert_eq!(stopped.released, 1);
    let released = Instant::now();
    within(
        "the parent drops the lease child",
        Duration::from_secs(1),
        || parent.served_subscriptions().is_empty(),
    )
    .await;
    assert!(released.elapsed() < Duration::from_millis(500));
    parent.shutdown();
    edge_mesh.shutdown();
}

/// A restarted edge holds `start`: a listener's subscription is answered
/// `MOVE draining` until the edge is warm (here: E4, no regional view for
/// the first 2 s), and taken once it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_edge_takes_no_listener_until_it_is_warm() {
    let edge = lane_edge(214).await;
    // As a process started with --start-drained.
    edge.router.lifecycle.drain(DrainReason::Start);
    assert!(edge.router.mesh.is_draining());
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let start = tokio::spawn(start_edge(
        edge.router.clone(),
        Duration::from_secs(4),
        shutdown_rx,
    ));
    let feeding = Arc::new(AtomicBool::new(false));
    let feeder = feed(vec![Arc::clone(&edge)], 0, Arc::clone(&feeding));
    let leave = Arc::new(AtomicBool::new(false));
    let listener = tokio::spawn(listen(vec![edge.client_port], true, Arc::clone(&leave)));
    let status = edge.router.node_status().await;
    assert_eq!(status.state, NodeState::Starting);
    assert!(
        status.not_ready.contains(&"no regional view".to_owned()),
        "{status:?}"
    );

    within("the edge is warm", Duration::from_secs(5), || {
        !edge.router.lifecycle.holds(DrainReason::Start)
    })
    .await;
    let warm_at = Instant::now();
    within("the listener is taken", Duration::from_secs(3), || {
        edge.stats.live_subscribers() == Some(1)
    })
    .await;
    // Parts reach the listener a moment after the edge takes it; on a
    // loaded machine leaving at once can be before the first.
    tokio::time::sleep(Duration::from_millis(300)).await;
    leave.store(true, Ordering::Relaxed);
    let heard = listener.await.unwrap();
    start.await.unwrap();
    tokio::time::sleep(NODE_STATUS_CACHE).await;
    let status = edge.router.node_status().await;
    assert!(status.warm && !status.ready_forced, "{status:?}");
    assert!(status.drain_reasons.is_empty(), "{status:?}");
    assert!(!edge.router.mesh.is_draining());
    // Asked while it started, it said MOVE draining and took nobody; its
    // first OK came after it was warm, two seconds after its start.
    assert!(
        heard
            .moves
            .iter()
            .any(|(_, _, reason)| reason == "draining"),
        "{heard:?}"
    );
    let (subscribed_at, _) = heard.subscribed[0];
    assert!(
        subscribed_at + Duration::from_millis(150) >= warm_at,
        "{heard:?}"
    );
    assert!(
        heard
            .moves
            .iter()
            .all(|(at, _, _)| *at <= warm_at + Duration::from_millis(150)),
        "{heard:?}"
    );
    assert!(heard.parts > 0, "{heard:?}");
    assert!(status.process.uptime_ms >= node_status::REGIONAL_VIEW_WAIT_MS);
    feeding.store(true, Ordering::Relaxed);
    feeder.await.unwrap();
    drop(shutdown_tx);
    stop_lane_edge(edge).await;
}

/// An edge that is not warm at --start-warm-ms drops `start` all the same,
/// serves cold and says so: `ready_forced`, `warm` false, its unmet warm-up
/// condition listed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_resumes_after_start_warm_ms_and_says_forced() {
    let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
    let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
    let lifecycle = NodeLifecycle::new(LifecycleConfig {
        edge: true,
        start_drained: true,
        state_file: None,
        warm_pass: false,
    });
    let router = router_with("edge", cache, mesh, CacheMeshRole::Edge, lifecycle, 0);
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let started = Instant::now();
    start_edge(router.clone(), Duration::from_millis(500), shutdown_rx).await;
    let took = started.elapsed();
    assert!(took >= Duration::from_millis(350), "{took:?}");
    assert!(took < Duration::from_millis(1_200), "{took:?}");
    assert!(router.lifecycle.ready_forced());
    assert!(!router.lifecycle.holds(DrainReason::Start));
    assert!(!router.lifecycle.is_draining().await);
    tokio::time::sleep(NODE_STATUS_CACHE).await;
    let status = router.node_status().await;
    assert!(status.ready_forced && !status.warm, "{status:?}");
    assert_ne!(status.state, NodeState::Starting);
    assert!(
        status.not_ready.contains(&"no regional view".to_owned()),
        "{status:?}"
    );
    assert!(status.drain_reasons.is_empty());
}

/// `drain-node` while a node starts: both reasons hold, the warm-up drops
/// only `start`, and the node stays drained (hello and advertisement).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_during_start_survives_the_warm_up() {
    let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
    let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
    let lifecycle = NodeLifecycle::new(LifecycleConfig {
        edge: true,
        start_drained: true,
        state_file: None,
        warm_pass: false,
    });
    let router = router_with(
        "edge",
        cache,
        Arc::clone(&mesh),
        CacheMeshRole::Edge,
        lifecycle,
        0,
    );
    router
        .execute_control(ControlKind::DrainNode, control(&router))
        .await;
    assert_eq!(
        router.node_status().await.drain_reasons,
        vec![DrainReason::Start, DrainReason::Operator]
    );
    router.telemetry.ingest_snapshot(neighbour_snapshot()).await;
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    tokio::time::timeout(
        Duration::from_secs(2),
        start_edge(router.clone(), Duration::from_secs(4), shutdown_rx),
    )
    .await
    .expect("warm with a regional view at once");
    assert!(!router.lifecycle.ready_forced());
    tokio::time::sleep(NODE_STATUS_CACHE).await;
    let status = router.node_status().await;
    assert!(status.warm, "{status:?}");
    assert_eq!(status.state, NodeState::Draining);
    assert_eq!(status.drain_reasons, vec![DrainReason::Operator]);
    assert!(router.lifecycle.is_draining().await);
    assert!(mesh.is_draining());
    assert!(router.local_mesh_snapshot().await.node.draining);
}

/// `resume-node` while a node starts removes the operator's reason only:
/// `start` holds (the hello says draining) until the warm-up drops it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_does_not_end_a_start() {
    for (role, edge) in [
        (CacheMeshRole::Edge, true),
        (CacheMeshRole::Distributor, false),
    ] {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let lifecycle = NodeLifecycle::new(LifecycleConfig {
            edge,
            start_drained: true,
            state_file: None,
            warm_pass: false,
        });
        let router = router_with("node", cache, Arc::clone(&mesh), role, lifecycle, 0);
        router
            .execute_control(ControlKind::DrainNode, control(&router))
            .await;
        router
            .execute_control(ControlKind::ResumeNode, control(&router))
            .await;
        assert_eq!(router.lifecycle.reasons().list(), vec![DrainReason::Start]);
        assert!(
            mesh.is_draining(),
            "{role:?}: the hello still says draining"
        );
        // An edge takes no reader while it starts; a starting forwarder
        // still answers as an entry point.
        assert_eq!(router.lifecycle.is_draining().await, edge, "{role:?}");
        assert_eq!(router.node_status().await.state, NodeState::Starting);

        router.telemetry.ingest_snapshot(neighbour_snapshot()).await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let start = if edge {
            tokio::spawn(start_edge(
                router.clone(),
                Duration::from_secs(4),
                shutdown_rx,
            ))
        } else {
            tokio::spawn(start_forwarder(router.clone(), shutdown_rx))
        };
        tokio::time::timeout(Duration::from_secs(3), start)
            .await
            .expect("the warm-up drops start")
            .unwrap();
        assert!(router.lifecycle.reasons().list().is_empty());
        assert!(!mesh.is_draining());
        assert!(!router.lifecycle.is_draining().await);
    }
}

/// An operator's drain is kept in the drain-state file: a node restarted
/// with the same file is drained from its first moment, and stays so after
/// its warm-up, until `resume-node`, which removes the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operator_drain_survives_a_restart() {
    let dir = std::env::temp_dir().join(format!(
        "av-mesh-drain-state-{}-{}",
        std::process::id(),
        now_unix_us()
    ));
    let file = dir.join("state").join("edge.drained");
    let config = || LifecycleConfig {
        edge: true,
        start_drained: true,
        state_file: Some(file.clone()),
        warm_pass: false,
    };
    let node = |lifecycle: NodeLifecycle| async move {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = router_with(
            "edge",
            cache,
            Arc::clone(&mesh),
            CacheMeshRole::Edge,
            lifecycle,
            0,
        );
        router.telemetry.ingest_snapshot(neighbour_snapshot()).await;
        (router, mesh)
    };
    let (first, first_mesh) = node(NodeLifecycle::new(config())).await;
    assert_eq!(first.lifecycle.reasons().list(), vec![DrainReason::Start]);
    first
        .execute_control(ControlKind::DrainNode, control(&first))
        .await;
    let kept = DrainState::read(&file).unwrap().expect("the drain is kept");
    assert_eq!(kept.reason, "operator");
    assert!(kept.since_unix_ms > 0);
    first_mesh.shutdown();

    // The restart: drained from its first moment, the hello too.
    let (second, second_mesh) = node(NodeLifecycle::new(config())).await;
    assert_eq!(
        second.lifecycle.reasons().list(),
        vec![DrainReason::Start, DrainReason::Operator]
    );
    assert!(second_mesh.is_draining());
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    start_edge(second.clone(), Duration::from_secs(4), shutdown_rx).await;
    tokio::time::sleep(NODE_STATUS_CACHE).await;
    let status = second.node_status().await;
    assert_eq!(status.state, NodeState::Draining);
    assert_eq!(status.drain_reasons, vec![DrainReason::Operator]);
    assert!(status.not_ready.contains(&"draining: operator".to_owned()));
    assert!(second_mesh.is_draining());
    second
        .execute_control(ControlKind::ResumeNode, control(&second))
        .await;
    assert!(
        DrainState::read(&file).unwrap().is_none(),
        "the resume removes it"
    );
    assert!(!second_mesh.is_draining());
    second_mesh.shutdown();

    let third = NodeLifecycle::new(LifecycleConfig {
        start_drained: false,
        ..config()
    });
    assert!(third.reasons().list().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

/// A restarted forwarder holds `start` until its window is full (F6): its
/// hellos say draining, so an edge ranks it after the other forwarder;
/// once warm it clears the bit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_starting_forwarder_is_ranked_last_until_its_window_is_full() {
    let lease = Duration::from_millis(600);
    let (a_addr, b_addr, edge_addr) = (
        unused_loopback_addr(),
        unused_loopback_addr(),
        unused_loopback_addr(),
    );
    let a_cache = canonical_gap_test_cache().await;
    let a = start_mesh(
        &a_cache,
        mesh_config("dist-a", a_addr, CacheMeshRole::Distributor, lease).with_peer(edge_addr),
    )
    .await;
    // B: restarted, with a window of 6 parts to fill.
    let b_cache = canonical_gap_test_cache().await;
    let b_lifecycle = NodeLifecycle::new(LifecycleConfig {
        edge: false,
        start_drained: true,
        state_file: None,
        warm_pass: false,
    });
    let b_mesh = start_mesh(
        &b_cache,
        mesh_config("dist-b", b_addr, CacheMeshRole::Distributor, lease)
            .with_peer(edge_addr)
            .with_hello_state(b_lifecycle.hello()),
    )
    .await;
    let b = router_with(
        "dist-b",
        Arc::clone(&b_cache),
        Arc::clone(&b_mesh),
        CacheMeshRole::Distributor,
        b_lifecycle,
        6,
    );
    assert!(b_mesh.is_draining());
    let edge_cache = canonical_gap_test_cache().await;
    let edge = start_mesh(
        &edge_cache,
        mesh_config("edge", edge_addr, CacheMeshRole::Edge, lease)
            .with_peers([a_addr, b_addr])
            .with_max_parents(2),
    )
    .await;
    for cache in [&a_cache, &b_cache] {
        commit_canonical_gap_test_part(cache, 7, 0).await;
    }
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let start = tokio::spawn(start_forwarder(b.clone(), shutdown_rx));
    within(
        "the edge hears B say it starts drained",
        Duration::from_secs(3),
        || edge.draining_peers() == vec![b_addr],
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while edge
            .peer_statuses()
            .await
            .iter()
            .filter(|peer| peer.parent && peer.role.is_some())
            .count()
            < 2
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the edge hears both forwarders");
    let mut window_full_at = None;
    for part in 1..12_u64 {
        commit_canonical_gap_test_part(&a_cache, 7, part).await;
        commit_canonical_gap_test_part(&b_cache, 7, part).await;
        let ranked = edge.subscribe(7, 0, 0).await.unwrap();
        let starting = b.lifecycle.holds(DrainReason::Start);
        if starting && edge.draining_peers().contains(&b_addr) {
            assert_eq!(ranked, vec![a_addr, b_addr], "part {part}: B ranks last");
        }
        if part + 1 >= 6 && window_full_at.is_none() {
            window_full_at = Some(Instant::now());
        }
        if part + 1 < 6 {
            assert!(starting, "part {part}: B is warm before its window is full");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::timeout(Duration::from_secs(3), start)
        .await
        .expect("B is warm once its window is full")
        .unwrap();
    within(
        "the edge hears B no longer drains",
        Duration::from_secs(1),
        || edge.draining_peers().is_empty(),
    )
    .await;
    let status = b.node_status().await;
    assert!(status.warm, "{status:?}");
    assert!(window_full_at.is_some());
    for mesh in [&a, &b_mesh, &edge] {
        mesh.shutdown();
    }
}

/// SIGTERM to a forwarder: its hellos say it is leaving at once, and it
/// keeps forwarding for --stop-linger-ms, so its child hears it leave
/// while it still serves and takes every part from its other parent after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarder_says_it_leaves_before_it_stops() {
    let lease = Duration::from_millis(600);
    let (x_addr, y_addr, edge_addr) = (
        unused_loopback_addr(),
        unused_loopback_addr(),
        unused_loopback_addr(),
    );
    let x_cache = canonical_gap_test_cache().await;
    let x_lifecycle = NodeLifecycle::new(LifecycleConfig {
        edge: false,
        ..LifecycleConfig::default()
    });
    let x_mesh = start_mesh(
        &x_cache,
        mesh_config("dist-x", x_addr, CacheMeshRole::Distributor, lease)
            .with_peer(edge_addr)
            .with_hello_state(x_lifecycle.hello()),
    )
    .await;
    let x = router_with(
        "dist-x",
        Arc::clone(&x_cache),
        Arc::clone(&x_mesh),
        CacheMeshRole::Distributor,
        x_lifecycle,
        0,
    );
    let y_cache = canonical_gap_test_cache().await;
    let y = start_mesh(
        &y_cache,
        mesh_config("dist-y", y_addr, CacheMeshRole::Distributor, lease).with_peer(edge_addr),
    )
    .await;
    let edge_cache = canonical_gap_test_cache().await;
    let edge = start_mesh(
        &edge_cache,
        mesh_config("edge", edge_addr, CacheMeshRole::Edge, lease)
            .with_peers([x_addr, y_addr])
            .with_max_parents(2),
    )
    .await;
    for cache in [&x_cache, &y_cache] {
        commit_canonical_gap_test_part(cache, 7, 0).await;
    }
    // The first subscribe that ranks both asks the window (part 0) of the
    // first.
    tokio::time::timeout(Duration::from_secs(3), async {
        while edge.subscribe(7, 0, 8).await.unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both forwarders are the edge's parents");
    // The contributor's parts reach both forwarders; the edge renews.
    let parts = 50_u64;
    let x_running = Arc::new(AtomicBool::new(true));
    let source = {
        let (x_cache, y_cache, edge, x_running) = (
            Arc::clone(&x_cache),
            Arc::clone(&y_cache),
            Arc::clone(&edge),
            Arc::clone(&x_running),
        );
        tokio::spawn(async move {
            for part in 1..parts {
                if x_running.load(Ordering::Relaxed) {
                    commit_canonical_gap_test_part(&x_cache, 7, part).await;
                }
                commit_canonical_gap_test_part(&y_cache, 7, part).await;
                if part % 10 == 0 {
                    edge.subscribe(7, 0, 0).await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let heard = {
        let edge = Arc::clone(&edge);
        tokio::spawn(async move {
            loop {
                if edge.draining_peers().contains(&x_addr) {
                    return Instant::now();
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    let signalled = Instant::now();
    let stopped = stop_forwarder(&x, Duration::from_millis(300), false, pending()).await;
    let stopped_at = Instant::now();
    assert!(stopped.drained && !stopped.skipped, "{stopped:?}");
    assert!(stopped.waited >= Duration::from_millis(300), "{stopped:?}");
    assert!(stopped.waited < Duration::from_millis(500), "{stopped:?}");
    assert_eq!(
        stopped.released, 0,
        "a compiled forwarder holds no lease of its own"
    );
    let heard_at = tokio::time::timeout(Duration::from_millis(10), heard)
        .await
        .expect("the edge heard X leave while it still served")
        .unwrap();
    assert!(
        heard_at < stopped_at,
        "heard {:?} after the signal",
        heard_at - signalled
    );
    assert_eq!(x.node_status().await.state, NodeState::Stopping);
    // X shuts down; Y carries the rest.
    x_running.store(false, Ordering::Relaxed);
    x_mesh.shutdown();
    source.await.unwrap();
    for part in 0..parts {
        tokio::time::timeout(Duration::from_secs(3), async {
            while edge_cache
                .chunk_cache
                .get_for_stream_id(7, part as usize)
                .await
                .is_none()
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the edge lacks part {part}"));
    }
    y.shutdown();
    edge.shutdown();
}

/// A node speaking as a later build (`--cache-mesh-test-future-frames`)
/// adds a field no struct knows to its telemetry, in JSON and in
/// MessagePack, and to its control envelopes: this build reads all of them.
#[test]
fn a_future_field_in_telemetry_and_control_is_ignored() {
    let snapshot = neighbour_snapshot();
    let json = json_with_future_field(&snapshot, true).unwrap();
    assert!(String::from_utf8_lossy(&json).contains("\"x_future\":1"));
    let read: MeshSnapshot = serde_json::from_slice(&json).unwrap();
    assert_eq!(read.node.node_id, snapshot.node.node_id);
    let binary = encode_mesh_telemetry_binary(&snapshot).unwrap();
    let future = msgpack_with_future_field(binary.clone());
    assert_eq!(future.len(), binary.len() + 10);
    let read = decode_mesh_telemetry_binary(&future).unwrap();
    assert_eq!(read.node.node_id, snapshot.node.node_id);
    // A map of 15 entries grows a 16-bit header.
    let mut fifteen = vec![0x8f];
    for index in 0..15_u8 {
        fifteen.extend_from_slice(&[0xa1, b'a' + index, index]);
    }
    let grown = msgpack_with_future_field(fifteen);
    assert_eq!(&grown[..3], &[0xde, 0x00, 0x10]);
    let envelope = ControlEnvelope {
        id: 1,
        origin_node_id: "uk-a".into(),
        kind: ControlKind::DrainNode,
        request: ControlRequest {
            node_id: Some("uk-b".into()),
            region: None,
            stream_id: None,
        },
        target_node_ids: vec!["uk-b".into()],
    };
    let json = json_with_future_field(&envelope, true).unwrap();
    let read: ControlEnvelope = serde_json::from_slice(&json).unwrap();
    assert_eq!(read.request.node_id.as_deref(), Some("uk-b"));
    assert_eq!(
        json_with_future_field(&envelope, false).unwrap(),
        serde_json::to_vec(&envelope).unwrap()
    );
}

/// The flags of C 2.2 and their defaults: an edge's stop bound comes from
/// B's drain spread and move grace.
#[test]
fn the_lifecycle_flags_and_the_stop_bound() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert!(args.start_drained);
    assert_eq!(args.start_warm_ms, 4_000);
    assert_eq!(args.stop_linger_ms, 300);
    assert_eq!(args.drain_state_file, None);
    assert!(!args.cache_mesh_test_future_frames);
    assert_eq!(args.stop_drain(), Duration::from_millis(14_000));
    let args = Args::try_parse_from([
        "av-mesh",
        "--start-drained",
        "false",
        "--stop-drain-ms",
        "9000",
        "--drain-state-file",
        "/var/lib/needletail/av-mesh.drained",
    ])
    .unwrap();
    assert!(!args.start_drained);
    assert_eq!(args.stop_drain(), Duration::from_millis(9_000));
    assert_eq!(
        args.drain_state_file.as_deref(),
        Some(std::path::Path::new("/var/lib/needletail/av-mesh.drained"))
    );
    let args = Args::try_parse_from([
        "av-mesh",
        "--client-udp-drain-spread-ms",
        "1000",
        "--client-udp-move-grace-ms",
        "5000",
    ])
    .unwrap();
    assert_eq!(args.stop_drain(), Duration::from_millis(8_000));
}

/// An edge of region `uk` that advertises `playlists` (stream, head,
/// video) at `updated_unix_ms`.
fn holder_snapshot(
    node_id: &str,
    updated_unix_ms: u64,
    playlists: &[(u64, u64, bool)],
) -> MeshSnapshot {
    let mut snapshot =
        telemetry_snapshot_for_tests(node_id, "uk", "europe", 51.5, -0.1, Vec::new(), 1);
    snapshot.updated_unix_ms = updated_unix_ms;
    let mut edge = EdgeServiceSnapshot::from_node(
        &snapshot.node,
        Some(format!("https://{node_id}.example/live")),
        "edge",
        EdgeLoadSnapshot::default(),
    );
    edge.playlists = playlists
        .iter()
        .map(|&(stream_id, head_part, video)| PlaylistHolding {
            stream_id,
            head_part,
            bitrate_bps: None,
            sessions: 1,
            request_rate: None,
            video,
        })
        .collect();
    snapshot.edge_service = Some(edge);
    snapshot
}

/// A distributor of region `uk` (no edge service) that advertises at
/// `updated_unix_ms`.
fn forwarder_snapshot(node_id: &str, updated_unix_ms: u64) -> MeshSnapshot {
    let mut snapshot =
        telemetry_snapshot_for_tests(node_id, "uk", "europe", 51.5, -0.1, Vec::new(), 1);
    snapshot.updated_unix_ms = updated_unix_ms;
    snapshot
}

fn demand_config(lease: Duration, warm_parts: usize) -> PlaylistDemand {
    PlaylistDemand::new(PlaylistDemandConfig {
        enabled: true,
        lease,
        idle: Duration::from_secs(60),
        warm_parts,
        max_playlists: MAX_EDGE_PLAYLISTS,
    })
}

/// A free UDP port at `ip`, and the next one (a class socket's).
fn free_addr(ip: Ipv4Addr) -> SocketAddr {
    loop {
        let port = std::net::UdpSocket::bind((ip, 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        if port < 64_000 && std::net::UdpSocket::bind((ip, port + 1_000)).is_ok() {
            return SocketAddr::from((ip, port));
        }
    }
}

/// C 2.1 (b): a parent restarted with a new boot id is greeted and asked
/// again at once by the edge's demand loop: its lease is back within
/// 500 ms, where the next renewal of a 30 s lease is 10 s away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_parent_has_its_leases_back_within_500_ms() {
    let lease = Duration::from_secs(30);
    let (parent_addr, edge_addr) = (unused_loopback_addr(), unused_loopback_addr());
    let parent_config = || {
        mesh_config("dist-a", parent_addr, CacheMeshRole::Distributor, lease).with_peer(edge_addr)
    };
    let parent_cache = canonical_gap_test_cache().await;
    commit_canonical_gap_test_part(&parent_cache, 7, 0).await;
    let parent = start_mesh(&parent_cache, parent_config()).await;
    let edge_cache = canonical_gap_test_cache().await;
    let edge = start_mesh(
        &edge_cache,
        mesh_config("edge", edge_addr, CacheMeshRole::Edge, lease).with_peer(parent_addr),
    )
    .await;
    let demand = demand_config(lease, 0);
    let router = app_router_for_tests(Arc::clone(&edge_cache), Arc::clone(&edge))
        .with_playlist_demand(demand.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let demand_task = tokio::spawn(run_playlist_demand(
        Arc::clone(&edge_cache),
        Arc::clone(&edge),
        router.edge_load.clone(),
        demand,
        shutdown_rx,
    ));
    let leased = |mesh: &CacheMeshHandle| {
        mesh.served_subscriptions()
            .iter()
            .any(|served| served.child == edge_addr && served.stream_id == 7)
    };
    // Each has heard the other: a parent drops a subscribe from a peer of a
    // region it does not know yet.
    tokio::time::timeout(Duration::from_secs(3), async {
        while edge.parents_for(7).await != vec![parent_addr]
            || !parent
                .peer_statuses()
                .await
                .iter()
                .any(|peer| peer.peer == edge_addr && peer.role.is_some())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the edge and its parent hear each other");
    assert!(router.open_playlist_demand(7).await);
    within(
        "the parent leases the playlist",
        Duration::from_secs(3),
        || leased(&parent),
    )
    .await;

    // The parent restarts on its address: a new process, a new boot id,
    // no lease.
    parent.shutdown();
    drop(parent);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let restarted_cache = canonical_gap_test_cache().await;
    commit_canonical_gap_test_part(&restarted_cache, 7, 0).await;
    let restarted = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match CacheMesh::new(Arc::clone(&restarted_cache.chunk_cache), parent_config())
                .start()
                .await
            {
                Ok(mesh) => break Arc::new(mesh),
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("the parent's address is free again");
    let restarted_at = Instant::now();
    within(
        "the restarted parent has the lease again",
        Duration::from_secs(3),
        || leased(&restarted),
    )
    .await;
    let back = restarted_at.elapsed();
    assert!(
        back <= Duration::from_millis(500),
        "the lease came back after {back:?}"
    );
    shutdown_tx.send(()).unwrap();
    demand_task.await.unwrap();
    restarted.shutdown();
    edge.shutdown();
}

/// The edge's fast plane at 127.0.0.30 and three parents at addresses of
/// their own (cache mesh and fast plane alike), as nodes of a VLAN.
const PARENT_IPS: [Ipv4Addr; 3] = [
    Ipv4Addr::new(127, 0, 0, 31),
    Ipv4Addr::new(127, 0, 0, 32),
    Ipv4Addr::new(127, 0, 0, 33),
];

/// A dispatch that admits each of `peers` as a parent of the edge.
fn dispatch_for(peers: &[SocketAddr]) -> RelayUdpDispatch {
    let mut dispatch = empty_relay_udp_dispatch();
    for (index, peer) in peers.iter().enumerate() {
        dispatch
            .bind_controlled_peer(
                *peer,
                ControlledRelayParentSession::new(
                    index as u64 + 1,
                    CarrierIdentity {
                        local: NodeId::new("edge-london").unwrap(),
                        peer: NodeId::new(format!("forwarder-{index}")).unwrap(),
                        kind: CarrierKind::PrivateUdp,
                        trust_mode: TrustMode::ControlledPrivateNetwork,
                    },
                    TopologyGeneration::new(FAST_GENERATION).unwrap(),
                    SubscriptionId::new(FAST_SUBSCRIPTION).unwrap(),
                    ParentPath::PromotedSecondary,
                )
                .unwrap(),
            )
            .unwrap();
    }
    dispatch
}

/// C 2.1 (c, d): a parent that drains leaves the edge's ranked set and is
/// renewed while its replacement is not seen delivering on the edge's fast
/// plane; it is released, confirmed, within a few looks of the
/// replacement's first sources, long before its timer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parent_is_kept_until_the_fast_plane_sees_its_replacement() {
    let lease = Duration::from_secs(5);
    let release_after = Duration::from_secs(4);
    let edge_ip = Ipv4Addr::new(127, 0, 0, 30);
    let parent_addrs = PARENT_IPS.map(free_addr);
    let edge_addr = free_addr(edge_ip);
    let mut parents = Vec::new();
    for (index, addr) in parent_addrs.iter().enumerate() {
        let cache = canonical_gap_test_cache().await;
        let mesh = start_mesh(
            &cache,
            mesh_config(
                &format!("dist-{index}"),
                *addr,
                CacheMeshRole::Distributor,
                lease,
            )
            .with_peer(edge_addr),
        )
        .await;
        parents.push((mesh, cache));
    }
    // The fast plane's senders, one at each parent's address.
    let mut senders = Vec::new();
    for ip in PARENT_IPS {
        senders.push(UdpSocket::bind((ip, 0)).await.unwrap());
    }
    let (ports, sockets) = super::fast_lease::bind_edge_class_ports(edge_ip);
    let edge_cache = canonical_gap_test_cache().await;
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let ingest = tokio::spawn(fast_plane::relay::run_relay_class_ingest(
        sockets,
        Arc::clone(&edge_cache),
        shutdown_rx.clone(),
        RelayIngestRuntime {
            dispatch: dispatch_for(
                &senders
                    .iter()
                    .map(|sender| sender.local_addr().unwrap())
                    .collect::<Vec<_>>(),
            ),
            secondary_socket: None,
            class_twins: Vec::new(),
            forwarder: None,
            audio_epochs: None,
            failover_controller: None,
            failover_heartbeat: Duration::from_millis(100),
            link_telemetry: None,
        },
        fast_plane::relay::RelayClassConfig {
            port_offset: 1_000,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 4_096,
            ..Default::default()
        },
    ));
    within(
        "the edge's class threads start",
        Duration::from_secs(3),
        || edge_cache.relay_ingress_snapshot().fast_plane.threads == 2,
    )
    .await;
    let edge = start_mesh(
        &edge_cache,
        mesh_config("edge", edge_addr, CacheMeshRole::Edge, lease)
            .with_peers(parent_addrs)
            .with_max_parents(2)
            .with_parent_release_after(release_after),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while edge
            .peer_statuses()
            .await
            .iter()
            .filter(|peer| peer.parent && peer.role.is_some())
            .count()
            < 3
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the edge hears its three parents");
    let ranked = edge.subscribe(7, 0, 0).await.unwrap();
    assert_eq!(ranked.len(), 2);
    let index_of = |addr: SocketAddr| {
        parent_addrs
            .iter()
            .position(|parent| *parent == addr)
            .unwrap()
    };
    let (leaving, staying) = (index_of(ranked[0]), index_of(ranked[1]));
    let replacement = 3 - leaving - staying;

    // The contributor's parts reach the edge from the parents that feed it.
    let feeding: Arc<[AtomicBool; 3]> = Arc::new(std::array::from_fn(|index| {
        AtomicBool::new(index == leaving || index == staying)
    }));
    let stop_feeding = Arc::new(AtomicBool::new(false));
    let feeder = {
        let (feeding, stop_feeding) = (Arc::clone(&feeding), Arc::clone(&stop_feeding));
        let to = SocketAddr::from((edge_ip, ports[0]));
        tokio::spawn(async move {
            let deadline_us = now_unix_us() + 60_000_000;
            let mut sequence = 1_000_u64;
            while !stop_feeding.load(Ordering::Relaxed) {
                for symbol in fast_audio_symbols(7, sequence, deadline_us) {
                    for (index, sender) in senders.iter().enumerate() {
                        if feeding[index].load(Ordering::Relaxed) {
                            sender.send_to(&symbol, to).await.unwrap();
                        }
                    }
                }
                sequence += 1;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    let release = tokio::spawn(run_parent_release(
        Arc::clone(&edge),
        Arc::clone(&edge_cache),
        shutdown_rx,
    ));
    let feeds = Arc::clone(edge_cache.parent_feeds().expect("the edge keeps the feeds"));
    within(
        "both ranked parents deliver",
        Duration::from_secs(3),
        || {
            [leaving, staying].iter().all(|index| {
                feeds.delivering(IpAddr::V4(PARENT_IPS[*index]), 7, now_unix_us(), 100_000)
            })
        },
    )
    .await;

    // The first parent drains: the edge ranks the third in its place, and
    // keeps the one that left while the third is not seen delivering.
    parents[leaving].0.set_draining(true);
    within("the edge hears the drain", Duration::from_secs(3), || {
        edge.draining_peers() == vec![parent_addrs[leaving]]
    })
    .await;
    let ranked = edge.subscribe(7, 0, 0).await.unwrap();
    assert_eq!(
        ranked
            .iter()
            .map(|addr| index_of(*addr))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([staying, replacement])
    );
    assert_eq!(edge.pending_parents(), vec![(7, parent_addrs[leaving])]);
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        edge.pending_parents(),
        vec![(7, parent_addrs[leaving])],
        "the parent that left is kept while its replacement delivers nothing"
    );
    assert_eq!(edge.fec_stats().parents_released, 0);

    // The replacement's sources arrive: the one that left is released,
    // confirmed, within a few looks.
    let seen = Instant::now();
    feeding[replacement].store(true, Ordering::Relaxed);
    within(
        "the parent that left is released",
        Duration::from_secs(2),
        || edge.pending_parents().is_empty(),
    )
    .await;
    let took = seen.elapsed();
    assert!(
        took < Duration::from_millis(500),
        "released {took:?} after the replacement's first sources"
    );
    let stats = edge.fec_stats();
    assert_eq!(
        (stats.parents_released, stats.parents_released_unconfirmed),
        (1, 0)
    );
    stop_feeding.store(true, Ordering::Relaxed);
    feeder.await.unwrap();
    shutdown_tx.send(()).unwrap();
    release.await.unwrap();
    ingest.await.unwrap().unwrap();
    edge.shutdown();
    for (mesh, _) in parents {
        mesh.shutdown();
    }
}

/// E5: a starting edge opens a copy of each audio playlist whose head moved
/// between a holder's last two advertisements, and of nothing else: not of
/// a held playlist whose head stands, never of video. It is not warm until
/// the copy has decoded up to the head advertised when it was opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_playlists_whose_heads_move_are_warmed_and_never_video() {
    let cache = canonical_gap_test_cache().await;
    let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
    let lifecycle = NodeLifecycle::new(LifecycleConfig {
        edge: true,
        start_drained: true,
        state_file: None,
        warm_pass: true,
    });
    let router = router_with(
        "uk-edge-1",
        Arc::clone(&cache),
        mesh,
        CacheMeshRole::Edge,
        lifecycle,
        0,
    )
    .with_playlist_demand(demand_config(Duration::from_secs(5), 0));
    let mut heads = RegionHeads::default();
    let at = now_unix_ms();
    // A holder's first advertisement: no head is known to move yet.
    router
        .telemetry
        .ingest_snapshot(holder_snapshot(
            "uk-edge-2",
            at,
            &[(7, 30, false), (8, 50, false), (9, 200, true)],
        ))
        .await;
    warm_playlists(&router, &mut heads).await;
    let pass = router.lifecycle.warm_pass();
    assert!(pass.copies.is_empty() && !pass.judged, "{pass:?}");
    let status = router.node_status().await;
    assert!(
        status
            .not_ready
            .contains(&"region heads not known".to_owned()),
        "{status:?}"
    );
    tokio::time::sleep(NODE_STATUS_CACHE).await;

    // Its next, 200 ms on (advertisements this close make the region's
    // interval short), once the region has been heard for one and a half
    // of them: 7 and 9 moved, 8 did not.
    tokio::time::sleep(Duration::from_millis(300)).await;
    router
        .telemetry
        .ingest_snapshot(holder_snapshot(
            "uk-edge-2",
            at + 200,
            &[(7, 40, false), (8, 50, false), (9, 230, true)],
        ))
        .await;
    warm_playlists(&router, &mut heads).await;
    let pass = router.lifecycle.warm_pass();
    assert!(pass.judged, "{pass:?}");
    assert_eq!(pass.copies, vec![(7, 40)]);
    let mut held: Vec<u64> = router
        .playlist_demand
        .streams
        .iter()
        .map(|entry| *entry.key())
        .collect();
    held.sort_unstable();
    assert_eq!(held, vec![7], "only the moving audio playlist is opened");
    let status = router.node_status().await;
    assert_eq!(status.state, NodeState::Starting);
    assert!(!status.warm);
    assert!(
        status
            .not_ready
            .contains(&"stream 7 not decoded".to_owned()),
        "{status:?}"
    );
    assert!(
        !status
            .not_ready
            .iter()
            .any(|reason| reason.contains(" 8 ") || reason.contains(" 9 ")),
        "{status:?}"
    );
    // A third pass opens nothing more.
    warm_playlists(&router, &mut heads).await;
    assert_eq!(router.lifecycle.warm_pass().copies, vec![(7, 40)]);

    // The copy decodes up to part 40: warm, and the start ends.
    for sequence in 0..=40 {
        commit_canonical_gap_test_part(&cache, 7, sequence).await;
    }
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    tokio::time::timeout(
        Duration::from_secs(2),
        start_edge(router.clone(), Duration::from_secs(4), shutdown_rx),
    )
    .await
    .expect("the copy is warm");
    assert!(!router.lifecycle.ready_forced());
    assert!(router.lifecycle.reasons().list().is_empty());
}

/// An edge that starts drained with no advertisement of its region (a new
/// process holds none, and peers publish once a second with no replay):
/// its first look at an empty view, a distributor heard first, and a
/// holder whose first advertisement comes 1.5 s into its uptime do not make
/// it warm. It is warm only once the holder's second advertisement showed
/// the head moving, the copy was opened, and the copy decoded to the head
/// advertised then; E4's two seconds do not stand in for the heads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_edge_waits_for_the_heads_of_its_region() {
    let cache = canonical_gap_test_cache().await;
    let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
    let lifecycle = NodeLifecycle::new(LifecycleConfig {
        edge: true,
        start_drained: true,
        state_file: None,
        warm_pass: true,
    });
    let router = router_with(
        "uk-edge-1",
        Arc::clone(&cache),
        mesh,
        CacheMeshRole::Edge,
        lifecycle,
        0,
    )
    .with_playlist_demand(demand_config(Duration::from_secs(5), 0));
    let uptime = || Duration::from_micros(now_unix_us().saturating_sub(cache.started_unix_us));
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    // A generous start timeout: this test is about the heads, not about
    // the forced start.
    let start = tokio::spawn(start_edge(
        router.clone(),
        Duration::from_secs(8),
        shutdown_rx,
    ));
    let at = now_unix_ms();
    tokio::time::sleep(Duration::from_millis(300).saturating_sub(uptime())).await;
    router
        .telemetry
        .ingest_snapshot(forwarder_snapshot("uk-dist-a", at))
        .await;
    tokio::time::sleep(Duration::from_millis(1_300).saturating_sub(uptime())).await;
    router
        .telemetry
        .ingest_snapshot(forwarder_snapshot("uk-dist-a", at + 1_000))
        .await;
    tokio::time::sleep(Duration::from_millis(1_500).saturating_sub(uptime())).await;
    router
        .telemetry
        .ingest_snapshot(holder_snapshot("uk-edge-2", at + 1_200, &[(7, 30, false)]))
        .await;
    tokio::time::sleep(Duration::from_millis(2_400).saturating_sub(uptime())).await;
    assert!(
        router.lifecycle.holds(DrainReason::Start),
        "{:?} {:?}",
        router.lifecycle.warm_pass(),
        router.node_status().await
    );
    assert!(router.lifecycle.warm_pass().copies.is_empty());
    assert!(router.playlist_demand.streams.is_empty());
    tokio::time::sleep(NODE_STATUS_CACHE).await;
    let status = router.node_status().await;
    assert_eq!(status.state, NodeState::Starting);
    assert!(!status.warm);
    assert!(
        status
            .not_ready
            .contains(&"region heads not known".to_owned()),
        "{status:?}"
    );

    // The holder's second advertisement: the head moved.
    router
        .telemetry
        .ingest_snapshot(holder_snapshot("uk-edge-2", at + 2_200, &[(7, 40, false)]))
        .await;
    within("the copy is opened", Duration::from_secs(2), || {
        router.playlist_demand.streams.contains_key(&7)
    })
    .await;
    assert_eq!(router.lifecycle.warm_pass().copies, vec![(7, 40)]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(router.lifecycle.holds(DrainReason::Start));
    tokio::time::sleep(NODE_STATUS_CACHE).await;
    let status = router.node_status().await;
    assert!(
        status
            .not_ready
            .contains(&"stream 7 not decoded".to_owned()),
        "{status:?}"
    );
    // Decoded, but short of the head advertised when it was opened.
    for sequence in 0..=20 {
        commit_canonical_gap_test_part(&cache, 7, sequence).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(router.lifecycle.holds(DrainReason::Start));
    for sequence in 21..=40 {
        commit_canonical_gap_test_part(&cache, 7, sequence).await;
    }
    tokio::time::timeout(Duration::from_secs(2), start)
        .await
        .expect("warm once the copy reached its head")
        .unwrap();
    assert!(!router.lifecycle.ready_forced());
    assert!(router.lifecycle.reasons().list().is_empty());
}

/// SIGTERM while a node starts: `stopping` wins (C 4). The start ends
/// without waiting for its warm-up, opens no copy and forces nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_during_the_start_ends_the_start() {
    let cache = canonical_gap_test_cache().await;
    let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
    let lifecycle = NodeLifecycle::new(LifecycleConfig {
        edge: true,
        start_drained: true,
        state_file: None,
        warm_pass: true,
    });
    let router = router_with(
        "uk-edge-1",
        Arc::clone(&cache),
        mesh,
        CacheMeshRole::Edge,
        lifecycle,
        0,
    )
    .with_playlist_demand(demand_config(Duration::from_secs(5), 0));
    // A region whose heads are known and move: a start would open a copy.
    let at = now_unix_ms();
    for (advert, head) in [(at, 30), (at + 1_000, 40)] {
        router
            .telemetry
            .ingest_snapshot(holder_snapshot("uk-edge-2", advert, &[(7, head, false)]))
            .await;
    }
    router.lifecycle.drain(DrainReason::Stopping);
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    // A start timeout already past: a start that ran would force itself.
    tokio::time::timeout(
        Duration::from_secs(1),
        start_edge(router.clone(), Duration::ZERO, shutdown_rx),
    )
    .await
    .expect("the start ends at once");
    assert!(!router.lifecycle.ready_forced());
    assert!(router.lifecycle.holds(DrainReason::Start));
    assert!(router.playlist_demand.streams.is_empty());
    assert!(router.lifecycle.warm_pass().copies.is_empty());
    let status = router.node_status().await;
    assert_eq!(status.state, NodeState::Stopping);
}

/// The hello's state byte follows the reasons: a state the evaluator found
/// before a reason changed is not stored over what the change set.
#[test]
fn a_state_found_before_a_change_does_not_overwrite_the_hello() {
    let lifecycle = NodeLifecycle::new(LifecycleConfig {
        edge: true,
        start_drained: true,
        ..LifecycleConfig::default()
    });
    let hello = lifecycle.hello();
    let found_with = lifecycle.reasons();
    lifecycle.resume(DrainReason::Start);
    assert_eq!(
        hello.load(Ordering::Relaxed),
        lifecycle::hello_state(NodeState::Ready)
    );
    lifecycle.drain(DrainReason::Stopping);
    // An evaluation that read the reasons before the stop stores late.
    lifecycle.set_hello_state(NodeState::Starting, found_with);
    assert_eq!(
        hello.load(Ordering::Relaxed),
        lifecycle::hello_state(NodeState::Stopping)
    );
    // One that read them as they are is stored.
    let operator = NodeLifecycle::default();
    operator.set_hello_state(NodeState::Degraded, operator.reasons());
    assert_eq!(
        operator.hello().load(Ordering::Relaxed),
        lifecycle::hello_state(NodeState::Degraded)
    );
}

/// F7: a slot that starts drained holds the playlists its region's edges
/// hold with a moving head, subscribes to them at its parent, and clears
/// its hello's bit only once its window is full. It starts with no
/// advertisement of its region, and the holder's second one comes after
/// the slot's leases have settled (F5) and E4's two seconds: the empty
/// cache of a slot that does not yet know what moves is not warm.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slot_fills_its_window_before_it_clears_its_bit() {
    let lease = Duration::from_millis(900);
    let warm_parts = 8;
    let (parent_addr, slot_addr) = (unused_loopback_addr(), unused_loopback_addr());
    let parent_cache = canonical_gap_test_cache().await;
    let parent = start_mesh(
        &parent_cache,
        mesh_config("dist-a", parent_addr, CacheMeshRole::Distributor, lease).with_peer(slot_addr),
    )
    .await;
    let slot_cache = canonical_gap_test_cache().await;
    let lifecycle = NodeLifecycle::new(LifecycleConfig {
        edge: false,
        start_drained: true,
        state_file: None,
        warm_pass: true,
    });
    let slot = start_mesh(
        &slot_cache,
        mesh_config("dist-c", slot_addr, CacheMeshRole::Distributor, lease)
            .with_peer(parent_addr)
            .with_hello_state(lifecycle.hello()),
    )
    .await;
    let router = router_with(
        "dist-c",
        Arc::clone(&slot_cache),
        Arc::clone(&slot),
        CacheMeshRole::Distributor,
        lifecycle,
        warm_parts as u64,
    )
    .with_playlist_demand(demand_config(lease, warm_parts));
    assert!(slot.is_draining());
    // A distributor feeds a slot that leases from it on the fast plane (its
    // lease children); here the parts its lease would carry are committed
    // at the slot, one every 100 ms from the lease on.
    let feeding = Arc::new(AtomicBool::new(true));
    let feeder = {
        let (parent, slot_cache, feeding) = (
            Arc::clone(&parent),
            Arc::clone(&slot_cache),
            Arc::clone(&feeding),
        );
        tokio::spawn(async move {
            let mut sequence = 0;
            while feeding.load(Ordering::Relaxed) {
                if parent
                    .served_subscriptions()
                    .iter()
                    .any(|served| served.child == slot_addr && served.stream_id == 7)
                {
                    commit_canonical_gap_test_part(&slot_cache, 7, sequence).await;
                    sequence += 1;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        while slot.parents_for(7).await != vec![parent_addr]
            || !parent
                .peer_statuses()
                .await
                .iter()
                .any(|peer| peer.peer == slot_addr && peer.role.is_some())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the slot and its parent hear each other");
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let upstream = tokio::spawn(run_upstream_demand(router.clone(), shutdown_rx.clone()));
    let start = tokio::spawn(start_forwarder(router.clone(), shutdown_rx));
    // An edge of the region holds playlist 7, its head moving: its first
    // advertisement.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let at = now_unix_ms();
    router
        .telemetry
        .ingest_snapshot(holder_snapshot("uk-edge-1", at, &[(7, 9, false)]))
        .await;
    let lease_settled = node_status::REGIONAL_VIEW_WAIT_MS
        .max(u64::try_from((lease / 3).as_millis()).unwrap() + PLAYLIST_DEMAND_SWEEP_MS)
        + 400;
    loop {
        tokio::time::sleep(NODE_STATUS_CACHE).await;
        if router.node_status().await.process.uptime_ms >= lease_settled {
            break;
        }
    }
    tokio::time::sleep(NODE_STATUS_CACHE).await;
    let status = router.node_status().await;
    assert!(slot.is_draining(), "{status:?}");
    assert_eq!(status.state, NodeState::Starting);
    assert!(
        status
            .not_ready
            .contains(&"region heads not known".to_owned()),
        "{status:?}"
    );
    assert!(router.lifecycle.warm_pass().slot_streams.is_empty());
    // Its next.
    router
        .telemetry
        .ingest_snapshot(holder_snapshot("uk-edge-1", at + 1_000, &[(7, 12, false)]))
        .await;

    // Until the bit clears, watch the window: it is full first.
    let window = |holdings: &[StreamHolding]| {
        holdings
            .iter()
            .find(|holding| holding.stream_id == 7)
            .map_or(0, |holding| holding.window_parts)
    };
    let mut full_at = None;
    let mut draining_with = Vec::new();
    let cleared_at = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let held = window(&slot_cache.stream_holdings().await);
            if full_at.is_none() && held >= warm_parts as u64 {
                full_at = Some(Instant::now());
            }
            if !slot.is_draining() {
                break Instant::now();
            }
            draining_with.push(held);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let Ok(cleared_at) = cleared_at else {
        panic!(
            "the slot did not clear its bit: {:?} {:?}",
            router.node_status().await,
            router.lifecycle.warm_pass(),
        );
    };
    let full_at = full_at.expect("the window was full before the bit cleared");
    assert!(full_at <= cleared_at);
    assert!(
        draining_with
            .iter()
            .any(|held| *held > 0 && *held < warm_parts as u64),
        "the bit held while the window filled: {draining_with:?}"
    );
    assert_eq!(router.lifecycle.warm_pass().slot_streams, vec![7]);
    start.await.unwrap();
    tokio::time::sleep(NODE_STATUS_CACHE).await;
    let status = router.node_status().await;
    assert!(status.warm, "{status:?}");
    assert!(status.drain_reasons.is_empty(), "{status:?}");
    feeding.store(false, Ordering::Relaxed);
    feeder.await.unwrap();
    shutdown_tx.send(()).unwrap();
    upstream.await.unwrap();
    parent.shutdown();
    slot.shutdown();
}

/// The cache-mesh part of `GET /api/node`: the live parents, the parents
/// waiting to be released and each peer as its hello described it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_node_status_shows_the_cache_mesh_peers() {
    let lease = Duration::from_secs(5);
    let (parent_addr, edge_addr) = (unused_loopback_addr(), unused_loopback_addr());
    let parent_cache = canonical_gap_test_cache().await;
    let parent_lifecycle = NodeLifecycle::new(LifecycleConfig {
        edge: false,
        start_drained: true,
        state_file: None,
        warm_pass: false,
    });
    let parent = start_mesh(
        &parent_cache,
        mesh_config("dist-a", parent_addr, CacheMeshRole::Distributor, lease)
            .with_peer(edge_addr)
            .with_build("3f2a9c1d77e0".to_owned())
            .with_hello_state(parent_lifecycle.hello()),
    )
    .await;
    let edge_cache = canonical_gap_test_cache().await;
    let edge = start_mesh(
        &edge_cache,
        mesh_config("edge", edge_addr, CacheMeshRole::Edge, lease).with_peer(parent_addr),
    )
    .await;
    let router = app_router_for_tests(edge_cache, Arc::clone(&edge));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let status = router.node_status().await;
            if status.cache_mesh.parents_live == 1
                && status
                    .cache_mesh
                    .peers
                    .first()
                    .is_some_and(|peer| peer.state.is_some())
            {
                break;
            }
            tokio::time::sleep(NODE_STATUS_CACHE).await;
        }
    })
    .await
    .expect("the edge hears its parent's hello");
    let status = router.node_status().await;
    let peer = &status.cache_mesh.peers[0];
    assert_eq!(peer.peer, parent_addr.to_string());
    assert_eq!(peer.role.as_deref(), Some("distributor"));
    assert_eq!(peer.build.as_deref(), Some("3f2a9c1d77e0"));
    assert_eq!(peer.state.as_deref(), Some("starting"));
    assert!(peer.boot_id.is_some_and(|boot| boot > 0));
    assert!(peer.heard_age_ms.is_some_and(|age| age <= 2_000));
    assert_eq!(status.cache_mesh.pending_release, 0);
    assert_eq!(status.cache_mesh.rx_unknown_kinds, 0);
    parent.shutdown();
    edge.shutdown();
}

/// What a holder advertises, read from advertisement to advertisement: a
/// head moves when a new advertisement has a higher one, and a repeat of
/// one advertisement tells nothing.
#[tokio::test]
async fn region_heads_move_between_two_advertisements() {
    let telemetry = TelemetryAggregator::default();
    let node = mesh_node_for_tests("uk-edge-1", "uk", "europe", 51.5, -0.1);
    let mut heads = RegionHeads::default();
    let at = now_unix_ms();
    assert!(
        !heads.observe(&telemetry, &node, 3_000, at).await,
        "an empty view is not an idle region"
    );
    telemetry
        .ingest_snapshot(holder_snapshot("uk-edge-2", at, &[(7, 10, false)]))
        .await;
    telemetry
        .ingest_snapshot(holder_snapshot(
            "uk-edge-3",
            at,
            &[(7, 12, false), (9, 5, true)],
        ))
        .await;
    assert!(!heads.observe(&telemetry, &node, 3_000, at).await);
    // The same advertisement again: still unknown.
    assert!(!heads.observe(&telemetry, &node, 3_000, at).await);
    assert!(heads.moving().is_empty());
    telemetry
        .ingest_snapshot(holder_snapshot("uk-edge-2", at + 1_000, &[(7, 10, false)]))
        .await;
    telemetry
        .ingest_snapshot(holder_snapshot(
            "uk-edge-3",
            at + 1_000,
            &[(7, 20, false), (9, 9, true)],
        ))
        .await;
    // Each was seen twice, but the region was first heard one interval
    // ago: a node whose first advertisement is half an interval late may
    // still come.
    assert!(!heads.observe(&telemetry, &node, 3_000, at + 1_000).await);
    assert!(heads.observe(&telemetry, &node, 3_000, at + 1_500).await);
    assert_eq!(
        heads.moving(),
        BTreeMap::from([(7, (20, false)), (9, (9, true))])
    );
    // A draining holder is not one.
    let mut draining = holder_snapshot("uk-edge-3", at + 2_000, &[(7, 30, false)]);
    draining.node.draining = true;
    telemetry.ingest_snapshot(draining).await;
    assert!(heads.observe(&telemetry, &node, 3_000, at + 2_000).await);
    assert!(heads.moving().is_empty(), "uk-edge-2's head stands");
    // A node of the region heard for the first time: unknown again until
    // its second advertisement (it may be a holder whose playlists come
    // next).
    telemetry
        .ingest_snapshot(forwarder_snapshot("uk-dist-a", at + 2_000))
        .await;
    assert!(!heads.observe(&telemetry, &node, 3_000, at + 2_000).await);
    telemetry
        .ingest_snapshot(forwarder_snapshot("uk-dist-a", at + 3_000))
        .await;
    assert!(heads.observe(&telemetry, &node, 3_000, at + 3_000).await);
}

/// A region heard only from nodes that hold nothing is idle once each of
/// them has been seen twice, and not before: its first advertisement may
/// have come before any holder's.
#[tokio::test]
async fn an_idle_region_is_known_after_two_advertisements_of_its_nodes() {
    let telemetry = TelemetryAggregator::default();
    let node = mesh_node_for_tests("uk-edge-1", "uk", "europe", 51.5, -0.1);
    let mut heads = RegionHeads::default();
    let at = now_unix_ms();
    telemetry
        .ingest_snapshot(forwarder_snapshot("uk-dist-a", at))
        .await;
    assert!(!heads.observe(&telemetry, &node, 3_000, at).await);
    telemetry
        .ingest_snapshot(forwarder_snapshot("uk-dist-a", at + 1_000))
        .await;
    assert!(!heads.observe(&telemetry, &node, 3_000, at + 1_000).await);
    assert!(heads.observe(&telemetry, &node, 3_000, at + 1_500).await);
    assert!(heads.moving().is_empty());
    // Another region's nodes are not this region's.
    let mut heads = RegionHeads::default();
    let mut far = forwarder_snapshot("fr-dist-a", at);
    far.node.region = "fr".to_owned();
    let telemetry = TelemetryAggregator::default();
    telemetry.ingest_snapshot(far).await;
    assert!(!heads.observe(&telemetry, &node, 3_000, at).await);
}
