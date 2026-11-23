//! Time to first audio (low-latency wave 7, item 8): a UDP subscriber of an
//! edge whose copy of the playlist is not playable yet (no whole segment:
//! the copy was just opened) is admitted as soon as the edge holds a part,
//! and gets that part with its `OK`.

use super::compact_lane::Payload;
use super::*;
use av_mesh::client_compact as compact;
use av_mesh::client_udp::{CompactPlan, Control, RepairPolicy, UNMEASURED_LOSS};
use tokio::time::timeout;

const STREAM: u64 = 3001;

/// An edge with the playlist demand of a deploy and parts 5..=9 of 1 ms
/// Opus (not a whole segment: the copy is not playable), and its client UDP
/// port.
async fn edge(
    admit_on_first_part: bool,
    first_part_at_once: bool,
) -> (SocketAddr, watch::Sender<()>) {
    let (port, shutdown, _) = edge_with(
        admit_on_first_part,
        first_part_at_once,
        Duration::from_millis(edge_udp::DEFAULT_FRESH_HEAD_MS),
    )
    .await;
    (port, shutdown)
}

/// [`edge`] with `--client-udp-fresh-head-ms`, and its cache.
async fn edge_with(
    admit_on_first_part: bool,
    first_part_at_once: bool,
    fresh_head: Duration,
) -> (SocketAddr, watch::Sender<()>, Arc<LiveTsCache>) {
    let cache =
        LiveTsCache::with_ring(1, Duration::from_micros(1_000), 400, 12_000, 64, 8, 0).await;
    let mesh = Arc::new(
        CacheMesh::new(
            Arc::clone(&cache.chunk_cache),
            CacheMeshConfig::new("edge-first", "uk", unused_loopback_addr())
                .with_role(playlists::mesh::CacheMeshRole::Edge),
        )
        .start()
        .await
        .unwrap(),
    );
    for sequence in 5..10 {
        let (envelope, _) = Payload::Opus1.envelope(STREAM, sequence);
        cache.commit_stream_payload(STREAM, envelope).await.unwrap();
    }
    assert!(!cache.state.read().await.playlist_is_playable(STREAM));
    let edge_cache = Arc::clone(&cache);
    let router = app_router_for_tests(Arc::clone(&cache), mesh).with_playlist_demand(
        PlaylistDemand::new(PlaylistDemandConfig {
            enabled: true,
            lease: Duration::from_secs(5),
            idle: Duration::from_secs(30),
            warm_parts: 64,
            max_playlists: MAX_EDGE_PLAYLISTS,
        }),
    );
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let sockets = edge_udp::EdgeSockets::single(socket);
    let port = sockets.for_stream(0).local_addr().unwrap();
    let mut config = edge_udp::EdgeUdpConfig::with_defaults(1_200, 64);
    config.admit_on_first_part = admit_on_first_part;
    config.first_part_at_once = first_part_at_once;
    config.fresh_head = fresh_head;
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    tokio::spawn(edge_udp::run_edge_udp(
        sockets,
        router,
        config,
        Arc::default(),
        Vec::new(),
        shutdown_rx,
    ));
    (port, shutdown_tx, edge_cache)
}

/// Subscribe to the compact lane; when it is answered and when its first
/// part came, from the first request, and which part that was.
async fn subscribe(
    edge: SocketAddr,
    wait: Duration,
) -> (Option<Duration>, Option<(Duration, u64)>) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let request = |token| {
        Control::Subscribe {
            stream_id: STREAM,
            from_part: None,
            repair: RepairPolicy::for_loss(
                UNMEASURED_LOSS,
                RepairPolicy::base_for_lane(true, None, None),
            ),
            token,
            fast_lane: true,
            compact: Some(CompactPlan::CLEAN),
            others: Vec::new(),
            resume: false,
        }
        .encode()
    };
    let started = Instant::now();
    socket.send_to(&request(None), edge).await.unwrap();
    let mut buf = vec![0_u8; 2_048];
    let (mut subscribed, mut first) = (None, None);
    let mut named = None;
    while let Ok(Ok((len, _))) = timeout(
        wait.saturating_sub(started.elapsed()),
        socket.recv_from(&mut buf),
    )
    .await
    {
        if let Some(view) = compact::parse(&buf[..len]) {
            if view.kind == compact::Kind::Part && first.is_none() {
                first = Some((started.elapsed(), u64::from(view.part_low)));
                break;
            }
            continue;
        }
        match Control::decode(&buf[..len]) {
            Some(Control::Token { token, .. }) => {
                socket.send_to(&request(Some(token)), edge).await.unwrap();
            }
            Some(Control::Subscribed { next_part, .. }) => {
                subscribed.get_or_insert(started.elapsed());
                named.get_or_insert(next_part);
            }
            _ => {}
        }
    }
    if let (Some((_, part)), Some(named)) = (first, named) {
        assert_eq!(part, named, "the first part is the one the OK names");
    }
    (subscribed, first)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscriber_is_admitted_on_the_first_part_and_gets_it_with_its_ok() {
    let (port, _shutdown) = edge(true, true).await;
    let (subscribed, first) = subscribe(port, Duration::from_secs(2)).await;
    let subscribed = subscribed.expect("admitted");
    assert!(
        subscribed < Duration::from_millis(200),
        "admitted in {subscribed:?}, not after a playable copy"
    );
    // No part is published after the subscription: the part it starts at
    // came with the OK, not after the next part.
    let (at, part) = first.expect("its first part");
    assert_eq!(part, 9, "the newest part");
    assert!(
        at < subscribed + Duration::from_millis(50),
        "{at:?} after OK {subscribed:?}"
    );
}

/// As before: the subscription waits for a playable copy (up to 4 s for a
/// copy that it opened).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_the_flag_a_subscriber_waits_for_a_playable_copy() {
    let (port, _shutdown) = edge(false, false).await;
    let (subscribed, first) = subscribe(port, Duration::from_millis(600)).await;
    assert_eq!(subscribed, None, "not admitted before the copy is playable");
    assert_eq!(first, None);
}

/// Wave 7 review: a head that arrived long ago (the last part before a
/// hold, or before an earlier demand lapsed) is not a start. The
/// subscription waits for a newer part and starts there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_head_is_not_a_start() {
    let (port, _shutdown, cache) = edge_with(true, true, Duration::from_millis(50)).await;
    // Parts 5..=9 are now older than 50 ms (and four 1 ms parts).
    tokio::time::sleep(Duration::from_millis(120)).await;
    let subscriber = tokio::spawn(subscribe(port, Duration::from_secs(2)));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!subscriber.is_finished());
    let (envelope, _) = Payload::Opus1.envelope(STREAM, 10);
    cache.commit_stream_payload(STREAM, envelope).await.unwrap();
    let (subscribed, first) = subscriber.await.unwrap();
    let subscribed = subscribed.expect("admitted once a fresh part came");
    assert!(subscribed >= Duration::from_millis(300), "{subscribed:?}");
    assert_eq!(first.expect("its first part").1, 10, "the fresh part");
}

#[test]
fn the_first_audio_flags_default_on_and_can_be_turned_off() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert!(args.client_udp_admit_on_first_part);
    assert!(args.client_udp_first_part_at_once);
    assert_eq!(args.client_udp_fresh_head_ms, 1_000);
    let args = Args::try_parse_from([
        "av-mesh",
        "--client-udp-admit-on-first-part",
        "false",
        "--client-udp-first-part-at-once",
        "false",
    ])
    .unwrap();
    assert!(!args.client_udp_admit_on_first_part);
    assert!(!args.client_udp_first_part_at_once);
}
