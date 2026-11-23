//! Edges fed by the fast plane (low-latency wave 4, B5 and B6): the
//! forwarder's lease children and the edge's RLS1 ingest and tee.

use super::*;
use fast_plane::lease::{LeaseChildConfig, LeaseChildren};
use fast_plane::relay::RelayClassConfig;
use playlists::mesh::{
    CacheMesh, CacheMeshConfig, CacheMeshRole as MeshRole, ReplicationMode, ServedSubscription,
};
use std::net::IpAddr;
use tokio::time::timeout;

const OFFSET: u16 = 1_000;

pub(super) fn loopback(last: u8) -> Ipv4Addr {
    Ipv4Addr::new(127, 0, 0, last)
}

/// Sockets of lease children at `ips`, each at the same two RLS1 ports
/// (where its primary and its secondary forwarder send) and their video
/// twins: per child `[primary audio, primary video, secondary audio,
/// secondary video]`.
async fn bind_lease_children(ips: &[Ipv4Addr]) -> ([u16; 2], Vec<[UdpSocket; 4]>) {
    'attempt: for _ in 0..128 {
        let probe = |ip: Ipv4Addr| {
            std::net::UdpSocket::bind((ip, 0))
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        };
        let ports = [probe(ips[0]), probe(ips[0])];
        if ports[0] == ports[1] || ports.iter().any(|port| port.checked_add(OFFSET).is_none()) {
            continue;
        }
        let mut children = Vec::new();
        for ip in ips {
            let mut sockets = Vec::new();
            for port in [ports[0], ports[0] + OFFSET, ports[1], ports[1] + OFFSET] {
                let Ok(socket) = UdpSocket::bind((*ip, port)).await else {
                    continue 'attempt;
                };
                let _ = SockRef::from(&socket).set_recv_buffer_size(4 << 20);
                sockets.push(socket);
            }
            children.push(sockets.try_into().unwrap());
        }
        return (ports, children);
    }
    panic!("no free child ports");
}

fn lease_children(ports: [u16; 2], primary: &[Ipv4Addr]) -> Arc<LeaseChildren> {
    lease_children_at(Ipv4Addr::LOCALHOST, ports, primary)
}

/// Lease children whose symbols leave from `bind`: a forwarder at an
/// address of its own.
fn lease_children_at(bind: Ipv4Addr, ports: [u16; 2], primary: &[Ipv4Addr]) -> Arc<LeaseChildren> {
    LeaseChildren::bind(
        LeaseChildConfig {
            bind: SocketAddr::from((bind, 0)),
            child_ports: ports,
            class_port_offset: OFFSET,
            primary_children: primary.iter().map(|ip| IpAddr::V4(*ip)).collect(),
            max_children: 16,
            ranked: true,
        },
        4 << 20,
    )
    .unwrap()
}

fn lease_config(lease: &Arc<LeaseChildren>) -> RelayClassConfig {
    RelayClassConfig {
        port_offset: OFFSET,
        cpus: [None, None],
        realtime: false,
        tee_capacity: 4_096,
        lease: Some(Arc::clone(lease)),
        ..Default::default()
    }
}

fn served(child: Ipv4Addr, stream_id: u64) -> ServedSubscription {
    ServedSubscription {
        child: SocketAddr::from((child, 9_101)),
        stream_id,
        backfill_parts_pending: 0,
        rank: None,
    }
}

fn sorted(mut datagrams: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    datagrams.sort();
    datagrams
}

/// Receive `count` datagrams, all from `from`.
async fn receive_from(socket: &UdpSocket, count: usize, from: SocketAddr) -> Vec<Vec<u8>> {
    let received = receive_all(socket, count).await;
    assert!(
        received.iter().all(|(_, source)| *source == from),
        "a lease child's symbols leave from the lease socket"
    );
    sorted(received.into_iter().map(|(bytes, _)| bytes).collect())
}

pub(super) async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    timeout(Duration::from_secs(3), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

/// B5: an edge subscribes to a playlist over the cache mesh, and the
/// forwarder sends it that stream's RLS1 symbols (and no other stream's)
/// within a sync interval; an Unsubscribe and an expiry stop them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_served_lease_makes_its_child_a_fast_plane_child_of_its_stream() {
    let edge_ip = loopback(2);
    let (ports, mut children) = bind_lease_children(&[edge_ip]).await;
    let [child_audio, _child_video, child_secondary, _] = children.pop().unwrap();
    let lease = lease_children(ports, &[edge_ip]);
    let relay = start_fast_relay_with(ParentPath::Secondary, &[], lease_config(&lease)).await;

    let distributor_addr = unused_loopback_addr();
    let edge_addr = SocketAddr::from((edge_ip, unused_loopback_addr().port()));
    let sync = Duration::from_millis(10);
    let mut distributor_config = CacheMeshConfig::new("forwarder", "uk", distributor_addr)
        .with_peer(edge_addr)
        .with_same_region_only(true)
        .with_role(MeshRole::Distributor)
        .with_replication(ReplicationMode::Demand)
        .with_subscription_lease(Duration::from_millis(600));
    distributor_config.sync_interval = sync;
    distributor_config.announce_interval = Duration::from_millis(50);
    let distributor_mesh = Arc::new(
        CacheMesh::new(Arc::clone(&relay.cache.chunk_cache), distributor_config)
            .start()
            .await
            .unwrap(),
    );
    let edge_cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
    let mut edge_config = CacheMeshConfig::new("edge", "uk", edge_addr)
        .with_peer(distributor_addr)
        .with_same_region_only(true)
        .with_role(MeshRole::Edge)
        .with_replication(ReplicationMode::Demand)
        .with_subscription_lease(Duration::from_millis(600));
    edge_config.sync_interval = sync;
    edge_config.announce_interval = Duration::from_millis(50);
    let edge_mesh = CacheMesh::new(Arc::clone(&edge_cache.chunk_cache), edge_config)
        .start()
        .await
        .unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    tokio::spawn(fast_control::run_lease_children(
        Arc::clone(&lease),
        Arc::clone(&distributor_mesh),
        shutdown_rx,
    ));
    timeout(Duration::from_secs(3), async {
        while edge_mesh.parents_for(4001).await.is_empty()
            || distributor_mesh.peers().await.is_empty()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the edge knows its parent");

    let subscribed = Instant::now();
    edge_mesh.subscribe(4001, 0, 0).await.unwrap();
    wait_until("the lease makes a child", || {
        lease.stats.snapshot().pairs == 1
    })
    .await;
    let joined = subscribed.elapsed();
    assert!(
        joined < sync * 10,
        "the child joins within a sync interval or so, not {joined:?}"
    );

    let deadline = now_unix_us() + 5_000_000;
    let mut expected = Vec::new();
    for sequence in 0..6_u64 {
        for stream_id in [4001_u64, 4002] {
            for symbol in fast_audio_symbols(stream_id, sequence, deadline) {
                relay
                    .primary_sender
                    .send_to(&symbol, relay.primary_audio)
                    .await
                    .unwrap();
                if stream_id == 4001 {
                    expected.push(symbol);
                }
            }
        }
    }
    let from = SocketAddr::from((Ipv4Addr::LOCALHOST, lease.local_addr().port()));
    assert_eq!(
        receive_from(&child_audio, expected.len(), from).await,
        sorted(expected.clone()),
        "the child takes its stream byte for byte, once"
    );
    assert_nothing_more(&child_audio).await;
    assert_nothing_more(&child_secondary).await;
    // The tee still commits both streams.
    wait_for_part(&relay.cache, 4002, 5).await;
    assert_eq!(
        relay.stats().lease_children.audio_sent,
        expected.len() as u64
    );

    // An Unsubscribe ends the child.
    edge_mesh.unsubscribe(4001).await.unwrap();
    wait_until("the release ends the child", || {
        lease.stats.snapshot().pairs == 0
    })
    .await;
    for symbol in fast_audio_symbols(4001, 6, deadline) {
        relay
            .primary_sender
            .send_to(&symbol, relay.primary_audio)
            .await
            .unwrap();
    }
    wait_for_part(&relay.cache, 4001, 6).await;
    assert_nothing_more(&child_audio).await;

    // A lease that is not renewed lapses, and ends the child too.
    edge_mesh.subscribe(4001, 0, 0).await.unwrap();
    wait_until("the second lease", || lease.stats.snapshot().pairs == 1).await;
    timeout(Duration::from_secs(3), async {
        while lease.stats.snapshot().pairs != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the lease lapses");
    for symbol in fast_audio_symbols(4001, 7, deadline) {
        relay
            .primary_sender
            .send_to(&symbol, relay.primary_audio)
            .await
            .unwrap();
    }
    wait_for_part(&relay.cache, 4001, 7).await;
    assert_nothing_more(&child_audio).await;

    let _ = shutdown_tx.send(());
    distributor_mesh.shutdown();
    edge_mesh.shutdown();
    relay.stop().await;
}

/// Wave 9 (cold start): a lease child that is new to a stream whose audio
/// parts come compact (RLC1) gets the stream's last whole part (RLS1, the
/// descriptor) before its first compact part, so that it can read the
/// compact parts at once; with --relay-lease-descriptors false it gets the
/// compact part alone and drops it until the next whole part.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_lease_child_gets_the_descriptor_before_its_first_compact_part() {
    use crate::tests::compact_lane::Payload;
    for (descriptors, ip) in [(true, 41_u8), (false, 42)] {
        let edge_ip = loopback(ip);
        let (ports, mut children) = bind_lease_children(&[edge_ip]).await;
        let [child_audio, _child_video, _child_secondary, _] = children.pop().unwrap();
        let lease = lease_children(ports, &[edge_ip]);
        let mut config = lease_config(&lease);
        config.lease_descriptors = descriptors;
        let relay = start_fast_relay_with(ParentPath::Secondary, &[], config).await;

        let distributor_addr = unused_loopback_addr();
        let edge_addr = SocketAddr::from((edge_ip, unused_loopback_addr().port()));
        let sync = Duration::from_millis(10);
        let mut distributor_config = CacheMeshConfig::new("forwarder", "uk", distributor_addr)
            .with_peer(edge_addr)
            .with_same_region_only(true)
            .with_role(MeshRole::Distributor)
            .with_replication(ReplicationMode::Demand)
            .with_subscription_lease(Duration::from_millis(2_000));
        distributor_config.sync_interval = sync;
        distributor_config.announce_interval = Duration::from_millis(50);
        let distributor_mesh = Arc::new(
            CacheMesh::new(Arc::clone(&relay.cache.chunk_cache), distributor_config)
                .start()
                .await
                .unwrap(),
        );
        let edge_cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mut edge_config = CacheMeshConfig::new("edge", "uk", edge_addr)
            .with_peer(distributor_addr)
            .with_same_region_only(true)
            .with_role(MeshRole::Edge)
            .with_replication(ReplicationMode::Demand)
            .with_subscription_lease(Duration::from_millis(2_000));
        edge_config.sync_interval = sync;
        edge_config.announce_interval = Duration::from_millis(50);
        let edge_mesh = CacheMesh::new(Arc::clone(&edge_cache.chunk_cache), edge_config)
            .start()
            .await
            .unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(fast_control::run_lease_children(
            Arc::clone(&lease),
            Arc::clone(&distributor_mesh),
            shutdown_rx,
        ));
        timeout(Duration::from_secs(3), async {
            while edge_mesh.parents_for(4001).await.is_empty()
                || distributor_mesh.peers().await.is_empty()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the edge knows its parent");

        // The stream's first part goes whole (it carries the descriptor),
        // before any child.
        let payload = Payload::Opus1;
        let deadline = now_unix_us() + 30_000_000;
        let mut sender = playlists::compact_object::CompactSender::new(3_600_000_000);
        let (envelope, _) = payload.envelope(4001, 0);
        assert!(crate::tests::fast_test_compact(&envelope, &mut sender, 0, deadline, 0).is_none());
        let (sources, _) = payload.symbols(&envelope);
        assert_eq!(sources.len(), 1, "a one-symbol audio part");
        let whole = sources[0].clone();
        relay
            .primary_sender
            .send_to(&whole, relay.primary_audio)
            .await
            .unwrap();
        wait_for_part(&relay.cache, 4001, 0).await;

        // Then the child comes, and the next part is compact.
        edge_mesh.subscribe(4001, 0, 0).await.unwrap();
        wait_until("the lease makes a child", || {
            lease.stats.snapshot().pairs == 1
        })
        .await;
        let (envelope, _) = payload.envelope(4001, 1);
        let (compact, _) = crate::tests::fast_test_compact(&envelope, &mut sender, 1_000, deadline, 0)
            .expect("the second part goes compact");
        relay
            .primary_sender
            .send_to(&compact[0], relay.primary_audio)
            .await
            .unwrap();
        let from = SocketAddr::from((Ipv4Addr::LOCALHOST, lease.local_addr().port()));
        let expected = if descriptors {
            vec![whole.clone(), compact[0].clone()]
        } else {
            vec![compact[0].clone()]
        };
        let received: Vec<Vec<u8>> = receive_all(&child_audio, expected.len())
            .await
            .into_iter()
            .map(|(bytes, source)| {
                assert_eq!(source, from, "from the lease socket");
                bytes
            })
            .collect();
        assert_eq!(received, expected, "descriptors {descriptors}: in this order");
        assert_nothing_more(&child_audio).await;
        assert_eq!(
            relay.stats().lease_descriptors,
            u64::from(descriptors),
            "one descriptor for the one new child"
        );
        // A third part, compact too: the child has the descriptor now.
        let (envelope, _) = payload.envelope(4001, 2);
        let (compact, _) = crate::tests::fast_test_compact(&envelope, &mut sender, 2_000, deadline, 0)
            .expect("the third part goes compact");
        relay
            .primary_sender
            .send_to(&compact[0], relay.primary_audio)
            .await
            .unwrap();
        assert_eq!(receive_all(&child_audio, 1).await[0].0, compact[0]);
        assert_eq!(relay.stats().lease_descriptors, u64::from(descriptors));

        let _ = shutdown_tx.send(());
        distributor_mesh.shutdown();
        edge_mesh.shutdown();
        relay.stop().await;
    }
}

/// B5, the lane rule: a video stream goes in full to the child this
/// forwarder is primary for and as repair to the other, which keeps the
/// sources warm; the other's failover controller promotes the forwarder
/// with RLF1, the warm sources are replayed and new sources follow. Audio
/// goes in full to both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_video_goes_whole_to_primary_children_and_as_repair_to_the_others() {
    let primary_ip = loopback(2);
    let other_ip = loopback(3);
    let (ports, children) = bind_lease_children(&[primary_ip, other_ip]).await;
    let [primary_audio, primary_video, primary_secondary_audio, primary_secondary_video] =
        &children[0];
    let [other_primary_audio, other_primary_video, other_audio, other_video] = &children[1];
    let lease = lease_children(ports, &[primary_ip]);
    lease.rebuild(&[
        served(primary_ip, 1),
        served(other_ip, 1),
        served(primary_ip, 4001),
        served(other_ip, 4001),
    ]);
    let relay =
        start_fast_relay_with(ParentPath::PromotedSecondary, &[], lease_config(&lease)).await;
    let from = SocketAddr::from((Ipv4Addr::LOCALHOST, lease.local_addr().port()));
    let deadline = now_unix_us() + 5_000_000;
    let video = |sequence: u64| {
        let envelope = encode_test_canonical_fmp4_bundle_with_duration(
            1,
            sequence,
            None,
            &deterministic_video_payload(6_000 + sequence as usize),
            None,
            sequence == 0,
        );
        fast_test_symbols(
            &envelope,
            relay_session::MediaPriority::VideoKey,
            400,
            48,
            2,
            deadline,
        )
    };
    let (sources, repairs) = video(0);
    assert!(sources.len() > 10);
    for symbol in &sources {
        relay
            .primary_sender
            .send_to(symbol, relay.primary_video.unwrap())
            .await
            .unwrap();
    }
    for symbol in &repairs {
        relay
            .secondary_sender
            .send_to(symbol, relay.secondary_video.unwrap())
            .await
            .unwrap();
    }
    let audio = fast_audio_symbols(4001, 0, deadline);
    for symbol in &audio {
        relay
            .primary_sender
            .send_to(symbol, relay.primary_audio)
            .await
            .unwrap();
    }
    let everything = sorted(sources.iter().chain(&repairs).cloned().collect());
    assert_eq!(
        receive_from(primary_video, everything.len(), from).await,
        everything,
        "the primary child takes the whole video stream at its primary port"
    );
    assert_eq!(
        receive_from(other_video, repairs.len(), from).await,
        sorted(repairs.clone()),
        "the other child takes its repair, at its secondary port"
    );
    assert_eq!(
        receive_from(primary_audio, audio.len(), from).await,
        sorted(audio.clone())
    );
    assert_eq!(
        receive_from(other_audio, audio.len(), from).await,
        sorted(audio.clone())
    );
    for socket in [
        primary_video,
        other_video,
        primary_audio,
        other_audio,
        primary_secondary_audio,
        primary_secondary_video,
        other_primary_audio,
        other_primary_video,
    ] {
        assert_nothing_more(socket).await;
    }
    wait_until("the held sources are warm", || {
        lease.stats.snapshot().warm_buffered == sources.len() as u64
    })
    .await;

    // A controller at an address without a lease is refused.
    let listener = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let listener_addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    tokio::spawn(fast_control::run_lease_failover_listener(
        listener,
        Arc::clone(&lease),
        TopologyGeneration::new(FAST_GENERATION).unwrap(),
        SubscriptionId::new(FAST_SUBSCRIPTION).unwrap(),
        shutdown_rx,
    ));
    let command = |transition_id: u64| {
        FailoverLeaseCommand::new(
            TopologyGeneration::new(FAST_GENERATION).unwrap(),
            SubscriptionId::new(FAST_SUBSCRIPTION).unwrap(),
            transition_id,
            now_unix_us(),
            5_000_000,
            FailoverForwardMode::SourceAndRepair,
        )
        .unwrap()
        .encode()
    };
    let stranger = UdpSocket::bind((loopback(9), 0)).await.unwrap();
    stranger.send_to(&command(1), listener_addr).await.unwrap();
    wait_until("the stranger is refused", || {
        lease.stats.snapshot().commands_rejected == 1
    })
    .await;
    assert_nothing_more(other_video).await;

    // The other child's controller promotes this forwarder: its warm
    // sources are replayed to it, and new sources follow.
    let controller = UdpSocket::bind((other_ip, 0)).await.unwrap();
    controller
        .send_to(&command(1), listener_addr)
        .await
        .unwrap();
    assert_eq!(
        receive_from(other_video, sources.len(), from).await,
        sorted(sources.clone()),
        "the warm ring is replayed on promotion"
    );
    let snapshot = lease.stats.snapshot();
    assert_eq!(snapshot.promotions, 1);
    assert_eq!(snapshot.warm_replayed, sources.len() as u64);
    let (next_sources, _) = video(1);
    for symbol in &next_sources {
        relay
            .primary_sender
            .send_to(symbol, relay.primary_video.unwrap())
            .await
            .unwrap();
    }
    assert_eq!(
        receive_from(other_video, next_sources.len(), from).await,
        sorted(next_sources.clone()),
        "a promoted child takes the sources"
    );
    assert_eq!(
        receive_from(primary_video, next_sources.len(), from).await,
        sorted(next_sources.clone())
    );

    let _ = shutdown_tx.send(());
    relay.stop().await;
}

/// B5: children beyond --relay-max-lease-children are refused and keep the
/// cache mesh; the others are fed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_children_above_the_cap_are_refused() {
    let ips = [loopback(2), loopback(3)];
    let (ports, children) = bind_lease_children(&ips).await;
    let lease = LeaseChildren::bind(
        LeaseChildConfig {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            child_ports: ports,
            class_port_offset: OFFSET,
            primary_children: ips.iter().map(|ip| IpAddr::V4(*ip)).collect(),
            max_children: 1,
            ranked: true,
        },
        4 << 20,
    )
    .unwrap();
    lease.rebuild(&[served(ips[0], 4001), served(ips[1], 4001)]);
    let relay = start_fast_relay_with(ParentPath::Secondary, &[], lease_config(&lease)).await;
    let symbols = fast_audio_symbols(4001, 0, now_unix_us() + 5_000_000);
    for symbol in &symbols {
        relay
            .primary_sender
            .send_to(symbol, relay.primary_audio)
            .await
            .unwrap();
    }
    let from = SocketAddr::from((Ipv4Addr::LOCALHOST, lease.local_addr().port()));
    assert_eq!(
        receive_from(&children[0][0], symbols.len(), from).await,
        sorted(symbols.clone())
    );
    assert_nothing_more(&children[1][0]).await;
    let stats = relay.stats().lease_children;
    assert_eq!((stats.children, stats.refused), (1, 1));
    relay.stop().await;
}

/// An edge's RLS1 ports, at the given address: the primary and secondary
/// relay binds, each with its video twin.
pub(super) fn bind_edge_class_ports(
    ip: Ipv4Addr,
) -> ([u16; 2], Vec<fast_plane::relay::ClassSocket>) {
    for _ in 0..128 {
        let probe = || {
            std::net::UdpSocket::bind((ip, 0))
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        };
        let ports = [probe(), probe()];
        if ports[0] == ports[1] || ports.iter().any(|port| port.checked_add(OFFSET).is_none()) {
            continue;
        }
        if let Ok(sockets) = fast_plane::relay::bind_class_sockets(
            SocketAddr::from((ip, ports[0])),
            Some(SocketAddr::from((ip, ports[1]))),
            OFFSET,
            4 << 20,
        ) {
            return (ports, sockets);
        }
    }
    panic!("no free edge ports");
}

/// An edge's dispatch: its two forwarders (their lease sockets) bound as
/// parents that may send both sources and repair, as deploy binds them.
pub(super) fn edge_dispatch(primary: SocketAddr, secondary: SocketAddr) -> RelayUdpDispatch {
    let mut dispatch = empty_relay_udp_dispatch();
    for (session_id, peer, peer_id) in [(1, primary, "forwarder-1"), (2, secondary, "forwarder-2")]
    {
        dispatch
            .bind_controlled_peer(
                peer,
                ControlledRelayParentSession::new(
                    session_id,
                    CarrierIdentity {
                        local: NodeId::new("edge-london").unwrap(),
                        peer: NodeId::new(peer_id).unwrap(),
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

/// A canonical opaque part of `epoch`.
fn encode_test_canonical_opaque_object_in_epoch(
    stream_id: u64,
    epoch: u64,
    sequence: u64,
    payload: &[u8],
) -> Bytes {
    let key = media_object::ObjectKey::for_payload(
        "default",
        stream_id.to_string(),
        "opaque-audio",
        epoch,
        0,
        sequence,
        1,
        payload,
    )
    .unwrap();
    let object = MediaObject::builder(key, ObjectKind::Media, payload.to_vec())
        .with_metadata("container", b"opaque".to_vec())
        .with_metadata("payload-format", b"opaque-part-v1".to_vec())
        .with_metadata("content-type", b"application/octet-stream".to_vec())
        .with_metadata("file-extension", b"bin".to_vec())
        .build()
        .unwrap();
    Bytes::from(media_object::encode(&object).unwrap())
}

async fn slot(cache: &LiveTsCache, stream_id: u64, part: u64) -> Option<Bytes> {
    cache
        .chunk_cache
        .get_for_stream_id(stream_id, part as usize)
        .await
        .map(|(bytes, _)| bytes)
}

/// B6: contributor -> F1 + F2 -> edge on the fast plane. The edge runs the
/// class threads on its two relay ports with both forwarders bound, and its
/// tee commits every part into its cache byte for byte as the forwarders
/// commit it (the part the cache mesh carries). With 5 % loss on the
/// contributor's lane to F1, and F1 stopped part way, the edge still
/// commits every audio part, from F2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_fed_by_two_forwarders_commits_every_part_byte_for_byte() {
    let edge_ip = loopback(2);
    let (ports, sockets) = bind_edge_class_ports(edge_ip);
    let lease_1 = lease_children(ports, &[edge_ip]);
    let lease_2 = lease_children(ports, &[]);
    for lease in [&lease_1, &lease_2] {
        lease.rebuild(&[served(edge_ip, 4001)]);
    }
    let forwarder_1 =
        start_fast_relay_with(ParentPath::Secondary, &[], lease_config(&lease_1)).await;
    let forwarder_2 =
        start_fast_relay_with(ParentPath::Secondary, &[], lease_config(&lease_2)).await;
    let lease_addr =
        |lease: &LeaseChildren| SocketAddr::from((Ipv4Addr::LOCALHOST, lease.local_addr().port()));

    let edge_cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 64, 64).await;
    edge_cache.set_hop_journal(Arc::new(HopJournal::new("edge-london", 4_096)));
    let (edge_shutdown_tx, edge_shutdown_rx) = watch::channel(());
    let edge_task = tokio::spawn(fast_plane::relay::run_relay_class_ingest(
        sockets,
        Arc::clone(&edge_cache),
        edge_shutdown_rx,
        RelayIngestRuntime {
            dispatch: edge_dispatch(lease_addr(&lease_1), lease_addr(&lease_2)),
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
            tee_capacity: 4_096,
            ..Default::default()
        },
    ));
    wait_until("the edge's threads start", || {
        edge_cache.relay_ingress_snapshot().fast_plane.threads == 2
    })
    .await;

    let deadline = now_unix_us() + 10_000_000;
    let parts = 40_u64;
    let mut forwarder_1 = Some(forwarder_1);
    for sequence in 0..parts {
        if sequence == 20 {
            forwarder_1.take().unwrap().stop().await;
        }
        // 5 % loss on the contributor's lane to F1.
        let lost_at_1 = sequence % 20 == 7;
        for symbol in fast_audio_symbols(4001, sequence, deadline) {
            if let Some(forwarder) = forwarder_1.as_ref().filter(|_| !lost_at_1) {
                forwarder
                    .primary_sender
                    .send_to(&symbol, forwarder.primary_audio)
                    .await
                    .unwrap();
            }
            forwarder_2
                .primary_sender
                .send_to(&symbol, forwarder_2.primary_audio)
                .await
                .unwrap();
        }
        // The forwarders keep a window of a few parts: each part is compared
        // as it comes.
        let at_edge = wait_for_part(&edge_cache, 4001, sequence).await;
        assert_eq!(
            at_edge,
            wait_for_part(&forwarder_2.cache, 4001, sequence).await,
            "part {sequence}"
        );
        assert_eq!(
            slot(&edge_cache, 4001, sequence).await,
            slot(&forwarder_2.cache, 4001, sequence).await,
            "part {sequence}: the edge's slot is F2's, byte for byte"
        );
        if let Some(forwarder) = forwarder_1.as_ref().filter(|_| !lost_at_1) {
            wait_for_part(&forwarder.cache, 4001, sequence).await;
            assert_eq!(
                slot(&edge_cache, 4001, sequence).await,
                slot(&forwarder.cache, 4001, sequence).await,
                "part {sequence}: the edge's slot is F1's, byte for byte"
            );
        }
    }
    for sequence in 0..parts {
        assert!(
            slot(&edge_cache, 4001, sequence).await.is_some(),
            "the edge holds part {sequence}"
        );
    }
    let stats = edge_cache.relay_ingress_snapshot().fast_plane;
    assert!(
        stats.duplicates > 0,
        "the second forwarder's copies are dropped as duplicates"
    );
    assert_eq!(stats.not_admitted, 0);
    assert_eq!(
        hop_events(&edge_cache, HopStage::Committed, 4001).len(),
        parts as usize,
        "each part is committed once"
    );
    // The edge's playlist has every part.
    let playlist = edge_cache.playlist_for_stream_id(4001).await;
    assert!(
        playlist.contains(&format!("part{}.", parts - 1)),
        "{playlist}"
    );

    let _ = edge_shutdown_tx.send(());
    edge_task.await.unwrap().unwrap();
    forwarder_2.stop().await;
}

/// The contributor sends a forwarder an object's video sources (primary
/// lane) and repair (secondary lane).
async fn send_video(forwarder: &FastRelay, (sources, repairs): &(Vec<Vec<u8>>, Vec<Vec<u8>>)) {
    for symbol in sources {
        forwarder
            .primary_sender
            .send_to(symbol, forwarder.primary_video.unwrap())
            .await
            .unwrap();
    }
    for symbol in repairs {
        forwarder
            .secondary_sender
            .send_to(symbol, forwarder.secondary_video.unwrap())
            .await
            .unwrap();
    }
}

/// B5 + B6, video: F1 is the edge's primary forwarder and sends it a 4K
/// stream in full, F2 its repair (keeping the sources warm). F1 stops: the
/// edge's failover controller promotes F2 with RLF1 to F2's lease failover
/// listener, F2 replays the warm sources and sends new ones, and the edge
/// commits every object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_promotes_its_secondary_forwarder_for_video_when_the_primary_stops() {
    let edge_ip = loopback(2);
    let (ports, sockets) = bind_edge_class_ports(edge_ip);
    let lease_1 = lease_children(ports, &[edge_ip]);
    let lease_2 = lease_children(ports, &[]);
    for lease in [&lease_1, &lease_2] {
        lease.rebuild(&[served(edge_ip, 1)]);
    }
    let forwarder_1 =
        start_fast_relay_with(ParentPath::PromotedSecondary, &[], lease_config(&lease_1)).await;
    let forwarder_2 =
        start_fast_relay_with(ParentPath::PromotedSecondary, &[], lease_config(&lease_2)).await;
    let lease_addr =
        |lease: &LeaseChildren| SocketAddr::from((Ipv4Addr::LOCALHOST, lease.local_addr().port()));
    let listener = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let listener_addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    tokio::spawn(fast_control::run_lease_failover_listener(
        listener,
        Arc::clone(&lease_2),
        TopologyGeneration::new(FAST_GENERATION).unwrap(),
        SubscriptionId::new(FAST_SUBSCRIPTION).unwrap(),
        shutdown_rx.clone(),
    ));
    let controller_args = Args::try_parse_from([
        "av-mesh".to_owned(),
        "--relay-failover-controller".to_owned(),
        format!("{edge_ip}:0={listener_addr}"),
        "--relay-topology-generation".to_owned(),
        FAST_GENERATION.to_string(),
        "--relay-subscription-id".to_owned(),
        FAST_SUBSCRIPTION.to_string(),
        "--relay-primary-silence-ms".to_owned(),
        "100".to_owned(),
    ])
    .unwrap();
    let controller = RelayFailoverController::bind(&controller_args)
        .await
        .unwrap()
        .unwrap();

    let edge_cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 64, 64).await;
    let (edge_shutdown_tx, edge_shutdown_rx) = watch::channel(());
    let edge_task = tokio::spawn(fast_plane::relay::run_relay_class_ingest(
        sockets,
        Arc::clone(&edge_cache),
        edge_shutdown_rx,
        RelayIngestRuntime {
            dispatch: edge_dispatch(lease_addr(&lease_1), lease_addr(&lease_2)),
            secondary_socket: None,
            class_twins: Vec::new(),
            forwarder: None,
            audio_epochs: None,
            failover_controller: Some(controller),
            failover_heartbeat: Duration::from_millis(20),
            link_telemetry: None,
        },
        RelayClassConfig {
            port_offset: OFFSET,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 4_096,
            ..Default::default()
        },
    ));

    let deadline = now_unix_us() + 20_000_000;
    let object = |sequence: u64| {
        let envelope = encode_test_canonical_fmp4_bundle_with_duration(
            1,
            sequence,
            None,
            &deterministic_video_payload(6_000 + sequence as usize),
            None,
            sequence == 0,
        );
        fast_test_symbols(
            &envelope,
            relay_session::MediaPriority::VideoKey,
            400,
            48,
            2,
            deadline,
        )
    };
    let mut sequence = 0_u64;
    // Healthy: the primary's sources and the secondary's repair arrive.
    timeout(Duration::from_secs(5), async {
        loop {
            send_video(&forwarder_1, &object(sequence)).await;
            send_video(&forwarder_2, &object(sequence)).await;
            wait_for_part(&edge_cache, 1, sequence).await;
            sequence += 1;
            if edge_cache
                .relay_ingress_snapshot()
                .failover_controller_state
                == RelayFailoverControllerState::Healthy
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the edge's controller sees both forwarders");
    // F2 sends the edge the two repair symbols of each object only.
    wait_until("F2 sends the edge its repair", || {
        lease_2.stats.snapshot().video_sent == 2 * sequence
    })
    .await;
    assert!(lease_2.stats.snapshot().warm_buffered > 0);
    assert_eq!(
        lease_1.stats.snapshot().warm_buffered,
        0,
        "F1 is the primary"
    );

    // F1 stops. The next objects reach F2 only; it holds their sources
    // until the edge promotes it, then replays them.
    forwarder_1.stop().await;
    let first_after = sequence;
    timeout(Duration::from_secs(5), async {
        while lease_2.stats.snapshot().promotions == 0 {
            send_video(&forwarder_2, &object(sequence)).await;
            sequence += 1;
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .expect("the edge promotes F2");
    for _ in 0..3 {
        send_video(&forwarder_2, &object(sequence)).await;
        sequence += 1;
    }
    for part in first_after..sequence {
        wait_for_part(&edge_cache, 1, part).await;
    }
    let lease = lease_2.stats.snapshot();
    assert!(lease.warm_replayed > 0, "{lease:?}");
    assert_eq!(
        edge_cache
            .relay_ingress_snapshot()
            .failover_controller_state,
        RelayFailoverControllerState::Promoted
    );

    let _ = shutdown_tx.send(());
    let _ = edge_shutdown_tx.send(());
    edge_task.await.unwrap().unwrap();
    forwarder_2.stop().await;
}

/// B6: an edge whose cache mesh wrote parts of a stream (a lease's warm-up
/// window) before the first part of the fast plane commits takes up their
/// epoch and keeps them; a part of a new epoch still replaces the copy.
#[tokio::test]
async fn an_edge_takes_up_the_epoch_of_its_cache_mesh_copy() {
    let distributor = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
    let edge = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
    let commit = |cache: Arc<LiveTsCache>, sequence: u64, epoch: u64| async move {
        let envelope = encode_test_canonical_opaque_object_in_epoch(
            4001,
            epoch,
            sequence,
            &[sequence as u8; 120],
        );
        let object = media_object::decode(&envelope).unwrap();
        commit_relay_object(&cache, object, envelope.to_vec())
            .await
            .unwrap();
    };
    for sequence in 0..4 {
        commit(Arc::clone(&distributor), sequence, 5).await;
    }
    // The cache mesh writes the warm-up window into the edge's cache.
    for sequence in 0..3_u64 {
        edge.chunk_cache
            .add_for_stream_id(
                4001,
                sequence as usize,
                slot(&distributor, 4001, sequence).await.unwrap(),
            )
            .await
            .unwrap();
    }
    // The fast plane's first part arrives before the mesh's copy is seen.
    commit(Arc::clone(&edge), 3, 5).await;
    for sequence in 0..4 {
        assert_eq!(
            slot(&edge, 4001, sequence).await,
            slot(&distributor, 4001, sequence).await,
            "part {sequence} is kept"
        );
    }
    // A new publication starts again.
    commit(Arc::clone(&edge), 0, 6).await;
    assert!(slot(&edge, 4001, 3).await.is_none());
}

#[test]
fn the_fast_plane_edge_flags_default_on_and_can_be_turned_off() {
    let args = Args::try_parse_from(["av-mesh"])
        .unwrap()
        .normalized()
        .unwrap();
    assert!(args.relay_lease_children);
    assert!(args.edge_fast_ingest);
    assert_eq!(args.relay_lease_child_ports, [22_001, 22_002]);
    assert_eq!(args.relay_max_lease_children, 16);
    assert!(args.relay_lease_bind.is_none());

    let edge = [
        "av-mesh",
        "--cache-mesh-role",
        "edge",
        "--relay-controlled-local",
        "--fec-bind",
        "0.0.0.0:22001",
        "--relay-primary-peer",
        "10.0.0.1:22601",
        "--relay-primary-promoted",
        "--relay-secondary-bind",
        "0.0.0.0:22002",
        "--relay-secondary-peer",
        "10.0.0.2:22601",
        "--relay-secondary-promoted",
    ];
    let args = Args::try_parse_from(edge).unwrap().normalized().unwrap();
    assert!(args.relay_controlled_local);
    assert_eq!(
        args.relay_secondary_bind,
        Some("0.0.0.0:22002".parse().unwrap())
    );
    let args = Args::try_parse_from(edge.iter().copied().chain(["--edge-fast-ingest", "false"]))
        .unwrap()
        .normalized()
        .unwrap();
    assert!(
        !args.relay_controlled_local,
        "the edge takes the cache mesh alone"
    );
    assert!(args.relay_primary_peer.is_none() && args.relay_secondary_peer.is_none());
    assert!(args.relay_secondary_bind.is_none());

    let forwarder = Args::try_parse_from([
        "av-mesh",
        "--relay-lease-bind",
        "10.0.0.1:22601",
        "--relay-lease-child-ports",
        "23001,23002",
        "--relay-lease-primary-children",
        "10.0.1.5,10.0.1.7",
        "--relay-max-lease-children",
        "8",
        "--relay-lease-failover-bind",
        "10.0.0.1:22602",
    ])
    .unwrap()
    .normalized()
    .unwrap();
    assert_eq!(forwarder.relay_lease_child_ports, [23_001, 23_002]);
    assert_eq!(
        forwarder.relay_lease_primary_children,
        vec![
            "10.0.1.5".parse::<IpAddr>().unwrap(),
            "10.0.1.7".parse::<IpAddr>().unwrap()
        ]
    );
    assert!(
        Args::try_parse_from(["av-mesh", "--relay-lease-bind", "127.0.0.1:12001"])
            .unwrap()
            .normalized()
            .is_err()
    );
    assert!(
        Args::try_parse_from(["av-mesh", "--relay-lease-children", "false"])
            .map(|args| !args.relay_lease_children)
            .unwrap()
    );
}

/// An edge at 127.0.0.2 on the fast plane and its two parents at addresses
/// of their own: forwarder 1's lease socket at 127.0.0.1 and forwarder 2's
/// at 127.0.0.3, as two nodes of a region's VLAN.
struct FedEdge {
    cache: Arc<LiveTsCache>,
    shutdown_tx: watch::Sender<()>,
    task: tokio::task::JoinHandle<Result<()>>,
    forwarders: [Option<FastRelay>; 2],
    parents: [IpAddr; 2],
}

impl FedEdge {
    async fn start(streams: &[u64]) -> Self {
        let edge_ip = loopback(2);
        let parents = [loopback(1), loopback(3)];
        let (ports, sockets) = bind_edge_class_ports(edge_ip);
        let leases = [
            lease_children_at(parents[0], ports, &[edge_ip]),
            lease_children_at(parents[1], ports, &[]),
        ];
        let leased: Vec<ServedSubscription> = streams
            .iter()
            .map(|stream_id| served(edge_ip, *stream_id))
            .collect();
        let mut forwarders = Vec::new();
        for lease in &leases {
            lease.rebuild(&leased);
            forwarders.push(Some(
                start_fast_relay_with(ParentPath::Secondary, &[], lease_config(lease)).await,
            ));
        }
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 64, 64).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(fast_plane::relay::run_relay_class_ingest(
            sockets,
            Arc::clone(&cache),
            shutdown_rx,
            RelayIngestRuntime {
                dispatch: edge_dispatch(leases[0].local_addr(), leases[1].local_addr()),
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
                tee_capacity: 4_096,
                ..Default::default()
            },
        ));
        wait_until("the edge's threads start", || {
            cache.relay_ingress_snapshot().fast_plane.threads == 2
        })
        .await;
        Self {
            cache,
            shutdown_tx,
            task,
            forwarders: forwarders.try_into().ok().unwrap(),
            parents: parents.map(IpAddr::V4),
        }
    }

    /// The contributor sends a part of `stream_id` to every forwarder that
    /// still runs.
    async fn send_part(&self, stream_id: u64, sequence: u64, deadline_us: u64) {
        for symbol in fast_audio_symbols(stream_id, sequence, deadline_us) {
            for forwarder in self.forwarders.iter().flatten() {
                forwarder
                    .primary_sender
                    .send_to(&symbol, forwarder.primary_audio)
                    .await
                    .unwrap();
            }
        }
    }

    fn feeds(&self) -> Vec<fast_plane::parent_feeds::ParentFeed> {
        self.cache
            .parent_feeds()
            .expect("the edge's class ingest keeps the feeds")
            .snapshot(now_unix_us())
    }

    async fn stop(self) {
        let _ = self.shutdown_tx.send(());
        self.task.await.unwrap().unwrap();
        for forwarder in self.forwarders.into_iter().flatten() {
            forwarder.stop().await;
        }
    }
}

/// The sampler publishes once a second: wait for figures over several of
/// its passes.
async fn wait_for_figures(what: &str, mut done: impl FnMut() -> bool) {
    timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

/// CR4: an edge fed two streams by two forwarders keeps a record per parent
/// and stream, and each is complete: the second copy of a part is dropped
/// at the first-arrival test, after its parent's record took it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_reports_each_parent_feeding_each_stream() {
    let edge = FedEdge::start(&[4001, 4002]).await;
    let parts = 50_u64;
    let deadline = now_unix_us() + 20_000_000;
    for sequence in 0..parts {
        for stream_id in [4001, 4002] {
            edge.send_part(stream_id, sequence, deadline).await;
        }
    }
    for stream_id in [4001, 4002] {
        wait_for_part(&edge.cache, stream_id, parts - 1).await;
    }
    wait_for_figures("every feed shows its objects", || {
        let feeds = edge.feeds();
        feeds.len() == 4 && feeds.iter().all(|feed| feed.objects_10s == parts)
    })
    .await;
    let feeds = edge.feeds();
    assert_eq!(
        feeds
            .iter()
            .map(|feed| (feed.address, feed.stream_id))
            .collect::<Vec<_>>(),
        vec![
            (edge.parents[0], Some(4001)),
            (edge.parents[0], Some(4002)),
            (edge.parents[1], Some(4001)),
            (edge.parents[1], Some(4002)),
        ]
    );
    for feed in &feeds {
        assert_eq!(feed.head_object, Some(parts - 1), "{feed:?}");
        assert_eq!(
            (feed.skipped_10s, feed.resets_10s, feed.multi_symbol_10s),
            (0, 0, 0),
            "{feed:?}"
        );
        assert!(feed.source_age_ms.is_some() && feed.repair_age_ms.is_none());
        // Every object arrived with most of its 20 s left.
        let slack = feed.slack_mean_us_10s.expect("objects have slack");
        assert!((10_000_000..=20_000_000).contains(&slack), "{feed:?}");
        assert!(feed.slack_min_us_10s.is_some_and(|least| least <= slack));
    }
    // Half of what arrived was the second parent's copy, dropped after its
    // record.
    wait_until("the tee publishes the duplicates", || {
        edge.cache.relay_ingress_snapshot().fast_plane.duplicates == 2 * parts
    })
    .await;
    assert_eq!(edge.cache.relay_ingress_snapshot().fast_plane.not_admitted, 0);
    edge.stop().await;
}

/// CR4: two parents send an edge the same parts, the second 4 ms after the
/// first. Both carry the same deadline for the same object, so the second
/// parent's objects have 4 ms less slack, which is what a gate compares: no
/// clock is involved but this node's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parent_delayed_by_4_ms_shows_4_ms_less_slack() {
    let edge_ip = loopback(2);
    let (ports, sockets) = bind_edge_class_ports(edge_ip);
    let prompt = UdpSocket::bind((loopback(1), 0)).await.unwrap();
    let late = UdpSocket::bind((loopback(3), 0)).await.unwrap();
    let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 64, 64).await;
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let task = tokio::spawn(fast_plane::relay::run_relay_class_ingest(
        sockets,
        Arc::clone(&cache),
        shutdown_rx,
        RelayIngestRuntime {
            dispatch: edge_dispatch(prompt.local_addr().unwrap(), late.local_addr().unwrap()),
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
            tee_capacity: 4_096,
            ..Default::default()
        },
    ));
    wait_until("the edge's threads start", || {
        cache.relay_ingress_snapshot().fast_plane.threads == 2
    })
    .await;

    let parts = 120_u64;
    let deadline = now_unix_us() + 20_000_000;
    // What the second parent's copies were really behind by, in all: a
    // sleep of 4 ms is never shorter and often a little longer.
    let mut behind_us = 0_u64;
    for sequence in 0..parts {
        let symbols = fast_audio_symbols(4001, sequence, deadline);
        let sent = now_unix_us();
        for symbol in &symbols {
            prompt
                .send_to(symbol, SocketAddr::from((edge_ip, ports[0])))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(4)).await;
        behind_us += now_unix_us() - sent;
        for symbol in &symbols {
            late.send_to(symbol, SocketAddr::from((edge_ip, ports[1])))
                .await
                .unwrap();
        }
    }
    let behind_us = i64::try_from(behind_us / parts).unwrap();
    assert!(behind_us >= 4_000, "{behind_us}");

    let feeds = cache.parent_feeds().expect("the edge keeps the feeds");
    wait_for_figures("both feeds show their objects", || {
        let rows = feeds.snapshot(now_unix_us());
        rows.len() == 2 && rows.iter().all(|row| row.objects_10s == parts)
    })
    .await;
    let rows = feeds.snapshot(now_unix_us());
    let slack = |ip: Ipv4Addr| {
        rows.iter()
            .find(|row| row.address == IpAddr::V4(ip))
            .and_then(|row| row.slack_mean_us_10s)
            .expect("the parent's objects have slack")
    };
    let less = slack(loopback(1)) - slack(loopback(3));
    assert!(
        (less - behind_us).abs() <= 1_000,
        "the late parent has {less} us less slack; its copies were {behind_us} us behind"
    );
    assert!(less >= 3_500, "{less}");
    // The late parent delivers every object all the same: only its slack
    // tells the two apart.
    assert!(rows.iter().all(|row| row.skipped_10s == 0));

    let _ = shutdown_tx.send(());
    task.await.unwrap().unwrap();
}

/// CR4: a parent that stops is seen by the age of its last source on the
/// child, while the other parent's stays near zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_parent_s_age_grows_and_the_other_s_does_not() {
    let mut edge = FedEdge::start(&[4001]).await;
    let deadline = now_unix_us() + 20_000_000;
    for sequence in 0..30_u64 {
        edge.send_part(4001, sequence, deadline).await;
    }
    wait_for_part(&edge.cache, 4001, 29).await;
    let feeds = Arc::clone(edge.cache.parent_feeds().expect("the edge keeps the feeds"));
    wait_until("both parents deliver", || {
        edge.parents
            .iter()
            .all(|parent| feeds.delivering(*parent, 4001, now_unix_us(), 2_000_000))
    })
    .await;

    edge.forwarders[0].take().unwrap().stop().await;
    for sequence in 30..100_u64 {
        edge.send_part(4001, sequence, deadline).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    wait_for_part(&edge.cache, 4001, 99).await;
    let now_us = now_unix_us();
    let rows = feeds.snapshot(now_us);
    let age = |parent: IpAddr| {
        rows.iter()
            .find(|row| row.address == parent)
            .and_then(|row| row.source_age_ms)
            .expect("the parent delivered once")
    };
    let (killed, live) = (age(edge.parents[0]), age(edge.parents[1]));
    assert!(killed >= 600, "the killed parent's age is {killed} ms");
    assert!(live < 400, "the live parent's age is {live} ms");
    assert!(!feeds.delivering(edge.parents[0], 4001, now_us, 500_000));
    assert!(feeds.delivering(edge.parents[1], 4001, now_us, 500_000));
    assert_eq!(
        rows.iter()
            .map(|row| row.head_object)
            .collect::<Vec<_>>(),
        vec![Some(29), Some(99)]
    );
    edge.stop().await;
}
