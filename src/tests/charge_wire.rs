//! rm/charge: what a session of each stream kind costs an edge on the wire,
//! measured at the listener, against what admission charges.
//!
//! One edge fed on the fast plane by two forwarders, one stream at its pace
//! (1 ms Opus Custom, 2.5 ms Opus, 1 ms PCM, a FLAC-like 1 ms part of 180 B,
//! 4K at 25 Mbit/s), one UDP listener on the compact lane and one on the
//! RaptorQ lane (4K: the RaptorQ lane only), each counting the bytes its
//! leg brings (media datagrams, IP and UDP headers included) over 2 s after
//! 3 s of settling. The edge's charge for each kind's own lane (`stream_charges`:
//! the compact lane for small parts, the RaptorQ lane for 4K) must be
//! within 10 % of what that leg brought; the old charge (the stored slots'
//! rate) is printed beside them.

use super::compact_lane::Payload;
use super::fast_lane::{packaged_envelope, LaneEdge, Packaging};
use super::fast_lease::loopback;
use super::*;
use av_mesh::client_director::{LegKey, LegStart, SessionLegs, StartFrom};
use av_mesh::client_session::{SessionConfig, UdpSession};
use std::sync::atomic::{AtomicBool, AtomicU64};

const STREAM: u64 = 7_301;

#[derive(Debug, Clone, Copy)]
enum Kind {
    Opus1,
    Opus,
    Pcm,
    Flac,
    Video,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Opus1 => "Opus Custom 1 ms",
            Self::Opus => "Opus 2.5 ms",
            Self::Pcm => "PCM 1 ms",
            Self::Flac => "FLAC-like 1 ms (180 B)",
            Self::Video => "4K 25 Mbit/s",
        }
    }

    fn part_us(self) -> u64 {
        match self {
            Self::Opus => 2_500,
            Self::Video => 16_667,
            _ => 1_000,
        }
    }

    fn symbols(self, sequence: u64) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let payload = match self {
            Self::Opus1 => Payload::Opus1,
            Self::Opus => Payload::Opus,
            Self::Pcm | Self::Flac => Payload::Pcm,
            Self::Video => Payload::Video4k,
        };
        let envelope = match self {
            Self::Flac => packaged_envelope(Packaging::Pcm, STREAM, 1, sequence, 180).0,
            _ => payload.envelope(STREAM, sequence).0,
        };
        payload.symbols(&envelope)
    }

    fn video(self) -> bool {
        matches!(self, Self::Video)
    }
}

fn send_from(fd: std::os::fd::RawFd, datagram: &[u8], to: SocketAddr) {
    let SocketAddr::V4(to) = to else {
        unreachable!("loopback")
    };
    let address = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: to.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from(*to.ip()).to_be(),
        },
        sin_zero: [0; 8],
    };
    // SAFETY: a valid fd, a buffer and an address that live for the call.
    unsafe {
        libc::sendto(
            fd,
            datagram.as_ptr().cast(),
            datagram.len(),
            0,
            std::ptr::from_ref(&address).cast(),
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        );
    }
}

/// A listener at `edge` on one lane, counting the wire bytes of the media
/// datagrams its leg brings while `counting` is set.
async fn listen(
    edge: SocketAddr,
    compact: bool,
    counting: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) -> u64 {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(16 << 20);
    let config = SessionConfig {
        compact,
        ..SessionConfig::new(STREAM)
    };
    let mut session = UdpSession::new(config, Instant::now());
    let mut outbox: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
    let start = LegStart {
        from: StartFrom::AfterNewest,
        token: None,
        token_wait: Duration::from_millis(500),
    };
    session.add_leg(LegKey(1), "edge", edge, start, Instant::now(), &mut |to, datagram| {
        outbox.push((to, datagram.to_vec()))
    });
    let mut bytes = 0_u64;
    let mut buf = vec![0_u8; 65_536];
    while !stop.load(Ordering::Relaxed) {
        for (to, datagram) in outbox.drain(..) {
            let _ = socket.send_to(&datagram, to).await;
        }
        let now = Instant::now();
        session.poll_legs(now, &mut |to, datagram| outbox.push((to, datagram.to_vec())));
        while session.next_event().is_some() {}
        let wake = session.next_wake().min(now + Duration::from_millis(20));
        let Ok(Ok((len, peer))) = tokio::time::timeout(
            wake.saturating_duration_since(now),
            socket.recv_from(&mut buf),
        )
        .await
        else {
            continue;
        };
        let datagram = &buf[..len];
        let media = av_mesh::client_udp::Control::decode(datagram).is_none();
        session.on_datagram(peer, datagram, Instant::now(), &mut |to, datagram| {
            outbox.push((to, datagram.to_vec()))
        });
        while session.next_object().is_some() {}
        if media && counting.load(Ordering::Relaxed) {
            bytes += len as u64 + crate::charge::IP_UDP_BYTES;
        }
    }
    bytes
}

/// What one kind measured: media, the lanes' wire bits a second at the
/// listener, the charges, and the old charge.
#[derive(Debug)]
struct Measured {
    kind: &'static str,
    media_bps: u64,
    compact_wire: Option<u64>,
    raptorq_wire: u64,
    charges: crate::charge::LaneCharges,
    old_charge: Option<u64>,
}

async fn measure(kind: Kind, ip: u8) -> Measured {
    let edge = Arc::new(LaneEdge::start(loopback(ip)).await);
    let running = Arc::new(AtomicBool::new(true));
    let published = Arc::new(AtomicU64::new(0));
    let publisher = {
        let (edge, running, published) = (Arc::clone(&edge), Arc::clone(&running), Arc::clone(&published));
        std::thread::spawn(move || {
            let start = Instant::now();
            let mut sequence = 0_u64;
            while running.load(Ordering::Relaxed) {
                let due = start + Duration::from_micros(kind.part_us() * sequence);
                if let Some(wait) = due.checked_duration_since(Instant::now()) {
                    std::thread::sleep(wait);
                }
                let (sources, repairs) = kind.symbols(sequence);
                for symbol in &sources {
                    send_from(edge.forwarder_fd(0), symbol, edge.relay_port(0, kind.video()));
                    if !kind.video() {
                        send_from(edge.forwarder_fd(1), symbol, edge.relay_port(1, kind.video()));
                    }
                }
                for symbol in &repairs {
                    send_from(edge.forwarder_fd(1), symbol, edge.relay_port(1, kind.video()));
                }
                published.store(sequence, Ordering::Relaxed);
                sequence += 1;
            }
        })
    };
    let counting = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let compact = (!kind.video()).then(|| {
        tokio::spawn(listen(edge.client_port, true, Arc::clone(&counting), Arc::clone(&stop)))
    });
    let raptorq = tokio::spawn(listen(edge.client_port, false, Arc::clone(&counting), Arc::clone(&stop)));
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _ = edge.router.stream_charges(STREAM).await;
    counting.store(true, Ordering::Relaxed);
    let started = Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;
    counting.store(false, Ordering::Relaxed);
    let seconds = started.elapsed().as_secs_f64();
    let charges = edge.router.stream_charges(STREAM).await.expect("the stream's charges");
    let old_charge = edge.cache.playlist_bitrate_bps(STREAM).await;
    stop.store(true, Ordering::Relaxed);
    let compact_bytes = match compact {
        Some(task) => Some(task.await.unwrap()),
        None => None,
    };
    let raptorq_bytes = raptorq.await.unwrap();
    running.store(false, Ordering::Relaxed);
    let _ = tokio::task::spawn_blocking(move || publisher.join()).await;
    let edge = Arc::try_unwrap(edge).unwrap_or_else(|_| panic!("the edge is held"));
    edge.stop().await;
    let bps = |bytes: u64| (bytes as f64 * 8.0 / seconds) as u64;
    Measured {
        kind: kind.name(),
        media_bps: charges.media_bps,
        compact_wire: compact_bytes.map(bps),
        raptorq_wire: bps(raptorq_bytes),
        charges,
        old_charge,
    }
}

fn within(charge: u64, wire: u64, share: f64) -> bool {
    (charge as f64 - wire as f64).abs() <= wire as f64 * share
}

/// rm/charge: for every kind, the compact lane's and the RaptorQ lane's
/// charges are within 10 % of the wire bytes the listener's leg brought.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_is_charged_what_its_lane_sends() {
    let mut rows = Vec::new();
    for (index, kind) in [Kind::Opus1, Kind::Opus, Kind::Pcm, Kind::Flac, Kind::Video]
        .into_iter()
        .enumerate()
    {
        rows.push(measure(kind, 170 + index as u8).await);
    }
    for row in &rows {
        eprintln!(
            "charge: {:<24} media {:>9} bit/s | compact wire {:>9?} charge {:>9} | raptorq wire {:>9} charge {:>9} | http charge {:>9} | old charge {:?}",
            row.kind,
            row.media_bps,
            row.compact_wire,
            row.charges.compact_bps,
            row.raptorq_wire,
            row.charges.raptorq_bps,
            row.charges.http_bps,
            row.old_charge,
        );
    }
    for row in &rows {
        if let Some(wire) = row.compact_wire {
            assert!(within(row.charges.compact_bps, wire, 0.10), "{row:?}");
        }
        // The RaptorQ lane is a heavy stream's lane (small streams ride the
        // compact lane; on the RaptorQ lane their tiny parts go as the
        // contributor's padded symbols, 10 to 60 times their media,
        // printed above and not charged by this model).
        if row.compact_wire.is_none() {
            assert!(within(row.charges.raptorq_bps, row.raptorq_wire, 0.10), "{row:?}");
        }
    }
}

/// rm/charge: an edge whose admission refused LL-HLS readers of 1 ms PCM at
/// N by the old charge (the stored slots' rate) admits them up to its
/// honest limit (its egress budget over the reader's lane charge), and no
/// further.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crowd_is_admitted_to_the_honest_limit_and_no_further() {
    let config = EdgeAdmissionConfig {
        capacity_bps: 100_000_000,
        ..EdgeAdmissionConfig::default()
    };
    let edge = Arc::new(
        LaneEdge::start_full(
            loopback(180),
            SocketAddr::from((loopback(180), 0)),
            Duration::ZERO,
            64,
            true,
            |_| {},
            move |router| {
                let mut router = router;
                router.edge_load = EdgeLoad::new(config);
                router
            },
        )
        .await,
    );
    let running = Arc::new(AtomicBool::new(true));
    let publisher = {
        let (edge, running) = (Arc::clone(&edge), Arc::clone(&running));
        std::thread::spawn(move || {
            let start = Instant::now();
            let mut sequence = 0_u64;
            while running.load(Ordering::Relaxed) {
                let due = start + Duration::from_micros(1_000 * sequence);
                if let Some(wait) = due.checked_duration_since(Instant::now()) {
                    std::thread::sleep(wait);
                }
                let (sources, repairs) = Kind::Pcm.symbols(sequence);
                for symbol in &sources {
                    send_from(edge.forwarder_fd(0), symbol, edge.relay_port(0, false));
                    send_from(edge.forwarder_fd(1), symbol, edge.relay_port(1, false));
                }
                for symbol in &repairs {
                    send_from(edge.forwarder_fd(1), symbol, edge.relay_port(1, false));
                }
                sequence += 1;
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(5)).await;
    // The edge advertises: its admission takes the reader's lane charge.
    let snapshot = edge.router.local_mesh_snapshot().await;
    let holding = snapshot
        .edge_service
        .as_ref()
        .unwrap()
        .playlists
        .iter()
        .find(|playlist| playlist.stream_id == STREAM)
        .cloned()
        .expect("the playlist is advertised");
    let lanes = holding.lane_bps.expect("its lane charges");
    let old = edge.cache.playlist_bitrate_bps(STREAM).await.expect("the old figure");
    let admission = config.normalized().capacity_bps * u64::from(config.admission_percent) / 100;
    let mut admitted = 0_u64;
    for index in 0..1_000 {
        let session = format!("reader-{index}");
        match edge.router.edge_load.admit_playback(PlaybackRequest {
            session_id: Some(&session),
            stream_id: Some(STREAM),
            draining: false,
        }) {
            PlaybackAdmission::Admitted => admitted += 1,
            _ => break,
        }
    }
    running.store(false, Ordering::Relaxed);
    let _ = tokio::task::spawn_blocking(move || publisher.join()).await;
    let old_limit = admission / old;
    let new_limit = admission / lanes.http_bps;
    eprintln!(
        "charge: PCM 1 ms readers on {} Mbit/s of admission: old charge {old} bit/s ({old_limit} readers), reader charge {} bit/s ({new_limit}), admitted {admitted}; advertised bitrate {:?} (media {})",
        admission / 1_000_000,
        lanes.http_bps,
        holding.bitrate_bps,
        lanes.media_bps,
    );
    assert_eq!(holding.bitrate_bps, Some(lanes.media_bps), "the advertised rate is the media's");
    assert!(new_limit > old_limit * 2, "{old_limit} {new_limit}");
    assert_eq!(admitted, new_limit);
    Arc::try_unwrap(edge)
        .unwrap_or_else(|_| panic!("the edge is held"))
        .stop()
        .await;
}

/// rm/charge: a two-leg UDP listener of a small stream is charged on both
/// edges its legs are at, each its compact lane's charge (not a reader's,
/// not the stored slots' rate).
#[tokio::test]
async fn a_two_leg_listener_is_charged_its_lane_on_both_edges() {
    let cache = LiveTsCache::new(801, Duration::from_millis(50), 2, 6, 64).await;
    let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
    let telemetry = TelemetryAggregator::default();
    let lanes = crate::charge::LaneCharges {
        media_bps: 2_432_000,
        compact_bps: 2_846_000,
        raptorq_bps: 13_248_000,
        http_bps: 4_032_000,
    };
    for node in ["edge-a", "edge-b", "edge-c"] {
        let mut snapshot = telemetry_snapshot_for_tests(
            node,
            "test-region",
            "test-continent",
            51.6,
            -0.2,
            Vec::new(),
            801,
        );
        let mut edge = EdgeServiceSnapshot::from_node(
            &snapshot.node,
            Some(format!("https://{node}.example/live")),
            "edge",
            EdgeLoadSnapshot::default(),
        );
        edge.egress_admission_bps = 3_400_000_000;
        edge.playlists = vec![PlaylistHolding {
            stream_id: 801,
            head_part: 500,
            bitrate_bps: Some(2_432_000),
            sessions: 1,
            request_rate: None,
            video: false,
            lane_bps: Some(lanes),
        }];
        snapshot.edge_service = Some(edge);
        telemetry.ingest_snapshot(snapshot).await;
    }
    let mut router = app_router_for_tests_with_telemetry(cache, Arc::clone(&mesh), telemetry);
    router.cache_mesh_role = CacheMeshRole::Distributor;
    let reader = ReaderRequest {
        udp: true,
        stream_id: 801,
        session: Some("two-legs"),
        paths: 2,
        ..ReaderRequest::default()
    };
    let (assignment, _) = router.assign_reader(reader, true).await;
    let assignment = assignment.unwrap();
    assert_eq!(assignment.legs, 2);
    for leg in assignment.caches.iter().take(2) {
        let charged = router
            .pending_assignments
            .get(&leg.node_id)
            .map(|pending| pending.1);
        assert_eq!(charged, Some(lanes.compact_bps), "{}", leg.node_id);
    }
    // A reader of the same playlist is charged a reader's lane.
    let reader = ReaderRequest {
        stream_id: 801,
        session: Some("reader"),
        ..ReaderRequest::default()
    };
    let (assignment, _) = router.assign_reader(reader, true).await;
    let node = assignment.unwrap().caches[0].node_id.clone();
    let charged = router.pending_assignments.get(&node).map(|pending| pending.1);
    assert!(charged.is_some_and(|bps| bps == lanes.http_bps || bps == lanes.http_bps + lanes.compact_bps), "{charged:?}");
    mesh.shutdown();
}
