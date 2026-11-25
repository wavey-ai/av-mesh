//! The pull loop (send240, C), in process: an edge whose video tee sheds
//! parts (a small ring and a slow tee, as a 2-vCPU edge out of CPU) and a
//! cache-mesh parent that holds every part. Before, each shed part was a
//! hole the edge pulled whole over the cache mesh; now a part shed here is
//! never asked for, and a lane whose tee lags or sheds pulls nothing.

use super::fast_lane::LaneEdge;
use super::fast_lease::loopback;
use super::video_throughput::{make_frames_shaped, send_frames_at, FrameShape, Spread};
use super::*;
use playlists::mesh::{CacheMesh, CacheMeshConfig, CacheMeshRole as MeshRole};
use socket2::SockRef;

const FPS: u64 = 24;
const SYMBOL: u16 = 1_088;
const STREAM: u64 = 1;

/// Fields printed (Debug) for the record, some asserted.
#[derive(Debug)]
#[allow(dead_code)]
struct PullRun {
    shed_parts: u64,
    pulls: crate::pull_gate::PullSnapshot,
    hole_requests: u64,
    /// What the edge's cache mesh received, and what the parent served.
    mesh_rx_bytes: u64,
    mesh_rx_parts: u64,
    parent_served: u64,
    /// The process's CPU over the run (s): the same source and tee in
    /// both runs, so the difference is the pulls'.
    cpu_s: f64,
}

fn process_cpu_s() -> f64 {
    // SAFETY: getrusage writes one rusage.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage
    };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1e6;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

async fn run(bypass: bool, ip: u8) -> PullRun {
    let frames = make_frames_shaped(
        STREAM,
        0,
        72,
        40_000_000,
        FrameShape {
            fps: FPS,
            symbol_bytes: SYMBOL,
            repair_per_sources: 20,
        },
    );
    // A tee short of processor time with a small ring: it sheds parts.
    let edge = LaneEdge::start_with_holes(loopback(ip), Duration::from_millis(15), |config| {
        config.tee_capacity = 4_096;
        config.video_tee_bytes = 4_096 * crate::fast_plane::tee_ring::BYTES_PER_ENTRY;
        config.test_video_tee_spin_ns = 230_000;
    })
    .await;
    edge.cache
        .pull_gate
        .bypass
        .store(bypass, Ordering::Relaxed);
    // The parent: a distributor's cache mesh that holds a part of the
    // stream's size at every number (before it starts: nothing is pushed).
    let parent_cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 2_048, 2_048).await;
    let part_bytes = frames[1].sources * usize::from(SYMBOL);
    for frame in &frames {
        parent_cache
            .chunk_cache
            .add_for_stream_id(STREAM, frame.part as usize, Bytes::from(vec![7_u8; part_bytes]))
            .await
            .unwrap();
    }
    let edge_mesh = Arc::clone(&edge.router.mesh);
    let parent = CacheMesh::new(
        Arc::clone(&parent_cache.chunk_cache),
        CacheMeshConfig::new("pull-parent", "uk", unused_loopback_addr())
            .with_role(MeshRole::Distributor)
            .with_peer(edge_mesh.local_addr()),
    )
    .start()
    .await
    .unwrap();
    edge_mesh.add_peer(parent.local_addr()).await;
    let mesh_rx_before = edge_mesh.fec_stats();
    let fd = edge.forwarder_fd(0);
    let _ = SockRef::from(edge.forwarder_socket(0)).set_send_buffer_size(16 << 20);
    let target = edge.relay_port(0, true);
    let cpu_before = process_cpu_s();
    tokio::task::spawn_blocking(move || {
        send_frames_at(
            fd,
            target,
            &frames,
            40_000_000,
            Spread::Paced,
            Instant::now(),
            FPS,
            SYMBOL,
        )
    })
    .await
    .unwrap();
    // The tee works off its backlog; the asks of the last parts go out.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let cpu_s = process_cpu_s() - cpu_before;
    let stats = edge.cache.relay_ingress_snapshot().fast_plane;
    let holes = edge.holes.as_ref().unwrap();
    let run = PullRun {
        shed_parts: stats.video_tee_parts_shed,
        pulls: edge.cache.pull_gate.snapshot(),
        hole_requests: holes.requests.load(Ordering::Relaxed),
        mesh_rx_bytes: edge_mesh.fec_stats().rx_wire_bytes - mesh_rx_before.rx_wire_bytes,
        mesh_rx_parts: edge_mesh.fec_stats().rx_decoded_objects - mesh_rx_before.rx_decoded_objects,
        parent_served: parent.fec_stats().replica_requests_serviced,
        cpu_s,
    };
    parent.shutdown();
    edge.stop().await;
    run
}

/// With a tee that sheds, the edge asks for none of the parts it shed, and
/// its cache mesh receives next to nothing; as before (the gate bypassed)
/// it pulled every shed part whole from its parent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_edge_never_pulls_the_parts_it_shed_itself() {
    let after = run(false, 171).await;
    let before = run(true, 172).await;
    println!("after:  {after:?}\nbefore: {before:?}");
    assert!(
        after.shed_parts > 0 && after.shed_parts < 60,
        "the tee shed some parts: {after:?}"
    );
    assert!(before.shed_parts > 0, "the tee shed parts: {before:?}");
    assert!(
        before.hole_requests > 0 && before.mesh_rx_bytes > 0,
        "before, the shed parts were pulled: {before:?}"
    );
    assert_eq!(after.hole_requests, 0, "{after:?}");
    assert!(after.pulls.not_requested_shed > 0, "{after:?}");
    assert_eq!(after.parent_served, 0, "{after:?}");
    // The mesh's own small objects only, no part.
    assert!(
        after.mesh_rx_bytes < before.mesh_rx_bytes / 5,
        "{} bytes received over the cache mesh, {} before",
        after.mesh_rx_bytes,
        before.mesh_rx_bytes
    );
}
