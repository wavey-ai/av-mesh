//! Pacing of the heavy lane's sends (send240, B), under fq, in process: a
//! forwarder sends each 4K part to five children, and 1 ms audio parts to
//! the same children, with and without `--send-pacing 4`. The test runs
//! itself again in a private network namespace whose loopback has the fq
//! qdisc (the kernel paces a socket only under fq), as the deploy renders.
//!
//! Measured at the children: the most bytes that reach them in one
//! millisecond (the burst at the forwarder's socket), how long after the
//! source's last datagram of a part its last copy completes (the bound: a
//! paced part is spread over about 1/k of its time at the egress rate), and
//! the audio parts' delay through the forwarder (another socket: another
//! flow of fq, never held behind the video).

use super::video_throughput::{in_fq_namespace, make_frames_shaped, send_frames_at, FrameShape, Spread};
use super::*;
use fast_plane::send_pacing::SendPacing;
use fast_plane::sys;
use socket2::SockRef;
use std::os::fd::AsRawFd;

const OFFSET: u16 = 1_000;
const FPS: u64 = 24;
const SYMBOL: u16 = 1_088;
const CHILDREN: usize = 5;
const MEDIA_BPS: u64 = 40_000_000;

/// Fields printed (Debug) for the record, some asserted.
#[derive(Debug)]
#[allow(dead_code)]
struct PacingRun {
    /// The most bytes all children received in one millisecond.
    burst_bytes_1ms: u64,
    /// A part's last copy after the source's last datagram of it (ms).
    part_p50_ms: f64,
    part_max_ms: f64,
    /// Audio through the forwarder (ms).
    audio_p50_ms: f64,
    audio_max_ms: f64,
    rate_bps: u64,
}

/// Every datagram a child's socket received, with its arrival.
fn record(socket: std::net::UdpSocket, stop: Arc<AtomicBool>) -> Vec<(Instant, Vec<u8>)> {
    socket.set_nonblocking(true).unwrap();
    let mut batch = sys::RecvBatch::new();
    let mut got = Vec::new();
    let fd = socket.as_raw_fd();
    loop {
        if sys::wait_readable(&[fd], 20).unwrap_or(0) == 0 {
            if stop.load(Ordering::Relaxed) {
                return got;
            }
            continue;
        }
        let count = batch.recv(&socket).unwrap_or(0);
        let now = Instant::now();
        for index in 0..count {
            got.push((now, batch.data(index).to_vec()));
        }
    }
}

fn quantile(values: &mut [f64], q: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    values
        .get(((values.len().max(1) - 1) as f64 * q) as usize)
        .copied()
        .unwrap_or(f64::NAN)
}

fn run(factor: u64) -> PacingRun {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let pacing = SendPacing::new(factor, 10);
    let (relay, children) = rt.block_on(async {
        let mut children = Vec::new();
        for _ in 0..CHILDREN {
            children.push(bind_class_pair(OFFSET).await);
        }
        let targets: Vec<(SocketAddr, RelayForwardRole)> = children
            .iter()
            .map(|(audio, _)| (audio.local_addr().unwrap(), RelayForwardRole::All))
            .collect();
        let relay = start_fast_relay_with(
            ParentPath::PromotedSecondary,
            &targets,
            fast_plane::relay::RelayClassConfig {
                port_offset: OFFSET,
                cpus: [None, None],
                realtime: false,
                tee_capacity: fast_plane::relay::DEFAULT_TEE_CAPACITY,
                video_gso: true,
                send_pacing: pacing.clone(),
                ..Default::default()
            },
        )
        .await;
        if let Some(forwarder) = &relay.forwarder {
            forwarder.configure_buffers(16 << 20);
        }
        (relay, children)
    });
    let frames = make_frames_shaped(
        1,
        0,
        (3 * FPS) as usize,
        MEDIA_BPS,
        FrameShape {
            fps: FPS,
            symbol_bytes: SYMBOL,
            repair_per_sources: 20,
        },
    );
    let stop = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();
    let mut keep = Vec::new();
    for (audio, video) in children {
        for socket in [audio, video] {
            let socket = socket.into_std().unwrap();
            let _ = SockRef::from(&socket).set_recv_buffer_size(32 << 20);
            let copy = socket.try_clone().unwrap();
            keep.push(socket);
            let stop = Arc::clone(&stop);
            readers.push(std::thread::spawn(move || record(copy, stop)));
        }
    }
    let video_fd = relay.secondary_sender.as_raw_fd();
    let _ = SockRef::from(&relay.secondary_sender).set_send_buffer_size(32 << 20);
    let video_target = relay.secondary_video.unwrap();
    let audio_fd = relay.primary_sender.as_raw_fd();
    let audio_target = relay.primary_audio;
    let start = Instant::now() + Duration::from_millis(50);
    let deadline = now_unix_us() + 60_000_000;
    let (video_sent, audio_sent) = std::thread::scope(|scope| {
        let frames = &frames;
        let video = scope.spawn(move || {
            send_frames_at(
                video_fd,
                video_target,
                frames,
                MEDIA_BPS,
                Spread::Burst,
                start,
                FPS,
                SYMBOL,
            )
        });
        let audio = scope.spawn(move || {
            let mut sent = HashMap::new();
            for sequence in 0..600_u64 {
                let due = start + Duration::from_millis(5 * sequence);
                std::thread::sleep(due.saturating_duration_since(Instant::now()));
                for datagram in fast_audio_symbols(4_001, sequence, deadline) {
                    let _ = sys::send_batch(audio_fd, 1, |_| (&datagram[..], audio_target));
                }
                sent.insert(sequence, Instant::now());
            }
            sent
        });
        // The pacing's measured egress, then its rate: set by the second.
        if let Some(pacing) = &pacing {
            pacing.turn();
            std::thread::sleep(Duration::from_millis(900));
            pacing.turn();
        }
        (video.join().unwrap(), audio.join().unwrap())
    });
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    let received: Vec<Vec<(Instant, Vec<u8>)>> =
        readers.into_iter().map(|reader| reader.join().unwrap()).collect();
    // From the second second on (the pacing's rate set).
    let measured_from = start + Duration::from_millis(1_100);
    let limits = relay_session::RelayLimits::default();
    let mut per_ms: HashMap<u128, u64> = HashMap::new();
    let mut last_copy: HashMap<u64, Instant> = HashMap::new();
    let mut audio_delay = Vec::new();
    for (index, child) in received.iter().enumerate() {
        let video = index % 2 == 1;
        for (at, datagram) in child {
            let Ok(view) = fast_plane::view::RlsView::parse(datagram, limits) else {
                continue;
            };
            if *at < measured_from {
                continue;
            }
            if video {
                *per_ms
                    .entry(at.duration_since(start).as_millis())
                    .or_default() += datagram.len() as u64;
                let last = last_copy.entry(view.object).or_insert(*at);
                *last = (*last).max(*at);
            } else if let Some(sent) = audio_sent.get(&view.object) {
                audio_delay.push(at.saturating_duration_since(*sent).as_secs_f64() * 1e3);
            }
        }
    }
    let mut part_ms: Vec<f64> = frames
        .iter()
        .zip(&video_sent)
        .filter_map(|(frame, sent)| {
            let last = last_copy.get(&frame.part)?;
            Some(last.saturating_duration_since(*sent).as_secs_f64() * 1e3)
        })
        .collect();
    let run = PacingRun {
        burst_bytes_1ms: per_ms.values().copied().max().unwrap_or(0),
        part_p50_ms: quantile(&mut part_ms, 0.5),
        part_max_ms: quantile(&mut part_ms, 1.0),
        audio_p50_ms: quantile(&mut audio_delay, 0.5),
        audio_max_ms: quantile(&mut audio_delay, 1.0),
        rate_bps: pacing.as_ref().map_or(0, |pacing| pacing.snapshot().rate_bps),
    };
    rt.block_on(relay.stop());
    drop(keep);
    run
}

/// Under fq, `--send-pacing 4` spreads each 4K part to five children over
/// about a quarter of its time at the egress rate: the burst at the socket
/// falls to about the paced rate's millisecond, every part's last copy
/// completes within a quarter of the part interval (plus a few ms), and
/// the audio through the same forwarder is not held.
#[test]
fn paced_children_get_each_part_within_its_bound_and_audio_is_not_held() {
    if !in_fq_namespace(
        "tests::send_pacing::paced_children_get_each_part_within_its_bound_and_audio_is_not_held",
    ) {
        return;
    }
    // The loopback's 64 KiB MTU gives fq a quantum of 128 kB (and 640 kB
    // for a new flow) sent before it paces: a NIC's 1,500-byte MTU gives
    // 3 kB and 15 kB, as here.
    let tc = std::process::Command::new("tc")
        .args([
            "qdisc", "change", "dev", "lo", "root", "fq", "quantum", "3028", "initial_quantum",
            "15140", "limit", "65536", "flow_limit", "4096",
        ])
        .status();
    assert!(tc.is_ok_and(|status| status.success()), "tc in the namespace");
    let unpaced = run(0);
    let paced = run(4);
    println!("unpaced: {unpaced:?}\npaced:   {paced:?}");
    // Each child's socket carries about 47 Mbit/s on the wire: 4 times it.
    assert!(paced.rate_bps > 4 * MEDIA_BPS, "{paced:?}");
    // A socket's GSO message (up to 63 kB) leaves whole, so a millisecond
    // may hold one of each of the five sockets and the paced rate's bytes.
    let paced_ms = CHILDREN as u64 * paced.rate_bps / 8 / 1_000;
    assert!(
        paced.burst_bytes_1ms <= CHILDREN as u64 * 64 * 1_024 + paced_ms,
        "a millisecond's burst {} B at a paced rate of {} B/ms",
        paced.burst_bytes_1ms,
        paced_ms
    );
    assert!(
        paced.burst_bytes_1ms * 4 < unpaced.burst_bytes_1ms * 3,
        "unpaced {} B in a millisecond, paced {}",
        unpaced.burst_bytes_1ms,
        paced.burst_bytes_1ms
    );
    // Spread: a part's last copy several ms after the source, not at once.
    assert!(paced.part_p50_ms > 2.0 * unpaced.part_p50_ms, "{paced:?}");
    let interval_ms = 1_000.0 / FPS as f64;
    assert!(
        paced.part_max_ms < interval_ms / 4.0 + 8.0,
        "a paced part's last copy {:.1} ms after the source",
        paced.part_max_ms
    );
    assert!(paced.audio_max_ms < 5.0, "audio held: {paced:?}");
}

/// The send240 flags and their defaults: keeping on (200 ms, a pool of
/// 0.8% of memory), pacing off until a lab A/B, a pull budget of 48 Mbit/s.
#[test]
fn the_send240_flags_default_as_documented() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert_eq!(args.send_defer_ms, 200);
    assert_eq!(args.send_defer_mb, av_mesh::mem_guard::MemSetting::Auto);
    assert_eq!(args.send_pacing, 0);
    assert_eq!(args.send_pacing_floor_mbit, 100);
    assert_eq!(args.edge_pull_budget_mbit, 48);
    // 31 MB on a 3,921 MB node; 8 MB at least, 64 at most.
    assert_eq!(
        fast_plane::send_defer::pool_bytes(None, 3_921 << 20) >> 20,
        31
    );
    assert_eq!(fast_plane::send_defer::pool_bytes(None, 512 << 20), 8 << 20);
    assert_eq!(fast_plane::send_defer::pool_bytes(Some(16), 0), 16 << 20);
    let args = Args::try_parse_from([
        "av-mesh",
        "--send-defer-ms",
        "0",
        "--send-pacing",
        "4",
        "--edge-pull-budget-mbit",
        "0",
    ])
    .unwrap();
    assert_eq!((args.send_defer_ms, args.send_pacing, args.edge_pull_budget_mbit), (0, 4, 0));
    assert!(fast_plane::send_defer::DeferConfig::new(0, 8 << 20).is_none(), "0 is off");
}
