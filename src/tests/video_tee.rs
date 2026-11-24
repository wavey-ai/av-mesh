//! The video tee at 4K rates (fix4-video): a forwarder's video class thread
//! and its tee, fed a 4K stream at 247 or 300 Mbit/s of 1,316-byte symbols,
//! with the tee's processor time, what it dropped and which parts the cache
//! holds.
//!
//! A dropped tee copy is a symbol missing from the node's cache: the part
//! is lost to its LL-HLS readers and cache-mesh children unless repair
//! covers it. The local gate at 247 Mbit/s dropped 9.3k to 13.9k copies at
//! every forwarder in a shared-CPU layout (local-proof-2, F7), more with the
//! class threads real-time.
//!
//! `node_cpus` squeezes the node onto two CPUs as a `g6-dedicated-2` is:
//! the class threads pinned one each (as `--relay-class-cpus auto`), the
//! tee's runtime on both, and `tee_nice` above the class threads (10: the
//! weight of the deploy's video thread at nice -10 against the runtime at
//! 0, which this unprivileged test cannot set the other way round). The
//! source and the children run on the other CPUs: they are other hosts.
//! `hogs` adds threads on the node's CPUs at the runtime's priority that
//! spin `hog_duty` of the time (the other nodes of a shared lab window).
//!
//! The measurements are ignored tests; run them alone, in release:
//! `cargo test --release video_tee -- --ignored --nocapture --test-threads 1`.
//! Env: VT_MBPS (247), VT_SECONDS (20), VT_FPS (24), VT_CHILDREN (4),
//! VT_CPUS (none, or "a,b"), VT_TEE_NICE (0), VT_HOGS (0), VT_HOG_DUTY
//! (0.5).

use super::video_throughput::{
    make_frames_shaped, run_child, send_frames_at, threads_cpu_ns, Frame, FrameShape, Received,
    Spread,
};
use super::*;
use socket2::SockRef;
use std::os::fd::AsRawFd;

/// The deploy's `mesh_symbol_size`.
pub(super) const SYMBOL_BYTES: u16 = 1_316;
const OFFSET: u16 = 1_000;
const STREAM: u64 = 1;
/// The node's `--slot-kb`: a 4K frame of 247 Mbit/s at 24 frames a second
/// (1.29 MB) fits.
const SLOT_KB: usize = 2_048;
/// Parts the test's cache keeps (4 s at 24 frames a second).
const WINDOW_PARTS: usize = 96;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_cpus() -> Option<[usize; 2]> {
    let value = std::env::var("VT_CPUS").ok()?;
    let mut cpus = value.split(',').filter_map(|cpu| cpu.trim().parse().ok());
    Some([cpus.next()?, cpus.next()?])
}

/// One run.
#[derive(Debug, Clone, Copy)]
pub(super) struct TeeOptions {
    /// Media rate, Mbit/s.
    pub(super) mbps: u64,
    pub(super) seconds: u64,
    pub(super) fps: u64,
    /// Compiled children of every role.
    pub(super) children: usize,
    pub(super) node_cpus: Option<[usize; 2]>,
    pub(super) tee_nice: i32,
    pub(super) hogs: usize,
    pub(super) hog_duty: f64,
    /// The video tee's extra processor time per datagram (a tee short of
    /// processor time), ns.
    pub(super) tee_spin_ns: u64,
}

impl TeeOptions {
    pub(super) fn from_env() -> Self {
        Self {
            mbps: env_u64("VT_MBPS", 247),
            seconds: env_u64("VT_SECONDS", 20),
            fps: env_u64("VT_FPS", 24),
            children: env_u64("VT_CHILDREN", 4) as usize,
            node_cpus: env_cpus(),
            tee_nice: env_u64("VT_TEE_NICE", 0) as i32,
            hogs: env_u64("VT_HOGS", 0) as usize,
            hog_duty: env_f64("VT_HOG_DUTY", 0.5),
            tee_spin_ns: env_u64("VT_TEE_SPIN_NS", 0),
        }
    }
}

/// What one run measured.
#[derive(Debug)]
pub(super) struct TeeRun {
    pub(super) options: TeeOptions,
    /// Datagrams and Mbit/s offered (on the wire, with IP and UDP headers),
    /// over the measured window.
    pub(super) offered_per_second: f64,
    pub(super) offered_mbps: f64,
    pub(super) parts: usize,
    /// Parts whose whole object the cache held when the checker looked.
    pub(super) parts_committed: usize,
    pub(super) decoded_objects: u64,
    pub(super) video_received: u64,
    pub(super) tee_dropped: u64,
    pub(super) kernel_dropped: u64,
    /// Children's frames complete, of all frames times children.
    pub(super) children_whole: u64,
    pub(super) children_expected: u64,
    /// Shares of one CPU over the measured window: the tee's runtime and
    /// the class threads.
    pub(super) tee_cpu: f64,
    pub(super) thread_cpu: f64,
    /// The tee's processor time per datagram it took, µs.
    pub(super) tee_us_per_datagram: f64,
    /// The video tee's own counters (`/api/mesh` fast_plane).
    pub(super) tee_batches: u64,
    pub(super) tee_lag_max_us: u64,
    pub(super) parts_shed: u64,
    pub(super) grow_lost: u64,
}

impl TeeRun {
    pub(super) fn line(&self) -> String {
        let o = &self.options;
        format!(
            "4K {} Mbit/s media at {} fps, {} B symbols, {} children, cpus {:?}, tee nice +{}, \
             hogs {}x{:.0}%, tee spin {} ns: offered {:.0} datagrams/s ({:.1} Mbit/s on the wire); video \
             received {}, tee_dropped {}, kernel_dropped {}, grow_lost {}; parts {} committed \
             {} (decoded {}, shed {}); children whole {}/{}; tee {:.1} % of a CPU ({:.2} µs a datagram, \
             sustainable {:.0} datagrams/s = {:.0} Mbit/s on one CPU), batches {}, lag max {} \
             µs; class threads {:.1} %",
            o.mbps,
            o.fps,
            SYMBOL_BYTES,
            o.children,
            o.node_cpus,
            o.tee_nice,
            o.hogs,
            o.hog_duty * 100.0,
            o.tee_spin_ns,
            self.offered_per_second,
            self.offered_mbps,
            self.video_received,
            self.tee_dropped,
            self.kernel_dropped,
            self.grow_lost,
            self.parts,
            self.parts_committed,
            self.decoded_objects,
            self.parts_shed,
            self.children_whole,
            self.children_expected,
            self.tee_cpu * 100.0,
            self.tee_us_per_datagram,
            1e6 / self.tee_us_per_datagram,
            1e6 / self.tee_us_per_datagram * self.offered_mbps / self.offered_per_second,
            self.tee_batches,
            self.tee_lag_max_us,
            self.thread_cpu * 100.0,
        )
    }

    pub(super) fn whole(&self) -> bool {
        self.tee_dropped == 0
            && self.parts_committed == self.parts
            && self.children_whole == self.children_expected
    }
}

/// Pin the calling thread to `cpus`.
fn pin_to(cpus: &[usize]) {
    // SAFETY: a zeroed cpu_set_t is the empty set; CPU_SET writes within it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for cpu in cpus {
            libc::CPU_SET(*cpu, &mut set);
        }
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

/// The CPUs of this process but `node`.
fn other_cpus(node: [usize; 2]) -> Vec<usize> {
    let count = std::thread::available_parallelism().map_or(2, |count| count.get());
    (0..count).filter(|cpu| !node.contains(cpu)).collect()
}

/// Make the threads whose names start with `prefix` nicer by `nice`.
fn renice(prefix: &str, nice: i32) {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return;
    };
    for task in tasks.flatten() {
        let Ok(comm) = std::fs::read_to_string(task.path().join("comm")) else {
            continue;
        };
        if !comm.starts_with(prefix) {
            continue;
        }
        if let Some(tid) = task.file_name().to_str().and_then(|tid| tid.parse::<u32>().ok()) {
            // SAFETY: setpriority with integer arguments.
            unsafe { libc::setpriority(libc::PRIO_PROCESS, tid, nice) };
        }
    }
}

/// Feed a forwarder `options.mbps` of 4K for a second of warm-up and
/// `options.seconds` measured, and read what its tee did.
pub(super) fn tee_4k(options: TeeOptions) -> TeeRun {
    fast_plane::relay::TEE_PARTS_OFF.store(
        std::env::var_os("VT_SYMBOL_BY_SYMBOL").is_some(),
        Ordering::Relaxed,
    );
    if std::env::var_os("VT_PTRACER").is_some() {
        // SAFETY: prctl with integer arguments only (eu-stack may sample).
        unsafe { libc::prctl(libc::PR_SET_PTRACER, libc::PR_SET_PTRACER_ANY, 0, 0, 0) };
    }
    let shape = FrameShape {
        fps: options.fps,
        symbol_bytes: SYMBOL_BYTES,
        repair_per_sources: 20,
    };
    let bits_per_second = options.mbps * 1_000_000;
    let warmup = options.fps as usize;
    let measured = (options.seconds * options.fps) as usize;
    let frames = make_frames_shaped(STREAM, 0, warmup + measured, bits_per_second, shape);
    let expected: Arc<HashMap<u64, usize>> = Arc::new(
        frames
            .iter()
            .map(|frame| (frame.part, frame.datagrams.len()))
            .collect(),
    );
    let node_cpus = options.node_cpus;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("vtee-rt")
        .on_thread_start(move || {
            if let Some(cpus) = node_cpus {
                pin_to(&cpus);
            }
        })
        .enable_all()
        .build()
        .unwrap();
    let (relay, sockets) = rt.block_on(async {
        tokio::spawn(async move {
            let mut sockets = Vec::new();
            for _ in 0..options.children {
                let (audio, video) = bind_class_pair(OFFSET).await;
                let _ = SockRef::from(&video).set_recv_buffer_size(32 << 20);
                sockets.push((audio, video));
            }
            let endpoints: Vec<RelayForwardEndpoint> = sockets
                .iter()
                .map(|(audio, _)| RelayForwardEndpoint {
                    bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                    target: audio.local_addr().unwrap(),
                    role: RelayForwardRole::All,
                })
                .collect();
            let cache = LiveTsCache::new(
                STREAM,
                Duration::from_micros(1_000_000 / options.fps),
                2,
                WINDOW_PARTS,
                SLOT_KB,
            )
            .await;
            let relay = start_fast_relay_in(
                ParentPath::PromotedSecondary,
                &endpoints,
                fast_plane::relay::RelayClassConfig {
                    // Every symbol to the children, as before 4k-repair's trim: these
                    // measure forwarding and the tee, and count each repair symbol.
                    trim_forwarded_repair: false,
                    port_offset: OFFSET,
                    cpus: node_cpus.map_or([None, None], |cpus| [Some(cpus[0]), Some(cpus[1])]),
                    realtime: false,
                    test_video_tee_spin_ns: options.tee_spin_ns,
                    ..Default::default()
                },
                cache,
            )
            .await;
            if let Some(forwarder) = &relay.forwarder {
                forwarder.configure_buffers(32 << 20);
            }
            (relay, sockets)
        })
        .await
        .unwrap()
    });
    if options.tee_nice > 0 {
        renice("vtee-rt", options.tee_nice);
        renice("needletail-tee", options.tee_nice);
    }
    let away = node_cpus.map(other_cpus);
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let mut keep_audio = Vec::new();
    let readers: Vec<_> = sockets
        .into_iter()
        .enumerate()
        .map(|(index, (audio, video))| {
            keep_audio.push(audio);
            let video = video.into_std().unwrap();
            let (expected, stop) = (Arc::clone(&expected), Arc::clone(&stop));
            let measuring = Arc::clone(&measuring);
            let away = away.clone();
            std::thread::Builder::new()
                .name(format!("child-{index}"))
                .spawn(move || {
                    if let Some(cpus) = away {
                        pin_to(&cpus);
                    }
                    run_child(video, expected, stop, measuring)
                })
                .unwrap()
        })
        .collect();
    // Other work on the node's CPUs at the runtime's priority.
    let hogs: Vec<_> = (0..node_cpus.map_or(0, |_| options.hogs))
        .map(|index| {
            let (stop, cpus) = (Arc::clone(&stop), node_cpus.unwrap());
            let duty = options.hog_duty.clamp(0.0, 1.0);
            std::thread::Builder::new()
                .name(format!("hog-{index}"))
                .spawn(move || {
                    pin_to(&cpus);
                    let period = Duration::from_millis(10);
                    while !stop.load(Ordering::Relaxed) {
                        let started = Instant::now();
                        while started.elapsed() < period.mul_f64(duty) {
                            std::hint::spin_loop();
                        }
                        std::thread::sleep(period.mul_f64(1.0 - duty));
                    }
                })
                .unwrap()
        })
        .collect();
    // The parts the cache holds, looked at every 50 ms from another runtime
    // (an LL-HLS reader's view): every part not yet seen that the cache
    // may still hold (its window behind the source's last).
    let sent = Arc::new(AtomicU64::new(0));
    let checker = {
        let (cache, sent, stop) = (
            Arc::clone(&relay.cache),
            Arc::clone(&sent),
            Arc::clone(&stop),
        );
        let away = away.clone();
        let total = frames.len() as u64;
        std::thread::spawn(move || {
            if let Some(cpus) = away {
                pin_to(&cpus);
            }
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let mut seen = vec![false; total as usize];
                let mut last_pass = false;
                loop {
                    let done = stop.load(Ordering::Relaxed);
                    let upto = sent.load(Ordering::Acquire).min(total);
                    let from = upto.saturating_sub(WINDOW_PARTS as u64 - 8);
                    for part in from..upto {
                        if !seen[part as usize]
                            && cache.get_part_for_stream_id(STREAM, part).await.is_some()
                        {
                            seen[part as usize] = true;
                        }
                    }
                    if last_pass {
                        return seen;
                    }
                    last_pass = done;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
        })
    };
    let fd = relay.secondary_sender.as_raw_fd();
    let _ = SockRef::from(&relay.secondary_sender).set_send_buffer_size(32 << 20);
    let target = relay.secondary_video.unwrap();
    let start = Instant::now() + Duration::from_millis(50);
    let node_threads = ["needletail-fast"];
    let tee_threads = ["vtee-rt", "needletail-tee"];
    let interval = Duration::from_nanos(1_000_000_000 / options.fps);
    let (before, after) = std::thread::scope(|scope| {
        let frames = &frames;
        let sent = &sent;
        let away = away.clone();
        let source = scope.spawn(move || {
            if let Some(cpus) = away {
                pin_to(&cpus);
            }
            // Frame by frame, so the checker knows how far the source is.
            for (index, frame) in frames.iter().enumerate() {
                send_frames_at(
                    fd,
                    target,
                    std::slice::from_ref(frame),
                    bits_per_second,
                    Spread::Paced,
                    start + interval * index as u32,
                    options.fps,
                    SYMBOL_BYTES,
                );
                sent.store(index as u64 + 1, Ordering::Release);
            }
        });
        let from = start + interval * warmup as u32;
        std::thread::sleep(from.saturating_duration_since(Instant::now()));
        measuring.store(true, Ordering::Relaxed);
        let stats = relay.stats();
        let before = (
            threads_cpu_ns(&node_threads),
            threads_cpu_ns(&tee_threads),
            Instant::now(),
            stats.video_received,
        );
        std::thread::sleep(Duration::from_secs(options.seconds));
        let stats = relay.stats();
        let after = (
            threads_cpu_ns(&node_threads),
            threads_cpu_ns(&tee_threads),
            Instant::now(),
            stats.video_received,
        );
        measuring.store(false, Ordering::Relaxed);
        source.join().unwrap();
        (before, after)
    });
    // The tee's last parts: until the cache's decoded count stops moving.
    let mut decoded = 0;
    for _ in 0..100 {
        std::thread::sleep(Duration::from_millis(50));
        let now = relay.cache.relay_ingress_snapshot().decoded_objects;
        if now == decoded && now >= frames.len() as u64 {
            break;
        }
        decoded = now;
    }
    stop.store(true, Ordering::Relaxed);
    let seen = checker.join().unwrap();
    for hog in hogs {
        hog.join().unwrap();
    }
    let received: Vec<Received> = readers
        .into_iter()
        .map(|reader| reader.join().unwrap())
        .collect();
    let stats = relay.stats();
    let ingress = relay.cache.relay_ingress_snapshot();
    let elapsed = after.2.duration_since(before.2).as_secs_f64();
    let window = warmup..warmup + measured;
    let window_seconds = measured as f64 / options.fps as f64;
    let wire_bytes: u64 = frames[window.clone()].iter().map(Frame::wire_bytes).sum();
    let wire_datagrams: usize = frames[window]
        .iter()
        .map(|frame| frame.datagrams.len())
        .sum();
    let teed = after.3.saturating_sub(before.3).max(1);
    let tee_ns = after.1.saturating_sub(before.1);
    let children_whole = received
        .iter()
        .map(|received| {
            frames
                .iter()
                .filter(|frame| received.complete.contains_key(&frame.part))
                .count() as u64
        })
        .sum();
    let run = TeeRun {
        options,
        offered_per_second: wire_datagrams as f64 / window_seconds,
        offered_mbps: wire_bytes as f64 * 8.0 / window_seconds / 1e6,
        parts: frames.len(),
        parts_committed: seen.iter().filter(|seen| **seen).count(),
        decoded_objects: ingress.decoded_objects,
        video_received: stats.video_received,
        tee_dropped: stats.tee_dropped,
        kernel_dropped: stats.kernel_dropped,
        children_whole,
        children_expected: (frames.len() * options.children) as u64,
        tee_cpu: tee_ns as f64 / 1e9 / elapsed,
        thread_cpu: after.0.saturating_sub(before.0) as f64 / 1e9 / elapsed,
        tee_us_per_datagram: tee_ns as f64 / 1e3 / teed as f64,
        tee_batches: stats.video_tee_batches,
        tee_lag_max_us: stats.video_tee_lag_max_us,
        parts_shed: stats.video_tee_parts_shed,
        grow_lost: stats.tee_dropped_growing,
    };
    rt.block_on(relay.stop());
    drop(keep_audio);
    run
}

#[test]
#[ignore = "a measurement: run it alone, in release"]
fn video_tee_at_4k_rates() {
    let run = tee_4k(TeeOptions::from_env());
    println!("{}", run.line());
}

/// Where the video tee's time goes per datagram, at the deploy's symbol
/// and frame rate: `RelayUdpDispatch::push` of each symbol (with its
/// allocations), the decoding push and the commit, on one thread.
#[tokio::test(flavor = "current_thread")]
#[ignore = "a measurement: run it alone, in release"]
async fn video_tee_dispatch_cost() {
    let mbps = env_u64("VT_MBPS", 247);
    let fps = env_u64("VT_FPS", 24);
    let count = env_u64("VT_FRAMES", 240) as usize;
    let shape = FrameShape {
        fps,
        symbol_bytes: SYMBOL_BYTES,
        repair_per_sources: 20,
    };
    let frames = make_frames_shaped(STREAM, 0, count, mbps * 1_000_000, shape);
    let peer: SocketAddr = "127.0.0.1:40002".parse().unwrap();
    let mut dispatch = fast_test_dispatch(
        "127.0.0.1:40001".parse().unwrap(),
        peer,
        ParentPath::PromotedSecondary,
    );
    let cache = LiveTsCache::new(STREAM, Duration::from_millis(42), 2, WINDOW_PARTS, SLOT_KB).await;
    let (mut push, mut decode, mut commit) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let (mut datagrams, mut allocations) = (0_u64, 0_u64);
    for frame in &frames {
        let now_us = now_unix_us();
        for datagram in &frame.datagrams {
            let allocated = crate::alloc_count::allocations();
            let started = Instant::now();
            let outcome = dispatch.push(peer, datagram, now_us).unwrap();
            let spent = started.elapsed();
            datagrams += 1;
            if let RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Decoded {
                object,
                envelope,
                ..
            }) = outcome
            {
                decode += spent;
                let started = Instant::now();
                commit_relay_object(&cache, *object, envelope)
                    .await
                    .unwrap();
                commit += started.elapsed();
            } else {
                push += spent;
                allocations += crate::alloc_count::allocations() - allocated;
            }
        }
    }
    let frames_n = frames.len() as f64;
    let per_datagram = |total: Duration| total.as_secs_f64() * 1e6 / datagrams as f64;
    println!(
        "4K {mbps} Mbit/s at {fps} fps, {} datagrams a frame: push {:.2} µs a datagram ({:.1} \
         allocations), the decoding push {:.0} µs a frame, commit {:.0} µs a frame; all {:.2} µs \
         a datagram",
        datagrams / frames.len() as u64,
        push.as_secs_f64() * 1e6 / (datagrams as f64 - frames_n),
        allocations as f64 / (datagrams as f64 - frames_n),
        decode.as_secs_f64() * 1e6 / frames_n,
        commit.as_secs_f64() * 1e6 / frames_n,
        per_datagram(push + decode + commit),
    );
}

/// The proof of fix4-video's task 1: 20 s of 247 Mbit/s of 4K (1,316-byte
/// symbols, 24 frames a second, a twentieth of repair: 25,600 datagrams a
/// second, 297 Mbit/s on the wire) through a forwarder's video class
/// thread and its tee. The tee drops nothing, the cache holds every part,
/// every child has every frame, and the tee's processor time is reported.
#[test]
fn the_video_tee_takes_20_s_of_247_mbit_s_whole() {
    let run = tee_4k(TeeOptions {
        mbps: 247,
        seconds: 20,
        fps: 24,
        children: 2,
        node_cpus: None,
        tee_nice: 0,
        hogs: 0,
        hog_duty: 0.0,
        tee_spin_ns: 0,
    });
    println!("{}", run.line());
    assert_eq!(run.tee_dropped, 0, "{}", run.line());
    assert_eq!(run.parts_shed, 0, "{}", run.line());
    assert_eq!(run.parts_committed, run.parts, "{}", run.line());
    assert_eq!(run.children_whole, run.children_expected, "{}", run.line());
    assert!(run.tee_cpu > 0.0 && run.tee_cpu < 1.0, "{}", run.line());
}

/// A tee short of processor time (45 µs more a datagram: it takes about
/// 18,000 of 25,600 a second) loses whole parts, about its shortfall, and
/// every part it takes is committed: before, the full ring took symbols
/// as entries freed and spoiled the parts of the whole episode (local, 20 s:
/// 207 of 504 parts decoded symbol by symbol, 414 whole).
#[test]
fn a_tee_short_of_processor_time_loses_whole_parts_not_all_of_them() {
    let run = tee_4k(TeeOptions {
        mbps: 247,
        seconds: 10,
        fps: 24,
        children: 1,
        node_cpus: None,
        tee_nice: 0,
        hogs: 0,
        hog_duty: 0.0,
        tee_spin_ns: 45_000,
    });
    println!("{}", run.line());
    assert!(run.parts_shed > 0, "the tee fell behind: {}", run.line());
    // Whole or nothing: every part either shed or committed (a part teed
    // whole is decoded), give or take the parts the ring refused between
    // a verdict and the next part's.
    let accounted = run.decoded_objects + run.parts_shed;
    assert!(
        accounted + 4 >= run.parts as u64,
        "parts neither committed nor shed: {}",
        run.line()
    );
    assert!(
        run.decoded_objects as usize >= run.parts * 6 / 10,
        "{}",
        run.line()
    );
    assert_eq!(run.children_whole, run.children_expected, "{}", run.line());
}
