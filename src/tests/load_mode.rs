//! The load mode (`needletail-load`, `av_mesh::load`) against an edge in
//! this process: an edge fed by the fast plane (two forwarders' RelaySession
//! symbols, the fast client lane and the compact lane, as `compact_lane`
//! starts it), a stream of 1 ms PCM (or 2.5 ms Opus) parts published at
//! their pace, and the load's simulated listeners, each its own socket and
//! subscription.
//!
//! `load_mode_counts_every_part_of_every_listener` checks that the load's
//! listeners get every part and that its intervals, summary and samples say
//! so. `load_mode_cost_per_listener` (ignored, a measurement) runs 1,000 to
//! 2,000 listeners and prints the processor time of the load's workers and
//! of the edge per listener, and the listeners that a g6-dedicated-4 load
//! host drives at that cost. Run it alone:
//! `cargo test --release load_mode_cost -- --ignored --nocapture`, with
//! LOAD_BENCH_LISTENERS (1000), LOAD_BENCH_SECONDS (12), LOAD_BENCH_THREADS
//! (4), LOAD_BENCH_EDGE_THREADS (4), LOAD_BENCH_PAYLOAD (pcm or opus) and
//! LOAD_BENCH_COALESCE_US (0: the workers read their sockets at once) and
//! LOAD_BENCH_FEED (fast, or mesh: an edge fed by two distributors over
//! the cache mesh).

use super::compact_lane::{publish_paced, AnyEdge, MeshEdge, Payload};
use super::fast_lane::LaneEdge;
use super::fast_lease::loopback;
use super::*;
use av_mesh::client_session::SessionConfig;
use av_mesh::load::{run, LoadConfig};
use serde_json::Value;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Processor time, ns, of this process's threads whose names start with one
/// of `prefixes`.
fn threads_cpu_ns(prefixes: &[&str]) -> u64 {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return 0;
    };
    let mut total = 0;
    for task in tasks.flatten() {
        let path = task.path();
        let Ok(comm) = std::fs::read_to_string(path.join("comm")) else {
            continue;
        };
        if !prefixes.iter().any(|prefix| comm.starts_with(prefix)) {
            continue;
        }
        if let Some(ns) = std::fs::read_to_string(path.join("schedstat"))
            .ok()
            .and_then(|stat| stat.split_whitespace().next()?.parse::<u64>().ok())
        {
            total += ns;
        }
    }
    total
}

/// The host's softirq and busy jiffies (all CPUs), from /proc/stat.
fn host_jiffies() -> (u64, u64, u64) {
    let text = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let values: Vec<u64> = text
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .skip(1)
        .filter_map(|value| value.parse().ok())
        .collect();
    let total: u64 = values.iter().take(8).sum();
    let idle = values.get(3).copied().unwrap_or(0) + values.get(4).copied().unwrap_or(0);
    (values.get(6).copied().unwrap_or(0), total - idle, total)
}

const EDGE_THREADS: [&str; 3] = ["edge-rt", "needletail-fast", "needletail-cut"];
const LOAD_THREADS: &str = "nt-load";

struct Bench {
    listeners: usize,
    seconds: u64,
    threads: usize,
    edge_threads: usize,
    payload: Payload,
    sample: usize,
    out_dir: PathBuf,
    /// Second of the run that the processor time is measured from.
    measure_from: f64,
}

struct BenchResult {
    summary: Value,
    /// Processor time over the measured window, in CPUs (1.0 = one CPU).
    load_cpu: f64,
    edge_cpu: f64,
    softirq_cpu: f64,
    host_busy_cpu: f64,
    window_s: f64,
    /// Media datagrams the listeners received in the window, a second.
    datagrams_per_second: f64,
    /// The busiest threads over the window: name and CPUs.
    threads: Vec<(String, f64)>,
    /// The edge's UDP counters at the end.
    edge_udp: Value,
}

/// CPU time, ns, of each thread of this process, by thread id.
fn each_thread_cpu_ns() -> BTreeMap<u64, (String, u64)> {
    let mut threads = BTreeMap::new();
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return threads;
    };
    for task in tasks.flatten() {
        let path = task.path();
        let Some(tid) = path
            .file_name()
            .and_then(|name| name.to_str()?.parse::<u64>().ok())
        else {
            continue;
        };
        let comm = std::fs::read_to_string(path.join("comm")).unwrap_or_default();
        if let Some(ns) = std::fs::read_to_string(path.join("schedstat"))
            .ok()
            .and_then(|stat| stat.split_whitespace().next()?.parse::<u64>().ok())
        {
            threads.insert(tid, (comm.trim_end().to_owned(), ns));
        }
    }
    threads
}

fn runtime(name: &str, workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name(name)
        .enable_all()
        .build()
        .unwrap()
}

fn run_bench(bench: &Bench, ip: u8) -> BenchResult {
    let edge_rt = runtime("edge-rt", bench.edge_threads);
    let pub_rt = runtime("pub-rt", 1);
    let stream_id = 4001;
    let payload = bench.payload;
    let capacity = bench.listeners + 64;
    let mesh = std::env::var("LOAD_BENCH_FEED").as_deref() == Ok("mesh");
    let (edge, first) = edge_rt
        .block_on(async move {
            tokio::spawn(async move {
                if mesh {
                    // Fed by two distributors over the cache mesh (up to
                    // 4,096 UDP clients).
                    let (edge, next) = MeshEdge::start(payload, stream_id, true).await;
                    return (AnyEdge::Mesh(edge), next);
                }
                let edge =
                    LaneEdge::start_with_capacity(loopback(ip), Duration::ZERO, capacity, true).await;
                // The stream is in the cache before anyone subscribes.
                let (envelope, _) = payload.envelope(stream_id, 0);
                edge.send(false, &payload.symbols(&envelope), false).await;
                wait_for_part(&edge.cache, stream_id, 0).await;
                (AnyEdge::Fast(edge), 1)
            })
            .await
        })
        .unwrap();
    let edge = Arc::new(edge);
    // The stream runs through the load's run and its drain.
    let per_second = 1_000_000 / payload.part_us();
    let last = first + per_second * (bench.seconds + 4);
    let publisher = {
        let edge = Arc::clone(&edge);
        pub_rt.spawn(async move { publish_paced(&edge, payload, stream_id, first, last).await })
    };
    std::thread::sleep(Duration::from_millis(300));

    let mut config = LoadConfig::new(
        SessionConfig::new(stream_id),
        vec![edge.client_port()],
        bench.listeners,
    );
    config.duration = Duration::from_secs(bench.seconds);
    config.threads = bench.threads;
    config.start_rate = 4_000.0;
    config.sample = bench.sample;
    config.out_dir = Some(bench.out_dir.clone());
    config.thread_name = LOAD_THREADS.into();
    config.progress = std::env::var_os("LOAD_BENCH_PROGRESS").is_some();
    config.coalesce = Duration::from_micros(env_u64("LOAD_BENCH_COALESCE_US", 0));
    let started = Instant::now();
    let load = std::thread::spawn(move || run(config, &mut |_| {}).unwrap());

    std::thread::sleep(Duration::from_secs_f64(bench.measure_from));
    let load_before = threads_cpu_ns(&[LOAD_THREADS]);
    let edge_before = threads_cpu_ns(&EDGE_THREADS);
    let host_before = host_jiffies();
    let each_before = each_thread_cpu_ns();
    let window_start = Instant::now();
    let window_end = started + Duration::from_secs(bench.seconds) - Duration::from_millis(200);
    std::thread::sleep(window_end.saturating_duration_since(Instant::now()));
    let window = window_start.elapsed().as_secs_f64();
    let load_cpu = (threads_cpu_ns(&[LOAD_THREADS]) - load_before) as f64 / 1e9 / window;
    let edge_cpu = (threads_cpu_ns(&EDGE_THREADS) - edge_before) as f64 / 1e9 / window;
    let host_after = host_jiffies();
    let mut threads: Vec<(String, f64)> = each_thread_cpu_ns()
        .into_iter()
        .map(|(tid, (name, ns))| {
            let before = each_before.get(&tid).map_or(0, |(_, ns)| *ns);
            (format!("{name}/{tid}"), (ns - before) as f64 / 1e9 / window)
        })
        .filter(|(_, cpu)| *cpu > 0.02)
        .collect();
    threads.sort_by(|a, b| b.1.total_cmp(&a.1));
    threads.truncate(16);
    let window_from = window_start.duration_since(started).as_secs_f64();
    let window_to = window_end.duration_since(started).as_secs_f64();
    // SAFETY: plain sysconf.
    let ticks = (unsafe { libc::sysconf(libc::_SC_CLK_TCK) }).max(1) as f64;
    let softirq_cpu = (host_after.0 - host_before.0) as f64 / ticks / window;
    let host_busy_cpu = (host_after.1 - host_before.1) as f64 / ticks / window;

    let summary = load.join().unwrap();
    // The datagrams of the interval lines that lie in the window.
    let intervals = std::fs::read_to_string(bench.out_dir.join("intervals.jsonl")).unwrap();
    let (mut datagrams, mut seconds) = (0_u64, 0.0_f64);
    for line in intervals.lines() {
        let line: Value = serde_json::from_str(line).unwrap();
        let t = line["t"].as_f64().unwrap_or(0.0);
        if line["phase"] == "run" && t - 1.0 >= window_from - 0.5 && t <= window_to + 0.5 {
            datagrams += line["total"]["datagrams"].as_u64().unwrap_or(0);
            seconds += 1.0;
        }
    }
    pub_rt.block_on(async {
        let mut publisher = publisher;
        // A publisher still pacing (a debug build lags its clock) holds the
        // edge: end it, so that the edge can be stopped below.
        if tokio::time::timeout(Duration::from_secs(10), &mut publisher)
            .await
            .is_err()
        {
            publisher.abort();
            let _ = publisher.await;
        }
    });
    let edge_udp = {
        let stats = edge.stats();
        let get = |counter: &std::sync::atomic::AtomicU64| counter.load(Ordering::Relaxed);
        serde_json::json!({
            "subscriptions": get(&stats.subscriptions),
            "refusals": get(&stats.refusals),
            "moves": get(&stats.moves),
            "datagrams": get(&stats.datagrams),
            "send_errors": get(&stats.send_errors),
            "would_block": get(&stats.would_block),
            "send_queued": get(&stats.send_queued),
            "send_queue_drops": get(&stats.send_queue_drops),
            "playout_skipped_parts": get(&stats.playout_skipped_parts),
            "holes_past_playout": get(&stats.holes_past_playout),
            "compact_parts": get(&stats.compact_parts),
            "nack_parts": get(&stats.nack_parts),
            "nack_resent": get(&stats.nack_resent),
            "nack_refused": get(&stats.nack_refused),
            "gaps": get(&stats.gaps),
        })
    };
    let edge = Arc::try_unwrap(edge).ok().expect("the edge is no longer shared");
    edge_rt.block_on(edge.stop());
    BenchResult {
        summary,
        load_cpu,
        edge_cpu,
        softirq_cpu,
        host_busy_cpu,
        window_s: window,
        datagrams_per_second: datagrams as f64 / seconds.max(1.0),
        threads,
        edge_udp,
    }
}

fn out_dir(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("load-bench")
        .join(name)
}

#[test]
fn load_mode_counts_every_part_of_every_listener() {
    let dir = out_dir("check");
    let _ = std::fs::remove_dir_all(&dir);
    let bench = Bench {
        listeners: 20,
        seconds: 4,
        threads: 2,
        edge_threads: 2,
        payload: Payload::Pcm,
        sample: 3,
        out_dir: dir.clone(),
        measure_from: 1.5,
    };
    let result = run_bench(&bench, 41);
    let summary = &result.summary;
    let total = &summary["total"];
    assert_eq!(total["listeners_started"], 20, "{summary:#}");
    assert_eq!(summary["peak_subscribed"], 20, "{summary:#}");
    assert_eq!(summary["listeners_without_parts"], 0, "{summary:#}");
    assert_eq!(total["start_errors"], 0);
    assert_eq!(total["missing"], 0, "no part missing: {summary:#}");
    assert_eq!(total["decode_errors"], 0);
    assert_eq!(total["truncated"], 0);
    // About 4 s of 1 ms parts, each listener (the first ones are subscribed
    // a little after the start).
    let parts = total["parts"].as_u64().unwrap();
    assert!(parts >= 20 * 3_000, "parts {parts}: {summary:#}");
    let latency = &total["latency_ms"];
    assert_eq!(latency["count"].as_u64(), Some(parts));
    assert!(latency["p50"].as_f64().unwrap() > 0.0);
    let buckets = latency["buckets_us"].as_array().unwrap();
    assert!(!buckets.is_empty());
    let bucketed: u64 = buckets.iter().map(|pair| pair[1].as_u64().unwrap()).sum();
    assert_eq!(bucketed, parts, "the buckets hold every part");
    assert_eq!(total["ttfp_ms"]["count"], 20);
    // Each listener's OK, no later than its first part (wave 7 item 8: the
    // first part comes with the OK).
    assert_eq!(total["ttok_ms"]["count"], 20, "{summary:#}");
    assert!(
        total["ttok_ms"]["p50"].as_f64().unwrap() <= total["ttfp_ms"]["p50"].as_f64().unwrap(),
        "{summary:#}"
    );
    // The wave 7 lanes: the compact lane in the short form, never given up.
    assert_eq!(total["compact_fallbacks"], 0, "{summary:#}");
    assert_eq!(total["short_header_dropped"], 0, "{summary:#}");
    assert_eq!(total["listeners_with_parts"], 20);
    assert_eq!(summary["per_edge"].as_object().unwrap().len(), 1);
    // One line an interval, and the final one after the drain.
    let intervals = std::fs::read_to_string(dir.join("intervals.jsonl")).unwrap();
    let lines: Vec<Value> = intervals
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(lines.len() >= 5, "{} lines", lines.len());
    assert_eq!(lines.last().unwrap()["phase"], "final");
    let summed: u64 = lines
        .iter()
        .map(|line| line["total"]["parts"].as_u64().unwrap())
        .sum();
    assert_eq!(summed, parts, "the intervals add up to the summary");
    assert!(lines
        .iter()
        .any(|line| line["total"]["up"].as_u64() == Some(20)));
    assert!(lines[1]["load_host"]["process_cpu_cores"].as_f64().is_some());
    assert!(lines[1]["load_host_saturated"].is_boolean());
    // The samples: needletail-udp-client --arrivals columns, every part.
    for id in 0..3 {
        let csv = std::fs::read_to_string(
            dir.join("arrivals").join(format!("listener-{id:05}.csv")),
        )
        .unwrap();
        let mut rows = csv.lines();
        assert_eq!(
            rows.next(),
            Some("part,arrival_unix_us,bytes,published_unix_us,available_unix_us")
        );
        let rows: Vec<Vec<u64>> = rows
            .map(|row| row.split(',').map(|v| v.parse().unwrap()).collect())
            .collect();
        assert!(rows.len() >= 3_000, "listener {id}: {} rows", rows.len());
        for row in &rows {
            assert_eq!(row[2], 16 + 288, "a PCM part's bytes");
            assert!(row[1] >= row[3], "arrival after publication");
        }
    }
    assert_eq!(summary["samples"].as_array().unwrap().len(), 3);
    assert!(std::fs::metadata(dir.join("summary.json")).is_ok());
}

#[test]
#[ignore = "a measurement: run it alone, in release"]
fn load_mode_cost_per_listener() {
    let listeners = env_u64("LOAD_BENCH_LISTENERS", 1_000) as usize;
    let seconds = env_u64("LOAD_BENCH_SECONDS", 12);
    let threads = env_u64("LOAD_BENCH_THREADS", 4) as usize;
    let edge_threads = env_u64("LOAD_BENCH_EDGE_THREADS", 4) as usize;
    let payload = match std::env::var("LOAD_BENCH_PAYLOAD").as_deref() {
        Ok("opus") => Payload::Opus,
        _ => Payload::Pcm,
    };
    let name = format!(
        "cost-{}-{listeners}x{threads}",
        if payload == Payload::Pcm { "pcm" } else { "opus" }
    );
    let dir = out_dir(&name);
    let _ = std::fs::remove_dir_all(&dir);
    let bench = Bench {
        listeners,
        seconds,
        threads,
        edge_threads,
        payload,
        sample: 10,
        out_dir: dir.clone(),
        // Started (4,000 a second) and settled.
        measure_from: listeners as f64 / 4_000.0 + 2.5,
    };
    let result = run_bench(&bench, 42);
    let summary = &result.summary;
    let total = &summary["total"];
    let parts_per_second = 1e6 / payload.part_us() as f64;
    // The cost of each datagram that the listeners actually received: an
    // edge in this process that cannot send every listener every part
    // sends fewer, and the cost a part is what a listener costs.
    let delivered = result.datagrams_per_second.max(1.0);
    let load_us_per_part = result.load_cpu * 1e6 / delivered;
    let edge_us_per_part = result.edge_cpu * 1e6 / delivered;
    let softirq_us_per_part = result.softirq_cpu * 1e6 / delivered;
    // A g6-dedicated-4: four vCPUs, kept at 80 %, for the workers and the
    // kernel's receive path (softirq) of their datagrams.
    let per_host = |us_per_part: f64| (4.0 * 0.8 * 1e6 / (us_per_part * parts_per_second)).floor();
    let line = serde_json::json!({
        "payload": payload.name(),
        "listeners": listeners,
        "load_threads": threads,
        "coalesce_us": env_u64("LOAD_BENCH_COALESCE_US", 0),
        "feed": std::env::var("LOAD_BENCH_FEED").unwrap_or_else(|_| "fast".into()),
        "edge_threads": edge_threads,
        "window_s": result.window_s,
        "load_cpu": result.load_cpu,
        "load_cpu_percent_per_listener": result.load_cpu * 100.0 / listeners as f64,
        "load_us_per_listener_part": load_us_per_part,
        "edge_cpu": result.edge_cpu,
        "edge_cpu_percent_per_listener": result.edge_cpu * 100.0 / listeners as f64,
        "edge_us_per_listener_part": edge_us_per_part,
        "host_softirq_cpu": result.softirq_cpu,
        "host_busy_cpu": result.host_busy_cpu,
        "datagrams_received_per_second": result.datagrams_per_second,
        "delivered_fraction": result.datagrams_per_second / (listeners as f64 * parts_per_second),
        "softirq_us_per_datagram_host_wide": softirq_us_per_part,
        "listeners_per_g6_dedicated_4_load_threads_only": per_host(load_us_per_part),
        "listeners_per_g6_dedicated_4_with_softirq": per_host(load_us_per_part + softirq_us_per_part),
        "busiest_threads": result.threads,
        "edge_udp": result.edge_udp,
        "parts": total["parts"],
        "missing": total["missing"],
        "missing_percent": total["missing_pct"],
        "recovered": total["recovered"],
        "nack_parts": total["nack_parts"],
        "latency_ms": {
            "p50": total["latency_ms"]["p50"],
            "p99": total["latency_ms"]["p99"],
            "p999": total["latency_ms"]["p999"],
            "max": total["latency_ms"]["max"],
        },
        "ttfp_ms": {"p50": total["ttfp_ms"]["p50"], "p99": total["ttfp_ms"]["p99"]},
        "recv_lag_ms": {
            "p50": total["recv_lag_ms"]["p50"],
            "p99": total["recv_lag_ms"]["p99"],
        },
        "socket_drops": total["socket_drops"],
        "udp_rcvbuf_errors": summary["load_host"]["udp_rcvbuf_errors"],
        "load_host_saturated_intervals": summary["load_host"]["saturated_intervals"],
        "worker_busy_max": summary["load_host"]["worker_busy_max"],
        "rss_mb_max": summary["load_host"]["rss_mb_max"],
    });
    std::fs::write(
        dir.join("bench.json"),
        serde_json::to_vec_pretty(&line).unwrap(),
    )
    .unwrap();
    println!("{}", serde_json::to_string_pretty(&line).unwrap());
}

// ------------------------------------------------------------------ a synthetic edge

/// User and system clock ticks of this process's threads whose names start
/// with `prefix`.
fn threads_user_system_ticks(prefix: &str) -> (u64, u64) {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return (0, 0);
    };
    let (mut user, mut system) = (0, 0);
    for task in tasks.flatten() {
        let path = task.path();
        let comm = std::fs::read_to_string(path.join("comm")).unwrap_or_default();
        if !comm.starts_with(prefix) {
            continue;
        }
        let stat = std::fs::read_to_string(path.join("stat")).unwrap_or_default();
        let Some((_, rest)) = stat.rsplit_once(')') else {
            continue;
        };
        let fields: Vec<&str> = rest.split_whitespace().collect();
        user += fields.get(11).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
        system += fields.get(12).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    }
    (user, system)
}

/// A cheap stand-in for an edge, to price the load's listeners alone: an
/// edge in this process cannot send a thousand listeners a thousand parts a
/// second over loopback (its sender also pays each receiver's kernel path),
/// so it gives out before the load does. Each of a few sender threads
/// answers the handshake as an edge does (`TOKEN`, then `OK ... C`) and
/// sends every subscriber one compact datagram of 1 ms PCM (16 + 288 bytes)
/// each millisecond, published now, with `sendmmsg`. NACKs are not
/// answered (a clean path has none).
struct SyntheticEdge {
    addresses: Vec<SocketAddr>,
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<u64>>,
}

const SYNTH_PART_BYTES: usize = 16 + 288;

impl SyntheticEdge {
    fn start(senders: usize, stream_id: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let t0 = Instant::now();
        let mut addresses = Vec::new();
        let mut threads = Vec::new();
        for index in 0..senders {
            let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            socket.set_nonblocking(true).unwrap();
            let _ = socket2::SockRef::from(&socket).set_send_buffer_size(32 << 20);
            let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(32 << 20);
            addresses.push(socket.local_addr().unwrap());
            let stop = Arc::clone(&stop);
            threads.push(
                std::thread::Builder::new()
                    .name(format!("synth-edge{index}"))
                    .spawn(move || synthetic_sender(socket, stream_id, t0, stop))
                    .unwrap(),
            );
        }
        Self {
            addresses,
            stop,
            threads,
        }
    }

    /// Stop; the datagrams sent.
    fn stop(self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        self.threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .sum()
    }
}

fn sockaddr_of(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let raw = socket2::SockAddr::from(addr);
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    unsafe {
        std::ptr::copy_nonoverlapping(
            raw.as_ptr().cast::<u8>(),
            (&mut storage as *mut libc::sockaddr_storage).cast::<u8>(),
            raw.len() as usize,
        );
    }
    (storage, raw.len())
}

fn synthetic_sender(
    socket: std::net::UdpSocket,
    stream_id: u64,
    t0: Instant,
    stop: Arc<AtomicBool>,
) -> u64 {
    use std::os::fd::AsRawFd;
    let fd = socket.as_raw_fd();
    let mut subscribers: Vec<(SocketAddr, libc::sockaddr_storage, libc::socklen_t)> = Vec::new();
    let mut index_of: HashMap<SocketAddr, usize> = HashMap::new();
    let mut buf = [0_u8; 2_048];
    let payload: Vec<u8> = (0..SYNTH_PART_BYTES).map(|i| i as u8).collect();
    let part_of = |at: Instant| at.duration_since(t0).as_micros() as u64 / 1_000;
    let mut next_part = part_of(Instant::now()) + 1;
    let mut sent = 0_u64;
    const BATCH: usize = 64;
    while !stop.load(Ordering::Relaxed) {
        // The control datagrams.
        while let Ok((len, peer)) = socket.recv_from(&mut buf) {
            match av_mesh::client_udp::Control::decode(&buf[..len]) {
                Some(av_mesh::client_udp::Control::Subscribe { token: None, .. }) => {
                    let token = av_mesh::client_udp::Control::Token {
                        stream_id,
                        token: u64::from(peer.port()) * 7 + 1,
                    };
                    let _ = socket.send_to(&token.encode(), peer);
                }
                Some(av_mesh::client_udp::Control::Subscribe { token: Some(_), .. }) => {
                    if !index_of.contains_key(&peer) {
                        let (storage, len) = sockaddr_of(peer);
                        index_of.insert(peer, subscribers.len());
                        subscribers.push((peer, storage, len));
                    }
                    let ok = av_mesh::client_udp::Control::Subscribed {
                        stream_id,
                        next_part,
                        compact: true,
                    };
                    let _ = socket.send_to(&ok.encode(), peer);
                }
                Some(av_mesh::client_udp::Control::Unsubscribe { .. }) => {
                    if let Some(index) = index_of.remove(&peer) {
                        subscribers.swap_remove(index);
                        if let Some((moved, ..)) = subscribers.get(index) {
                            index_of.insert(*moved, index);
                        }
                    }
                }
                _ => {}
            }
        }
        // The parts that are due.
        let now = Instant::now();
        let due = part_of(now);
        while next_part <= due {
            let published = now_unix_us() - (due - next_part) * 1_000;
            let mut datagram =
                av_mesh::client_compact::encode_part(next_part, published, &payload).unwrap();
            for chunk in subscribers.chunks_mut(BATCH) {
                let mut iov = libc::iovec {
                    iov_base: datagram.as_mut_ptr().cast(),
                    iov_len: datagram.len(),
                };
                let mut headers: [libc::mmsghdr; BATCH] = unsafe { std::mem::zeroed() };
                for (header, (_, storage, len)) in headers.iter_mut().zip(chunk.iter_mut()) {
                    header.msg_hdr.msg_name = (storage as *mut libc::sockaddr_storage).cast();
                    header.msg_hdr.msg_namelen = *len;
                    header.msg_hdr.msg_iov = &mut iov;
                    header.msg_hdr.msg_iovlen = 1;
                }
                let mut offset = 0;
                while offset < chunk.len() {
                    let result = unsafe {
                        libc::sendmmsg(
                            fd,
                            headers[offset..].as_mut_ptr(),
                            (chunk.len() - offset) as u32,
                            0,
                        )
                    };
                    if result <= 0 {
                        break;
                    }
                    offset += result as usize;
                    sent += result as u64;
                }
            }
            next_part += 1;
        }
        // Until the next part.
        let next_at = t0 + Duration::from_micros(next_part * 1_000);
        let wait = next_at.saturating_duration_since(Instant::now());
        if wait > Duration::from_micros(200) {
            std::thread::sleep(wait - Duration::from_micros(100));
        }
    }
    sent
}

#[test]
#[ignore = "a measurement: run it alone, in release"]
fn load_mode_cost_against_a_synthetic_edge() {
    let listeners = env_u64("LOAD_SYNTH_LISTENERS", 2_000) as usize;
    let seconds = env_u64("LOAD_SYNTH_SECONDS", 10);
    let threads = env_u64("LOAD_SYNTH_THREADS", 4) as usize;
    let senders = env_u64("LOAD_SYNTH_SENDERS", 8) as usize;
    let coalesce_us = env_u64("LOAD_BENCH_COALESCE_US", 0);
    let stream_id = 4001;
    let name = format!(
        "synth-{listeners}x{threads}-c{coalesce_us}-k{}",
        env_u64("LOAD_BENCH_KERNEL_TIMESTAMPS", 1)
    );
    let dir = out_dir(&name);
    let _ = std::fs::remove_dir_all(&dir);
    let edge = SyntheticEdge::start(senders, stream_id);
    let mut config = LoadConfig::new(
        SessionConfig::new(stream_id),
        edge.addresses.clone(),
        listeners,
    );
    config.duration = Duration::from_secs(seconds);
    config.threads = threads;
    config.start_rate = 4_000.0;
    config.sample = 4;
    config.out_dir = Some(dir.clone());
    config.thread_name = LOAD_THREADS.into();
    config.coalesce = Duration::from_micros(coalesce_us);
    config.kernel_timestamps = env_u64("LOAD_BENCH_KERNEL_TIMESTAMPS", 1) == 1;
    config.progress = std::env::var_os("LOAD_BENCH_PROGRESS").is_some();
    let started = Instant::now();
    let load = std::thread::spawn(move || run(config, &mut |_| {}).unwrap());
    let measure_from = listeners as f64 / 4_000.0 + 2.0;
    std::thread::sleep(Duration::from_secs_f64(measure_from));
    let load_before = threads_cpu_ns(&[LOAD_THREADS]);
    let ticks_before = threads_user_system_ticks(LOAD_THREADS);
    let edge_before = threads_cpu_ns(&["synth-edge"]);
    let host_before = host_jiffies();
    let window_start = Instant::now();
    let window_end = started + Duration::from_secs(seconds) - Duration::from_millis(200);
    std::thread::sleep(window_end.saturating_duration_since(Instant::now()));
    let window = window_start.elapsed().as_secs_f64();
    let load_cpu = (threads_cpu_ns(&[LOAD_THREADS]) - load_before) as f64 / 1e9 / window;
    let edge_cpu = (threads_cpu_ns(&["synth-edge"]) - edge_before) as f64 / 1e9 / window;
    let ticks_after = threads_user_system_ticks(LOAD_THREADS);
    let user_ticks = ticks_after.0 - ticks_before.0;
    let system_ticks = ticks_after.1 - ticks_before.1;
    let system_share = system_ticks as f64 / (user_ticks + system_ticks).max(1) as f64;
    let host_after = host_jiffies();
    let ticks = (unsafe { libc::sysconf(libc::_SC_CLK_TCK) }).max(1) as f64;
    let softirq_cpu = (host_after.0 - host_before.0) as f64 / ticks / window;
    let summary = load.join().unwrap();
    let sent = edge.stop();
    let window_from = window_start.duration_since(started).as_secs_f64();
    let window_to = window_end.duration_since(started).as_secs_f64();
    let intervals = std::fs::read_to_string(dir.join("intervals.jsonl")).unwrap();
    let (mut datagrams, mut counted) = (0_u64, 0.0_f64);
    let mut loops = Vec::new();
    for line in intervals.lines() {
        let line: Value = serde_json::from_str(line).unwrap();
        let t = line["t"].as_f64().unwrap_or(0.0);
        if line["phase"] == "run" && t - 1.0 >= window_from - 0.5 && t <= window_to + 0.5 {
            datagrams += line["total"]["datagrams"].as_u64().unwrap_or(0);
            counted += 1.0;
            loops.push(line["load_host"]["loop"].clone());
        }
    }
    let per_second = datagrams as f64 / counted.max(1.0);
    let us_per_datagram = load_cpu * 1e6 / per_second.max(1.0);
    let total = &summary["total"];
    let line = serde_json::json!({
        "listeners": listeners,
        "load_threads": threads,
        "coalesce_us": coalesce_us,
        "kernel_timestamps": env_u64("LOAD_BENCH_KERNEL_TIMESTAMPS", 1) == 1,
        "senders": senders,
        "window_s": window,
        "datagrams_received_per_second": per_second,
        "delivered_fraction": per_second / (listeners as f64 * 1_000.0),
        "synthetic_edge_sent": sent,
        "load_cpu": load_cpu,
        "load_us_per_datagram": us_per_datagram,
        "load_system_share": system_share,
        "load_cpu_percent_per_listener_at_1000_parts_s": us_per_datagram * 1_000.0 / 1e6 * 100.0,
        "synthetic_edge_cpu": edge_cpu,
        "host_softirq_cpu": softirq_cpu,
        // Four vCPUs kept at 80 %: the workers alone, and with 1.5 µs of
        // kernel receive (softirq on a virtio NIC) for each datagram.
        "listeners_per_g6_dedicated_4_workers_only": (3.2e6 / (us_per_datagram * 1_000.0)).floor(),
        "listeners_per_g6_dedicated_4_with_1_5us_softirq": (3.2e6 / ((us_per_datagram + 1.5) * 1_000.0)).floor(),
        "loop": loops.get(loops.len() / 2),
        "parts": total["parts"],
        "missing": total["missing"],
        "missing_pct": total["missing_pct"],
        "nack_parts": total["nack_parts"],
        "latency_ms": total["latency_ms"]["p50"],
        "latency_p99_ms": total["latency_ms"]["p99"],
        "recv_lag_p99_ms": total["recv_lag_ms"]["p99"],
        "ttfp_ms_p50": total["ttfp_ms"]["p50"],
        "socket_drops": total["socket_drops"],
        "udp_rcvbuf_errors": summary["load_host"]["udp_rcvbuf_errors"],
        "worker_busy_max": summary["load_host"]["worker_busy_max"],
        "saturated_intervals": summary["load_host"]["saturated_intervals"],
        "rss_mb_max": summary["load_host"]["rss_mb_max"],
    });
    std::fs::write(dir.join("bench.json"), serde_json::to_vec_pretty(&line).unwrap()).unwrap();
    println!("{}", serde_json::to_string_pretty(&line).unwrap());
}
