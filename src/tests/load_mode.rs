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
use av_mesh::load::{run, Churn, LoadConfig, Waves};
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
    /// `--paths` and `--spare` (with one edge, `--edges` alone: one leg
    /// either way, the second asked for again and again).
    paths: u8,
    spare: bool,
    /// `--ramp` (none: `listeners` from the start).
    ramp: Option<Vec<(Duration, usize)>>,
    /// The party: churn, join waves and the raw sample (`None, None, 0`:
    /// none).
    party: (Option<Churn>, Option<Waves>, usize),
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

    let mut session = SessionConfig::new(stream_id);
    // LOAD_BENCH_SHORT_HEADER=0: the listeners ask for the 11-byte long
    // form (as before wave 7 item 9).
    session.compact_short_header = env_u64("LOAD_BENCH_SHORT_HEADER", 1) != 0;
    let mut config = LoadConfig::new(session, vec![edge.client_port()], bench.listeners);
    config.director.paths = bench.paths;
    config.director.spare = bench.spare;
    if let Some(ramp) = &bench.ramp {
        config.ramp = ramp.clone();
    }
    (config.churn, config.waves, config.raw_sample) = bench.party;
    config.seed = 11;
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

/// B Y3: with `--paths 1 --spare false` and with the defaults (two paths
/// and a spare; one edge here, so one leg).
#[test]
fn load_mode_counts_every_part_of_every_listener() {
    counts_every_part("check-one-path", 1, false, 41);
    counts_every_part("check", 2, true, 43);
}

fn counts_every_part(name: &str, paths: u8, spare: bool, ip: u8) {
    let dir = out_dir(name);
    let _ = std::fs::remove_dir_all(&dir);
    let bench = Bench {
        paths,
        spare,
        ramp: None,
        party: (None, None, 0),
        listeners: 20,
        seconds: 4,
        threads: 2,
        edge_threads: 2,
        payload: Payload::Pcm,
        sample: 3,
        out_dir: dir.clone(),
        measure_from: 1.5,
    };
    let result = run_bench(&bench, ip);
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
        paths: 1,
        spare: false,
        ramp: None,
        party: (None, None, 0),
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
        "short_header": env_u64("LOAD_BENCH_SHORT_HEADER", 1) != 0,
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
        "ttok_ms": {"p50": total["ttok_ms"]["p50"], "p99": total["ttok_ms"]["p99"]},
        // Bytes on the wire (IP and UDP headers included) a part received.
        "wire_bytes_per_part": total["bytes_in"].as_f64().unwrap_or(0.0)
            / total["parts"].as_f64().unwrap_or(0.0).max(1.0),
        "compact_fallbacks": total["compact_fallbacks"],
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
    // LOAD_SYNTH_PATHS=1: one leg a listener (the cost before B Y3).
    config.director.paths = env_u64("LOAD_SYNTH_PATHS", 2) as u8;
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
    let (mut datagrams, mut parts, mut counted) = (0_u64, 0_u64, 0.0_f64);
    let mut loops = Vec::new();
    for line in intervals.lines() {
        let line: Value = serde_json::from_str(line).unwrap();
        let t = line["t"].as_f64().unwrap_or(0.0);
        if line["phase"] == "run" && t - 1.0 >= window_from - 0.5 && t <= window_to + 0.5 {
            datagrams += line["total"]["datagrams"].as_u64().unwrap_or(0);
            parts += line["total"]["parts"].as_u64().unwrap_or(0);
            counted += 1.0;
            loops.push(line["load_host"]["loop"].clone());
        }
    }
    let per_second = datagrams as f64 / counted.max(1.0);
    let parts_per_second = parts as f64 / counted.max(1.0);
    let us_per_datagram = load_cpu * 1e6 / per_second.max(1.0);
    let total = &summary["total"];
    let line = serde_json::json!({
        "listeners": listeners,
        "paths": env_u64("LOAD_SYNTH_PATHS", 2),
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
        // A listener's part costs a datagram from each of its legs.
        "load_us_per_listener_part": load_cpu * 1e6 / parts_per_second.max(1.0),
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

// ------------------------------------------------------- moves (Phase AM 10)

/// An entry point for the load's assignment (`/api/assignment` over
/// HTTPS/2, B 2.2): it lists the edges that are alive, ranked for each
/// session by rendezvous, as a region's entry point ranks its caches, and
/// names the legs it grants (`paths`) and the spare. The CA file is its own
/// self-signed certificate (for `localhost`).
struct FakeEntry {
    url: String,
    ca: PathBuf,
    alive: Arc<std::sync::Mutex<Vec<(String, SocketAddr)>>>,
    task: tokio::task::JoinHandle<()>,
}

async fn fake_entry(name: &str, alive: Vec<(String, SocketAddr)>) -> FakeEntry {
    use std::hash::{Hash, Hasher};
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let ca = out_dir(name).with_extension("ca.pem");
    std::fs::create_dir_all(ca.parent().unwrap()).unwrap();
    std::fs::write(&ca, certified.cert.pem()).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()),
    );
    let mut server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certified.cert.der().clone()], key)
    .unwrap();
    server.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("https://{}", listener.local_addr().unwrap());
    let alive = Arc::new(std::sync::Mutex::new(alive));
    let shared = Arc::clone(&alive);
    let task = tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let (acceptor, alive) = (acceptor.clone(), Arc::clone(&shared));
            tokio::spawn(async move {
                let Ok(tls) = acceptor.accept(tcp).await else {
                    return;
                };
                let service = hyper::service::service_fn(
                    move |request: http::Request<hyper::body::Incoming>| {
                        let alive = Arc::clone(&alive);
                        async move {
                            let query = request.uri().query().unwrap_or("").to_owned();
                            let field = |name: &str| -> String {
                                query
                                    .split('&')
                                    .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
                                    .unwrap_or("")
                                    .to_owned()
                            };
                            let list = |name: &str| -> Vec<String> {
                                field(name)
                                    .split(',')
                                    .filter(|node| !node.is_empty())
                                    .map(str::to_owned)
                                    .collect()
                            };
                            let sid = field("sid");
                            let (skip, hold) = (list("skip"), list("hold"));
                            let paths: usize = field("paths").parse().unwrap_or(1);
                            let spare = field("spare") == "1";
                            let mut caches = alive.lock().unwrap().clone();
                            caches.sort_by_key(|(node, _)| {
                                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                                sid.hash(&mut hasher);
                                node.hash(&mut hasher);
                                std::cmp::Reverse(hasher.finish())
                            });
                            // B 3.6: the held legs never, the skipped ones
                            // unless no cache is left; the first `paths` are
                            // the legs, the next the spare.
                            caches.retain(|(node, _)| !hold.contains(node));
                            if caches.iter().any(|(node, _)| !skip.contains(node)) {
                                caches.retain(|(node, _)| !skip.contains(node));
                            }
                            let granted = paths.min(caches.len());
                            let spare = (spare && caches.len() > granted)
                                .then(|| caches[granted].0.clone());
                            let assigned: Vec<Value> = caches
                                .iter()
                                .map(|(node, addr)| {
                                    serde_json::json!({
                                        "node_id": node,
                                        "playback_base_url": format!("https://{}:443", addr.ip()),
                                        "client_udp_port": addr.port(),
                                    })
                                })
                                .collect();
                            let body = serde_json::json!({
                                "assigned": assigned,
                                "paths": granted,
                                "spare": spare,
                            })
                            .to_string();
                            Ok::<_, std::convert::Infallible>(http::Response::new(
                                http_body_util::Full::new(Bytes::from(body)),
                            ))
                        }
                    },
                );
                let _ = hyper::server::conn::http2::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .serve_connection(hyper_util::rt::TokioIo::new(tls), service)
                .await;
            });
        }
    });
    FakeEntry {
        url,
        ca,
        alive,
        task,
    }
}

/// What happens 4 s into a run of [`change_one_edge`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Change {
    /// The first edge is killed: the region no longer lists it, its stream
    /// stops, it answers nothing.
    Kill,
    /// The first edge drains: it sends its listeners `MOVE draining` and
    /// serves them until they leave; the region no longer lists it.
    Drain,
    /// The publisher stops for 400 ms at every edge, then goes on with the
    /// next part (an upstream pause: no part is lost).
    Pause,
}

/// The publisher's pause in [`Change::Pause`].
const PAUSE: Duration = Duration::from_millis(400);

/// Four edges of 2.5 ms Opus, 500 listeners spread over them by the entry
/// point, with `--paths` and `--spare` as given; 4 s in, the first edge is
/// killed. Returns the load's summary, its interval lines and the killed
/// edge's address.
fn kill_one_edge(name: &str, paths: u8, spare: bool, ip: u8) -> (Value, Vec<Value>, String) {
    change_one_edge(name, paths, spare, ip, 500, Change::Kill)
}

/// Four edges of 2.5 ms Opus, `listeners` spread over them by the entry
/// point, with `--paths` and `--spare` as given, and `change` 4 s in.
/// Returns the load's summary, its interval lines and the first edge's
/// address.
fn change_one_edge(
    name: &str,
    paths: u8,
    spare: bool,
    ip: u8,
    listeners: usize,
    change: Change,
) -> (Value, Vec<Value>, String) {
    let edge_rt = runtime("kill-edge-rt", 4);
    let pub_rt = runtime("kill-pub-rt", 2);
    let payload = Payload::Opus;
    let stream_id = 4001;
    let seconds = 9;
    let mut edges = Vec::new();
    for index in 0..4_u8 {
        let edge = edge_rt
            .block_on(async move {
                tokio::spawn(async move {
                    let edge = LaneEdge::start_with_capacity(
                        loopback(ip + index),
                        Duration::ZERO,
                        1_024,
                        true,
                    )
                    .await;
                    // The stream is in the cache before anyone subscribes.
                    let (envelope, _) = payload.envelope(stream_id, 0);
                    edge.send(false, &payload.symbols(&envelope), false).await;
                    wait_for_part(&edge.cache, stream_id, 0).await;
                    AnyEdge::Fast(edge)
                })
                .await
            })
            .unwrap();
        edges.push((Arc::new(edge), 1));
    }
    let last = 1 + (seconds + 6) * 1_000_000 / payload.part_us();
    // A pause holds the part due 4.3 s in (the listeners have started) for
    // the pause's length at every edge.
    let held = (change == Change::Pause).then_some(1 + 4_300_000 / payload.part_us());
    let publishers: Vec<_> = edges
        .iter()
        .map(|(edge, first)| {
            let (edge, first) = (Arc::clone(edge), *first);
            pub_rt.spawn(async move {
                match held {
                    Some(held) => {
                        publish_paced(&edge, payload, stream_id, first, held).await;
                        tokio::time::sleep(PAUSE).await;
                        publish_paced(&edge, payload, stream_id, held, last).await;
                    }
                    None => publish_paced(&edge, payload, stream_id, first, last).await,
                }
            })
        })
        .collect();
    let ports: Vec<SocketAddr> = edges.iter().map(|(edge, _)| edge.client_port()).collect();
    let alive: Vec<(String, SocketAddr)> = ports
        .iter()
        .enumerate()
        .map(|(index, port)| (format!("edge-{index}"), *port))
        .collect();
    let entry = edge_rt.block_on(async { fake_entry(name, alive).await });
    std::thread::sleep(Duration::from_millis(300));

    let dir = out_dir(name);
    let _ = std::fs::remove_dir_all(&dir);
    let session = SessionConfig::new(stream_id);
    let mut config = LoadConfig::new(session, Vec::new(), listeners);
    config.duration = Duration::from_secs(seconds);
    config.threads = 2;
    config.start_rate = 4_000.0;
    config.out_dir = Some(dir.clone());
    config.thread_name = "kill-load-w".into();
    config.sid_prefix = name.into();
    config.director.entries = vec![entry.url.clone()];
    config.director.paths = paths;
    config.director.spare = spare;
    // The fake entry point answers HTTPS alone.
    config.director.assignment_udp_port = 0;
    config.director.assignment_timeout = Duration::from_secs(2);
    config.assign = Some(av_mesh::load_assign::AssignConfig {
        tls_ca: entry.ca.clone(),
        server_name: Some("localhost".into()),
        in_flight: 64,
        timeout: Duration::from_secs(2),
    });
    let load = std::thread::spawn(move || run(config, &mut |_| {}).unwrap());
    std::thread::sleep(Duration::from_secs(4));
    let mut publishers: Vec<_> = publishers.into_iter().collect();
    match change {
        Change::Kill => {
            entry.alive.lock().unwrap().retain(|(node, _)| node != "edge-0");
            let first = publishers.remove(0);
            first.abort();
            let _ = pub_rt.block_on(first);
            let (killed, _) = edges.remove(0);
            let killed = Arc::try_unwrap(killed).ok().expect("the killed edge is not shared");
            let AnyEdge::Fast(killed) = killed else {
                panic!("the edges here are fast-lane edges");
            };
            edge_rt.block_on(killed.kill());
        }
        Change::Drain => {
            entry.alive.lock().unwrap().retain(|(node, _)| node != "edge-0");
            let AnyEdge::Fast(drained) = &*edges[0].0 else {
                panic!("the edges here are fast-lane edges");
            };
            edge_rt.block_on(drained.router.lifecycle.set_draining(true));
        }
        Change::Pause => {}
    }
    let summary = load.join().unwrap();
    for publisher in publishers {
        publisher.abort();
        let _ = pub_rt.block_on(publisher);
    }
    entry.task.abort();
    for (edge, _) in edges {
        let edge = Arc::try_unwrap(edge).ok().expect("not shared");
        edge_rt.block_on(edge.stop());
    }
    let lines = std::fs::read_to_string(dir.join("intervals.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (summary, lines, ports[0].to_string())
}

/// The moves of a killed edge's listeners: into each edge left, their
/// spread (max/mean), and the killed edge's moves out.
fn move_spread(summary: &Value, killed: &str) -> (Vec<u64>, f64, u64) {
    let per_edge = summary["per_edge"].as_object().unwrap();
    let into: Vec<u64> = per_edge
        .iter()
        .filter(|(name, _)| name.as_str() != killed && name.as_str() != "unassigned")
        .map(|(_, edge)| edge["moves"].as_u64().unwrap_or(0))
        .collect();
    let mean = into.iter().sum::<u64>() as f64 / into.len().max(1) as f64;
    let max = into.iter().copied().max().unwrap_or(0) as f64;
    let out = per_edge[killed]["moves_out"].as_u64().unwrap_or(0);
    (into, if mean > 0.0 { max / mean } else { 0.0 }, out)
}

/// Phase AM item 10, B Y3: 500 listeners that ask `/api/assignment` for
/// their legs over four edges; one edge is killed. With `--paths 1 --spare
/// false` its listeners' legs are replaced through the entry point (the
/// edges left, ranked for each session); with the defaults (two legs and a
/// spare) the spare takes the dead leg's place. Either way the replacements
/// spread over the three others (max/mean at most 1.5), none is stranded,
/// every listener resumes; each replacement's reason is counted, and the
/// time to move of the listeners left with no leg.
#[test]
fn load_mode_moves_spread_over_the_edges_left_when_one_is_killed() {
    for (name, paths, spare, ip, legs) in [
        ("kill-one-path", 1, false, 71, (80, 200)),
        ("kill-two-paths", 2, true, 75, (180, 330)),
    ] {
        let (summary, lines, killed) = kill_one_edge(name, paths, spare, ip);
        let total = &summary["total"];
        let (into, spread, out) = move_spread(&summary, &killed);
        let started = total["listeners_started"].as_u64().unwrap();
        assert_eq!(started, 500, "{name}: {summary:#}");
        assert_eq!(summary["listeners_without_parts"], 0, "{name}: {summary:#}");
        assert_eq!(summary["never_resumed"], 0, "{name}: {summary:#}");
        assert!(total["assignments"].as_u64().unwrap() >= 500, "{name}: {summary:#}");
        // A quarter of the listeners had their leg on the killed edge with
        // one path, half of them with two.
        assert!(
            (legs.0..=legs.1).contains(&out),
            "{name}: moves out {out}: {summary:#}"
        );
        assert_eq!(into.len(), 3, "{name}: {into:?}");
        // Every move landed (a concurrent test's stray MOVE can add a few).
        assert!(into.iter().sum::<u64>() >= out, "{name}: every move landed: {into:?}");
        assert!(into.iter().all(|moves| *moves > 0), "{name}: {into:?}");
        assert!(spread <= 1.5, "{name}: spread {spread:.2}: {into:?}");
        let stranded = lines
            .iter()
            .filter(|line| line["phase"] == "run")
            .filter_map(|line| line["total"]["stranded"].as_u64())
            .max()
            .unwrap_or(0);
        assert_eq!(stranded, 0, "{name}: no listener without a part for 5 s");
        let reasons = summary["per_edge"][&killed]["moves_by_reason"].clone();
        assert!(
            reasons.as_object().unwrap().keys().all(|reason| {
                ["silence", "unanswered", "no-answer"]
                    .iter()
                    .any(|cause| reason.starts_with(cause))
            }),
            "{name}: {reasons}"
        );
        let moved = &total["time_to_move_ms"];
        if paths == 1 {
            // Each listener of the killed edge had no leg until its new one.
            assert!(moved["count"].as_u64().unwrap() >= out, "{name}: every move timed: {summary:#}");
            let p99 = moved["p99"].as_f64().unwrap();
            assert!(p99 <= 1_500.0, "{name}: time to move p99 {p99} ms");
        }
        eprintln!(
            "{}",
            serde_json::json!({
                "run": name, "moves_out": out, "into": into, "spread": spread,
                "time_to_move_ms": moved, "missing": total["missing"],
                "move_lost": total["move_lost"], "missing_moved": total["missing_moved"],
                "assignment_ms": total["assignment_ms"], "ttfp_ms": total["ttfp_ms"],
            })
        );
    }
}

// ------------------------------------------------------- B Y4 accounting

/// The interval lines of a run (not the final line) after second `from`.
fn run_lines(lines: &[Value], from: f64) -> impl Iterator<Item = &Value> {
    lines
        .iter()
        .filter(move |line| line["phase"] == "run" && line["t"].as_f64().unwrap_or(0.0) > from)
}

/// The count of a histogram of a summary block.
fn count(block: &Value, name: &str) -> u64 {
    block[name]["count"].as_u64().unwrap_or(0)
}

/// B Y4: two-path listeners (the defaults) of a killed edge lose nothing
/// and none is interrupted: the other leg plays on, the spare takes the
/// dead leg's place, and each one's time on one path is booked as a span.
#[test]
fn two_paths_lose_nothing_and_no_listener_is_interrupted_when_an_edge_is_killed() {
    let (summary, lines, killed) = change_one_edge("y4-kill-two", 2, true, 81, 200, Change::Kill);
    let total = &summary["total"];
    assert_eq!(total["missing"], 0, "{summary:#}");
    assert_eq!(summary["listeners_with_missing"], 0, "{summary:#}");
    assert_eq!(summary["listeners_interrupted"], 0, "{summary:#}");
    assert_eq!(total["interruptions"], 0, "{summary:#}");
    let out = summary["per_edge"][&killed]["moves_out"].as_u64().unwrap();
    assert!(out >= 50, "about half the listeners had a leg there: {out}");
    // The spare took each dead leg's place, beside the leg that played on.
    let kinds = &summary["per_edge"][&killed]["leg_replacements"]["by_kind"];
    assert_eq!(kinds["hedge"].as_u64(), Some(out), "{kinds}");
    let exposure = total["exposure_ms"]["p99"].as_f64().unwrap();
    assert!(exposure <= 150.0, "exposure p99 {exposure} ms");
    // Their time on one path: a span each, about the exposure.
    let single = &total["single_path_ms"];
    assert!(single["count"].as_u64().unwrap() >= out, "{single}");
    assert!(single["max"].as_f64().unwrap() <= 200.0, "{single}");
    assert!(total["hedges"].as_u64().unwrap() >= out);
    // No interval had a listener without a delivering leg once all had
    // started, and the longest silence stayed a few parts.
    for line in run_lines(&lines, 2.0) {
        assert_eq!(line["total"]["paths"]["none"], 0, "{line}");
    }
    let longest = summary["longest_silence_ms"].as_f64().unwrap();
    assert!(longest < 30.0, "longest silence {longest} ms");
    // A datagram a part from each of two legs (2.5 ms parts).
    let per_listener = total["datagrams_per_listener_s"].as_f64().unwrap();
    let per_leg = total["datagrams_per_leg_s"].as_f64().unwrap();
    assert!((700.0..=900.0).contains(&per_listener), "{per_listener} a listener and second");
    assert!((350.0..=450.0).contains(&per_leg), "{per_leg} a leg and second");
    assert!(summary["bytes_per_listener_second"].as_f64().unwrap() > 0.0);
}

/// B Y4: one-path listeners of a killed edge (no spare) are each
/// interrupted once, for about the silence (200 ms) and a round trip.
#[test]
fn one_path_reports_the_interruption_of_a_kill() {
    let (summary, lines, killed) = change_one_edge("y4-kill-one", 1, false, 85, 200, Change::Kill);
    let total = &summary["total"];
    let out = summary["per_edge"][&killed]["moves_out"].as_u64().unwrap();
    assert!((25..=80).contains(&out), "a quarter of them: {out}: {summary:#}");
    let interrupted = summary["listeners_interrupted"].as_u64().unwrap();
    assert!(
        interrupted >= out && interrupted <= out + 3,
        "{interrupted} interrupted, {out} moved"
    );
    assert!(total["interruptions"].as_u64().unwrap() >= out);
    let by_line: u64 = lines
        .iter()
        .filter(|line| line["phase"] == "run")
        .map(|line| line["total"]["listeners_interrupted"].as_u64().unwrap())
        .sum();
    assert!(by_line >= out, "the intervals saw them: {by_line}");
    let longest = summary["longest_silence_ms"].as_f64().unwrap();
    assert!((150.0..=1_000.0).contains(&longest), "longest silence {longest} ms");
    let worst = &summary["worst_listeners"]["by_longest_silence"];
    assert_eq!(worst.as_array().unwrap().len(), 20);
    assert!(worst[0]["longest_silence_ms"].as_f64().unwrap() >= 150.0);
    assert!(worst[0]["sid"].as_str().unwrap().starts_with("y4-kill-one-"));
    // Nothing is lost: the new leg starts from the oldest hole.
    assert_eq!(total["missing"], 0, "{summary:#}");
    assert_eq!(count(total, "single_path_ms"), 0, "one path is no two-path session");
    assert_eq!(count(total, "time_to_move_ms"), out);
}

/// B Y4: a drain moves every listener make-before-break, with one path or
/// two: nobody is interrupted, nothing is missing, and two-path listeners
/// never fall to one path.
#[test]
fn a_drain_interrupts_no_listener_with_one_path_or_two() {
    for (name, paths, spare, ip) in [("y4-drain-one", 1, false, 89), ("y4-drain-two", 2, true, 93)] {
        let (summary, _, drained) = change_one_edge(name, paths, spare, ip, 200, Change::Drain);
        let total = &summary["total"];
        assert_eq!(summary["listeners_interrupted"], 0, "{name}: {summary:#}");
        assert_eq!(total["interruptions"], 0, "{name}: {summary:#}");
        assert_eq!(total["missing"], 0, "{name}: {summary:#}");
        let edge = &summary["per_edge"][&drained];
        let moved = edge["moves_by_reason"]["draining"].as_u64().unwrap_or(0);
        assert!(moved >= 25, "{name}: {moved} moved off the drained edge: {summary:#}");
        assert_eq!(
            edge["leg_replacements"]["by_kind"]["handover"].as_u64(),
            Some(moved),
            "{name}"
        );
        assert_eq!(count(edge, "handover_ms"), moved, "{name}");
        if paths == 2 {
            assert_eq!(count(total, "single_path_ms"), 0, "{name}: never on one path");
        }
    }
}

/// B Y4: the publisher pauses 400 ms: every listener sees the gap (an
/// interruption nothing can spare it), and none moves or loses a part.
#[test]
fn a_pause_of_the_publisher_is_a_gap_at_listeners_that_do_not_move() {
    let (summary, _, _) = change_one_edge("y4-pause", 2, true, 97, 120, Change::Pause);
    let total = &summary["total"];
    assert_eq!(total["moves"], 0, "{summary:#}");
    assert_eq!(total["missing"], 0, "{summary:#}");
    assert_eq!(summary["listeners_with_missing"], 0);
    assert_eq!(summary["listeners_interrupted"], 120, "{summary:#}");
    let pause = PAUSE.as_millis() as f64;
    let longest = summary["longest_silence_ms"].as_f64().unwrap();
    assert!(
        longest >= pause * 0.85 && longest <= 1_000.0,
        "longest silence {longest} ms"
    );
    // The pause is in the gap histogram, not in the latency (publication
    // to arrival): the parts after it are published late.
    assert!(total["part_gap_ms"]["max"].as_f64().unwrap() >= pause * 0.85);
    let latency = &total["latency_ms"];
    assert!(latency["p999"].as_f64().unwrap() < 100.0, "{latency}");
}

/// B Y4: listeners that leave (a falling ramp) leave their parts not yet
/// due unjudged: nothing is booked missing.
#[test]
fn a_falling_ramp_books_nothing_missing() {
    let dir = out_dir("y4-falling");
    let _ = std::fs::remove_dir_all(&dir);
    let bench = Bench {
        paths: 2,
        spare: true,
        ramp: Some(vec![(Duration::ZERO, 40), (Duration::from_secs(3), 10)]),
        party: (None, None, 0),
        listeners: 40,
        seconds: 5,
        threads: 2,
        edge_threads: 2,
        payload: Payload::Pcm,
        sample: 0,
        out_dir: dir.clone(),
        measure_from: 1.0,
    };
    let result = run_bench(&bench, 101);
    let summary = &result.summary;
    let total = &summary["total"];
    assert_eq!(total["listeners_started"], 40, "{summary:#}");
    let lines: Vec<Value> = std::fs::read_to_string(dir.join("intervals.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let stopped: u64 = lines
        .iter()
        .map(|line| line["total"]["stopped"].as_u64().unwrap())
        .sum();
    assert_eq!(stopped, 30, "{summary:#}");
    assert_eq!(total["missing"], 0, "{summary:#}");
    assert_eq!(summary["listeners_with_missing"], 0, "{summary:#}");
    assert_eq!(summary["listeners_interrupted"], 0, "{summary:#}");
}

// ------------------------------------------------------- the listening party

/// The owner's listening party, in small: 60 listeners join in waves of 20
/// a second; listeners leave (three in a hundred a second) and come back
/// after 0.3 to 0.8 s as new sessions; one in ten writes its parts to the
/// raw sample. Joins, leaves and rejoins are counted per interval, each
/// join's and rejoin's time to first audio apart, and nothing is missing.
#[test]
fn a_party_joins_in_waves_and_comes_back_after_leaving() {
    let dir = out_dir("party");
    let _ = std::fs::remove_dir_all(&dir);
    let churn = Churn {
        per_minute: 1.8,
        absent_min: Duration::from_millis(300),
        absent_max: Duration::from_millis(800),
    };
    let waves = Waves {
        size: 20,
        every: Duration::from_secs(1),
    };
    let bench = Bench {
        paths: 2,
        spare: true,
        ramp: None,
        party: (Some(churn), Some(waves), 10),
        listeners: 60,
        seconds: 6,
        threads: 2,
        edge_threads: 2,
        payload: Payload::Pcm,
        sample: 0,
        out_dir: dir.clone(),
        measure_from: 1.0,
    };
    let result = run_bench(&bench, 105);
    let summary = &result.summary;
    let total = &summary["total"];
    let lines: Vec<Value> = std::fs::read_to_string(dir.join("intervals.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    // The waves: 20, 40, then 60 wanted, a second apart (a line gives the
    // target at its end, when the next wave may have come).
    let waved: Vec<u64> = run_lines(&lines, 0.0)
        .map(|line| line["wave_target"].as_u64().unwrap())
        .collect();
    assert!(waved[0] == 20 || waved[0] == 40, "{waved:?}");
    assert!(waved.windows(2).all(|pair| pair[1] >= pair[0] && pair[1] - pair[0] <= 20), "{waved:?}");
    assert!(waved.iter().all(|wanted| wanted % 20 == 0) && waved[3] == 60, "{waved:?}");
    let first = run_lines(&lines, 0.0).next().unwrap();
    assert!(first["total"]["joins"].as_u64().unwrap() <= 40, "{first}");
    assert_eq!(summary["joins"], 60, "{summary:#}");
    let leaves = summary["leaves"].as_u64().unwrap();
    let rejoins = summary["rejoins"].as_u64().unwrap();
    // 1.8 a minute of 60 listeners: about 1.8 a second over 5 s.
    assert!((3..=25).contains(&leaves), "{leaves} leaves");
    assert!(rejoins >= 1 && rejoins <= leaves, "{rejoins} rejoins of {leaves} leaves");
    let by_line = |name: &str| -> u64 {
        lines
            .iter()
            .map(|line| line["total"][name].as_u64().unwrap_or(0))
            .sum()
    };
    assert_eq!(by_line("leaves"), leaves);
    assert_eq!(by_line("rejoins"), rejoins);
    assert_eq!(total["listeners_started"].as_u64(), Some(60 + rejoins));
    assert_eq!(count(total, "join_first_audio_ms"), 60);
    let back = count(total, "rejoin_first_audio_ms");
    assert!(back + 2 >= rejoins && back <= rejoins, "{back} of {rejoins} rejoins had audio");
    assert!(total["rejoin_first_audio_ms"]["buckets_us"].is_array());
    assert_eq!(total["missing"], 0, "{summary:#}");
    assert_eq!(summary["listeners_with_missing"], 0, "{summary:#}");
    // The raw sample: one listener in ten (by number, over its sessions).
    assert_eq!(summary["raw_sample"]["one_in"], 10);
    let files = summary["raw_sample"]["files"].as_array().unwrap();
    assert_eq!(files.len(), 2, "a file a worker");
    let mut rows = 0;
    for file in files {
        let text = std::fs::read_to_string(file.as_str().unwrap()).unwrap();
        let mut csv = text.lines();
        assert_eq!(
            csv.next(),
            Some("sid,part,arrival_unix_us,bytes,published_unix_us,available_unix_us,edge")
        );
        for row in csv {
            let sid = row.split(',').next().unwrap();
            let number: usize = sid
                .trim_start_matches("load-")
                .split('.')
                .next()
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(number % 10, 0, "{row}");
            rows += 1;
        }
    }
    assert!(rows >= 6 * 3_000, "{rows} rows");
}
