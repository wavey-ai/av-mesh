//! Edge capacity (low-latency wave 7, item 10): how many UDP listeners of
//! 1 ms parts one edge serves, and that the edge's fast-plane ingest never
//! drops a part because of its fan-out to them.
//!
//! An edge fed by the fast plane (two forwarders' RelaySession symbols, the
//! fast client lane and the compact lane), its threads pinned to two CPUs as
//! a g6-dedicated-2 has (`EDGE_CAP_EDGE_CPUS`, 0: not pinned), a publisher
//! thread that sends each part's source symbol from both forwarders at the
//! part's pace (as the forwarders do since wave 7 item 2), and listeners on
//! worker threads of their own (on other CPUs): each its own socket and
//! compact-lane subscription on a clean path (the CLEAN plan, no NACK, so a
//! part that the edge does not send is missing), counting each part once at
//! its kernel receive time.
//!
//! `edge_capacity_sweep` (ignored, a measurement) prints one JSON line a run:
//! the parts each listener missed, publication to kernel arrival, the edge's
//! ingest drops (`kernel_dropped`, `tee_dropped`), the fan-out's own sheds,
//! the edge's processor time per listener and per datagram, and the CPU of
//! the two edge CPUs (softirq apart: over loopback the receivers' kernel
//! path runs in the edge's sends, which a NIC does not do). Run it alone:
//! `cargo test --release edge_capacity_sweep -- --ignored --nocapture
//! --test-threads 1`, with EDGE_CAP_LISTENERS ("100,200,400"),
//! EDGE_CAP_PAYLOAD (opus1, pcm or opus: 1 ms Opus Custom, 1 ms PCM, 2.5 ms
//! Opus), EDGE_CAP_SECONDS (8), EDGE_CAP_THREADS (4 listener workers),
//! EDGE_CAP_EDGE_CPUS (2), EDGE_CAP_COALESCE_US (1000: a worker reads its
//! sockets each millisecond, so a send to it wakes no one) and
//! EDGE_CAP_FANOUT_THREADS (the edge's `--client-udp-fanout-threads`) and
//! EDGE_CAP_SEND_TWINS (`--client-udp-send-twins`, true).

use super::compact_lane::Payload;
use super::fast_lane::LaneEdge;
use super::fast_lease::loopback;
use super::*;
use av_mesh::client_compact as compact;
use av_mesh::client_udp::{CompactPlan, Control, RepairPolicy, UNMEASURED_LOSS};
use fast_plane::sys::RecvBatch;
use serde_json::Value;
use std::os::fd::AsRawFd;
use std::sync::atomic::AtomicU64;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

// ------------------------------------------------------------------ CPUs

/// Pin thread `tid` (0: the caller) to `cpus`.
fn pin(tid: libc::pid_t, cpus: &[usize]) {
    if cpus.is_empty() {
        return;
    }
    // SAFETY: a zeroed cpu_set_t is empty; CPU_SET writes inside it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for cpu in cpus {
            libc::CPU_SET(*cpu, &mut set);
        }
        libc::sched_setaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

/// The ids and names of this process's threads.
fn threads() -> Vec<(libc::pid_t, String)> {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return Vec::new();
    };
    tasks
        .flatten()
        .filter_map(|task| {
            let tid = task.file_name().to_str()?.parse().ok()?;
            let comm = std::fs::read_to_string(task.path().join("comm")).ok()?;
            Some((tid, comm.trim_end().to_owned()))
        })
        .collect()
}

const EDGE_THREADS: [&str; 4] = ["needletail-fast", "edge-rt", "needletail-cut", "nt-fanout"];

fn is_edge_thread(name: &str) -> bool {
    EDGE_THREADS.iter().any(|prefix| name.starts_with(prefix))
}

/// Processor time, ns, of this process's threads by name.
fn thread_cpu_ns() -> BTreeMap<String, u64> {
    let mut by_name = BTreeMap::new();
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return by_name;
    };
    for task in tasks.flatten() {
        let path = task.path();
        let comm = std::fs::read_to_string(path.join("comm")).unwrap_or_default();
        if let Some(ns) = std::fs::read_to_string(path.join("schedstat"))
            .ok()
            .and_then(|stat| stat.split_whitespace().next()?.parse::<u64>().ok())
        {
            *by_name.entry(comm.trim_end().to_owned()).or_insert(0) += ns;
        }
    }
    by_name
}

/// Busy and softirq jiffies of each of `cpus` (from /proc/stat).
fn cpu_jiffies(cpus: &[usize]) -> (u64, u64) {
    let text = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let (mut busy, mut softirq) = (0, 0);
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let Some(name) = fields.next() else { continue };
        let Some(cpu) = name.strip_prefix("cpu").and_then(|n| n.parse::<usize>().ok()) else {
            continue;
        };
        if !cpus.contains(&cpu) {
            continue;
        }
        let values: Vec<u64> = fields.filter_map(|v| v.parse().ok()).collect();
        // user nice system idle iowait irq softirq steal
        busy += values.iter().take(8).sum::<u64>() - values[3] - values[4];
        softirq += values[6];
    }
    (busy, softirq)
}

// ------------------------------------------------------------------ listeners

const SEEN_WORDS: usize = 2_048;

struct Listener {
    socket: std::net::UdpSocket,
    token: Option<u64>,
    first: Option<u64>,
    asked_at: Option<Instant>,
    next_refresh: Instant,
    /// Parts received, a bit a part from `first`.
    seen: Vec<u64>,
    newest: u64,
}

impl Listener {
    fn new() -> Self {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(1 << 20);
        fast_plane::sys::enable_receive_metadata(&socket).unwrap();
        Self {
            socket,
            token: None,
            first: None,
            asked_at: None,
            next_refresh: Instant::now(),
            seen: vec![0; SEEN_WORDS],
            newest: 0,
        }
    }

    fn subscribe(&self, edge: SocketAddr, stream_id: u64) {
        let repair = RepairPolicy::for_loss(
            UNMEASURED_LOSS,
            RepairPolicy::base_for_lane(true, None, None),
        );
        let request = Control::Subscribe {
            stream_id,
            from_part: None,
            repair,
            token: self.token,
            fast_lane: true,
            compact: Some(CompactPlan::CLEAN),
        }
        .encode();
        let _ = self.socket.send_to(&request, edge);
    }

    /// Mark `part`; `true` if it is new.
    fn mark(&mut self, part: u64) -> bool {
        let Some(first) = self.first else {
            return false;
        };
        let Some(index) = part.checked_sub(first).map(|i| i as usize) else {
            return false;
        };
        if index >= SEEN_WORDS * 64 {
            return false;
        }
        self.newest = self.newest.max(part);
        let (word, bit) = (index / 64, 1_u64 << (index % 64));
        let new = self.seen[word] & bit == 0;
        self.seen[word] |= bit;
        new
    }

    fn has(&self, part: u64) -> bool {
        let Some(first) = self.first else {
            return false;
        };
        let Some(index) = part.checked_sub(first).map(|i| i as usize) else {
            return false;
        };
        index < SEEN_WORDS * 64 && self.seen[index / 64] & (1 << (index % 64)) != 0
    }
}

/// Shared by the listener workers and the run.
struct Load {
    stop: AtomicBool,
    subscribed: AtomicU64,
    /// The parts measured: from, to (inclusive).
    window_from: AtomicU64,
    window_to: AtomicU64,
}

/// Publication to kernel arrival, in buckets of 25 µs up to 100 ms.
const BUCKET_US: u64 = 25;
const BUCKETS: usize = 4_000;

struct WorkerResult {
    listeners: Vec<Listener>,
    latency: Vec<u64>,
    datagrams: u64,
    gaps: u64,
}

fn listener_worker(
    mut listeners: Vec<Listener>,
    edge: SocketAddr,
    stream_id: u64,
    load: Arc<Load>,
    coalesce: Duration,
    start_per_ms: usize,
) -> WorkerResult {
    // SAFETY: plain epoll calls on descriptors this thread owns.
    let epoll = unsafe { libc::epoll_create1(0) };
    assert!(epoll >= 0);
    for (index, listener) in listeners.iter().enumerate() {
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: index as u64,
        };
        unsafe {
            libc::epoll_ctl(
                epoll,
                libc::EPOLL_CTL_ADD,
                listener.socket.as_raw_fd(),
                &mut event,
            );
        }
    }
    let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; 256];
    let mut batch = RecvBatch::new();
    let mut latency = vec![0_u64; BUCKETS];
    let (mut datagrams, mut gaps) = (0_u64, 0_u64);
    let mut next_start = 0_usize;
    while !load.stop.load(Ordering::Relaxed) {
        let now = Instant::now();
        // Subscribe a few at a time; ask again after 500 ms unanswered.
        let mut started = 0;
        while next_start < listeners.len() && started < start_per_ms {
            listeners[next_start].subscribe(edge, stream_id);
            listeners[next_start].asked_at = Some(now);
            next_start += 1;
            started += 1;
        }
        for listener in listeners.iter_mut() {
            let due = match (listener.first, listener.asked_at) {
                (None, Some(asked)) => now.duration_since(asked) > Duration::from_millis(500),
                (Some(_), _) => now >= listener.next_refresh,
                (None, None) => false,
            };
            if due {
                listener.subscribe(edge, stream_id);
                listener.asked_at = Some(now);
                listener.next_refresh = now + Duration::from_secs(2);
            }
        }
        if !coalesce.is_zero() {
            std::thread::sleep(coalesce);
        }
        let timeout = if coalesce.is_zero() { 5 } else { 0 };
        // SAFETY: the events buffer outlives the call.
        let ready = unsafe {
            libc::epoll_wait(epoll, events.as_mut_ptr(), events.len() as i32, timeout)
        };
        let (from, to) = (
            load.window_from.load(Ordering::Relaxed),
            load.window_to.load(Ordering::Relaxed),
        );
        for event in &events[..ready.max(0) as usize] {
            let index = event.u64 as usize;
            loop {
                let received = batch.recv(&listeners[index].socket).unwrap_or(0);
                for message in 0..received {
                    let got = batch.get(message);
                    let listener = &mut listeners[index];
                    if let Some(view) = compact::parse(got.data) {
                        datagrams += 1;
                        let reference = listener.newest.max(listener.first.unwrap_or(0));
                        let part = compact::extend_u32(reference, view.part_low);
                        match view.kind {
                            compact::Kind::Part => {
                                if listener.mark(part) && part >= from && part <= to {
                                    let published = u32::from_be_bytes(view.field);
                                    let arrived = got.kernel_us.unwrap_or_else(now_unix_us) as u32;
                                    let us = u64::from(arrived.wrapping_sub(published));
                                    latency[((us / BUCKET_US) as usize).min(BUCKETS - 1)] += 1;
                                }
                            }
                            compact::Kind::Gap => gaps += 1,
                            compact::Kind::Repair => {}
                        }
                        continue;
                    }
                    match Control::decode(got.data) {
                        Some(Control::Token { token, .. }) => {
                            if listener.token != Some(token) {
                                listener.token = Some(token);
                                listener.subscribe(edge, stream_id);
                            }
                        }
                        Some(Control::Subscribed { next_part, .. }) => {
                            if listener.first.is_none() {
                                listener.first = Some(next_part);
                                listener.next_refresh = Instant::now() + Duration::from_secs(2);
                                load.subscribed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        _ => {}
                    }
                }
                if received < fast_plane::sys::RECV_BATCH {
                    break;
                }
            }
        }
    }
    unsafe { libc::close(epoll) };
    WorkerResult {
        listeners,
        latency,
        datagrams,
        gaps,
    }
}

// ------------------------------------------------------------------ the publisher

/// Send each part's source symbols from both forwarders at the part's pace,
/// on a thread of its own: part `first` at once, then one a part interval.
fn publisher(
    edge: &LaneEdge,
    payload: Payload,
    stream_id: u64,
    first: u64,
    stop: Arc<AtomicBool>,
    published: Arc<AtomicU64>,
    cpu: Option<usize>,
) -> std::thread::JoinHandle<()> {
    let fds = [edge.forwarder_fd(0), edge.forwarder_fd(1)];
    let ports = [edge.relay_port(0, false), edge.relay_port(1, false)];
    std::thread::Builder::new()
        .name("cap-publisher".into())
        .spawn(move || {
            if let Some(cpu) = cpu {
                pin(0, &[cpu]);
            }
            let started = Instant::now();
            let part_us = payload.part_us();
            let mut sequence = first;
            while !stop.load(Ordering::Relaxed) {
                let due = started + Duration::from_micros(part_us * (sequence - first));
                let now = Instant::now();
                if due > now + Duration::from_micros(200) {
                    std::thread::sleep(due - now - Duration::from_micros(150));
                }
                while Instant::now() < due {
                    std::hint::spin_loop();
                }
                let (envelope, _) = payload.envelope(stream_id, sequence);
                let (sources, _) = payload.symbols(&envelope);
                for symbol in &sources {
                    for (fd, port) in fds.iter().zip(ports) {
                        let _ = fast_plane::sys::send_batch(*fd, 1, |_| (symbol.as_slice(), port));
                    }
                }
                published.store(sequence, Ordering::Relaxed);
                sequence += 1;
            }
        })
        .unwrap()
}

// ------------------------------------------------------------------ the run

struct Run {
    listeners: usize,
    payload: Payload,
    seconds: u64,
    workers: usize,
    edge_cpus: Vec<usize>,
    coalesce: Duration,
    fanout_threads: Option<usize>,
    /// `--client-udp-fanout-inline-sends` (its default when `None`).
    inline_sends: Option<usize>,
    send_twins: bool,
}

fn edge_counters(edge: &LaneEdge) -> Value {
    let plane = edge.cache.relay_ingress_snapshot().fast_plane;
    let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    let stats = &edge.stats;
    let mut counters = serde_json::json!({
        "ingest_received": plane.audio_received,
        "ingest_kernel_dropped": plane.kernel_dropped,
        "tee_dropped": plane.tee_dropped,
        "cache_lane_datagrams": get(&stats.datagrams),
        "cache_lane_compact_parts": get(&stats.compact_parts),
        "window_repair": get(&stats.compact_window_repair),
        "send_queue_drops": get(&stats.send_queue_drops),
        "playout_skipped_parts": get(&stats.playout_skipped_parts),
        "gaps": get(&stats.gaps),
        "subscriptions": get(&stats.subscriptions),
        "refusals": get(&stats.refusals),
    });
    // Every counter of the fast client lane, as lane_<name>.
    if let Value::Object(lane) = serde_json::to_value(edge.lane.snapshot_stats()).unwrap() {
        for (name, value) in lane {
            if value.is_u64() {
                counters[format!("lane_{name}")] = value;
            }
        }
    }
    counters
}

fn delta(after: &Value, before: &Value) -> Value {
    let mut out = serde_json::Map::new();
    for (key, value) in after.as_object().unwrap() {
        let a = value.as_u64().unwrap_or(0);
        let b = before[key].as_u64().unwrap_or(0);
        out.insert(key.clone(), Value::from(a.saturating_sub(b)));
    }
    Value::Object(out)
}

fn quantile_ms(buckets: &[u64], q: f64) -> f64 {
    let total: u64 = buckets.iter().sum();
    if total == 0 {
        return 0.0;
    }
    let target = ((total as f64) * q).ceil().max(1.0) as u64;
    let mut seen = 0;
    for (index, count) in buckets.iter().enumerate() {
        seen += count;
        if seen >= target {
            return ((index as u64 + 1) * BUCKET_US) as f64 / 1_000.0;
        }
    }
    (BUCKETS as u64 * BUCKET_US) as f64 / 1_000.0
}

fn run(run: &Run, ip: u8) -> Value {
    // W3_PTRACER=1 lets a sampling profiler (eu-stack) read the threads.
    if std::env::var_os("W3_PTRACER").is_some() {
        // SAFETY: prctl with integer arguments only.
        unsafe { libc::prctl(libc::PR_SET_PTRACER, libc::PR_SET_PTRACER_ANY, 0, 0, 0) };
    }
    let edge_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("edge-rt")
        .enable_all()
        .build()
        .unwrap();
    let stream_id = 4001;
    let payload = run.payload;
    let capacity = run.listeners + 64;
    let fanout = run.fanout_threads;
    let inline_sends = run
        .inline_sends
        .unwrap_or(fast_plane::edge_clients::DEFAULT_FANOUT_INLINE_SENDS);
    let send_twins = run.send_twins;
    let edge = edge_rt.block_on(async move {
        tokio::spawn(async move {
            let edge =
                LaneEdge::start_with_capacity(loopback(ip), Duration::ZERO, capacity, send_twins)
                    .await;
            if let Some(threads) = fanout {
                edge.lane.set_fanout(threads, inline_sends);
            }
            let (envelope, _) = payload.envelope(stream_id, 0);
            edge.send(false, &payload.symbols(&envelope), false).await;
            wait_for_part(&edge.cache, stream_id, 0).await;
            edge
        })
        .await
        .unwrap()
    });
    // The edge's threads on its CPUs; the rest elsewhere.
    let others: Vec<usize> = (0..num_cpus())
        .filter(|cpu| !run.edge_cpus.contains(cpu) && *cpu >= 2)
        .collect();
    let pin_edge = || {
        for (tid, name) in threads() {
            if is_edge_thread(&name) {
                pin(tid, &run.edge_cpus);
            }
        }
    };
    pin_edge();
    let stop = Arc::new(AtomicBool::new(false));
    let published = Arc::new(AtomicU64::new(0));
    let publisher = publisher(
        &edge,
        payload,
        stream_id,
        1,
        Arc::clone(&stop),
        Arc::clone(&published),
        others.last().copied(),
    );
    std::thread::sleep(Duration::from_millis(300));
    let load = Arc::new(Load {
        stop: AtomicBool::new(false),
        subscribed: AtomicU64::new(0),
        window_from: AtomicU64::new(u64::MAX),
        window_to: AtomicU64::new(u64::MAX),
    });
    let client_port = edge.client_port;
    let mut workers = Vec::new();
    let per_worker = run.listeners.div_ceil(run.workers.max(1));
    for worker in 0..run.workers {
        let count = per_worker.min(run.listeners.saturating_sub(worker * per_worker));
        let load = Arc::clone(&load);
        let coalesce = run.coalesce;
        let cpu = others.get(worker).copied();
        workers.push(
            std::thread::Builder::new()
                .name(format!("cap-load{worker}"))
                .spawn(move || {
                    if let Some(cpu) = cpu {
                        pin(0, &[cpu]);
                    }
                    let listeners = (0..count).map(|_| Listener::new()).collect();
                    listener_worker(listeners, client_port, stream_id, load, coalesce, 2)
                })
                .unwrap(),
        );
    }
    let subscribed_by = Instant::now() + Duration::from_secs(30);
    while load.subscribed.load(Ordering::Relaxed) < run.listeners as u64
        && Instant::now() < subscribed_by
    {
        std::thread::sleep(Duration::from_millis(20));
    }
    let subscribed = load.subscribed.load(Ordering::Relaxed);
    // Threads that the edge started since (the stream sender's runtime
    // threads, fan-out threads) on its CPUs too.
    pin_edge();
    std::thread::sleep(Duration::from_secs(2));
    // The window.
    let before = edge_counters(&edge);
    let cpu_before = thread_cpu_ns();
    let jiffies_before = cpu_jiffies(&run.edge_cpus);
    let window_from = published.load(Ordering::Relaxed) + 1;
    load.window_from.store(window_from, Ordering::Relaxed);
    let started = Instant::now();
    std::thread::sleep(Duration::from_secs(run.seconds));
    let window_to = published.load(Ordering::Relaxed);
    load.window_to.store(window_to, Ordering::Relaxed);
    let window = started.elapsed().as_secs_f64();
    let after = edge_counters(&edge);
    let cpu_after = thread_cpu_ns();
    let jiffies_after = cpu_jiffies(&run.edge_cpus);
    // Late parts come in a drain.
    std::thread::sleep(Duration::from_millis(600));
    load.stop.store(true, Ordering::Relaxed);
    let results: Vec<WorkerResult> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    stop.store(true, Ordering::Relaxed);
    publisher.join().unwrap();

    let mut latency = vec![0_u64; BUCKETS];
    let (mut expected, mut missing, mut listeners_missing, mut datagrams, mut gaps) =
        (0_u64, 0_u64, 0_u64, 0_u64, 0_u64);
    for result in &results {
        for (bucket, count) in latency.iter_mut().zip(&result.latency) {
            *bucket += count;
        }
        datagrams += result.datagrams;
        gaps += result.gaps;
        for listener in &result.listeners {
            let Some(first) = listener.first else {
                continue;
            };
            let from = window_from.max(first);
            let lost = (from..=window_to).filter(|part| !listener.has(*part)).count() as u64;
            expected += window_to.saturating_sub(from) + 1;
            missing += lost;
            listeners_missing += u64::from(lost > 0);
        }
    }
    let ticks = (unsafe { libc::sysconf(libc::_SC_CLK_TCK) }).max(1) as f64;
    let edge_cpu: BTreeMap<String, f64> = cpu_after
        .iter()
        .filter(|(name, _)| is_edge_thread(name))
        .map(|(name, ns)| {
            let before = cpu_before.get(name).copied().unwrap_or(0);
            (name.clone(), (ns - before) as f64 / 1e9 / window)
        })
        .collect();
    let edge_threads_cpu: f64 = edge_cpu.values().sum();
    let counters = delta(&after, &before);
    let sent = counters["lane_datagrams"].as_u64().unwrap_or(0)
        + counters["cache_lane_datagrams"].as_u64().unwrap_or(0);
    let edge_cpus_busy = (jiffies_after.0 - jiffies_before.0) as f64 / ticks / window;
    let edge_cpus_softirq = (jiffies_after.1 - jiffies_before.1) as f64 / ticks / window;
    let per_second = |value: u64| value as f64 / window;
    // The parts whose send to a client the edge's journal holds.
    let udp_sent_parts = edge
        .cache
        .hop_journal
        .get()
        .map(|journal| {
            journal
                .events(HopQuery {
                    since_us: None,
                    stream: Some(stream_id),
                })
                .into_iter()
                .filter(|event| event.stage == HopStage::UdpSent)
                .map(|event| event.seq)
                .collect::<std::collections::BTreeSet<u64>>()
                .len()
        })
        .unwrap_or(0);
    let result = serde_json::json!({
        "payload": payload.name(),
        "listeners": run.listeners,
        "subscribed": subscribed,
        "fanout_threads": run.fanout_threads,
        "send_twins": run.send_twins,
        "edge_cpus": run.edge_cpus,
        "coalesce_us": run.coalesce.as_micros() as u64,
        "window_s": window,
        "parts_published": window_to + 1 - window_from,
        "expected_listener_parts": expected,
        "missing_listener_parts": missing,
        "missing_percent": missing as f64 * 100.0 / expected.max(1) as f64,
        "listeners_missing_parts": listeners_missing,
        "gaps": gaps,
        "datagrams_received": datagrams,
        "udp_sent_parts": udp_sent_parts,
        "latency_ms": {
            "p50": quantile_ms(&latency, 0.5),
            "p99": quantile_ms(&latency, 0.99),
            "p999": quantile_ms(&latency, 0.999),
            "max": quantile_ms(&latency, 1.0),
        },
        "edge_threads_cpu": edge_threads_cpu,
        "edge_thread_cpu": edge_cpu,
        "edge_cpus_busy": edge_cpus_busy,
        "edge_cpus_softirq": edge_cpus_softirq,
        "edge_datagrams_per_second": per_second(sent),
        "edge_threads_us_per_datagram": edge_threads_cpu * 1e6 / per_second(sent).max(1.0),
        "edge_cpus_us_per_datagram": edge_cpus_busy * 1e6 / per_second(sent).max(1.0),
        "edge": counters,
    });
    edge_rt.block_on(edge.stop());
    result
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map_or(4, usize::from)
}

fn payload_of(name: &str) -> Payload {
    match name {
        "pcm" => Payload::Pcm,
        "opus" => Payload::Opus,
        _ => Payload::Opus1,
    }
}

/// Listeners served through the fan-out threads (every batch handed off,
/// the sends from the client sockets' send-only twins) get every part once,
/// the edge's ingest takes every symbol, the control loop answers the
/// requests that the SO_REUSEPORT group hands the twins (else some
/// listeners would never subscribe), and the journal keeps the parts'
/// `udp_sent`.
#[test]
fn listeners_of_the_fan_out_threads_get_every_part_and_the_ingest_every_symbol() {
    let line = run(
        &Run {
            listeners: 48,
            payload: Payload::Pcm,
            seconds: 3,
            workers: 2,
            edge_cpus: Vec::new(),
            coalesce: Duration::from_micros(1_000),
            fanout_threads: Some(2),
            inline_sends: Some(0),
            send_twins: true,
        },
        45,
    );
    let edge = &line["edge"];
    assert_eq!(line["subscribed"], 48, "{line:#}");
    assert_eq!(line["missing_listener_parts"], 0, "{line:#}");
    assert_eq!(line["gaps"], 0, "{line:#}");
    assert_eq!(edge["ingest_kernel_dropped"], 0, "{line:#}");
    assert_eq!(edge["tee_dropped"], 0, "{line:#}");
    let parts = line["parts_published"].as_u64().unwrap();
    assert!(parts >= 2_500, "{line:#}");
    // Both forwarders' copy of each part.
    let received = edge["ingest_received"].as_u64().unwrap();
    assert!(received + 4 >= 2 * parts, "{line:#}");
    assert!(edge["lane_fanout_batches"].as_u64().unwrap() > 0, "{line:#}");
    assert!(edge["lane_fanout_sends"].as_u64().unwrap() >= 40 * parts, "{line:#}");
    assert!(line["udp_sent_parts"].as_u64().unwrap() >= parts, "{line:#}");
}

#[test]
fn the_fan_out_and_send_twin_flags_default_on_and_can_be_turned_off() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert_eq!(args.client_udp_fanout_threads, 2);
    assert_eq!(args.client_udp_fanout_inline_sends, 32);
    assert!(args.client_udp_send_twins);
    let args = Args::try_parse_from([
        "av-mesh",
        "--client-udp-fanout-threads",
        "0",
        "--client-udp-fanout-inline-sends",
        "8",
        "--client-udp-send-twins",
        "false",
    ])
    .unwrap();
    assert_eq!(args.client_udp_fanout_threads, 0);
    assert_eq!(args.client_udp_fanout_inline_sends, 8);
    assert!(!args.client_udp_send_twins);
}

#[test]
#[ignore = "a measurement: run it alone, in release"]
fn edge_capacity_sweep() {
    let listeners = std::env::var("EDGE_CAP_LISTENERS").unwrap_or_else(|_| "100,200,400".into());
    let payloads = std::env::var("EDGE_CAP_PAYLOAD").unwrap_or_else(|_| "opus1".into());
    let edge_cpus: Vec<usize> = (0..env_u64("EDGE_CAP_EDGE_CPUS", 2) as usize)
        .map(|index| 2 + index)
        .collect();
    let fanout_threads = std::env::var("EDGE_CAP_FANOUT_THREADS")
        .ok()
        .and_then(|value| value.parse().ok());
    let mut lines = Vec::new();
    for payload in payloads.split(',') {
        for count in listeners.split(',') {
            let line = run(
                &Run {
                    listeners: count.trim().parse().unwrap(),
                    payload: payload_of(payload.trim()),
                    seconds: env_u64("EDGE_CAP_SECONDS", 8),
                    workers: env_u64("EDGE_CAP_THREADS", 4) as usize,
                    edge_cpus: edge_cpus.clone(),
                    coalesce: Duration::from_micros(env_u64("EDGE_CAP_COALESCE_US", 1_000)),
                    fanout_threads,
                    inline_sends: std::env::var("EDGE_CAP_INLINE_SENDS")
                        .ok()
                        .and_then(|value| value.parse().ok()),
                    send_twins: std::env::var("EDGE_CAP_SEND_TWINS").as_deref() != Ok("false"),
                },
                44,
            );
            println!("{}", serde_json::to_string(&line).unwrap());
            lines.push(line);
        }
    }
    if let Ok(path) = std::env::var("EDGE_CAP_OUT") {
        let text: String = lines
            .iter()
            .map(|line| serde_json::to_string(line).unwrap() + "\n")
            .collect();
        std::fs::write(path, text).unwrap();
    }
}

// ------------------------------------------------------------------ the kernel's cost a datagram

/// The kernel's cost of a fan-out send, a datagram (an ignored
/// measurement): one thread, pinned, hands `FANOUT_COST_BYTES`-byte
/// datagrams to `FANOUT_COST_DESTS` destinations in `sendmmsg` calls of
/// [`fast_plane::sys::SEND_BATCH`], as the fan-out does, for
/// `FANOUT_COST_SECONDS`, and prints its processor time and the softirq
/// time of its CPU a datagram. `FANOUT_COST_TARGET=loopback` sends to
/// sockets of this process (drained by another thread: the receivers'
/// kernel path runs in the sender's softirq, as in the capacity test);
/// `dummy` sends to 10.99.0.0/16 through a dummy device, which the caller
/// makes in a network namespace of its own, so that the cost is the
/// sender's alone, as on a NIC (less its driver):
/// `unshare -rn sh -c 'ip link set lo up; ip link add d0 type dummy;
/// ip link set d0 up; ip addr add 10.99.0.1/16 dev d0;
/// tc qdisc add dev d0 root fq; <test binary> fanout_send_cost --ignored
/// --nocapture'`.
#[test]
#[ignore = "a measurement: run it alone, in release"]
fn fanout_send_cost_per_datagram() {
    let dests = env_u64("FANOUT_COST_DESTS", 400) as usize;
    let bytes = env_u64("FANOUT_COST_BYTES", 67) as usize;
    let seconds = env_u64("FANOUT_COST_SECONDS", 3);
    let cpu = env_u64("FANOUT_COST_CPU", 5) as usize;
    let loopback = std::env::var("FANOUT_COST_TARGET").as_deref() != Ok("dummy");
    let stop = Arc::new(AtomicBool::new(false));
    let mut drainer = None;
    let targets: Vec<SocketAddr> = if loopback {
        let sockets: Vec<std::net::UdpSocket> = (0..dests)
            .map(|_| {
                let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
                socket.set_nonblocking(true).unwrap();
                socket
            })
            .collect();
        let targets = sockets.iter().map(|s| s.local_addr().unwrap()).collect();
        let stop = Arc::clone(&stop);
        drainer = Some(std::thread::spawn(move || {
            pin(0, &[cpu + 1]);
            let mut batch = RecvBatch::new();
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(1));
                for socket in &sockets {
                    while batch.recv(socket).unwrap_or(0) == fast_plane::sys::RECV_BATCH {}
                }
            }
        }));
        targets
    } else {
        (0..dests)
            .map(|index| SocketAddr::from(([10, 99, 1 + (index / 250) as u8, 1 + (index % 250) as u8], 40_000)))
            .collect()
    };
    let socket = std::net::UdpSocket::bind(if loopback { "127.0.0.1:0" } else { "10.99.0.1:0" })
        .unwrap();
    let _ = socket2::SockRef::from(&socket).set_send_buffer_size(4 << 20);
    let fd = socket.as_raw_fd();
    let payload = vec![0x5a_u8; bytes];
    pin(0, &[cpu]);
    let started = Instant::now();
    let (mut sent, mut refused, mut busy) = (0_u64, 0_u64, Duration::ZERO);
    // A round a millisecond: each destination one datagram, as a 1 ms part.
    let mut round = 0_u64;
    while started.elapsed() < Duration::from_secs(seconds) {
        let due = started + Duration::from_micros(round * 1_000);
        while Instant::now() < due {
            std::hint::spin_loop();
        }
        round += 1;
        // The time in sendmmsg is the sender's processor time (the call
        // never waits), with whatever softirq work runs inline in it.
        let call = Instant::now();
        let count = fast_plane::sys::send_batch(fd, targets.len(), |index| {
            (payload.as_slice(), targets[index])
        })
        .unwrap_or(0);
        busy += call.elapsed();
        sent += count as u64;
        refused += (targets.len() - count) as u64;
    }
    let wall = started.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    if let Some(drainer) = drainer {
        drainer.join().unwrap();
    }
    println!(
        "{}",
        serde_json::json!({
            "target": if loopback { "loopback" } else { "dummy" },
            "destinations": dests,
            "bytes": bytes,
            "seconds": wall,
            "datagrams": sent,
            "refused": refused,
            "busy_fraction": busy.as_secs_f64() / wall,
            "us_per_datagram": busy.as_secs_f64() * 1e6 / sent.max(1) as f64,
        })
    );
}
