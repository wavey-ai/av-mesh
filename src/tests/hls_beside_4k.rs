//! LL-HLS audio beside 4K on one edge (cmpd7's sweep): the referee's 1 ms
//! PCM listener over LL-HLS fell seconds behind on an edge that carried 4K
//! at 160 to 320 Mbit/s, while the UDP listeners beside it did not.
//!
//! An edge fed by the fast plane, squeezed onto two CPUs as a
//! `g6-dedicated-2` is: the class threads pinned one each, the node's
//! runtime (the tees, the cache-fed lane) and the runtime that serves
//! readers on both. A 4K stream at `HB_MBPS` (247) arrives as the
//! forwarders send it, 24 frames a second of 1,316-byte symbols, with
//! `HB_UDP_CLIENTS` (1) fast-lane UDP clients and `HB_VIDEO_READERS` (1)
//! LL-HLS readers of it over HTTP/2 and TLS; a 1 ms PCM stream arrives from
//! both forwarders, and `HB_AUDIO_READERS` (2) LL-HLS readers ask for each
//! of its parts by number, `HB_WINDOW` (5) requests in flight, as the
//! referee's probe does (`ntcmp --mode chase --window 5`). The source and
//! the readers run on other CPUs: they are other hosts.
//!
//! The test is unprivileged, so it cannot raise a thread above the
//! process's priority as the deploy does (the audio class thread
//! SCHED_FIFO 10, the video class thread at nice -10, the fan-out at -5).
//! The node's threads keep the deploy's order of priority instead, laid
//! from the process's own nice up (`test_nice`). `HB_HOGS` (2) threads at
//! the node runtime's priority, busy `HB_HOG_DUTY` (0.5) of the time, stand
//! for the rest of a Linode edge's work, which ran its two CPUs 65 to 100 %
//! busy at 240 Mbit/s.
//!
//! What it measures, for each audio part a reader asked for before the
//! edge had it (the request waited at the edge): the edge's commit to the
//! edge's reply stamp (`x-needletail-reply-unix-us`, the handler's answer)
//! and to the reader's last byte (on loopback, the response leaving the
//! edge). For every part: the request's own wait at the edge (from the
//! later of its arrival and the commit to the reply), and the parts a
//! second each reader took.
//!
//! The measurement is ignored; run it alone, in release:
//! `cargo test --release hls_beside_4k -- --ignored --nocapture
//! --test-threads 1`. Env: HB_CPUS ("a,b", default the last two of the
//! box), HB_MBPS, HB_SECONDS (8), HB_AUDIO_READERS, HB_WINDOW,
//! HB_VIDEO_READERS, HB_UDP_CLIENTS, HB_PASSES ("0,247": 4K off, then
//! on).

use super::compact_lane::Payload;
use super::fast_lane::LaneEdge;
use super::fast_lease::loopback;
use super::video_throughput::{
    make_frames_shaped, run_client, send_frames_at, threads_cpu_ns, FrameShape, Spread,
};
use super::*;
use fast_plane::relay::RelayClassConfig;
use futures_util::stream::FuturesUnordered;
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http2::SendRequest;
use serde_json::Value;
use socket2::SockRef;

const VIDEO: u64 = 1;
const AUDIO: u64 = 2;
/// The symbol of the edge tests (`video_throughput`).
const SYMBOL_BYTES: u16 = 1_088;
const FPS: u64 = 24;
/// The video class thread's nice in the deploy (`fast_plane::relay`).
const DEPLOY_VIDEO_NICE: i32 = -10;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_list(name: &str, default: &[u64]) -> Vec<u64> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|item| item.trim().parse().ok())
                .collect()
        })
        .unwrap_or_else(|| default.to_vec())
}

/// One run.
#[derive(Debug, Clone, Copy)]
pub(super) struct Options {
    /// 4K media rate, Mbit/s; 0: no 4K.
    pub(super) mbps: u64,
    pub(super) seconds: u64,
    pub(super) audio_readers: usize,
    pub(super) window: usize,
    pub(super) video_readers: usize,
    pub(super) udp_clients: usize,
    pub(super) hogs: usize,
    pub(super) hog_duty: f64,
    /// The node's two CPUs.
    pub(super) cpus: [usize; 2],
    /// The edge's loopback address, 127.0.0.<ip>.
    pub(super) ip: u8,
}

impl Options {
    pub(super) fn from_env(mbps: u64, ip: u8) -> Self {
        let count = std::thread::available_parallelism().map_or(4, |count| count.get());
        let cpus = std::env::var("HB_CPUS")
            .ok()
            .and_then(|value| {
                let mut cpus = value.split(',').filter_map(|cpu| cpu.trim().parse().ok());
                Some([cpus.next()?, cpus.next()?])
            })
            .unwrap_or([count.saturating_sub(2), count.saturating_sub(1)]);
        Self {
            mbps,
            seconds: env_u64("HB_SECONDS", 8),
            audio_readers: env_u64("HB_AUDIO_READERS", 2) as usize,
            window: env_u64("HB_WINDOW", 5) as usize,
            video_readers: env_u64("HB_VIDEO_READERS", 1) as usize,
            udp_clients: env_u64("HB_UDP_CLIENTS", 1) as usize,
            hogs: env_u64("HB_HOGS", 2) as usize,
            hog_duty: std::env::var("HB_HOG_DUTY")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0.5),
            cpus,
            ip,
        }
    }
}

// ------------------------------------------------------------------ CPUs

fn pin(tid: libc::pid_t, cpus: &[usize]) {
    // SAFETY: a zeroed cpu_set_t is the empty set; CPU_SET writes within it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for cpu in cpus {
            libc::CPU_SET(*cpu, &mut set);
        }
        libc::sched_setaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn other_cpus(node: [usize; 2]) -> Vec<usize> {
    let count = std::thread::available_parallelism().map_or(4, |count| count.get());
    (0..count).filter(|cpu| !node.contains(cpu)).collect()
}

/// This process's threads: id and name.
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

/// The deploy's nice of a node thread by name, `None` for a thread that is
/// not the node's.
fn deploy_nice(name: &str) -> Option<i32> {
    if name.starts_with("needletail-fast") {
        // The class threads (their names cut to 15 bytes): the video
        // thread at nice -10 in the deploy, the audio thread SCHED_FIFO 10
        // there, and here the most this test can give either.
        Some(DEPLOY_VIDEO_NICE)
    } else if name.starts_with("nt-fanout") {
        Some(fast_plane::edge_clients::DEFAULT_FANOUT_NICE)
    } else if name.starts_with("needletail-http") {
        Some(reader_nice_in_deploy())
    } else if ["edge-rt", "cache-mesh", "needletail-cut", "nt-window", "needletail-tee"]
        .iter()
        .any(|prefix| name.starts_with(prefix))
    {
        Some(0)
    } else {
        None
    }
}

/// This process's nice: no thread of it can run below it.
fn process_nice() -> i32 {
    // SAFETY: getpriority of this process.
    unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) }
}

/// The nice that stands in here for the deploy's `deploy`: the deploy's
/// range, -10 (the video class thread) to the readers' runtime, laid from
/// this process's own nice up, squeezed when it does not fit below 19 (the
/// build throttle runs tests at nice 10: -10, -5, 0 and 5 become 10, 13,
/// 16 and 19). The order of the weights stays the deploy's.
fn test_nice(deploy: i32) -> i32 {
    let base = process_nice();
    let top = reader_nice_in_deploy();
    let span = (top + 10).max(1);
    let room = (19 - base).clamp(0, span);
    base + ((deploy + 10).clamp(0, span) * room + span / 2) / span
}

/// The readers' runtime's nice in the deploy (`HB_READER_NICE` to try
/// another).
fn reader_nice_in_deploy() -> i32 {
    std::env::var("HB_READER_NICE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(READER_THREAD_NICE)
}

/// The readers' threads' slice (`HB_READER_SLICE_US`, the node's default
/// `--reader-slice-us` without it; 0: the kernel's).
fn reader_slice_us() -> u64 {
    env_u64("HB_READER_SLICE_US", DEFAULT_READER_SLICE_US)
}

/// The kernel's base slice on a two-vCPU guest (`HB_GUEST_SLICE_US`,
/// 1,500: 0.75 ms times 1 + log2 of the CPUs, Linux 6.12), given here to
/// every thread of the node that has no slice of its own; this box's own is
/// longer (more CPUs).
fn guest_slice_us() -> u64 {
    env_u64("HB_GUEST_SLICE_US", 1_500)
}

/// The kernel time slice of thread `tid`, µs (`/proc/<tid>/sched`).
fn thread_slice_us(tid: libc::pid_t) -> Option<f64> {
    let text = std::fs::read_to_string(format!("/proc/self/task/{tid}/sched")).ok()?;
    let line = text.lines().find(|line| line.trim_start().starts_with("se.slice"))?;
    let ns: f64 = line.split(':').nth(1)?.trim().parse().ok()?;
    Some(ns / 1_000.0)
}

/// Give every node thread the deploy's nice (as [`test_nice`] maps it) and
/// the node's CPUs (a class thread keeps its own CPU).
fn place_node_threads(node: [usize; 2]) {
    for (tid, name) in threads() {
        let Some(nice) = deploy_nice(&name) else {
            continue;
        };
        // SAFETY: setpriority on a thread of this process.
        unsafe { libc::setpriority(libc::PRIO_PROCESS, tid as libc::id_t, test_nice(nice)) };
        if !name.starts_with("needletail-fast") {
            pin(tid, &node);
        }
        if !name.starts_with("needletail-http") {
            set_slice_of(tid, guest_slice_us());
        }
    }
}

/// [`set_thread_slice`] for another thread of this process: run on a
/// helper that the kernel lets act on `tid` (a thread may only set its own
/// attributes through `sched_setattr` with pid 0, so the helper names it).
fn set_slice_of(tid: libc::pid_t, slice_us: u64) {
    #[repr(C)]
    struct SchedAttr {
        size: u32,
        sched_policy: u32,
        sched_flags: u64,
        sched_nice: i32,
        sched_priority: u32,
        sched_runtime: u64,
        sched_deadline: u64,
        sched_period: u64,
        sched_util_min: u32,
        sched_util_max: u32,
    }
    // SAFETY: getpriority and sched_setattr on a thread of this process with
    // a valid `sched_attr`.
    unsafe {
        let attr = SchedAttr {
            size: std::mem::size_of::<SchedAttr>() as u32,
            sched_policy: libc::SCHED_OTHER as u32,
            sched_flags: 0,
            sched_nice: libc::getpriority(libc::PRIO_PROCESS, tid as libc::id_t),
            sched_priority: 0,
            sched_runtime: slice_us * 1_000,
            sched_deadline: 0,
            sched_period: 0,
            sched_util_min: 0,
            sched_util_max: 0,
        };
        libc::syscall(libc::SYS_sched_setattr, tid, &attr as *const SchedAttr, 0_u32);
    }
}

/// Other work of the node (its cache mesh, its other clients and streams,
/// the kernel's network work for them): `hogs` threads on the node's CPUs
/// at the node runtime's priority, each busy `duty` of every 4 ms.
fn start_hogs(
    node: [usize; 2],
    hogs: usize,
    duty: f64,
    stop: &Arc<AtomicBool>,
) -> Vec<std::thread::JoinHandle<()>> {
    (0..hogs)
        .map(|index| {
            let stop = Arc::clone(stop);
            std::thread::Builder::new()
                .name(format!("hb-hog-{index}"))
                .spawn(move || {
                    pin(0, &[node[index % 2]]);
                    // SAFETY: setpriority on this thread.
                    unsafe { libc::setpriority(libc::PRIO_PROCESS, 0, test_nice(0)) };
                    set_slice_of(0, guest_slice_us());
                    let period = Duration::from_millis(4);
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
        .collect()
}

/// Busy jiffies and all jiffies of `cpus`.
fn cpu_jiffies(cpus: &[usize]) -> (u64, u64) {
    let text = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let (mut busy, mut all) = (0, 0);
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let Some(cpu) = fields
            .next()
            .and_then(|name| name.strip_prefix("cpu"))
            .and_then(|n| n.parse::<usize>().ok())
        else {
            continue;
        };
        if !cpus.contains(&cpu) {
            continue;
        }
        let values: Vec<u64> = fields.filter_map(|value| value.parse().ok()).collect();
        let total: u64 = values.iter().sum();
        let idle = values.get(3).copied().unwrap_or(0) + values.get(4).copied().unwrap_or(0);
        busy += total - idle;
        all += total;
    }
    (busy, all)
}

const GROUPS: [(&str, &[&str]); 5] = [
    ("class_threads", &["needletail-fast"]),
    ("node_runtime", &["edge-rt", "cache-mesh", "needletail-tee", "nt-window"]),
    ("readers_runtime", &["needletail-http"]),
    ("fanout", &["nt-fanout", "needletail-cut"]),
    ("hogs", &["hb-hog"]),
];

fn group_cpu_ns() -> Vec<u64> {
    GROUPS
        .iter()
        .map(|(_, prefixes)| threads_cpu_ns(prefixes))
        .collect()
}

// ------------------------------------------------------------------ the readers

/// One part as a reader got it: unix µs.
#[derive(Debug, Clone, Copy, Default)]
struct Got {
    ok: bool,
    sent: u64,
    published: u64,
    committed: u64,
    reply: u64,
    complete: u64,
    bytes: usize,
}

fn header_us(response: &http::Response<hyper::body::Incoming>, name: &str) -> u64 {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

/// An HTTP/2 connection to the edge, over TLS, as the probe makes one.
async fn connect(
    port: u16,
    connector: &tokio_rustls::TlsConnector,
) -> SendRequest<Empty<Bytes>> {
    let tcp = tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .unwrap();
    tcp.set_nodelay(true).unwrap();
    let name = rustls::pki_types::ServerName::try_from("local.wavey.ai").unwrap();
    let tls = connector.connect(name, tcp).await.unwrap();
    let (sender, connection) = hyper::client::conn::http2::handshake(
        hyper_util::rt::TokioExecutor::new(),
        TokioIo::new(tls),
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    sender
}

async fn fetch(mut sender: SendRequest<Empty<Bytes>>, port: u16, path: String) -> Got {
    let sent = now_unix_us();
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("https://local.wavey.ai:{port}{path}"))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let mut got = Got {
        sent,
        ..Got::default()
    };
    let Ok(response) = sender.send_request(request).await else {
        return got;
    };
    got.ok = response.status() == StatusCode::OK;
    got.published = header_us(&response, "x-needletail-published-unix-us");
    got.committed = header_us(&response, "x-needletail-edge-committed-unix-us");
    got.reply = header_us(&response, "x-needletail-reply-unix-us");
    if let Ok(body) = response.into_body().collect().await {
        got.bytes = body.to_bytes().len();
    }
    got.complete = now_unix_us();
    got
}

/// A chase reader: `window` requests in flight for the next parts by
/// number, from `first`, until `stop`. Each answer frees a slot for the
/// next part, as `ntcmp`'s chase window does.
async fn chase(
    sender: SendRequest<Empty<Bytes>>,
    port: u16,
    stream_id: u64,
    extension: &'static str,
    first: u64,
    window: usize,
    stop: Arc<AtomicBool>,
) -> Vec<Got> {
    let mut inflight = FuturesUnordered::new();
    let mut next = first;
    let mut got = Vec::new();
    loop {
        while inflight.len() < window && !stop.load(Ordering::Relaxed) {
            inflight.push(fetch(
                sender.clone(),
                port,
                format!("/live/{stream_id}/part{next}.{extension}"),
            ));
            next += 1;
        }
        match inflight.next().await {
            Some(part) => got.push(part),
            None => return got,
        }
    }
}

// ------------------------------------------------------------------ the sources

/// 1 ms PCM parts from both forwarders at their pace, from part 0.
fn audio_publisher(
    edge: &LaneEdge,
    stop: Arc<AtomicBool>,
    cpus: Vec<usize>,
) -> std::thread::JoinHandle<()> {
    let fds = [edge.forwarder_fd(0), edge.forwarder_fd(1)];
    let ports = [edge.relay_port(0, false), edge.relay_port(1, false)];
    std::thread::Builder::new()
        .name("hb-audio-source".into())
        .spawn(move || {
            pin(0, &cpus);
            let payload = Payload::Pcm;
            let started = Instant::now();
            let mut sequence = 0;
            while !stop.load(Ordering::Relaxed) {
                let due = started + Duration::from_micros(payload.part_us() * sequence);
                let now = Instant::now();
                if due > now + Duration::from_micros(200) {
                    std::thread::sleep(due - now - Duration::from_micros(150));
                }
                while Instant::now() < due {
                    std::hint::spin_loop();
                }
                let (envelope, _) = payload.envelope(AUDIO, sequence);
                let (sources, _) = payload.symbols(&envelope);
                for symbol in &sources {
                    for (fd, port) in fds.iter().zip(ports) {
                        let _ = fast_plane::sys::send_batch(*fd, 1, |_| (symbol.as_slice(), port));
                    }
                }
                sequence += 1;
            }
        })
        .unwrap()
}

// ------------------------------------------------------------------ the run

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

fn spread_ms(mut values: Vec<f64>) -> Value {
    values.sort_by(f64::total_cmp);
    let round = |value: f64| (value * 100.0).round() / 100.0;
    serde_json::json!({
        "n": values.len(),
        "p50": round(quantile(&values, 0.5)),
        "p99": round(quantile(&values, 0.99)),
        "p999": round(quantile(&values, 0.999)),
        "max": round(values.last().copied().unwrap_or(f64::NAN)),
    })
}

/// What one run measured, as one JSON object.
#[derive(Debug)]
pub(super) struct HlsRun {
    pub(super) json: Value,
    /// Audio parts asked for before the commit: commit to reply stamp,
    /// commit to the reader's last byte, ms, sorted.
    pub(super) commit_to_reply_ms: Vec<f64>,
    pub(super) commit_to_complete_ms: Vec<f64>,
    /// Audio parts a second each reader took, the slowest reader's.
    pub(super) slowest_reader_parts_per_second: f64,
    pub(super) audio_missing: usize,
    pub(super) video_parts: usize,
}

impl HlsRun {
    pub(super) fn p99_commit_to_complete_ms(&self) -> f64 {
        quantile(&self.commit_to_complete_ms, 0.99)
    }

    pub(super) fn p99_commit_to_reply_ms(&self) -> f64 {
        quantile(&self.commit_to_reply_ms, 0.99)
    }
}

pub(super) fn hls_beside_4k(options: Options) -> HlsRun {
    if std::env::var_os("HB_PTRACER").is_some() {
        // SAFETY: prctl with integer arguments only (eu-stack may sample).
        unsafe { libc::prctl(libc::PR_SET_PTRACER, libc::PR_SET_PTRACER_ANY, 0, 0, 0) };
        eprintln!("hls_beside_4k pid {}", std::process::id());
    }
    let node = options.cpus;
    let away = other_cpus(node);
    let edge_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("edge-rt")
        .on_thread_start(move || pin(0, &node))
        .enable_all()
        .build()
        .unwrap();
    let edge = edge_rt.block_on(async {
        tokio::spawn(LaneEdge::start_configured(
            loopback(options.ip),
            Duration::ZERO,
            64,
            true,
            move |config: &mut RelayClassConfig| {
                config.cpus = [Some(node[0]), Some(node[1])];
                // The deploy's tee rings (LaneEdge's 8,192 entries hold no
                // 4K frame of these rates).
                config.tee_capacity = fast_plane::relay::DEFAULT_TEE_CAPACITY;
            },
        ))
        .await
        .unwrap()
    });
    // The readers' runtime as the node makes it, from a thread on the
    // node's CPUs.
    let readers_rt = std::thread::spawn(move || {
        pin(0, &node);
        reader_runtime(
            2,
            match reader_slice_us() {
                0 => guest_slice_us(),
                slice => slice,
            },
        )
        .unwrap()
    })
    .join()
    .unwrap();
    let (cert, key) = tls_pair_for_tests();
    let port = unused_tcp_loopback_addr().port();
    let server = H2H3Server::builder()
        .with_tls(cert.clone(), key)
        .with_port(port)
        .enable_h2(true)
        .enable_h3(false)
        .with_router(Box::new(edge.router.clone()))
        .build()
        .unwrap();
    let server = readers_rt.block_on(async move {
        let handle = tokio::spawn(async move { server.start().await })
            .await
            .unwrap()
            .unwrap();
        let _ = (&mut { handle.ready_rx }).await;
        (handle.shutdown_tx, handle.finished_rx)
    });

    // The 4K source: part 0 makes the stream known, then the rest paced.
    let bits_per_second = options.mbps * 1_000_000;
    let warmup = FPS as usize * 2;
    let measured = (options.seconds * FPS) as usize;
    let frames = if options.mbps > 0 {
        make_frames_shaped(
            VIDEO,
            0,
            1 + warmup + measured + FPS as usize,
            bits_per_second,
            FrameShape {
                fps: FPS,
                symbol_bytes: SYMBOL_BYTES,
                repair_per_sources: 20,
            },
        )
    } else {
        Vec::new()
    };
    let video_fd = edge.forwarder_fd(0);
    let _ = SockRef::from(edge.forwarder_socket(0)).set_send_buffer_size(32 << 20);
    let video_target = edge.relay_port(0, true);
    if let Some(first) = frames.first() {
        send_frames_at(
            video_fd,
            video_target,
            std::slice::from_ref(first),
            bits_per_second,
            // Paced: a whole 4K frame at once overflows the class socket.
            Spread::Paced,
            Instant::now(),
            FPS,
            SYMBOL_BYTES,
        );
        edge_rt.block_on(async {
            for _ in 0..300 {
                if edge.cache.get_part_for_stream_id(VIDEO, 0).await.is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let plane = edge.cache.relay_ingress_snapshot().fast_plane;
            panic!(
                "4K part 0 not committed: video received {} tee_dropped {} kernel_dropped {} invalid {} not_admitted {} truncated {}",
                plane.video_received, plane.tee_dropped, plane.kernel_dropped, plane.invalid, plane.not_admitted, plane.truncated
            );
        });
    }
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let mut udp_clients = Vec::new();
    if options.mbps > 0 {
        for index in 0..options.udp_clients {
            let (tx, rx) = std::sync::mpsc::channel();
            let (port, stop, measuring) = (
                edge.client_port,
                Arc::clone(&stop),
                Arc::clone(&measuring),
            );
            let away = away.clone();
            udp_clients.push(
                std::thread::Builder::new()
                    .name(format!("hb-udp-{index}"))
                    .spawn(move || {
                        pin(0, &away);
                        run_client(port, VIDEO, stop, measuring, tx)
                    })
                    .unwrap(),
            );
            rx.recv_timeout(Duration::from_secs(5))
                .expect("the edge subscribes the UDP client");
        }
    }
    let audio_stop = Arc::new(AtomicBool::new(false));
    let publisher = audio_publisher(&edge, Arc::clone(&audio_stop), away.clone());
    edge_rt.block_on(async { wait_for_part(&edge.cache, AUDIO, 0).await });
    place_node_threads(node);
    let hogs = start_hogs(node, options.hogs, options.hog_duty, &stop);
    if std::env::var_os("HB_SHOW_THREADS").is_some() {
        for (tid, name) in threads() {
            let status = std::fs::read_to_string(format!("/proc/self/task/{tid}/status")).unwrap_or_default();
            let cpus = status.lines().find(|line| line.starts_with("Cpus_allowed_list")).unwrap_or("");
            // SAFETY: getpriority on a thread of this process.
            let nice = unsafe { libc::getpriority(libc::PRIO_PROCESS, tid as libc::id_t) };
            eprintln!("thread {tid} {name} nice {nice} {cpus} slice_us {:?}", thread_slice_us(tid));
        }
    }

    // The readers: other hosts, on the other CPUs.
    let client_rt = {
        let away = away.clone();
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("hb-reader")
            .on_thread_start(move || pin(0, &away))
            .enable_all()
            .build()
            .unwrap()
    };
    let connector = {
        use base64::Engine;
        let pem = base64::engine::general_purpose::STANDARD
            .decode(&cert)
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        for certificate in rustls_pemfile::certs(&mut &pem[..]) {
            roots.add(certificate.unwrap()).unwrap();
        }
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        config.alpn_protocols = vec![b"h2".to_vec()];
        tokio_rustls::TlsConnector::from(Arc::new(config))
    };
    let reader_stop = Arc::new(AtomicBool::new(false));
    let (audio_tasks, video_tasks) = client_rt.block_on(async {
        let head = |stream_id| {
            let cache = Arc::clone(&edge.cache);
            async move {
                cache
                    .stream_position_for_id(stream_id)
                    .await
                    .map_or(0, |(_, last)| last as u64)
            }
        };
        let mut audio = Vec::new();
        for _ in 0..options.audio_readers {
            let sender = connect(port, &connector).await;
            let first = head(AUDIO).await + 1;
            audio.push(tokio::spawn(chase(
                sender,
                port,
                AUDIO,
                "bin",
                first,
                options.window,
                Arc::clone(&reader_stop),
            )));
        }
        let mut video = Vec::new();
        if options.mbps > 0 {
            for _ in 0..options.video_readers {
                let sender = connect(port, &connector).await;
                video.push(tokio::spawn(chase(
                    sender,
                    port,
                    VIDEO,
                    "mp4",
                    1,
                    2,
                    Arc::clone(&reader_stop),
                )));
            }
        }
        (audio, video)
    });

    let start = Instant::now() + Duration::from_millis(50);
    let frames = frames.get(1..).unwrap_or_default();
    let (cpu_before, cpu_after, jiffies_before, jiffies_after, elapsed, window_us) =
        std::thread::scope(|scope| {
            let away = away.clone();
            let source = scope.spawn(move || {
                pin(0, &away);
                send_frames_at(
                    video_fd,
                    video_target,
                    frames,
                    bits_per_second,
                    Spread::Paced,
                    start,
                    FPS,
                    SYMBOL_BYTES,
                )
            });
            std::thread::sleep(
                (start + Duration::from_nanos(1_000_000_000 / FPS) * warmup as u32)
                    .saturating_duration_since(Instant::now()),
            );
            measuring.store(true, Ordering::Relaxed);
            let from_us = now_unix_us();
            let before = (group_cpu_ns(), cpu_jiffies(&node), Instant::now());
            std::thread::sleep(Duration::from_secs(options.seconds));
            let after = (group_cpu_ns(), cpu_jiffies(&node), Instant::now());
            let to_us = now_unix_us();
            measuring.store(false, Ordering::Relaxed);
            if options.mbps > 0 {
                source.join().unwrap();
            }
            (
                before.0,
                after.0,
                before.1,
                after.1,
                after.2.duration_since(before.2).as_secs_f64(),
                from_us..to_us,
            )
        });
    reader_stop.store(true, Ordering::Relaxed);
    let (audio, video) = client_rt.block_on(async {
        let mut audio = Vec::new();
        for task in audio_tasks {
            audio.push(task.await.unwrap());
        }
        let mut video = Vec::new();
        for task in video_tasks {
            video.extend(task.await.unwrap());
        }
        (audio, video)
    });
    audio_stop.store(true, Ordering::Relaxed);
    publisher.join().unwrap();
    stop.store(true, Ordering::Relaxed);
    for hog in hogs {
        hog.join().unwrap();
    }
    let udp_whole: Vec<usize> = udp_clients
        .into_iter()
        .map(|client| client.join().unwrap().complete.len())
        .collect();

    // What the audio readers got in the measured window.
    let in_window = |got: &Got| got.committed > 0 && window_us.contains(&got.committed);
    let mut commit_to_reply = Vec::new();
    let mut commit_to_complete = Vec::new();
    let mut held = Vec::new();
    let mut published_to_commit = Vec::new();
    let mut missing = 0;
    let mut slowest = f64::INFINITY;
    for reader in &audio {
        let window: Vec<&Got> = reader.iter().filter(|got| in_window(got)).collect();
        missing += reader
            .iter()
            .filter(|got| !got.ok && window_us.contains(&got.sent))
            .count();
        slowest = slowest.min(window.len() as f64 / options.seconds as f64);
        for got in window {
            let ms = |from: u64, to: u64| to.saturating_sub(from) as f64 / 1_000.0;
            if got.sent < got.committed {
                commit_to_reply.push(ms(got.committed, got.reply));
                commit_to_complete.push(ms(got.committed, got.complete));
            }
            held.push(ms(got.sent.max(got.committed), got.reply));
            if got.published > 0 {
                published_to_commit.push(ms(got.published, got.committed));
            }
        }
    }
    let video_window: Vec<&Got> = video.iter().filter(|got| in_window(got)).collect();
    let video_parts = video_window.len();
    let video_late: Vec<f64> = video_window
        .iter()
        .map(|got| got.complete.saturating_sub(got.committed) as f64 / 1_000.0)
        .collect();
    let video_mbps = video_window.iter().map(|got| got.bytes).sum::<usize>() as f64 * 8.0
        / options.seconds as f64
        / 1e6;
    let cpu: serde_json::Map<String, Value> = GROUPS
        .iter()
        .zip(cpu_before.iter().zip(&cpu_after))
        .map(|((name, _), (before, after))| {
            let share = after.saturating_sub(*before) as f64 / 1e9 / elapsed * 100.0;
            ((*name).to_owned(), Value::from((share * 10.0).round() / 10.0))
        })
        .collect();
    let node_busy = (jiffies_after.0 - jiffies_before.0) as f64
        / (jiffies_after.1 - jiffies_before.1).max(1) as f64
        * 100.0;
    commit_to_reply.sort_by(f64::total_cmp);
    commit_to_complete.sort_by(f64::total_cmp);
    let json = serde_json::json!({
        "mbps": options.mbps,
        "seconds": options.seconds,
        "node_cpus": node,
        "audio_readers": options.audio_readers,
        "window": options.window,
        "hogs": format!("{}x{:.0}%", options.hogs, options.hog_duty * 100.0),
        "reader_nice_in_deploy": reader_nice_in_deploy(),
        "reader_slice_us": reader_slice_us(),
        "guest_slice_us": guest_slice_us(),
        "audio": {
            "waited_commit_to_reply_ms": spread_ms(commit_to_reply.clone()),
            "waited_commit_to_last_byte_ms": spread_ms(commit_to_complete.clone()),
            "held_at_edge_ms": spread_ms(held),
            "published_to_commit_ms": spread_ms(published_to_commit),
            "slowest_reader_parts_per_second": slowest.round(),
            "missing": missing,
        },
        "video": {
            "llhls_parts": video_parts,
            "llhls_mbps": (video_mbps * 10.0).round() / 10.0,
            "llhls_commit_to_last_byte_ms": spread_ms(video_late),
            "udp_parts_whole": udp_whole,
        },
        "node_busy_percent": node_busy.round(),
        "cpu_percent_of_one": cpu,
    });
    let _ = server.0.send(());
    edge_rt.block_on(async {
        let _ = timeout_ok(server.1).await;
    });
    readers_rt.shutdown_background();
    client_rt.shutdown_background();
    edge_rt.block_on(edge.stop());
    HlsRun {
        json,
        commit_to_reply_ms: commit_to_reply,
        commit_to_complete_ms: commit_to_complete,
        slowest_reader_parts_per_second: slowest,
        audio_missing: missing,
        video_parts,
    }
}

async fn timeout_ok<T>(future: impl std::future::Future<Output = T>) -> Option<T> {
    tokio::time::timeout(Duration::from_secs(2), future).await.ok()
}

#[test]
#[ignore = "a measurement: run it alone, in release"]
fn hls_beside_4k_measured() {
    let mut lines = Vec::new();
    for (index, mbps) in env_list("HB_PASSES", &[0, 247]).into_iter().enumerate() {
        let run = hls_beside_4k(Options::from_env(mbps, 150 + index as u8));
        println!("{}", run.json);
        lines.push(run.json.to_string());
    }
    println!("\n{}", lines.join("\n"));
}

/// The gate: on a loaded two-CPU edge, a 1 ms part a reader waits for
/// leaves within a few ms of its commit at p99 with 4K at 247 Mbit/s
/// committed and read beside it, as without it, and every reader keeps up.
#[test]
#[ignore = "pins two CPUs for half a minute: run it alone, in release"]
fn audio_parts_leave_within_a_few_ms_of_their_commit_beside_4k() {
    let without = hls_beside_4k(Options::from_env(0, 160));
    let with = hls_beside_4k(Options::from_env(247, 161));
    println!("{}\n{}", without.json, with.json);
    for run in [&without, &with] {
        assert_eq!(run.audio_missing, 0, "{}", run.json);
        assert!(run.slowest_reader_parts_per_second >= 990.0, "{}", run.json);
        assert!(run.p99_commit_to_complete_ms() < 5.0, "{}", run.json);
    }
    assert!(with.video_parts > 0, "the 4K reader read: {}", with.json);
    assert!(
        with.p99_commit_to_reply_ms() < without.p99_commit_to_reply_ms() + 3.0,
        "{}\n{}",
        without.json,
        with.json
    );
}
