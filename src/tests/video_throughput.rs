//! 4K past 164 Mbit/s (low-latency wave 5, W3), in process: a forwarder's
//! video thread carrying a 4K stream to eight children, and an edge's fast
//! client lane sending it to N clients, with the processor time each takes
//! per Mbit/s.
//!
//! The source sends one fMP4 frame a part at 60 frames a second, in the
//! contributor's 1,088-byte RelaySession symbols with a twentieth of repair,
//! as av-contrib's video lane sends it: a frame's symbols in slices of a
//! millisecond of 1.25 times the stream's rate (`W3_SOURCE=burst` sends each
//! frame at once). Every child (or client) must get every frame whole, and a
//! frame is late when its last symbol reaches a child more than a frame
//! interval after the source sent it.
//!
//! The processor time is read from the kernel per thread (schedstat): the
//! forwarder's video thread, and the runtime that runs its tee (decode,
//! SHA-256, commit). On loopback the sender's system call also delivers the
//! datagram into the receiver's socket, so a thread's time here includes
//! work that a NIC and the child's host do: it is an upper bound.
//!
//! The measurements are ignored tests; run them alone, in release:
//! `cargo test --release video_throughput -- --ignored --nocapture
//! --test-threads 1`. Env: W3_MBPS (default 200), W3_SECONDS (5),
//! W3_CHILDREN (8), W3_CLIENTS (a list, default 1,4,8), W3_SOURCE (paced or
//! burst), W3_GSO (both, on or off), W3_PACE (both, on or off: the edge's
//! per-client pacing), W3_PTRACER=1 (lets eu-stack sample the threads).
//! `tee_cost_of_a_4k_frame` times a tee's dispatch and commit of a frame.
//!
//! Rates on the wire: the media rate plus the fMP4 and envelope bytes, a
//! twentieth of repair and each datagram's RelaySession and IP/UDP headers
//! (200 Mbit/s of media is about 247 Mbit/s into a forwarder).

use super::fast_lane::LaneEdge;
use super::fast_lease::loopback;
use super::*;
use av_mesh::client_udp::{Control, ObjectKind as UdpObjectKind, ObjectReceiver, RepairPolicy};
use fast_plane::sys;
use fast_plane::view::RlsView;
use media_object::{ClockConfidence, ClockTimestamp, StageTimestamp};
use socket2::SockRef;
use std::os::fd::{AsRawFd, RawFd};

/// The contributor's video symbol (deploy renders 1,088).
const SYMBOL_BYTES: u16 = 1_088;
const FRAMES_PER_SECOND: u64 = 60;
/// IPv4 and UDP headers of each datagram on the wire.
const HEADER_BYTES: u64 = 28;
const OFFSET: u16 = 1_000;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_list(name: &str, default: &[u64]) -> Vec<u64> {
    std::env::var(name).ok().map_or_else(
        || default.to_vec(),
        |value| {
            value
                .split(',')
                .filter_map(|item| item.trim().parse().ok())
                .collect()
        },
    )
}

/// Which of a pair of settings to run: `both` (the default), `on` or `off`.
fn env_switch(name: &str) -> Vec<bool> {
    match std::env::var(name).as_deref() {
        Ok("on") => vec![true],
        Ok("off") => vec![false],
        _ => vec![true, false],
    }
}

/// Processor time of this process's threads whose names start with one of
/// `prefixes`, in nanoseconds.
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

// ------------------------------------------------------------------ the source

/// One 4K frame: its RelaySession datagrams, sources then repair.
pub(super) struct Frame {
    pub(super) part: u64,
    pub(super) datagrams: Vec<Vec<u8>>,
    pub(super) sources: usize,
}

impl Frame {
    fn wire_bytes(&self) -> u64 {
        self.datagrams
            .iter()
            .map(|datagram| datagram.len() as u64 + HEADER_BYTES)
            .sum()
    }
}

/// The canonical envelope of 4K frame `sequence` of `media_bytes`, as
/// av-contrib's `build_fmp4_media_object` builds it.
fn frame_envelope(stream_id: u64, sequence: u64, media_bytes: usize) -> Vec<u8> {
    let mut media = vec![0_u8; media_bytes];
    for (index, byte) in media.iter_mut().enumerate() {
        *byte = ((index as u64 * 31 + sequence * 7) % 251) as u8;
    }
    let now_ns = i64::try_from(now_unix_us() * 1_000).unwrap();
    let stamp = |ns: i64| {
        ClockTimestamp::new(
            ns,
            "system:realtime:av-contrib",
            ClockConfidence::estimated(1_000_000),
        )
        .unwrap()
    };
    let payload = encode_test_fmp4_slot((sequence == 0).then_some(&[7_u8; 900][..]), &media);
    let key = media_object::ObjectKey::for_payload(
        "default",
        stream_id.to_string(),
        "muxed-fmp4",
        0,
        0,
        sequence,
        1,
        &payload,
    )
    .unwrap();
    let init = media_object::ObjectKey::for_payload(
        "default",
        stream_id.to_string(),
        "muxed-fmp4-init",
        0,
        0,
        0,
        1,
        b"init",
    )
    .unwrap();
    let part_us = 1_000_000 / FRAMES_PER_SECOND;
    let mut builder = MediaObject::builder(key, ObjectKind::Media, payload.to_vec())
        .with_deadline(stamp(now_ns + 600_000_000_000))
        .with_stage_timestamp(StageTimestamp::new(Stage::Packaged, stamp(now_ns)))
        .with_stage_timestamp(StageTimestamp::new(Stage::Published, stamp(now_ns)))
        .with_keyframe(sequence % FRAMES_PER_SECOND == 0)
        .with_configuration_epoch(1)
        .with_dependency(init);
    for (name, value) in [
        ("container", "fmp4".to_string()),
        ("duration-ms", part_us.div_ceil(1_000).to_string()),
        ("duration-us", part_us.to_string()),
        ("payload-format", "fmp4-slot-v1".to_string()),
        ("scheduler-class", "video".to_string()),
        ("track-composition", "video".to_string()),
        ("video-codec", "avc1.640033".to_string()),
        ("video-width", "3840".to_string()),
        ("video-height", "2160".to_string()),
        ("hls-msn", (sequence / FRAMES_PER_SECOND).to_string()),
        ("hls-part", (sequence % FRAMES_PER_SECOND).to_string()),
        ("hls-part-target-ms", part_us.div_ceil(1_000).to_string()),
        ("hls-part-target-us", part_us.to_string()),
        ("hls-target-duration-ms", "1000".to_string()),
        ("hls-target-duration-us", "1000000".to_string()),
    ] {
        builder = builder.with_metadata(name, value.into_bytes());
    }
    media_object::encode(&builder.build().unwrap()).unwrap()
}

/// Frames `first..first + count` of a stream at `bits_per_second`, encoded
/// on every core.
pub(super) fn make_frames(
    stream_id: u64,
    first: u64,
    count: usize,
    bits_per_second: u64,
) -> Vec<Frame> {
    let media_bytes = (bits_per_second / 8 / FRAMES_PER_SECOND) as usize;
    let deadline = now_unix_us() + 600_000_000;
    let threads = std::thread::available_parallelism()
        .map_or(4, |threads| threads.get())
        .clamp(1, 16);
    let mut frames: Vec<Option<Frame>> = (0..count).map(|_| None).collect();
    let per_thread = count.div_ceil(threads).max(1);
    std::thread::scope(|scope| {
        for (chunk, slots) in frames.chunks_mut(per_thread).enumerate() {
            scope.spawn(move || {
                for (offset, slot) in slots.iter_mut().enumerate() {
                    let part = first + (chunk * per_thread + offset) as u64;
                    let envelope = frame_envelope(stream_id, part, media_bytes);
                    let sources = envelope.len().div_ceil(usize::from(SYMBOL_BYTES)) as u32;
                    let (source, repair) = fast_test_symbols(
                        &envelope,
                        if part % FRAMES_PER_SECOND == 0 {
                            relay_session::MediaPriority::VideoKey
                        } else {
                            relay_session::MediaPriority::VideoDelta
                        },
                        SYMBOL_BYTES,
                        u16::try_from(sources.max(400)).unwrap(),
                        sources.div_ceil(20),
                        deadline,
                    );
                    let sources = source.len();
                    let mut datagrams = source;
                    datagrams.extend(repair);
                    *slot = Some(Frame {
                        part,
                        datagrams,
                        sources,
                    });
                }
            });
        }
    });
    frames.into_iter().map(Option::unwrap).collect()
}

/// How the source spreads a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Spread {
    /// In slices of a millisecond at 1.25 times the stream's rate, as the
    /// contributor's paced video lane sends.
    Paced,
    /// The whole frame at once.
    Burst,
}

fn spread() -> Spread {
    match std::env::var("W3_SOURCE").as_deref() {
        Ok("burst") => Spread::Burst,
        _ => Spread::Paced,
    }
}

/// Send `frames` from `fd` to `target` at 60 frames a second from `start`,
/// never waiting on a full socket for long. Returns when each frame's last
/// datagram was handed to the kernel.
fn send_frames(
    fd: RawFd,
    target: SocketAddr,
    frames: &[Frame],
    bits_per_second: u64,
    spread: Spread,
    start: Instant,
) -> Vec<Instant> {
    let interval = Duration::from_nanos(1_000_000_000 / FRAMES_PER_SECOND);
    // A millisecond of 1.25 times the rate, in datagrams.
    let slice_bytes = (bits_per_second as f64 * 1.25 / 8.0 / 1_000.0) as usize;
    let mut sent_last = Vec::with_capacity(frames.len());
    for (index, frame) in frames.iter().enumerate() {
        let due = start + interval * index as u32;
        let per_slice = match spread {
            Spread::Burst => frame.datagrams.len(),
            Spread::Paced => (slice_bytes / (usize::from(SYMBOL_BYTES) + 60)).max(1),
        };
        for (slice, datagrams) in frame.datagrams.chunks(per_slice).enumerate() {
            let slice_due = due + Duration::from_millis(slice as u64);
            let now = Instant::now();
            if slice_due > now {
                std::thread::sleep(slice_due - now);
            }
            let mut sent = 0;
            while sent < datagrams.len() {
                match sys::send_batch(fd, datagrams.len() - sent, |index| {
                    (&datagrams[sent + index][..], target)
                }) {
                    Ok(0) | Err(_) => std::thread::sleep(Duration::from_micros(50)),
                    Ok(taken) => sent += taken,
                }
            }
        }
        sent_last.push(Instant::now());
    }
    sent_last
}

// ------------------------------------------------------------------ receivers

/// What a child or client received: when each part was complete.
#[derive(Debug, Default)]
struct Received {
    datagrams: u64,
    wire_bytes: u64,
    /// Wire bytes while the run measured.
    window_bytes: u64,
    /// When each part was complete (every datagram of it for a child, the
    /// decoded part for a client).
    complete: HashMap<u64, Instant>,
    /// A client's first and last fast-lane datagram of each part.
    spans: HashMap<u64, (Instant, Instant)>,
    errors: u64,
}

/// A forwarder's child: counts each frame's datagrams until `stop`.
fn run_child(
    socket: std::net::UdpSocket,
    expected: Arc<HashMap<u64, usize>>,
    stop: Arc<AtomicBool>,
    measuring: Arc<AtomicBool>,
) -> Received {
    let mut received = Received::default();
    let mut counts: HashMap<u64, usize> = HashMap::new();
    let mut batch = sys::RecvBatch::new();
    let fd = socket.as_raw_fd();
    loop {
        let ready = sys::wait_readable(&[fd], 20).unwrap_or(0);
        if ready == 0 {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            continue;
        }
        loop {
            let count = batch.recv(&socket).unwrap_or(0);
            let now = Instant::now();
            for index in 0..count {
                let data = batch.data(index);
                received.datagrams += 1;
                received.wire_bytes += data.len() as u64 + HEADER_BYTES;
                if measuring.load(Ordering::Relaxed) {
                    received.window_bytes += data.len() as u64 + HEADER_BYTES;
                }
                let Ok(view) = RlsView::parse(data, relay_session::RelayLimits::default()) else {
                    received.errors += 1;
                    continue;
                };
                let seen = counts.entry(view.object).or_default();
                *seen += 1;
                if Some(&*seen) == expected.get(&view.object) {
                    received.complete.insert(view.object, now);
                }
            }
            if count < sys::RECV_BATCH {
                break;
            }
        }
    }
    received
}

/// An edge's fast-lane client of `stream_id`: subscribes, then decodes each
/// part until `stop`.
fn run_client(
    edge: SocketAddr,
    stream_id: u64,
    stop: Arc<AtomicBool>,
    measuring: Arc<AtomicBool>,
    ready: std::sync::mpsc::Sender<u64>,
) -> Received {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let _ = SockRef::from(&socket).set_recv_buffer_size(16 << 20);
    socket
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let subscribe = |token| {
        Control::Subscribe {
            stream_id,
            from_part: None,
            repair: RepairPolicy::default(),
            token,
            fast_lane: true,
            compact: None,
        }
        .encode()
    };
    let mut buf = vec![0_u8; 65_536];
    socket.send_to(&subscribe(None), edge).unwrap();
    let token = loop {
        let (len, _) = socket.recv_from(&mut buf).expect("the edge sends a token");
        if let Some(Control::Token { token, .. }) = Control::decode(&buf[..len]) {
            break token;
        }
    };
    socket.send_to(&subscribe(Some(token)), edge).unwrap();
    socket.set_nonblocking(true).unwrap();
    let mut received = Received::default();
    let mut receiver = ObjectReceiver::new();
    let mut batch = sys::RecvBatch::new();
    let fd = socket.as_raw_fd();
    let mut ready = Some(ready);
    loop {
        let readable = sys::wait_readable(&[fd], 20).unwrap_or(0);
        if readable == 0 {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            continue;
        }
        loop {
            let count = batch.recv(&socket).unwrap_or(0);
            let now = Instant::now();
            for index in 0..count {
                let data = batch.data(index);
                if let Some(control) = Control::decode(data) {
                    if let Control::Subscribed { next_part, .. } = control {
                        if let Some(ready) = ready.take() {
                            let _ = ready.send(next_part);
                        }
                    }
                    continue;
                }
                received.datagrams += 1;
                received.wire_bytes += data.len() as u64 + HEADER_BYTES;
                if measuring.load(Ordering::Relaxed) {
                    received.window_bytes += data.len() as u64 + HEADER_BYTES;
                }
                // A fast-lane datagram: block id 0xC000_0000 | part.
                if data.len() > 12 && data.starts_with(&raptorq_datagram_fec::DATAGRAM_MAGIC) {
                    let block = u32::from_le_bytes(data[8..12].try_into().unwrap());
                    if block >> 30 == 3 {
                        let span = received
                            .spans
                            .entry(u64::from(block & 0x3fff_ffff))
                            .or_insert((now, now));
                        span.1 = now;
                    }
                }
                if let Some(object) = receiver.push(data) {
                    if object.kind == UdpObjectKind::Part {
                        received.complete.entry(object.part).or_insert(now);
                    }
                }
            }
            if count < sys::RECV_BATCH {
                break;
            }
        }
    }
    received.errors = receiver.errors;
    let _ = socket.send_to(&Control::Unsubscribe { stream_id, token }.encode(), edge);
    received
}

// ------------------------------------------------------------------ the report

/// Latency of each frame at each receiver: its completion after the source
/// handed its last datagram to the kernel.
struct Delivery {
    /// Frames of the measured window complete at every receiver.
    whole: u64,
    expected: u64,
    late: u64,
    p50_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

fn delivery(
    receivers: &[Received],
    frames: &[Frame],
    sent_last: &[Instant],
    measured: std::ops::Range<usize>,
) -> Delivery {
    let interval = Duration::from_nanos(1_000_000_000 / FRAMES_PER_SECOND);
    let mut latencies = Vec::new();
    let (mut whole, mut late) = (0, 0);
    for received in receivers {
        for index in measured.clone() {
            let Some(complete) = received.complete.get(&frames[index].part) else {
                continue;
            };
            whole += 1;
            let latency = complete.saturating_duration_since(sent_last[index]);
            if latency > interval {
                late += 1;
            }
            latencies.push(latency.as_secs_f64() * 1_000.0);
        }
    }
    latencies.sort_by(f64::total_cmp);
    let at = |q: f64| {
        latencies
            .get(((latencies.len().max(1) - 1) as f64 * q) as usize)
            .copied()
            .unwrap_or(f64::NAN)
    };
    Delivery {
        whole,
        expected: (receivers.len() * measured.len()) as u64,
        late,
        p50_ms: at(0.5),
        p99_ms: at(0.99),
        max_ms: latencies.last().copied().unwrap_or(f64::NAN),
    }
}

/// W3_PTRACER=1 lets a sampling profiler that is not this process's parent
/// (eu-stack) read the stacks of its threads.
fn allow_sampling() {
    if std::env::var_os("W3_PTRACER").is_some() {
        // SAFETY: prctl with integer arguments only.
        unsafe { libc::prctl(libc::PR_SET_PTRACER, libc::PR_SET_PTRACER_ANY, 0, 0, 0) };
    }
}

fn runtime(name: &str, workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name(name)
        .enable_all()
        .build()
        .unwrap()
}

// ------------------------------------------------------------------ the forwarder

pub(super) struct ForwarderRun {
    pub(super) media_mbps: u64,
    pub(super) ingress_mbps: f64,
    pub(super) egress_mbps: f64,
    /// Processor time, as a share of one CPU: the video thread, and the tee
    /// runtime.
    pub(super) thread_cpu: f64,
    pub(super) tee_cpu: f64,
    delivery: Delivery,
    pub(super) stats: fast_plane::relay::FastPlaneSnapshot,
}

impl ForwarderRun {
    pub(super) fn all_whole_on_time(&self) -> bool {
        self.delivery.whole == self.delivery.expected && self.delivery.late == 0
    }

    fn line(&self, children: usize, gso: bool) -> String {
        let per_100 = |cpu: f64| cpu * 100.0 / (self.ingress_mbps / 100.0);
        format!(
            "forwarder, 4K {} Mbit/s media, {children} children, GSO {gso}: in {:.1} Mbit/s, out {:.1} Mbit/s; \
             frames whole {}/{} ({} late); last symbol at a child after the source's p50 {:.2} \
             p99 {:.2} max {:.2} ms; video thread {:.1} % of a CPU ({:.2} % per 100 Mbit/s in, \
             {:.3} % per 100 Mbit/s out), tee {:.1} % ({:.2} % per 100 Mbit/s in); \
             GSO messages {} for {} datagrams; would_block {} kernel_dropped {} tee_dropped {}",
            self.media_mbps,
            self.ingress_mbps,
            self.egress_mbps,
            self.delivery.whole,
            self.delivery.expected,
            self.delivery.late,
            self.delivery.p50_ms,
            self.delivery.p99_ms,
            self.delivery.max_ms,
            self.thread_cpu * 100.0,
            per_100(self.thread_cpu),
            self.thread_cpu * 100.0 / (self.egress_mbps / 100.0),
            self.tee_cpu * 100.0,
            per_100(self.tee_cpu),
            self.stats.gso_messages,
            self.stats.gso_datagrams,
            self.stats.would_block,
            self.stats.kernel_dropped,
            self.stats.tee_dropped,
        )
    }
}

/// A forwarder with `children` children of every role (as a level-1
/// forwarder feeds the long-haul forwarders and its edges), fed a 4K stream
/// at `mbps` for `seconds` after a second of warm-up.
pub(super) fn forward_4k(mbps: u64, seconds: u64, children: usize, gso: bool) -> ForwarderRun {
    allow_sampling();
    let rt = runtime("fwd-rt", 2);
    let bits_per_second = mbps * 1_000_000;
    let warmup = FRAMES_PER_SECOND as usize;
    let measured = (seconds * FRAMES_PER_SECOND) as usize;
    let frames = make_frames(1, 0, warmup + measured + 10, bits_per_second);
    let expected: Arc<HashMap<u64, usize>> = Arc::new(
        frames
            .iter()
            .map(|frame| (frame.part, frame.datagrams.len()))
            .collect(),
    );
    let (relay, sockets) = rt.block_on(async {
        let mut sockets = Vec::new();
        for _ in 0..children {
            let (audio, video) = bind_class_pair(OFFSET).await;
            let _ = SockRef::from(&video).set_recv_buffer_size(32 << 20);
            sockets.push((audio, video));
        }
        let targets: Vec<(SocketAddr, RelayForwardRole)> = sockets
            .iter()
            .map(|(audio, _)| (audio.local_addr().unwrap(), RelayForwardRole::All))
            .collect();
        // The contributor's lane carries sources and repair (deploy binds it
        // promoted).
        let relay = start_fast_relay_with(
            ParentPath::PromotedSecondary,
            &targets,
            fast_plane::relay::RelayClassConfig {
                port_offset: OFFSET,
                cpus: [None, None],
                realtime: false,
                tee_capacity: fast_plane::relay::DEFAULT_TEE_CAPACITY,
                video_gso: gso,
                ..Default::default()
            },
        )
        .await;
        if let Some(forwarder) = &relay.forwarder {
            forwarder.configure_buffers(32 << 20);
        }
        (relay, sockets)
    });
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
            std::thread::Builder::new()
                .name(format!("child-{index}"))
                .spawn(move || run_child(video, expected, stop, measuring))
                .unwrap()
        })
        .collect();
    let fd = relay.secondary_sender.as_raw_fd();
    let _ = SockRef::from(&relay.secondary_sender).set_send_buffer_size(32 << 20);
    let target = relay.secondary_video.unwrap();
    let start = Instant::now() + Duration::from_millis(50);
    let cpu = |prefixes: &[&str]| threads_cpu_ns(prefixes);
    let source = std::thread::scope(|scope| {
        let frames = &frames;
        let source =
            scope.spawn(move || send_frames(fd, target, frames, bits_per_second, spread(), start));
        // Measure from the end of the warm-up to the last measured frame.
        let from = start + Duration::from_nanos(1_000_000_000 / FRAMES_PER_SECOND) * warmup as u32;
        std::thread::sleep(from.saturating_duration_since(Instant::now()));
        measuring.store(true, Ordering::Relaxed);
        let before = (cpu(&["needletail-fast"]), cpu(&["fwd-rt"]), Instant::now());
        std::thread::sleep(Duration::from_secs(seconds));
        let after = (cpu(&["needletail-fast"]), cpu(&["fwd-rt"]), Instant::now());
        measuring.store(false, Ordering::Relaxed);
        (source.join().unwrap(), before, after)
    });
    let (sent_last, before, after) = source;
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    let received: Vec<Received> = readers
        .into_iter()
        .map(|reader| reader.join().unwrap())
        .collect();
    let elapsed = after.2.duration_since(before.2).as_secs_f64();
    let window = warmup..warmup + measured;
    let ingress_bytes: u64 = frames[window.clone()].iter().map(Frame::wire_bytes).sum();
    let window_seconds = measured as f64 / FRAMES_PER_SECOND as f64;
    let stats = relay.stats();
    let run = ForwarderRun {
        media_mbps: mbps,
        ingress_mbps: ingress_bytes as f64 * 8.0 / window_seconds / 1e6,
        egress_mbps: received
            .iter()
            .map(|received| received.window_bytes)
            .sum::<u64>() as f64
            * 8.0
            / elapsed
            / 1e6,
        thread_cpu: (after.0 - before.0) as f64 / 1e9 / elapsed,
        tee_cpu: (after.1 - before.1) as f64 / 1e9 / elapsed,
        delivery: delivery(&received, &frames, &sent_last, window),
        stats,
    };
    rt.block_on(relay.stop());
    drop(keep_audio);
    run
}

#[test]
#[ignore = "a measurement: run it alone, in release"]
fn video_throughput_of_a_forwarder() {
    let mbps = env_u64("W3_MBPS", 200);
    let seconds = env_u64("W3_SECONDS", 5);
    let children = env_u64("W3_CHILDREN", 8) as usize;
    let mut lines = Vec::new();
    let mut runs = Vec::new();
    for gso in env_switch("W3_GSO") {
        let run = forward_4k(mbps, seconds, children, gso);
        let line = run.line(children, gso);
        println!("{line}");
        lines.push(line);
        runs.push(run);
    }
    println!("\n{}", lines.join("\n"));
    for run in &runs {
        assert!(run.all_whole_on_time(), "{}", lines.join("\n"));
    }
}

// ------------------------------------------------------------------ the edge

/// One edge run.
#[derive(Debug, Clone, Copy)]
pub(super) struct EdgeOptions {
    pub(super) mbps: u64,
    pub(super) seconds: u64,
    pub(super) clients: usize,
    pub(super) pacing: Option<fast_plane::edge_clients::VideoPacing>,
    spread: Spread,
    pub(super) ip: u8,
}

pub(super) struct EdgeRun {
    pub(super) ingress_mbps: f64,
    pub(super) egress_mbps: f64,
    /// The class threads, and the edge's runtime (tee, cache-fed lane,
    /// repair top-up), as a share of one CPU.
    pub(super) thread_cpu: f64,
    pub(super) runtime_cpu: f64,
    delivery: Delivery,
    /// First to last datagram of each measured frame at each client, ms,
    /// sorted.
    spans_ms: Vec<f64>,
    lane: fast_plane::edge_clients::FastLaneSnapshot,
}

impl EdgeRun {
    pub(super) fn all_whole_on_time(&self) -> bool {
        self.delivery.whole == self.delivery.expected && self.delivery.late == 0
    }

    fn span_ms(&self, q: f64) -> f64 {
        self.spans_ms
            .get(((self.spans_ms.len().max(1) - 1) as f64 * q) as usize)
            .copied()
            .unwrap_or(f64::NAN)
    }

    fn line(&self, options: &EdgeOptions) -> String {
        let cpu = self.thread_cpu + self.runtime_cpu;
        format!(
            "edge, 4K {} Mbit/s media, {} clients, pacing {:?}: in {:.1} Mbit/s, out {:.1} Mbit/s; \
             frames whole {}/{} ({} late); part decoded at a client after the source's last \
             datagram p50 {:.2} p99 {:.2} max {:.2} ms; a frame's first to last datagram at a \
             client p50 {:.2} p99 {:.2} ms; class threads {:.1} % of a CPU, runtime {:.1} %, \
             {:.2} % of a CPU per 100 Mbit/s out; lane datagrams {} GSO messages {} paced {} \
             clamped {} would_block {} given_back {}",
            options.mbps,
            options.clients,
            options.pacing.map(|pacing| (pacing.percent, pacing.max_delay_us)),
            self.ingress_mbps,
            self.egress_mbps,
            self.delivery.whole,
            self.delivery.expected,
            self.delivery.late,
            self.delivery.p50_ms,
            self.delivery.p99_ms,
            self.delivery.max_ms,
            self.span_ms(0.5),
            self.span_ms(0.99),
            self.thread_cpu * 100.0,
            self.runtime_cpu * 100.0,
            cpu * 100.0 / (self.egress_mbps.max(1e-9) / 100.0),
            self.lane.datagrams,
            self.lane.gso_messages,
            self.lane.paced_messages,
            self.lane.paced_clamped,
            self.lane.would_block,
            self.lane.given_back,
        )
    }
}

/// An edge fed a 4K stream by its forwarder, sending it on the fast lane to
/// its clients, for a while after a second of warm-up.
pub(super) fn edge_4k(options: EdgeOptions) -> EdgeRun {
    allow_sampling();
    let rt = runtime("edge-rt", 2);
    let bits_per_second = options.mbps * 1_000_000;
    let warmup = FRAMES_PER_SECOND as usize;
    let measured = (options.seconds * FRAMES_PER_SECOND) as usize;
    let stream_id = 1;
    // Part 0 makes the stream known (and its init); the clients join after.
    let frames = make_frames(stream_id, 0, 1 + warmup + measured + 10, bits_per_second);
    let edge = rt.block_on(async {
        tokio::spawn(LaneEdge::start(loopback(options.ip)))
            .await
            .unwrap()
    });
    edge.lane.set_video_pacing(options.pacing);
    let fd = edge.forwarder_fd(0);
    let _ = SockRef::from(&edge.forwarder_socket(0)).set_send_buffer_size(32 << 20);
    let target = edge.relay_port(0, true);
    send_frames(
        fd,
        target,
        &frames[..1],
        bits_per_second,
        Spread::Burst,
        Instant::now(),
    );
    rt.block_on(async { wait_for_part(&edge.cache, stream_id, 0).await });
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();
    for index in 0..options.clients {
        let (tx, rx) = std::sync::mpsc::channel();
        let (port, stop) = (edge.client_port, Arc::clone(&stop));
        let measuring = Arc::clone(&measuring);
        readers.push(
            std::thread::Builder::new()
                .name(format!("client-{index}"))
                .spawn(move || run_client(port, stream_id, stop, measuring, tx))
                .unwrap(),
        );
        rx.recv_timeout(Duration::from_secs(5))
            .expect("the edge subscribes the client");
    }
    let frames = &frames[1..];
    let start = Instant::now() + Duration::from_millis(50);
    let cpu = |prefixes: &[&str]| threads_cpu_ns(prefixes);
    let spread = options.spread;
    let (sent_last, before, after) = std::thread::scope(|scope| {
        let source =
            scope.spawn(move || send_frames(fd, target, frames, bits_per_second, spread, start));
        let from = start + Duration::from_nanos(1_000_000_000 / FRAMES_PER_SECOND) * warmup as u32;
        std::thread::sleep(from.saturating_duration_since(Instant::now()));
        measuring.store(true, Ordering::Relaxed);
        let before = (cpu(&["needletail-fast"]), cpu(&["edge-rt"]), Instant::now());
        std::thread::sleep(Duration::from_secs(options.seconds));
        let after = (cpu(&["needletail-fast"]), cpu(&["edge-rt"]), Instant::now());
        measuring.store(false, Ordering::Relaxed);
        (source.join().unwrap(), before, after)
    });
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    let received: Vec<Received> = readers
        .into_iter()
        .map(|reader| reader.join().unwrap())
        .collect();
    let elapsed = after.2.duration_since(before.2).as_secs_f64();
    let window = warmup..warmup + measured;
    let window_seconds = measured as f64 / FRAMES_PER_SECOND as f64;
    let ingress_bytes: u64 = frames[window.clone()].iter().map(Frame::wire_bytes).sum();
    let egress_bytes: u64 = received.iter().map(|received| received.window_bytes).sum();
    let errors: u64 = received.iter().map(|received| received.errors).sum();
    assert_eq!(errors, 0, "the clients decode every datagram");
    let mut spans_ms: Vec<f64> = received
        .iter()
        .flat_map(|received| {
            frames[window.clone()].iter().filter_map(|frame| {
                received
                    .spans
                    .get(&frame.part)
                    .map(|(first, last)| last.duration_since(*first).as_secs_f64() * 1_000.0)
            })
        })
        .collect();
    spans_ms.sort_by(f64::total_cmp);
    let run = EdgeRun {
        ingress_mbps: ingress_bytes as f64 * 8.0 / window_seconds / 1e6,
        egress_mbps: egress_bytes as f64 * 8.0 / elapsed / 1e6,
        thread_cpu: (after.0 - before.0) as f64 / 1e9 / elapsed,
        runtime_cpu: (after.1 - before.1) as f64 / 1e9 / elapsed,
        delivery: delivery(&received, frames, &sent_last, window),
        spans_ms,
        lane: edge.lane.snapshot_stats(),
    };
    rt.block_on(edge.stop());
    run
}

#[test]
#[ignore = "a measurement: run it alone, in release"]
fn video_throughput_of_an_edge() {
    let mbps = env_u64("W3_MBPS", 200);
    let seconds = env_u64("W3_SECONDS", 5);
    let mut lines = Vec::new();
    let mut failed = Vec::new();
    for clients in env_list("W3_CLIENTS", &[1, 4, 8]) {
        for pace in env_switch("W3_PACE") {
            let options = EdgeOptions {
                mbps,
                seconds,
                clients: clients as usize,
                pacing: pace.then(fast_plane::edge_clients::VideoPacing::default),
                spread: spread(),
                ip: 40,
            };
            let run = edge_4k(options);
            let line = run.line(&options);
            println!("{line}");
            if !run.all_whole_on_time() {
                failed.push(line.clone());
            }
            lines.push(line);
        }
    }
    println!("\n{}", lines.join("\n"));
    assert!(
        failed.is_empty(),
        "not every frame whole and on time:\n{}",
        failed.join("\n")
    );
}

// ------------------------------------------------------------------ pacing under fq

/// Run the test `name` again, alone, in a private network namespace whose
/// loopback has an `fq` qdisc as the deploy renders it (the kernel keeps
/// SO_TXTIME departures only under fq). Returns `true` inside it, `false`
/// outside once the inner run passed; skips (with a note) where the host
/// allows no user namespace.
fn in_fq_namespace(name: &str) -> bool {
    if std::env::var_os("W3_IN_FQ_NAMESPACE").is_some() {
        return true;
    }
    let probe = std::process::Command::new("unshare")
        .args([
            "-r",
            "-n",
            "sh",
            "-c",
            "ip link set lo up && tc qdisc replace dev lo root fq",
        ])
        .output();
    if !probe.is_ok_and(|output| output.status.success()) {
        eprintln!("{name}: skipped, this host gives no user network namespace with fq");
        return false;
    }
    let exe = std::env::current_exe().unwrap();
    let output = std::process::Command::new("unshare")
        .args([
            "-r",
            "-n",
            "sh",
            "-c",
            "ip link set lo up && tc qdisc replace dev lo root fq limit 65536 flow_limit 4096 \
             && exec \"$0\" --exact \"$1\" --nocapture --test-threads 1 --include-ignored",
        ])
        .arg(exe)
        .arg(name)
        .env("W3_IN_FQ_NAMESPACE", "1")
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("{text}");
    assert!(output.status.success(), "{name} in the namespace:\n{text}");
    assert!(
        text.contains("1 passed"),
        "{name} ran in the namespace:\n{text}"
    );
    false
}

/// Under fq, each paced 4K client gets a frame that its forwarder sent at
/// once spread over about the frame's bytes at 1.25 times the stream's rate
/// (13 ms of a 16.7 ms frame), whole and in time; unpaced, the frame reaches
/// the client in a burst of about a millisecond.
#[test]
fn paced_4k_clients_get_each_frame_at_their_rate_under_fq() {
    if !in_fq_namespace(
        "tests::video_throughput::paced_4k_clients_get_each_frame_at_their_rate_under_fq",
    ) {
        return;
    }
    let pacing = fast_plane::edge_clients::VideoPacing {
        percent: 125,
        max_delay_us: 20_000,
    };
    let mut runs = Vec::new();
    for pacing in [Some(pacing), None] {
        let options = EdgeOptions {
            mbps: 40,
            seconds: 2,
            clients: 2,
            pacing,
            spread: Spread::Burst,
            ip: 41,
        };
        let run = edge_4k(options);
        println!("{}", run.line(&options));
        runs.push(run);
    }
    let (paced, unpaced) = (&runs[0], &runs[1]);
    for run in &runs {
        assert_eq!(
            run.delivery.whole, run.delivery.expected,
            "every frame whole"
        );
    }
    // 40 Mbit/s of media: about 87 kB a frame, 13.9 ms at 1.25 times the
    // measured rate (which counts the datagrams' headers too).
    assert!(paced.lane.paced_messages > 0);
    assert!(
        paced.span_ms(0.5) > 8.0 && paced.span_ms(0.5) < 18.0,
        "paced: a frame's first to last datagram p50 {:.2} ms",
        paced.span_ms(0.5)
    );
    assert!(
        unpaced.span_ms(0.5) < 3.0,
        "unpaced: a frame's first to last datagram p50 {:.2} ms",
        unpaced.span_ms(0.5)
    );
    assert_eq!(unpaced.lane.paced_messages, 0);
    // Pacing holds a frame no longer than it takes at the client's rate.
    assert!(paced.delivery.p99_ms < 20.0, "{:.2}", paced.delivery.p99_ms);
}

/// Where a 4K tee's time goes, per frame: the dispatch of each symbol
/// (parse, checks, assembly, decode at the last) and the commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a measurement: run it alone, in release"]
async fn tee_cost_of_a_4k_frame() {
    let mbps = env_u64("W3_MBPS", 200);
    let count = 120;
    let frames = make_frames(1, 0, count, mbps * 1_000_000);
    // The contributor's promoted lane: sources and repair.
    let peer: SocketAddr = "127.0.0.1:40002".parse().unwrap();
    let mut dispatch = fast_test_dispatch(
        "127.0.0.1:40001".parse().unwrap(),
        peer,
        ParentPath::PromotedSecondary,
    );
    let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 2_048, 64).await;
    let (mut push, mut decode_last, mut commit) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let mut datagrams = 0;
    for frame in &frames {
        let now_us = now_unix_us();
        for (index, datagram) in frame.datagrams.iter().enumerate() {
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
                decode_last += spent;
                let started = Instant::now();
                commit_relay_object(&cache, *object, envelope)
                    .await
                    .unwrap();
                commit += started.elapsed();
                assert_eq!(index + 1, frame.sources, "decoded at the last source");
            } else {
                push += spent;
            }
        }
    }
    let per_frame = |total: Duration| total.as_secs_f64() * 1e6 / count as f64;
    println!(
        "4K {mbps} Mbit/s, {} datagrams a frame: dispatch {:.1} µs a frame ({:.2} µs a datagram), \
         the decoding push {:.1} µs, commit {:.1} µs; at 60 frames a second {:.1} % of a CPU",
        datagrams / count,
        per_frame(push),
        push.as_secs_f64() * 1e6 / (datagrams - count) as f64,
        per_frame(decode_last),
        per_frame(commit),
        (per_frame(push) + per_frame(decode_last) + per_frame(commit)) * 60.0 / 1e4,
    );
}

/// Children that share one source socket (one bind given twice) take a 4K
/// part on the video thread as GSO messages, a child's symbols together,
/// each child every symbol in order; the plan took them in turn symbol by
/// symbol, which left GSO nothing to join.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn children_of_a_shared_socket_take_4k_as_gso_messages() {
    let (first_audio, first_video) = bind_class_pair(OFFSET).await;
    let (second_audio, second_video) = bind_class_pair(OFFSET).await;
    // One fixed bind for both children (its video twin at the same port).
    let bind = loop {
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        if std::net::UdpSocket::bind(port).is_ok() {
            break port;
        }
    };
    let endpoints = [
        RelayForwardEndpoint {
            bind,
            target: first_audio.local_addr().unwrap(),
            role: RelayForwardRole::All,
        },
        RelayForwardEndpoint {
            bind,
            target: second_audio.local_addr().unwrap(),
            role: RelayForwardRole::All,
        },
    ];
    let relay = start_fast_relay_endpoints(
        ParentPath::PromotedSecondary,
        &endpoints,
        fast_plane::relay::RelayClassConfig {
            port_offset: OFFSET,
            cpus: [None, None],
            realtime: false,
            tee_capacity: 4_096,
            video_gso: true,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        relay.forwarder.as_ref().unwrap().socket_groups,
        vec![vec![0, 1]],
        "one source socket for both children"
    );
    let frames = make_frames(1, 0, 1, 40_000_000);
    let datagrams = &frames[0].datagrams;
    // The whole part at once, as the drains then hold up to 64 symbols.
    let sent = sys::send_batch(
        relay.secondary_sender.as_raw_fd(),
        datagrams.len(),
        |index| (&datagrams[index][..], relay.secondary_video.unwrap()),
    )
    .unwrap();
    assert_eq!(sent, datagrams.len());
    for video in [&first_video, &second_video] {
        let got: Vec<Vec<u8>> = receive_all(video, datagrams.len())
            .await
            .into_iter()
            .map(|(bytes, _)| bytes)
            .collect();
        assert_eq!(&got, datagrams, "every symbol, in order");
    }
    let stats = relay.stats();
    assert_eq!(stats.video_forwarded, 2 * datagrams.len() as u64);
    if stats.gso_fallbacks == 0 {
        assert_eq!(stats.gso_datagrams, stats.video_forwarded);
        assert!(
            stats.gso_messages * 4 < stats.gso_datagrams,
            "a child's symbols of a drain in one message: {stats:?}"
        );
    }
    // The counters of the sends, counted per call, add up as per symbol.
    let forwarded = relay.forwarder.as_ref().unwrap().snapshot();
    assert_eq!(
        forwarded.source_datagrams + forwarded.repair_datagrams,
        2 * datagrams.len() as u64
    );
    assert_eq!(
        forwarded.bytes,
        2 * datagrams
            .iter()
            .map(|datagram| datagram.len() as u64)
            .sum::<u64>()
    );
    relay.stop().await;
    drop((first_audio, second_audio));
}

#[test]
fn the_video_pacing_flags_default_on_and_can_be_turned_off() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    assert_eq!(args.client_udp_video_pace_percent, 125);
    assert_eq!(args.client_udp_video_pace_max_ms, 8);
    let args = Args::try_parse_from([
        "av-mesh",
        "--client-udp-video-pace-percent",
        "0",
        "--client-udp-video-pace-max-ms",
        "4",
    ])
    .unwrap();
    assert_eq!(args.client_udp_video_pace_percent, 0);
    assert_eq!(args.client_udp_video_pace_max_ms, 4);
    // 0 is off: the lane sends video at once.
    let lane = fast_plane::edge_clients::EdgeFastLane::new(0);
    lane.set_video_pacing(Some(fast_plane::edge_clients::VideoPacing {
        percent: 0,
        max_delay_us: 4_000,
    }));
    assert_eq!(lane.snapshot_stats().txtime_refused, 0);
}
