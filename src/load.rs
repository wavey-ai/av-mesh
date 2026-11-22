//! Thousands of simulated UDP listeners on one host (`needletail-load`).
//!
//! Each listener is a real client of the edge: its own UDP socket, its own
//! token and subscription, the `NTU/1` handshake, the compact lane and the
//! RaptorQ lanes decoded by `crate::client_udp`, NACKs, loss measurement and
//! the repair and compact plan that follow from it, and moves on `MOVE`,
//! `REFUSED`, no answer or silence: the state machine of
//! `crate::client_session`, which `needletail-udp-client` runs too.
//!
//! For scale, a few worker threads each own many listeners: each worker has
//! one epoll set of its listeners' non-blocking sockets and a timer heap of
//! their wake times, and reads each ready socket with `recvmmsg` into
//! buffers that it allocated once. A listener writes no log line. With
//! kernel receive timestamps (`SO_TIMESTAMPNS`) a part's arrival is the time
//! the kernel took the datagram, so a busy load host does not add its own
//! lag to the latency that it measures; the lag itself is measured apart
//! (kernel timestamp to processing) and reported as the host's own.
//!
//! Measurement is sampled, not logged: per interval and per edge, counters
//! and log-bucket histograms (1.6 % wide, in microseconds) merged over all
//! listeners; the full arrivals of the first `sample` listeners; and the
//! load host's CPU, the workers' busy time, timer lateness and receive drops
//! (`UdpRcvbufErrors`, `SO_RXQ_OVFL`), so a saturated load host is told
//! apart from a saturated edge.

use crate::client_session::{Received, SessionConfig, UdpSession};
use crate::client_udp::ObjectKind;
use serde_json::{json, Map, Value};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ------------------------------------------------------------------ histogram

const SUB_BITS: u32 = 6;
const SUB: u64 = 1 << SUB_BITS;
/// Values below `2 * SUB` exactly, then `SUB` buckets for each power of two.
const BUCKETS: usize = (2 * SUB) as usize + (64 - SUB_BITS as usize - 1) * SUB as usize;

/// A log-bucket histogram of non-negative integers (microseconds here):
/// exact below 128, then 64 buckets for each power of two (1.6 % wide).
/// Empty histograms hold no buckets, so the many that stay empty cost
/// nothing to send and merge.
#[derive(Debug, Clone, Default)]
pub struct Histogram {
    counts: Vec<u64>,
    total: u64,
    max: u64,
    sum: u128,
}

impl Histogram {
    pub fn new() -> Self {
        Self::default()
    }

    fn index(value: u64) -> usize {
        if value < 2 * SUB {
            return value as usize;
        }
        let exponent = 63 - value.leading_zeros();
        let shift = exponent - SUB_BITS;
        (2 * SUB + u64::from(exponent - SUB_BITS - 1) * SUB + ((value >> shift) - SUB)) as usize
    }

    /// The middle of bucket `index`.
    fn value_of(index: usize) -> u64 {
        let index = index as u64;
        if index < 2 * SUB {
            return index;
        }
        let k = index - 2 * SUB;
        let exponent = k / SUB + u64::from(SUB_BITS) + 1;
        let shift = exponent - u64::from(SUB_BITS);
        let low = (SUB + k % SUB) << shift;
        low + ((1_u64 << shift) - 1) / 2
    }

    pub fn record(&mut self, value: u64) {
        if self.counts.is_empty() {
            self.counts = vec![0; BUCKETS];
        }
        self.counts[Self::index(value)] += 1;
        self.total += 1;
        self.max = self.max.max(value);
        self.sum += u128::from(value);
    }

    pub fn merge(&mut self, other: &Histogram) {
        if other.total == 0 {
            return;
        }
        if self.counts.is_empty() {
            self.counts = vec![0; BUCKETS];
        }
        for (mine, theirs) in self.counts.iter_mut().zip(&other.counts) {
            *mine += theirs;
        }
        self.total += other.total;
        self.max = self.max.max(other.max);
        self.sum += other.sum;
    }

    pub fn count(&self) -> u64 {
        self.total
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    /// The `q` quantile (the value at rank ceil(q * count)), never above
    /// the largest value recorded.
    pub fn quantile(&self, q: f64) -> Option<u64> {
        if self.total == 0 {
            return None;
        }
        let rank = ((q * self.total as f64).ceil() as u64).clamp(1, self.total);
        let mut seen = 0;
        for (index, count) in self.counts.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return Some(Self::value_of(index).min(self.max));
            }
        }
        Some(self.max)
    }

    /// The buckets that hold values, as `[lowest value, count]` pairs:
    /// histograms of several load hosts merge by adding the counts of equal
    /// lowest values.
    pub fn buckets(&self) -> Vec<[u64; 2]> {
        self.counts
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 0)
            .map(|(index, count)| [Self::low_of(index), *count])
            .collect()
    }

    /// The lowest value of bucket `index`.
    fn low_of(index: usize) -> u64 {
        let index = index as u64;
        if index < 2 * SUB {
            return index;
        }
        let k = index - 2 * SUB;
        let exponent = k / SUB + u64::from(SUB_BITS) + 1;
        (SUB + k % SUB) << (exponent - u64::from(SUB_BITS))
    }

    /// The summary in milliseconds with the buckets (in µs) besides.
    pub fn summary_with_buckets(&self) -> Value {
        let mut value = self.summary_ms();
        value["buckets_us"] = json!(self.buckets());
        value
    }

    /// count, p50, p95, p99, p99.9, max and mean, in milliseconds.
    pub fn summary_ms(&self) -> Value {
        let ms = |us: Option<u64>| us.map(|us| (us as f64 / 1_000.0 * 1_000.0).round() / 1_000.0);
        json!({
            "count": self.total,
            "p50": ms(self.quantile(0.5)),
            "p95": ms(self.quantile(0.95)),
            "p99": ms(self.quantile(0.99)),
            "p999": ms(self.quantile(0.999)),
            "max": ms((self.total > 0).then_some(self.max)),
            "mean": (self.total > 0)
                .then(|| ((self.sum as f64 / self.total as f64) / 1_000.0 * 1_000.0).round() / 1_000.0),
        })
    }
}

// ------------------------------------------------------------------ counters

/// What the listeners of one edge did in an interval (or a run). The
/// gauges (`active`, `up`) are the listeners at the end of the interval.
#[derive(Debug, Clone, Default)]
pub struct Counters {
    pub started: u64,
    pub stopped: u64,
    pub start_errors: u64,
    pub active: u64,
    pub up: u64,
    /// New parts received; parts settled as never received, and as gaps;
    /// parts that came after they were settled; copies of parts already
    /// received.
    pub parts: u64,
    pub missing: u64,
    pub gaps: u64,
    pub late: u64,
    pub duplicates: u64,
    /// Parts recovered by FEC (RaptorQ repair or the compact lane's window
    /// repair), parts asked for again and the NACK datagrams that asked,
    /// holes given up.
    pub recovered: u64,
    pub nack_parts: u64,
    pub nack_datagrams: u64,
    pub abandoned: u64,
    /// Media datagrams and their bytes with IP and UDP headers.
    pub datagrams: u64,
    pub bytes_in: u64,
    pub moves: u64,
    pub decode_errors: u64,
    /// Datagrams the kernel dropped at a full listener socket
    /// (`SO_RXQ_OVFL`), and datagrams larger than the receive buffers.
    pub socket_drops: u64,
    pub truncated: u64,
    /// A listener's part numbers jumped (a contributor restart): its
    /// accounting began again.
    pub resets: u64,
    pub first_parts: u64,
    /// Publication to arrival of each new part, µs.
    pub latency: Histogram,
    /// Listener start (its first `SUB`) to its first part, µs.
    pub ttfp: Histogram,
    /// Kernel receive timestamp to processing, µs: the load host's own lag.
    pub recv_lag: Histogram,
}

impl Counters {
    pub fn merge(&mut self, other: &Counters) {
        self.started += other.started;
        self.stopped += other.stopped;
        self.start_errors += other.start_errors;
        self.active += other.active;
        self.up += other.up;
        self.parts += other.parts;
        self.missing += other.missing;
        self.gaps += other.gaps;
        self.late += other.late;
        self.duplicates += other.duplicates;
        self.recovered += other.recovered;
        self.nack_parts += other.nack_parts;
        self.nack_datagrams += other.nack_datagrams;
        self.abandoned += other.abandoned;
        self.datagrams += other.datagrams;
        self.bytes_in += other.bytes_in;
        self.moves += other.moves;
        self.decode_errors += other.decode_errors;
        self.socket_drops += other.socket_drops;
        self.truncated += other.truncated;
        self.resets += other.resets;
        self.first_parts += other.first_parts;
        self.latency.merge(&other.latency);
        self.ttfp.merge(&other.ttfp);
        self.recv_lag.merge(&other.recv_lag);
    }

    /// Missing over all the parts that were due (received, missing, gaps).
    pub fn missing_percent(&self) -> Option<f64> {
        let due = self.parts + self.missing + self.gaps;
        (due > 0).then(|| self.missing as f64 * 100.0 / due as f64)
    }

    pub fn to_json(&self, seconds: f64, gauges: bool) -> Value {
        let mut value = json!({
            "started": self.started,
            "stopped": self.stopped,
            "start_errors": self.start_errors,
            "parts": self.parts,
            "missing": self.missing,
            "missing_pct": self.missing_percent(),
            "gaps": self.gaps,
            "late": self.late,
            "duplicates": self.duplicates,
            "recovered": self.recovered,
            "nack_parts": self.nack_parts,
            "nack_datagrams": self.nack_datagrams,
            "abandoned": self.abandoned,
            "datagrams": self.datagrams,
            "bytes_in": self.bytes_in,
            "mbit_s_in": (seconds > 0.0).then(|| self.bytes_in as f64 * 8.0 / seconds / 1e6),
            "moves": self.moves,
            "decode_errors": self.decode_errors,
            "socket_drops": self.socket_drops,
            "truncated": self.truncated,
            "resets": self.resets,
            "first_parts": self.first_parts,
            "latency_ms": self.latency.summary_ms(),
            "ttfp_ms": self.ttfp.summary_ms(),
            "recv_lag_ms": self.recv_lag.summary_ms(),
        });
        if gauges {
            value["active"] = json!(self.active);
            value["up"] = json!(self.up);
        }
        value
    }
}

// ------------------------------------------------------------------ parts

const TRACK: u64 = 4_096;
const TRACK_WORDS: usize = (TRACK / 64) as usize;

#[derive(Debug, PartialEq, Eq)]
enum Mark {
    New,
    Duplicate,
    Late,
}

/// The parts of one listener, from the part its first edge said it would
/// send first: received, gaps, and those that never came, counted when they
/// are `settle_parts` behind the newest part (past the NACKs' give-up).
struct PartTrack {
    start: Option<u64>,
    next_settle: u64,
    highest: Option<u64>,
    got: [u64; TRACK_WORDS],
    gap: [u64; TRACK_WORDS],
}

impl PartTrack {
    fn new() -> Box<Self> {
        Box::new(Self {
            start: None,
            next_settle: 0,
            highest: None,
            got: [0; TRACK_WORDS],
            gap: [0; TRACK_WORDS],
        })
    }

    fn begin(&mut self, first: u64) {
        if self.start.is_none() {
            self.start = Some(first);
            self.next_settle = first;
        }
    }

    fn reset(&mut self, first: u64) {
        self.got = [0; TRACK_WORDS];
        self.gap = [0; TRACK_WORDS];
        self.start = Some(first);
        self.next_settle = first;
        self.highest = None;
    }

    fn settle(&mut self, upto: u64, counters: &mut Counters) {
        while self.next_settle < upto {
            let part = self.next_settle;
            let (word, bit) = (((part % TRACK) / 64) as usize, 1_u64 << (part % 64));
            if self.got[word] & bit == 0 {
                if self.gap[word] & bit != 0 {
                    counters.gaps += 1;
                } else {
                    counters.missing += 1;
                }
            }
            self.got[word] &= !bit;
            self.gap[word] &= !bit;
            self.next_settle += 1;
        }
    }

    fn mark(&mut self, part: u64, gap: bool, settle_parts: u64, counters: &mut Counters) -> Mark {
        if self.start.is_none() {
            self.begin(part);
        }
        // A contributor restart counts its parts again; a jump far ahead
        // is one too (or a listener that was away): start again there.
        if part + TRACK < self.next_settle || part >= self.next_settle + 16 * TRACK {
            counters.resets += 1;
            self.reset(part);
        }
        if part < self.next_settle {
            return Mark::Late;
        }
        if part >= self.next_settle + TRACK {
            self.settle(part + 1 - TRACK, counters);
        }
        let (word, bit) = (((part % TRACK) / 64) as usize, 1_u64 << (part % 64));
        if self.got[word] & bit != 0 {
            return Mark::Duplicate;
        }
        if gap {
            if self.gap[word] & bit != 0 {
                return Mark::Duplicate;
            }
            self.gap[word] |= bit;
        } else {
            self.got[word] |= bit;
            self.gap[word] &= !bit;
        }
        let highest = self.highest.map_or(part, |highest| highest.max(part));
        self.highest = Some(highest);
        if highest + 1 > self.next_settle + settle_parts {
            self.settle(highest + 1 - settle_parts, counters);
        }
        Mark::New
    }
}

// ------------------------------------------------------------------ config

/// Parts whose bytes a load listener keeps for window repair.
pub const LOAD_KNOWN_PARTS: usize = 512;

#[derive(Debug, Clone)]
pub struct LoadConfig {
    pub session: SessionConfig,
    /// Client UDP ports of the edges; listener `i` starts at edge
    /// `i % edges.len()` and moves to the next one when it must.
    pub edges: Vec<SocketAddr>,
    /// Listeners wanted from each time on (a step function); see
    /// [`parse_ramp`].
    pub ramp: Vec<(Duration, usize)>,
    pub duration: Duration,
    pub threads: usize,
    /// Listeners whose every arrival is kept (the first `sample`).
    pub sample: usize,
    pub receive_buffer: usize,
    /// Listeners started a second, at most.
    pub start_rate: f64,
    pub interval: Duration,
    /// Local addresses to bind the listeners to, in turn (each address has
    /// about 28,000 ephemeral ports).
    pub bind_ips: Vec<IpAddr>,
    pub settle_parts: u64,
    pub kernel_timestamps: bool,
    /// After the duration, keep receiving this long (NACKs still recover
    /// the newest parts) before the parts are settled and the listeners
    /// leave.
    pub drain: Duration,
    pub out_dir: Option<PathBuf>,
    /// Name prefix of the worker threads.
    pub thread_name: String,
    /// Carried into every line and the summary.
    pub label: Option<String>,
    /// The load host counts as saturated in an interval whose timers or
    /// datagrams waited longer than this (p99) for the workers.
    pub lag_limit: Duration,
    /// One status line on standard error each interval.
    pub progress: bool,
    /// Set it to end the run early (with its drain and summary).
    pub stop: Option<Arc<AtomicBool>>,
    /// A worker reads its sockets at most this often, so that each socket
    /// holds several datagrams and one `recvmmsg` takes them all (zero:
    /// at once). With kernel timestamps the latency measured is the same;
    /// timers (NACKs, refreshes) run up to this late.
    pub coalesce: Duration,
}

impl LoadConfig {
    /// A load of `listeners`; a session that keeps the receiver's default
    /// window memory keeps [`LOAD_KNOWN_PARTS`] here.
    pub fn new(mut session: SessionConfig, edges: Vec<SocketAddr>, listeners: usize) -> Self {
        if session.known_parts == 0 {
            session.known_parts = LOAD_KNOWN_PARTS;
        }
        Self {
            session,
            edges,
            ramp: vec![(Duration::ZERO, listeners)],
            duration: Duration::from_secs(60),
            threads: std::thread::available_parallelism().map_or(4, |n| n.get()),
            sample: 0,
            receive_buffer: 512 * 1_024,
            start_rate: 2_000.0,
            interval: Duration::from_secs(1),
            bind_ips: vec![IpAddr::V4(Ipv4Addr::UNSPECIFIED)],
            settle_parts: 1_500,
            kernel_timestamps: true,
            drain: Duration::from_millis(600),
            out_dir: None,
            thread_name: "load-w".into(),
            label: None,
            lag_limit: Duration::from_millis(5),
            progress: false,
            stop: None,
            coalesce: Duration::ZERO,
        }
    }

    fn peak(&self) -> usize {
        self.ramp.iter().map(|(_, count)| *count).max().unwrap_or(0)
    }

    fn target_at(&self, elapsed: Duration) -> usize {
        self.ramp
            .iter()
            .take_while(|(at, _)| *at <= elapsed)
            .last()
            .map_or(0, |(_, count)| *count)
    }
}

/// `count@seconds,...`, for example `100@0,1000@30,5000@60`: `count`
/// listeners from `seconds` on. A bare count means from 0.
pub fn parse_ramp(text: &str) -> Result<Vec<(Duration, usize)>, String> {
    let mut steps = Vec::new();
    for item in text.split(',').map(str::trim).filter(|item| !item.is_empty()) {
        let (count, at) = item.split_once('@').unwrap_or((item, "0"));
        let count: usize = count
            .trim()
            .replace('_', "")
            .parse()
            .map_err(|_| format!("bad listener count in ramp step {item:?}"))?;
        let at: f64 = at
            .trim()
            .parse()
            .map_err(|_| format!("bad time in ramp step {item:?}"))?;
        if !(at >= 0.0) {
            return Err(format!("bad time in ramp step {item:?}"));
        }
        steps.push((Duration::from_secs_f64(at), count));
    }
    if steps.is_empty() {
        return Err("empty ramp".into());
    }
    steps.sort_by_key(|(at, _)| *at);
    Ok(steps)
}

/// The listeners of worker `worker` of `threads` when `total` are wanted:
/// listener `i` belongs to worker `i % threads`.
fn share(total: usize, worker: usize, threads: usize) -> usize {
    (total + threads - 1 - worker) / threads
}

fn now_unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_micros() as u64)
}

// ------------------------------------------------------------------ receive

const BATCH: usize = 32;
const DATAGRAM: usize = 2_048;
const CONTROL_WORDS: usize = 16;

/// `recvmmsg` buffers of one worker, allocated once.
struct RecvBufs {
    data: Vec<u8>,
    names: Vec<libc::sockaddr_storage>,
    iovs: Vec<libc::iovec>,
    control: Vec<[u64; CONTROL_WORDS]>,
    headers: Vec<libc::mmsghdr>,
}

struct Datagram<'a> {
    data: &'a [u8],
    peer: Option<SocketAddr>,
    kernel_us: Option<u64>,
    overflows: Option<u32>,
    truncated: bool,
}

impl RecvBufs {
    fn new() -> Self {
        let mut bufs = Self {
            data: vec![0; BATCH * DATAGRAM],
            names: vec![unsafe { std::mem::zeroed() }; BATCH],
            iovs: Vec::with_capacity(BATCH),
            control: vec![[0; CONTROL_WORDS]; BATCH],
            headers: Vec::with_capacity(BATCH),
        };
        for index in 0..BATCH {
            bufs.iovs.push(libc::iovec {
                iov_base: bufs.data[index * DATAGRAM..].as_mut_ptr().cast(),
                iov_len: DATAGRAM,
            });
        }
        for index in 0..BATCH {
            let mut header: libc::mmsghdr = unsafe { std::mem::zeroed() };
            header.msg_hdr.msg_name = (&mut bufs.names[index] as *mut libc::sockaddr_storage).cast();
            header.msg_hdr.msg_iov = &mut bufs.iovs[index];
            header.msg_hdr.msg_iovlen = 1;
            header.msg_hdr.msg_control = bufs.control[index].as_mut_ptr().cast();
            bufs.headers.push(header);
        }
        bufs
    }

    /// Datagrams waiting at `fd`, at most [`BATCH`].
    fn recv(&mut self, fd: i32) -> usize {
        for header in &mut self.headers {
            header.msg_hdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as u32;
            header.msg_hdr.msg_controllen = CONTROL_WORDS * 8;
            header.msg_hdr.msg_flags = 0;
            header.msg_len = 0;
        }
        let received = unsafe {
            libc::recvmmsg(
                fd,
                self.headers.as_mut_ptr(),
                BATCH as u32,
                libc::MSG_DONTWAIT,
                std::ptr::null_mut(),
            )
        };
        received.max(0) as usize
    }

    fn get(&self, index: usize) -> Datagram<'_> {
        let header = &self.headers[index];
        let len = (header.msg_len as usize).min(DATAGRAM);
        let mut datagram = Datagram {
            data: &self.data[index * DATAGRAM..index * DATAGRAM + len],
            peer: socket_addr(&self.names[index]),
            kernel_us: None,
            overflows: None,
            truncated: header.msg_hdr.msg_flags & libc::MSG_TRUNC != 0,
        };
        unsafe {
            let mut cmsg = libc::CMSG_FIRSTHDR(&header.msg_hdr);
            while !cmsg.is_null() {
                let level = (*cmsg).cmsg_level;
                let kind = (*cmsg).cmsg_type;
                if level == libc::SOL_SOCKET && kind == libc::SCM_TIMESTAMPNS {
                    let ts: libc::timespec =
                        std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<libc::timespec>());
                    datagram.kernel_us =
                        Some(ts.tv_sec as u64 * 1_000_000 + ts.tv_nsec as u64 / 1_000);
                } else if level == libc::SOL_SOCKET && kind == libc::SO_RXQ_OVFL {
                    datagram.overflows =
                        Some(std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<u32>()));
                }
                cmsg = libc::CMSG_NXTHDR(&header.msg_hdr, cmsg);
            }
        }
        datagram
    }
}

fn socket_addr(storage: &libc::sockaddr_storage) -> Option<SocketAddr> {
    match i32::from(storage.ss_family) {
        libc::AF_INET => {
            let addr = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr))),
                u16::from_be(addr.sin_port),
            ))
        }
        libc::AF_INET6 => {
            let addr = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
            Some(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::from(addr.sin6_addr.s6_addr)),
                u16::from_be(addr.sin6_port),
            ))
        }
        _ => None,
    }
}

fn set_int_option(fd: i32, level: i32, option: i32, value: i32) -> bool {
    unsafe {
        libc::setsockopt(
            fd,
            level,
            option,
            (&value as *const i32).cast(),
            std::mem::size_of::<i32>() as libc::socklen_t,
        ) == 0
    }
}

fn listener_socket(config: &LoadConfig, index: usize) -> std::io::Result<UdpSocket> {
    let ip = config.bind_ips[index % config.bind_ips.len()];
    let domain = if ip.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, None)?;
    socket.set_nonblocking(true)?;
    let fd = socket.as_raw_fd();
    // SO_RCVBUFFORCE needs CAP_NET_ADMIN; SO_RCVBUF is capped by rmem_max.
    let size = config.receive_buffer.min(i32::MAX as usize) as i32;
    if !set_int_option(fd, libc::SOL_SOCKET, libc::SO_RCVBUFFORCE, size) {
        let _ = socket.set_recv_buffer_size(config.receive_buffer);
    }
    if config.kernel_timestamps {
        set_int_option(fd, libc::SOL_SOCKET, libc::SO_TIMESTAMPNS, 1);
    }
    set_int_option(fd, libc::SOL_SOCKET, libc::SO_RXQ_OVFL, 1);
    socket.bind(&SocketAddr::new(ip, 0).into())?;
    Ok(socket.into())
}

// ------------------------------------------------------------------ worker

/// Counters of a receiver at the last interval, for the next one's deltas.
#[derive(Default, Clone, Copy)]
struct Seen {
    recovered: u64,
    duplicates: u64,
    nack_parts: u64,
    nack_datagrams: u64,
    abandoned: u64,
    datagrams: u64,
    wire: u64,
    errors: u64,
}

struct Listener {
    index: usize,
    edge: usize,
    socket: UdpSocket,
    session: UdpSession,
    started: Instant,
    first_part: bool,
    track: Box<PartTrack>,
    scheduled: Instant,
    seen: Seen,
    overflows: u32,
    highest_at_end: Option<u64>,
    arrivals: Option<BTreeMap<u64, (u64, usize, u64, u64)>>,
}

enum Report {
    Interval {
        worker: usize,
        interval: u64,
        edges: Vec<Counters>,
        cpu_ns: u64,
        busy_ns: u64,
        lateness: Histogram,
        work: LoopCounts,
    },
    Done {
        worker: usize,
        edges: Vec<Counters>,
        never_first: u64,
        samples: Vec<Value>,
    },
}

/// What a worker's loop did in an interval: where its time goes.
#[derive(Debug, Clone, Copy, Default)]
struct LoopCounts {
    epoll_waits: u64,
    ready_sockets: u64,
    recv_calls: u64,
    datagrams: u64,
    polls: u64,
}

impl LoopCounts {
    fn add(&mut self, other: &LoopCounts) {
        self.epoll_waits += other.epoll_waits;
        self.ready_sockets += other.ready_sockets;
        self.recv_calls += other.recv_calls;
        self.datagrams += other.datagrams;
        self.polls += other.polls;
    }

    fn to_json(self, seconds: f64) -> Value {
        let rate = |count: u64| (count as f64 / seconds.max(1e-3)).round();
        json!({
            "epoll_waits_s": rate(self.epoll_waits),
            "ready_sockets_s": rate(self.ready_sockets),
            "recv_calls_s": rate(self.recv_calls),
            "datagrams_s": rate(self.datagrams),
            "datagrams_per_recv": (self.recv_calls > 0)
                .then(|| self.datagrams as f64 / self.recv_calls as f64),
            "polls_s": rate(self.polls),
        })
    }
}

struct Shared {
    t0: Instant,
    target: AtomicUsize,
    stop: AtomicBool,
}

fn thread_cpu_ns() -> u64 {
    std::fs::read_to_string("/proc/thread-self/schedstat")
        .ok()
        .and_then(|stat| stat.split_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

fn handle_datagram(
    listener: &mut Listener,
    counters: &mut [Counters],
    config: &LoadConfig,
    datagram: &Datagram<'_>,
    now: Instant,
    now_us: u64,
) -> Option<String> {
    let socket = &listener.socket;
    let session = &mut listener.session;
    let track = &mut listener.track;
    let edge = &mut counters[listener.edge];
    if let Some(overflows) = datagram.overflows {
        edge.socket_drops += u64::from(overflows.wrapping_sub(listener.overflows));
        listener.overflows = overflows;
    }
    if datagram.truncated {
        edge.truncated += 1;
        return None;
    }
    let peer = datagram.peer?;
    let mut send = |to: SocketAddr, bytes: &[u8]| {
        // A full socket loses a request: the next refresh repeats it.
        let _ = socket.send_to(bytes, to);
    };
    match session.on_datagram(peer, datagram.data, now, &mut send) {
        Received::Media => {}
        Received::Subscribed { next_part, first } => {
            if first {
                track.begin(next_part);
            }
            return None;
        }
        Received::Move(reason) => return Some(reason),
        Received::Control | Received::Ignored => return None,
    }
    let arrival_us = datagram.kernel_us.unwrap_or(now_us);
    if let Some(kernel_us) = datagram.kernel_us {
        edge.recv_lag.record(now_us.saturating_sub(kernel_us));
    }
    while let Some(object) = session.next_object() {
        match object.kind {
            ObjectKind::Init => {}
            ObjectKind::Gap => {
                track.mark(object.part, true, config.settle_parts, edge);
            }
            ObjectKind::Part => match track.mark(object.part, false, config.settle_parts, edge) {
                Mark::New => {
                    edge.parts += 1;
                    if object.published_unix_us > 0 {
                        edge.latency
                            .record(arrival_us.saturating_sub(object.published_unix_us));
                    }
                    if !listener.first_part {
                        listener.first_part = true;
                        edge.first_parts += 1;
                        edge.ttfp
                            .record(now.saturating_duration_since(listener.started).as_micros() as u64);
                    }
                    if let Some(arrivals) = &mut listener.arrivals {
                        arrivals.insert(
                            object.part,
                            (
                                arrival_us,
                                object.payload.len(),
                                object.published_unix_us,
                                object.available_unix_us,
                            ),
                        );
                    }
                }
                Mark::Duplicate => edge.duplicates += 1,
                Mark::Late => edge.late += 1,
            },
        }
    }
    None
}

/// Move `listener` to the next edge.
fn move_listener(listener: &mut Listener, config: &LoadConfig, counters: &mut [Counters], now: Instant) {
    if config.edges.len() > 1 {
        listener.edge = (listener.edge + 1) % config.edges.len();
    }
    counters[listener.edge].moves += 1;
    let from = listener.track.highest.map(|part| part + 1);
    let socket = &listener.socket;
    listener
        .session
        .begin(config.edges[listener.edge], from, now, &mut |to, bytes| {
            let _ = socket.send_to(bytes, to);
        });
}

fn collect_deltas(listeners: &mut [Listener], counters: &mut [Counters]) {
    for listener in listeners {
        let edge = &mut counters[listener.edge];
        edge.active += 1;
        if listener.session.is_subscribed() {
            edge.up += 1;
        }
        let receiver = &listener.session.receiver;
        let now = Seen {
            recovered: receiver.recovered_objects,
            duplicates: receiver.duplicate_parts,
            nack_parts: receiver.compact.stats.nacked,
            nack_datagrams: listener.session.nack_datagrams,
            abandoned: receiver.compact.stats.abandoned,
            datagrams: receiver.datagrams,
            wire: receiver.wire_bytes,
            errors: receiver.errors,
        };
        let seen = listener.seen;
        edge.recovered += now.recovered.saturating_sub(seen.recovered);
        edge.duplicates += now.duplicates.saturating_sub(seen.duplicates);
        edge.nack_parts += now.nack_parts.saturating_sub(seen.nack_parts);
        edge.nack_datagrams += now.nack_datagrams.saturating_sub(seen.nack_datagrams);
        edge.abandoned += now.abandoned.saturating_sub(seen.abandoned);
        edge.datagrams += now.datagrams.saturating_sub(seen.datagrams);
        edge.bytes_in += now.wire.saturating_sub(seen.wire);
        edge.decode_errors += now.errors.saturating_sub(seen.errors);
        listener.seen = now;
    }
}

fn worker(worker: usize, config: Arc<LoadConfig>, shared: Arc<Shared>, tx: mpsc::Sender<Report>) {
    let threads = config.threads;
    let edges = config.edges.len();
    let epoll = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    assert!(epoll >= 0, "epoll_create1");
    let mut events: Vec<libc::epoll_event> = vec![libc::epoll_event { events: 0, u64: 0 }; 1_024];
    let mut bufs = RecvBufs::new();
    let mut listeners: Vec<Listener> = Vec::new();
    let mut heap: BinaryHeap<Reverse<(Instant, usize)>> = BinaryHeap::new();
    let mut counters = vec![Counters::default(); edges];
    let mut lateness = Histogram::new();
    let mut work = LoopCounts::default();
    let t0 = shared.t0;
    let end = t0 + config.duration;
    let mut draining = false;
    let mut drain_until = end;
    let interval = config.interval;
    let mut interval_index = 0_u64;
    let mut next_flush = t0 + interval;
    let rate = (config.start_rate / threads as f64).max(1.0);
    let mut budget = 1.0_f64;
    let mut budget_at = Instant::now();
    let mut busy_ns = 0_u64;
    let mut last_read = Instant::now();
    let mut cpu_before = thread_cpu_ns();

    loop {
        let loop_start = Instant::now();
        let now = loop_start;
        if now >= next_flush {
            collect_deltas(&mut listeners, &mut counters);
            let cpu = thread_cpu_ns();
            let _ = tx.send(Report::Interval {
                worker,
                interval: interval_index,
                edges: std::mem::replace(&mut counters, vec![Counters::default(); edges]),
                cpu_ns: cpu.saturating_sub(cpu_before),
                busy_ns: std::mem::take(&mut busy_ns),
                lateness: std::mem::take(&mut lateness),
                work: std::mem::take(&mut work),
            });
            cpu_before = cpu;
            let elapsed = now.saturating_duration_since(t0);
            interval_index = (elapsed.as_nanos() / interval.as_nanos()) as u64;
            next_flush = t0 + interval * (interval_index as u32 + 1);
        }
        if !draining && (now >= end || shared.stop.load(Ordering::Relaxed)) {
            draining = true;
            drain_until = now + config.drain;
            for listener in &mut listeners {
                listener.highest_at_end = listener.track.highest;
            }
        }
        if draining && now >= drain_until {
            break;
        }
        if !draining {
            let wanted = share(shared.target.load(Ordering::Relaxed), worker, threads);
            budget = (budget + now.duration_since(budget_at).as_secs_f64() * rate).min(rate * 0.05 + 1.0);
            budget_at = now;
            while listeners.len() < wanted && budget >= 1.0 {
                budget -= 1.0;
                let index = worker + listeners.len() * threads;
                let socket = match listener_socket(&config, index) {
                    Ok(socket) => socket,
                    Err(_) => {
                        counters[index % edges].start_errors += 1;
                        break;
                    }
                };
                let slot = listeners.len();
                let mut event = libc::epoll_event {
                    events: libc::EPOLLIN as u32,
                    u64: slot as u64,
                };
                unsafe {
                    libc::epoll_ctl(epoll, libc::EPOLL_CTL_ADD, socket.as_raw_fd(), &mut event);
                }
                let edge = index % edges;
                let mut session = UdpSession::new(config.session, now);
                session.begin(config.edges[edge], None, now, &mut |to, bytes| {
                    let _ = socket.send_to(bytes, to);
                });
                let scheduled = session.next_wake();
                heap.push(Reverse((scheduled, slot)));
                counters[edge].started += 1;
                listeners.push(Listener {
                    index,
                    edge,
                    socket,
                    session,
                    started: now,
                    first_part: false,
                    track: PartTrack::new(),
                    scheduled,
                    seen: Seen::default(),
                    overflows: 0,
                    highest_at_end: None,
                    arrivals: (index < config.sample).then(BTreeMap::new),
                });
            }
            while listeners.len() > wanted {
                let mut listener = listeners.pop().unwrap();
                let socket = &listener.socket;
                listener.session.leave(&mut |to, bytes| {
                    let _ = socket.send_to(bytes, to);
                });
                unsafe {
                    libc::epoll_ctl(
                        epoll,
                        libc::EPOLL_CTL_DEL,
                        listener.socket.as_raw_fd(),
                        std::ptr::null_mut(),
                    );
                }
                let edge = listener.edge;
                if let Some(highest) = listener.track.highest {
                    listener.track.settle(highest + 1, &mut counters[edge]);
                }
                counters[edge].stopped += 1;
            }
        }
        // The timers that are due.
        while let Some(&Reverse((at, slot))) = heap.peek() {
            if at > now {
                break;
            }
            heap.pop();
            let Some(listener) = listeners.get_mut(slot) else {
                continue;
            };
            if listener.scheduled != at {
                continue;
            }
            lateness.record(now.saturating_duration_since(at).as_micros() as u64);
            work.polls += 1;
            let socket = &listener.socket;
            let failed = listener.session.poll(now, &mut |to, bytes| {
                let _ = socket.send_to(bytes, to);
            });
            if let Some(_reason) = failed {
                if !draining {
                    move_listener(listener, &config, &mut counters, now);
                }
            }
            listener.scheduled = listener.session.next_wake().max(now + Duration::from_micros(50));
            heap.push(Reverse((listener.scheduled, slot)));
        }
        // Wait for datagrams until the next timer.
        let mut wake = next_flush;
        if let Some(&Reverse((at, _))) = heap.peek() {
            wake = wake.min(at);
        }
        let wait = wake.saturating_duration_since(Instant::now());
        let timeout_ms = if wait.is_zero() {
            0
        } else {
            (wait.as_micros().div_ceil(1_000) as i32).min(5)
        };
        busy_ns += loop_start.elapsed().as_nanos() as u64;
        if !config.coalesce.is_zero() {
            let since = last_read.elapsed();
            if since < config.coalesce {
                std::thread::sleep(config.coalesce - since);
            }
        }
        let ready = unsafe { libc::epoll_wait(epoll, events.as_mut_ptr(), events.len() as i32, timeout_ms) };
        work.epoll_waits += 1;
        work.ready_sockets += ready.max(0) as u64;
        let work_start = Instant::now();
        if ready > 0 {
            last_read = work_start;
        }
        for event in events.iter().take(ready.max(0) as usize) {
            let slot = event.u64 as usize;
            let Some(listener) = listeners.get_mut(slot) else {
                continue;
            };
            let fd = listener.socket.as_raw_fd();
            loop {
                let received = bufs.recv(fd);
                work.recv_calls += 1;
                work.datagrams += received as u64;
                if received == 0 {
                    break;
                }
                let now = Instant::now();
                let now_us = now_unix_us();
                for index in 0..received {
                    let datagram = bufs.get(index);
                    if let Some(_reason) =
                        handle_datagram(listener, &mut counters, &config, &datagram, now, now_us)
                    {
                        if !draining {
                            move_listener(listener, &config, &mut counters, now);
                        }
                    }
                }
                if received < BATCH {
                    break;
                }
            }
            // A datagram can bring a NACK forward.
            let wake = listener.session.next_wake();
            if wake < listener.scheduled {
                listener.scheduled = wake;
                heap.push(Reverse((wake, slot)));
            }
        }
        busy_ns += work_start.elapsed().as_nanos() as u64;
    }

    // Settle each listener's parts up to the newest it had at the end, and
    // leave.
    let mut never_first = 0;
    let mut samples = Vec::new();
    for listener in &mut listeners {
        let edge = listener.edge;
        if let Some(highest) = listener.highest_at_end {
            listener.track.settle(highest + 1, &mut counters[edge]);
        }
        if !listener.first_part {
            never_first += 1;
        }
        let socket = &listener.socket;
        listener.session.leave(&mut |to, bytes| {
            let _ = socket.send_to(bytes, to);
        });
        if let Some(arrivals) = &listener.arrivals {
            let file = config.out_dir.as_ref().map(|dir| {
                dir.join("arrivals")
                    .join(format!("listener-{:05}.csv", listener.index))
            });
            if let Some(path) = &file {
                let mut text =
                    String::from("part,arrival_unix_us,bytes,published_unix_us,available_unix_us\n");
                for (part, (arrival, bytes, published, available)) in arrivals {
                    text.push_str(&format!("{part},{arrival},{bytes},{published},{available}\n"));
                }
                let _ = std::fs::write(path, text);
            }
            samples.push(json!({
                "listener": listener.index,
                "edge": config.edges[edge].to_string(),
                "parts": arrivals.len(),
                "file": file.map(|path| path.display().to_string()),
            }));
        }
    }
    collect_deltas(&mut listeners, &mut counters);
    for counters in &mut counters {
        // Not gauges at the end: the listeners left.
        counters.active = 0;
        counters.up = 0;
    }
    unsafe {
        libc::close(epoll);
    }
    let _ = tx.send(Report::Done {
        worker,
        edges: counters,
        never_first,
        samples,
    });
}

// ------------------------------------------------------------------ the host

/// The load host's own counters.
#[derive(Debug, Clone, Copy, Default)]
struct HostSample {
    process_ticks: u64,
    host_busy: u64,
    host_total: u64,
    udp_in_errors: u64,
    udp_rcvbuf_errors: u64,
}

fn host_sample() -> HostSample {
    let mut sample = HostSample::default();
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
        // The fields after the command name (which may hold spaces).
        if let Some(rest) = stat.rsplit_once(')').map(|(_, rest)| rest) {
            let fields: Vec<&str> = rest.split_whitespace().collect();
            // utime and stime are fields 14 and 15 of the line: 11 and 12
            // after the name and state.
            let utime: u64 = fields.get(11).and_then(|v| v.parse().ok()).unwrap_or(0);
            let stime: u64 = fields.get(12).and_then(|v| v.parse().ok()).unwrap_or(0);
            sample.process_ticks = utime + stime;
        }
    }
    if let Ok(stat) = std::fs::read_to_string("/proc/stat") {
        if let Some(line) = stat.lines().next() {
            let values: Vec<u64> = line
                .split_whitespace()
                .skip(1)
                .filter_map(|v| v.parse().ok())
                .collect();
            let total: u64 = values.iter().take(8).sum();
            let idle = values.get(3).copied().unwrap_or(0) + values.get(4).copied().unwrap_or(0);
            sample.host_total = total;
            sample.host_busy = total - idle;
        }
    }
    if let Ok(snmp) = std::fs::read_to_string("/proc/net/snmp") {
        let lines: Vec<&str> = snmp.lines().filter(|line| line.starts_with("Udp:")).collect();
        if lines.len() >= 2 {
            let names: Vec<&str> = lines[0].split_whitespace().collect();
            let values: Vec<&str> = lines[1].split_whitespace().collect();
            for (name, value) in names.iter().zip(&values) {
                match *name {
                    "InErrors" => sample.udp_in_errors = value.parse().unwrap_or(0),
                    "RcvbufErrors" => sample.udp_rcvbuf_errors = value.parse().unwrap_or(0),
                    _ => {}
                }
            }
        }
    }
    sample
}

fn rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmRSS:"))?
                .split_whitespace()
                .nth(1)?
                .parse::<f64>()
                .ok()
        })
        .map_or(0.0, |kb| kb / 1_024.0)
}

fn clock_ticks() -> f64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks > 0 {
        ticks as f64
    } else {
        100.0
    }
}

/// Raise the open-file limit to its hard limit: each listener holds a socket.
pub fn raise_file_limit() -> u64 {
    unsafe {
        let mut limit: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            limit.rlim_cur = limit.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &limit);
            return limit.rlim_cur;
        }
    }
    0
}

// ------------------------------------------------------------------ the run

/// Saturation of the load host in an interval, if any, and why: its CPU,
/// a worker that was busy nearly all the time, timers or datagrams handled
/// late, or the kernel dropping datagrams for full sockets.
fn saturation(
    host_busy: f64,
    worker_busy_max: f64,
    lateness_p99_us: u64,
    recv_lag_p99_us: u64,
    rcvbuf_errors: u64,
    socket_drops: u64,
    lag_limit_us: u64,
) -> Vec<&'static str> {
    let mut reasons = Vec::new();
    if host_busy > 0.9 {
        reasons.push("host-cpu");
    }
    if worker_busy_max > 0.9 {
        reasons.push("worker-busy");
    }
    if lateness_p99_us > lag_limit_us {
        reasons.push("timers-late");
    }
    if recv_lag_p99_us > lag_limit_us {
        reasons.push("receive-lag");
    }
    if rcvbuf_errors > 0 || socket_drops > 0 {
        reasons.push("receive-drops");
    }
    reasons
}

/// Run the listeners as `config` says; `on_interval` gets each interval's
/// line (also written to `intervals.jsonl` in the output directory). Returns
/// the summary (also written to `summary.json`).
pub fn run(config: LoadConfig, on_interval: &mut dyn FnMut(&Value)) -> std::io::Result<Value> {
    if config.edges.is_empty() {
        return Err(std::io::Error::other("no edge"));
    }
    if config.threads == 0 || config.bind_ips.is_empty() {
        return Err(std::io::Error::other("no worker thread or no bind address"));
    }
    let file_limit = raise_file_limit();
    let mut intervals_out = match &config.out_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir.join("arrivals"))?;
            Some(std::io::BufWriter::new(std::fs::File::create(
                dir.join("intervals.jsonl"),
            )?))
        }
        None => None,
    };
    let config = Arc::new(config);
    let shared = Arc::new(Shared {
        t0: Instant::now(),
        target: AtomicUsize::new(config.target_at(Duration::ZERO)),
        stop: AtomicBool::new(false),
    });
    let started_unix_us = now_unix_us();
    let (tx, rx) = mpsc::channel();
    let mut handles = Vec::new();
    for index in 0..config.threads {
        let (config, shared, tx) = (Arc::clone(&config), Arc::clone(&shared), tx.clone());
        handles.push(
            std::thread::Builder::new()
                .name(format!("{}{index}", config.thread_name))
                .spawn(move || worker(index, config, shared, tx))?,
        );
    }
    drop(tx);
    let edge_names: Vec<String> = config.edges.iter().map(ToString::to_string).collect();
    let ticks = clock_ticks();
    let interval_s = config.interval.as_secs_f64();
    let mut host_before = host_sample();
    let mut host_at = Instant::now();
    let mut pending: BTreeMap<u64, Vec<Report>> = BTreeMap::new();
    let mut reported = vec![None::<u64>; config.threads];
    let mut total_by_edge = vec![Counters::default(); config.edges.len()];
    // What the workers counted after their last interval (the drain, and
    // the parts settled at the end): the final line.
    let mut final_by_edge = vec![Counters::default(); config.edges.len()];
    let mut done = 0;
    let mut never_first = 0;
    let mut samples = Vec::new();
    let mut peak_up = 0_u64;
    let mut peak_active = 0_u64;
    let mut cpu_cores = Histogram::new();
    let mut saturated_intervals = Vec::new();
    let mut rcvbuf_total = 0_u64;
    let mut in_errors_total = 0_u64;
    let mut worker_busy_max_run = 0.0_f64;
    let mut host_busy_max_run = 0.0_f64;
    let mut first_saturated: Option<Value> = None;
    let mut lines = 0_u64;
    let mut rss_mb_max = 0.0_f64;

    while done < config.threads {
        let elapsed = shared.t0.elapsed();
        shared
            .target
            .store(config.target_at(elapsed), Ordering::Relaxed);
        if config
            .stop
            .as_ref()
            .is_some_and(|stop| stop.load(Ordering::Relaxed))
        {
            shared.stop.store(true, Ordering::Relaxed);
        }
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(report @ Report::Interval { .. }) => {
                if let Report::Interval {
                    worker, interval, ..
                } = &report
                {
                    reported[*worker] = Some(*interval);
                    pending.entry(*interval).or_default().push(report);
                }
            }
            Ok(Report::Done {
                worker,
                edges,
                never_first: none,
                samples: mine,
            }) => {
                reported[worker] = Some(u64::MAX);
                for (total, counters) in total_by_edge.iter_mut().zip(&edges) {
                    total.merge(counters);
                }
                for (last, counters) in final_by_edge.iter_mut().zip(&edges) {
                    last.merge(counters);
                }
                never_first += none;
                samples.extend(mine);
                done += 1;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        // Every interval that all workers have reported (a worker that
        // fell behind reports its next interval with the work of both).
        let ready = reported
            .iter()
            .try_fold(u64::MAX, |least, last| last.map(|last| least.min(last)));
        while let Some((&interval, _)) = pending.first_key_value() {
            if done < config.threads && ready.is_none_or(|ready| interval > ready) {
                break;
            }
            let reports = pending.remove(&interval).unwrap();
            let mut by_edge = vec![Counters::default(); config.edges.len()];
            let mut worker_busy = Vec::new();
            let mut worker_cpu = Vec::new();
            let mut lateness = Histogram::new();
            let mut work = LoopCounts::default();
            for report in &reports {
                if let Report::Interval {
                    edges,
                    cpu_ns,
                    busy_ns,
                    lateness: late,
                    work: counts,
                    ..
                } = report
                {
                    for (sum, counters) in by_edge.iter_mut().zip(edges) {
                        sum.merge(counters);
                    }
                    worker_busy.push(*busy_ns as f64 / 1e9 / interval_s);
                    worker_cpu.push(*cpu_ns as f64 / 1e9 / interval_s);
                    lateness.merge(late);
                    work.add(counts);
                }
            }
            let host = host_sample();
            let host_elapsed = host_at.elapsed().as_secs_f64().max(1e-3);
            let process_cores =
                host.process_ticks.saturating_sub(host_before.process_ticks) as f64 / ticks / host_elapsed;
            let host_busy = {
                let total = host.host_total.saturating_sub(host_before.host_total);
                (total > 0).then(|| host.host_busy.saturating_sub(host_before.host_busy) as f64 / total as f64)
            }
            .unwrap_or(0.0);
            let rcvbuf = host
                .udp_rcvbuf_errors
                .saturating_sub(host_before.udp_rcvbuf_errors);
            let in_errors = host.udp_in_errors.saturating_sub(host_before.udp_in_errors);
            host_before = host;
            host_at = Instant::now();
            rcvbuf_total += rcvbuf;
            in_errors_total += in_errors;
            let mut total = Counters::default();
            for counters in &by_edge {
                total.merge(counters);
            }
            for (sum, counters) in total_by_edge.iter_mut().zip(&by_edge) {
                sum.merge(counters);
            }
            peak_up = peak_up.max(total.up);
            peak_active = peak_active.max(total.active);
            cpu_cores.record((process_cores * 1_000.0) as u64);
            let busy_max = worker_busy.iter().copied().fold(0.0, f64::max);
            worker_busy_max_run = worker_busy_max_run.max(busy_max);
            host_busy_max_run = host_busy_max_run.max(host_busy);
            let reasons = saturation(
                host_busy,
                busy_max,
                lateness.quantile(0.99).unwrap_or(0),
                total.recv_lag.quantile(0.99).unwrap_or(0),
                rcvbuf,
                total.socket_drops,
                config.lag_limit.as_micros() as u64,
            );
            let t = (interval + 1) as f64 * interval_s;
            let mut per_edge = Map::new();
            for (name, counters) in edge_names.iter().zip(&by_edge) {
                per_edge.insert(name.clone(), counters.to_json(interval_s, true));
            }
            let rss_mb = rss_mb();
            rss_mb_max = rss_mb_max.max(rss_mb);
            let line = json!({
                "schema": "needletail.load.interval.v1",
                "label": config.label,
                "phase": "run",
                "t": t,
                "load_host_saturated": !reasons.is_empty(),
                "interval": interval,
                "unix_ms": started_unix_us / 1_000 + (t * 1_000.0) as u64,
                "target": config.target_at(Duration::from_secs_f64(t)),
                "total": total.to_json(interval_s, true),
                "edges": per_edge,
                "load_host": {
                    "process_cpu_cores": process_cores,
                    "host_cpu_busy": host_busy,
                    "worker_busy": worker_busy,
                    "worker_cpu": worker_cpu,
                    "timer_lateness_ms": lateness.summary_ms(),
                    "loop": work.to_json(interval_s),
                    "udp_rcvbuf_errors": rcvbuf,
                    "udp_in_errors": in_errors,
                    "rss_mb": rss_mb,
                    "saturated": !reasons.is_empty(),
                    "saturation": reasons,
                },
            });
            if !reasons.is_empty() {
                saturated_intervals.push(t);
                if first_saturated.is_none() {
                    first_saturated = Some(json!({"t": t, "listeners": total.active, "why": reasons}));
                }
            }
            if let Some(out) = &mut intervals_out {
                let _ = writeln!(out, "{line}");
                let _ = out.flush();
            }
            if config.progress {
                eprintln!(
                    "t={t:>6.1}s listeners {:>6}/{:<6} up {:>6} parts {:>9} missing {:>6} \
                     p50 {} p99 {} ms, load cpu {:.2} cores{}",
                    total.active,
                    config.target_at(Duration::from_secs_f64(t)),
                    total.up,
                    total.parts,
                    total.missing,
                    total
                        .latency
                        .quantile(0.5)
                        .map_or("-".into(), |us| format!("{:.2}", us as f64 / 1e3)),
                    total
                        .latency
                        .quantile(0.99)
                        .map_or("-".into(), |us| format!("{:.2}", us as f64 / 1e3)),
                    process_cores,
                    if reasons.is_empty() {
                        String::new()
                    } else {
                        format!(" SATURATED {}", reasons.join(","))
                    },
                );
            }
            on_interval(&line);
            lines += 1;
        }
    }
    for handle in handles {
        let _ = handle.join();
    }
    {
        let mut last = Counters::default();
        let mut per_edge = Map::new();
        for (name, counters) in edge_names.iter().zip(&final_by_edge) {
            last.merge(counters);
            per_edge.insert(name.clone(), counters.to_json(0.0, false));
        }
        let line = json!({
            "schema": "needletail.load.interval.v1",
            "label": config.label,
            "phase": "final",
            "t": shared.t0.elapsed().as_secs_f64(),
            "load_host_saturated": false,
            "total": last.to_json(0.0, false),
            "edges": per_edge,
        });
        if let Some(out) = &mut intervals_out {
            let _ = writeln!(out, "{line}");
            let _ = out.flush();
        }
        on_interval(&line);
        lines += 1;
    }
    let seconds = shared.t0.elapsed().as_secs_f64();
    let mut total = Counters::default();
    for counters in &total_by_edge {
        total.merge(counters);
    }
    let edge_summary = |counters: &Counters| {
        json!({
            "listeners_started": counters.started,
            "start_errors": counters.start_errors,
            "parts": counters.parts,
            "missing": counters.missing,
            "missing_pct": counters.missing_percent(),
            "gaps": counters.gaps,
            "late": counters.late,
            "duplicates": counters.duplicates,
            "recovered": counters.recovered,
            "nack_parts": counters.nack_parts,
            "nack_datagrams": counters.nack_datagrams,
            "abandoned": counters.abandoned,
            "datagrams": counters.datagrams,
            "bytes_in": counters.bytes_in,
            "mbit_s_in": counters.bytes_in as f64 * 8.0 / seconds.max(1e-3) / 1e6,
            "moves": counters.moves,
            "decode_errors": counters.decode_errors,
            "socket_drops": counters.socket_drops,
            "truncated": counters.truncated,
            "resets": counters.resets,
            "listeners_with_parts": counters.first_parts,
            "latency_ms": counters.latency.summary_with_buckets(),
            "ttfp_ms": counters.ttfp.summary_with_buckets(),
            "recv_lag_ms": counters.recv_lag.summary_with_buckets(),
        })
    };
    let mut per_edge = Map::new();
    for (name, counters) in edge_names.iter().zip(&total_by_edge) {
        per_edge.insert(name.clone(), edge_summary(counters));
    }
    samples.sort_by_key(|sample| sample["listener"].as_u64());
    let summary = json!({
        "schema": "needletail.load.v1",
        "label": config.label,
        "stream_id": config.session.stream_id,
        "edges_used": edge_names,
        "threads": config.threads,
        "ramp": config.ramp.iter().map(|(at, count)| json!({"at_s": at.as_secs_f64(), "listeners": count})).collect::<Vec<_>>(),
        "peak_target": config.peak(),
        "duration_s": config.duration.as_secs_f64(),
        "wall_s": seconds,
        "intervals": lines,
        "peak_listeners": peak_active,
        "peak_subscribed": peak_up,
        "listeners_without_parts": never_first,
        "lanes": {
            "fast_lane": config.session.fast_lane,
            "compact": config.session.compact,
        },
        "kernel_timestamps": config.kernel_timestamps,
        "total": edge_summary(&total),
        "per_edge": per_edge,
        "load_host": {
            "first_saturated": first_saturated.clone(),
            "process_cpu_cores_p50": cpu_cores.quantile(0.5).map(|v| v as f64 / 1_000.0),
            "process_cpu_cores_max": (cpu_cores.count() > 0).then(|| cpu_cores.max() as f64 / 1_000.0),
            "cpu_cores_per_1000_listeners": (peak_active > 0)
                .then(|| cpu_cores.max() as f64 / 1_000.0 / peak_active as f64 * 1_000.0),
            "host_cpu_busy_max": host_busy_max_run,
            "worker_busy_max": worker_busy_max_run,
            "udp_rcvbuf_errors": rcvbuf_total,
            "udp_in_errors": in_errors_total,
            "rss_mb_max": rss_mb_max,
            "saturated_intervals": saturated_intervals,
            "open_file_limit": file_limit,
        },
        "load_host_saturated": first_saturated.is_some(),
        "samples": samples,
    });
    if let Some(dir) = &config.out_dir {
        std::fs::write(
            dir.join("summary.json"),
            serde_json::to_vec_pretty(&summary).unwrap_or_default(),
        )?;
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_histogram_buckets_are_within_two_percent() {
        let mut histogram = Histogram::new();
        for value in [0_u64, 1, 127, 128, 255, 256, 1_000, 12_345, 1_000_000, u64::MAX / 2] {
            let index = Histogram::index(value);
            assert!(index < BUCKETS, "{value}");
            let middle = Histogram::value_of(index);
            let error = (middle as f64 - value as f64).abs() / (value.max(1) as f64);
            assert!(error < 0.02, "{value}: bucket middle {middle}");
            histogram.record(value);
        }
        assert_eq!(Histogram::index(u64::MAX), BUCKETS - 1);
        // Bucket indexes grow with the value.
        let mut last = 0;
        for value in 0..100_000_u64 {
            let index = Histogram::index(value);
            assert!(index >= last);
            last = index;
        }
        assert_eq!(histogram.count(), 10);
        // A bucket's lowest value falls in it, and the one below does not.
        for index in [0, 127, 128, 129, 191, 192, 1_000, BUCKETS - 1] {
            let low = Histogram::low_of(index);
            assert_eq!(Histogram::index(low), index);
            if low > 0 {
                assert_eq!(Histogram::index(low - 1), index - 1);
            }
        }
        let buckets = histogram.buckets();
        assert_eq!(buckets.iter().map(|[_, count]| count).sum::<u64>(), 10);
        assert_eq!(buckets[0], [0, 1]);
    }

    #[test]
    fn quantiles_of_a_merged_histogram() {
        let mut a = Histogram::new();
        let mut b = Histogram::new();
        for value in 1..=1_000_u64 {
            a.record(value * 10);
        }
        for value in 1..=1_000_u64 {
            b.record(value * 10 + 5);
        }
        let empty = Histogram::new();
        a.merge(&empty);
        let mut merged = Histogram::new();
        merged.merge(&a);
        merged.merge(&b);
        assert_eq!(merged.count(), 2_000);
        let p50 = merged.quantile(0.5).unwrap() as f64;
        assert!((p50 - 5_000.0).abs() / 5_000.0 < 0.02, "{p50}");
        let p99 = merged.quantile(0.99).unwrap() as f64;
        assert!((p99 - 9_900.0).abs() / 9_900.0 < 0.02, "{p99}");
        assert_eq!(merged.quantile(1.0), Some(10_005));
        assert_eq!(empty.quantile(0.5), None);
    }

    #[test]
    fn parts_settle_as_missing_or_gaps_once_they_are_far_enough_behind() {
        let mut track = PartTrack::new();
        let mut counters = Counters::default();
        track.begin(100);
        for part in 100..215 {
            if part == 150 || part == 151 {
                continue;
            }
            let gap = part == 160;
            assert_eq!(track.mark(part, gap, 50, &mut counters), Mark::New);
        }
        // Not yet 50 parts behind the newest part (214): not counted.
        assert_eq!(track.next_settle, 165);
        // 150 and 151 are: missing; 160 a gap.
        assert_eq!(counters.missing, 2);
        assert_eq!(counters.gaps, 1);
        assert_eq!(track.mark(120, false, 50, &mut counters), Mark::Late);
        assert_eq!(track.mark(199, false, 50, &mut counters), Mark::Duplicate);
        // A part that comes after its gap within the window is a part.
        assert_eq!(track.mark(220, true, 50, &mut counters), Mark::New);
        assert_eq!(track.mark(220, true, 50, &mut counters), Mark::Duplicate);
        assert_eq!(track.mark(220, false, 50, &mut counters), Mark::New);
        // Holes near the end settle when the run ends.
        let mut counters = Counters::default();
        let mut track = PartTrack::new();
        track.begin(0);
        track.mark(0, false, 1_000, &mut counters);
        track.mark(3, false, 1_000, &mut counters);
        track.settle(track.highest.unwrap() + 1, &mut counters);
        assert_eq!(counters.missing, 2);
        // A restart (parts from 0 again) starts the accounting again.
        let mut counters = Counters::default();
        track.mark(10_000_000, false, 1_000, &mut counters);
        assert_eq!(counters.resets, 1);
        assert_eq!(counters.missing, 0);
    }

    #[test]
    fn a_long_run_of_parts_beyond_the_ring_counts_what_never_came() {
        let mut track = PartTrack::new();
        let mut counters = Counters::default();
        track.begin(0);
        for part in (0..20_000_u64).filter(|part| part % 100 != 7) {
            track.mark(part, false, 1_500, &mut counters);
        }
        track.settle(20_000, &mut counters);
        assert_eq!(counters.missing, 200);
        assert_eq!(counters.resets, 0);
    }

    #[test]
    fn the_ramp_is_a_step_function_and_each_worker_has_its_share() {
        let ramp = parse_ramp("100@0, 1000@30,5_000@60").unwrap();
        let config = LoadConfig {
            ramp,
            ..LoadConfig::new(SessionConfig::new(1), vec!["127.0.0.1:1".parse().unwrap()], 0)
        };
        assert_eq!(config.target_at(Duration::ZERO), 100);
        assert_eq!(config.target_at(Duration::from_secs(29)), 100);
        assert_eq!(config.target_at(Duration::from_secs(30)), 1_000);
        assert_eq!(config.target_at(Duration::from_secs(600)), 5_000);
        assert_eq!(config.peak(), 5_000);
        assert_eq!(parse_ramp("250").unwrap(), vec![(Duration::ZERO, 250)]);
        assert!(parse_ramp("x@1").is_err());
        assert!(parse_ramp("").is_err());
        for total in [0, 1, 7, 1_000, 5_001] {
            let sum: usize = (0..6).map(|worker| share(total, worker, 6)).sum();
            assert_eq!(sum, total);
        }
    }

    #[test]
    fn saturation_names_its_reasons() {
        assert!(saturation(0.5, 0.5, 100, 100, 0, 0, 5_000).is_empty());
        assert_eq!(
            saturation(0.95, 0.99, 10_000, 10_000, 1, 0, 5_000),
            vec!["host-cpu", "worker-busy", "timers-late", "receive-lag", "receive-drops"]
        );
    }
}
