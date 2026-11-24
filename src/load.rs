//! Thousands of simulated UDP listeners on one host (`needletail-load`).
//!
//! Each listener is a real client of the edge: its own UDP socket, its own
//! token and subscription, the `NTU/1` handshake, the compact lane and the
//! RaptorQ lanes decoded by `crate::client_udp`, NACKs, loss measurement and
//! the repair and compact plan that follow from it: the session of
//! `crate::client_session` and the edge-choosing code of
//! `crate::client_director`, both as `needletail-udp-client` runs them and
//! with its defaults (two legs on two edges and a spare; B 2.4, 3.5). A
//! listener asks its entry point for its legs with `ASSIGN` from its own
//! socket, the HTTPS hedge beside it (`crate::load_assign`).
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

use crate::client_compact::IP_UDP_HEADER_BYTES;
use crate::client_director::{Action, Director, DirectorConfig, LegKey, ReplacementKind, MAX_LEGS};
use crate::client_session::{Received, SessionConfig, UdpSession};
use crate::client_udp::ObjectKind;
use crate::load_assign::{rendezvous_score, AssignConfig, Job, Reply, ResolveAnswer, ResolveRequest, Resolver};
use serde_json::{json, Map, Value};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::sync::Mutex;
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
    /// Edges that did not answer a listener's compact `SUB` in 1.5 s and
    /// were asked without the compact lane (the RaptorQ lane from then on),
    /// and edges that refused the short form and were asked with the long
    /// form (wave 7).
    pub compact_fallbacks: u64,
    pub short_header_dropped: u64,
    /// Publication to arrival of each new part, µs.
    pub latency: Histogram,
    /// Listener start (its first `SUB`) to its first part, µs.
    pub ttfp: Histogram,
    /// Listener start to its first `OK`, µs: with `ttfp`, the time to first
    /// audio split into the handshake and the first part after it.
    pub ttok: Histogram,
    /// Kernel receive timestamp to processing, µs: the load host's own lag.
    pub recv_lag: Histogram,
    /// Phase AM item 10. Moves away from this edge (`moves` counts those
    /// that came to it), by reason; the time from a moving listener's last
    /// part on its old edge to its first part on the new one (µs, booked
    /// on the new edge); the parts lost in moves (missing between the two)
    /// and every missing part of a listener that has moved, both booked on
    /// the edge the listener is on when the part settles.
    pub moves_out: u64,
    pub moves_by_reason: BTreeMap<String, u64>,
    /// Wave 9 review: the moves that came to this edge (`moves`), by the
    /// reason the listener left its last edge (a scale run judges a
    /// fault's spread by the fault's own reasons, not by rebalances).
    pub moves_in_by_reason: BTreeMap<String, u64>,
    pub time_to_move: Histogram,
    pub move_lost: u64,
    pub missing_moved: u64,
    /// Parts lost in moves away from this edge (from a listener's last part
    /// here to its first on the next edge), booked here when they settle:
    /// what a fault of this edge cost its listeners (wave 8 review).
    pub left_lost: u64,
    /// Gauges: listeners with no part for `stranded_after` (5 s), and moved
    /// listeners still waiting for their first part on their new edge. At
    /// the end, `never_resumed`: moved listeners that never had one.
    pub stranded: u64,
    pub resuming: u64,
    pub never_resumed: u64,
    /// Assignment (`/api/assignment`, Phase AM item 10): answers, their
    /// time (µs), failures (no entry point answered), and listeners placed
    /// by rendezvous over the known edges for want of an answer.
    pub assignments: u64,
    pub assignment_errors: u64,
    pub assignment_time: Histogram,
    pub fallbacks: u64,
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
        self.compact_fallbacks += other.compact_fallbacks;
        self.short_header_dropped += other.short_header_dropped;
        self.latency.merge(&other.latency);
        self.ttfp.merge(&other.ttfp);
        self.ttok.merge(&other.ttok);
        self.recv_lag.merge(&other.recv_lag);
        self.moves_out += other.moves_out;
        for (reason, count) in &other.moves_in_by_reason {
            *self.moves_in_by_reason.entry(reason.clone()).or_default() += count;
        }
        for (reason, count) in &other.moves_by_reason {
            *self.moves_by_reason.entry(reason.clone()).or_default() += count;
        }
        self.time_to_move.merge(&other.time_to_move);
        self.move_lost += other.move_lost;
        self.missing_moved += other.missing_moved;
        self.left_lost += other.left_lost;
        self.stranded += other.stranded;
        self.resuming += other.resuming;
        self.never_resumed += other.never_resumed;
        self.assignments += other.assignments;
        self.assignment_errors += other.assignment_errors;
        self.assignment_time.merge(&other.assignment_time);
        self.fallbacks += other.fallbacks;
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
            "compact_fallbacks": self.compact_fallbacks,
            "short_header_dropped": self.short_header_dropped,
            "latency_ms": self.latency.summary_ms(),
            "ttfp_ms": self.ttfp.summary_ms(),
            "ttok_ms": self.ttok.summary_ms(),
            "recv_lag_ms": self.recv_lag.summary_ms(),
        });
        self.am10_json(&mut value);
        if gauges {
            value["active"] = json!(self.active);
            value["up"] = json!(self.up);
            value["stranded"] = json!(self.stranded);
            value["resuming"] = json!(self.resuming);
        }
        value
    }

    /// The Phase AM item 10 counters, into `value`. The time to move has
    /// its buckets, so that a window of intervals merges.
    fn am10_json(&self, value: &mut Value) {
        value["moves_out"] = json!(self.moves_out);
        value["moves_by_reason"] = json!(self.moves_by_reason);
        value["moves_in_by_reason"] = json!(self.moves_in_by_reason);
        value["time_to_move_ms"] = self.time_to_move.summary_with_buckets();
        value["move_lost"] = json!(self.move_lost);
        value["missing_moved"] = json!(self.missing_moved);
        value["left_lost"] = json!(self.left_lost);
        value["never_resumed"] = json!(self.never_resumed);
        value["assignments"] = json!(self.assignments);
        value["assignment_errors"] = json!(self.assignment_errors);
        value["assignment_ms"] = self.assignment_time.summary_ms();
        value["fallbacks"] = json!(self.fallbacks);
    }
}

// ------------------------------------------------------------------ parts

const TRACK: u64 = 4_096;
const TRACK_WORDS: usize = (TRACK / 64) as usize;
/// A part far from a listener's others starts its accounting again only
/// when the next far part follows it within this many parts (a contributor
/// restart, or a listener that was away): one stray part (a late copy, a
/// misread number) is counted late and changes nothing.
const JUMP_CONFIRM: u64 = 256;

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
    /// The last far part (and whether it was a gap), until the next part
    /// confirms the jump or a near part shows it was a stray.
    jump: Option<(u64, bool)>,
    /// Phase AM item 10: the part from which a leg of the listener was
    /// first replaced (its missing parts are a moved listener's), and the
    /// parts of its recent interruptions (from the part after its last
    /// before the silence to its first after it).
    moved_from: Option<u64>,
    /// Each window: its first part, the part after its last, and the edge
    /// (book index) that delivered the part before it.
    windows: VecDeque<(u64, u64, usize)>,
    /// Parts lost in moves, by the edge left, not yet booked (see
    /// [`PartTrack::book_left_lost`]).
    left_lost: Vec<(usize, u64)>,
}

/// Interruptions whose parts a listener keeps track of.
const MOVE_WINDOWS: usize = 8;

impl PartTrack {
    fn new() -> Box<Self> {
        Box::new(Self {
            start: None,
            next_settle: 0,
            highest: None,
            got: [0; TRACK_WORDS],
            gap: [0; TRACK_WORDS],
            jump: None,
            moved_from: None,
            windows: VecDeque::new(),
            left_lost: Vec::new(),
        })
    }

    /// A leg of the listener was replaced: the parts from the next one on
    /// are a moved listener's.
    fn moved(&mut self) {
        let from = self.highest.map_or(self.next_settle, |highest| highest + 1);
        self.moved_from.get_or_insert(from);
    }

    /// The listener was interrupted (no part for the interruption's length)
    /// between part `from - 1`, delivered by edge `left`, and part `to`:
    /// the parts between that never come are the move's.
    fn interrupted(&mut self, from: u64, to: u64, left: usize) {
        let from = from.max(self.next_settle);
        if to <= from {
            return;
        }
        if self.windows.len() == MOVE_WINDOWS {
            self.windows.pop_front();
        }
        self.windows.push_back((from, to, left));
    }

    /// Book the parts lost in moves on the edges the listener left
    /// (`left_lost`): a scale run takes the moves off a faulted node as the
    /// fault's, and every other loss as the unaffected listeners' (wave 8
    /// review: `missing_moved` counted every part a listener lost after its
    /// first move, whatever the cause, to the end of the run).
    fn book_left_lost(&mut self, counters: &mut [Counters]) {
        for (edge, lost) in self.left_lost.drain(..) {
            if let Some(counters) = counters.get_mut(edge) {
                counters.left_lost += lost;
            }
        }
    }

    fn set(&mut self, part: u64, gap: bool) {
        let (word, bit) = (((part % TRACK) / 64) as usize, 1_u64 << (part % 64));
        if gap {
            self.gap[word] |= bit;
        } else {
            self.got[word] |= bit;
        }
        self.highest = Some(self.highest.map_or(part, |highest| highest.max(part)));
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
        self.windows.clear();
        if self.moved_from.is_some() {
            self.moved_from = Some(first);
        }
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
                    if self.moved_from.is_some_and(|from| part >= from) {
                        counters.missing_moved += 1;
                    }
                    if let Some((_, _, left)) = self
                        .windows
                        .iter()
                        .find(|(from, to, _)| *from <= part && part < *to)
                    {
                        counters.move_lost += 1;
                        match self.left_lost.last_mut() {
                            Some((edge, lost)) if *edge == *left => *lost += 1,
                            _ => self.left_lost.push((*left, 1)),
                        }
                    }
                }
            }
            self.got[word] &= !bit;
            self.gap[word] &= !bit;
            self.next_settle += 1;
        }
        let settled = self.next_settle;
        self.windows.retain(|(_, to, _)| *to > settled);
    }

    fn mark(&mut self, part: u64, gap: bool, settle_parts: u64, counters: &mut Counters) -> Mark {
        if self.start.is_none() {
            self.begin(part);
        }
        // A contributor restart counts its parts again; a jump far ahead
        // is one too (or a listener that was away): start again there, once
        // a second far part confirms it. A single far part is a stray.
        if part + TRACK < self.next_settle || part >= self.next_settle + 16 * TRACK {
            match self.jump {
                Some((first, first_gap)) if part > first && part - first <= JUMP_CONFIRM => {
                    counters.resets += 1;
                    self.reset(first);
                    self.set(first, first_gap);
                    self.jump = None;
                }
                _ => {
                    self.jump = Some((part, gap));
                    return Mark::Late;
                }
            }
        } else {
            self.jump = None;
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
    /// The director's settings (`--paths`, `--spare`, the waits and the
    /// assignment's transport, B 2.4) with the client's defaults, and in
    /// `entries` the home region's entry points. Each listener takes a copy
    /// with its own session id and nonces.
    pub director: DirectorConfig,
    /// `--edges` alone (no entry point; the in-process tests): the edges'
    /// client UDP ports, which a listener's director is answered from in
    /// the session's rendezvous order, legs on two different edges and no
    /// spare. Empty with entry points.
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
    /// The HTTPS of the entry points (the hedge of each assignment request,
    /// and name lookups): needed with entry points.
    pub assign: Option<AssignConfig>,
    /// A listener with no part for this long is stranded.
    pub stranded_after: Duration,
    /// Session ids, `<prefix>-<listener>` (the CMCD `sid` of assignment).
    pub sid_prefix: String,
}

impl LoadConfig {
    /// A load of `listeners`; a session that keeps the receiver's default
    /// window memory keeps [`LOAD_KNOWN_PARTS`] here.
    pub fn new(mut session: SessionConfig, edges: Vec<SocketAddr>, listeners: usize) -> Self {
        if session.known_parts == 0 {
            session.known_parts = LOAD_KNOWN_PARTS;
        }
        Self {
            director: DirectorConfig::new(session.stream_id, "", Vec::new()),
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
            assign: None,
            stranded_after: Duration::from_secs(5),
            sid_prefix: "load".into(),
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

/// The pause before a worker tries again to make a socket that failed.
const START_RETRY: Duration = Duration::from_millis(100);
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

/// Counters of a listener at the last interval, for the next one's deltas.
#[derive(Default, Clone, Copy)]
struct Seen {
    recovered: u64,
    nack_parts: u64,
    nack_datagrams: u64,
    abandoned: u64,
    errors: u64,
    compact_fallbacks: u64,
    short_header_dropped: u64,
    answers: u64,
    assignment_failures: u64,
    failovers: u64,
}

/// A leg of a listener as the load books it: its edge in the book, and when
/// its edge last sent it media (kernel receive time, µs; 0: never).
#[derive(Debug, Clone)]
struct BookedLeg {
    key: LegKey,
    addr: SocketAddr,
    edge: usize,
    node: String,
    added_at: Instant,
    live: bool,
    last_media_us: u64,
}

/// One simulated listener: a session and its director, as
/// `needletail-udp-client` holds them, on a socket of its own.
struct Listener {
    /// Its session number in the worker: an answer of the resolver names it.
    id: u64,
    index: usize,
    sid: String,
    socket: UdpSocket,
    session: UdpSession,
    director: Director,
    /// The director's legs, booked (at most [`MAX_LEGS`]).
    legs: Vec<BookedLeg>,
    /// The edge of its longest-held live leg (before any: the edge it was
    /// started on, or "unassigned"): its gauges, missing parts and gaps go
    /// there (B 2.9).
    home: usize,
    /// An assignment asked and not answered yet, since then.
    asking_since: Option<Instant>,
    started: Instant,
    first_part: bool,
    subscribed_once: bool,
    track: Box<PartTrack>,
    /// The newest part delivered first and when (kernel time, µs), and the
    /// edge whose datagram delivered it.
    last_first: Option<(u64, u64, usize)>,
    last_part_at: Option<Instant>,
    scheduled: Instant,
    seen: Seen,
    overflows: u32,
    highest_at_end: Option<u64>,
    arrivals: Option<BTreeMap<u64, (u64, usize, u64, u64)>>,
}

impl Listener {
    /// When its session or its director must run next.
    fn wake(&self, draining: bool) -> Instant {
        let session = self.session.next_wake();
        if draining {
            session
        } else {
            session.min(self.director.next_wake())
        }
    }

    /// The edge of the leg whose edge sent a datagram from `peer`: the leg
    /// at exactly that address, else the only one at its IP (B 3.1), else
    /// its home; and that leg.
    fn sender(&self, peer: SocketAddr) -> (usize, Option<usize>) {
        if let Some(index) = self.legs.iter().position(|leg| leg.addr == peer) {
            return (self.legs[index].edge, Some(index));
        }
        let mut same_ip = self
            .legs
            .iter()
            .enumerate()
            .filter(|(_, leg)| leg.addr.ip() == peer.ip());
        match (same_ip.next(), same_ip.next()) {
            (Some((index, leg)), None) => (leg.edge, Some(index)),
            _ => (self.home, None),
        }
    }
}

/// Every edge that the listeners use, by index (the counters' order): the
/// static ones first ("unassigned" before them with entry points), then the
/// ones that answers name, as they come.
pub(crate) struct EdgeBook {
    edges: Mutex<Vec<BookEdge>>,
    unassigned: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct BookEdge {
    addr: Option<SocketAddr>,
    node: Option<String>,
}

impl BookEdge {
    /// The name of its counters: its address.
    fn name(&self) -> String {
        self.addr
            .map_or_else(|| "unassigned".to_owned(), |addr| addr.to_string())
    }
}

impl EdgeBook {
    fn new(edges: &[SocketAddr], unassigned: bool) -> Self {
        let mut list = Vec::new();
        if unassigned {
            list.push(BookEdge {
                addr: None,
                node: None,
            });
        }
        list.extend(edges.iter().map(|addr| BookEdge {
            addr: Some(*addr),
            node: Some(addr.to_string()),
        }));
        Self {
            edges: Mutex::new(list),
            unassigned: unassigned.then_some(0),
        }
    }

    /// The index of the edge at `addr` (added if new), naming its node.
    fn index(&self, addr: SocketAddr, node: &str) -> usize {
        let mut edges = self.edges.lock().unwrap();
        if let Some(index) = edges.iter().position(|edge| edge.addr == Some(addr)) {
            edges[index].node.get_or_insert_with(|| node.to_owned());
            return index;
        }
        edges.push(BookEdge {
            addr: Some(addr),
            node: Some(node.to_owned()),
        });
        edges.len() - 1
    }

    /// The index of the edge of node `node`, if it is booked.
    fn index_of_node(&self, node: &str) -> Option<usize> {
        self.edges
            .lock()
            .unwrap()
            .iter()
            .position(|edge| edge.node.as_deref() == Some(node))
    }

    fn snapshot(&self) -> Vec<BookEdge> {
        self.edges.lock().unwrap().clone()
    }
}

/// `counters` long enough for edge `index`.
fn ensure(counters: &mut Vec<Counters>, index: usize) {
    if counters.len() <= index {
        counters.resize_with(index + 1, Counters::default);
    }
}

/// Add `from` into `into`, edge by edge.
fn merge_edges(into: &mut Vec<Counters>, from: &[Counters]) {
    if into.len() < from.len() {
        into.resize_with(from.len(), Counters::default);
    }
    for (sum, counters) in into.iter_mut().zip(from) {
        sum.merge(counters);
    }
}

/// The entry point of `--edges` alone: its requests are answered in the
/// worker ([`static_answer`]), never sent.
const STATIC_ENTRY: &str = "https://static-edges.invalid";

/// The answer of `--edges` alone to a director's assignment request (its
/// `/api/assignment` path): the edges in the session's rendezvous order,
/// without those the listener holds (`hold=`) and those it skips (`skip=`,
/// unless that leaves none, as an entry point does), as many legs granted as
/// it asks for (`paths=`) and edges remain, and no spare.
fn static_answer(edges: &[SocketAddr], sid: &str, path: &str) -> Vec<u8> {
    let query = path.split_once('?').map_or("", |(_, query)| query);
    let list = |name: &str| -> Vec<&str> {
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
            .map(|value| value.split(',').filter(|node| !node.is_empty()).collect())
            .unwrap_or_default()
    };
    let (skip, hold) = (list("skip"), list("hold"));
    let paths = list("paths")
        .first()
        .and_then(|paths| paths.parse::<usize>().ok())
        .unwrap_or(1);
    let mut ranked: Vec<(u64, String, SocketAddr)> = edges
        .iter()
        .map(|addr| {
            let node = addr.to_string();
            (rendezvous_score(sid, node.as_str()), node, *addr)
        })
        .filter(|(_, node, _)| !hold.contains(&node.as_str()))
        .collect();
    ranked.sort_by(|a, b| b.0.cmp(&a.0));
    if ranked.iter().any(|(_, node, _)| !skip.contains(&node.as_str())) {
        ranked.retain(|(_, node, _)| !skip.contains(&node.as_str()));
    }
    let assigned: Vec<Value> = ranked
        .iter()
        .map(|(_, node, addr)| {
            let host = match addr.ip() {
                IpAddr::V4(ip) => ip.to_string(),
                IpAddr::V6(ip) => format!("[{ip}]"),
            };
            json!({
                "node_id": node,
                "playback_base_url": format!("https://{host}:443"),
                "client_udp_port": addr.port(),
            })
        })
        .collect();
    json!({
        "assigned": assigned,
        "paths": paths.min(ranked.len()),
        "spare": null,
    })
    .to_string()
    .into_bytes()
}

/// What a worker needs to drive its listeners' directors: its settings, the
/// edge book, the resolver (HTTPS and names, with entry points) and two
/// buffers of actions, kept between calls.
struct Drive<'a> {
    config: &'a LoadConfig,
    book: &'a EdgeBook,
    resolver: Option<&'a Resolver>,
    worker: usize,
    actions: Vec<Action>,
    doing: Vec<Action>,
}

impl Drive<'_> {
    /// Do what the director asked for (`self.actions`): datagrams from the
    /// listener's socket, the HTTPS hedge and name lookups to the resolver,
    /// and with `--edges` alone the answers at once. Then book what changed.
    fn perform(
        &mut self,
        listener: &mut Listener,
        slot: usize,
        counters: &mut Vec<Counters>,
        now: Instant,
    ) {
        while !self.actions.is_empty() {
            std::mem::swap(&mut self.actions, &mut self.doing);
            for action in self.doing.drain(..) {
                match action {
                    Action::Send { to, datagram } => {
                        // A full socket loses a request: it is repeated.
                        let _ = listener.socket.send_to(&datagram, to);
                    }
                    Action::Https { id, entry, path } => {
                        if self.config.director.entries.is_empty() {
                            let body = static_answer(&self.config.edges, &listener.sid, &path);
                            listener.director.on_https(
                                &mut listener.session,
                                id,
                                Some(&body),
                                now,
                                &mut self.actions,
                            );
                        } else if let Some(resolver) = self.resolver {
                            resolver.request(ResolveRequest {
                                worker: self.worker,
                                slot,
                                listener: listener.id,
                                job: Job::Https {
                                    id,
                                    entry,
                                    path,
                                    sid: listener.sid.clone(),
                                },
                            });
                        }
                    }
                    Action::Resolve { host } => {
                        if let Some(resolver) = self.resolver {
                            resolver.request(ResolveRequest {
                                worker: self.worker,
                                slot,
                                listener: listener.id,
                                job: Job::Resolve { host },
                            });
                        }
                    }
                }
            }
        }
        self.book_director(listener, counters, now);
    }

    /// Book the director's legs, its replacements and its assignments.
    fn book_director(&self, listener: &mut Listener, counters: &mut Vec<Counters>, now: Instant) {
        sync_legs(listener, self.book);
        ensure(counters, listener.home);
        for replacement in listener.director.replacements.drain(..) {
            if replacement.kind == ReplacementKind::Takeback {
                continue;
            }
            let from = self
                .book
                .index_of_node(&replacement.from)
                .unwrap_or(listener.home);
            let to = self
                .book
                .index_of_node(&replacement.to)
                .unwrap_or(listener.home);
            ensure(counters, from.max(to));
            counters[from].moves_out += 1;
            *counters[from]
                .moves_by_reason
                .entry(replacement.reason.clone())
                .or_default() += 1;
            counters[to].moves += 1;
            *counters[to]
                .moves_in_by_reason
                .entry(replacement.reason)
                .or_default() += 1;
            // The time to move is a listener's without a delivering leg.
            if replacement.kind == ReplacementKind::Interrupt {
                if let Some(exposure) = replacement.exposure {
                    counters[to].time_to_move.record(duration_us(exposure));
                }
            }
            listener.track.moved();
        }
        let stats = &listener.director.stats;
        let answers = stats.answers_udp + stats.answers_https;
        let seen = &mut listener.seen;
        let home = &mut counters[listener.home];
        if answers > seen.answers {
            home.assignments += answers - seen.answers;
            let since = listener.asking_since.take().unwrap_or(now);
            home.assignment_time
                .record(duration_us(now.saturating_duration_since(since)));
        }
        if stats.assignment_failures > seen.assignment_failures {
            home.assignment_errors += stats.assignment_failures - seen.assignment_failures;
            listener.asking_since = None;
        }
        home.fallbacks += stats.failovers.saturating_sub(seen.failovers);
        if stats.requests > answers + stats.assignment_failures {
            listener.asking_since.get_or_insert(now);
        }
        seen.answers = answers;
        seen.assignment_failures = stats.assignment_failures;
        seen.failovers = stats.failovers;
    }
}

fn duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

/// The director's legs into the listener's book of them (nothing is
/// allocated while they stay the same), and its home edge.
fn sync_legs(listener: &mut Listener, book: &EdgeBook) {
    let legs = listener.director.legs();
    listener
        .legs
        .retain(|booked| legs.iter().any(|leg| leg.key == booked.key));
    for leg in legs {
        match listener.legs.iter_mut().find(|booked| booked.key == leg.key) {
            Some(booked) => booked.live = leg.live,
            None => listener.legs.push(BookedLeg {
                key: leg.key,
                addr: leg.addr,
                edge: book.index(leg.addr, &leg.node),
                node: leg.node.clone(),
                added_at: leg.added_at,
                live: leg.live,
                last_media_us: 0,
            }),
        }
    }
    if let Some(home) = listener
        .legs
        .iter()
        .filter(|leg| leg.live)
        .min_by_key(|leg| leg.added_at)
        .or_else(|| listener.legs.first())
    {
        listener.home = home.edge;
    }
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

/// A silence between two parts delivered first this long or longer is an
/// interruption: the parts lost in it are the move's (`move_lost`, booked
/// on the edge that delivered the part before it, `left_lost`).
const INTERRUPT: Duration = Duration::from_millis(30);

/// One datagram of a listener. The director takes its own (`ASSIGNED`, a
/// `TOKEN` of an entry point or of the spare), the session the rest; the
/// datagram is booked on the edge that sent it, and each part it delivered
/// first on that edge too.
fn handle_datagram(
    listener: &mut Listener,
    counters: &mut Vec<Counters>,
    drive: &mut Drive<'_>,
    datagram: &Datagram<'_>,
    draining: bool,
    now: Instant,
    now_us: u64,
) {
    let home = listener.home;
    ensure(counters, home);
    if let Some(overflows) = datagram.overflows {
        counters[home].socket_drops += u64::from(overflows.wrapping_sub(listener.overflows));
        listener.overflows = overflows;
    }
    if datagram.truncated {
        counters[home].truncated += 1;
        return;
    }
    let Some(peer) = datagram.peer else {
        return;
    };
    if !draining
        && listener.director.on_datagram(
            &mut listener.session,
            peer,
            datagram.data,
            now,
            &mut drive.actions,
        )
    {
        return;
    }
    let (edge, leg) = listener.sender(peer);
    ensure(counters, edge);
    let arrival_us = datagram.kernel_us.unwrap_or(now_us);
    // The load host's own lag: the time from the kernel's receive timestamp
    // to now, which neither the latency nor the time to first part holds.
    let lag_us = datagram
        .kernel_us
        .map_or(0, |kernel_us| now_us.saturating_sub(kernel_us));
    let duplicates = listener.session.receiver.duplicate_parts;
    let socket = &listener.socket;
    let mut send = |to: SocketAddr, bytes: &[u8]| {
        // A full socket loses a request: the next refresh repeats it.
        let _ = socket.send_to(bytes, to);
    };
    match listener
        .session
        .on_datagram(peer, datagram.data, now, &mut send)
    {
        Received::Media => {
            let sender = &mut counters[edge];
            sender.datagrams += 1;
            sender.bytes_in += (datagram.data.len() + IP_UDP_HEADER_BYTES) as u64;
            if datagram.kernel_us.is_some() {
                sender.recv_lag.record(lag_us);
            }
            if let Some(leg) = leg {
                listener.legs[leg].last_media_us = arrival_us;
            }
        }
        Received::Subscribed { .. } => {
            if !listener.subscribed_once {
                listener.subscribed_once = true;
                counters[home].ttok.record(
                    duration_us(now.saturating_duration_since(listener.started))
                        .saturating_sub(lag_us),
                );
            }
            // The parts that came before the OK are taken now.
            if !listener.session.has_objects() {
                return;
            }
        }
        Received::Move(_) | Received::Control | Received::Ignored => return,
    }
    counters[edge].duplicates += listener
        .session
        .receiver
        .duplicate_parts
        .saturating_sub(duplicates);
    let settle_parts = drive.config.settle_parts;
    while let Some(object) = listener.session.next_object() {
        match object.kind {
            ObjectKind::Init => {}
            ObjectKind::Gap => {
                listener
                    .track
                    .mark(object.part, true, settle_parts, &mut counters[home]);
            }
            ObjectKind::Part => match listener.track.mark(
                object.part,
                false,
                settle_parts,
                &mut counters[home],
            ) {
                Mark::New => {
                    counters[edge].parts += 1;
                    if object.published_unix_us > 0 {
                        counters[edge]
                            .latency
                            .record(arrival_us.saturating_sub(object.published_unix_us));
                    }
                    if let Some((part, at_us, left)) = listener.last_first {
                        if arrival_us.saturating_sub(at_us) >= duration_us(INTERRUPT) {
                            listener.track.interrupted(part + 1, object.part, left);
                        }
                    }
                    listener.last_first = Some((object.part, arrival_us, edge));
                    listener.last_part_at = Some(now);
                    if !listener.first_part {
                        listener.first_part = true;
                        counters[home].first_parts += 1;
                        counters[home].ttfp.record(
                            duration_us(now.saturating_duration_since(listener.started))
                                .saturating_sub(lag_us),
                        );
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
                Mark::Duplicate => counters[edge].duplicates += 1,
                Mark::Late => counters[edge].late += 1,
            },
        }
    }
}

/// Read what waits at a listener's socket, every datagram of it (so that no
/// verdict is taken while datagrams are queued, B 3.1).
#[allow(clippy::too_many_arguments)]
fn read_socket(
    listener: &mut Listener,
    bufs: &mut RecvBufs,
    counters: &mut Vec<Counters>,
    drive: &mut Drive<'_>,
    work: &mut LoopCounts,
    draining: bool,
) {
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
            handle_datagram(listener, counters, drive, &datagram, draining, now, now_us);
        }
        listener.track.book_left_lost(counters);
        if received < BATCH {
            break;
        }
    }
}

/// The listener's timers after its socket was read: the session's (requests,
/// refreshes, NACKs, the verdicts on its legs) when due, then the director's
/// (it reads the session's events), and what they asked for. In the drain
/// at the end the director is left alone and the session holds its legs.
fn service(
    listener: &mut Listener,
    slot: usize,
    counters: &mut Vec<Counters>,
    drive: &mut Drive<'_>,
    draining: bool,
    now: Instant,
) {
    if now >= listener.session.next_wake() {
        let socket = &listener.socket;
        listener.session.poll_legs(now, &mut |to, bytes| {
            let _ = socket.send_to(bytes, to);
        });
    }
    if !draining {
        listener
            .director
            .poll(&mut listener.session, now, &mut drive.actions);
    }
    drive.perform(listener, slot, counters, now);
}

/// A listener's counters since the last interval into its home edge's, with
/// the gauges (`active`, `up`, `stranded`, `resuming`) when `gauges` (not
/// for one that stops).
fn collect_one(
    listener: &mut Listener,
    counters: &mut Vec<Counters>,
    gauges: Option<(Instant, Duration)>,
) {
    ensure(counters, listener.home);
    let edge = &mut counters[listener.home];
    if let Some((now, stranded_after)) = gauges {
        edge.active += 1;
        if listener.session.is_subscribed() {
            edge.up += 1;
        }
        let quiet_since = listener.last_part_at.unwrap_or(listener.started);
        if now.saturating_duration_since(quiet_since) >= stranded_after {
            edge.stranded += 1;
        }
        if listener.first_part && now.saturating_duration_since(quiet_since) >= INTERRUPT {
            edge.resuming += 1;
        }
    }
    let session = &listener.session;
    let receiver = &session.receiver;
    let now = Seen {
        recovered: receiver.recovered_objects,
        nack_parts: receiver.compact.stats.nacked,
        nack_datagrams: session.nack_datagrams,
        abandoned: receiver.compact.stats.abandoned,
        errors: receiver.errors,
        compact_fallbacks: session.compact_fallbacks,
        short_header_dropped: session.short_header_dropped,
        ..listener.seen
    };
    let seen = listener.seen;
    edge.recovered += now.recovered.saturating_sub(seen.recovered);
    edge.nack_parts += now.nack_parts.saturating_sub(seen.nack_parts);
    edge.nack_datagrams += now.nack_datagrams.saturating_sub(seen.nack_datagrams);
    edge.abandoned += now.abandoned.saturating_sub(seen.abandoned);
    edge.decode_errors += now.errors.saturating_sub(seen.errors);
    edge.compact_fallbacks += now.compact_fallbacks.saturating_sub(seen.compact_fallbacks);
    edge.short_header_dropped += now
        .short_header_dropped
        .saturating_sub(seen.short_header_dropped);
    listener.seen = now;
}

impl Listener {
    /// Its session number in the worker and its settings: a new listener,
    /// its socket made and its first request not yet asked.
    #[allow(clippy::too_many_arguments)]
    fn new(
        config: &LoadConfig,
        id: u64,
        index: usize,
        home: usize,
        socket: UdpSocket,
        now: Instant,
    ) -> Self {
        let sid = format!("{}-{index}", config.sid_prefix);
        let mut director = config.director.clone();
        director.stream_id = config.session.stream_id;
        director.session = sid.clone();
        director.nonce_seed = rendezvous_score(&sid, &now_unix_us());
        if director.entries.is_empty() {
            // --edges alone: the requests are answered in the worker, over
            // "HTTPS" at once, and no spare is named.
            director.entries = vec![STATIC_ENTRY.to_owned()];
            director.assignment_udp_port = 0;
            director.spare = false;
        }
        Self {
            id,
            index,
            sid,
            socket,
            session: UdpSession::new(config.session, now),
            director: Director::new(director, now),
            legs: Vec::with_capacity(MAX_LEGS),
            home,
            asking_since: None,
            started: now,
            first_part: false,
            subscribed_once: false,
            track: PartTrack::new(),
            last_first: None,
            last_part_at: None,
            scheduled: now,
            seen: Seen::default(),
            overflows: 0,
            highest_at_end: None,
            arrivals: (index < config.sample).then(BTreeMap::new),
        }
    }
}

/// The listener in `slot`, if it holds one.
fn occupied(listeners: &mut [Option<Listener>], slot: usize) -> Option<&mut Listener> {
    listeners.get_mut(slot).and_then(Option::as_mut)
}

fn worker(
    worker: usize,
    config: Arc<LoadConfig>,
    shared: Arc<Shared>,
    tx: mpsc::Sender<Report>,
    book: Arc<EdgeBook>,
    assign: Option<(Arc<Resolver>, mpsc::Receiver<ResolveAnswer>)>,
) {
    let threads = config.threads;
    let edges = book.snapshot().len();
    let stranded_after = config.stranded_after;
    let epoll = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    assert!(epoll >= 0, "epoll_create1");
    let mut events: Vec<libc::epoll_event> = vec![libc::epoll_event { events: 0, u64: 0 }; 1_024];
    let mut bufs = RecvBufs::new();
    // The listeners by slot (a slot keeps its listener for the listener's
    // life; free slots are taken again).
    let mut listeners: Vec<Option<Listener>> = Vec::new();
    let mut free: Vec<usize> = Vec::new();
    let mut live = 0_usize;
    let mut joined = 0_usize;
    let mut next_id = 0_u64;
    let mut heap: BinaryHeap<Reverse<(Instant, usize)>> = BinaryHeap::new();
    let mut counters = vec![Counters::default(); edges];
    let mut lateness = Histogram::new();
    let mut work = LoopCounts::default();
    let mut drive = Drive {
        config: &config,
        book: &book,
        resolver: assign.as_ref().map(|(resolver, _)| resolver.as_ref()),
        worker,
        actions: Vec::new(),
        doing: Vec::new(),
    };
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
    // A socket that could not be made (no file or port left) is tried again
    // after a pause, so that `start_errors` counts failures, not loops.
    let mut start_retry_at = Instant::now();
    let mut busy_ns = 0_u64;
    let mut last_read = Instant::now();
    let mut cpu_before = thread_cpu_ns();

    loop {
        let loop_start = Instant::now();
        let now = loop_start;
        if now >= next_flush {
            for listener in listeners.iter_mut().flatten() {
                collect_one(listener, &mut counters, Some((now, stranded_after)));
            }
            let cpu = thread_cpu_ns();
            let width = counters.len();
            let _ = tx.send(Report::Interval {
                worker,
                interval: interval_index,
                edges: std::mem::replace(&mut counters, vec![Counters::default(); width]),
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
            for listener in listeners.iter_mut().flatten() {
                listener.highest_at_end = listener.track.highest;
                // The session keeps its legs and waits for NACK answers.
                listener.session.hold = true;
                // Silent at the end: it had parts and was not delivered to
                // again.
                if listener
                    .last_part_at
                    .is_some_and(|at| now.saturating_duration_since(at) >= INTERRUPT)
                {
                    ensure(&mut counters, listener.home);
                    counters[listener.home].never_resumed += 1;
                }
            }
        }
        if draining && now >= drain_until {
            break;
        }
        if !draining {
            let wanted = share(shared.target.load(Ordering::Relaxed), worker, threads);
            budget = (budget + now.duration_since(budget_at).as_secs_f64() * rate).min(rate * 0.05 + 1.0);
            budget_at = now;
            while live < wanted && budget >= 1.0 && now >= start_retry_at {
                budget -= 1.0;
                let index = worker + joined * threads;
                // --edges alone: the edge rendezvous names first; with
                // entry points, "unassigned" until a leg is added.
                let home = match book.unassigned {
                    Some(unassigned) => unassigned,
                    None => {
                        let sid = format!("{}-{index}", config.sid_prefix);
                        let first = config
                            .edges
                            .iter()
                            .max_by_key(|addr| rendezvous_score(&sid, addr.to_string().as_str()))
                            .copied()
                            .unwrap_or(config.edges[index % config.edges.len()]);
                        book.index(first, &first.to_string())
                    }
                };
                let socket = match listener_socket(&config, index) {
                    Ok(socket) => socket,
                    Err(_) => {
                        ensure(&mut counters, home);
                        counters[home].start_errors += 1;
                        start_retry_at = now + START_RETRY;
                        break;
                    }
                };
                joined += 1;
                let slot = free.pop().unwrap_or_else(|| {
                    listeners.push(None);
                    listeners.len() - 1
                });
                let mut event = libc::epoll_event {
                    events: libc::EPOLLIN as u32,
                    u64: slot as u64,
                };
                unsafe {
                    libc::epoll_ctl(epoll, libc::EPOLL_CTL_ADD, socket.as_raw_fd(), &mut event);
                }
                ensure(&mut counters, home);
                counters[home].started += 1;
                next_id += 1;
                let listener = listeners[slot].insert(Listener::new(
                    &config, next_id, index, home, socket, now,
                ));
                live += 1;
                service(listener, slot, &mut counters, &mut drive, false, now);
                listener.scheduled = listener.wake(false);
                heap.push(Reverse((listener.scheduled, slot)));
            }
            while live > wanted {
                // The listener started last leaves first.
                let Some(slot) = listeners.iter().rposition(Option::is_some) else {
                    break;
                };
                let mut listener = listeners[slot].take().expect("occupied");
                free.push(slot);
                live -= 1;
                listener
                    .director
                    .leave(&mut listener.session, now, &mut drive.actions);
                drive.perform(&mut listener, slot, &mut counters, now);
                unsafe {
                    libc::epoll_ctl(
                        epoll,
                        libc::EPOLL_CTL_DEL,
                        listener.socket.as_raw_fd(),
                        std::ptr::null_mut(),
                    );
                }
                // What it received since the last interval counts too.
                collect_one(&mut listener, &mut counters, None);
                let home = listener.home;
                if let Some(highest) = listener.track.highest {
                    listener.track.settle(highest + 1, &mut counters[home]);
                }
                listener.track.book_left_lost(&mut counters);
                counters[home].stopped += 1;
            }
        }
        // The answers of the resolver (the HTTPS hedge, names).
        if let Some((_, answers)) = &assign {
            while let Ok(answer) = answers.try_recv() {
                if draining {
                    continue;
                }
                let slot = answer.slot;
                let Some(listener) =
                    occupied(&mut listeners, slot).filter(|listener| listener.id == answer.listener)
                else {
                    continue;
                };
                match answer.reply {
                    Reply::Https { id, body } => listener.director.on_https(
                        &mut listener.session,
                        id,
                        body.as_deref(),
                        now,
                        &mut drive.actions,
                    ),
                    Reply::Resolved { host, ip } => listener.director.on_resolved(
                        &mut listener.session,
                        &host,
                        ip,
                        now,
                        &mut drive.actions,
                    ),
                }
                drive.perform(listener, slot, &mut counters, now);
                let wake = listener.wake(false);
                if wake < listener.scheduled {
                    listener.scheduled = wake;
                    heap.push(Reverse((wake, slot)));
                }
            }
        }
        // The timers that are due: each listener's socket is read first.
        while let Some(&Reverse((at, slot))) = heap.peek() {
            if at > now {
                break;
            }
            heap.pop();
            let Some(listener) = occupied(&mut listeners, slot) else {
                continue;
            };
            if listener.scheduled != at {
                continue;
            }
            lateness.record(now.saturating_duration_since(at).as_micros() as u64);
            work.polls += 1;
            read_socket(listener, &mut bufs, &mut counters, &mut drive, &mut work, draining);
            let now = Instant::now();
            service(listener, slot, &mut counters, &mut drive, draining, now);
            listener.scheduled = listener
                .wake(draining)
                .max(now + Duration::from_micros(50));
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
            let Some(listener) = occupied(&mut listeners, slot) else {
                continue;
            };
            read_socket(listener, &mut bufs, &mut counters, &mut drive, &mut work, draining);
            // The verdicts after the batch: the director reads what the
            // datagrams told the session, and a datagram can bring a NACK
            // forward.
            let now = Instant::now();
            if !draining {
                listener
                    .director
                    .poll(&mut listener.session, now, &mut drive.actions);
                drive.perform(listener, slot, &mut counters, now);
            }
            let wake = listener.wake(draining);
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
    let end_now = Instant::now();
    for (slot, listener) in listeners.iter_mut().enumerate() {
        let Some(listener) = listener.as_mut() else {
            continue;
        };
        let home = listener.home;
        ensure(&mut counters, home);
        if let Some(highest) = listener.highest_at_end {
            listener.track.settle(highest + 1, &mut counters[home]);
        }
        listener.track.book_left_lost(&mut counters);
        if !listener.first_part {
            never_first += 1;
        }
        listener
            .director
            .leave(&mut listener.session, end_now, &mut drive.actions);
        drive.perform(listener, slot, &mut counters, end_now);
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
                "edges": listener.legs.iter().map(|leg| leg.node.clone()).collect::<Vec<_>>(),
                "parts": arrivals.len(),
                "file": file.map(|path| path.display().to_string()),
            }));
        }
        collect_one(listener, &mut counters, None);
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
#[derive(Debug, Clone, Default)]
struct HostSample {
    process_ticks: u64,
    host_busy: u64,
    host_total: u64,
    /// (busy, total) ticks of each processor: a receive queue's softirq
    /// can fill one while the host as a whole is not busy.
    cores: Vec<(u64, u64)>,
    udp_in_errors: u64,
    udp_rcvbuf_errors: u64,
    /// Datagrams dropped before any socket: by the NICs (`/proc/net/dev`
    /// receive drops, loopback aside) and by the kernel's receive backlog
    /// (`/proc/net/softnet_stat`). `UdpRcvbufErrors` does not count them.
    nic_rx_drops: u64,
    softnet_drops: u64,
}

/// The busiest processor's share of busy time between two samples.
fn core_busy_max(before: &HostSample, after: &HostSample) -> f64 {
    after
        .cores
        .iter()
        .zip(&before.cores)
        .filter_map(|((busy, total), (busy_before, total_before))| {
            let total = total.saturating_sub(*total_before);
            (total > 0).then(|| busy.saturating_sub(*busy_before) as f64 / total as f64)
        })
        .fold(0.0, f64::max)
}

/// The receive drops of `/proc/net/dev` (every interface but `lo`).
fn parse_net_dev_drops(text: &str) -> u64 {
    text.lines()
        .skip(2)
        .filter_map(|line| {
            let (name, data) = line.split_once(':')?;
            if name.trim() == "lo" {
                return None;
            }
            data.split_whitespace().nth(3)?.parse::<u64>().ok()
        })
        .sum()
}

/// The backlog drops of `/proc/net/softnet_stat` (hexadecimal, the second
/// column of each processor's line).
fn parse_softnet_drops(text: &str) -> u64 {
    text.lines()
        .filter_map(|line| u64::from_str_radix(line.split_whitespace().nth(1)?, 16).ok())
        .sum()
}

fn host_sample() -> HostSample {
    let mut sample = HostSample::default();
    if let Ok(dev) = std::fs::read_to_string("/proc/net/dev") {
        sample.nic_rx_drops = parse_net_dev_drops(&dev);
    }
    if let Ok(softnet) = std::fs::read_to_string("/proc/net/softnet_stat") {
        sample.softnet_drops = parse_softnet_drops(&softnet);
    }
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
        for line in stat.lines().take_while(|line| line.starts_with("cpu")) {
            let mut fields = line.split_whitespace();
            let name = fields.next().unwrap_or("");
            let values: Vec<u64> = fields.filter_map(|v| v.parse().ok()).collect();
            let total: u64 = values.iter().take(8).sum();
            let idle = values.get(3).copied().unwrap_or(0) + values.get(4).copied().unwrap_or(0);
            if name == "cpu" {
                sample.host_total = total;
                sample.host_busy = total.saturating_sub(idle);
            } else {
                sample.cores.push((total.saturating_sub(idle), total));
            }
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

/// Receive drops before the sockets (NIC and backlog) in an interval above
/// which the load host counts as saturated: a few are other traffic.
const NIC_DROPS_LIMIT: u64 = 10;
/// A processor this busy (a receive queue's softirq, a worker) saturates
/// the load host even when the host as a whole is not busy.
const CORE_BUSY_LIMIT: f64 = 0.95;

/// What the load host measured in an interval, for [`saturation`].
#[derive(Debug, Clone, Copy, Default)]
struct HostInterval {
    host_busy: f64,
    core_busy_max: f64,
    worker_busy_max: f64,
    lateness_p99_us: u64,
    recv_lag_p99_us: u64,
    rcvbuf_errors: u64,
    socket_drops: u64,
    nic_drops: u64,
}

/// Saturation of the load host in an interval, if any, and why: its CPU
/// or one processor of it, a worker that was busy nearly all the time,
/// timers or datagrams handled late (`lag_limit_us`: the limit plus the
/// --coalesce-us wait, which delays both on purpose), the kernel dropping
/// datagrams for full sockets, or before them (NIC, backlog).
fn saturation(host: &HostInterval, lag_limit_us: u64) -> Vec<&'static str> {
    let mut reasons = Vec::new();
    if host.host_busy > 0.9 {
        reasons.push("host-cpu");
    }
    if host.core_busy_max > CORE_BUSY_LIMIT {
        reasons.push("core-busy");
    }
    if host.worker_busy_max > 0.9 {
        reasons.push("worker-busy");
    }
    if host.lateness_p99_us > lag_limit_us {
        reasons.push("timers-late");
    }
    if host.recv_lag_p99_us > lag_limit_us {
        reasons.push("receive-lag");
    }
    if host.rcvbuf_errors > 0 || host.socket_drops > 0 {
        reasons.push("receive-drops");
    }
    if host.nic_drops > NIC_DROPS_LIMIT {
        reasons.push("nic-drops");
    }
    reasons
}

/// Run the listeners as `config` says; `on_interval` gets each interval's
/// line (also written to `intervals.jsonl` in the output directory). Returns
/// the summary (also written to `summary.json`).
pub fn run(config: LoadConfig, on_interval: &mut dyn FnMut(&Value)) -> std::io::Result<Value> {
    match (config.edges.is_empty(), config.director.entries.is_empty()) {
        (true, true) => return Err(std::io::Error::other("no edge and no entry point")),
        (false, false) => {
            return Err(std::io::Error::other(
                "edges or entry points, not both: --edges alone is the in-process tests' mode",
            ))
        }
        (true, false) if config.assign.is_none() => {
            return Err(std::io::Error::other("entry points need their HTTPS settings"))
        }
        _ => {}
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
    let entries = !config.director.entries.is_empty();
    let book = Arc::new(EdgeBook::new(&config.edges, entries));
    // The HTTPS hedge and name lookups: one thread, one answer channel a
    // worker.
    let (resolver, mut answer_rxs) = match config.assign.as_ref().filter(|_| entries) {
        Some(assign) => {
            let (resolver, rxs) = Resolver::start(assign.clone(), config.threads)
                .map_err(|error| std::io::Error::other(format!("{error:#}")))?;
            (Some(Arc::new(resolver)), rxs.into_iter().map(Some).collect())
        }
        None => (None, Vec::new()),
    };
    let (tx, rx) = mpsc::channel();
    let mut handles = Vec::new();
    for index in 0..config.threads {
        let (config, shared, tx, book) = (
            Arc::clone(&config),
            Arc::clone(&shared),
            tx.clone(),
            Arc::clone(&book),
        );
        let assign = match (&resolver, answer_rxs.get_mut(index).and_then(Option::take)) {
            (Some(resolver), Some(answers)) => Some((Arc::clone(resolver), answers)),
            _ => None,
        };
        handles.push(
            std::thread::Builder::new()
                .name(format!("{}{index}", config.thread_name))
                .spawn(move || worker(index, config, shared, tx, book, assign))?,
        );
    }
    drop(tx);
    let edge_json = |book: &EdgeBook, by_edge: &[Counters], seconds: f64, gauges: bool| {
        let mut per_edge = Map::new();
        for (edge, counters) in book.snapshot().iter().zip(by_edge) {
            let mut value = counters.to_json(seconds, gauges);
            value["node"] = json!(edge.node);
            per_edge.insert(edge.name(), value);
        }
        per_edge
    };
    let ticks = clock_ticks();
    let interval_s = config.interval.as_secs_f64();
    let mut host_before = host_sample();
    let mut host_at = Instant::now();
    let mut pending: BTreeMap<u64, Vec<Report>> = BTreeMap::new();
    let mut reported = vec![None::<u64>; config.threads];
    let mut total_by_edge: Vec<Counters> = Vec::new();
    // What the workers counted after their last interval (the drain, and
    // the parts settled at the end): the final line.
    let mut final_by_edge: Vec<Counters> = Vec::new();
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
    let mut nic_drops_total = 0_u64;
    let mut core_busy_max_run = 0.0_f64;
    // Timers and datagrams wait up to --coalesce-us on purpose.
    let lag_limit_us = (config.lag_limit + config.coalesce).as_micros() as u64;

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
                merge_edges(&mut total_by_edge, &edges);
                merge_edges(&mut final_by_edge, &edges);
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
            let mut by_edge: Vec<Counters> = Vec::new();
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
                    merge_edges(&mut by_edge, edges);
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
            let nic_drops = host.nic_rx_drops.saturating_sub(host_before.nic_rx_drops)
                + host.softnet_drops.saturating_sub(host_before.softnet_drops);
            let core_max = core_busy_max(&host_before, &host);
            host_before = host;
            host_at = Instant::now();
            rcvbuf_total += rcvbuf;
            in_errors_total += in_errors;
            nic_drops_total += nic_drops;
            core_busy_max_run = core_busy_max_run.max(core_max);
            let mut total = Counters::default();
            for counters in &by_edge {
                total.merge(counters);
            }
            merge_edges(&mut total_by_edge, &by_edge);
            peak_up = peak_up.max(total.up);
            peak_active = peak_active.max(total.active);
            cpu_cores.record((process_cores * 1_000.0) as u64);
            let busy_max = worker_busy.iter().copied().fold(0.0, f64::max);
            worker_busy_max_run = worker_busy_max_run.max(busy_max);
            host_busy_max_run = host_busy_max_run.max(host_busy);
            let reasons = saturation(
                &HostInterval {
                    host_busy,
                    core_busy_max: core_max,
                    worker_busy_max: busy_max,
                    lateness_p99_us: lateness.quantile(0.99).unwrap_or(0),
                    recv_lag_p99_us: total.recv_lag.quantile(0.99).unwrap_or(0),
                    rcvbuf_errors: rcvbuf,
                    socket_drops: total.socket_drops,
                    nic_drops,
                },
                lag_limit_us,
            );
            let t = (interval + 1) as f64 * interval_s;
            let per_edge = edge_json(&book, &by_edge, interval_s, true);
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
                    "core_busy_max": core_max,
                    "nic_rx_drops": nic_drops,
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
        for counters in &final_by_edge {
            last.merge(counters);
        }
        let per_edge = edge_json(&book, &final_by_edge, 0.0, false);
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
        let mut summary = json!({
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
            "compact_fallbacks": counters.compact_fallbacks,
            "short_header_dropped": counters.short_header_dropped,
            "latency_ms": counters.latency.summary_with_buckets(),
            "ttfp_ms": counters.ttfp.summary_with_buckets(),
            "ttok_ms": counters.ttok.summary_with_buckets(),
            "recv_lag_ms": counters.recv_lag.summary_with_buckets(),
        });
        counters.am10_json(&mut summary);
        summary
    };
    let mut per_edge = Map::new();
    let edges_now = book.snapshot();
    for (edge, counters) in edges_now.iter().zip(&total_by_edge) {
        let mut summary = edge_summary(counters);
        summary["node"] = json!(edge.node);
        per_edge.insert(edge.name(), summary);
    }
    let edge_names: Vec<String> = edges_now.iter().map(BookEdge::name).collect();
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
        "never_resumed": total.never_resumed,
        "assignment": entries.then(|| json!({
            "entries": config.director.entries,
            "udp_port": config.director.assignment_udp_port,
            "https_after_ms": config.director.assignment_https_after.as_millis() as u64,
            "timeout_ms": config.director.assignment_timeout.as_millis() as u64,
        })),
        "paths": config.director.paths,
        "spare": config.director.spare && entries,
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
            "core_busy_max": core_busy_max_run,
            "nic_rx_drops": nic_drops_total,
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
    fn the_parts_lost_in_a_move_are_counted_as_the_move_s() {
        let mut track = PartTrack::new();
        let mut counters = Counters::default();
        track.begin(0);
        // Before any move: a missing part is no moved listener's.
        for part in (0..10).filter(|part| *part != 3) {
            track.mark(part, false, 1_000, &mut counters);
        }
        // A leg is replaced after part 9, the listener interrupted until
        // 14 (10..13 never come), then it loses 20 later on its new edge.
        track.moved();
        track.mark(14, false, 1_000, &mut counters);
        track.interrupted(10, 14, 3);
        assert_eq!(track.windows.front(), Some(&(10, 14, 3)));
        for part in (15..30).filter(|part| *part != 20) {
            track.mark(part, false, 1_000, &mut counters);
        }
        track.settle(30, &mut counters);
        assert_eq!(counters.missing, 1 + 4 + 1);
        assert_eq!(counters.move_lost, 4, "10..13: the move's");
        assert_eq!(counters.missing_moved, 5, "every part lost after the move");
        assert!(track.windows.is_empty(), "settled moves are forgotten");
        // The move's parts are booked on the edge left (3), the later loss
        // is not.
        let mut book = vec![Counters::default(); 4];
        track.book_left_lost(&mut book);
        assert_eq!(book[3].left_lost, 4);
        assert_eq!(book.iter().map(|edge| edge.left_lost).sum::<u64>(), 4);
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
        assert_eq!(track.mark(10_000_000, false, 1_000, &mut counters), Mark::Late);
        assert_eq!(counters.resets, 0);
        assert_eq!(track.mark(10_000_001, false, 1_000, &mut counters), Mark::New);
        assert_eq!(counters.resets, 1);
        assert_eq!(counters.missing, 0);
        // The part that showed the jump is received, not missing.
        track.settle(10_000_002, &mut counters);
        assert_eq!(counters.missing, 0);
    }

    #[test]
    fn one_stray_part_far_behind_or_ahead_changes_nothing() {
        let mut track = PartTrack::new();
        let mut counters = Counters::default();
        track.begin(0);
        for part in 0..10_000_u64 {
            assert_eq!(track.mark(part, false, 1_500, &mut counters), Mark::New);
            if part == 8_000 {
                // A copy of a part 8,000 behind, and a misread number far
                // ahead: late, and the accounting goes on.
                assert_eq!(track.mark(10, false, 1_500, &mut counters), Mark::Late);
                assert_eq!(track.mark(1 << 40, false, 1_500, &mut counters), Mark::Late);
            }
        }
        track.settle(10_000, &mut counters);
        assert_eq!((counters.resets, counters.missing, counters.gaps), (0, 0, 0));
        // Two far parts that do not follow each other are strays too.
        assert_eq!(track.mark(5, false, 1_500, &mut counters), Mark::Late);
        assert_eq!(track.mark(4, false, 1_500, &mut counters), Mark::Late);
        assert_eq!(track.mark(10_000, false, 1_500, &mut counters), Mark::New);
        assert_eq!(counters.resets, 0);
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
        let calm = HostInterval {
            host_busy: 0.5,
            core_busy_max: 0.6,
            worker_busy_max: 0.5,
            lateness_p99_us: 100,
            recv_lag_p99_us: 100,
            nic_drops: 3,
            ..HostInterval::default()
        };
        assert!(saturation(&calm, 5_000).is_empty());
        let busy = HostInterval {
            host_busy: 0.95,
            core_busy_max: 0.99,
            worker_busy_max: 0.99,
            lateness_p99_us: 10_000,
            recv_lag_p99_us: 10_000,
            rcvbuf_errors: 1,
            socket_drops: 0,
            nic_drops: 500,
        };
        assert_eq!(
            saturation(&busy, 5_000),
            vec![
                "host-cpu",
                "core-busy",
                "worker-busy",
                "timers-late",
                "receive-lag",
                "receive-drops",
                "nic-drops"
            ]
        );
        // One processor full (a receive queue's softirq) while the host is
        // not: drops before the sockets follow, which no socket counts.
        let one_core = HostInterval {
            core_busy_max: 1.0,
            nic_drops: 40,
            ..calm
        };
        assert_eq!(saturation(&one_core, 5_000), vec!["core-busy", "nic-drops"]);
        // --coalesce-us 4000 delays timers and datagrams on purpose: the
        // limit is 5 ms past it.
        let coalesced = HostInterval {
            lateness_p99_us: 4_600,
            recv_lag_p99_us: 4_300,
            ..calm
        };
        assert!(saturation(&coalesced, 5_000 + 4_000).is_empty());
    }

    #[test]
    fn drops_before_the_sockets_and_the_busiest_processor() {
        let dev = "Inter-|   Receive                                                |  Transmit\n                    face |bytes    packets errs drop fifo frame compressed multicast|bytes\n                       lo: 100 10 0 7 0 0 0 0 100 10 0 0 0 0 0 0\n                     eth0: 5000 50 0 3 0 0 0 0 100 10 0 0 0 0 0 0\n                     eth1: 9000 90 0 11 0 0 0 0 100 10 0 0 0 0 0 0\n";
        assert_eq!(parse_net_dev_drops(dev), 14);
        let softnet = "0001e240 0000000a 00000003 00000000\n00000010 00000005 00000000 00000000\n";
        assert_eq!(parse_softnet_drops(softnet), 15);
        let before = HostSample {
            cores: vec![(10, 100), (50, 100)],
            ..HostSample::default()
        };
        let after = HostSample {
            cores: vec![(20, 200), (148, 200)],
            ..HostSample::default()
        };
        assert!((core_busy_max(&before, &after) - 0.98).abs() < 1e-9);
    }
}
