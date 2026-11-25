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
use crate::client_record::{OrderedOutput, ORDER_LIMITS};
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
    /// Fix 4 (F2): datagrams received and not taken, by reason (an old or
    /// held-off source generation, a part before the session's start; the
    /// duplicates are `duplicates`); generations taken again after they
    /// were refused; legs failed for refusing everything for a second.
    pub refused_stale_generation: u64,
    pub refused_before_start: u64,
    pub generation_returns: u64,
    pub refusing_legs: u64,
    /// Legs failed because no media came for a second while their edge
    /// said the stream went on (stuck4k).
    pub stalled_legs: u64,
    /// Fix 4 (F5): a listener's part numbers jumped ahead past its tracked
    /// window (an outage of the source, a wide hole): each jump once, and
    /// the parts it passed over, which are not counted as `missing`.
    pub jumps: u64,
    pub jump_parts: u64,
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
    /// Gauges: listeners with no part for `stranded_after` (5 s), and
    /// listeners with none for `--interrupt-ms` or more (B Y3: a move is a
    /// leg replacement now). At the end of the run (before the drain),
    /// `never_resumed`: listeners with no part for `--interrupt-ms` before
    /// the newest datagram their worker read.
    pub stranded: u64,
    pub resuming: u64,
    pub never_resumed: u64,
    /// Joins: of `never_resumed`, the listeners whose edges still sent them
    /// datagrams (`WAIT`s) within the interruption's length of the newest
    /// datagram: held, not dead, the subscription anchored at a part the
    /// stream does not send (an old session's number).
    pub anchored_stale: u64,
    /// Joins: each listener's time from the start of a session of the
    /// stream (its part 0's publication, from the first part's publication
    /// and number and the part interval) to its first part of that session,
    /// µs; one sample a session a listener.
    pub session_join: Histogram,
    /// Assignment (`/api/assignment`, Phase AM item 10): answers, their
    /// time (µs), failures (no entry point answered), and listeners placed
    /// by rendezvous over the known edges for want of an answer.
    pub assignments: u64,
    pub assignment_errors: u64,
    pub assignment_time: Histogram,
    pub fallbacks: u64,
    /// B Y4. Gauges: live legs on this edge, and listeners by the legs that
    /// delivered within max(20 ms, 8 part intervals) (two, one, none).
    pub legs: u64,
    pub paths_two: u64,
    pub paths_one: u64,
    pub paths_none: u64,
    /// The time between consecutive parts delivered first at one listener
    /// (kernel receive times, µs), and the longest silence still open at
    /// the end of the interval (a gauge, µs).
    pub part_gap: Histogram,
    pub silent_now_max: u64,
    /// Silences of `--interrupt-ms` or more (a silence counts once, open
    /// or ended), the listeners that had one in the interval, and the
    /// listener sessions that had their first (the run's distinct count).
    pub interruptions: u64,
    pub listeners_interrupted: u64,
    pub interrupted_sessions: u64,
    /// Listeners whose first missing part settled in the interval (each
    /// session once: their sum is the run's distinct count).
    pub listeners_with_missing: u64,
    /// Leg replacements of the legs that left this edge, by reason and by
    /// kind (B 2.8); `MOVE` to the old leg's release (µs); a leg's last
    /// media to its replacement's first (µs).
    pub leg_replacements_by_reason: BTreeMap<String, u64>,
    pub leg_replacements_by_kind: BTreeMap<&'static str, u64>,
    pub handover: Histogram,
    pub exposure: Histogram,
    /// The spans of a two-path listener on fewer than two delivering legs
    /// that ended in the interval (µs).
    pub single_path: Histogram,
    /// Hedged `SUB`s sent, and hedges whose silent leg came back.
    pub hedges: u64,
    pub hedges_released: u64,
    /// Listener time (on its home edge) and live-leg time (on each leg's
    /// edge), µs: the denominators of the datagrams and bytes per listener
    /// and per leg.
    pub listener_us: u64,
    pub leg_us: u64,
    /// The party: sessions that started as a listener's first (`joins`) or
    /// as a listener coming back (`rejoins`; `started` is both), sessions
    /// that left to come back (`leaves`; the ramp's are `stopped`), and
    /// each one's time to first audio (start to first part, µs).
    pub joins: u64,
    pub rejoins: u64,
    pub leaves: u64,
    pub join_first_audio: Histogram,
    pub rejoin_first_audio: Histogram,
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
        self.refused_stale_generation += other.refused_stale_generation;
        self.refused_before_start += other.refused_before_start;
        self.generation_returns += other.generation_returns;
        self.refusing_legs += other.refusing_legs;
        self.stalled_legs += other.stalled_legs;
        self.jumps += other.jumps;
        self.jump_parts += other.jump_parts;
        self.first_parts += other.first_parts;
        self.compact_fallbacks += other.compact_fallbacks;
        self.short_header_dropped += other.short_header_dropped;
        self.latency.merge(&other.latency);
        self.ttfp.merge(&other.ttfp);
        self.session_join.merge(&other.session_join);
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
        self.anchored_stale += other.anchored_stale;
        self.assignments += other.assignments;
        self.assignment_errors += other.assignment_errors;
        self.assignment_time.merge(&other.assignment_time);
        self.fallbacks += other.fallbacks;
        self.legs += other.legs;
        self.paths_two += other.paths_two;
        self.paths_one += other.paths_one;
        self.paths_none += other.paths_none;
        self.part_gap.merge(&other.part_gap);
        self.silent_now_max = self.silent_now_max.max(other.silent_now_max);
        self.interruptions += other.interruptions;
        self.listeners_interrupted += other.listeners_interrupted;
        self.interrupted_sessions += other.interrupted_sessions;
        self.listeners_with_missing += other.listeners_with_missing;
        for (reason, count) in &other.leg_replacements_by_reason {
            *self
                .leg_replacements_by_reason
                .entry(reason.clone())
                .or_default() += count;
        }
        for (kind, count) in &other.leg_replacements_by_kind {
            *self.leg_replacements_by_kind.entry(kind).or_default() += count;
        }
        self.handover.merge(&other.handover);
        self.exposure.merge(&other.exposure);
        self.single_path.merge(&other.single_path);
        self.hedges += other.hedges;
        self.hedges_released += other.hedges_released;
        self.listener_us += other.listener_us;
        self.leg_us += other.leg_us;
        self.joins += other.joins;
        self.rejoins += other.rejoins;
        self.leaves += other.leaves;
        self.join_first_audio.merge(&other.join_first_audio);
        self.rejoin_first_audio.merge(&other.rejoin_first_audio);
    }

    /// Missing over all the parts that were due (received, missing, gaps).
    pub fn missing_percent(&self) -> Option<f64> {
        let due = self.parts + self.missing + self.gaps;
        (due > 0).then(|| self.missing as f64 * 100.0 / due as f64)
    }

    /// One interval's (or the final line's) counters; `buckets`: the
    /// latency histogram with its buckets (`--interval-buckets`).
    pub fn to_json(&self, seconds: f64, gauges: bool, buckets: bool) -> Value {
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
            "refused": {
                "stale_generation": self.refused_stale_generation,
                "before_start": self.refused_before_start,
                "duplicate": self.duplicates,
            },
            "generation_returns": self.generation_returns,
            "refusing_legs": self.refusing_legs,
            "stalled_legs": self.stalled_legs,
            "jumps": self.jumps,
            "jump_parts": self.jump_parts,
            "first_parts": self.first_parts,
            "compact_fallbacks": self.compact_fallbacks,
            "short_header_dropped": self.short_header_dropped,
            "latency_ms": if buckets {
                self.latency.summary_with_buckets()
            } else {
                self.latency.summary_ms()
            },
            "ttfp_ms": self.ttfp.summary_ms(),
            "session_join_ms": self.session_join.summary_ms(),
            "ttok_ms": self.ttok.summary_ms(),
            "recv_lag_ms": self.recv_lag.summary_ms(),
        });
        self.am10_json(&mut value);
        self.y4_json(&mut value, false);
        value["listeners_interrupted"] = json!(self.listeners_interrupted);
        if gauges {
            value["active"] = json!(self.active);
            value["up"] = json!(self.up);
            value["stranded"] = json!(self.stranded);
            value["resuming"] = json!(self.resuming);
            value["legs"] = json!(self.legs);
            value["paths"] = json!({
                "two": self.paths_two,
                "one": self.paths_one,
                "none": self.paths_none,
            });
            value["silent_now_max_ms"] = json!(self.silent_now_max as f64 / 1_000.0);
        }
        value
    }

    /// The B Y4 counters that an interval and the summary share, into
    /// `value` (`summary`: over the run).
    fn y4_json(&self, value: &mut Value, summary: bool) {
        let ms = |us: u64| us as f64 / 1_000.0;
        let per_second = |count: u64, us: u64| (us > 0).then(|| count as f64 * 1e6 / us as f64);
        value["part_gap_ms"] = self.part_gap.summary_with_buckets();
        value["longest_silence_ms"] = json!((self.part_gap.count() > 0).then(|| ms(self.part_gap.max())));
        value["interruptions"] = json!(self.interruptions);
        value["listeners_with_missing"] = json!(self.listeners_with_missing);
        if summary {
            value["listeners_interrupted"] = json!(self.interrupted_sessions);
        }
        value["leg_replacements"] = json!({
            "by_reason": self.leg_replacements_by_reason,
            "by_kind": self.leg_replacements_by_kind,
        });
        value["handover_ms"] = self.handover.summary_with_buckets();
        value["exposure_ms"] = self.exposure.summary_with_buckets();
        value["single_path_ms"] = self.single_path.summary_with_buckets();
        value["hedges"] = json!(self.hedges);
        value["hedges_released"] = json!(self.hedges_released);
        value["listener_seconds"] = json!(self.listener_us as f64 / 1e6);
        value["leg_seconds"] = json!(self.leg_us as f64 / 1e6);
        value["datagrams_per_leg_s"] = json!(per_second(self.datagrams, self.leg_us));
        value["wire_bytes_per_leg_s"] = json!(per_second(self.bytes_in, self.leg_us));
        value["joins"] = json!(self.joins);
        value["rejoins"] = json!(self.rejoins);
        value["leaves"] = json!(self.leaves);
        value["join_first_audio_ms"] = self.join_first_audio.summary_with_buckets();
        value["rejoin_first_audio_ms"] = self.rejoin_first_audio.summary_with_buckets();
    }

    /// Datagrams and wire bytes a listener and second, into a total's
    /// `value` only: an edge's datagrams are booked where they were sent and
    /// its listener time where its listeners are homed, so their ratio on
    /// one edge means nothing (the per-leg rates are right on every edge).
    fn listener_rates_json(&self, value: &mut Value) {
        let per_second = |count: u64| {
            (self.listener_us > 0).then(|| count as f64 * 1e6 / self.listener_us as f64)
        };
        value["datagrams_per_listener_s"] = json!(per_second(self.datagrams));
        value["wire_bytes_per_listener_s"] = json!(per_second(self.bytes_in));
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
        value["anchored_stale"] = json!(self.anchored_stale);
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
    /// The listener's missing parts (its first counts it in
    /// `listeners_with_missing`).
    missing: u64,
}

/// Interruptions whose parts a listener keeps track of.
const MOVE_WINDOWS: usize = 8;

/// A sampled listener's arrivals file (`--sample`), written as its parts
/// come, in part order (`crate::client_record::OrderedOutput`): the parts
/// were kept to the end of the run before (udp-client memory round: 1 ms
/// parts for an hour were 3.6 million entries a listener). It is written
/// beside its name (`.partial`) and named at the end of the run; a listener
/// that leaves before the end writes no file, as before.
struct ListenerArrivals {
    output: OrderedOutput<std::io::BufWriter<std::fs::File>, std::io::Sink>,
    /// The file's name, and its name while it is written.
    path: Option<PathBuf>,
    partial: Option<PathBuf>,
    /// Parts taken (each once).
    parts: u64,
    failed: bool,
}

impl ListenerArrivals {
    fn open(config: &LoadConfig, index: usize, session_no: u32) -> Self {
        let path = config.out_dir.as_ref().map(|dir| {
            dir.join("arrivals").join(match session_no {
                0 => format!("listener-{index:05}.csv"),
                n => format!("listener-{index:05}.{n}.csv"),
            })
        });
        let partial = path
            .as_ref()
            .map(|path| path.with_extension("csv.partial"));
        let file = partial.as_ref().and_then(|partial| {
            let mut file =
                std::io::BufWriter::with_capacity(16 << 10, std::fs::File::create(partial).ok()?);
            file.write_all(b"part,arrival_unix_us,bytes,published_unix_us,available_unix_us\n")
                .ok()?;
            Some(file)
        });
        let failed = partial.is_some() && file.is_none();
        Self {
            output: OrderedOutput::new(file, None, ORDER_LIMITS),
            path,
            partial,
            parts: 0,
            failed,
        }
    }

    fn push(
        &mut self,
        part: u64,
        arrival: u64,
        bytes: usize,
        published: u64,
        available: u64,
        now: Instant,
    ) {
        self.parts += 1;
        if self.failed || self.partial.is_none() {
            return;
        }
        let line = format!("{part},{arrival},{bytes},{published},{available}\n");
        if self.output.push(part, Some(line), None, now).is_err() {
            self.failed = true;
        }
    }

    /// The end of the run: the file is written and named; its name (as
    /// before, also when it could not be written).
    fn finish(&mut self) -> Option<PathBuf> {
        if let Some(partial) = self.partial.take() {
            if !self.failed && self.output.finish().is_ok() {
                if let Some(path) = &self.path {
                    let _ = std::fs::rename(&partial, path);
                }
            } else {
                let _ = std::fs::remove_file(&partial);
            }
        }
        self.path.clone()
    }
}

impl Drop for ListenerArrivals {
    fn drop(&mut self) {
        // A listener that left before the end: no file.
        if let Some(partial) = self.partial.take() {
            let _ = std::fs::remove_file(partial);
        }
    }
}

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
            missing: 0,
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
                    if self.missing == 0 {
                        counters.listeners_with_missing += 1;
                    }
                    self.missing += 1;
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
        // A contributor restart counts its parts again; a jump past the
        // tracked window ahead is one too (an outage of the source, a
        // listener that was away): start again there, once a second far
        // part confirms it. A single far part is a stray. The parts that a
        // jump ahead passed over are one gap of that length (`jumps`,
        // `jump_parts`), not that many missing parts: what was missing
        // before it is settled first (fix 4, F5: two number lines of a
        // stream 40,000 apart settled 36,000 parts as missing at each of
        // 1.8 million switches, 3.3e10 in all).
        let ahead_from = self.highest.map_or(self.next_settle, |highest| highest + 1);
        if part + TRACK < self.next_settle || part >= ahead_from.max(self.next_settle) + TRACK {
            match self.jump {
                Some((first, first_gap)) if part > first && part - first <= JUMP_CONFIRM => {
                    counters.resets += 1;
                    let settled = self.highest.map_or(self.next_settle, |highest| highest + 1);
                    self.settle(settled, counters);
                    if first > settled {
                        counters.jumps += 1;
                        counters.jump_parts += first - settled;
                    }
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
/// `--settle-ms`: just past the 3 s give-up of a hole (B 2.4).
pub const DEFAULT_SETTLE_MS: u64 = 3_500;
/// `--interrupt-ms`.
pub const DEFAULT_INTERRUPT_MS: u64 = 30;

/// The latency histograms of an interval line that carry their buckets, so
/// that a window of intervals (of several load hosts) merges into a p99.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntervalBuckets {
    /// The total's (the default).
    Total,
    /// The total's and each edge's.
    Edges,
    Off,
}

impl std::str::FromStr for IntervalBuckets {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        match text {
            "total" => Ok(Self::Total),
            "edges" => Ok(Self::Edges),
            "off" => Ok(Self::Off),
            other => Err(format!("{other:?}: total, edges or off")),
        }
    }
}

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
    /// A part is counted missing once it is this far behind its listener's
    /// newest part (`--settle-ms`, past the 3 s give-up of a hole), in parts
    /// of the stream's interval once a listener has measured it ...
    pub settle: Duration,
    /// ... or this many parts (`--settle-parts`, which overrides it).
    pub settle_parts: Option<u64>,
    /// A silence between two parts delivered first of this length or more
    /// is an interruption (`--interrupt-ms`).
    pub interrupt: Duration,
    /// Which latency histograms of an interval line carry their buckets
    /// (`--interval-buckets`).
    pub interval_buckets: IntervalBuckets,
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
    /// The listening party: listeners that leave and come back as new
    /// sessions (`--churn-per-min`, `--churn-absence-ms`).
    pub churn: Option<Churn>,
    /// Joins in bursts (`--wave-size`, `--wave-every-ms`).
    pub waves: Option<Waves>,
    /// Seed of the churn's draws (`--seed`): two hosts with different seeds
    /// do not leave and come back together.
    pub seed: u64,
    /// One listener in this many writes each part it receives to
    /// `<out>/raw/worker-<w>.csv` (`--raw-sample`; 0: none).
    pub raw_sample: usize,
}

/// Listeners that leave and come back.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Churn {
    /// The share of the listeners present that leave in a minute (0.05:
    /// five in a hundred), at random times and in random order.
    pub per_minute: f64,
    /// Each comes back as a new session (a new session id, a join of its
    /// own) after an absence drawn evenly from this range.
    pub absent_min: Duration,
    pub absent_max: Duration,
}

/// A target reached in bursts: up to `size` more listeners every `every`,
/// from the start of the run (each started at `--start-rate`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Waves {
    pub size: usize,
    pub every: Duration,
}

/// The listeners wanted after a wave (`due`) or between two: up to `size`
/// more than `last` at a wave, never more than the ramp's `target`, and at
/// once fewer when the ramp falls.
fn wave_target(waves: &Waves, last: usize, target: usize, due: bool) -> usize {
    if target <= last {
        target
    } else if due {
        (last + waves.size).min(target)
    } else {
        last
    }
}

/// splitmix64: the churn's draws.
#[derive(Debug, Clone)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Evenly in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1_u64 << 53) as f64
    }

    /// Evenly in [low, high].
    fn between(&mut self, low: Duration, high: Duration) -> Duration {
        low + (high.saturating_sub(low)).mul_f64(self.unit())
    }

    /// The wait to the next event of a Poisson process of `rate` a second.
    fn exponential(&mut self, rate: f64) -> Duration {
        Duration::from_secs_f64((-(1.0 - self.unit()).ln() / rate).min(3_600.0))
    }
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
            settle: Duration::from_millis(DEFAULT_SETTLE_MS),
            settle_parts: None,
            interrupt: Duration::from_millis(DEFAULT_INTERRUPT_MS),
            interval_buckets: IntervalBuckets::Total,
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
            churn: None,
            waves: None,
            seed: 0,
            raw_sample: 0,
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

/// `MIN-MAX` milliseconds (or one value), for `--churn-absence-ms`.
pub fn parse_range_ms(text: &str) -> Result<(Duration, Duration), String> {
    let (low, high) = text.split_once('-').unwrap_or((text, text));
    let parse = |value: &str| {
        value
            .trim()
            .replace('_', "")
            .parse::<u64>()
            .map_err(|_| format!("bad milliseconds in {text:?}"))
    };
    let (low, high) = (parse(low)?, parse(high)?);
    if low > high {
        return Err(format!("{text:?}: the low end is above the high one"));
    }
    Ok((Duration::from_millis(low), Duration::from_millis(high)))
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
    hedges: u64,
    hedges_released: u64,
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
    /// Its newest datagram of any kind (kernel time, µs), and the receiver's
    /// flows (source sessions) when it last took a part (joins).
    last_datagram_us: u64,
    flows_seen: u64,
    scheduled: Instant,
    seen: Seen,
    overflows: u32,
    highest_at_end: Option<u64>,
    arrivals: Option<ListenerArrivals>,
    started_unix_us: u64,
    /// A part settles this many parts behind the newest: `--settle-parts`,
    /// or (`learn_settle`) `--settle-ms` in parts of the shortest part
    /// interval measured so far over windows of 256 parts (publication
    /// times of the window's first part, `interval_from`, and of a part 256
    /// later), once [`SETTLE_WINDOWS`] windows gave one; until then, as far
    /// as the track reaches. The shortest, so that a pause upstream within a
    /// window (it lengthens the window's interval) never shortens the settle.
    settle_parts: u64,
    learn_settle: bool,
    interval_from: Option<(u64, u64)>,
    interval_best_us: Option<f64>,
    interval_windows: u32,
    /// The open silence was counted as an interruption; one came in this
    /// interval; one ever came.
    silence_counted: bool,
    interrupted_now: bool,
    ever_interrupted: bool,
    /// Its longest silence (µs) and when it ended (Unix µs).
    longest_silence: (u64, u64),
    /// Time on one path (B 2.8): it has had two delivering legs, until when
    /// two legs delivered (the second newest leg media + the window, µs),
    /// the start of the open span on fewer, and the spans ended so far (µs,
    /// for `worst_listeners`).
    two_paths_seen: bool,
    two_until_us: u64,
    single_since_us: Option<u64>,
    single_path_us: u64,
    /// When its time was last booked (listener and leg seconds).
    booked_at: Instant,
    /// Its sessions before this one (it left and came back that often),
    /// and whether it writes each part to the raw sample.
    session_no: u32,
    raw: bool,
}

/// A leg delivers while it brought media within max(20 ms, 8 part
/// intervals) (B 2.8, as the session judges it).
const DELIVERING_FLOOR: Duration = Duration::from_millis(20);
const DELIVERING_PARTS: u32 = 8;
/// Parts between the two whose publication times give a listener the
/// stream's part interval (for `--settle-ms`).
const INTERVAL_PARTS: u64 = 256;
/// Windows of [`INTERVAL_PARTS`] a listener measures before its settle
/// follows `--settle-ms`.
const SETTLE_WINDOWS: u32 = 2;

impl Listener {
    /// max(20 ms, 8 part intervals), µs.
    fn window_us(&self) -> u64 {
        let interval = self
            .session
            .receiver
            .compact
            .part_interval()
            .unwrap_or_default();
        duration_us(DELIVERING_FLOOR.max(interval * DELIVERING_PARTS))
    }

    /// Its legs that delivered within the window of `now_us`.
    fn delivering(&self, now_us: u64) -> usize {
        let window = self.window_us();
        self.legs
            .iter()
            .filter(|leg| leg.last_media_us > 0 && now_us.saturating_sub(leg.last_media_us) <= window)
            .count()
    }

    /// Time on one path (B 2.8): a two-path listener's spans in which fewer
    /// than two legs brought media within the window, booked as they end.
    fn track_paths(&mut self, home: &mut Counters, now_us: u64) {
        if self.director.want() < 2 {
            self.end_single_path(home, now_us);
            return;
        }
        let window = self.window_us();
        let (mut newest, mut second) = (0, 0);
        for leg in &self.legs {
            if leg.last_media_us > newest {
                second = newest;
                newest = leg.last_media_us;
            } else if leg.last_media_us > second {
                second = leg.last_media_us;
            }
        }
        if second > 0 && now_us.saturating_sub(second) <= window {
            self.two_paths_seen = true;
            self.two_until_us = second + window;
            // It ended with the datagram that made the two again.
            if let Some(since) = self.single_since_us.take() {
                self.book_single_path(home, second.saturating_sub(since));
            }
        } else if self.two_paths_seen && self.single_since_us.is_none() {
            self.single_since_us = Some(self.two_until_us.min(now_us));
        }
    }

    fn end_single_path(&mut self, home: &mut Counters, now_us: u64) {
        if let Some(since) = self.single_since_us.take() {
            self.book_single_path(home, now_us.saturating_sub(since));
        }
    }

    fn book_single_path(&mut self, home: &mut Counters, span_us: u64) {
        home.single_path.record(span_us);
        self.single_path_us += span_us;
    }

    /// A silence of the interruption's length or more (counted once).
    fn interrupted(&mut self, home: &mut Counters) {
        home.interruptions += 1;
        self.interrupted_now = true;
        if !self.ever_interrupted {
            self.ever_interrupted = true;
            home.interrupted_sessions += 1;
        }
    }

    /// The parts behind the newest at which its parts settle, from the
    /// shortest part interval of its windows of 256 parts (publication
    /// times), once [`SETTLE_WINDOWS`] windows have given one.
    fn learn_interval(&mut self, settle: Duration, part: u64, published_us: u64) {
        if !self.learn_settle || published_us == 0 {
            return;
        }
        let Some((first, first_us)) = self.interval_from else {
            self.interval_from = Some((part, published_us));
            return;
        };
        // A part from before the window (a restart numbers from 0 again)
        // starts the window again.
        if part < first {
            self.interval_from = Some((part, published_us));
            return;
        }
        if part < first + INTERVAL_PARTS {
            return;
        }
        if published_us > first_us {
            let interval_us = (published_us - first_us) as f64 / (part - first) as f64;
            let best = self
                .interval_best_us
                .map_or(interval_us, |best| best.min(interval_us));
            self.interval_best_us = Some(best);
            self.interval_windows += 1;
            if self.interval_windows >= SETTLE_WINDOWS {
                self.settle_parts =
                    ((settle.as_micros() as f64 / best).ceil() as u64).clamp(1, TRACK - 1);
            }
        }
        self.interval_from = Some((part, published_us));
    }

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
    /// its home; and that leg. (The session takes a datagram by the same
    /// rule and ignores one from an edge it let go, so media is booked on
    /// the leg the session gave it to.)
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
    /// The worker's raw sample (`--raw-sample`).
    raw: Option<std::io::BufWriter<std::fs::File>>,
    /// The newest kernel receive time of a media datagram the worker read
    /// (µs): the clock that a silence at the end of the run is judged by,
    /// so that a worker that fell behind silences none of its listeners.
    newest_us: u64,
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
            let from = self
                .book
                .index_of_node(&replacement.from)
                .unwrap_or(listener.home);
            ensure(counters, from);
            let left = &mut counters[from];
            *left
                .leg_replacements_by_reason
                .entry(replacement.reason.clone())
                .or_default() += 1;
            *left
                .leg_replacements_by_kind
                .entry(replacement.kind.name())
                .or_default() += 1;
            if let Some(handover) = replacement.handover {
                left.handover.record(duration_us(handover));
            }
            if let Some(exposure) = replacement.exposure {
                left.exposure.record(duration_us(exposure));
            }
            if replacement.kind == ReplacementKind::Takeback {
                continue;
            }
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
        home.hedges += stats.hedges.saturating_sub(seen.hedges);
        home.hedges_released += stats.hedges_released.saturating_sub(seen.hedges_released);
        seen.hedges = stats.hedges;
        seen.hedges_released = stats.hedges_released;
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
        worst: Worst,
        /// Listener sessions in the raw sample, and its file.
        raw_sessions: u64,
        raw_file: Option<PathBuf>,
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

/// One datagram of a listener (a silence of `--interrupt-ms` or more
/// between two parts delivered first is an interruption, and the parts lost
/// in it are the move's: `move_lost`, booked on the edge that delivered the
/// part before it, `left_lost`). The director takes its own (`ASSIGNED`, a
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
    // Every datagram the worker read, a WAIT too (joins: listeners held on
    // WAITs and nothing else made the newest datagram their last part, and
    // `never_resumed` never counted them).
    drive.newest_us = drive.newest_us.max(arrival_us);
    listener.last_datagram_us = listener.last_datagram_us.max(arrival_us);
    // The load host's own lag: the time from the kernel's receive timestamp
    // to now, which neither the latency nor the time to first part holds.
    let lag_us = datagram
        .kernel_us
        .map_or(0, |kernel_us| now_us.saturating_sub(kernel_us));
    let duplicates = listener.session.receiver.duplicate_parts;
    let refused = {
        let stats = &listener.session.receiver.compact.stats;
        (stats.stale_generation, stats.before_start, stats.generation_returns)
    };
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
    {
        let stats = &listener.session.receiver.compact.stats;
        let sender = &mut counters[edge];
        sender.refused_stale_generation += stats.stale_generation.saturating_sub(refused.0);
        sender.refused_before_start += stats.before_start.saturating_sub(refused.1);
        sender.generation_returns += stats.generation_returns.saturating_sub(refused.2);
    }
    counters[edge].duplicates += listener
        .session
        .receiver
        .duplicate_parts
        .saturating_sub(duplicates);
    let interrupt_us = duration_us(drive.config.interrupt);
    while let Some(object) = listener.session.next_object() {
        let settle_parts = listener.settle_parts;
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
                    // The first part of a session (the first, or the first
                    // after the receiver took a new one): how long after
                    // the session's start (joins).
                    let flows = listener.session.receiver.flows;
                    if (listener.last_first.is_none() || flows != listener.flows_seen)
                        && object.published_unix_us > 0
                    {
                        let since_start_us = listener
                            .interval_best_us
                            .map_or(0, |interval| (object.part as f64 * interval) as u64);
                        if since_start_us < 60_000_000 {
                            counters[home].session_join.record(
                                arrival_us
                                    .saturating_sub(object.published_unix_us)
                                    .saturating_add(since_start_us),
                            );
                        }
                    }
                    listener.flows_seen = flows;
                    if object.published_unix_us > 0 {
                        counters[edge]
                            .latency
                            .record(arrival_us.saturating_sub(object.published_unix_us));
                    }
                    listener.learn_interval(
                        drive.config.settle,
                        object.part,
                        object.published_unix_us,
                    );
                    // The gap since the part delivered first before it.
                    if let Some((part, at_us, left)) = listener.last_first {
                        let gap = arrival_us.saturating_sub(at_us);
                        counters[home].part_gap.record(gap);
                        if gap >= interrupt_us {
                            listener.track.interrupted(part + 1, object.part, left);
                            if !listener.silence_counted {
                                listener.interrupted(&mut counters[home]);
                            }
                        }
                        if gap > listener.longest_silence.0 {
                            listener.longest_silence = (gap, arrival_us);
                        }
                    }
                    listener.silence_counted = false;
                    listener.last_first = Some((object.part, arrival_us, edge));
                    listener.last_part_at = Some(now);
                    if !listener.first_part {
                        listener.first_part = true;
                        let first = &mut counters[home];
                        first.first_parts += 1;
                        let audio = duration_us(now.saturating_duration_since(listener.started))
                            .saturating_sub(lag_us);
                        first.ttfp.record(audio);
                        if listener.session_no == 0 {
                            first.join_first_audio.record(audio);
                        } else {
                            first.rejoin_first_audio.record(audio);
                        }
                    }
                    if listener.raw {
                        if let Some(raw) = &mut drive.raw {
                            let node = leg.map_or("", |leg| listener.legs[leg].node.as_str());
                            let _ = writeln!(
                                raw,
                                "{},{},{},{},{},{},{}",
                                listener.sid,
                                object.part,
                                arrival_us,
                                object.payload.len(),
                                object.published_unix_us,
                                object.available_unix_us,
                                node
                            );
                        }
                    }
                    if let Some(arrivals) = &mut listener.arrivals {
                        arrivals.push(
                            object.part,
                            arrival_us,
                            object.payload.len(),
                            object.published_unix_us,
                            object.available_unix_us,
                            now,
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
        let refusing = listener.session.refusing_legs;
        let stalled = listener.session.stalled_legs;
        listener.session.poll_legs(now, &mut |to, bytes| {
            let _ = socket.send_to(bytes, to);
        });
        if listener.session.refusing_legs > refusing || listener.session.stalled_legs > stalled {
            ensure(counters, listener.home);
            counters[listener.home].refusing_legs += listener.session.refusing_legs - refusing;
            counters[listener.home].stalled_legs += listener.session.stalled_legs - stalled;
        }
    }
    if !draining {
        listener
            .director
            .poll(&mut listener.session, now, &mut drive.actions);
    }
    drive.perform(listener, slot, counters, now);
}

/// A listener's counters since the last interval into its home edge's (its
/// legs' time into their edges'), with the gauges (`active`, `up`,
/// `stranded`, `resuming`, `legs`, `paths`, `silent_now_max`) when `gauges`
/// (not for one that leaves). An open silence of `--interrupt-ms` is an
/// interruption already.
fn collect_one(
    listener: &mut Listener,
    counters: &mut Vec<Counters>,
    config: &LoadConfig,
    now: Instant,
    now_us: u64,
    gauges: bool,
) {
    let home = listener.home;
    ensure(counters, home);
    let elapsed = duration_us(now.saturating_duration_since(listener.booked_at));
    listener.booked_at = now;
    counters[home].listener_us += elapsed;
    for leg in listener.legs.iter().filter(|leg| leg.live) {
        ensure(counters, leg.edge);
        counters[leg.edge].leg_us += elapsed;
        if gauges {
            counters[leg.edge].legs += 1;
        }
    }
    if let Some((_, at_us, _)) = listener.last_first {
        let open = now_us.saturating_sub(at_us);
        if gauges {
            counters[home].silent_now_max = counters[home].silent_now_max.max(open);
        }
        if open >= duration_us(config.interrupt) && !listener.silence_counted {
            listener.silence_counted = true;
            listener.interrupted(&mut counters[home]);
        }
    }
    listener.track_paths(&mut counters[home], now_us);
    let edge = &mut counters[home];
    if std::mem::take(&mut listener.interrupted_now) {
        edge.listeners_interrupted += 1;
    }
    if gauges {
        edge.active += 1;
        if listener.session.is_subscribed() {
            edge.up += 1;
        }
        let quiet_since = listener.last_part_at.unwrap_or(listener.started);
        if now.saturating_duration_since(quiet_since) >= config.stranded_after {
            edge.stranded += 1;
        }
        if listener.first_part && now.saturating_duration_since(quiet_since) >= config.interrupt {
            edge.resuming += 1;
        }
        match listener.delivering(now_us) {
            0 => edge.paths_none += 1,
            1 => edge.paths_one += 1,
            _ => edge.paths_two += 1,
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

/// One of the listeners a run reports as its worst (B 2.9), with what it
/// held when it ended: its legs (node, live, how long since its edge last
/// sent it media), its spare and the legs it wanted, so that a listener
/// left on one path names the leg that did not deliver.
#[derive(Debug, Clone)]
struct WorstListener {
    sid: String,
    longest_silence_us: u64,
    silence_end_unix_us: u64,
    missing: u64,
    single_path_us: u64,
    legs: Vec<(String, bool, Option<u64>)>,
    spare: Option<String>,
    want: u8,
    started_unix_us: u64,
    ended_unix_us: u64,
}

impl WorstListener {
    fn to_json(&self) -> Value {
        json!({
            "sid": self.sid,
            "longest_silence_ms": self.longest_silence_us as f64 / 1_000.0,
            "silence_end_unix_ms": (self.silence_end_unix_us > 0).then_some(self.silence_end_unix_us / 1_000),
            "missing": self.missing,
            "single_path_ms": self.single_path_us as f64 / 1_000.0,
            "legs": self.legs.iter().map(|(node, live, ago_us)| json!({
                "node": node,
                "live": live,
                "last_media_ago_ms": ago_us.map(|us| us as f64 / 1_000.0),
            })).collect::<Vec<_>>(),
            "spare": self.spare,
            "want": self.want,
            "started_unix_ms": self.started_unix_us / 1_000,
            "ended_unix_ms": self.ended_unix_us / 1_000,
        })
    }
}

/// Listeners kept by longest silence, by missing parts and by time on one
/// path.
const WORST: usize = 20;

#[derive(Debug, Default)]
struct Worst {
    by_silence: Vec<WorstListener>,
    by_missing: Vec<WorstListener>,
    by_single_path: Vec<WorstListener>,
}

impl Worst {
    fn silence(entry: &WorstListener) -> u64 {
        entry.longest_silence_us
    }

    fn missing(entry: &WorstListener) -> u64 {
        entry.missing
    }

    fn single_path(entry: &WorstListener) -> u64 {
        entry.single_path_us
    }

    /// Keep `entry` in `list` if it is among the worst by `key` so far.
    fn keep(list: &mut Vec<WorstListener>, entry: &WorstListener, key: fn(&WorstListener) -> u64) {
        if key(entry) == 0 || (list.len() == WORST && key(&list[WORST - 1]) >= key(entry)) {
            return;
        }
        let at = list.partition_point(|other| key(other) >= key(entry));
        list.insert(at, entry.clone());
        list.truncate(WORST);
    }

    /// A listener session that ended (each session is offered once).
    fn offer(&mut self, entry: WorstListener) {
        Self::keep(&mut self.by_silence, &entry, Self::silence);
        Self::keep(&mut self.by_missing, &entry, Self::missing);
        Self::keep(&mut self.by_single_path, &entry, Self::single_path);
    }

    /// Another worker's lists: each list from its own, so that a session in
    /// several of the other's lists is still once in each.
    fn merge(&mut self, other: Worst) {
        for entry in &other.by_silence {
            Self::keep(&mut self.by_silence, entry, Self::silence);
        }
        for entry in &other.by_missing {
            Self::keep(&mut self.by_missing, entry, Self::missing);
        }
        for entry in &other.by_single_path {
            Self::keep(&mut self.by_single_path, entry, Self::single_path);
        }
    }

    fn to_json(&self) -> Value {
        let list = |list: &[WorstListener]| list.iter().map(WorstListener::to_json).collect::<Vec<_>>();
        json!({
            "by_longest_silence": list(&self.by_silence),
            "by_missing": list(&self.by_missing),
            "by_single_path": list(&self.by_single_path),
        })
    }
}

/// The end of a listener (it leaves, or the run ends): its open single-path
/// span and silence are booked, its counters collected, and it is offered
/// to the worst listeners.
fn finish_listener(
    listener: &mut Listener,
    counters: &mut Vec<Counters>,
    config: &LoadConfig,
    worst: &mut Worst,
    now: Instant,
) {
    let now_us = now_unix_us();
    collect_one(listener, counters, config, now, now_us, false);
    ensure(counters, listener.home);
    listener.end_single_path(&mut counters[listener.home], now_us);
    if let Some((_, at_us, _)) = listener.last_first {
        let open = now_us.saturating_sub(at_us);
        if open > listener.longest_silence.0 {
            listener.longest_silence = (open, now_us);
        }
    }
    let (silence, ended) = listener.longest_silence;
    if silence > 0 || listener.track.missing > 0 || listener.single_path_us > 0 {
        worst.offer(WorstListener {
            sid: listener.sid.clone(),
            longest_silence_us: silence,
            silence_end_unix_us: ended,
            missing: listener.track.missing,
            single_path_us: listener.single_path_us,
            legs: listener
                .legs
                .iter()
                .map(|leg| {
                    let ago = (leg.last_media_us > 0).then(|| now_us.saturating_sub(leg.last_media_us));
                    (leg.node.clone(), leg.live, ago)
                })
                .collect(),
            spare: listener.director.spare().map(str::to_owned),
            want: listener.director.want(),
            started_unix_us: listener.started_unix_us,
            ended_unix_us: now_us,
        });
    }
}

impl Listener {
    /// Its session number in the worker and its settings: a new listener,
    /// its socket made and its first request not yet asked.
    #[allow(clippy::too_many_arguments)]
    fn new(
        config: &LoadConfig,
        id: u64,
        index: usize,
        session_no: u32,
        home: usize,
        socket: UdpSocket,
        now: Instant,
    ) -> Self {
        let sid = session_id(config, index, session_no);
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
            last_datagram_us: 0,
            flows_seen: 0,
            scheduled: now,
            seen: Seen::default(),
            overflows: 0,
            highest_at_end: None,
            arrivals: (index < config.sample)
                .then(|| ListenerArrivals::open(config, index, session_no)),
            started_unix_us: now_unix_us(),
            settle_parts: config.settle_parts.unwrap_or(TRACK - 1),
            learn_settle: config.settle_parts.is_none(),
            interval_from: None,
            interval_best_us: None,
            interval_windows: 0,
            silence_counted: false,
            interrupted_now: false,
            ever_interrupted: false,
            longest_silence: (0, 0),
            two_paths_seen: false,
            two_until_us: 0,
            single_since_us: None,
            single_path_us: 0,
            booked_at: now,
            session_no,
            raw: config.raw_sample > 0 && index % config.raw_sample == 0,
        }
    }
}

/// The session id of listener `index`'s session `session_no` (it left and
/// came back that often): `<prefix>-<index>`, then `<prefix>-<index>.<n>`.
fn session_id(config: &LoadConfig, index: usize, session_no: u32) -> String {
    match session_no {
        0 => format!("{}-{index}", config.sid_prefix),
        n => format!("{}-{index}.{n}", config.sid_prefix),
    }
}

/// Take the listener out of `slot` (its socket out of the epoll set).
fn remove_listener(
    listeners: &mut [Option<Listener>],
    free: &mut Vec<usize>,
    slot: usize,
    epoll: i32,
) -> Listener {
    let listener = listeners[slot].take().expect("an occupied slot");
    free.push(slot);
    unsafe {
        libc::epoll_ctl(
            epoll,
            libc::EPOLL_CTL_DEL,
            listener.socket.as_raw_fd(),
            std::ptr::null_mut(),
        );
    }
    listener
}

/// A listener leaves before the end of the run (the ramp, the churn): what
/// it received since the last interval is counted, the parts it had not
/// settled were not due yet and are not judged, and its legs are released.
/// Returns its home edge.
fn end_listener(
    mut listener: Listener,
    slot: usize,
    counters: &mut Vec<Counters>,
    drive: &mut Drive<'_>,
    worst: &mut Worst,
    now: Instant,
) -> usize {
    finish_listener(&mut listener, counters, drive.config, worst, now);
    listener
        .director
        .leave(&mut listener.session, now, &mut drive.actions);
    drive.perform(&mut listener, slot, counters, now);
    listener.track.book_left_lost(counters);
    listener.home
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
    let raw_path = config
        .out_dir
        .as_ref()
        .filter(|_| config.raw_sample > 0)
        .map(|dir| dir.join("raw").join(format!("worker-{worker}.csv")));
    let mut drive = Drive {
        config: &config,
        book: &book,
        resolver: assign.as_ref().map(|(resolver, _)| resolver.as_ref()),
        worker,
        actions: Vec::new(),
        doing: Vec::new(),
        raw: raw_path.as_ref().and_then(|path| {
            let mut raw = std::io::BufWriter::with_capacity(1 << 16, std::fs::File::create(path).ok()?);
            writeln!(raw, "sid,part,arrival_unix_us,bytes,published_unix_us,available_unix_us,edge").ok()?;
            Some(raw)
        }),
        newest_us: 0,
    };
    // The churn: listeners away until when (listener, its next session),
    // the draws (the run's seed and the worker), and the leaves expected
    // since the last (on the clock of the listeners present) against the
    // next leave's draw.
    let mut absent: BinaryHeap<Reverse<(Instant, usize, u32)>> = BinaryHeap::new();
    let mut rng = Rng(config.seed ^ (worker as u64 + 1).wrapping_mul(0xa076_1d64_78bd_642f));
    let (mut churn_at, mut churn_due) = (Instant::now(), 0.0_f64);
    let mut churn_next = rng.exponential(1.0).as_secs_f64();
    let mut raw_sessions = 0_u64;
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
    let mut worst = Worst::default();

    loop {
        let loop_start = Instant::now();
        let now = loop_start;
        if now >= next_flush {
            let now_us = now_unix_us();
            for listener in listeners.iter_mut().flatten() {
                collect_one(listener, &mut counters, &config, now, now_us, true);
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
            // What waits at every socket first (the director is left alone
            // from now on): the silences are judged after.
            for listener in listeners.iter_mut().flatten() {
                read_socket(listener, &mut bufs, &mut counters, &mut drive, &mut work, true);
            }
            let interrupt_us = duration_us(config.interrupt);
            for listener in listeners.iter_mut().flatten() {
                listener.highest_at_end = listener.track.highest;
                // The session keeps its legs and waits for NACK answers.
                listener.session.hold = true;
                // Silent at the end: it had parts, and none came for the
                // interruption's length before the newest datagram that
                // its worker read.
                if listener
                    .last_first
                    .is_some_and(|(_, at_us, _)| drive.newest_us.saturating_sub(at_us) >= interrupt_us)
                {
                    ensure(&mut counters, listener.home);
                    counters[listener.home].never_resumed += 1;
                    if drive.newest_us.saturating_sub(listener.last_datagram_us) < interrupt_us {
                        counters[listener.home].anchored_stale += 1;
                    }
                }
            }
        }
        if draining && now >= drain_until {
            break;
        }
        if !draining {
            // The listeners away (churn) count as present: they come back.
            let wanted = share(shared.target.load(Ordering::Relaxed), worker, threads);
            budget = (budget + now.duration_since(budget_at).as_secs_f64() * rate).min(rate * 0.05 + 1.0);
            budget_at = now;
            loop {
                if budget < 1.0 || now < start_retry_at {
                    break;
                }
                // A listener back from its absence, else a new one.
                let (index, session_no) = match absent.peek() {
                    Some(Reverse((at, index, session_no))) if *at <= now => (*index, *session_no),
                    _ if live + absent.len() < wanted => (worker + joined * threads, 0),
                    _ => break,
                };
                budget -= 1.0;
                // --edges alone: the edge rendezvous names first; with
                // entry points, "unassigned" until a leg is added.
                let home = match book.unassigned {
                    Some(unassigned) => unassigned,
                    None => {
                        let sid = session_id(&config, index, session_no);
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
                if session_no == 0 {
                    joined += 1;
                } else {
                    absent.pop();
                }
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
                if session_no == 0 {
                    counters[home].joins += 1;
                } else {
                    counters[home].rejoins += 1;
                }
                next_id += 1;
                let listener = listeners[slot].insert(Listener::new(
                    &config, next_id, index, session_no, home, socket, now,
                ));
                raw_sessions += u64::from(listener.raw && drive.raw.is_some());
                live += 1;
                service(listener, slot, &mut counters, &mut drive, false, now);
                listener.scheduled = listener.wake(false);
                heap.push(Reverse((listener.scheduled, slot)));
            }
            // The ramp falls: the listeners away first (they do not come
            // back), then the one started last.
            while live + absent.len() > wanted {
                if absent.pop().is_some() {
                    continue;
                }
                let Some(slot) = listeners.iter().rposition(Option::is_some) else {
                    break;
                };
                let listener = remove_listener(&mut listeners, &mut free, slot, epoll);
                live -= 1;
                let home = end_listener(listener, slot, &mut counters, &mut drive, &mut worst, now);
                counters[home].stopped += 1;
            }
            // The churn: a listener leaves at random times (a Poisson process
            // of the churn's rate times the listeners present, run on the
            // clock of the leaves expected so far, so that a crowd that
            // grows leaves faster) and comes back after its absence as a
            // new session.
            if let Some(churn) = config.churn.filter(|churn| churn.per_minute > 0.0) {
                let elapsed = now.saturating_duration_since(churn_at).as_secs_f64();
                churn_at = now;
                churn_due += churn.per_minute / 60.0 * live as f64 * elapsed;
                while churn_due >= churn_next && live > 0 {
                    churn_due -= churn_next;
                    churn_next = rng.exponential(1.0).as_secs_f64();
                    let mut slot = rng.next() as usize % listeners.len();
                    while listeners[slot].is_none() {
                        slot = (slot + 1) % listeners.len();
                    }
                    let listener = remove_listener(&mut listeners, &mut free, slot, epoll);
                    live -= 1;
                    let (index, session_no) = (listener.index, listener.session_no);
                    let home = end_listener(listener, slot, &mut counters, &mut drive, &mut worst, now);
                    counters[home].leaves += 1;
                    let back = now + rng.between(churn.absent_min, churn.absent_max);
                    absent.push(Reverse((back, index, session_no + 1)));
                }
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
            listener.track_paths(&mut counters[listener.home], now_unix_us());
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
            listener.track_paths(&mut counters[listener.home], now_unix_us());
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
        finish_listener(listener, &mut counters, &config, &mut worst, end_now);
        if let Some(arrivals) = &mut listener.arrivals {
            let file = arrivals.finish();
            let parts = arrivals.parts;
            let legs = &listener.legs;
            samples.push(json!({
                "listener": listener.index,
                // v1: its home edge; v2: every leg it held at the end.
                "edge": legs.iter().find(|leg| leg.edge == listener.home).map(|leg| leg.node.clone()),
                "edges": legs.iter().map(|leg| leg.node.clone()).collect::<Vec<_>>(),
                "parts": parts,
                "file": file.map(|path| path.display().to_string()),
            }));
        }
        listener
            .director
            .leave(&mut listener.session, end_now, &mut drive.actions);
        drive.perform(listener, slot, &mut counters, end_now);
    }
    unsafe {
        libc::close(epoll);
    }
    if let Some(raw) = &mut drive.raw {
        let _ = raw.flush();
    }
    let _ = tx.send(Report::Done {
        worker,
        edges: counters,
        never_first,
        samples,
        worst,
        raw_sessions,
        raw_file: raw_path.filter(|_| drive.raw.is_some()),
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

/// An interval line's `total` and `edges` (`gauges` for the run's lines,
/// not the final one): the latency buckets where `buckets` says.
fn interval_json(
    book: &[BookEdge],
    by_edge: &[Counters],
    total: &Counters,
    seconds: f64,
    gauges: bool,
    buckets: IntervalBuckets,
) -> (Value, Map<String, Value>) {
    let mut per_edge = Map::new();
    for (edge, counters) in book.iter().zip(by_edge) {
        let mut value = counters.to_json(seconds, gauges, buckets == IntervalBuckets::Edges);
        value["node"] = json!(edge.node);
        per_edge.insert(edge.name(), value);
    }
    let mut total_json = total.to_json(seconds, gauges, buckets != IntervalBuckets::Off);
    total.listener_rates_json(&mut total_json);
    (total_json, per_edge)
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
    if config.raw_sample > 0 && config.out_dir.is_none() {
        return Err(std::io::Error::other(
            "a raw sample is written under the output directory: --raw-sample needs --out",
        ));
    }
    let file_limit = raise_file_limit();
    let mut intervals_out = match &config.out_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir.join("arrivals"))?;
            if config.raw_sample > 0 {
                std::fs::create_dir_all(dir.join("raw"))?;
            }
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
    let buckets = config.interval_buckets;
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
    let mut worst = Worst::default();
    let mut raw_sessions = 0_u64;
    let mut raw_files: Vec<String> = Vec::new();
    // Join waves: the listeners wanted after the last wave, and the next.
    let mut waved = 0_usize;
    let mut next_wave = shared.t0;
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
        let target = config.target_at(elapsed);
        let target = match &config.waves {
            Some(waves) => {
                let due = Instant::now() >= next_wave;
                if due {
                    next_wave += waves.every.max(Duration::from_millis(1));
                }
                waved = wave_target(waves, waved, target, due);
                waved
            }
            None => target,
        };
        shared.target.store(target, Ordering::Relaxed);
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
                worst: theirs,
                raw_sessions: sampled,
                raw_file,
            }) => {
                worst.merge(theirs);
                raw_sessions += sampled;
                raw_files.extend(raw_file.map(|path| path.display().to_string()));
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
            let (total_json, per_edge) =
                interval_json(&book.snapshot(), &by_edge, &total, interval_s, true, buckets);
            let rss_mb = rss_mb();
            rss_mb_max = rss_mb_max.max(rss_mb);
            let line = json!({
                "schema": "needletail.load.interval.v2",
                "label": config.label,
                "phase": "run",
                "t": t,
                "load_host_saturated": !reasons.is_empty(),
                "interval": interval,
                "unix_ms": started_unix_us / 1_000 + (t * 1_000.0) as u64,
                "target": config.target_at(Duration::from_secs_f64(t)),
                "wave_target": config.waves.is_some().then(|| shared.target.load(Ordering::Relaxed)),
                "total": total_json,
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
        let (total_json, per_edge) =
            interval_json(&book.snapshot(), &final_by_edge, &last, 0.0, false, buckets);
        let line = json!({
            "schema": "needletail.load.interval.v2",
            "label": config.label,
            "phase": "final",
            "t": shared.t0.elapsed().as_secs_f64(),
            "load_host_saturated": false,
            "total": total_json,
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
            "refused": {
                "stale_generation": counters.refused_stale_generation,
                "before_start": counters.refused_before_start,
                "duplicate": counters.duplicates,
            },
            "generation_returns": counters.generation_returns,
            "refusing_legs": counters.refusing_legs,
            "stalled_legs": counters.stalled_legs,
            "jumps": counters.jumps,
            "jump_parts": counters.jump_parts,
            "listeners_with_parts": counters.first_parts,
            "compact_fallbacks": counters.compact_fallbacks,
            "short_header_dropped": counters.short_header_dropped,
            "latency_ms": counters.latency.summary_with_buckets(),
            "ttfp_ms": counters.ttfp.summary_with_buckets(),
            "session_join_ms": counters.session_join.summary_with_buckets(),
            "ttok_ms": counters.ttok.summary_with_buckets(),
            "recv_lag_ms": counters.recv_lag.summary_with_buckets(),
        });
        counters.am10_json(&mut summary);
        counters.y4_json(&mut summary, true);
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
    let mut total_summary = edge_summary(&total);
    total.listener_rates_json(&mut total_summary);
    let summary = json!({
        "schema": "needletail.load.v2",
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
        "anchored_stale": total.anchored_stale,
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
        "total": total_summary,
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
    let mut summary = summary;
    let fields = [
        ("listeners_interrupted", json!(total.interrupted_sessions)),
        ("listeners_with_missing", json!(total.listeners_with_missing)),
        (
            "longest_silence_ms",
            json!(worst
                .by_silence
                .first()
                .map(|listener| listener.longest_silence_us)
                .max((total.part_gap.count() > 0).then(|| total.part_gap.max()))
                .map(|us| us as f64 / 1_000.0)),
        ),
        (
            "bytes_per_listener_second",
            json!((total.listener_us > 0)
                .then(|| total.bytes_in as f64 * 1e6 / total.listener_us as f64)),
        ),
        ("worst_listeners", worst.to_json()),
        ("interrupt_ms", json!(config.interrupt.as_millis() as u64)),
        (
            "settle",
            match config.settle_parts {
                Some(parts) => json!({"parts": parts}),
                None => json!({"ms": config.settle.as_millis() as u64}),
            },
        ),
        (
            "churn",
            json!(config.churn.map(|churn| json!({
                "per_minute": churn.per_minute,
                "absent_min_ms": churn.absent_min.as_millis() as u64,
                "absent_max_ms": churn.absent_max.as_millis() as u64,
            }))),
        ),
        (
            "waves",
            json!(config.waves.map(|waves| json!({
                "size": waves.size,
                "every_ms": waves.every.as_millis() as u64,
            }))),
        ),
        ("seed", json!(config.seed)),
        ("joins", json!(total.joins)),
        ("rejoins", json!(total.rejoins)),
        ("leaves", json!(total.leaves)),
        (
            "raw_sample",
            json!((config.raw_sample > 0).then(|| json!({
                "one_in": config.raw_sample,
                "listener_sessions": raw_sessions,
                "files": raw_files,
            }))),
        ),
    ];
    for (name, value) in fields {
        summary[name] = value;
    }
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

    /// Fix 4, F5: a jump of part numbers past the tracked window (an
    /// outage of the source: 5b's 4,315 parts; a misread number line 2^31
    /// ahead) is one gap of its length, not that many missing parts; the
    /// holes before it still count, and the parts after it are tracked.
    #[test]
    fn a_jump_past_the_window_is_one_gap_of_its_length() {
        for jump in [5_000_u64, 1 << 31] {
            let mut track = PartTrack::new();
            let mut counters = Counters::default();
            track.begin(0);
            for part in (0..2_000_u64).filter(|part| *part != 1_990) {
                assert_eq!(track.mark(part, false, 1_500, &mut counters), Mark::New);
            }
            let resume = 2_000 + jump;
            // The first part after the jump waits for a second to confirm it.
            assert_eq!(track.mark(resume, false, 1_500, &mut counters), Mark::Late);
            for part in resume + 1..resume + 3_000 {
                assert_eq!(track.mark(part, false, 1_500, &mut counters), Mark::New, "{jump}");
            }
            track.settle(resume + 3_000, &mut counters);
            assert_eq!(counters.jumps, 1, "{jump}");
            assert_eq!(counters.jump_parts, jump, "{jump}");
            assert_eq!(counters.missing, 1, "{jump}: only the hole before the jump");
            assert_eq!(counters.listeners_with_missing, 1);
        }
        // Two number lines of one stream 40,000 apart, alternating (lp2's
        // starved senders): each switch is a jump or a restart, never
        // 36,000 missing parts.
        let mut track = PartTrack::new();
        let mut counters = Counters::default();
        track.begin(0);
        let mut low = 0_u64;
        for round in 0..50_u64 {
            for _ in 0..10 {
                track.mark(low, false, 1_500, &mut counters);
                low += 1;
            }
            for offset in 0..10 {
                track.mark(40_000 + round * 10 + offset, false, 1_500, &mut counters);
            }
        }
        assert!(counters.missing < 1_000, "{}", counters.missing);
        assert!(counters.jumps >= 49, "{}", counters.jumps);
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

    // ------------------------------------------------------- B Y4 accounting

    const STREAM: u64 = 4001;

    /// One listener of `--edges` alone on two edges, its two legs added by
    /// its director, fed datagram by datagram with the kernel receive times
    /// given (milliseconds from the start), as a worker feeds it.
    struct Harness {
        config: LoadConfig,
        book: EdgeBook,
        listener: Listener,
        counters: Vec<Counters>,
        edges: [SocketAddr; 2],
        t0: Instant,
        base_us: u64,
    }

    impl Harness {
        fn new(paths: u8) -> Self {
            let edges: [SocketAddr; 2] = ["127.0.0.1:41001".parse().unwrap(), "127.0.0.1:41002".parse().unwrap()];
            let mut config = LoadConfig::new(SessionConfig::new(STREAM), edges.to_vec(), 1);
            config.director.paths = paths;
            let book = EdgeBook::new(&edges, false);
            let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
            socket.set_nonblocking(true).unwrap();
            let t0 = Instant::now();
            let listener = Listener::new(&config, 1, 0, 0, 0, socket, t0);
            let mut harness = Self {
                config,
                book,
                listener,
                counters: vec![Counters::default(); 2],
                edges,
                t0,
                base_us: now_unix_us(),
            };
            harness.service(0.0);
            assert_eq!(harness.listener.legs.len(), paths as usize, "the director added its legs");
            // Each edge: TOKEN, then OK from part 100 with the compact lane.
            for from in 0..paths as usize {
                let token = Control::Token {
                    stream_id: STREAM,
                    token: 7 + from as u64,
                }
                .encode();
                harness.datagram(from, &token, 0.1);
                let ok = Control::Subscribed {
                    stream_id: STREAM,
                    next_part: 100,
                    compact: true,
                    waiting: false,
                }
                .encode();
                harness.datagram(from, &ok, 0.2);
            }
            harness
        }

        fn at(&self, ms: f64) -> (Instant, u64) {
            let us = (ms * 1_000.0) as u64;
            (self.t0 + Duration::from_micros(us), self.base_us + us)
        }

        fn drive<'a>(config: &'a LoadConfig, book: &'a EdgeBook) -> Drive<'a> {
            Drive {
                config,
                book,
                resolver: None,
                worker: 0,
                actions: Vec::new(),
                doing: Vec::new(),
                raw: None,
                newest_us: 0,
            }
        }

        fn service(&mut self, ms: f64) {
            let (now, _) = self.at(ms);
            let mut drive = Self::drive(&self.config, &self.book);
            service(&mut self.listener, 0, &mut self.counters, &mut drive, false, now);
        }

        /// A datagram from edge `from` at `ms`, then the director, and the
        /// time on one path, as after a worker's batch.
        fn datagram(&mut self, from: usize, bytes: &[u8], ms: f64) {
            let (now, now_us) = self.at(ms);
            let datagram = Datagram {
                data: bytes,
                peer: Some(self.edges[from]),
                kernel_us: Some(now_us),
                overflows: None,
                truncated: false,
            };
            let mut drive = Self::drive(&self.config, &self.book);
            handle_datagram(&mut self.listener, &mut self.counters, &mut drive, &datagram, false, now, now_us);
            self.listener
                .director
                .poll(&mut self.listener.session, now, &mut drive.actions);
            drive.perform(&mut self.listener, 0, &mut self.counters, now);
            let home = self.listener.home;
            self.listener.track_paths(&mut self.counters[home], now_us);
        }

        fn part(&mut self, from: usize, part: u64, ms: f64) {
            let (_, now_us) = self.at(ms);
            let datagram = crate::client_compact::encode_part(part, now_us - 300, &[part as u8; 56]).unwrap();
            self.datagram(from, &datagram, ms);
        }

        fn collect(&mut self, ms: f64) -> Counters {
            let (now, now_us) = self.at(ms);
            let mut counters = vec![Counters::default(); 2];
            collect_one(&mut self.listener, &mut counters, &self.config, now, now_us, true);
            let mut total = Counters::default();
            for edge in &counters {
                total.merge(edge);
            }
            total
        }

        fn total(&self) -> Counters {
            let mut total = Counters::default();
            for edge in &self.counters {
                total.merge(edge);
            }
            total
        }
    }

    use crate::client_udp::Control;

    #[test]
    fn the_gap_histogram_takes_first_arrivals_only() {
        let mut h = Harness::new(2);
        // Twenty parts a millisecond apart, each from edge 0 and 0.3 ms
        // later from edge 1.
        for k in 0..20_u64 {
            let ms = 10.0 + k as f64;
            h.part(0, 100 + k, ms);
            h.part(1, 100 + k, ms + 0.3);
        }
        let total = h.total();
        assert_eq!(total.parts, 20);
        assert_eq!(total.part_gap.count(), 19, "one gap between two first arrivals");
        let p50 = total.part_gap.quantile(0.5).unwrap() as f64;
        assert!((p50 - 1_000.0).abs() < 20.0, "{p50}");
        assert!(total.part_gap.max() < 1_100);
        // Booked by the edge that sent each datagram.
        assert_eq!(h.counters[0].parts, 20);
        assert_eq!(h.counters[1].parts, 0);
        assert_eq!(h.counters[1].duplicates, 20);
        assert_eq!(h.counters[0].datagrams, 20);
        assert_eq!(h.counters[1].datagrams, 20);
        // Edge 1 first this time: its arrival is the one measured.
        h.part(1, 120, 30.5);
        h.part(0, 120, 31.0);
        let total = h.total();
        assert_eq!(total.part_gap.count(), 20);
        assert_eq!(h.counters[1].parts, 1);
        assert_eq!(total.part_gap.max(), 1_500);
        assert_eq!(total.interruptions, 0);
    }

    #[test]
    fn a_silence_interrupts_a_listener_once() {
        let mut h = Harness::new(2);
        for k in 0..10_u64 {
            h.part(0, 100 + k, 10.0 + k as f64);
            h.part(1, 100 + k, 10.2 + k as f64);
        }
        // Silent since 19 ms: at 79 ms the silence is 60 ms and open.
        let first = h.collect(79.0);
        assert_eq!(first.interruptions, 1);
        assert_eq!(first.listeners_interrupted, 1);
        assert_eq!(first.interrupted_sessions, 1);
        assert_eq!(first.silent_now_max, 60_000);
        // Still silent in the next interval: nothing more.
        let second = h.collect(99.0);
        assert_eq!(second.interruptions, 0);
        assert_eq!(second.listeners_interrupted, 0);
        // The silence ends at 119 ms: a 100 ms gap, the same interruption.
        h.part(0, 110, 119.0);
        h.part(1, 110, 119.2);
        let total = h.total();
        assert_eq!(total.part_gap.max(), 100_000);
        assert_eq!(total.interruptions, 0, "counted when it was open");
        let third = h.collect(120.0);
        assert_eq!((third.interruptions, third.listeners_interrupted), (0, 0));
        // A second silence, 40 ms, seen only once it ended: one more
        // interruption, the same listener session. 111 and 112 never come.
        h.part(0, 113, 159.0);
        let fourth = h.collect(160.0);
        let total = h.total();
        assert_eq!(total.interruptions, 1);
        assert_eq!(total.interrupted_sessions, 0, "the session was counted at its first");
        assert_eq!(fourth.listeners_interrupted, 1);
        // A gap under the threshold is no interruption.
        h.part(0, 114, 185.0);
        assert_eq!(h.total().interruptions, 1);
        // The parts of an interruption that never come are the move's,
        // booked on the edge that delivered the part before it.
        assert_eq!(h.listener.track.windows.back(), Some(&(111, 113, 0)));
    }

    #[test]
    fn interval_lines_carry_latency_buckets() {
        let book = vec![
            BookEdge {
                addr: Some("127.0.0.1:1".parse().unwrap()),
                node: Some("a".into()),
            },
            BookEdge {
                addr: Some("127.0.0.1:2".parse().unwrap()),
                node: Some("b".into()),
            },
        ];
        let mut by_edge = vec![Counters::default(), Counters::default()];
        for value in 1..=1_000_u64 {
            by_edge[0].latency.record(500 + value);
            by_edge[1].latency.record(900 + 3 * value);
        }
        let mut total = Counters::default();
        for edge in &by_edge {
            total.merge(edge);
        }
        let has = |value: &Value| value["latency_ms"]["buckets_us"].is_array();
        let (line, edges) = interval_json(&book, &by_edge, &total, 1.0, true, IntervalBuckets::Total);
        assert!(has(&line));
        assert!(!edges.values().any(has));
        let (line, edges) = interval_json(&book, &by_edge, &total, 1.0, true, IntervalBuckets::Edges);
        assert!(has(&line) && edges.values().all(has));
        let (line, edges) = interval_json(&book, &by_edge, &total, 1.0, true, IntervalBuckets::Off);
        assert!(!has(&line) && !edges.values().any(has));
        // Two hosts' lines (here each edge's) merge into the window's p99:
        // add the counts of equal lowest values, take the rank.
        let (_, edges) = interval_json(&book, &by_edge, &total, 1.0, true, IntervalBuckets::Edges);
        let mut merged: BTreeMap<u64, u64> = BTreeMap::new();
        for edge in edges.values() {
            for pair in edge["latency_ms"]["buckets_us"].as_array().unwrap() {
                *merged.entry(pair[0].as_u64().unwrap()).or_default() += pair[1].as_u64().unwrap();
            }
        }
        let count: u64 = merged.values().sum();
        assert_eq!(count, 2_000);
        let rank = (0.99 * count as f64).ceil() as u64;
        let mut seen = 0;
        let p99_low = merged
            .iter()
            .find(|(_, n)| {
                seen += **n;
                seen >= rank
            })
            .map(|(low, _)| *low)
            .unwrap();
        let p99 = total.latency.quantile(0.99).unwrap();
        assert!((p99_low as f64 - p99 as f64).abs() / p99 as f64 <= 0.02, "{p99_low} {p99}");
        // The other Y4 figures of a line: gap and single-path histograms
        // with buckets, and the per-listener and per-leg rates.
        let (line, edges) = interval_json(&book, &by_edge, &total, 1.0, true, IntervalBuckets::Off);
        for name in ["part_gap_ms", "handover_ms", "exposure_ms", "single_path_ms"] {
            assert!(line[name]["buckets_us"].is_array(), "{name}");
        }
        // Per listener only in the total: an edge's datagrams and its
        // listeners' time are booked on different edges.
        for name in ["datagrams_per_listener_s", "wire_bytes_per_listener_s"] {
            assert!(edges.values().all(|edge| edge.get(name).is_none()), "{name}");
        }
        for name in ["paths", "legs", "silent_now_max_ms", "listeners_interrupted", "listeners_with_missing", "datagrams_per_leg_s", "wire_bytes_per_listener_s"] {
            assert!(line.get(name).is_some(), "{name}");
        }
    }

    #[test]
    fn time_on_one_path_is_a_span_per_listener() {
        let mut h = Harness::new(2);
        let mut part = 100;
        let mut both = |h: &mut Harness, from: u64, to: u64, legs: &[usize]| {
            for ms in from..to {
                for leg in legs {
                    h.part(*leg, part, ms as f64 + 0.1 * *leg as f64);
                }
                part += 1;
            }
        };
        both(&mut h, 10, 60, &[0, 1]);
        // Edge 1 silent from its part at 59 ms: one path from 79 ms (its
        // window, 20 ms, ran out) until its part at 160 ms.
        both(&mut h, 60, 160, &[0]);
        assert_eq!(h.total().single_path.count(), 0, "open");
        both(&mut h, 160, 200, &[0, 1]);
        // Edge 0 silent from 199 ms: one path from 219 ms until 260 ms.
        both(&mut h, 200, 260, &[1]);
        both(&mut h, 260, 270, &[0, 1]);
        let spans = &h.total().single_path;
        assert_eq!(spans.count(), 2, "a span each time");
        let (low, high) = (spans.quantile(0.0).unwrap(), spans.max());
        assert!((40_000..=42_500).contains(&low), "{low}");
        assert!((80_000..=82_500).contains(&high), "{high}");
        // The gauge: two delivering legs now.
        let line = h.collect(270.0);
        assert_eq!((line.paths_two, line.paths_one, line.paths_none), (1, 0, 0));
        assert_eq!(line.legs, 2);
        assert!(line.listener_us > 0 && line.leg_us >= line.listener_us * 2 - 2);
        // A one-path listener books no time on one path.
        let mut one = Harness::new(1);
        for k in 0..100_u64 {
            one.part(0, 100 + k, 10.0 + k as f64);
        }
        assert_eq!(one.total().single_path.count(), 0);
        let line = one.collect(110.0);
        assert_eq!((line.paths_two, line.paths_one), (0, 1));
    }

    /// B Y3: with entry points a listener asks for its legs as the client
    /// does: `ASSIGN` (padded) from its own socket to the entry's UDP port,
    /// the `ASSIGNED` read back by its director (never its session), the
    /// legs' first `SUB`s and the spare's token request from that socket,
    /// no HTTPS hedge once UDP answered, and the spare's `TOKEN` taken by
    /// the director.
    #[test]
    fn a_listener_asks_for_its_legs_over_udp_from_its_own_socket() {
        use crate::client_udp::{AssignedCache, ASSIGN_REQUEST_BYTES};
        let wait = Some(Duration::from_secs(2));
        let entry = UdpSocket::bind("127.0.0.1:0").unwrap();
        entry.set_read_timeout(wait).unwrap();
        // Three edges at one port (an answer names one port) on three
        // addresses: two legs and the spare.
        let first = UdpSocket::bind("127.0.0.2:0").unwrap();
        let port = first.local_addr().unwrap().port();
        let mut edges = vec![first];
        for host in ["127.0.0.3", "127.0.0.4"] {
            edges.push(UdpSocket::bind((host, port)).unwrap());
        }
        for edge in &edges {
            edge.set_read_timeout(wait).unwrap();
        }
        let mut config = LoadConfig::new(SessionConfig::new(STREAM), Vec::new(), 1);
        config.director.entries = vec!["https://127.0.0.1:19444".into()];
        config.director.assignment_udp_port = entry.local_addr().unwrap().port();
        let book = EdgeBook::new(&[], true);
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let me = socket.local_addr().unwrap();
        let t0 = Instant::now();
        let mut listener = Listener::new(&config, 1, 0, 0, 0, socket, t0);
        let mut counters = vec![Counters::default()];
        let mut drive = Harness::drive(&config, &book);
        service(&mut listener, 0, &mut counters, &mut drive, false, t0);
        let mut buf = [0_u8; 2_048];
        let (len, from) = entry.recv_from(&mut buf).unwrap();
        assert_eq!(from, me, "ASSIGN from the listener's own socket");
        assert!(len >= ASSIGN_REQUEST_BYTES, "padded: {len}");
        let Some(Control::Assign {
            nonce,
            paths: 2,
            spare: true,
            token: None,
            ..
        }) = Control::decode(&buf[..len])
        else {
            panic!("{:?}", Control::decode(&buf[..len]));
        };
        let caches = (2..=4)
            .map(|n| AssignedCache {
                node_id: format!("edge-{n}"),
                playback_base_url: format!("https://127.0.0.{n}:443"),
                failover: false,
            })
            .collect();
        let assigned = Control::Assigned {
            stream_id: STREAM,
            nonce,
            port: Some(port),
            caches,
            paths: 2,
            spare: true,
            why: None,
        }
        .encode();
        let feed = |listener: &mut Listener,
                    counters: &mut Vec<Counters>,
                    drive: &mut Drive<'_>,
                    bytes: &[u8],
                    peer: SocketAddr,
                    at: Instant| {
            let datagram = Datagram {
                data: bytes,
                peer: Some(peer),
                kernel_us: None,
                overflows: None,
                truncated: false,
            };
            handle_datagram(listener, counters, drive, &datagram, false, at, now_unix_us());
            drive.perform(listener, 0, counters, at);
        };
        let answered = t0 + Duration::from_millis(5);
        feed(&mut listener, &mut counters, &mut drive, &assigned, from_entry(&entry), answered);
        let stats = &listener.director.stats;
        assert_eq!((stats.answers_udp, stats.answers_https), (1, 0), "the director's");
        let nodes: Vec<&str> = listener.director.legs().iter().map(|leg| leg.node.as_str()).collect();
        assert_eq!(nodes, ["edge-2", "edge-3"]);
        assert_eq!(listener.legs.len(), 2, "booked");
        assert_eq!(listener.director.spare(), Some("edge-4"));
        assert_eq!(counters.iter().map(|edge| edge.assignments).sum::<u64>(), 1);
        assert_eq!(counters.iter().map(|edge| edge.datagrams).sum::<u64>(), 0, "no media");
        // Each leg's first SUB, and the spare's token request, from the
        // listener's socket.
        for edge in &edges {
            let (len, from) = edge.recv_from(&mut buf).unwrap();
            assert_eq!(from, me);
            assert!(
                matches!(Control::decode(&buf[..len]), Some(Control::Subscribe { token: None, .. })),
                "{:?}",
                Control::decode(&buf[..len])
            );
        }
        // The HTTPS hedge (50 ms after the ASSIGN) never goes.
        let mut actions = Vec::new();
        listener
            .director
            .poll(&mut listener.session, t0 + Duration::from_millis(100), &mut actions);
        assert!(!actions.iter().any(|action| matches!(action, Action::Https { .. })), "{actions:?}");
        // The spare's TOKEN is the director's: its request is not repeated
        // after the token wait.
        let spare = edges[2].local_addr().unwrap();
        let token = Control::Token {
            stream_id: STREAM,
            token: 0x55,
        }
        .encode();
        feed(&mut listener, &mut counters, &mut drive, &token, spare, t0 + Duration::from_millis(8));
        let mut actions = Vec::new();
        listener
            .director
            .poll(&mut listener.session, t0 + Duration::from_millis(600), &mut actions);
        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, Action::Send { to, .. } if *to == spare)),
            "{actions:?}"
        );
    }

    fn from_entry(entry: &UdpSocket) -> SocketAddr {
        entry.local_addr().unwrap()
    }

    /// Finding of the review: a run's worst listeners hold each session
    /// once in each list, however many lists of a worker it was in.
    #[test]
    fn worst_listeners_keep_a_session_once_in_each_list() {
        let entry = |sid: &str, silence_us, missing, single_path_us| WorstListener {
            sid: sid.into(),
            longest_silence_us: silence_us,
            silence_end_unix_us: 1,
            missing,
            single_path_us,
            legs: vec![("edge-1".into(), true, Some(2_000))],
            spare: None,
            want: 2,
            started_unix_us: 0,
            ended_unix_us: 0,
        };
        let mut worker = Worst::default();
        worker.offer(entry("a", 300_000, 12, 50_000));
        worker.offer(entry("b", 200_000, 0, 0));
        let mut other = Worst::default();
        other.offer(entry("c", 250_000, 3, 0));
        let mut run = Worst::default();
        run.merge(worker);
        run.merge(other);
        let sids = |list: &[WorstListener]| list.iter().map(|entry| entry.sid.clone()).collect::<Vec<_>>();
        assert_eq!(sids(&run.by_silence), ["a", "c", "b"]);
        assert_eq!(sids(&run.by_missing), ["a", "c"]);
        assert_eq!(sids(&run.by_single_path), ["a"]);
        let json = run.to_json();
        assert_eq!(json["by_single_path"][0]["single_path_ms"], 50.0);
        assert_eq!(json["by_single_path"][0]["legs"][0]["last_media_ago_ms"], 2.0);
    }

    /// `--settle-ms` follows the shortest part interval of the windows
    /// measured (a pause upstream in one does not shorten the settle), once
    /// two windows gave one; `--settle-parts` is never replaced, 4095
    /// included.
    #[test]
    fn the_settle_follows_the_shortest_interval_and_settle_parts_stays() {
        let settle = Duration::from_millis(DEFAULT_SETTLE_MS);
        let mut h = Harness::new(1);
        // 2.5 ms parts; a 400 ms pause in the first window.
        let mut published = 1_000_000_u64;
        for part in 0..=256_u64 {
            h.listener.learn_interval(settle, part, published);
            published += if part == 100 { 400_000 } else { 2_500 };
        }
        assert_eq!(h.listener.settle_parts, TRACK - 1, "one window: not yet");
        for part in 257..=512_u64 {
            h.listener.learn_interval(settle, part, published);
            published += 2_500;
        }
        assert_eq!(h.listener.settle_parts, 1_400, "3.5 s of 2.5 ms parts");
        // A window with a pause later changes nothing.
        for part in 513..=768_u64 {
            h.listener.learn_interval(settle, part, published);
            published += if part == 600 { 400_000 } else { 2_500 };
        }
        assert_eq!(h.listener.settle_parts, 1_400);
        for parts in [500, TRACK - 1] {
            let mut h = Harness::new(1);
            h.config.settle_parts = Some(parts);
            let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
            let mut listener = Listener::new(&h.config, 2, 0, 0, 0, socket, h.t0);
            for part in 0..=1_024_u64 {
                listener.learn_interval(settle, part, 1_000_000 + part * 2_500);
            }
            assert_eq!(listener.settle_parts, parts);
        }
    }

    /// Review finding 11 (refuted): media from an edge whose leg the
    /// director let go is not booked on the listener's home or another
    /// leg. The session ignores a datagram from an edge it released (it
    /// answers `UNSUB`), so nothing of it reaches the books.
    #[test]
    fn media_from_a_released_leg_is_booked_nowhere() {
        let mut h = Harness::new(2);
        for k in 0..10_u64 {
            h.part(0, 100 + k, 10.0 + k as f64);
            h.part(1, 100 + k, 10.2 + k as f64);
        }
        let (now, _) = h.at(20.0);
        let mut actions = Vec::new();
        h.listener.director.leave(&mut h.listener.session, now, &mut actions);
        sync_legs(&mut h.listener, &h.book);
        assert!(h.listener.legs.is_empty());
        let before: Vec<u64> = h.counters.iter().map(|edge| edge.datagrams).collect();
        h.part(1, 110, 21.0);
        let after: Vec<u64> = h.counters.iter().map(|edge| edge.datagrams).collect();
        assert_eq!(before, after);
        assert_eq!(h.total().parts, 10);
    }

    /// A raw sample is written under the output directory: none, no run.
    #[test]
    fn a_raw_sample_needs_an_output_directory() {
        let mut config = LoadConfig::new(SessionConfig::new(STREAM), vec!["127.0.0.1:1".parse().unwrap()], 1);
        config.raw_sample = 10;
        let error = run(config, &mut |_| {}).unwrap_err();
        assert!(error.to_string().contains("--raw-sample needs --out"), "{error}");
    }

    // ------------------------------------------------------- the party

    #[test]
    fn waves_reach_the_target_in_bursts() {
        let waves = Waves {
            size: 500,
            every: Duration::from_secs(10),
        };
        // A ramp to 1,200 from the start: 500, 1,000, then 1,200, a wave
        // at a time; nothing between two waves.
        let mut wanted = 0;
        let mut seen = Vec::new();
        for due in [true, false, true, false, true, true] {
            wanted = wave_target(&waves, wanted, 1_200, due);
            seen.push(wanted);
        }
        assert_eq!(seen, [500, 500, 1_000, 1_000, 1_200, 1_200]);
        // The ramp falls: at once, wave or not.
        assert_eq!(wave_target(&waves, 1_200, 300, false), 300);
        assert_eq!(wave_target(&waves, 1_200, 300, true), 300);
    }

    #[test]
    fn the_churn_s_draws_follow_their_seed() {
        let mut a = Rng(7);
        let mut b = Rng(7);
        let mut c = Rng(8);
        let draws: Vec<u64> = (0..8).map(|_| a.next()).collect();
        assert_eq!(draws, (0..8).map(|_| b.next()).collect::<Vec<_>>());
        assert_ne!(draws, (0..8).map(|_| c.next()).collect::<Vec<_>>(), "another host, other draws");
        // Absences fall evenly in their range; waits between leaves are
        // exponential with the rate's mean.
        let (low, high) = (Duration::from_secs(10), Duration::from_secs(60));
        let mut sum = 0.0;
        for _ in 0..10_000 {
            let absent = a.between(low, high);
            assert!(absent >= low && absent <= high);
            sum += absent.as_secs_f64();
        }
        assert!((sum / 10_000.0 - 35.0).abs() < 1.0, "{}", sum / 10_000.0);
        let mean = (0..10_000).map(|_| a.exponential(4.0).as_secs_f64()).sum::<f64>() / 10_000.0;
        assert!((mean - 0.25).abs() < 0.02, "{mean}");
        assert_eq!(
            parse_range_ms("10_000-60000").unwrap(),
            (Duration::from_secs(10), Duration::from_secs(60))
        );
        assert_eq!(parse_range_ms("500").unwrap(), (Duration::from_millis(500), Duration::from_millis(500)));
        assert!(parse_range_ms("9-3").is_err() && parse_range_ms("x").is_err());
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
