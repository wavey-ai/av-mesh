//! The edge's fast client lane (`--client-udp-fast-lane`).
//!
//! An edge whose class threads take its forwarders' RelaySession symbols
//! hands each first-arrival symbol of a stream on to the stream's UDP
//! clients that asked for the fast lane (`SUB ... F1`), on the same thread,
//! before the tee decodes anything: the contributor's own RQD2 datagram,
//! with the RLS1 header stripped, the block id rewritten to the fast quarter
//! of the client block id space (`client_udp::fast_block_id`) and its CRC
//! recomputed. One rewrapped buffer per symbol serves every client; the
//! symbols of a drained batch leave in one non-blocking `sendmmsg` per class
//! from the client socket of the class, with the clients of a part taken in
//! an order that rotates with the part. Nothing on the thread waits, locks
//! or allocates.
//!
//! - Upstream repair goes to a client only up to the repair of its policy
//!   (repair ordinal below `RepairPolicy::repair_symbols(K)`). When the
//!   tee has decoded the object, the edge tops each client up to that repair
//!   from the decoded envelope ([`EdgeFastLane::top_up`]): repair ordinals
//!   from `256 + 64 * rank` (disjoint from the contributor's), packet
//!   sequences from `0xE000_0000`, as UDP GSO per client.
//! - The cache-fed lane (`edge_udp::run_stream_sender`) and the fast lane
//!   share a ring per client of who sent which part ([`FastClient`]): a part
//!   that the fast lane claimed for a client is skipped by the cache-fed
//!   lane, and one that the cache-fed lane sent first (a warm-up, a backlog)
//!   is not sent again on the fast lane. A source symbol that the client
//!   socket refused gives the part back to the cache-fed lane.
//! - A client joins at the part the edge names in its `OK` (`fast_from`):
//!   older parts, the init segment and gaps come cache-fed.
//! - Init and codec configuration never ride the lane, and neither does a
//!   stream until the tee has seen it commit as a canonical slot (a Soundkit
//!   bundle is reboxed at the commit, so its part is not the envelope's).
//! - A client of the compact lane (`av_mesh::client_compact`) gets an object
//!   of one symbol (an audio part) as its compact datagram, made here once
//!   per part from the envelope that the symbol carries, with the edge's own
//!   slot functions (the bytes that the client would build from the
//!   envelope), and kept in the edge's compact ring for the window repair
//!   and NACK answers. It takes no upstream repair of such an object, and no
//!   top-up: its plan's copies, window repair and NACK protect it.
//! - Video is paced per client (`--client-udp-video-pace-percent`, wave 5
//!   W3): the video thread measures each stream's rate from the symbols it
//!   takes, cuts each client's symbols of a batch into GSO messages of about
//!   a millisecond of the client's rate (that percent of the stream's), and
//!   gives each message an earliest departure (`SO_TXTIME`): the client's
//!   last departure plus its bytes at that rate, never earlier than now and
//!   never later than `--client-udp-video-pace-max-ms` from now. The host's
//!   `fq` qdisc holds each message until then, so a client gets a 4K frame
//!   at its rate instead of at the NIC's line rate, with no timer on the
//!   thread. Other qdiscs send at once. Audio is never paced.
//! - Fan-out off the ingest thread (`--client-udp-fanout-threads`, wave 7
//!   item 10): a send to a client costs the kernel about as much as the
//!   class thread's whole receipt of the symbol, so an audio class thread
//!   that sent each 1 ms part to a few hundred clients itself fell behind
//!   its own socket, and the edge's ingest (and with it the LL-HLS commit)
//!   lost parts to the fan-out. The audio thread still plans the sends (the
//!   claims, the rewrap and the compact datagram), but a batch of more than
//!   `--client-udp-fanout-inline-sends` sends (or any batch while earlier
//!   ones are still out) goes to fan-out threads, each with the sends of
//!   its own clients (by address, so that a client's parts stay in order).
//!   A thread that has [`FANOUT_DEPTH`] batches in hand is not waited for:
//!   its clients' sends of the batch are shed (their parts go back to the
//!   cache-fed lane) and the class thread reads on.

use super::sys;
use super::view::RlsView;
use super::TrafficClass;
use crate::edge_udp::{EdgeSockets, EdgeUdpStats, Table};
use crate::memory::touch_spare;
use arc_swap::ArcSwap;
use av_mesh::client_compact as compact;
use av_mesh::client_udp::{fast_block_id, rewrap_fast, CompactPlan, RepairPolicy};
use dashmap::DashMap;
use raptorq_datagram_fec::{
    DatagramFecHeader, MediaObjectKind, RaptorQBlockProfile, RaptorQRepairEncoder,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing::debug;

/// Parts that a client's ring remembers: the fast lane and the cache-fed
/// lane are never further apart than this (the cache-fed lane sends a part
/// within its playout deadline of the newest).
pub const CLAIM_RING: usize = 2_048;
/// The largest RQD2 datagram that the lane rewraps (an RLS1 datagram is at
/// most 1,472 bytes).
const REWRAP_BYTES: usize = 1_536;
/// The first repair ordinal of the edge's own repair for rank 0, past any
/// the contributor sends.
pub const EDGE_REPAIR_ORDINAL_BASE: u32 = 256;
/// Repair ordinals of each edge rank.
pub const EDGE_REPAIR_ORDINALS_PER_RANK: u32 = 64;
/// The packet sequences of the edge's own repair.
const EDGE_REPAIR_SEQUENCE_BASE: u32 = 0xE000_0000;
/// The edge's repair of an object larger than this is encoded on the
/// blocking pool, not on the tee.
const INLINE_TOP_UP_BYTES: usize = 16 * 1024;

const LANE_FAST: u64 = 1;
const LANE_CACHE: u64 = 2;

/// Each paced GSO message carries about this much of its client's rate.
pub const PACING_SLICE_US: u64 = 1_000;
/// A stream's rate is measured over windows of this long.
const RATE_WINDOW_NS: u64 = 500_000_000;
/// The rate is known once a stream has been seen this long.
const RATE_FIRST_NS: u64 = 100_000_000;
/// A client is never paced below this rate.
const MIN_CLIENT_BPS: u64 = 1_000_000;
/// Departures the video thread remembers before it forgets past ones.
const PACED_CLIENTS: usize = 8_192;

/// The edge's pacing of its video clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VideoPacing {
    /// Each client's rate, in percent of the stream's measured rate
    /// (`--client-udp-video-pace-percent`).
    pub percent: u32,
    /// A message never departs later than this after the thread sends it
    /// (`--client-udp-video-pace-max-ms`): a burst above the rate (a
    /// keyframe) is delayed at most this much.
    pub max_delay_us: u64,
}

/// The default spreads a frame over about a sixth of its interval (a 4K
/// frame at 60 fps over some 2.8 ms, not 13 ms at 125 %) and delays no
/// message more than 2 ms: pacing takes the line-rate burst off a client's
/// path without moving a frame's completion by most of a frame interval,
/// which the frame-lateness budget of a 60 fps listener (16.7 ms) cannot
/// spare at every edge.
impl Default for VideoPacing {
    fn default() -> Self {
        Self {
            percent: 600,
            max_delay_us: 2_000,
        }
    }
}

/// A stream's rate, from the bytes of its symbols.
#[derive(Debug, Default, Clone, Copy)]
struct StreamRate {
    window_start_ns: u64,
    bytes: u64,
    bps: u64,
}

impl StreamRate {
    fn observe(&mut self, now_ns: u64, bytes: u64) {
        let elapsed = now_ns.saturating_sub(self.window_start_ns);
        if self.window_start_ns == 0 || elapsed > 2 * RATE_WINDOW_NS {
            // First seen, or back after a pause: a new window, and the
            // rate before the pause stands.
            self.window_start_ns = now_ns;
            self.bytes = 0;
        } else if elapsed >= RATE_WINDOW_NS {
            let measured = self.bytes.saturating_mul(8_000_000_000) / elapsed.max(1);
            self.bps = if self.bps == 0 {
                measured
            } else {
                (self.bps + measured) / 2
            };
            self.window_start_ns = now_ns;
            self.bytes = 0;
        }
        self.bytes = self.bytes.saturating_add(bytes);
    }

    fn bps(&self, now_ns: u64) -> Option<u64> {
        if self.bps > 0 {
            return Some(self.bps);
        }
        let elapsed = now_ns.saturating_sub(self.window_start_ns);
        (self.window_start_ns > 0 && elapsed >= RATE_FIRST_NS)
            .then(|| self.bytes.saturating_mul(8_000_000_000) / elapsed)
            .filter(|bps| *bps > 0)
    }
}

/// The video thread's pacing state: each stream's rate, each client's
/// next departure. Made with room for [`PACER_STREAMS`] streams and
/// [`PLAN_PEERS`] clients, so that the video thread's maps do not grow in
/// a pass (stall follow-up); beyond them they grow, once each.
#[derive(Debug)]
struct Pacer {
    rates: HashMap<u64, StreamRate>,
    next: HashMap<SocketAddr, u64>,
}

/// Streams whose rate a video plan has room for.
const PACER_STREAMS: usize = 64;
/// Clients a video plan has room for in its maps.
const PLAN_PEERS: usize = 1_024;

impl Default for Pacer {
    fn default() -> Self {
        Self {
            rates: HashMap::with_capacity(PACER_STREAMS),
            next: HashMap::with_capacity(PLAN_PEERS),
        }
    }
}

impl Pacer {
    /// The client's rate on `stream_id`, bits a second, once it is known.
    fn client_bps(&self, stream_id: u64, pacing: VideoPacing, now_ns: u64) -> Option<u64> {
        let bps = self.rates.get(&stream_id)?.bps(now_ns)?;
        Some((bps.saturating_mul(u64::from(pacing.percent)) / 100).max(MIN_CLIENT_BPS))
    }

    /// When a message of `bytes` to `peer` at `bps` departs, and whether
    /// the bound on the delay held it earlier than the rate would.
    fn departure(
        &mut self,
        peer: SocketAddr,
        bytes: usize,
        bps: u64,
        pacing: VideoPacing,
        now_ns: u64,
    ) -> (u64, bool) {
        let latest = now_ns.saturating_add(pacing.max_delay_us.saturating_mul(1_000));
        let next = self.next.entry(peer).or_insert(0);
        let wanted = (*next).max(now_ns);
        let departure = wanted.min(latest);
        *next = departure.saturating_add((bytes as u64).saturating_mul(8_000_000_000) / bps.max(1));
        (departure, wanted > latest)
    }

    fn forget_past(&mut self, now_ns: u64) {
        if self.next.len() > PACED_CLIENTS {
            self.next.retain(|_, next| *next > now_ns);
        }
    }
}

const fn claim_tag(part: u64, lane: u64) -> u64 {
    (part.wrapping_add(1) << 2) | lane
}

/// A fast-lane client, shared by the snapshot that the threads read and
/// the subscriber that the cache-fed lane holds.
#[derive(Debug)]
pub struct FastClient {
    /// The first part that this client takes on the fast lane.
    pub fast_from: u64,
    /// `(part + 1) << 2 | lane` of the lane that took each part, by part.
    ring: Box<[AtomicU64]>,
}

impl FastClient {
    /// Made by the thread that takes the client's SUB, which faults its
    /// ring in: the class thread writes it at the client's first part (a
    /// zeroed allocation's pages are not mapped until written).
    pub fn new(fast_from: u64) -> Arc<Self> {
        let mut ring: Box<[AtomicU64]> = (0..CLAIM_RING).map(|_| AtomicU64::new(0)).collect();
        crate::memory::touch_owned(&mut *ring);
        Arc::new(Self { fast_from, ring })
    }

    fn slot(&self, part: u64) -> &AtomicU64 {
        &self.ring[(part % CLAIM_RING as u64) as usize]
    }

    /// The fast lane takes `part` for this client: `true` if it has it
    /// already or takes it now, `false` if the cache-fed lane has it (or a
    /// send of it failed), or the part is older than the ring remembers.
    pub fn claim_fast(&self, part: u64) -> bool {
        self.claim(part, LANE_FAST)
    }

    /// The cache-fed lane takes `part` for this client: `false` if the fast
    /// lane has it.
    pub fn claim_cache(&self, part: u64) -> bool {
        self.claim(part, LANE_CACHE)
    }

    fn claim(&self, part: u64, lane: u64) -> bool {
        let slot = self.slot(part);
        let mine = claim_tag(part, lane);
        let other = claim_tag(part, lane ^ 3);
        let mut current = slot.load(Ordering::Acquire);
        loop {
            if current == mine {
                return true;
            }
            if current == other {
                return false;
            }
            if current >> 2 > part.wrapping_add(1) {
                // A newer part is in the slot: this one is older than the
                // ring. The cache-fed lane sends it (at worst twice); the
                // fast lane does not.
                return lane == LANE_CACHE;
            }
            match slot.compare_exchange(current, mine, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return true,
                Err(seen) => current = seen,
            }
        }
    }

    /// A source symbol of `part` did not reach the client socket: the
    /// cache-fed lane sends the part.
    pub fn give_back(&self, part: u64) {
        let slot = self.slot(part);
        let _ = slot.compare_exchange(
            claim_tag(part, LANE_FAST),
            claim_tag(part, LANE_CACHE),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// The fast lane has `part` for this client.
    pub fn has_fast(&self, part: u64) -> bool {
        self.slot(part).load(Ordering::Acquire) == claim_tag(part, LANE_FAST)
    }
}

/// The fast-lane clients of one repair policy and compact plan.
#[derive(Debug, Clone)]
pub struct FastGroup {
    pub repair: RepairPolicy,
    pub compact: Option<CompactPlan>,
    pub clients: Vec<(SocketAddr, Arc<FastClient>)>,
}

/// The fast-lane clients of the streams that ride the lane.
#[derive(Debug, Default)]
pub struct FastSnapshot {
    pub streams: HashMap<u64, Vec<FastGroup>>,
    /// The compact ring of each stream with compact clients, made by the
    /// rebuild (an ordinary thread), so that the class thread never makes
    /// one for a new stream (stall follow-up).
    pub(crate) rings: CompactRings,
}

/// The snapshot's compact rings, by stream.
#[derive(Default)]
pub(crate) struct CompactRings(HashMap<u64, Arc<crate::edge_udp::CompactRing>>);

impl std::fmt::Debug for CompactRings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_set().entries(self.0.keys()).finish()
    }
}

impl FastSnapshot {
    pub fn clients(&self) -> usize {
        self.streams
            .values()
            .flat_map(|groups| groups.iter())
            .map(|group| group.clients.len())
            .sum()
    }
}

/// Counters of the fast client lane.
#[derive(Debug, Default)]
pub struct FastLaneStats {
    /// Fast-lane datagrams handed to the kernel (one per client).
    pub datagrams: AtomicU64,
    /// Their UDP payload bytes (Phase AM item 8: the edge's load counts
    /// them).
    pub bytes: AtomicU64,
    /// Parts claimed for a client on the fast lane.
    pub parts: AtomicU64,
    /// Datagrams that found the client socket full: not waited for.
    pub would_block: AtomicU64,
    pub send_errors: AtomicU64,
    /// Parts given back to the cache-fed lane after a refused source symbol.
    pub given_back: AtomicU64,
    /// Upstream repair held back from a client above its policy.
    pub repair_capped: AtomicU64,
    /// The edge's own repair: objects topped up, and datagrams.
    pub top_up_objects: AtomicU64,
    pub top_up_datagrams: AtomicU64,
    pub top_up_errors: AtomicU64,
    /// Symbols of streams with fast-lane clients that cannot ride the lane
    /// (init, codec configuration, a stream not yet seen committed).
    pub not_eligible: AtomicU64,
    /// Video: UDP GSO messages (a client's symbols of a batch, together),
    /// and the times the host refused GSO (sent without it from then on).
    pub gso_messages: AtomicU64,
    pub gso_fallbacks: AtomicU64,
    /// Compact lane: parts sent as compact datagrams (one per client), and
    /// one-symbol objects whose compact datagram could not be made (they
    /// went as symbols).
    pub compact_parts: AtomicU64,
    pub compact_failed: AtomicU64,
    /// Compact parts sent from a decoded envelope (the part's source symbol
    /// was lost upstream).
    pub compact_top_ups: AtomicU64,
    /// Video pacing: messages sent with an earliest departure, those whose
    /// departure the bound on the delay brought forward, and refusals of
    /// SO_TXTIME by the host (video then goes unpaced).
    pub paced_messages: AtomicU64,
    pub paced_clamped: AtomicU64,
    pub txtime_refused: AtomicU64,
    /// Fan-out off the ingest thread: shard batches handed to the fan-out
    /// threads, their sends, and the sends shed because a thread had
    /// [`FANOUT_DEPTH`] batches in hand (their parts go back to the
    /// cache-fed lane).
    pub fanout_batches: AtomicU64,
    pub fanout_sends: AtomicU64,
    pub fanout_shed: AtomicU64,
    /// Batches whose fan-out thread waited for room in a full socket.
    pub fanout_waited: AtomicU64,
    /// Window repair of the class thread's batches (wave 8 S1): jobs handed
    /// to the repair thread, jobs it dropped because its queue was full,
    /// and the window repair that the class thread ran itself
    /// (`--client-udp-repair-thread false`): calls, the longest (µs; it
    /// takes the subscriber table's lock) and those over a millisecond.
    pub repair_handoffs: AtomicU64,
    pub repair_dropped: AtomicU64,
    pub class_repairs: AtomicU64,
    pub class_repair_max_us: AtomicU64,
    pub class_repair_over_1ms: AtomicU64,
    /// The repair thread's longest window repair (µs).
    pub repair_thread_max_us: AtomicU64,
    /// Fan-out (wave 8 B4): the longest wait of a batch for its thread
    /// (from the hand-off to the thread's first send, µs), batches that
    /// waited over a millisecond, and the sends that the class thread made
    /// itself for its own shard (`--client-udp-fanout-inline-shard`).
    pub fanout_lag_max_us: AtomicU64,
    pub fanout_lag_over_1ms: AtomicU64,
    pub fanout_inline_sends: AtomicU64,
    /// The fan-out threads' start (wave 9 first-session stall): threads
    /// that set their own scheduling and run, how long the start took
    /// (µs, its buffers made and touched included), and the bytes of the
    /// plans made for them. They start with the class thread, before it is
    /// made real-time and pinned, never on its first client.
    pub fanout_started: AtomicU64,
    pub fanout_start_us: AtomicU64,
    pub fanout_plan_bytes: AtomicU64,
    /// The scheduling that the fan-out threads run under, as each read it
    /// back once set: their policies (bit `1 << SCHED_*`: 1 is SCHED_OTHER,
    /// 2 SCHED_FIFO), their CPUs (bit per CPU, the first 64) and their nice
    /// value.
    pub fanout_policies: AtomicU64,
    pub fanout_cpus: AtomicU64,
    pub fanout_nice: std::sync::atomic::AtomicI32,
    /// The class thread's longest hand-off of a batch (µs), and the page
    /// faults that it took over its first hand-off (its buffers are touched
    /// before, so 0 is expected).
    pub fanout_dispatch_max_us: AtomicU64,
    pub fanout_first_dispatch_faults: AtomicU64,
    /// Times a plan's list of sends grew on the class thread (an allocation
    /// on it: a batch with more sends than any before).
    pub class_plan_grows: AtomicU64,
    /// The class thread's longest compact datagram (µs: the compact ring's
    /// locks, shared with ordinary threads, and an allocation), and its
    /// longest send of a batch itself (µs).
    pub class_compact_max_us: AtomicU64,
    pub class_send_max_us: AtomicU64,
    /// The audio class thread's allocator heap grown and touched before it
    /// was made real-time (bytes).
    pub class_arena_warm_bytes: AtomicU64,
}

/// What `/api/mesh` reports of the fast client lane.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(default)]
pub struct FastLaneSnapshot {
    pub enabled: bool,
    pub clients: u64,
    pub streams: u64,
    pub datagrams: u64,
    pub parts: u64,
    pub would_block: u64,
    pub send_errors: u64,
    pub given_back: u64,
    pub repair_capped: u64,
    pub top_up_objects: u64,
    pub top_up_datagrams: u64,
    pub top_up_errors: u64,
    pub not_eligible: u64,
    pub gso_messages: u64,
    pub gso_fallbacks: u64,
    pub compact_parts: u64,
    pub compact_failed: u64,
    pub compact_top_ups: u64,
    pub paced_messages: u64,
    pub paced_clamped: u64,
    pub txtime_refused: u64,
    pub fanout_threads: u64,
    pub fanout_batches: u64,
    pub fanout_sends: u64,
    pub fanout_shed: u64,
    #[serde(default)]
    pub fanout_waited: u64,
    #[serde(default)]
    pub repair_handoffs: u64,
    #[serde(default)]
    pub repair_dropped: u64,
    #[serde(default)]
    pub class_repairs: u64,
    #[serde(default)]
    pub class_repair_max_us: u64,
    #[serde(default)]
    pub class_repair_over_1ms: u64,
    #[serde(default)]
    pub repair_thread_max_us: u64,
    #[serde(default)]
    pub fanout_lag_max_us: u64,
    #[serde(default)]
    pub fanout_lag_over_1ms: u64,
    #[serde(default)]
    pub fanout_inline_sends: u64,
    #[serde(default)]
    pub bytes: u64,
    #[serde(default)]
    pub fanout_started: u64,
    #[serde(default)]
    pub fanout_start_us: u64,
    #[serde(default)]
    pub fanout_plan_bytes: u64,
    #[serde(default)]
    pub fanout_policies: u64,
    #[serde(default)]
    pub fanout_cpus: u64,
    #[serde(default)]
    pub fanout_nice: i64,
    #[serde(default)]
    pub fanout_dispatch_max_us: u64,
    #[serde(default)]
    pub fanout_first_dispatch_faults: u64,
    #[serde(default)]
    pub class_plan_grows: u64,
    #[serde(default)]
    pub class_compact_max_us: u64,
    #[serde(default)]
    pub class_send_max_us: u64,
    #[serde(default)]
    pub class_arena_warm_bytes: u64,
}

struct Armed {
    sockets: EdgeSockets,
    table: Table,
    stats: Arc<EdgeUdpStats>,
}

/// The fast client lane of an edge. Made at start, given to the class
/// threads and the tee, and armed by `edge_udp::run_edge_udp` with the
/// client sockets and the subscriber table once they exist.
pub struct EdgeFastLane {
    snapshot: ArcSwap<FastSnapshot>,
    /// The snapshots that rebuilds replaced, kept while a class thread
    /// holds one (see `retired`): their HashMap, groups and clients are
    /// freed by a rebuilding (ordinary) thread, not by the real-time thread
    /// into another thread's allocator arena on a subscriber's join or
    /// leave.
    retired: super::retired::Retired<FastSnapshot>,
    /// The clients that rebuilds dropped, kept while a class thread's plan
    /// holds one (a plan keeps its clients until its next batch).
    gone: super::retired::Retired<FastClient>,
    /// Streams the tee has seen commit: `true` for a canonical slot (the
    /// lane may carry them), `false` for a reboxed one.
    eligible: DashMap<u64, bool>,
    armed: OnceLock<Armed>,
    /// The client sockets' descriptors by class, once armed.
    fds: [AtomicU64; 2],
    rank: u32,
    next_sequence: AtomicU32,
    /// A one-symbol part decoded from repair alone goes to the compact
    /// clients at once from the decoded envelope
    /// (`--client-udp-compact-top-up`; `false`: the cache-fed lane sends
    /// it, as before).
    compact_top_up: AtomicBool,
    /// Compact datagrams of this lane carry the fast-lane mark
    /// (`--client-udp-compact-mark-fast-lane`), so that a client can count
    /// the parts of each lane.
    compact_mark: AtomicBool,
    /// Video pacing: the percent of the stream's rate (0: off), the bound
    /// on the delay, and whether the video socket takes SO_TXTIME
    /// (`TXTIME_*`).
    pace_percent: AtomicU32,
    pace_max_delay_us: AtomicU64,
    txtime: AtomicU8,
    /// Fan-out threads of the audio class thread (0: it sends every batch
    /// itself, as before), and the most sends of a batch that it sends
    /// itself.
    fanout_threads: AtomicU32,
    fanout_inline_sends: AtomicU32,
    /// With fan-out threads, the class thread sends one shard of each batch
    /// itself (`--client-udp-fanout-inline-shard`, wave 8 B4): its clients
    /// wait for no hand-off, and a batch wakes one thread fewer.
    fanout_inline_shard: AtomicBool,
    /// The nice value of the fan-out threads (`--client-udp-fanout-nice`,
    /// wave 8; [`FANOUT_NICE_PROCESS`]: the process's, as in wave 7).
    fanout_nice: std::sync::atomic::AtomicI32,
    /// The class thread hands its batches' window repair to a repair
    /// thread (`--client-udp-repair-thread`, wave 8 S1): window repair
    /// takes the subscriber table's lock, which ordinary tasks hold (SUB
    /// admission, NACK answers, the cache-fed lane), so the real-time
    /// thread would wait behind them.
    repair_thread: AtomicBool,
    repair_jobs: OnceLock<std::sync::mpsc::SyncSender<RepairJob>>,
    pub stats: FastLaneStats,
}

/// The window repair of one part, for the repair thread.
#[derive(Debug, Clone, Copy)]
struct RepairJob {
    stream_id: u64,
    part: u64,
    windows: [u8; REPAIR_JOB_WINDOWS],
    count: u8,
    /// The part's canonical compact datagram, which the repair thread keeps
    /// in the ring before the window's repair reads it.
    canonical: [u8; compact::MAX_DATAGRAM],
    len: u16,
}

/// The plans' windows a job carries (distinct windows of one part: the
/// plans in use have one or two).
const REPAIR_JOB_WINDOWS: usize = 8;
/// Jobs that wait for the repair thread, at most: a window completes once
/// in 8-64 parts of a stream, so this is seconds of jobs.
const REPAIR_QUEUE: usize = 4_096;

const TXTIME_UNSET: u8 = 0;
const TXTIME_ON: u8 = 1;
const TXTIME_REFUSED: u8 = 2;

impl std::fmt::Debug for EdgeFastLane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EdgeFastLane")
            .field("armed", &self.is_armed())
            .field("rank", &self.rank)
            .finish_non_exhaustive()
    }
}

impl EdgeFastLane {
    /// `rank` (`--client-udp-edge-rank`) keeps the repair of edges apart.
    pub fn new(rank: u32) -> Arc<Self> {
        // Faulted in here: the class threads write its counters.
        crate::memory::touched_arc(Self {
            snapshot: ArcSwap::from_pointee(FastSnapshot::default()),
            retired: super::retired::Retired::default(),
            gone: super::retired::Retired::default(),
            eligible: DashMap::new(),
            armed: OnceLock::new(),
            fds: [AtomicU64::new(u64::MAX), AtomicU64::new(u64::MAX)],
            rank,
            next_sequence: AtomicU32::new(0),
            compact_top_up: AtomicBool::new(true),
            compact_mark: AtomicBool::new(true),
            pace_percent: AtomicU32::new(0),
            pace_max_delay_us: AtomicU64::new(0),
            txtime: AtomicU8::new(TXTIME_UNSET),
            fanout_threads: AtomicU32::new(DEFAULT_FANOUT_THREADS as u32),
            fanout_inline_sends: AtomicU32::new(DEFAULT_FANOUT_INLINE_SENDS as u32),
            repair_thread: AtomicBool::new(true),
            repair_jobs: OnceLock::new(),
            fanout_inline_shard: AtomicBool::new(DEFAULT_FANOUT_INLINE_SHARD),
            fanout_nice: std::sync::atomic::AtomicI32::new(DEFAULT_FANOUT_NICE),
            stats: FastLaneStats::default(),
        })
    }

    /// Tests: the subscriber table the lane was armed with.
    #[cfg(test)]
    pub fn table_for_test(&self) -> Option<Table> {
        self.armed.get().map(|armed| Arc::clone(&armed.table))
    }

    /// The class thread hands its window repair to a repair thread (`true`,
    /// the default), or runs it itself, taking the subscriber table's lock
    /// (`false`, as before wave 8).
    pub fn set_repair_thread(&self, on: bool) {
        self.repair_thread.store(on, Ordering::Relaxed);
    }

    /// The window repair of `part` for the plans' `windows`, from the class
    /// thread: handed to the repair thread (started when the lane was armed),
    /// or, with `--client-udp-repair-thread false` (or a repair thread that
    /// did not start), run here. Never waits on a lock when handed off; a
    /// full queue drops the job (counted): the part's clients then have NACK.
    fn class_window_repair(
        self: &EdgeFastLane,
        stream_id: u64,
        part: u64,
        windows: &[u8],
        canonical: &[u8],
    ) {
        let Some(armed) = self.armed.get() else {
            return;
        };
        let jobs = self
            .repair_thread
            .load(Ordering::Relaxed)
            .then(|| self.repair_jobs.get())
            .flatten();
        let Some(jobs) = jobs else {
            // Without the repair thread (--client-udp-repair-thread false,
            // or it did not start) the class thread keeps the part and
            // repairs itself, allocating as it does.
            let started = Instant::now();
            let mut kept = canonical.to_vec();
            compact::set_flags(&mut kept, 0, false);
            self.keep_canonical(stream_id, part, &kept);
            crate::edge_udp::window_repair_now(
                &armed.sockets,
                &armed.table,
                true,
                &armed.stats,
                stream_id,
                part,
                Some(windows),
            );
            let took = started.elapsed().as_micros() as u64;
            self.stats.class_repairs.fetch_add(1, Ordering::Relaxed);
            self.stats
                .class_repair_max_us
                .fetch_max(took, Ordering::Relaxed);
            if took > 1_000 {
                self.stats
                    .class_repair_over_1ms
                    .fetch_add(1, Ordering::Relaxed);
            }
            return;
        };
        let mut job = RepairJob {
            stream_id,
            part,
            windows: [0; REPAIR_JOB_WINDOWS],
            count: 0,
            canonical: [0; compact::MAX_DATAGRAM],
            len: 0,
        };
        if canonical.len() <= compact::MAX_DATAGRAM {
            job.canonical[..canonical.len()].copy_from_slice(canonical);
            // The canonical form: no copies, no mark.
            compact::set_flags(&mut job.canonical[..canonical.len()], 0, false);
            job.len = canonical.len() as u16;
        }
        for window in windows {
            if usize::from(job.count) == REPAIR_JOB_WINDOWS {
                break;
            }
            if !job.windows[..usize::from(job.count)].contains(window) {
                job.windows[usize::from(job.count)] = *window;
                job.count += 1;
            }
        }
        match jobs.try_send(job) {
            Ok(()) => self.stats.repair_handoffs.fetch_add(1, Ordering::Relaxed),
            Err(_) => self.stats.repair_dropped.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Start the repair thread (at ordinary priority, on any of the
    /// process's CPUs) from the thread that arms the lane. It used to start
    /// on first use, from the class thread: a thread inherits the policy and
    /// the CPUs of the thread that starts it, so it was born SCHED_FIFO on the
    /// class thread's CPU, and the start's allocations and system calls ran
    /// on the real-time thread with clients waiting (wave 9).
    fn start_repair_thread(&self) {
        if self.repair_jobs.get().is_some() {
            return;
        }
        let Some(armed) = self.armed.get() else {
            return;
        };
        let (tx, rx) = std::sync::mpsc::sync_channel::<RepairJob>(REPAIR_QUEUE);
        let sockets = armed.sockets.clone();
        let table = Arc::clone(&armed.table);
        let stats = Arc::clone(&armed.stats);
        // At the fan-out threads' nice value: the repair of the audio's
        // windows, above the runtime's threads.
        let nice = self.fanout_nice.load(Ordering::Relaxed);
        let nice = (nice != FANOUT_NICE_PROCESS).then_some(nice);
        let spawned = std::thread::Builder::new()
            .name("nt-window-repair".into())
            .spawn(move || {
                if let Err(error) = sys::set_ordinary_at(nice) {
                    tracing::warn!(
                        error,
                        "the repair thread keeps the class thread's scheduling"
                    );
                }
                while let Ok(job) = rx.recv() {
                    let started = Instant::now();
                    if job.len > 0 {
                        sockets.compact().ring(job.stream_id).put(
                            job.part,
                            bytes::Bytes::copy_from_slice(&job.canonical[..usize::from(job.len)]),
                        );
                    }
                    crate::edge_udp::window_repair_now(
                        &sockets,
                        &table,
                        true,
                        &stats,
                        job.stream_id,
                        job.part,
                        Some(&job.windows[..usize::from(job.count)]),
                    );
                    if let Some(lane) = stats.fast_lane.get() {
                        lane.stats
                            .repair_thread_max_us
                            .fetch_max(started.elapsed().as_micros() as u64, Ordering::Relaxed);
                    }
                }
            });
        if let Err(error) = spawned {
            tracing::warn!(error = %error, "the repair thread did not start: window repair runs on the class thread");
            self.repair_thread.store(false, Ordering::Relaxed);
            return;
        }
        // Another thread may have set it first: its sender is used, and
        // this thread's ends with its channel.
        let _ = self.repair_jobs.set(tx);
    }

    /// The audio class thread hands batches of more than `inline_sends`
    /// sends to `threads` fan-out threads (`0`: it sends every batch itself,
    /// as before). The threads are started with the class thread
    /// ([`ClientSender::new`]): set before it starts.
    pub fn set_fanout(&self, threads: usize, inline_sends: usize) {
        self.fanout_threads
            .store(threads.min(64) as u32, Ordering::Relaxed);
        self.fanout_inline_sends.store(
            inline_sends.min(u32::MAX as usize) as u32,
            Ordering::Relaxed,
        );
    }

    /// The class thread sends one shard of each handed-off batch itself
    /// (`true`), or hands every shard to the fan-out threads (`false`, as in
    /// wave 7). Read when the threads start.
    pub fn set_fanout_inline_shard(&self, inline: bool) {
        self.fanout_inline_shard.store(inline, Ordering::Relaxed);
    }

    /// The fan-out threads' nice value ([`FANOUT_NICE_PROCESS`]: the
    /// process's, as in wave 7). Read when the threads start.
    pub fn set_fanout_nice(&self, nice: i32) {
        self.fanout_nice.store(nice, Ordering::Relaxed);
    }

    fn fanout(&self) -> (usize, usize) {
        (
            self.fanout_threads.load(Ordering::Relaxed) as usize,
            self.fanout_inline_sends.load(Ordering::Relaxed) as usize,
        )
    }

    /// The hop journal's `udp_sent` of a send made off the class thread.
    fn journal_udp_sent(&self, stream_id: u64, part: u64, peer: SocketAddr, sent_us: u64) {
        if let Some(journal) = self
            .armed
            .get()
            .and_then(|armed| armed.sockets.hop_journal())
        {
            journal.record_udp_sent(stream_id, part, peer, sent_us);
        }
    }

    /// Pace each video client (`Some`), or send video at once (`None`, as
    /// before). Takes effect on the video socket once the lane is armed.
    pub fn set_video_pacing(&self, pacing: Option<VideoPacing>) {
        let pacing = pacing.filter(|pacing| pacing.percent > 0);
        self.pace_max_delay_us.store(
            pacing.map_or(0, |pacing| pacing.max_delay_us),
            Ordering::Relaxed,
        );
        self.pace_percent
            .store(pacing.map_or(0, |pacing| pacing.percent), Ordering::Release);
        if pacing.is_some() {
            self.enable_txtime();
        }
    }

    /// The pacing in force: set, and the video socket takes SO_TXTIME.
    fn video_pacing(&self) -> Option<VideoPacing> {
        let percent = self.pace_percent.load(Ordering::Acquire);
        (percent > 0 && self.txtime.load(Ordering::Acquire) == TXTIME_ON).then(|| VideoPacing {
            percent,
            max_delay_us: self.pace_max_delay_us.load(Ordering::Relaxed),
        })
    }

    /// SO_TXTIME on the video client socket, once, when the lane is armed
    /// and paces.
    fn enable_txtime(&self) {
        if self.pace_percent.load(Ordering::Acquire) == 0 {
            return;
        }
        let Some(fd) = self.fd(TrafficClass::Video) else {
            return;
        };
        if self.txtime.load(Ordering::Acquire) != TXTIME_UNSET {
            return;
        }
        let state = match sys::enable_txtime(fd) {
            Ok(()) => TXTIME_ON,
            Err(error) => {
                self.stats.txtime_refused.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    error = %error,
                    "SO_TXTIME is unavailable: the edge's video clients are not paced"
                );
                TXTIME_REFUSED
            }
        };
        self.txtime.store(state, Ordering::Release);
    }

    /// Send a one-symbol part decoded from repair alone to the compact
    /// clients from the decoded envelope (`true`, the default), or leave it
    /// to the cache-fed lane, as before (`false`).
    pub fn set_compact_top_up(&self, top_up: bool) {
        self.compact_top_up.store(top_up, Ordering::Relaxed);
    }

    /// Mark this lane's compact datagrams as the fast lane's (`true`, the
    /// default), or send them unmarked, as before (`false`).
    pub fn set_compact_mark(&self, mark: bool) {
        self.compact_mark.store(mark, Ordering::Relaxed);
    }

    fn mark_compact(&self, datagram: &mut [u8]) {
        if self.compact_mark.load(Ordering::Relaxed) {
            compact::set_fast_lane(datagram);
        }
    }

    /// Give the lane its client sockets and subscriber table.
    pub fn arm(&self, sockets: EdgeSockets, table: Table, stats: Arc<EdgeUdpStats>) {
        // The send-only twins of the client sockets when there are (no
        // runtime is woken by their sends), else the sockets themselves.
        let audio = sockets.send_fd(TrafficClass::Audio);
        let video = sockets.send_fd(TrafficClass::Video);
        if self
            .armed
            .set(Armed {
                sockets,
                table,
                stats,
            })
            .is_ok()
        {
            self.fds[0].store(audio as u64, Ordering::Release);
            self.fds[1].store(video as u64, Ordering::Release);
            self.enable_txtime();
            if self.repair_thread.load(Ordering::Relaxed) {
                self.start_repair_thread();
            }
        }
    }

    pub fn is_armed(&self) -> bool {
        self.armed.get().is_some()
    }

    fn fd(&self, class: TrafficClass) -> Option<RawFd> {
        let fd = self.fds[class.index()].load(Ordering::Acquire);
        (fd != u64::MAX).then_some(fd as RawFd)
    }

    pub fn load(&self) -> arc_swap::Guard<Arc<FastSnapshot>> {
        self.snapshot.load()
    }

    /// The tee committed an object of `stream_id`: whether its slot is the
    /// envelope's (`canonical`). A change rebuilds the snapshot.
    pub fn mark_committed(&self, stream_id: u64, canonical: bool) {
        if self.eligible.get(&stream_id).map(|seen| *seen) == Some(canonical) {
            return;
        }
        self.eligible.insert(stream_id, canonical);
        self.rebuild();
    }

    pub fn is_eligible(&self, stream_id: u64) -> bool {
        self.eligible.get(&stream_id).is_some_and(|seen| *seen)
    }

    /// Rebuild the snapshot from the subscriber table.
    pub fn rebuild(&self) {
        if let Some(armed) = self.armed.get() {
            let table = armed.table.lock().unwrap();
            self.rebuild_locked(&table);
        }
    }

    /// Rebuild the snapshot from the table, which the caller has locked.
    pub fn rebuild_locked(&self, table: &HashMap<u64, crate::edge_udp::StreamEntry>) {
        let mut streams = HashMap::new();
        for (stream_id, entry) in table {
            if !self.is_eligible(*stream_id) {
                continue;
            }
            let groups = crate::edge_udp::fast_groups(entry);
            if !groups.is_empty() {
                streams.insert(*stream_id, groups);
            }
        }
        // Each stream with compact clients has its ring made here, not by
        // the class thread at its first part.
        let mut rings = HashMap::new();
        if let Some(armed) = self.armed.get() {
            for (stream_id, groups) in &streams {
                if groups.iter().any(|group| group.compact.is_some()) {
                    rings.insert(*stream_id, armed.sockets.compact().ring(*stream_id));
                }
            }
        }
        let new = Arc::new(FastSnapshot {
            streams,
            rings: CompactRings(rings),
        });
        let old = self.snapshot.swap(Arc::clone(&new));
        let kept: HashSet<*const FastClient> = new
            .streams
            .values()
            .flatten()
            .flat_map(|group| group.clients.iter().map(|(_, client)| Arc::as_ptr(client)))
            .collect();
        for (_, client) in old
            .streams
            .values()
            .flatten()
            .flat_map(|group| &group.clients)
        {
            if !kept.contains(&Arc::as_ptr(client)) {
                self.gone.keep(Arc::clone(client));
            }
        }
        self.retired.keep(old);
    }

    pub fn snapshot_stats(&self) -> FastLaneSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let snapshot = self.load();
        FastLaneSnapshot {
            enabled: true,
            clients: snapshot.clients() as u64,
            streams: snapshot.streams.len() as u64,
            datagrams: load(&self.stats.datagrams),
            parts: load(&self.stats.parts),
            would_block: load(&self.stats.would_block),
            send_errors: load(&self.stats.send_errors),
            given_back: load(&self.stats.given_back),
            repair_capped: load(&self.stats.repair_capped),
            top_up_objects: load(&self.stats.top_up_objects),
            top_up_datagrams: load(&self.stats.top_up_datagrams),
            top_up_errors: load(&self.stats.top_up_errors),
            not_eligible: load(&self.stats.not_eligible),
            gso_messages: load(&self.stats.gso_messages),
            gso_fallbacks: load(&self.stats.gso_fallbacks),
            compact_parts: load(&self.stats.compact_parts),
            compact_failed: load(&self.stats.compact_failed),
            compact_top_ups: load(&self.stats.compact_top_ups),
            paced_messages: load(&self.stats.paced_messages),
            paced_clamped: load(&self.stats.paced_clamped),
            txtime_refused: load(&self.stats.txtime_refused),
            fanout_threads: self.fanout().0 as u64,
            fanout_batches: load(&self.stats.fanout_batches),
            fanout_sends: load(&self.stats.fanout_sends),
            fanout_shed: load(&self.stats.fanout_shed),
            fanout_waited: load(&self.stats.fanout_waited),
            repair_handoffs: load(&self.stats.repair_handoffs),
            repair_dropped: load(&self.stats.repair_dropped),
            class_repairs: load(&self.stats.class_repairs),
            class_repair_max_us: load(&self.stats.class_repair_max_us),
            class_repair_over_1ms: load(&self.stats.class_repair_over_1ms),
            repair_thread_max_us: load(&self.stats.repair_thread_max_us),
            fanout_lag_max_us: load(&self.stats.fanout_lag_max_us),
            fanout_lag_over_1ms: load(&self.stats.fanout_lag_over_1ms),
            fanout_inline_sends: load(&self.stats.fanout_inline_sends),
            bytes: load(&self.stats.bytes),
            fanout_started: load(&self.stats.fanout_started),
            fanout_start_us: load(&self.stats.fanout_start_us),
            fanout_plan_bytes: load(&self.stats.fanout_plan_bytes),
            fanout_policies: load(&self.stats.fanout_policies),
            fanout_cpus: load(&self.stats.fanout_cpus),
            fanout_nice: i64::from(self.stats.fanout_nice.load(Ordering::Relaxed)),
            fanout_dispatch_max_us: load(&self.stats.fanout_dispatch_max_us),
            fanout_first_dispatch_faults: load(&self.stats.fanout_first_dispatch_faults),
            class_plan_grows: load(&self.stats.class_plan_grows),
            class_compact_max_us: load(&self.stats.class_compact_max_us),
            class_send_max_us: load(&self.stats.class_send_max_us),
            class_arena_warm_bytes: load(&self.stats.class_arena_warm_bytes),
        }
    }

    /// The canonical compact datagram of a one-symbol object whose source
    /// symbol is `view`, written into `out`: the one the stream's ring keeps,
    /// else made from the part read in place; its length, or `None` when the
    /// object is not a canonical part that fits one datagram, is late, or
    /// cannot be read in place. The class thread's: no allocation, no lock
    /// but the ring slot's, and no write to the ring (the tee keeps the part
    /// there, `keep_compact`, and a window repair's thread before it runs).
    fn compact_into(
        &self,
        snapshot: &FastSnapshot,
        stream_id: u64,
        view: &RlsView<'_>,
        out: &mut [u8],
    ) -> Option<usize> {
        let ring = snapshot.rings.0.get(&stream_id)?;
        let ring_epoch = ring.epoch();
        // A part of an older source epoch is late. A newer epoch (the
        // contributor restarted) is the ring's to take up (on the tee): its
        // kept parts are the old epoch's and are not read.
        if view.epoch != 0 && view.epoch < ring_epoch {
            return None;
        }
        let newer = view.epoch > ring_epoch;
        // A gap kept (the cache-fed lane gave the part up) gives way to the
        // part.
        if !newer {
            if let Some(len) = ring.copy_part(view.object, out) {
                return Some(len);
            }
        }
        let generation = match if view.epoch != 0 {
            view.epoch
        } else {
            ring_epoch
        } {
            0 => 0,
            epoch => compact::generation_of_epoch(epoch),
        };
        // A compact (RLC1) datagram of an opaque part: the slim object
        // holds the part itself, and every copy is whole.
        let len = if let Some(slim) = view.slim {
            if view.track != "opaque-audio" {
                return None;
            }
            let slim = playlists::compact_object::SlimView::parse(slim).ok()?;
            if slim.object != view.object {
                return None;
            }
            let published_us = slim
                .published_unix_ns()
                .map_or(0, |ns| u64::try_from(ns / 1_000).unwrap_or(0));
            compact::encode_part_into(view.object, published_us, slim.payload, out)?
        } else {
            let envelope = one_symbol_envelope(view)?;
            // The part read in place: no decode, no slot (the only way the
            // class thread makes it: a full decode allocates). What it does
            // not plainly read gets no compact datagram here; the tee
            // decodes it for the ring (`keep_compact`).
            let part = av_mesh::canonical_slot::envelope_part(envelope)?;
            if part.stream_id != stream_id || part.object != view.object {
                return None;
            }
            compact::encode_part_into(view.object, part.published_unix_us, part.media, out)?
        };
        compact::set_generation(&mut out[..len], generation);
        Some(len)
    }

    /// The tee's half of `compact_into`: keep the part's compact datagram
    /// in its stream's ring (the first lane to make it keeps it), for a
    /// stream with compact clients.
    pub fn keep_compact(&self, stream_id: u64, view: &RlsView<'_>) {
        if !self.load().rings.0.contains_key(&stream_id) {
            return;
        }
        let Some(armed) = self.armed.get() else {
            return;
        };
        let ring = armed.sockets.compact().ring(stream_id);
        if !ring.observe_epoch(view.epoch) || ring.get_part(view.object).is_some() {
            return;
        }
        let datagram = if let Some(slim) = view.slim {
            if view.track != "opaque-audio" {
                return;
            }
            let Ok(slim) = playlists::compact_object::SlimView::parse(slim) else {
                return;
            };
            if slim.object != view.object {
                return;
            }
            let published_us = slim
                .published_unix_ns()
                .map_or(0, |ns| u64::try_from(ns / 1_000).unwrap_or(0));
            compact::encode_part(view.object, published_us, slim.payload)
        } else {
            one_symbol_envelope(view)
                .and_then(|envelope| compact_of_envelope(stream_id, view.object, envelope))
        };
        if let Some(mut datagram) = datagram {
            compact::set_generation(&mut datagram, ring.generation());
            ring.put(view.object, bytes::Bytes::from(datagram));
        }
    }

    /// Keep a part's canonical compact datagram in its stream's ring before
    /// a window repair that it completes reads the ring (the repair thread
    /// and the fan-out threads, which may run before the tee keeps it).
    fn keep_canonical(&self, stream_id: u64, part: u64, canonical: &[u8]) {
        if let Some(armed) = self.armed.get() {
            armed
                .sockets
                .compact()
                .ring(stream_id)
                .put(part, bytes::Bytes::copy_from_slice(canonical));
        }
    }

    /// A one-symbol part that this edge decoded from repair alone (its source
    /// symbol was lost upstream): the class threads made no compact datagram
    /// of it (a repair symbol is not the part), so its compact clients would
    /// wait for the cache-fed lane, which goes part by part behind any hole.
    /// Make the datagram from the decoded envelope and send it now, with
    /// each plan's copies, to the compact clients that the fast lane has not
    /// carried it to (`--client-udp-compact-top-up`).
    fn compact_top_up(&self, stream_id: u64, part: u64, groups: &[FastGroup], envelope: &[u8]) {
        if !self.compact_top_up.load(Ordering::Relaxed) {
            return;
        }
        let Some(armed) = self.armed.get() else {
            return;
        };
        let waiting: Vec<(SocketAddr, (u8, bool), &Arc<FastClient>)> = groups
            .iter()
            .filter_map(|group| group.compact.map(|plan| (group, plan)))
            .flat_map(|(group, plan)| {
                group
                    .clients
                    .iter()
                    .filter(move |(_, client)| part >= client.fast_from && !client.has_fast(part))
                    .map(move |(peer, client)| (*peer, (plan.copies, plan.short_header), client))
            })
            .collect();
        if waiting.is_empty() {
            return;
        }
        let ring = armed.sockets.compact().ring(stream_id);
        let canonical = match ring.get_part(part) {
            Some(kept) => kept,
            None => {
                let Some(mut datagram) = compact_of_envelope(stream_id, part, envelope) else {
                    self.stats.compact_failed.fetch_add(1, Ordering::Relaxed);
                    return;
                };
                compact::set_generation(&mut datagram, ring.generation());
                ring.put(part, bytes::Bytes::from(datagram))
            }
        };
        let mut marked: HashMap<(u8, bool), Vec<u8>> = HashMap::new();
        let mut claimed: Vec<(SocketAddr, (u8, bool))> = Vec::new();
        for (peer, (copies, short), client) in waiting {
            // The cache-fed lane may have taken it meanwhile.
            if !client.claim_fast(part) {
                continue;
            }
            marked.entry((copies, short)).or_insert_with(|| {
                let mut datagram = canonical.to_vec();
                compact::set_flags(&mut datagram, copies, false);
                self.mark_compact(&mut datagram);
                if short {
                    if let Some(short) = compact::shorten(&datagram) {
                        return short;
                    }
                }
                datagram
            });
            claimed.push((peer, (copies, short)));
        }
        if claimed.is_empty() {
            return;
        }
        let list: Vec<(&[u8], SocketAddr)> = claimed
            .iter()
            .flat_map(|(peer, form)| {
                std::iter::repeat_n((marked[form].as_slice(), *peer), 1 + usize::from(form.0))
            })
            .collect();
        let (delivered, _) =
            armed
                .sockets
                .out_for_stream(stream_id)
                .send_many(&armed.stats, true, false, &list);
        self.stats
            .compact_parts
            .fetch_add(claimed.len() as u64, Ordering::Relaxed);
        self.stats
            .compact_top_ups
            .fetch_add(claimed.len() as u64, Ordering::Relaxed);
        let windows: Vec<u8> = groups
            .iter()
            .filter_map(|group| group.compact.map(|plan| plan.window))
            .collect();
        crate::edge_udp::window_repair_now(
            &armed.sockets,
            &armed.table,
            true,
            &armed.stats,
            stream_id,
            part,
            Some(&windows),
        );
        self.stats
            .datagrams
            .fetch_add(delivered as u64, Ordering::Relaxed);
    }

    /// The edge's own repair of a decoded object for the clients that took
    /// it on the fast lane: up to each client's policy, less the upstream
    /// repair that the stream's objects carry (`upstream_repair`, which the
    /// thread forwarded). `fec` is any RQD2 datagram of the object (its
    /// geometry); `envelope` the decoded object.
    pub fn top_up(
        self: &Arc<Self>,
        stream_id: u64,
        part: u64,
        fec: &[u8],
        envelope: &[u8],
        upstream_repair: u32,
    ) {
        if !self.is_armed() {
            return;
        }
        let snapshot = self.load();
        let Some(groups) = snapshot.streams.get(&stream_id) else {
            return;
        };
        let Ok(header) = DatagramFecHeader::decode(fec) else {
            return;
        };
        let k = u32::from(header.source_symbols);
        if k == 1 {
            self.compact_top_up(stream_id, part, groups, envelope);
        }
        let mut wanted: Vec<(u32, Vec<SocketAddr>)> = Vec::new();
        for group in groups {
            if group.compact.is_some() && k == 1 {
                // Its compact datagram went instead of the symbol.
                continue;
            }
            let extra = group
                .repair
                .repair_symbols(k)
                .saturating_sub(upstream_repair)
                .min(EDGE_REPAIR_ORDINALS_PER_RANK);
            if extra == 0 {
                continue;
            }
            let peers: Vec<SocketAddr> = group
                .clients
                .iter()
                .filter(|(_, client)| client.has_fast(part))
                .map(|(peer, _)| *peer)
                .collect();
            if !peers.is_empty() {
                wanted.push((extra, peers));
            }
        }
        let Some(most) = wanted.iter().map(|(extra, _)| *extra).max() else {
            return;
        };
        let Ok(profile) = RaptorQBlockProfile::new(
            fast_block_id(part),
            header.transfer_length,
            header.source_symbols,
            header.symbol_size,
        ) else {
            return;
        };
        let first_ordinal = EDGE_REPAIR_ORDINAL_BASE
            .saturating_add(self.rank.saturating_mul(EDGE_REPAIR_ORDINALS_PER_RANK));
        let sequence = EDGE_REPAIR_SEQUENCE_BASE
            | (self.next_sequence.fetch_add(most, Ordering::Relaxed) & 0x1fff_ffff);
        let lane = Arc::clone(self);
        let job = move |envelope: &[u8]| {
            let datagrams = RaptorQRepairEncoder::new(envelope, profile, first_ordinal, sequence)
                .and_then(|mut encoder| encoder.encode_additional(most));
            let datagrams = match datagrams {
                Ok(datagrams) => datagrams,
                Err(error) => {
                    lane.stats.top_up_errors.fetch_add(1, Ordering::Relaxed);
                    debug!(stream_id, part, error = %error, "edge fast-lane repair failed");
                    return;
                }
            };
            let Some(armed) = lane.armed.get() else {
                return;
            };
            // Each client's repair together: one GSO message of the batch.
            let list: Vec<(&[u8], SocketAddr)> = wanted
                .iter()
                .flat_map(|(extra, peers)| {
                    let datagrams = &datagrams[..*extra as usize];
                    peers.iter().flat_map(move |peer| {
                        datagrams
                            .iter()
                            .map(move |datagram| (datagram.as_slice(), *peer))
                    })
                })
                .collect();
            let (delivered, _) =
                armed
                    .sockets
                    .out_for_stream(stream_id)
                    .send_many(&armed.stats, true, true, &list);
            lane.stats.top_up_objects.fetch_add(1, Ordering::Relaxed);
            lane.stats
                .top_up_datagrams
                .fetch_add(delivered as u64, Ordering::Relaxed);
        };
        if envelope.len() <= INLINE_TOP_UP_BYTES {
            job(envelope);
        } else {
            let envelope = envelope.to_vec();
            tokio::task::spawn_blocking(move || job(&envelope));
        }
    }
}

/// The envelope of a one-symbol object, read in place from its source
/// symbol `view`; `None` for any other symbol.
fn one_symbol_envelope<'a>(view: &RlsView<'a>) -> Option<&'a [u8]> {
    let header = DatagramFecHeader::decode(view.fec).ok()?;
    if header.source_symbols != 1 || view.esi != 0 {
        return None;
    }
    let symbol = header.payload(view.fec).ok()?;
    symbol.get(4..4 + header.transfer_length as usize)
}

/// The canonical compact datagram of part `part` of `stream_id` from its
/// decoded envelope; `None` when it is not a canonical part that fits one.
fn compact_of_envelope(stream_id: u64, part: u64, envelope: &[u8]) -> Option<Vec<u8>> {
    if let Some(parsed) = av_mesh::canonical_slot::envelope_part(envelope) {
        if parsed.stream_id != stream_id || parsed.object != part {
            return None;
        }
        return compact::encode_part(part, parsed.published_unix_us, parsed.media);
    }
    let decoded = av_mesh::canonical_slot::canonical_part_from_envelope(
        bytes::Bytes::copy_from_slice(envelope),
    )
    .ok()?;
    if decoded.stream_id != stream_id || decoded.object.key().object() != part {
        return None;
    }
    let (media, _init) = av_mesh::canonical_slot::slot_media(&decoded.slot, decoded.stream_id)?;
    let published = av_mesh::canonical_slot::object_published_unix_us(&decoded.object).unwrap_or(0);
    compact::encode_part(part, published, &media)
}

/// A send planned by a class thread.
#[derive(Debug, Clone, Copy)]
struct PlannedClient {
    message: u16,
    /// The compact datagram of the message, marked with this many copies,
    /// instead of its rewrapped symbol.
    compact: Option<u8>,
    source: bool,
    part: u64,
    peer: SocketAddr,
    /// Its client, by index in `ClientPlan::clients`.
    client: u32,
}

/// One class thread's working state of the fast client lane: the rewrap
/// buffers of a drained batch, the sends, and the repair that each policy
/// asks of a block of K symbols. Allocated once per thread.
pub struct ClientPlan {
    buffers: Vec<u8>,
    lens: Vec<u16>,
    /// The compact datagrams of the batch's messages: one marked copy for
    /// each count of copies (0 to MAX_COPIES), and their lengths (0: none
    /// made).
    compact_buffers: Vec<u8>,
    compact_lens: Vec<u16>,
    sends: Vec<PlannedClient>,
    /// The clients of the sends, by index in `sends` (kept apart so that a
    /// send is `Copy`).
    clients: Vec<Arc<FastClient>>,
    memo: Vec<(RepairPolicy, u32, u32)>,
    memo_next: usize,
    /// Video with GSO: the place of each client in the batch (its first
    /// send), so that a client's symbols go together and the clients keep
    /// the turns of the plan.
    turns: HashMap<SocketAddr, u32>,
    /// Video is sent as UDP GSO messages until the host refuses it once.
    gso: bool,
    /// This plan is a video thread's: it measures its streams' rates and
    /// paces its clients when the lane paces.
    video: bool,
    /// CLOCK_MONOTONIC at the start of the batch (video only).
    now_ns: u64,
    /// The stream and the part of each message of the batch.
    streams: [u64; sys::RECV_BATCH],
    parts: [u64; sys::RECV_BATCH],
    pacer: Pacer,
    runs: Vec<(usize, usize)>,
    departures: Vec<u64>,
    /// The first client that each message of the batch was handed to the
    /// kernel for, and when the batch's sends returned (the hop journal's
    /// `udp_sent`, recorded by the tee).
    first_sent: [Option<SocketAddr>; sys::RECV_BATCH],
    sent_us: u64,
    /// The compact parts of the batch and their plans' windows (stream,
    /// part, window): after the sends, the window repair of each window
    /// that one of them completes goes from here.
    compact_sent: Vec<(u64, u64, u8, u16)>,
    /// When the class thread handed this plan to a fan-out thread.
    dispatched_at: Option<Instant>,
}

const REPAIR_MEMO: usize = 64;
/// Marked copies of a compact datagram: one for each count of copies.
const COMPACT_VARIANTS: usize = compact::MAX_COPIES as usize + 1;
/// The variants in the long form, then the same in the short form (wave 7
/// item 9): variant `copies + COMPACT_VARIANTS` is `copies` in short.
const COMPACT_SLOTS: usize = 2 * COMPACT_VARIANTS;

fn compact_at(message: usize, variant: u8) -> usize {
    (message * COMPACT_SLOTS + usize::from(variant)) * compact::MAX_DATAGRAM
}

/// The variant of a plan's copies in its form.
fn compact_variant(copies: u8, short: bool) -> u8 {
    copies + if short { COMPACT_VARIANTS as u8 } else { 0 }
}

/// The length of a variant of a message's compact datagram (`len`: its long
/// form's).
fn compact_variant_len(len: u16, variant: u8) -> usize {
    if usize::from(variant) >= COMPACT_VARIANTS {
        usize::from(len).saturating_sub(compact::SHORT_SAVES)
    } else {
        usize::from(len)
    }
}

impl Default for ClientPlan {
    fn default() -> Self {
        Self::new()
    }
}

/// The size of the pages that [`touch_pages`] writes one byte of.
const TOUCH_PAGE: usize = 4_096;

/// Write a byte of every page of `bytes` (with the value it holds), so that
/// a page that `calloc` handed over untouched is faulted in now.
fn touch_pages(bytes: &mut [u8]) {
    for at in (0..bytes.len()).step_by(TOUCH_PAGE) {
        let byte = &mut bytes[at];
        // SAFETY: a valid, aligned, exclusive reference; volatile so that
        // the write of the value already there is not elided.
        unsafe { std::ptr::write_volatile(byte, *byte) };
    }
}

/// Fault in a map's whole table (its buckets are untouched until an
/// insert writes them): fill it to its capacity with keys made by `key`,
/// then empty it, which keeps the table.
fn touch_map<K: std::hash::Hash + Eq, V: Default>(
    map: &mut HashMap<K, V>,
    key: impl Fn(usize) -> K,
) {
    for index in 0..map.capacity() {
        map.insert(key(index), V::default());
    }
    map.clear();
}

/// A distinct address for each index, for [`touch_map`].
fn touch_peer(index: usize) -> SocketAddr {
    SocketAddr::from((std::net::Ipv4Addr::from(index as u32), 0))
}

impl ClientPlan {
    pub fn new() -> Self {
        let mut plan = Self {
            buffers: vec![0_u8; sys::RECV_BATCH * REWRAP_BYTES],
            lens: vec![0; sys::RECV_BATCH],
            compact_buffers: vec![0_u8; sys::RECV_BATCH * COMPACT_SLOTS * compact::MAX_DATAGRAM],
            compact_lens: vec![0; sys::RECV_BATCH],
            sends: Vec::with_capacity(sys::RECV_BATCH * 8),
            clients: Vec::with_capacity(sys::RECV_BATCH * 8),
            memo: Vec::with_capacity(REPAIR_MEMO),
            memo_next: 0,
            turns: HashMap::with_capacity(PLAN_PEERS),
            gso: false,
            video: false,
            now_ns: 0,
            streams: [0; sys::RECV_BATCH],
            parts: [0; sys::RECV_BATCH],
            pacer: Pacer::default(),
            runs: Vec::with_capacity(sys::RECV_BATCH),
            departures: Vec::with_capacity(sys::RECV_BATCH),
            first_sent: [None; sys::RECV_BATCH],
            sent_us: 0,
            compact_sent: Vec::with_capacity(sys::RECV_BATCH),
            dispatched_at: None,
        };
        plan.touch();
        plan
    }

    /// Write every page of the plan's buffers and of its lists' room, so
    /// that their page faults are taken now, by the thread that makes the
    /// plan, and not by the class thread when its first clients arrive
    /// (wave 9: the plans were made on the real-time thread at the first
    /// client, and their pages faulted in on it as it wrote them).
    fn touch(&mut self) {
        touch_pages(&mut self.buffers);
        touch_pages(&mut self.compact_buffers);
        touch_spare(&mut self.lens);
        touch_spare(&mut self.compact_lens);
        touch_spare(&mut self.sends);
        touch_spare(&mut self.clients);
        touch_spare(&mut self.memo);
        touch_spare(&mut self.runs);
        touch_spare(&mut self.departures);
        touch_spare(&mut self.compact_sent);
        touch_map(&mut self.turns, touch_peer);
        touch_map(&mut self.pacer.next, touch_peer);
        touch_map(&mut self.pacer.rates, |index| index as u64);
    }

    /// The bytes that the plan holds (its buffers and its lists' room).
    fn bytes(&self) -> usize {
        fn room<T>(list: &Vec<T>) -> usize {
            list.capacity() * std::mem::size_of::<T>()
        }
        std::mem::size_of::<Self>()
            + room(&self.buffers)
            + room(&self.lens)
            + room(&self.compact_buffers)
            + room(&self.compact_lens)
            + room(&self.sends)
            + room(&self.clients)
            + room(&self.memo)
            + room(&self.runs)
            + room(&self.departures)
            + room(&self.compact_sent)
    }

    /// Send video as UDP GSO messages (--relay-video-gso).
    pub fn with_gso(mut self, gso: bool) -> Self {
        self.gso = gso;
        self
    }

    /// The plan of a video thread: it measures its streams' rates, and
    /// paces its clients when the lane paces.
    pub fn with_video(mut self, video: bool) -> Self {
        self.video = video;
        self
    }

    /// Start a batch.
    pub fn clear(&mut self) {
        self.sends.clear();
        self.clients.clear();
        self.compact_sent.clear();
        self.first_sent = [None; sys::RECV_BATCH];
        if self.video {
            self.now_ns = sys::monotonic_ns();
        }
    }

    /// The first client that the batch's `message` was handed to the kernel
    /// for, and when: `None` if it went to no client.
    pub fn sent_to(&self, message: usize) -> Option<(SocketAddr, u64)> {
        self.first_sent
            .get(message)
            .copied()
            .flatten()
            .map(|peer| (peer, self.sent_us))
    }

    fn repair_symbols(&mut self, policy: RepairPolicy, k: u32) -> u32 {
        if let Some((_, _, r)) = self
            .memo
            .iter()
            .find(|(memo_policy, memo_k, _)| *memo_policy == policy && *memo_k == k)
        {
            return *r;
        }
        let r = policy.repair_symbols(k);
        if self.memo.len() < REPAIR_MEMO {
            self.memo.push((policy, k, r));
        } else {
            self.memo[self.memo_next] = (policy, k, r);
            self.memo_next = (self.memo_next + 1) % REPAIR_MEMO;
        }
        r
    }

    /// Plan the sends of one admitted first-arrival symbol to the stream's
    /// fast-lane clients.
    pub fn plan(
        &mut self,
        lane: &EdgeFastLane,
        snapshot: &FastSnapshot,
        message: usize,
        view: &RlsView<'_>,
    ) {
        let Some(stream_id) = view.stream_id() else {
            return;
        };
        let Some(groups) = snapshot.streams.get(&stream_id) else {
            return;
        };
        if matches!(
            view.kind,
            MediaObjectKind::Initialization | MediaObjectKind::CodecConfig
        ) {
            lane.stats.not_eligible.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let part = view.object;
        let start = message * REWRAP_BYTES;
        // A compact (RLC1) datagram has no symbol to rewrap: its compact
        // clients get the part, its RaptorQ-lane clients the cache-fed
        // lane's symbols.
        let compact_feed = view.is_compact();
        let len = if compact_feed {
            0
        } else {
            let Some(len) = rewrap_fast(
                view.fec,
                part,
                &mut self.buffers[start..start + REWRAP_BYTES],
            ) else {
                return;
            };
            len
        };
        self.lens[message] = len as u16;
        self.compact_lens[message] = 0;
        self.streams[message] = stream_id;
        self.parts[message] = part;
        if self.video {
            self.pacer
                .rates
                .entry(stream_id)
                .or_default()
                .observe(self.now_ns, len as u64);
        }
        let k = u32::from(view.source_symbols);
        // Every copy of a compact part is the part.
        let repair_ordinal = view.esi.checked_sub(k).filter(|_| !compact_feed);
        // The compact datagram of a one-symbol object, made at most once.
        let mut compact_made: Option<bool> = None;
        for group in groups {
            let mut copies = None;
            let mut short = false;
            if let Some(plan) = group.compact.filter(|_| k == 1) {
                if repair_ordinal.is_some() {
                    // Its compact datagram goes instead of the symbols.
                    lane.stats
                        .repair_capped
                        .fetch_add(group.clients.len() as u64, Ordering::Relaxed);
                    continue;
                }
                let made = *compact_made.get_or_insert_with(|| {
                    // The compact ring's map and slot locks, which ordinary
                    // threads take too, and the datagram's allocation.
                    let started = Instant::now();
                    let made = self.make_compact(lane, snapshot, stream_id, message, view);
                    lane.stats
                        .class_compact_max_us
                        .fetch_max(started.elapsed().as_micros() as u64, Ordering::Relaxed);
                    made
                });
                if made {
                    copies = Some(plan.copies);
                    short = plan.short_header;
                    let sent = (stream_id, part, plan.window, message as u16);
                    if plan.window_repair > 0 && !self.compact_sent.contains(&sent) {
                        self.compact_sent.push(sent);
                    }
                }
            }
            if compact_feed && copies.is_none() {
                lane.stats
                    .not_eligible
                    .fetch_add(group.clients.len() as u64, Ordering::Relaxed);
                continue;
            }
            if let (None, Some(ordinal)) = (copies, repair_ordinal) {
                if ordinal >= self.repair_symbols(group.repair, k) {
                    lane.stats
                        .repair_capped
                        .fetch_add(group.clients.len() as u64, Ordering::Relaxed);
                    continue;
                }
            }
            // The clients take turns at the head of the batch, part by part.
            let count = group.clients.len();
            let first = (part % count.max(1) as u64) as usize;
            for index in 0..count {
                let (peer, client) = &group.clients[(first + index) % count];
                if part < client.fast_from {
                    continue;
                }
                let fresh = !client.has_fast(part);
                // A compact part's copies are the same part: the first
                // that arrives goes, the others do not.
                if compact_feed && !fresh {
                    continue;
                }
                if !client.claim_fast(part) {
                    continue;
                }
                if fresh {
                    lane.stats.parts.fetch_add(1, Ordering::Relaxed);
                }
                let sends = 1 + copies.map_or(0, usize::from);
                for _ in 0..sends {
                    self.sends.push(PlannedClient {
                        message: message as u16,
                        // Every 256th part goes long (its time's high bits).
                        compact: copies.map(|copies| {
                            compact_variant(copies, short && !compact::keeps_long_form(part))
                        }),
                        source: repair_ordinal.is_none(),
                        part,
                        peer: *peer,
                        client: self.clients.len() as u32,
                    });
                }
                if copies.is_some() {
                    lane.stats.compact_parts.fetch_add(1, Ordering::Relaxed);
                }
                self.clients.push(Arc::clone(client));
            }
        }
    }

    /// Make the compact datagram of the message's one-symbol object, in each
    /// marked copy; `false` if it cannot be made (the clients then get the
    /// symbol).
    fn make_compact(
        &mut self,
        lane: &EdgeFastLane,
        snapshot: &FastSnapshot,
        stream_id: u64,
        message: usize,
        view: &RlsView<'_>,
    ) -> bool {
        // The canonical form, written in place in the long form's first
        // variant, which the others copy.
        let canonical_at = compact_at(message, 0);
        let Some(len) = lane.compact_into(
            snapshot,
            stream_id,
            view,
            &mut self.compact_buffers[canonical_at..canonical_at + compact::MAX_DATAGRAM],
        ) else {
            lane.stats.compact_failed.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        for copies in (0..COMPACT_VARIANTS as u8).rev() {
            let start = compact_at(message, copies);
            if copies > 0 {
                self.compact_buffers
                    .copy_within(canonical_at..canonical_at + len, start);
            }
            let buffer = &mut self.compact_buffers[start..start + len];
            compact::set_flags(buffer, copies, false);
            lane.mark_compact(buffer);
            // The same in the short form, for the clients that ask for it.
            let short_at = compact_at(message, compact_variant(copies, true));
            self.compact_buffers
                .copy_within(start..start + len, short_at);
            let _ =
                compact::shorten_in_place(&mut self.compact_buffers[short_at..short_at + len], len);
        }
        self.compact_lens[message] = len as u16;
        true
    }

    /// Group each client's sends together, keeping the order of its own
    /// sends and the clients' turns (the order of their first sends).
    fn group_by_client(&mut self) {
        self.turns.clear();
        for send in &self.sends {
            let turn = self.turns.len() as u32;
            self.turns.entry(send.peer).or_insert(turn);
        }
        let turns = &self.turns;
        // In place (a stable sort takes a buffer): a client's own sends
        // are in message order, and sends equal in both are copies of one
        // compact part.
        self.sends
            .sort_unstable_by_key(|send| (turns[&send.peer], send.message));
    }

    /// Cut each client's sends into runs of about [`PACING_SLICE_US`] of
    /// its rate (one datagram each without `gso`), and give each run its
    /// earliest departure (`runs`, `departures`). The sends are grouped by
    /// client.
    /// Returns how many departures the bound on the delay brought forward.
    fn plan_departures(&mut self, pacing: VideoPacing, gso: bool) -> u64 {
        let Self {
            sends,
            lens,
            compact_lens,
            streams,
            pacer,
            runs,
            departures,
            now_ns,
            ..
        } = self;
        let now_ns = *now_ns;
        let len_of = |index: usize| {
            let send = &sends[index];
            let message = usize::from(send.message);
            match send.compact {
                Some(variant) => compact_variant_len(compact_lens[message], variant),
                None => usize::from(lens[message]),
            }
        };
        sys::gso_runs_into(
            runs,
            sends.len(),
            |index| (len_of(index), sends[index].peer),
            |first| {
                if !gso {
                    return 1;
                }
                let stream_id = streams[usize::from(sends[first].message)];
                pacer
                    .client_bps(stream_id, pacing, now_ns)
                    .map_or(sys::GSO_MAX_SEGMENTS, |bps| {
                        let slice_bytes = bps.saturating_mul(PACING_SLICE_US) / 8_000_000;
                        (slice_bytes / len_of(first).max(1) as u64).max(1) as usize
                    })
            },
        );
        departures.clear();
        let mut clamped = 0_u64;
        for &(first, count) in runs.iter() {
            let send = &sends[first];
            let stream_id = streams[usize::from(send.message)];
            let departure = match pacer.client_bps(stream_id, pacing, now_ns) {
                Some(bps) => {
                    let bytes = (first..first + count).map(len_of).sum();
                    let (departure, held) = pacer.departure(send.peer, bytes, bps, pacing, now_ns);
                    clamped += u64::from(held);
                    departure
                }
                // The stream's rate is not known yet: at once.
                None => now_ns,
            };
            departures.push(departure);
        }
        pacer.forget_past(now_ns);
        clamped
    }

    /// Send the plan from the client socket of `class`: one non-blocking
    /// `sendmmsg` (video with `gso`: each client's symbols of the batch
    /// together, as UDP GSO messages), then one more try each for what the
    /// kernel did not take, until the socket is full. A source symbol that
    /// is refused gives its part back to the cache-fed lane for that
    /// client. A host that refuses GSO is sent to without it from then on.
    pub fn send(&mut self, lane: &EdgeFastLane, class: TrafficClass) {
        self.send_with_patience(lane, class, None);
        self.class_window_repair(lane);
    }

    /// [`Self::send`], waiting up to `patience` for room in a full socket
    /// before a part goes back to the cache-fed lane. A fan-out thread may
    /// wait (it is not the ingest), and must: it can send a batch several
    /// milliseconds after the class thread claimed its parts, when the
    /// cache-fed lane has passed over them already, so a part it gave back
    /// would go to the client only after a NACK (wave 7 review).
    pub fn send_with_patience(
        &mut self,
        lane: &EdgeFastLane,
        class: TrafficClass,
        patience: Option<Duration>,
    ) {
        if self.sends.is_empty() {
            return;
        }
        let Some(fd) = lane.fd(class) else {
            self.sends.clear();
            self.clients.clear();
            return;
        };
        let use_gso = self.gso && class == TrafficClass::Video;
        // Video is paced per client when the lane paces (and the host takes
        // SO_TXTIME).
        let pacing = (self.video && class == TrafficClass::Video)
            .then(|| lane.video_pacing())
            .flatten();
        if use_gso || pacing.is_some() {
            self.group_by_client();
        }
        if let Some(pacing) = pacing {
            let clamped = self.plan_departures(pacing, use_gso);
            if clamped > 0 {
                lane.stats
                    .paced_clamped
                    .fetch_add(clamped, Ordering::Relaxed);
            }
        }
        let (sends, buffers, lens) = (&self.sends, &self.buffers, &self.lens);
        let (compact_buffers, compact_lens) = (&self.compact_buffers, &self.compact_lens);
        let message = |index: usize| {
            let send = &sends[index];
            let message = usize::from(send.message);
            if let Some(variant) = send.compact {
                let start = compact_at(message, variant);
                return (
                    &compact_buffers
                        [start..start + compact_variant_len(compact_lens[message], variant)],
                    send.peer,
                );
            }
            let start = message * REWRAP_BYTES;
            (
                &buffers[start..start + usize::from(lens[message])],
                send.peer,
            )
        };
        let mut sent = None;
        if pacing.is_some() || use_gso {
            let result = if pacing.is_some() {
                sys::send_gso_runs(fd, &self.runs, message, Some(&self.departures))
            } else {
                sys::send_gso_batch(fd, self.sends.len(), message)
            };
            match result {
                Ok(gso_sent) => {
                    if use_gso {
                        lane.stats
                            .gso_messages
                            .fetch_add(gso_sent.messages as u64, Ordering::Relaxed);
                    }
                    if pacing.is_some() {
                        let now_ns = self.now_ns;
                        let paced = self.departures[..gso_sent.messages]
                            .iter()
                            .filter(|departure| **departure > now_ns)
                            .count();
                        lane.stats
                            .paced_messages
                            .fetch_add(paced as u64, Ordering::Relaxed);
                    }
                    sent = Some(gso_sent.datagrams);
                }
                Err(error) if use_gso && sys::gso_unsupported(&error) => {
                    self.gso = false;
                    lane.stats.gso_fallbacks.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        error = %error,
                        "UDP GSO is unavailable: the edge's video clients are sent each symbol on its own"
                    );
                }
                Err(_) => sent = Some(0),
            }
        }
        let mut sent = match sent {
            Some(sent) => sent,
            None => sys::send_batch(fd, self.sends.len(), message).unwrap_or(0),
        };
        if let Some(patience) = patience.filter(|_| !use_gso && pacing.is_none()) {
            let give_up = Instant::now() + patience;
            let mut waited = false;
            while sent < sends.len() {
                let left = give_up.saturating_duration_since(Instant::now());
                if left.is_zero() || !sys::wait_writable(fd, left.as_millis().max(1) as i32) {
                    break;
                }
                waited = true;
                let more = sys::send_batch(fd, sends.len() - sent, |index| message(sent + index))
                    .unwrap_or(0);
                if more == 0 {
                    break;
                }
                sent += more;
            }
            if waited {
                lane.stats.fanout_waited.fetch_add(1, Ordering::Relaxed);
            }
        }
        let mut delivered = sent as u64;
        let mut bytes = 0_u64;
        let (mut would_block, mut errors, mut given_back) = (0_u64, 0_u64, 0_u64);
        let (sends, clients, first_sent) = (&self.sends, &self.clients, &mut self.first_sent);
        for (index, send) in sends[..sent.min(sends.len())].iter().enumerate() {
            first_sent[usize::from(send.message)].get_or_insert(send.peer);
            bytes += message(index).0.len() as u64;
        }
        // One more try each for what the kernel did not take, until the
        // socket is full: the rest are refused without a syscall each.
        sys::send_rest(
            sent,
            sends.len(),
            |index| sys::send_batch(fd, 1, |_| message(index)),
            |index, outcome| {
                match outcome {
                    sys::RestOutcome::Sent => {
                        delivered += 1;
                        bytes += message(index).0.len() as u64;
                        let send = &sends[index];
                        first_sent[usize::from(send.message)].get_or_insert(send.peer);
                        return;
                    }
                    sys::RestOutcome::WouldBlock => would_block += 1,
                    sys::RestOutcome::Error => errors += 1,
                }
                let send = &sends[index];
                if send.source {
                    clients[send.client as usize].give_back(send.part);
                    given_back += 1;
                }
            },
        );
        for (counter, count) in [
            (&lane.stats.would_block, would_block),
            (&lane.stats.send_errors, errors),
            (&lane.stats.given_back, given_back),
        ] {
            if count > 0 {
                counter.fetch_add(count, Ordering::Relaxed);
            }
        }
        lane.stats.datagrams.fetch_add(delivered, Ordering::Relaxed);
        lane.stats.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.sent_us = crate::now_unix_us();
        self.sends.clear();
        self.clients.clear();
    }

    /// The sends planned for the batch so far.
    pub fn pending_sends(&self) -> usize {
        self.sends.len()
    }

    /// The sends of the clients of fan-out `shard` (of `shards`), into
    /// `into` (cleared first), with the bytes of the messages that they
    /// send, and the batch's window repair if `with_repair`.
    fn split_into(&self, shard: usize, shards: usize, into: &mut ClientPlan, with_repair: bool) {
        into.clear();
        let mut copied = [false; sys::RECV_BATCH];
        for send in &self.sends {
            if shard_of(send.peer, shards) != shard {
                continue;
            }
            let message = usize::from(send.message);
            if !copied[message] {
                copied[message] = true;
                let start = message * REWRAP_BYTES;
                let len = usize::from(self.lens[message]);
                into.buffers[start..start + len].copy_from_slice(&self.buffers[start..start + len]);
                into.lens[message] = self.lens[message];
                let compact_len = usize::from(self.compact_lens[message]);
                into.compact_lens[message] = self.compact_lens[message];
                if compact_len > 0 {
                    for variant in 0..COMPACT_SLOTS as u8 {
                        let at = compact_at(message, variant);
                        into.compact_buffers[at..at + compact_len]
                            .copy_from_slice(&self.compact_buffers[at..at + compact_len]);
                    }
                }
                into.streams[message] = self.streams[message];
                into.parts[message] = self.parts[message];
            }
            into.sends.push(PlannedClient {
                client: into.clients.len() as u32,
                ..*send
            });
            into.clients
                .push(Arc::clone(&self.clients[send.client as usize]));
        }
        if with_repair {
            into.compact_sent.extend_from_slice(&self.compact_sent);
        }
    }

    /// Shed the sends of fan-out `shard` (of `shards`) unsent: each part
    /// claimed for one of its clients goes back to the cache-fed lane.
    /// Returns the sends shed.
    fn shed_shard(&self, shard: usize, shards: usize, lane: &EdgeFastLane) -> u64 {
        let mut shed = 0;
        for send in &self.sends {
            if shard_of(send.peer, shards) != shard {
                continue;
            }
            shed += 1;
            if send.source {
                self.clients[send.client as usize].give_back(send.part);
            }
        }
        if shed > 0 {
            lane.stats.fanout_shed.fetch_add(shed, Ordering::Relaxed);
        }
        shed
    }

    /// The batch's sends went to the fan-out threads (or were shed); its
    /// window repair too, unless `repair_kept` (no thread took the batch:
    /// this thread sends the repair, which is rare and small).
    fn handed_off(&mut self, lane: &EdgeFastLane, repair_kept: bool) {
        self.sends.clear();
        self.clients.clear();
        if repair_kept {
            self.class_window_repair(lane);
        }
        self.compact_sent.clear();
    }

    /// The hop journal's `udp_sent` of each message that this plan sent
    /// (a fan-out thread's; the class thread's go with the tee's copy).
    fn journal_sent(&self, lane: &EdgeFastLane) {
        for (message, peer) in self.first_sent.iter().enumerate() {
            if let Some(peer) = peer {
                lane.journal_udp_sent(
                    self.streams[message],
                    self.parts[message],
                    *peer,
                    self.sent_us,
                );
            }
        }
    }

    /// [`Self::window_repair`] from the class thread: each part's job goes
    /// to the lane's repair thread (wave 8 S1), so the real-time thread
    /// takes no lock that ordinary tasks hold.
    fn class_window_repair(&mut self, lane: &EdgeFastLane) {
        if self.compact_sent.is_empty() {
            return;
        }
        let mut windows = [0_u8; REPAIR_JOB_WINDOWS];
        for (index, (stream_id, part, _, message)) in self.compact_sent.iter().enumerate() {
            if self.compact_sent[..index]
                .iter()
                .any(|(other, at, _, _)| other == stream_id && at == part)
            {
                continue;
            }
            let mut count = 0;
            for (_, _, window, _) in self
                .compact_sent
                .iter()
                .filter(|(other, at, _, _)| other == stream_id && at == part)
            {
                if count < REPAIR_JOB_WINDOWS && !windows[..count].contains(window) {
                    windows[count] = *window;
                    count += 1;
                }
            }
            let message = usize::from(*message);
            let at = compact_at(message, 0);
            let canonical = &self.compact_buffers[at..at + usize::from(self.compact_lens[message])];
            lane.class_window_repair(*stream_id, *part, &windows[..count], canonical);
        }
        self.compact_sent.clear();
    }

    /// The window repair of the windows that the batch's compact parts
    /// complete, from this thread: a window's repair does not wait for the
    /// cache-fed lane to work its way to the window's end.
    fn window_repair(&mut self, lane: &EdgeFastLane) {
        if self.compact_sent.is_empty() {
            return;
        }
        if let Some(armed) = lane.armed.get() {
            let mut windows: Vec<u8> = Vec::new();
            for (index, (stream_id, part, _, message)) in self.compact_sent.iter().enumerate() {
                // Once for each part, with all its plans' windows.
                if self.compact_sent[..index]
                    .iter()
                    .any(|(other, at, _, _)| other == stream_id && at == part)
                {
                    continue;
                }
                windows.clear();
                windows.extend(
                    self.compact_sent
                        .iter()
                        .filter(|(other, at, _, _)| other == stream_id && at == part)
                        .map(|(_, _, window, _)| *window),
                );
                // The ring has the part before its window's repair reads it.
                let message = usize::from(*message);
                let at = compact_at(message, 0);
                let mut canonical =
                    self.compact_buffers[at..at + usize::from(self.compact_lens[message])].to_vec();
                compact::set_flags(&mut canonical, 0, false);
                lane.keep_canonical(*stream_id, *part, &canonical);
                crate::edge_udp::window_repair_now(
                    &armed.sockets,
                    &armed.table,
                    true,
                    &armed.stats,
                    *stream_id,
                    *part,
                    Some(&windows),
                );
            }
        }
        self.compact_sent.clear();
    }
}

// ------------------------------------------------------------------ fan-out

/// Default of `--client-udp-fanout-threads`.
pub const DEFAULT_FANOUT_THREADS: usize = 2;
/// Default of `--client-udp-fanout-inline-sends`: a batch for a few clients
/// is sent by the class thread itself (no hand-off, no wake of another
/// thread); its cost is a small part of a part's interval.
pub const DEFAULT_FANOUT_INLINE_SENDS: usize = 32;
/// Batches that a fan-out thread may have in hand before the class thread
/// sheds its clients' sends of a batch rather than wait: a few parts of
/// 1 ms.
pub const FANOUT_DEPTH: usize = 8;
/// Default of `--client-udp-fanout-nice` (wave 8): the audio's fan-out
/// above the runtime's threads (HTTP, SUB admission, NACK answers, the
/// cache-fed lane), below the real-time class thread.
pub const DEFAULT_FANOUT_NICE: i32 = -5;
/// `--client-udp-fanout-nice` of the process's own nice value (wave 7).
pub const FANOUT_NICE_PROCESS: i32 = 99;
/// Default of `--client-udp-fanout-inline-shard` (decided by the wave 8
/// lab A/B).
pub const DEFAULT_FANOUT_INLINE_SHARD: bool = false;

/// The fan-out shard of a client: fixed by its address, so that all its
/// sends go from the same thread, in order.
fn shard_of(peer: SocketAddr, shards: usize) -> usize {
    if shards <= 1 {
        return 0;
    }
    let ip = match peer.ip() {
        std::net::IpAddr::V4(ip) => u64::from(u32::from(ip)),
        std::net::IpAddr::V6(ip) => {
            let bits = u128::from(ip);
            (bits as u64) ^ ((bits >> 64) as u64)
        }
    };
    let mixed = (ip << 16 | u64::from(peer.port())).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    ((mixed >> 32) % shards as u64) as usize
}

/// A ring of plans from one thread to one other (single producer, single
/// consumer), of [`FANOUT_DEPTH`] slots, each empty or holding a plan.
///
/// It replaces `std::sync::mpsc::sync_channel` between the class thread and
/// a fan-out thread (wave 9 first-session stall). The standard channel is
/// lock-free but not wait-free: `try_send` and `try_recv` spin (and yield)
/// while the other side is between claiming a slot and marking it done. A
/// fan-out thread may run on the class thread's CPU, where the SCHED_FIFO
/// class thread preempts it whenever it wakes; caught between the two
/// steps, it stays preempted for as long as the class thread spins, and a
/// yield of a real-time thread never lets an ordinary one run. Only the
/// kernel's guard against real-time starvation ended such a wait (CFS gets
/// 50 ms of each second after about 950 ms), a stall of about a second.
///
/// Here each side keeps its own place in the ring and touches one slot per
/// call, once: a push fills an empty slot, a pop empties a full one, and
/// neither ever waits for the other.
struct PlanRing {
    slots: Box<[std::sync::atomic::AtomicPtr<ClientPlan>]>,
}

impl Drop for PlanRing {
    fn drop(&mut self) {
        for slot in self.slots.iter_mut() {
            let plan = std::mem::replace(slot.get_mut(), std::ptr::null_mut());
            if !plan.is_null() {
                // SAFETY: a slot holds only a pointer from `Box::into_raw`
                // whose plan was moved into the ring and not taken out.
                drop(unsafe { Box::from_raw(plan) });
            }
        }
    }
}

/// The sending side of a [`PlanRing`]: one per ring, used by one thread.
struct PlanTx {
    ring: Arc<PlanRing>,
    next: usize,
}

/// The receiving side of a [`PlanRing`]: one per ring, used by one thread.
struct PlanRx {
    ring: Arc<PlanRing>,
    next: usize,
}

/// A plan crosses threads through the ring.
const _: () = {
    const fn sendable<T: Send>() {}
    sendable::<Box<ClientPlan>>();
};

fn plan_ring(slots: usize) -> (PlanTx, PlanRx) {
    let ring = Arc::new(PlanRing {
        slots: (0..slots.max(1))
            .map(|_| std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()))
            .collect(),
    });
    (
        PlanTx {
            ring: Arc::clone(&ring),
            next: 0,
        },
        PlanRx { ring, next: 0 },
    )
}

impl PlanTx {
    /// Hand `plan` over, or give it back if the ring is full. Never waits.
    fn push(&mut self, plan: Box<ClientPlan>) -> Result<(), Box<ClientPlan>> {
        let slot = &self.ring.slots[self.next];
        // Acquire: the receiver is done with the slot before it is reused.
        if !slot.load(Ordering::Acquire).is_null() {
            return Err(plan);
        }
        // Release: the plan's contents before its pointer.
        slot.store(Box::into_raw(plan), Ordering::Release);
        self.next = (self.next + 1) % self.ring.slots.len();
        Ok(())
    }
}

impl PlanRx {
    /// The next plan, if one was handed over. Never waits.
    fn pop(&mut self) -> Option<Box<ClientPlan>> {
        let slot = &self.ring.slots[self.next];
        let plan = slot.load(Ordering::Acquire);
        if plan.is_null() {
            return None;
        }
        slot.store(std::ptr::null_mut(), Ordering::Release);
        self.next = (self.next + 1) % self.ring.slots.len();
        // SAFETY: the sender stored a pointer from `Box::into_raw` and gave
        // up the plan; this side is the only one that empties the slot.
        Some(unsafe { Box::from_raw(plan) })
    }
}

struct FanoutShard {
    /// Batches to the thread, and its plans back once sent.
    work: PlanTx,
    free: PlanRx,
    spare: Option<Box<ClientPlan>>,
    /// Batches handed to the thread and not yet sent.
    in_hand: Arc<AtomicU32>,
    /// The thread is to end (the class thread is ending).
    closed: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// The thread, woken when a batch is handed to it (a futex wake only
    /// when it sleeps).
    waker: Option<std::thread::Thread>,
}

/// The fan-out threads of a class thread.
struct Fanout {
    shards: Vec<FanoutShard>,
    /// The class thread's own shard (the last of `shards.len() + 1`), when
    /// it sends one itself.
    inline: Option<Box<ClientPlan>>,
}

/// How long the start of the fan-out waits for each thread to confirm that
/// it runs at its own scheduling.
const FANOUT_READY_WAIT: Duration = Duration::from_secs(2);

/// The scheduling that a thread runs under, as it reads it back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Scheduling {
    /// `SCHED_OTHER`, `SCHED_FIFO`, ...
    policy: i32,
    /// The CPUs it may run on (the first 64).
    cpus: u64,
    nice: i32,
}

/// The calling thread's scheduling.
fn current_scheduling() -> Scheduling {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: pid 0 is the calling thread.
        let policy = unsafe { libc::sched_getscheduler(0) };
        // SAFETY: a zeroed cpu_set_t is valid; sched_getaffinity writes at
        // most its size.
        let cpus = unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0 {
                (0..64_usize)
                    .filter(|cpu| libc::CPU_ISSET(*cpu, &set))
                    .fold(0_u64, |mask, cpu| mask | 1 << cpu)
            } else {
                0
            }
        };
        // SAFETY: errno is reset first, as -1 is also a nice value; gettid
        // has no preconditions.
        let nice = unsafe {
            *libc::__errno_location() = 0;
            let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
            libc::getpriority(libc::PRIO_PROCESS, tid)
        };
        Scheduling {
            policy: policy.max(0),
            cpus,
            nice,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        Scheduling {
            policy: 0,
            cpus: 0,
            nice: 0,
        }
    }
}

impl Fanout {
    /// Start `threads` fan-out threads for the class thread of `class`,
    /// with every plan that they and the class thread will hand each other
    /// made and touched, and wait until each runs at its own scheduling.
    ///
    /// Called by the class thread before it is made real-time and pinned
    /// (`ClientSender::new`), never on its first client: a thread inherits
    /// the policy, the priority and the CPUs of the thread that starts it,
    /// so a fan-out thread started by the SCHED_FIFO class thread was born
    /// on its one CPU at its priority, and could not even set itself
    /// ordinary until the class thread slept; and the start's allocations
    /// (FANOUT_DEPTH plans of about 750 KB a thread), its two `clone`s and
    /// the plans' page faults all ran on the real-time thread as the first
    /// clients arrived (wave 9 first-session stall). Each thread still sets
    /// its policy, its CPUs and its nice value itself, never inheriting them.
    fn start(
        lane: &Arc<EdgeFastLane>,
        class: TrafficClass,
        threads: usize,
    ) -> std::io::Result<Self> {
        let started = Instant::now();
        let nice = lane.fanout_nice.load(Ordering::Relaxed);
        let nice = (nice != FANOUT_NICE_PROCESS).then_some(nice);
        let inline = lane
            .fanout_inline_shard
            .load(Ordering::Relaxed)
            .then(|| Box::new(ClientPlan::new()));
        let mut plan_bytes = inline.as_ref().map_or(0, |plan| plan.bytes());
        // Built as the threads start: a thread that fails to start ends the
        // ones before it (the fan-out's drop).
        let mut fanout = Self {
            shards: Vec::with_capacity(threads),
            inline,
        };
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<Scheduling, String>>();
        for index in 0..threads {
            let (work_tx, work_rx) = plan_ring(FANOUT_DEPTH);
            let (mut free_tx, free_rx) = plan_ring(FANOUT_DEPTH);
            // FANOUT_DEPTH plans in all: one spare, the rest free.
            for _ in 1..FANOUT_DEPTH {
                let plan = Box::new(ClientPlan::new());
                plan_bytes += plan.bytes();
                if free_tx.push(plan).is_err() {
                    unreachable!("a ring of FANOUT_DEPTH slots takes FANOUT_DEPTH - 1 plans");
                }
            }
            let spare = Box::new(ClientPlan::new());
            plan_bytes += spare.bytes();
            let in_hand = Arc::new(AtomicU32::new(0));
            let closed = Arc::new(AtomicBool::new(false));
            let thread = {
                let lane = Arc::clone(lane);
                let in_hand = Arc::clone(&in_hand);
                let closed = Arc::clone(&closed);
                let ready = ready_tx.clone();
                std::thread::Builder::new()
                    .name(format!("nt-fanout-{}{index}", class.as_str()))
                    .spawn(move || {
                        // Not the class thread's real-time priority or CPU;
                        // above the runtime's threads (wave 8). Set here,
                        // whatever the starting thread ran at.
                        let set = sys::set_ordinary_at(nice).map(|()| current_scheduling());
                        if let Ok(scheduling) = &set {
                            lane.stats
                                .fanout_policies
                                .fetch_or(1 << scheduling.policy.min(62), Ordering::Relaxed);
                            lane.stats
                                .fanout_cpus
                                .fetch_or(scheduling.cpus, Ordering::Relaxed);
                            lane.stats
                                .fanout_nice
                                .store(scheduling.nice, Ordering::Relaxed);
                        }
                        lane.stats.fanout_started.fetch_add(1, Ordering::Relaxed);
                        let _ = ready.send(set);
                        drop(ready);
                        run_fanout(work_rx, free_tx, &closed, &lane, class, &in_hand);
                    })?
            };
            let waker = Some(thread.thread().clone());
            fanout.shards.push(FanoutShard {
                work: work_tx,
                free: free_rx,
                spare: Some(spare),
                in_hand,
                closed,
                thread: Some(thread),
                waker,
            });
        }
        drop(ready_tx);
        for _ in 0..threads {
            match ready_rx.recv_timeout(FANOUT_READY_WAIT) {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    tracing::warn!(
                        error,
                        "a fan-out thread keeps the scheduling it started with"
                    );
                }
                Err(_) => {
                    tracing::warn!("a fan-out thread did not confirm its scheduling in time");
                    break;
                }
            }
        }
        let start_us = started.elapsed().as_micros() as u64;
        lane.stats
            .fanout_start_us
            .store(start_us, Ordering::Relaxed);
        lane.stats
            .fanout_plan_bytes
            .fetch_add(plan_bytes as u64, Ordering::Relaxed);
        let scheduling = lane.snapshot_stats();
        tracing::info!(
            class = class.as_str(),
            threads,
            inline_shard = fanout.inline.is_some(),
            start_us,
            plan_bytes,
            policies = scheduling.fanout_policies,
            cpus = scheduling.fanout_cpus,
            nice = scheduling.fanout_nice,
            "edge client UDP: the fan-out to clients runs off the ingest thread (started before the first client)"
        );
        Ok(fanout)
    }

    /// Whether a fan-out thread still has batches in hand.
    fn busy(&self) -> bool {
        self.shards
            .iter()
            .any(|shard| shard.in_hand.load(Ordering::Acquire) > 0)
    }

    /// Hand each shard's sends of the batch to its thread, or shed them if
    /// the thread has [`FANOUT_DEPTH`] batches in hand. Never waits.
    fn dispatch(&mut self, plan: &mut ClientPlan, lane: &EdgeFastLane) {
        // With an inline shard, the batch splits one way more: the last is
        // the class thread's, sent after the threads have theirs.
        let shards = self.shards.len() + usize::from(self.inline.is_some());
        // The batch's window repair goes with the first shard that takes it.
        let mut repair_kept = !plan.compact_sent.is_empty();
        let mut grew = 0;
        for (index, shard) in self.shards.iter_mut().enumerate() {
            let Some(mut next) = shard.spare.take().or_else(|| shard.free.pop()) else {
                plan.shed_shard(index, shards, lane);
                continue;
            };
            let room = next.sends.capacity();
            plan.split_into(index, shards, &mut next, repair_kept);
            grew += u64::from(next.sends.capacity() > room);
            if next.sends.is_empty() && next.compact_sent.is_empty() {
                shard.spare = Some(next);
                continue;
            }
            let sends = next.sends.len() as u64;
            let with_repair = !next.compact_sent.is_empty();
            next.dispatched_at = Some(Instant::now());
            shard.in_hand.fetch_add(1, Ordering::AcqRel);
            match shard.work.push(next) {
                Ok(()) => {
                    if let Some(waker) = &shard.waker {
                        waker.unpark();
                    }
                    repair_kept &= !with_repair;
                    lane.stats.fanout_batches.fetch_add(1, Ordering::Relaxed);
                    lane.stats.fanout_sends.fetch_add(sends, Ordering::Relaxed);
                }
                Err(back) => {
                    shard.in_hand.fetch_sub(1, Ordering::AcqRel);
                    back.shed_shard(0, 1, lane);
                    shard.spare = Some(back);
                }
            }
        }
        if let Some(inline) = self.inline.as_mut() {
            let room = inline.sends.capacity();
            plan.split_into(shards - 1, shards, inline, false);
            grew += u64::from(inline.sends.capacity() > room);
            if !inline.sends.is_empty() {
                lane.stats
                    .fanout_inline_sends
                    .fetch_add(inline.sends.len() as u64, Ordering::Relaxed);
                inline.send_with_patience(lane, TrafficClass::Audio, None);
                inline.journal_sent(lane);
            }
        }
        if grew > 0 {
            lane.stats
                .class_plan_grows
                .fetch_add(grew, Ordering::Relaxed);
        }
        plan.handed_off(lane, repair_kept);
    }
}

impl Drop for Fanout {
    fn drop(&mut self) {
        for shard in &self.shards {
            shard.closed.store(true, Ordering::Release);
            if let Some(waker) = &shard.waker {
                waker.unpark();
            }
        }
        for shard in &mut self.shards {
            if let Some(thread) = shard.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

/// How long a fan-out thread waits for room in a full client socket before
/// it gives a part back to the cache-fed lane.
const FANOUT_SEND_PATIENCE: Duration = Duration::from_millis(2);

fn run_fanout(
    mut work: PlanRx,
    mut free: PlanTx,
    closed: &AtomicBool,
    lane: &EdgeFastLane,
    class: TrafficClass,
    in_hand: &AtomicU32,
) {
    loop {
        let Some(mut plan) = work.pop() else {
            if closed.load(Ordering::Acquire) {
                break;
            }
            // Woken by the class thread's next hand-off (or at once if it
            // came between the pop and here).
            std::thread::park();
            continue;
        };
        if let Some(at) = plan.dispatched_at.take() {
            let lag = at.elapsed().as_micros() as u64;
            lane.stats
                .fanout_lag_max_us
                .fetch_max(lag, Ordering::Relaxed);
            if lag > 1_000 {
                lane.stats
                    .fanout_lag_over_1ms
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        plan.send_with_patience(lane, class, Some(FANOUT_SEND_PATIENCE));
        // A fan-out thread is an ordinary thread: it runs its share of the
        // window repair itself.
        plan.window_repair(lane);
        plan.journal_sent(lane);
        in_hand.fetch_sub(1, Ordering::AcqRel);
        // FANOUT_DEPTH plans in all, so the ring of free plans has room.
        let _ = free.push(plan);
    }
}

/// Memory that the audio class thread allocates, touches and frees before it
/// is made real-time ([`warm_thread_arena`]).
pub const CLASS_ARENA_WARM_BYTES: usize = 32 << 20;
/// The blocks it does so in: well under `--malloc-tune`'s mmap threshold
/// (32 MB), so that they come from the thread's own heap.
const ARENA_WARM_BLOCK: usize = 1 << 20;

/// Grow the calling thread's allocator heap by about `bytes`, its pages
/// faulted in, and give it back to the heap. The class thread allocates for
/// every datagram (the tee's copy) and for every compact part; in the first
/// session its glibc arena grows under that load, and each growth is an
/// `mprotect` under the process's mmap lock and fresh pages faulted in, on
/// the real-time thread, waiting on whichever ordinary thread holds the
/// lock (wave 9: the stops were in the first session only, at the first
/// load; the second, with the heap grown, was clean). With
/// `--malloc-tune` the freed heap is kept (trim threshold 256 MB), so the
/// growth happens here, before the thread is real-time. Returns the bytes
/// touched.
fn warm_thread_arena(bytes: usize) -> usize {
    let mut blocks: Vec<Vec<u8>> = Vec::with_capacity(bytes / ARENA_WARM_BLOCK + 1);
    let mut touched = 0;
    while touched < bytes {
        let mut block: Vec<u8> = Vec::with_capacity(ARENA_WARM_BLOCK);
        touch_spare(&mut block);
        touched += block.capacity();
        blocks.push(block);
    }
    drop(blocks);
    touched
}

/// How a class thread sends its batches to the edge's clients: itself, or
/// through its fan-out threads, started with it ([`ClientSender::new`]).
#[derive(Default)]
pub struct ClientSender {
    fanout: Option<Fanout>,
    /// The room of the class thread's own plan's sends, as last seen (its
    /// growth is an allocation on the class thread).
    room: usize,
    /// The first hand-off is still to come (its page faults are counted).
    dispatched: bool,
}

impl ClientSender {
    /// The sender of a class thread, its fan-out threads started now when
    /// the class (audio) and the lane's settings have them: called by the
    /// class thread before it is made real-time and pinned, so that the
    /// threads inherit nothing of it and its first client finds them
    /// running, with every buffer made. Without a lane, without fan-out
    /// threads, or if they do not start, the class thread sends.
    pub fn new(lane: Option<&Arc<EdgeFastLane>>, class: TrafficClass) -> Self {
        let Some(lane) = lane else {
            return Self::default();
        };
        if class == TrafficClass::Audio {
            let warmed = warm_thread_arena(CLASS_ARENA_WARM_BYTES);
            lane.stats
                .class_arena_warm_bytes
                .store(warmed as u64, Ordering::Relaxed);
        }
        let (threads, _) = lane.fanout();
        if class != TrafficClass::Audio || threads == 0 {
            return Self::default();
        }
        match Fanout::start(lane, class, threads) {
            Ok(fanout) => Self {
                fanout: Some(fanout),
                ..Self::default()
            },
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "the fan-out threads did not start: the class thread sends to its clients"
                );
                Self::default()
            }
        }
    }

    /// Whether the fan-out threads run.
    #[cfg(test)]
    pub fn has_fanout(&self) -> bool {
        self.fanout.is_some()
    }

    /// Send the batch's planned sends (see the module's documentation).
    /// Starts nothing, and waits for nothing.
    pub fn send(&mut self, plan: &mut ClientPlan, lane: &Arc<EdgeFastLane>, class: TrafficClass) {
        let room = plan.sends.capacity();
        if room > self.room {
            if self.room > 0 {
                lane.stats.class_plan_grows.fetch_add(1, Ordering::Relaxed);
            }
            self.room = room;
        }
        let (_, inline_sends) = lane.fanout();
        let offload = self.fanout.as_ref().is_some_and(|fanout| {
            class == TrafficClass::Audio && (plan.pending_sends() > inline_sends || fanout.busy())
        });
        let Some(fanout) = self.fanout.as_mut().filter(|_| offload) else {
            let started = (plan.pending_sends() > 0).then(Instant::now);
            plan.send(lane, class);
            if let Some(started) = started {
                lane.stats
                    .class_send_max_us
                    .fetch_max(started.elapsed().as_micros() as u64, Ordering::Relaxed);
            }
            return;
        };
        let first = !self.dispatched;
        self.dispatched = true;
        // The first hand-off's page faults: two getrusage calls, once.
        let faults_before = first.then(|| crate::memory::faults(true).0);
        let started = Instant::now();
        fanout.dispatch(plan, lane);
        lane.stats
            .fanout_dispatch_max_us
            .fetch_max(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        if let Some(before) = faults_before {
            let after = crate::memory::faults(true).0;
            lane.stats
                .fanout_first_dispatch_faults
                .store(after.saturating_sub(before), Ordering::Relaxed);
        }
    }
}

/// The upstream repair of each stream's objects, as the tee sees it: the
/// most repair ordinals of the newest object and of the one before it. The
/// edge's top-up of a decoded object assumes the larger of the two arrived
/// (a contributor sends each object of a stream with about the same repair).
#[derive(Debug, Default)]
pub struct UpstreamRepair {
    streams: HashMap<u64, (u64, u32, u32)>,
}

impl UpstreamRepair {
    pub fn observe(&mut self, stream_id: u64, object: u64, esi: u32, k: u16) {
        let Some(ordinal) = esi.checked_sub(u32::from(k)) else {
            return;
        };
        let count = ordinal.saturating_add(1);
        let entry = self.streams.entry(stream_id).or_insert((object, 0, 0));
        if object == entry.0 {
            entry.1 = entry.1.max(count);
        } else if object > entry.0 {
            *entry = (object, count, entry.1);
        }
    }

    /// Upstream repair expected of `object`.
    pub fn expected(&self, stream_id: u64, object: u64) -> u32 {
        match self.streams.get(&stream_id) {
            Some((newest, this, before)) if *newest == object => (*this).max(*before),
            Some((_, this, before)) => (*this).max(*before),
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A plan of `messages` audio parts (part 100 + message) to `peers`
    /// clients, each part claimed for each client on the fast lane as
    /// `plan` claims it; message 1 a compact part with a copy, and a window
    /// repair to send.
    fn audio_plan(messages: usize, peers: usize) -> (ClientPlan, Vec<Arc<FastClient>>) {
        let mut plan = ClientPlan::new();
        let clients: Vec<Arc<FastClient>> = (0..peers).map(|_| FastClient::new(0)).collect();
        for message in 0..messages {
            let part = 100 + message as u64;
            let start = message * REWRAP_BYTES;
            plan.lens[message] = 300;
            for (index, byte) in plan.buffers[start..start + 300].iter_mut().enumerate() {
                *byte = (index + message * 7) as u8;
            }
            plan.streams[message] = 4001;
            plan.parts[message] = part;
            let compact = (message == 1).then_some(1_u8);
            plan.compact_lens[message] = 0;
            if compact.is_some() {
                plan.compact_lens[message] = 70;
                for copies in 0..COMPACT_VARIANTS as u8 {
                    let at = compact_at(message, copies);
                    plan.compact_buffers[at..at + 70].fill(0xC0 | copies);
                }
            }
            for (index, client) in clients.iter().enumerate() {
                assert!(client.claim_fast(part));
                let peer =
                    SocketAddr::from(([10, 0, (index / 200) as u8, (index % 200) as u8], 50_000));
                for _ in 0..1 + compact.map_or(0, usize::from) {
                    plan.sends.push(PlannedClient {
                        message: message as u16,
                        compact,
                        source: true,
                        part,
                        peer,
                        client: plan.clients.len() as u32,
                    });
                }
                plan.clients.push(Arc::clone(client));
            }
        }
        plan.compact_sent.push((4001, 101, 32, 1));
        (plan, clients)
    }

    /// The fan-out (wave 7 item 10): a batch splits into its shards' sends,
    /// each client in one shard (so its parts leave in order from one
    /// thread), every send in exactly one, with the bytes of the messages
    /// that it sends and the window repair in one shard only.
    #[test]
    fn a_batch_splits_into_shards_each_client_in_one_and_every_send_once() {
        let (plan, _clients) = audio_plan(3, 120);
        let shards = 3;
        let mut parts: Vec<ClientPlan> = (0..shards).map(|_| ClientPlan::new()).collect();
        let mut repair_kept = true;
        let mut seen: Vec<(u16, SocketAddr, Option<u8>)> = Vec::new();
        for (shard, into) in parts.iter_mut().enumerate() {
            plan.split_into(shard, shards, into, repair_kept);
            repair_kept &= into.compact_sent.is_empty();
            assert!(!into.sends.is_empty(), "120 clients reach every shard");
            for send in &into.sends {
                assert_eq!(shard_of(send.peer, shards), shard);
                // The send names its own client in the shard's plan.
                assert!(into.clients[send.client as usize].has_fast(send.part));
                let message = usize::from(send.message);
                let start = message * REWRAP_BYTES;
                assert_eq!(into.lens[message], plan.lens[message]);
                assert_eq!(
                    into.buffers[start..start + 300],
                    plan.buffers[start..start + 300]
                );
                if let Some(copies) = send.compact {
                    let at = compact_at(message, copies);
                    assert_eq!(into.compact_lens[message], 70);
                    assert_eq!(
                        into.compact_buffers[at..at + 70],
                        plan.compact_buffers[at..at + 70]
                    );
                }
                assert_eq!(into.parts[message], 100 + message as u64);
                assert_eq!(into.streams[message], 4001);
                seen.push((send.message, send.peer, send.compact));
            }
        }
        assert!(!repair_kept, "one shard took the window repair");
        let with_repair = parts
            .iter()
            .filter(|part| part.compact_sent == plan.compact_sent)
            .count();
        assert_eq!(with_repair, 1);
        let mut all: Vec<(u16, SocketAddr, Option<u8>)> = plan
            .sends
            .iter()
            .map(|send| (send.message, send.peer, send.compact))
            .collect();
        all.sort();
        seen.sort();
        assert_eq!(seen, all, "every send in exactly one shard");
        // One shard: everything.
        let mut one = ClientPlan::new();
        plan.split_into(0, 1, &mut one, true);
        assert_eq!(one.sends.len(), plan.sends.len());
    }

    /// A fan-out thread that has FANOUT_DEPTH batches in hand is not
    /// waited for: its clients' sends of the next batch are shed and their
    /// parts go back to the cache-fed lane; the others' are handed on.
    #[test]
    fn a_fan_out_thread_behind_sheds_its_clients_sends_to_the_cache_fed_lane() {
        let lane = EdgeFastLane::new(0);
        let mut receivers = Vec::new();
        let mut shards = Vec::new();
        for _ in 0..2 {
            let (work_tx, work_rx) = plan_ring(FANOUT_DEPTH);
            let (mut free_tx, free_rx) = plan_ring(FANOUT_DEPTH);
            for _ in 1..FANOUT_DEPTH {
                assert!(free_tx.push(Box::new(ClientPlan::new())).is_ok());
            }
            shards.push(FanoutShard {
                work: work_tx,
                free: free_rx,
                spare: Some(Box::new(ClientPlan::new())),
                in_hand: Arc::new(AtomicU32::new(0)),
                closed: Arc::new(AtomicBool::new(false)),
                thread: None,
                waker: None,
            });
            // Shard 0's thread never takes its batches; shard 1's sends
            // them at once (gives its plans back).
            receivers.push((work_rx, free_tx));
        }
        let mut fanout = Fanout {
            shards,
            inline: None,
        };
        let mut handed = 0;
        for batch in 0..=FANOUT_DEPTH {
            let (mut plan, clients) = audio_plan(1, 40);
            fanout.dispatch(&mut plan, &lane);
            assert!(plan.sends.is_empty() && plan.compact_sent.is_empty());
            // Shard 1's thread.
            let (work, free) = &mut receivers[1];
            while let Some(mut done) = work.pop() {
                done.clear();
                done.sends.clear();
                fanout.shards[1].in_hand.fetch_sub(1, Ordering::AcqRel);
                assert!(free.push(done).is_ok());
                handed += 1;
            }
            let shed = lane.stats.fanout_shed.load(Ordering::Relaxed);
            let behind: Vec<usize> = (0..40)
                .filter(|index| {
                    let peer = SocketAddr::from(([10, 0, 0, *index as u8], 50_000));
                    shard_of(peer, 2) == 0
                })
                .collect();
            assert!(!behind.is_empty());
            if batch < FANOUT_DEPTH {
                assert_eq!(shed, 0, "batch {batch} is in hand, not shed");
                for index in &behind {
                    assert!(clients[*index].has_fast(100), "still the fast lane's");
                }
            } else {
                assert_eq!(
                    shed,
                    behind.len() as u64,
                    "shard 0's sends of the batch shed"
                );
                for index in &behind {
                    assert!(
                        clients[*index].claim_cache(100),
                        "the part went back to the cache-fed lane"
                    );
                }
                for index in (0..40).filter(|index| !behind.contains(index)) {
                    assert!(clients[index].has_fast(100), "shard 1 took its sends");
                }
            }
        }
        assert_eq!(handed, FANOUT_DEPTH + 1, "shard 1's thread had every batch");
        assert!(fanout.busy(), "shard 0 still has its batches in hand");
        assert_eq!(
            lane.stats.fanout_batches.load(Ordering::Relaxed),
            (2 * FANOUT_DEPTH + 1) as u64
        );
        // Shard 0's batches are all still there, each with its window
        // repair (the first shard that takes a batch takes its repair).
        let (work, _) = &mut receivers[0];
        let mut in_hand = 0;
        while let Some(batch) = work.pop() {
            assert_eq!(batch.compact_sent, vec![(4001, 101, 32, 1)]);
            in_hand += 1;
        }
        assert_eq!(in_hand, FANOUT_DEPTH);
    }

    #[test]
    fn the_fan_out_defaults_to_two_threads_past_a_few_clients() {
        let lane = EdgeFastLane::new(0);
        assert_eq!(
            lane.fanout(),
            (DEFAULT_FANOUT_THREADS, DEFAULT_FANOUT_INLINE_SENDS)
        );
        assert_eq!(DEFAULT_FANOUT_THREADS, 2);
        lane.set_fanout(0, 10);
        assert_eq!(lane.fanout(), (0, 10));
        assert_eq!(lane.snapshot_stats().fanout_threads, 0);
    }

    // ---------------------------------------------- the fan-out's start (wave 9)

    /// The CPUs of the process's main thread, as a mask of the first 64.
    fn process_cpus() -> u64 {
        // SAFETY: a zeroed cpu_set_t is valid; sched_getaffinity writes at
        // most its size; getpid is the main thread's id.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            assert_eq!(
                libc::sched_getaffinity(
                    libc::getpid(),
                    std::mem::size_of::<libc::cpu_set_t>(),
                    &mut set
                ),
                0
            );
            (0..64_usize)
                .filter(|cpu| libc::CPU_ISSET(*cpu, &set))
                .fold(0, |mask, cpu| mask | 1 << cpu)
        }
    }

    /// Make the calling thread a stand-in for a deployed class thread:
    /// pinned to one CPU, at a policy other than SCHED_OTHER. SCHED_FIFO
    /// needs CAP_SYS_NICE, which tests do not have; SCHED_BATCH does not,
    /// and is inherited the same way.
    fn become_class_thread_stand_in() -> usize {
        let cpu = process_cpus().trailing_zeros() as usize;
        crate::fast_path::pin_current_thread(cpu).unwrap();
        let param = libc::sched_param { sched_priority: 0 };
        // SAFETY: pid 0 is the calling thread; the parameter is valid.
        assert_eq!(
            unsafe { libc::sched_setscheduler(0, libc::SCHED_BATCH, &param) },
            0
        );
        cpu
    }

    /// The fan-out threads' names in /proc/self/task.
    fn fanout_threads_running() -> usize {
        std::fs::read_dir("/proc/self/task")
            .unwrap()
            .flatten()
            .filter(|task| {
                std::fs::read_to_string(task.path().join("comm"))
                    .is_ok_and(|name| name.starts_with("nt-fanout-audio"))
            })
            .count()
    }

    /// The mechanism of the wave 9 first-session stall, as far as it shows
    /// without real-time privileges: a thread started by a pinned thread at
    /// a policy of its own is born with that policy and on that one CPU. A
    /// fan-out thread started by the SCHED_FIFO class thread on its first
    /// client was born SCHED_FIFO 10 on the class thread's CPU, where it
    /// could not run (to set itself ordinary) until the class thread slept.
    #[test]
    fn a_thread_inherits_the_policy_and_the_cpus_of_the_thread_that_starts_it() {
        std::thread::spawn(|| {
            let cpu = become_class_thread_stand_in();
            let child = std::thread::spawn(current_scheduling).join().unwrap();
            assert_eq!(child.policy, libc::SCHED_BATCH, "{child:?}");
            assert_eq!(child.cpus, 1 << cpu, "{child:?}");
        })
        .join()
        .unwrap();
    }

    /// The fix: the fan-out threads run before the first client, started
    /// with the class thread (here from a pinned stand-in at another
    /// policy, which they must not inherit), each at SCHED_OTHER on the
    /// process's CPUs at the nice value asked for; the class thread's first
    /// hand-off starts no thread and takes no page fault; the threads send
    /// and give their plans back; and they end with the sender.
    #[test]
    fn the_fan_out_threads_run_before_the_first_client_at_their_own_scheduling() {
        let lane = EdgeFastLane::new(0);
        lane.set_fanout(2, 0);
        // Raising a thread's nice value needs no privilege (the tests may
        // run niced already).
        // SAFETY: getpriority takes plain integers.
        let process =
            unsafe { libc::getpriority(libc::PRIO_PROCESS, libc::getpid() as libc::id_t) };
        let nice = (process + 2).min(19);
        lane.set_fanout_nice(nice);
        let class_lane = Arc::clone(&lane);
        std::thread::spawn(move || {
            let lane = class_lane;
            let before = fanout_threads_running();
            become_class_thread_stand_in();
            let mut sender = ClientSender::new(Some(&lane), TrafficClass::Audio);
            assert!(sender.has_fanout());
            let stats = lane.snapshot_stats();
            // Running before any client, each at its own scheduling.
            assert_eq!(stats.fanout_started, 2, "{stats:?}");
            assert_eq!(fanout_threads_running(), before + 2);
            assert_eq!(stats.fanout_policies, 1 << libc::SCHED_OTHER, "{stats:?}");
            assert_eq!(stats.fanout_cpus, process_cpus(), "{stats:?}");
            assert_eq!(stats.fanout_nice, i64::from(nice), "{stats:?}");
            // 2 x FANOUT_DEPTH plans of about 750 KB, made and touched.
            assert!(
                stats.fanout_plan_bytes >= 2 * FANOUT_DEPTH as u64 * 700_000,
                "{stats:?}"
            );
            // The first client: a batch for 120 clients, handed off.
            let (mut plan, _clients) = audio_plan(3, 120);
            let sends = plan.pending_sends() as u64;
            sender.send(&mut plan, &lane, TrafficClass::Audio);
            let stats = lane.snapshot_stats();
            assert_eq!(fanout_threads_running(), before + 2, "no thread started");
            assert_eq!(stats.fanout_sends, sends, "{stats:?}");
            assert_eq!(stats.fanout_batches, 2, "{stats:?}");
            assert_eq!(stats.fanout_shed, 0, "{stats:?}");
            assert!(
                stats.fanout_first_dispatch_faults <= 4,
                "the first hand-off faulted: {stats:?}"
            );
            assert_eq!(stats.class_plan_grows, 0, "{stats:?}");
            // The threads took their batches (the lane has no socket: the
            // sends are dropped) and gave their plans back.
            let deadline = Instant::now() + Duration::from_secs(5);
            while sender.fanout.as_ref().is_some_and(Fanout::busy) {
                assert!(Instant::now() < deadline, "the fan-out threads never ran");
                std::thread::sleep(Duration::from_millis(1));
            }
            // Many more batches: every one handed off, none shed, the plans
            // going round.
            for _ in 0..200 {
                let (mut plan, _clients) = audio_plan(3, 120);
                sender.send(&mut plan, &lane, TrafficClass::Audio);
                while sender.fanout.as_ref().is_some_and(Fanout::busy) {
                    std::thread::yield_now();
                }
            }
            let stats = lane.snapshot_stats();
            assert_eq!(stats.fanout_batches, 2 * 201, "{stats:?}");
            assert_eq!(stats.fanout_shed, 0, "{stats:?}");
            assert_eq!(stats.class_plan_grows, 0, "{stats:?}");
            drop(sender);
            assert_eq!(fanout_threads_running(), before, "the threads ended");
        })
        .join()
        .unwrap();
    }

    /// Without fan-out threads (none asked for, the video class, or no
    /// lane) the class thread sends every batch itself, starting nothing.
    #[test]
    fn a_sender_without_fan_out_threads_starts_none_and_sends_itself() {
        let lane = EdgeFastLane::new(0);
        assert!(!ClientSender::new(None, TrafficClass::Audio).has_fanout());
        assert!(!ClientSender::new(Some(&lane), TrafficClass::Video).has_fanout());
        lane.set_fanout(0, 0);
        let mut sender = ClientSender::new(Some(&lane), TrafficClass::Audio);
        assert!(!sender.has_fanout());
        let (mut plan, _clients) = audio_plan(2, 50);
        sender.send(&mut plan, &lane, TrafficClass::Audio);
        assert_eq!(plan.pending_sends(), 0);
        let stats = lane.snapshot_stats();
        assert_eq!(stats.fanout_started, 0);
        assert_eq!(stats.fanout_batches, 0);
    }

    /// A rebuild keeps the snapshot it replaced until the next one, so that
    /// a class thread holding it for a batch is never the one that frees it
    /// (a subscriber's join or leave rebuilds).
    #[test]
    fn a_replaced_snapshot_is_freed_by_the_next_rebuild_not_its_reader() {
        let lane = EdgeFastLane::new(0);
        let read = arc_swap::Guard::into_inner(lane.load());
        lane.rebuild_locked(&HashMap::new());
        assert_eq!(
            Arc::strong_count(&read),
            2,
            "the reader's and the retired copy"
        );
        let weak = Arc::downgrade(&read);
        drop(read);
        assert!(weak.upgrade().is_some(), "the reader did not free it");
        lane.rebuild_locked(&HashMap::new());
        assert!(weak.upgrade().is_none(), "the next rebuild freed it");
    }

    /// The class thread's heap, grown and touched before it is real-time,
    /// serves its later allocations without page faults (with the
    /// allocator tuned as on a deployed node).
    #[test]
    fn a_warmed_thread_allocates_without_page_faults() {
        crate::memory::tune_allocator(true).unwrap();
        let (warmed, faults) = std::thread::spawn(|| {
            let warmed = warm_thread_arena(8 << 20);
            let before = crate::memory::faults(true).0;
            let mut blocks = Vec::new();
            for _ in 0..64 {
                let mut block: Vec<u8> = Vec::with_capacity(64 << 10);
                touch_spare(&mut block);
                blocks.push(block);
            }
            (warmed, crate::memory::faults(true).0 - before)
        })
        .join()
        .unwrap();
        assert!(warmed >= 8 << 20);
        // 4 MB is 1,024 pages of 4 KB cold.
        assert!(faults <= 64, "{faults} faults after the warm-up");
    }

    /// The ring between the class thread and a fan-out thread: plans come
    /// out in the order they went in, a full ring gives the plan back at
    /// once, and between two threads (plans going round, as in the
    /// fan-out) nothing is lost, repeated or reordered.
    #[test]
    fn a_plan_ring_hands_plans_over_in_order_and_never_waits() {
        let (mut tx, mut rx) = plan_ring(FANOUT_DEPTH);
        assert!(rx.pop().is_none());
        for mark in 0..FANOUT_DEPTH {
            let mut plan = Box::new(ClientPlan::new());
            plan.parts[0] = mark as u64;
            assert!(tx.push(plan).is_ok());
        }
        let started = Instant::now();
        let back = tx.push(Box::new(ClientPlan::new())).unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(50));
        drop(back);
        for mark in 0..FANOUT_DEPTH {
            assert_eq!(rx.pop().unwrap().parts[0], mark as u64);
        }
        assert!(rx.pop().is_none());
        // A ring dropped with plans in it frees them.
        assert!(tx.push(Box::new(ClientPlan::new())).is_ok());
        drop((tx, rx));

        let (mut work_tx, mut work_rx) = plan_ring(FANOUT_DEPTH);
        let (mut free_tx, mut free_rx) = plan_ring(FANOUT_DEPTH);
        for _ in 0..FANOUT_DEPTH {
            assert!(free_tx.push(Box::new(ClientPlan::new())).is_ok());
        }
        const HANDOFFS: u64 = 200_000;
        let consumer = std::thread::spawn(move || {
            let mut expected = 0;
            while expected < HANDOFFS {
                let Some(plan) = work_rx.pop() else {
                    std::hint::spin_loop();
                    continue;
                };
                assert_eq!(plan.parts[0], expected);
                expected += 1;
                assert!(free_tx.push(plan).is_ok(), "the free ring has room");
            }
        });
        let mut sent = 0;
        while sent < HANDOFFS {
            let Some(mut plan) = free_rx.pop() else {
                std::hint::spin_loop();
                continue;
            };
            plan.parts[0] = sent;
            assert!(work_tx.push(plan).is_ok(), "the work ring has room");
            sent += 1;
        }
        consumer.join().unwrap();
    }

    /// 4K on the edge's fast lane: the plan interleaves the clients symbol
    /// by symbol; grouped for GSO, each client's symbols of a part are one
    /// run (one message), in the clients' turns, its own symbols in order.
    #[test]
    fn a_video_batch_is_one_gso_message_per_client_per_part() {
        let mut plan = ClientPlan::new().with_gso(true);
        let peers: Vec<SocketAddr> = (0..25_u16)
            .map(|port| SocketAddr::from(([127, 0, 0, 1], 40_000 + port)))
            .collect();
        let symbols = 48_usize;
        for message in 0..symbols {
            plan.lens[message] = 1_100;
            // The turns of part 7: the clients rotate from 7 % 25.
            for index in 0..peers.len() {
                let turn = (7 + index) % peers.len();
                let peer = peers[turn];
                plan.sends.push(PlannedClient {
                    message: message as u16,
                    compact: None,
                    source: true,
                    part: 7,
                    peer,
                    client: plan.clients.len() as u32,
                });
                plan.clients.push(FastClient::new(turn as u64));
            }
        }
        let interleaved = sys::gso_runs(plan.sends.len(), |index| (1_100, plan.sends[index].peer));
        assert_eq!(interleaved.len(), symbols * peers.len(), "one per datagram");
        plan.group_by_client();
        let runs = sys::gso_runs(plan.sends.len(), |index| (1_100, plan.sends[index].peer));
        assert_eq!(runs.len(), peers.len(), "one message per client");
        // The clients keep their turns, and each its symbols in order.
        assert_eq!(plan.sends[0].peer, peers[7]);
        for (run, (first, count)) in runs.iter().enumerate() {
            assert_eq!(*count, symbols);
            let turn = (7 + run) % peers.len();
            let peer = peers[turn];
            for (offset, send) in plan.sends[*first..first + count].iter().enumerate() {
                assert_eq!(send.peer, peer);
                assert_eq!(usize::from(send.message), offset);
                // The send still names its own client.
                assert_eq!(
                    plan.clients[send.client as usize].fast_from, turn as u64,
                    "the send names its own client"
                );
            }
        }
    }

    #[test]
    fn a_stream_rate_is_measured_from_its_symbols() {
        let mut rate = StreamRate::default();
        let ms = 1_000_000_u64;
        let start = 1_000 * ms;
        assert_eq!(rate.bps(start), None);
        // 25 kB a millisecond: 200 Mbit/s.
        for step in 0..=99 {
            rate.observe(start + step * ms, 25_000);
        }
        assert_eq!(rate.bps(start + 50 * ms), None, "too soon to tell");
        let early = rate.bps(start + 100 * ms).unwrap();
        assert!((190_000_000..=210_000_000).contains(&early), "{early}");
        for step in 100..=1_000 {
            rate.observe(start + step * ms, 25_000);
        }
        let steady = rate.bps(start + 1_000 * ms).unwrap();
        assert!((195_000_000..=205_000_000).contains(&steady), "{steady}");
        // A pause of seconds keeps the rate of before it.
        rate.observe(start + 5_000 * ms, 25_000);
        assert_eq!(rate.bps(start + 5_000 * ms), Some(steady));
    }

    #[test]
    fn the_default_pacing_delays_a_4k_frame_at_most_two_milliseconds() {
        let pacing = VideoPacing::default();
        let mut pacer = Pacer::default();
        let now = 10_000_000_000_u64;
        let peer = SocketAddr::from(([127, 0, 0, 1], 1));
        for (mbps, frame_bytes) in [(25_u64, 52_000_usize), (200, 420_000)] {
            pacer.rates.insert(
                mbps,
                StreamRate {
                    window_start_ns: now,
                    bytes: 0,
                    bps: mbps * 1_000_000,
                },
            );
            let bps = pacer.client_bps(mbps, pacing, now).unwrap();
            let slice = (bps * PACING_SLICE_US / 8_000_000) as usize;
            let mut last = now;
            let mut left = frame_bytes;
            while left > 0 {
                let bytes = left.min(slice);
                last = pacer.departure(peer, bytes, bps, pacing, now).0;
                left -= bytes;
            }
            // A frame's last message: within the bound, and spread over
            // more than one slice (it is paced, not sent at once).
            assert!(
                last - now <= pacing.max_delay_us * 1_000,
                "{mbps}: {}",
                last - now
            );
            assert!(last - now >= 1_000_000, "{mbps}: {}", last - now);
            pacer.next.clear();
        }
    }

    #[test]
    fn a_client_departs_at_its_rate_and_never_later_than_the_bound() {
        let pacing = VideoPacing {
            percent: 125,
            max_delay_us: 8_000,
        };
        let mut pacer = Pacer::default();
        let stream = 1;
        let now = 10_000_000_000_u64;
        pacer.rates.insert(
            stream,
            StreamRate {
                window_start_ns: now,
                bytes: 0,
                bps: 80_000_000,
            },
        );
        // 1.25 times 80 Mbit/s: 100 Mbit/s, 12.5 kB a millisecond.
        let bps = pacer.client_bps(stream, pacing, now).unwrap();
        assert_eq!(bps, 100_000_000);
        let (a, b) = (
            SocketAddr::from(([127, 0, 0, 1], 1)),
            SocketAddr::from(([127, 0, 0, 1], 2)),
        );
        let departures: Vec<(u64, bool)> = (0..12)
            .map(|_| pacer.departure(a, 12_500, bps, pacing, now))
            .collect();
        for (index, (departure, clamped)) in departures.iter().enumerate() {
            let wanted = now + index as u64 * 1_000_000;
            // Up to 8 ms ahead at the rate, then held at the bound.
            assert_eq!(*departure, wanted.min(now + 8_000_000), "{index}");
            assert_eq!(*clamped, wanted > now + 8_000_000, "{index}");
        }
        // Each client has its own departures.
        assert_eq!(pacer.departure(b, 12_500, bps, pacing, now), (now, false));
        // Later, a client that has caught up departs at once.
        let later = now + 50_000_000;
        assert_eq!(
            pacer.departure(a, 12_500, bps, pacing, later),
            (later, false)
        );
        // Unknown streams are not paced; a slow stream is paced at 1 Mbit/s
        // at least.
        assert_eq!(pacer.client_bps(2, pacing, now), None);
        pacer.rates.insert(
            3,
            StreamRate {
                window_start_ns: now,
                bytes: 0,
                bps: 10_000,
            },
        );
        assert_eq!(pacer.client_bps(3, pacing, now), Some(MIN_CLIENT_BPS));
    }

    /// A paced video batch: each client's symbols are cut into GSO messages
    /// of about a millisecond at its rate, each departing a millisecond
    /// after the one before; without GSO, one datagram a message.
    #[test]
    fn a_paced_batch_is_cut_into_a_millisecond_of_each_clients_rate() {
        let pacing = VideoPacing {
            percent: 100,
            max_delay_us: 50_000,
        };
        for gso in [true, false] {
            let mut plan = ClientPlan::new().with_gso(gso).with_video(true);
            plan.clear();
            let now = plan.now_ns;
            // 8.8 Mbit/s: a millisecond is 1,100 bytes, one symbol; at 88
            // Mbit/s, ten.
            plan.pacer.rates.insert(
                1,
                StreamRate {
                    window_start_ns: now,
                    bytes: 0,
                    bps: 88_000_000,
                },
            );
            let peers = [
                SocketAddr::from(([127, 0, 0, 1], 1)),
                SocketAddr::from(([127, 0, 0, 1], 2)),
            ];
            for message in 0..40_usize {
                plan.lens[message] = 1_100;
                plan.streams[message] = 1;
                for peer in peers {
                    plan.sends.push(PlannedClient {
                        message: message as u16,
                        compact: None,
                        source: true,
                        part: 3,
                        peer,
                        client: plan.clients.len() as u32,
                    });
                    plan.clients.push(FastClient::new(0));
                }
            }
            plan.group_by_client();
            let clamped = plan.plan_departures(pacing, gso);
            assert_eq!(clamped, 0);
            let per_run = if gso { 10 } else { 1 };
            assert_eq!(plan.runs.len(), 80 / per_run, "gso {gso}");
            assert_eq!(plan.departures.len(), plan.runs.len());
            for (run, (&(first, count), departure)) in
                plan.runs.iter().zip(&plan.departures).enumerate()
            {
                assert_eq!(count, per_run);
                let within = (run % (40 / per_run)) as u64;
                let peer = plan.sends[first].peer;
                assert!(plan.sends[first..first + count]
                    .iter()
                    .all(|send| send.peer == peer));
                // 1,100 bytes at 88 Mbit/s: 100 µs a symbol.
                assert_eq!(
                    *departure,
                    now + within * per_run as u64 * 100_000,
                    "run {run}"
                );
            }
        }
    }

    #[test]
    fn a_part_goes_to_a_client_on_one_lane_only() {
        let client = FastClient::new(10);
        // The fast lane takes 10; the cache-fed lane then skips it.
        assert!(client.claim_fast(10));
        assert!(client.claim_fast(10), "every symbol of the part");
        assert!(!client.claim_cache(10));
        assert!(client.has_fast(10));
        // The cache-fed lane sent 11 first: the fast lane does not.
        assert!(client.claim_cache(11));
        assert!(!client.claim_fast(11));
        assert!(!client.has_fast(11));
        // A refused source symbol gives 12 back to the cache-fed lane.
        assert!(client.claim_fast(12));
        client.give_back(12);
        assert!(!client.has_fast(12));
        assert!(client.claim_cache(12));
        assert!(!client.claim_fast(12));
        // A slot reused by a part a ring later.
        let later = 10 + CLAIM_RING as u64;
        assert!(client.claim_fast(later));
        // The old part is older than the ring: cache-fed, never fast.
        assert!(client.claim_cache(10));
        assert!(!client.claim_fast(10));
        assert!(client.has_fast(later));
    }

    #[test]
    fn the_lanes_race_for_a_part_and_one_wins() {
        for part in 0..2_000_u64 {
            let client = FastClient::new(0);
            let fast_client = Arc::clone(&client);
            let fast = std::thread::spawn(move || fast_client.claim_fast(part));
            let cache = client.claim_cache(part);
            let fast = fast.join().unwrap();
            assert!(fast ^ cache, "part {part}: fast {fast} cache {cache}");
        }
    }

    #[test]
    fn upstream_repair_is_read_from_the_newest_objects() {
        let mut upstream = UpstreamRepair::default();
        assert_eq!(upstream.expected(1, 0), 0);
        // Object 5 of K = 2 brought repair ordinals 0..3.
        for esi in [0, 1, 2, 3, 4] {
            upstream.observe(1, 5, esi, 2);
        }
        assert_eq!(upstream.expected(1, 5), 3);
        // Object 6 decodes before its repair arrives: 5's count holds.
        upstream.observe(1, 6, 0, 2);
        assert_eq!(upstream.expected(1, 6), 3);
        upstream.observe(1, 6, 2, 2);
        assert_eq!(upstream.expected(1, 6), 3);
        assert_eq!(upstream.expected(2, 6), 0);
    }
}
