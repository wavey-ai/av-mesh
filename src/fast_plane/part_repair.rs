//! Repair of a part on every hop of the fast plane, sized to the link
//! (4k-repair, after the transport bench of 24 Nov: RaptorQ with repair
//! sized to the loss plus a re-request was the best long-haul transport;
//! a fixed repair fails closed on bursts and outages, and a whole-part
//! re-request costs a 4K frame and a fixed 50 ms).
//!
//! Every hop forwards each symbol on arrival, as before. Behind the
//! forward, three things repair a part that a link damaged:
//!
//! - **Re-request (child side, [`Requester`], in each class's tee).** A part
//!   of several symbols (any stream: by size, never by media kind)
//!   that is still short of symbols once its parent has moved on (a symbol
//!   of a later part came from it, plus a few ms), or that has had no symbol
//!   for a few ms, is asked of its parent with `RLQ1`: "N more symbols of
//!   part X", N being what it is short of (K + 2 less what it holds), grown
//!   by the lane's measured loss. A part of which no symbol came (an
//!   outage) is found by the gap in a stream's part numbers and asked for
//!   whole (N = 0: the parent sends K + 2 and the margin). Unanswered, the
//!   ask is repeated after the link's round trip (measured: every answer
//!   echoes the ask's time, and an idle link is probed every 500 ms) times
//!   1.25 plus 3 ms, growing by half each time, to both parents from the
//!   second ask, until the part's deadline. The cache mesh's whole-part
//!   hole request (`edge_holes`) stays as the last resort behind it.
//! - **Answer (parent side, [`PartRepair`]).** A node with children keeps
//!   each part of several symbols its tees decoded (the exact envelope, bounded: the last
//!   [`STORE_PARTS`] parts and [`STORE_BYTES`]) and answers an ask with
//!   fresh repair symbols of the same RaptorQ object: ordinals from this
//!   node's window ([`window_for`]), disjoint from the contributor's
//!   (below 65,536) and, by node, from other forwarders', so a receiver can
//!   mix them; packet sequences continue the object's own (base + ESI). An
//!   ask for a part not decoded here yet waits (bounded) and is answered as
//!   it is decoded. The answer is `RLA1` (status, the echoed time) and the
//!   symbols, from the socket the child's symbols leave from. Encoding is
//!   on the node's repair thread, never on a class thread or the tee.
//! - **Link repair (parent side, `--relay-link-repair`).** Each child
//!   measures the loss of the lane from this node against the ledger
//!   (`RLL1`) that this node sends it (`loss_report`), and reports it
//!   (`RLR1`); when a part is decoded here, each child whose link loses
//!   gets [`link_repair`] fresh symbols of it, sent after the forwarded
//!   ones, from the same window.
//!
//! Wire formats, big-endian, 48 bytes each:
//! - `RLQ1`, version 1, flags (1: probe), wanted symbols (u16; 0 with
//!   flags 0: the parent decides, a whole part), the ask's time on the
//!   child's clock (u64 µs), the part ([`PartKey`]: stream key, epoch,
//!   object, three u64), symbols held (u32), K (u16, 0 unknown), two zero
//!   bytes.
//! - `RLA1`, version 1, status ([`AnswerStatus`]), symbols that follow
//!   (u16), the echoed time (u64), the part (three u64), the answering
//!   node's window (u32: its first repair ordinal), 8 zero bytes.

use super::view::RlsView;
use super::{sys, TrafficClass};
use av_mesh::media_clock::MediaClocks;
use media_object::ObjectKey;
use raptorq::{ObjectTransmissionInformation, SourceBlockEncoder, SourceBlockEncodingPlan};
use raptorq_datagram_fec::{
    DatagramFecHeader, MediaDatagramRole, MediaDeadline, MediaObjectKind, MediaPathIntent,
    RaptorQBlockProfile, HEADER_LEN,
};
use relay_session::{encode_datagram, RelayDatagram, RelayLimits, SubscriptionId, TopologyGeneration};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tracing::warn;
use xxhash_rust::xxh3::xxh3_64;

pub const REQUEST_MAGIC: [u8; 4] = *b"RLQ1";
pub const ANSWER_MAGIC: [u8; 4] = *b"RLA1";
const VERSION: u8 = 1;
pub const REQUEST_LEN: usize = 48;
pub const ANSWER_LEN: usize = 48;
const FLAG_PROBE: u8 = 1;

/// A part's repair ordinals at a forwarder start at `WINDOW_BYTES * (1 +
/// slot)`: the contributor's (and an edge's client top-up, from 256) stay
/// below the first window.
pub const WINDOW: u32 = 1 << 16;
/// Forwarder windows: slots 0..WINDOW_SLOTS, all inside RaptorQ's 24-bit
/// symbol id space with room for K up to 56,403.
pub const WINDOW_SLOTS: u32 = 240;
/// Parts that a node keeps for answers, at most, and their bytes.
pub const STORE_PARTS: usize = 128;
pub const STORE_BYTES: usize = 160 << 20;
/// Asks waiting for a part not decoded here yet, at most, and how long one
/// waits.
const PENDING_ASKS: usize = 256;
const PENDING_FOR: Duration = Duration::from_secs(1);
/// Jobs queued for the repair thread before an ask is refused (`busy`).
const JOB_QUEUE: usize = 512;
/// Symbols one answer carries at most.
pub const MAX_ANSWER_SYMBOLS: u32 = 4_096;
/// The residual that repair is sized for: at most this share of parts
/// undecodable from one link's symbols at the planned loss.
pub const TARGET_PART_FAILURE: f64 = 1e-4;
/// The loss a plan takes is the measured one times this, plus
/// [`PLAN_LOSS_FLOOR`] (the client lane's margin; the bench's 1.5 x).
pub const PLAN_LOSS_FACTOR: f64 = 1.5;
pub const PLAN_LOSS_FLOOR: f64 = 0.001;
/// Link repair never exceeds this share of K (and at least 16).
pub const LINK_REPAIR_CAP_PERCENT: u32 = 30;
/// A child's loss report counts for this long.
const CHILD_LOSS_TTL: Duration = Duration::from_secs(3);

/// Which part an ask names: the stream (tenant, stream and track, hashed as
/// every node hashes them), its epoch and the part (the object number).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize)]
pub struct PartKey {
    pub stream: u64,
    pub epoch: u64,
    pub object: u64,
}

impl PartKey {
    pub fn of(view: &RlsView<'_>) -> Self {
        Self {
            stream: stream_key(view.tenant, view.stream, view.track),
            epoch: view.epoch,
            object: view.object,
        }
    }
}

/// The stream key of [`PartKey`]: the media clocks' stream hash and the
/// track's.
pub fn stream_key(tenant: &str, stream: &str, track: &str) -> u64 {
    MediaClocks::key(tenant, stream).hash() ^ xxh3_64(track.as_bytes()).rotate_left(17)
}

/// An `RLQ1` ask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request {
    pub part: PartKey,
    /// Symbols wanted; 0 (not a probe): the part whole.
    pub wanted: u16,
    pub probe: bool,
    pub sent_us: u64,
    pub held: u32,
    pub k: u16,
}

impl Request {
    pub fn encode(&self) -> [u8; REQUEST_LEN] {
        let mut out = [0_u8; REQUEST_LEN];
        out[..4].copy_from_slice(&REQUEST_MAGIC);
        out[4] = VERSION;
        out[5] = if self.probe { FLAG_PROBE } else { 0 };
        out[6..8].copy_from_slice(&self.wanted.to_be_bytes());
        out[8..16].copy_from_slice(&self.sent_us.to_be_bytes());
        out[16..24].copy_from_slice(&self.part.stream.to_be_bytes());
        out[24..32].copy_from_slice(&self.part.epoch.to_be_bytes());
        out[32..40].copy_from_slice(&self.part.object.to_be_bytes());
        out[40..44].copy_from_slice(&self.held.to_be_bytes());
        out[44..46].copy_from_slice(&self.k.to_be_bytes());
        out
    }

    pub fn decode(datagram: &[u8]) -> Option<Self> {
        if datagram.len() != REQUEST_LEN
            || datagram[..4] != REQUEST_MAGIC
            || datagram[4] != VERSION
        {
            return None;
        }
        let u64_at = |at: usize| u64::from_be_bytes(datagram[at..at + 8].try_into().unwrap());
        Some(Self {
            probe: datagram[5] & FLAG_PROBE != 0,
            wanted: u16::from_be_bytes([datagram[6], datagram[7]]),
            sent_us: u64_at(8),
            part: PartKey {
                stream: u64_at(16),
                epoch: u64_at(24),
                object: u64_at(32),
            },
            held: u32::from_be_bytes(datagram[40..44].try_into().unwrap()),
            k: u16::from_be_bytes([datagram[44], datagram[45]]),
        })
    }
}

/// What an `RLA1` says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AnswerStatus {
    /// `count` symbols follow.
    Sending = 0,
    /// The part is not here and was not decoded within the wait.
    Missing = 1,
    /// The answer to a probe.
    Probe = 2,
    /// The repair thread is behind: ask again later, or elsewhere.
    Busy = 3,
    /// Not decoded here yet: answered when it is.
    Waiting = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Answer {
    pub part: PartKey,
    pub status: AnswerStatus,
    pub count: u16,
    pub echo_us: u64,
    /// The first repair ordinal of the answering node's symbols (its link
    /// repair and answers): a child counts its parent's link repair by it.
    pub window: u32,
}

impl Answer {
    pub fn encode(&self) -> [u8; ANSWER_LEN] {
        let mut out = [0_u8; ANSWER_LEN];
        out[..4].copy_from_slice(&ANSWER_MAGIC);
        out[4] = VERSION;
        out[5] = self.status as u8;
        out[6..8].copy_from_slice(&self.count.to_be_bytes());
        out[8..16].copy_from_slice(&self.echo_us.to_be_bytes());
        out[16..24].copy_from_slice(&self.part.stream.to_be_bytes());
        out[24..32].copy_from_slice(&self.part.epoch.to_be_bytes());
        out[32..40].copy_from_slice(&self.part.object.to_be_bytes());
        out[40..44].copy_from_slice(&self.window.to_be_bytes());
        out
    }

    pub fn decode(datagram: &[u8]) -> Option<Self> {
        if datagram.len() != ANSWER_LEN || datagram[..4] != ANSWER_MAGIC || datagram[4] != VERSION
        {
            return None;
        }
        let status = match datagram[5] {
            0 => AnswerStatus::Sending,
            1 => AnswerStatus::Missing,
            2 => AnswerStatus::Probe,
            3 => AnswerStatus::Busy,
            4 => AnswerStatus::Waiting,
            _ => return None,
        };
        let u64_at = |at: usize| u64::from_be_bytes(datagram[at..at + 8].try_into().unwrap());
        Some(Self {
            status,
            count: u16::from_be_bytes([datagram[6], datagram[7]]),
            echo_us: u64_at(8),
            part: PartKey {
                stream: u64_at(16),
                epoch: u64_at(24),
                object: u64_at(32),
            },
            window: u32::from_be_bytes(datagram[40..44].try_into().unwrap()),
        })
    }
}

/// Whether a datagram is an `RLQ1` or `RLA1` (not media).
pub fn is_part_repair_control(datagram: &[u8]) -> bool {
    datagram.len() == REQUEST_LEN
        && (datagram[..4] == REQUEST_MAGIC || datagram[..4] == ANSWER_MAGIC)
}

// ------------------------------------------------------------------ sizing

/// Smallest `r` such that a part of `k` source symbols sent with `r` repair
/// symbols is undecodable with probability at most `target` when each
/// datagram is lost with probability `p` (one symbol more than `k` counts
/// as needed for a block of more than one symbol), at most `limit`.
pub fn repair_for_loss(k: u32, p: f64, target: f64, limit: u32) -> u32 {
    if !(p > 0.0) || k == 0 {
        return 0;
    }
    let p = p.min(0.5);
    let margin = u32::from(k > 1);
    let (ln_p, ln_q) = (p.ln(), (1.0 - p).ln());
    // Start near the mean loss: every r below it fails.
    let mean = (f64::from(k) * p / (1.0 - p)).floor() as u32;
    let mut r = margin.max(mean.saturating_sub(1));
    while r < limit {
        let n = k + r;
        let allowed = r - margin;
        // P(lost <= allowed), terms in log space.
        let mut ln_term = f64::from(n) * ln_q;
        let mut delivered = ln_term.exp();
        for j in 0..allowed {
            ln_term += (f64::from(n - j) / f64::from(j + 1)).ln() + ln_p - ln_q;
            delivered += ln_term.exp();
        }
        if 1.0 - delivered <= target {
            return r;
        }
        r += 1;
    }
    limit
}

/// The loss a plan takes for a measured `loss_ppm`.
pub fn planned_loss(loss_ppm: u32) -> f64 {
    (f64::from(loss_ppm) / 1e6 * PLAN_LOSS_FACTOR + PLAN_LOSS_FLOOR).min(0.5)
}

/// The repair a part of `k` symbols gets on a link that loses `loss_ppm`:
/// none on a clean link (the re-request covers what a clean link drops),
/// else sized to [`planned_loss`] at [`TARGET_PART_FAILURE`], at most
/// [`LINK_REPAIR_CAP_PERCENT`] of K (16 at the least).
pub fn link_repair(k: u32, loss_ppm: u32) -> u32 {
    if loss_ppm == 0 || k <= 1 {
        return 0;
    }
    let cap = (k * LINK_REPAIR_CAP_PERCENT / 100).max(16);
    repair_for_loss(k, planned_loss(loss_ppm), TARGET_PART_FAILURE, cap)
}

/// The ordinal window of a forwarder: by its node id, so that the two
/// parents of a child rarely share one (and when they do, the same ordinal
/// is the same symbol: only the duplicate is wasted).
pub fn window_for(node_id: &str) -> u32 {
    let slot = (xxh3_64(node_id.as_bytes()) % u64::from(WINDOW_SLOTS)) as u32;
    WINDOW * (1 + slot)
}

// ------------------------------------------------------------------ parent

/// Something a datagram can leave from.
pub trait SendSocket: Send + Sync {
    fn send_fd(&self) -> RawFd;
}

impl SendSocket for tokio::net::UdpSocket {
    fn send_fd(&self) -> RawFd {
        self.as_raw_fd()
    }
}

impl SendSocket for std::net::UdpSocket {
    fn send_fd(&self) -> RawFd {
        self.as_raw_fd()
    }
}

/// The lease children's socket of a class.
pub struct LeaseSocket(pub Arc<super::lease::LeaseChildren>, pub TrafficClass);

impl SendSocket for LeaseSocket {
    fn send_fd(&self) -> RawFd {
        self.0.fd(self.1)
    }
}

/// A part this node decoded, kept to answer asks with.
pub struct StoredPart {
    pub key: PartKey,
    object_key: ObjectKey,
    kind: MediaObjectKind,
    generation: TopologyGeneration,
    subscription: SubscriptionId,
    deadline: MediaDeadline,
    profile: RaptorQBlockProfile,
    /// The object's packet sequence less its ESI.
    seq_base: u32,
    envelope: Vec<u8>,
    /// Made at the first answer (the RaptorQ intermediate symbols, from a
    /// cached plan of its K), then kept with the next ordinal to send: its
    /// cursor makes every answer's symbols new.
    encoder: StdMutex<Option<(SourceBlockEncoder, u32)>>,
}

/// RaptorQ encoding plans by K, for the repair thread (as av-contrib's
/// `PlanCache`): `raptorq`'s own cache is one FIFO of 64 shared by the
/// process, and a plan of a 4K part's K costs milliseconds to tens of
/// milliseconds to generate. With the contributor's plan grid (16 K values
/// an octave) a 4K stream's parts need a few dozen plans.
#[derive(Default)]
pub struct Plans {
    plans: HashMap<u16, Arc<SourceBlockEncodingPlan>>,
    order: VecDeque<u16>,
    symbols: usize,
    pub generated: u64,
}

/// Plans kept at most, and their source symbols in all (a plan of K
/// symbols is roughly 0.75 K KiB: about 48 MiB at this bound).
const PLAN_CAPACITY: usize = 96;
const PLAN_SYMBOL_BUDGET: usize = 65_536;

impl Plans {
    pub fn plan(&mut self, k: u16) -> Arc<SourceBlockEncodingPlan> {
        if let Some(plan) = self.plans.get(&k) {
            return Arc::clone(plan);
        }
        let plan = Arc::new(SourceBlockEncodingPlan::generate(k));
        self.generated += 1;
        while !self.order.is_empty()
            && (self.order.len() >= PLAN_CAPACITY
                || self.symbols + usize::from(k) > PLAN_SYMBOL_BUDGET)
        {
            if let Some(old) = self.order.pop_front() {
                self.plans.remove(&old);
                self.symbols -= usize::from(old);
            }
        }
        self.plans.insert(k, Arc::clone(&plan));
        self.order.push_back(k);
        self.symbols += usize::from(k);
        plan
    }
}

impl StoredPart {
    /// From the view of one of its symbols and its decoded envelope.
    pub fn new(
        view: &RlsView<'_>,
        object_key: ObjectKey,
        envelope: Vec<u8>,
    ) -> Option<Self> {
        let profile = RaptorQBlockProfile::from_datagram(view.fec).ok()?;
        if profile.transfer_length() as usize != envelope.len() {
            return None;
        }
        Some(Self {
            key: PartKey::of(view),
            object_key,
            kind: view.kind,
            generation: TopologyGeneration::new(view.generation).ok()?,
            subscription: SubscriptionId::new(view.subscription_id).ok()?,
            deadline: MediaDeadline::from_micros(view.deadline_us),
            profile,
            seq_base: view.packet_sequence.wrapping_sub(view.esi),
            envelope,
            encoder: StdMutex::new(None),
        })
    }

    pub fn source_symbols(&self) -> u32 {
        u32::from(self.profile.source_symbols())
    }

    /// `count` new repair symbols as RLS1 datagrams, from `window` on.
    pub fn repair(
        &self,
        window: u32,
        count: u32,
        plans: &mut Plans,
        limits: RelayLimits,
    ) -> Vec<Vec<u8>> {
        let mut state = self.encoder.lock().unwrap_or_else(|e| e.into_inner());
        let (k, size) = (self.profile.source_symbols(), self.profile.symbol_size());
        if state.is_none() {
            let padded = usize::from(k) * usize::from(size);
            if self.envelope.len() > padded {
                return Vec::new();
            }
            let mut block = Vec::with_capacity(padded);
            block.extend_from_slice(&self.envelope);
            block.resize(padded, 0);
            let oti = ObjectTransmissionInformation::new(
                u64::from(self.profile.transfer_length()),
                size,
                1,
                1,
                1,
            );
            let plan = plans.plan(k);
            *state = Some((
                SourceBlockEncoder::with_encoding_plan(0, &oti, &block, &plan),
                0,
            ));
        }
        let Some((encoder, cursor)) = state.as_mut() else {
            return Vec::new();
        };
        let count = count.clamp(1, MAX_ANSWER_SYMBOLS);
        let packets = encoder.repair_packets(window + *cursor, count);
        *cursor += count;
        packets
            .into_iter()
            .filter_map(|packet| {
                let esi = packet.payload_id().encoding_symbol_id();
                let serialized = packet.serialize();
                let header = DatagramFecHeader::raptorq(
                    self.profile.block_id(),
                    self.profile.transfer_length(),
                    self.seq_base.wrapping_add(esi),
                    k,
                    size,
                    &serialized,
                )
                .ok()?;
                let mut fec_datagram = vec![0_u8; HEADER_LEN];
                header.encode(&mut fec_datagram).ok()?;
                fec_datagram.extend_from_slice(&serialized);
                let datagram = RelayDatagram::new(
                    self.generation,
                    self.subscription,
                    self.object_key.clone(),
                    self.kind,
                    MediaDatagramRole::Repair,
                    MediaPathIntent::SecondaryRepair,
                    self.deadline,
                    fec_datagram,
                    limits,
                )
                .ok()?;
                encode_datagram(&datagram, limits).ok()
            })
            .collect()
    }
}

struct PendingAsk {
    via: Arc<dyn SendSocket>,
    to: SocketAddr,
    request: Request,
    at: Instant,
}

#[derive(Default)]
struct Store {
    parts: VecDeque<Arc<StoredPart>>,
    bytes: usize,
    pending: Vec<PendingAsk>,
}

enum Job {
    Answer {
        via: Arc<dyn SendSocket>,
        to: SocketAddr,
        request: Request,
        part: Arc<StoredPart>,
    },
    Link {
        part: Arc<StoredPart>,
        sends: Vec<(Arc<dyn SendSocket>, SocketAddr, u32)>,
    },
}

/// The counters of both sides, for `GET /api/node` (fast_plane.part_repair).
#[derive(Debug, Default)]
pub struct PartRepairStats {
    // Child side.
    pub requests: AtomicU64,
    pub retries: AtomicU64,
    pub symbols_asked: AtomicU64,
    pub whole_parts_asked: AtomicU64,
    pub gap_parts: AtomicU64,
    pub recovered_parts: AtomicU64,
    pub gave_up_parts: AtomicU64,
    pub probes: AtomicU64,
    pub answers_heard: AtomicU64,
    pub missing_heard: AtomicU64,
    pub srtt_us_max: AtomicU64,
    /// Longest wait from a part's first ask to its decode.
    pub repair_wait_max_us: AtomicU64,
    // Parent side.
    pub asks: AtomicU64,
    pub answers: AtomicU64,
    pub answer_symbols: AtomicU64,
    pub answer_missing: AtomicU64,
    pub answer_waiting: AtomicU64,
    pub busy: AtomicU64,
    pub stored_parts: AtomicU64,
    pub encode_us: AtomicU64,
    pub encode_us_max: AtomicU64,
    pub link_repair_parts: AtomicU64,
    pub link_repair_symbols: AtomicU64,
    pub child_loss_reports: AtomicU64,
    pub ledgers: AtomicU64,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, Default, PartialEq, Eq)]
#[serde(default)]
pub struct PartRepairSnapshot {
    pub requests: u64,
    pub retries: u64,
    pub symbols_asked: u64,
    pub whole_parts_asked: u64,
    pub gap_parts: u64,
    pub recovered_parts: u64,
    pub gave_up_parts: u64,
    pub probes: u64,
    pub answers_heard: u64,
    pub missing_heard: u64,
    pub srtt_us_max: u64,
    pub repair_wait_max_us: u64,
    pub asks: u64,
    pub answers: u64,
    pub answer_symbols: u64,
    pub answer_missing: u64,
    pub answer_waiting: u64,
    pub busy: u64,
    pub stored_parts: u64,
    pub encode_us: u64,
    pub encode_us_max: u64,
    pub link_repair_parts: u64,
    pub link_repair_symbols: u64,
    pub child_loss_reports: u64,
    pub ledgers: u64,
}

impl PartRepairStats {
    pub fn snapshot(&self) -> PartRepairSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        PartRepairSnapshot {
            requests: load(&self.requests),
            retries: load(&self.retries),
            symbols_asked: load(&self.symbols_asked),
            whole_parts_asked: load(&self.whole_parts_asked),
            gap_parts: load(&self.gap_parts),
            recovered_parts: load(&self.recovered_parts),
            gave_up_parts: load(&self.gave_up_parts),
            probes: load(&self.probes),
            answers_heard: load(&self.answers_heard),
            missing_heard: load(&self.missing_heard),
            srtt_us_max: load(&self.srtt_us_max),
            repair_wait_max_us: load(&self.repair_wait_max_us),
            asks: load(&self.asks),
            answers: load(&self.answers),
            answer_symbols: load(&self.answer_symbols),
            answer_missing: load(&self.answer_missing),
            answer_waiting: load(&self.answer_waiting),
            busy: load(&self.busy),
            stored_parts: load(&self.stored_parts),
            encode_us: load(&self.encode_us),
            encode_us_max: load(&self.encode_us_max),
            link_repair_parts: load(&self.link_repair_parts),
            link_repair_symbols: load(&self.link_repair_symbols),
            child_loss_reports: load(&self.child_loss_reports),
            ledgers: load(&self.ledgers),
        }
    }
}

/// How a node takes part.
#[derive(Debug, Clone)]
pub struct PartRepairConfig {
    /// `--relay-part-repair`: ask parents for what a part lacks, and keep
    /// decoded parts to answer children.
    pub requests: bool,
    /// `--relay-link-repair`: ledgers to children, and repair sized to each
    /// child's measured loss when a part is decoded here.
    pub link_repair: bool,
    /// `--relay-part-repair-rtt-ms`: the round trip assumed before one is
    /// measured.
    pub initial_rtt_us: u64,
    /// The first repair ordinal of this node's answers.
    pub window: u32,
    /// The media deadline: no ask after it.
    pub deadline_us: u64,
}

impl Default for PartRepairConfig {
    fn default() -> Self {
        Self {
            requests: true,
            link_repair: true,
            initial_rtt_us: 50_000,
            window: WINDOW,
            deadline_us: 1_000_000,
        }
    }
}

/// A node's part repair: its kept parts, the asks waiting for them, its
/// children's loss, and the repair thread.
pub struct PartRepair {
    pub config: PartRepairConfig,
    pub stats: Arc<PartRepairStats>,
    store: StdMutex<Store>,
    jobs: SyncSender<Job>,
    child_loss: StdMutex<HashMap<SocketAddr, (u32, Instant)>>,
    /// Repair datagrams sent to each child here (its ledger counts them).
    extra_sent: Arc<StdMutex<HashMap<SocketAddr, u64>>>,
    /// Tests: the parts this node asked for.
    #[cfg(test)]
    asked: StdMutex<Vec<u64>>,
}

impl std::fmt::Debug for PartRepair {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PartRepair")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl PartRepair {
    /// The node's part repair, with its repair thread running.
    pub fn start(config: PartRepairConfig, limits: RelayLimits) -> Arc<Self> {
        let (jobs, queue) = sync_channel(JOB_QUEUE);
        let stats = Arc::new(PartRepairStats::default());
        let extra_sent = Arc::new(StdMutex::new(HashMap::new()));
        let repair = Arc::new(Self {
            config: config.clone(),
            stats: Arc::clone(&stats),
            store: StdMutex::new(Store::default()),
            jobs,
            child_loss: StdMutex::new(HashMap::new()),
            extra_sent: Arc::clone(&extra_sent),
            #[cfg(test)]
            asked: StdMutex::new(Vec::new()),
        });
        let window = config.window;
        if let Err(error) = std::thread::Builder::new()
            .name("needletail-repair".into())
            .spawn(move || run_repair_thread(queue, window, limits, stats, extra_sent))
        {
            warn!(error = %error, "failed to start the part repair thread: no answers");
        }
        repair
    }

    /// Keep a part decoded here; answer the asks that waited for it, and
    /// send each lossy child its link repair (`children`: where each child
    /// of the part's stream takes the part's class, and the socket to it).
    pub fn keep(
        &self,
        part: StoredPart,
        children: impl FnOnce(&dyn Fn(SocketAddr) -> u32) -> Vec<(Arc<dyn SendSocket>, SocketAddr, u32)>,
    ) {
        let part = Arc::new(part);
        let waiting = {
            let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
            store.bytes += part.envelope.len();
            store.parts.push_back(Arc::clone(&part));
            while store.parts.len() > STORE_PARTS || store.bytes > STORE_BYTES {
                let Some(old) = store.parts.pop_front() else {
                    break;
                };
                store.bytes -= old.envelope.len();
            }
            let now = Instant::now();
            let mut waiting = Vec::new();
            store.pending.retain_mut(|ask| {
                if now.duration_since(ask.at) > PENDING_FOR {
                    return false;
                }
                if ask.request.part == part.key {
                    waiting.push((Arc::clone(&ask.via), ask.to, ask.request));
                    return false;
                }
                true
            });
            waiting
        };
        self.stats.stored_parts.fetch_add(1, Ordering::Relaxed);
        for (via, to, request) in waiting {
            self.queue(Job::Answer {
                via,
                to,
                request,
                part: Arc::clone(&part),
            });
        }
        if !self.config.link_repair {
            return;
        }
        let k = part.source_symbols();
        let loss = |child: SocketAddr| self.child_loss(child);
        let sends: Vec<_> = children(&loss)
            .into_iter()
            .filter_map(|(via, to, loss_ppm)| {
                let count = link_repair(k, loss_ppm);
                (count > 0).then_some((via, to, count))
            })
            .collect();
        if !sends.is_empty() {
            self.queue(Job::Link { part, sends });
        }
    }

    /// A child's measured loss of the lane from this node (ppm), while its
    /// report is fresh.
    pub fn child_loss(&self, child: SocketAddr) -> u32 {
        self.child_loss
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&child)
            .filter(|(_, at)| at.elapsed() < CHILD_LOSS_TTL)
            .map_or(0, |(ppm, _)| *ppm)
    }

    /// Tests: the parts (object numbers) this node asked a parent for.
    #[cfg(test)]
    pub fn asked_parts(&self) -> Vec<u64> {
        self.asked.lock().unwrap().clone()
    }

    /// Repair datagrams this node's repair thread sent `child` so far.
    pub fn extra_sent(&self, child: SocketAddr) -> u64 {
        self.extra_sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&child)
            .copied()
            .unwrap_or(0)
    }

    fn queue(&self, job: Job) {
        match self.jobs.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(job) | TrySendError::Disconnected(job)) => {
                self.stats.busy.fetch_add(1, Ordering::Relaxed);
                if let Job::Answer { via, to, request, .. } = job {
                    answer(&*via, to, &request, AnswerStatus::Busy, 0, self.config.window);
                }
            }
        }
    }

    /// A datagram that a child sent to the socket `via` (where its symbols
    /// leave from): an ask (`RLQ1`) or a loss report (`RLR1`). `false` for
    /// anything else (the caller reads it on).
    pub fn handle_child_datagram(
        &self,
        via: &Arc<dyn SendSocket>,
        from: SocketAddr,
        datagram: &[u8],
    ) -> bool {
        if let Some((ppm, _)) = super::loss_report::decode_loss_report(datagram) {
            self.stats.child_loss_reports.fetch_add(1, Ordering::Relaxed);
            self.child_loss
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(from, (ppm, Instant::now()));
            return true;
        }
        let Some(request) = Request::decode(datagram) else {
            return false;
        };
        if request.probe {
            answer(&**via, from, &request, AnswerStatus::Probe, 0, self.config.window);
            return true;
        }
        self.stats.asks.fetch_add(1, Ordering::Relaxed);
        if !self.config.requests {
            answer(&**via, from, &request, AnswerStatus::Missing, 0, self.config.window);
            self.stats.answer_missing.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let found = {
            let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
            let found = store
                .parts
                .iter()
                .rev()
                .find(|part| part.key == request.part)
                .cloned();
            if found.is_none() {
                // Answered when the part is decoded here: one ask per child
                // and part (the newest), the oldest dropped when full.
                store
                    .pending
                    .retain(|ask| !(ask.to == from && ask.request.part == request.part));
                if store.pending.len() >= PENDING_ASKS {
                    store.pending.remove(0);
                }
                store.pending.push(PendingAsk {
                    via: Arc::clone(via),
                    to: from,
                    request,
                    at: Instant::now(),
                });
            }
            found
        };
        match found {
            Some(part) => self.queue(Job::Answer {
                via: Arc::clone(via),
                to: from,
                request,
                part,
            }),
            None => {
                self.stats.answer_waiting.fetch_add(1, Ordering::Relaxed);
                answer(&**via, from, &request, AnswerStatus::Waiting, 0, self.config.window);
            }
        }
        true
    }
}

fn answer(
    via: &dyn SendSocket,
    to: SocketAddr,
    request: &Request,
    status: AnswerStatus,
    count: u16,
    window: u32,
) {
    let bytes = Answer {
        part: request.part,
        status,
        count,
        echo_us: request.sent_us,
        window,
    }
    .encode();
    let _ = sys::send_batch(via.send_fd(), 1, |_| (&bytes[..], to));
}

/// Send `datagrams` to `to`, waiting briefly on a full socket (this is the
/// repair thread, not a class thread).
fn send_all(via: &dyn SendSocket, to: SocketAddr, datagrams: &[Vec<u8>]) -> usize {
    let mut sent = 0;
    let mut tries = 0;
    while sent < datagrams.len() && tries < 50 {
        let more = sys::send_batch(via.send_fd(), datagrams.len() - sent, |index| {
            (&datagrams[sent + index][..], to)
        })
        .unwrap_or(0);
        sent += more;
        if more == 0 {
            tries += 1;
            std::thread::sleep(Duration::from_micros(200));
        }
    }
    sent
}

fn run_repair_thread(
    queue: Receiver<Job>,
    window: u32,
    limits: RelayLimits,
    stats: Arc<PartRepairStats>,
    extra_sent: Arc<StdMutex<HashMap<SocketAddr, u64>>>,
) {
    let mut plans = Plans::default();
    let count_extra = |to: SocketAddr, count: usize| {
        *extra_sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(to)
            .or_default() += count as u64;
    };
    while let Ok(job) = queue.recv() {
        match job {
            Job::Answer {
                via,
                to,
                request,
                part,
            } => {
                let k = part.source_symbols();
                // Whole (0): K and the margin of the link's loss.
                let wanted = if request.wanted == 0 {
                    k + 2 + k / 50
                } else {
                    u32::from(request.wanted)
                };
                let started = Instant::now();
                let datagrams = part.repair(window, wanted.min(MAX_ANSWER_SYMBOLS), &mut plans, limits);
                let took = started.elapsed().as_micros() as u64;
                stats.encode_us.fetch_add(took, Ordering::Relaxed);
                stats.encode_us_max.fetch_max(took, Ordering::Relaxed);
                if datagrams.is_empty() {
                    stats.answer_missing.fetch_add(1, Ordering::Relaxed);
                    answer(&*via, to, &request, AnswerStatus::Missing, 0, window);
                    continue;
                }
                answer(
                    &*via,
                    to,
                    &request,
                    AnswerStatus::Sending,
                    u16::try_from(datagrams.len()).unwrap_or(u16::MAX),
                    window,
                );
                let sent = send_all(&*via, to, &datagrams);
                count_extra(to, sent);
                stats.answers.fetch_add(1, Ordering::Relaxed);
                stats.answer_symbols.fetch_add(sent as u64, Ordering::Relaxed);
            }
            Job::Link { part, sends } => {
                let most = sends.iter().map(|(_, _, count)| *count).max().unwrap_or(0);
                if most == 0 {
                    continue;
                }
                let started = Instant::now();
                let datagrams = part.repair(window, most, &mut plans, limits);
                let took = started.elapsed().as_micros() as u64;
                stats.encode_us.fetch_add(took, Ordering::Relaxed);
                stats.encode_us_max.fetch_max(took, Ordering::Relaxed);
                for (via, to, count) in sends {
                    let count = (count as usize).min(datagrams.len());
                    let sent = send_all(&*via, to, &datagrams[..count]);
                    count_extra(to, sent);
                    stats.link_repair_symbols.fetch_add(sent as u64, Ordering::Relaxed);
                }
                stats.link_repair_parts.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

// ------------------------------------------------------------------ child

/// Parts under repair that a requester follows, at most.
const OPEN_PARTS: usize = 256;
/// Streams whose part numbers it follows for gaps.
const STREAMS: usize = 64;
/// A run of missing part numbers longer than this is a new epoch or a
/// stream that started again, not an outage to ask for.
const MAX_GAP_PARTS: u64 = 64;
/// The parent has moved on to a later part: this long more for its
/// datagrams still in flight (another parent's, reordered).
const MOVED_ON_GRACE_US: u64 = 3_000;
/// No symbol of a part for this long: it is as complete as it will get.
const IDLE_US: u64 = 6_000;
/// A parent that sends parts of several symbols is probed this often for
/// the round trip.
const PROBE_EVERY_US: u64 = 500_000;
/// Asks of one part at most.
const MAX_ATTEMPTS: u8 = 10;

#[derive(Debug, Clone, Copy)]
struct OpenPart {
    key: PartKey,
    k: u16,
    held: u32,
    first_us: u64,
    last_us: u64,
    /// The last source symbol's arrival (0: none yet).
    last_source_us: u64,
    /// Symbols of it that its parent made (ordinals in the parent's window:
    /// link repair or answers), and when the link repair this node expects
    /// of its parent had all come (0: not yet).
    regenerated: u32,
    link_done_us: u64,
    moved_on_us: u64,
    /// The parents that sent it, by lane (primary, secondary).
    parents: [Option<SocketAddr>; 2],
    attempts: u8,
    first_ask_us: u64,
    next_due_us: u64,
    /// A parent said it does not have the part.
    missing_at: [bool; 2],
}

#[derive(Debug, Clone, Copy)]
struct StreamHead {
    stream: u64,
    epoch: u64,
    newest: u64,
    lane: usize,
    peer: SocketAddr,
}

#[derive(Debug, Clone, Copy)]
struct Parent {
    addr: SocketAddr,
    lane: usize,
    srtt_us: u64,
    measured: bool,
    /// Ask to the first symbols of its answer, beyond the round trip (the
    /// parent's encode).
    answer_extra_us: u64,
    last_media_us: u64,
    last_probe_us: u64,
    /// Its window, from its first answer (`None`: not heard yet).
    window: Option<u32>,
}

/// The child side, owned by a class's tee.
pub struct Requester {
    repair: Arc<PartRepair>,
    /// The class's receive socket of each lane, which asks leave from.
    sockets: [Option<Arc<dyn SendSocket>>; 2],
    /// The measured loss of each lane (ppm), from the class thread.
    lanes: Arc<super::relay::FastPlaneStats>,
    /// The class lane whose loss the asks read.
    class: TrafficClass,
    open: Vec<OpenPart>,
    heads: Vec<StreamHead>,
    parents: Vec<Parent>,
    /// From a part's last source to its decode, for parts that decoded with
    /// no ask (µs, a running mean): the repair that is still coming.
    repair_tail_us: u64,
}

impl Requester {
    pub fn new(
        repair: Arc<PartRepair>,
        sockets: [Option<Arc<dyn SendSocket>>; 2],
        lanes: Arc<super::relay::FastPlaneStats>,
        class: TrafficClass,
    ) -> Self {
        Self {
            repair,
            sockets,
            lanes,
            class,
            open: Vec::with_capacity(OPEN_PARTS),
            heads: Vec::with_capacity(STREAMS),
            parents: Vec::new(),
            repair_tail_us: 0,
        }
    }

    fn stats(&self) -> &PartRepairStats {
        &self.repair.stats
    }

    fn parent(&mut self, addr: SocketAddr, lane: usize) -> &mut Parent {
        let at = match self.parents.iter().position(|parent| parent.addr == addr) {
            Some(at) => at,
            None => {
                self.parents.push(Parent {
                    addr,
                    lane,
                    srtt_us: self.repair.config.initial_rtt_us,
                    measured: false,
                    answer_extra_us: 0,
                    last_media_us: 0,
                    last_probe_us: 0,
                    window: None,
                });
                self.parents.len() - 1
            }
        };
        &mut self.parents[at]
    }

    /// A symbol of a part arrived (and was buffered: the part is not
    /// decoded yet) from `peer` on `lane`.
    pub fn observe(&mut self, view: &RlsView<'_>, peer: SocketAddr, lane: usize, now_us: u64) {
        let key = PartKey::of(view);
        self.parent(peer, lane).last_media_us = now_us;
        self.follow_head(key, lane, peer, now_us);
        let k = u32::from(view.source_symbols);
        // This parent's own symbols (its link repair and answers): not the
        // forwarded ones of a node further up.
        let window = self.parent(peer, lane).window;
        let regenerated = u32::from(window.is_some_and(|window| {
            view.esi >= k + window && view.esi < k + window + WINDOW
        }));
        let expected = self.expected_link_repair(k, lane);
        if let Some(part) = self.open.iter_mut().find(|part| part.key == key) {
            part.held += 1;
            part.k = view.source_symbols;
            part.last_us = now_us;
            if view.esi < k {
                part.last_source_us = now_us;
            }
            part.regenerated += regenerated;
            if expected > 0 && part.regenerated >= expected && part.link_done_us == 0 {
                part.link_done_us = now_us;
                // How long after its sources a part's link repair has come:
                // the first ask waits that out (every part, asked or not).
                if part.last_source_us != 0 {
                    let sample = now_us.saturating_sub(part.last_source_us).min(200_000);
                    self.repair_tail_us = (self.repair_tail_us * 7 + sample) / 8;
                }
            }
            part.parents[lane] = Some(peer);
            return;
        }
        self.open_part(OpenPart {
            key,
            k: view.source_symbols,
            held: 1,
            first_us: now_us,
            last_us: now_us,
            last_source_us: if view.esi < k { now_us } else { 0 },
            regenerated,
            link_done_us: 0,
            moved_on_us: 0,
            parents: if lane == 0 {
                [Some(peer), None]
            } else {
                [None, Some(peer)]
            },
            attempts: 0,
            first_ask_us: 0,
            next_due_us: 0,
            missing_at: [false; 2],
        });
    }

    /// A part's symbol came that completed nothing new (a part
    /// decoded already): only the parent's liveness and the head.
    pub fn observe_done(&mut self, view: &RlsView<'_>, peer: SocketAddr, lane: usize, now_us: u64) {
        let key = PartKey::of(view);
        self.parent(peer, lane).last_media_us = now_us;
        self.follow_head(key, lane, peer, now_us);
    }

    fn open_part(&mut self, part: OpenPart) {
        if self.open.len() >= OPEN_PARTS {
            // The oldest goes: it is the nearest its deadline.
            if let Some((at, _)) = self
                .open
                .iter()
                .enumerate()
                .min_by_key(|(_, part)| part.first_us)
            {
                self.open.swap_remove(at);
            }
        }
        self.open.push(part);
    }

    /// The stream's newest part: earlier open parts have their parent moved
    /// on; part numbers skipped are parts of which nothing came.
    fn follow_head(&mut self, key: PartKey, lane: usize, peer: SocketAddr, now_us: u64) {
        let head = match self.heads.iter_mut().find(|head| head.stream == key.stream) {
            Some(head) => head,
            None => {
                if self.heads.len() >= STREAMS {
                    self.heads.remove(0);
                }
                self.heads.push(StreamHead {
                    stream: key.stream,
                    epoch: key.epoch,
                    newest: key.object,
                    lane,
                    peer,
                });
                return;
            }
        };
        if head.epoch != key.epoch {
            if key.epoch > head.epoch {
                *head = StreamHead {
                    stream: key.stream,
                    epoch: key.epoch,
                    newest: key.object,
                    lane,
                    peer,
                };
            }
            return;
        }
        if key.object <= head.newest {
            return;
        }
        let (from, upto) = (head.newest + 1, key.object);
        head.newest = key.object;
        head.lane = lane;
        head.peer = peer;
        for part in &mut self.open {
            if part.key.stream == key.stream
                && part.key.epoch == key.epoch
                && part.key.object < key.object
                && part.moved_on_us == 0
            {
                part.moved_on_us = now_us;
            }
        }
        if upto - from > MAX_GAP_PARTS {
            return;
        }
        for object in from..upto {
            let gap = PartKey { object, ..key };
            if self.open.iter().any(|part| part.key == gap) {
                continue;
            }
            self.stats().gap_parts.fetch_add(1, Ordering::Relaxed);
            self.open_part(OpenPart {
                key: gap,
                k: 0,
                held: 0,
                first_us: now_us,
                last_us: now_us,
                last_source_us: 0,
                regenerated: 0,
                link_done_us: 0,
                moved_on_us: now_us,
                parents: if lane == 0 {
                    [Some(peer), None]
                } else {
                    [None, Some(peer)]
                },
                attempts: 0,
                first_ask_us: 0,
                next_due_us: 0,
                missing_at: [false; 2],
            });
        }
    }

    /// The part was decoded here.
    pub fn done(&mut self, key: PartKey, now_us: u64) {
        if let Some(at) = self.open.iter().position(|part| part.key == key) {
            let part = self.open.swap_remove(at);
            // How long after its sources a part that needed no ask kept
            // getting repair (forwarded and link repair, each made after an
            // upstream decode): the first ask waits that out.
            if part.attempts == 0 && part.last_source_us != 0 && part.link_done_us == 0 {
                let sample = now_us.saturating_sub(part.last_source_us).min(200_000);
                self.repair_tail_us = (self.repair_tail_us * 7 + sample) / 8;
            }
            if part.attempts > 0 {
                self.stats().recovered_parts.fetch_add(1, Ordering::Relaxed);
                self.stats().repair_wait_max_us.fetch_max(
                    now_us.saturating_sub(part.first_ask_us),
                    Ordering::Relaxed,
                );
            }
        }
    }

    /// An `RLA1` from `peer`.
    pub fn on_answer(&mut self, answer: &Answer, peer: SocketAddr, now_us: u64) {
        let sample = now_us.saturating_sub(answer.echo_us);
        self.stats().answers_heard.fetch_add(1, Ordering::Relaxed);
        let Some(parent) = self.parents.iter_mut().find(|parent| parent.addr == peer) else {
            return;
        };
        parent.window = Some(answer.window);
        // Samples over 10 s are clocks gone wrong, not round trips.
        if sample < 10_000_000 {
            match answer.status {
                AnswerStatus::Probe | AnswerStatus::Waiting | AnswerStatus::Missing => {
                    parent.srtt_us = if parent.measured {
                        (parent.srtt_us * 7 + sample) / 8
                    } else {
                        sample
                    };
                    parent.measured = true;
                }
                AnswerStatus::Sending => {
                    let extra = sample.saturating_sub(parent.srtt_us);
                    parent.answer_extra_us = (parent.answer_extra_us * 3 + extra) / 4;
                }
                AnswerStatus::Busy => {}
            }
        }
        let (srtt, lane) = (parent.srtt_us, parent.lane);
        self.stats().srtt_us_max.fetch_max(srtt, Ordering::Relaxed);
        if answer.status == AnswerStatus::Waiting {
            // The parent answers when it has the part: no ask again for two
            // more of its waits.
            let wait = 2 * (srtt + srtt / 4 + 3_000);
            if let Some(part) = self.open.iter_mut().find(|part| part.key == answer.part) {
                part.next_due_us = part.next_due_us.max(now_us + wait);
            }
        }
        if answer.status == AnswerStatus::Missing || answer.status == AnswerStatus::Busy {
            self.stats().missing_heard.fetch_add(1, Ordering::Relaxed);
            if let Some(part) = self.open.iter_mut().find(|part| part.key == answer.part) {
                part.missing_at[lane] = true;
                // The other parent at once, if there is one.
                if part.parents[1 - lane].is_some() && !part.missing_at[1 - lane] {
                    part.next_due_us = now_us;
                }
            }
        }
    }

    /// The link repair this node's parent on `lane` sends a part of `k`
    /// symbols: the parent sizes it from this node's reported loss (here a
    /// tenth less, for the report's lag).
    fn expected_link_repair(&self, k: u32, lane: usize) -> u32 {
        let ppm = self.lanes.lane_loss_ppm(self.class, lane);
        let expected = link_repair(k, u32::try_from(ppm).unwrap_or(u32::MAX));
        expected - expected / 10
    }

    /// How long after its last source symbol a part is first asked for:
    /// the repair still to come (`repair_tail_us`) and a quarter more, and
    /// 2 ms; at least [`IDLE_US`], at most 80 ms.
    pub fn first_wait_us(&self) -> u64 {
        (self.repair_tail_us + self.repair_tail_us / 4 + 2_000).clamp(IDLE_US, 80_000)
    }

    /// The wait before asking again after `attempts` asks of a part whose
    /// parents are `parents`.
    fn retry_after(&self, part: &OpenPart) -> u64 {
        let rtt = part
            .parents
            .iter()
            .flatten()
            .filter_map(|addr| self.parents.iter().find(|parent| parent.addr == *addr))
            .map(|parent| parent.srtt_us + parent.answer_extra_us)
            .max()
            .unwrap_or(self.repair.config.initial_rtt_us);
        let base = rtt + rtt / 4 + 3_000;
        // Half as much again with each ask.
        let mut wait = base;
        for _ in 1..part.attempts {
            wait += wait / 2;
        }
        wait
    }

    /// Ask for what is due now; probe the parents. Called after each batch
    /// of the tee and on its tick.
    pub fn poll(&mut self, now_us: u64) {
        let deadline = self.repair.config.deadline_us;
        let mut index = 0;
        while index < self.open.len() {
            let part = self.open[index];
            if now_us.saturating_sub(part.first_us) > deadline || part.attempts >= MAX_ATTEMPTS {
                if part.attempts > 0 || part.k == 0 {
                    self.stats().gave_up_parts.fetch_add(1, Ordering::Relaxed);
                }
                self.open.swap_remove(index);
                continue;
            }
            index += 1;
            let due = if part.attempts == 0 {
                let wait = self.first_wait_us();
                if part.held == 0 {
                    // Nothing of it came: from when a later part came.
                    now_us >= part.moved_on_us + MOVED_ON_GRACE_US.max(wait)
                } else {
                    // Its symbols have stopped, and the link repair this
                    // node's lane gets has come (then the part is short for
                    // good), or the repair that follows a part's sources has
                    // had its usual time.
                    let lane = lane_of(&part);
                    let link_done = self.expected_link_repair(u32::from(part.k), lane) > 0
                        && part.link_done_us != 0;
                    now_us >= part.last_us + MOVED_ON_GRACE_US
                        && (link_done || now_us >= part.last_source_us.max(part.first_us) + wait)
                }
            } else {
                now_us >= part.next_due_us
            };
            if !due {
                continue;
            }
            self.ask(index - 1, now_us);
        }
        // Probes: each parent that sent such parts in the last 2 s.
        for at in 0..self.parents.len() {
            let parent = self.parents[at];
            if now_us.saturating_sub(parent.last_media_us) > 2_000_000
                || now_us.saturating_sub(parent.last_probe_us) < PROBE_EVERY_US
            {
                continue;
            }
            self.parents[at].last_probe_us = now_us;
            let request = Request {
                part: PartKey::default(),
                wanted: 0,
                probe: true,
                sent_us: now_us,
                held: 0,
                k: 0,
            };
            if self.send(parent.lane, parent.addr, &request) {
                self.stats().probes.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn send(&self, lane: usize, to: SocketAddr, request: &Request) -> bool {
        let Some(socket) = self.sockets[lane].as_ref().or(self.sockets[1 - lane].as_ref()) else {
            return false;
        };
        let bytes = request.encode();
        sys::send_batch(socket.send_fd(), 1, |_| (&bytes[..], to)).unwrap_or(0) == 1
    }

    fn ask(&mut self, index: usize, now_us: u64) {
        let part = self.open[index];
        // What it is short of: K and two (a block decodes with K, and with
        // K + 2 nearly always), grown by the lane's loss.
        let wanted = if part.k == 0 {
            0
        } else {
            let short = (u32::from(part.k) + 2).saturating_sub(part.held).max(1);
            let lane = part.parents[0].map_or(1, |_| 0);
            let p = (self.lanes.lane_loss_ppm(self.class, lane) as f64 / 1e6).max(0.005);
            let grown = (f64::from(short) / (1.0 - p.min(0.5))).ceil() as u32 + 1;
            grown.min(MAX_ANSWER_SYMBOLS)
        };
        let request = Request {
            part: part.key,
            wanted: u16::try_from(wanted).unwrap_or(u16::MAX),
            probe: false,
            sent_us: now_us,
            held: part.held,
            k: part.k,
        };
        // The first ask goes to one parent (the primary's lane first); later
        // ones to every parent that has not said it lacks the part.
        let mut sent = false;
        for lane in [0, 1] {
            let Some(addr) = part.parents[lane] else {
                continue;
            };
            if part.missing_at[lane] && part.parents[1 - lane].is_some() {
                continue;
            }
            if self.send(lane, addr, &request) {
                sent = true;
            }
            if part.attempts == 0 {
                break;
            }
        }
        #[cfg(test)]
        if sent && part.attempts == 0 {
            self.repair.asked.lock().unwrap().push(part.key.object);
        }
        let stats = self.stats();
        if sent {
            stats.requests.fetch_add(1, Ordering::Relaxed);
            if part.attempts > 0 {
                stats.retries.fetch_add(1, Ordering::Relaxed);
            }
            if wanted == 0 {
                stats.whole_parts_asked.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.symbols_asked.fetch_add(u64::from(wanted), Ordering::Relaxed);
            }
        }
        let mut updated = part;
        updated.attempts += 1;
        if updated.first_ask_us == 0 {
            updated.first_ask_us = now_us;
        }
        // Both lanes' parents for the next asks.
        if updated.parents[1 - lane_of(&updated)].is_none() {
            if let Some(other) = self
                .parents
                .iter()
                .find(|parent| parent.lane != lane_of(&updated))
            {
                updated.parents[other.lane] = Some(other.addr);
            }
        }
        updated.next_due_us = now_us + self.retry_after(&updated);
        self.open[index] = updated;
    }
}

const fn lane_of(part: &OpenPart) -> usize {
    if part.parents[0].is_some() {
        0
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asks_and_answers_round_trip_and_other_datagrams_are_not_read() {
        let request = Request {
            part: PartKey {
                stream: 0xdead_beef_0000_0001,
                epoch: 3,
                object: 9_876,
            },
            wanted: 41,
            probe: false,
            sent_us: 1_234_567,
            held: 1_100,
            k: 1_120,
        };
        let bytes = request.encode();
        assert_eq!(&bytes[..4], b"RLQ1");
        assert_eq!(Request::decode(&bytes), Some(request));
        assert!(is_part_repair_control(&bytes));
        let probe = Request {
            probe: true,
            ..request
        };
        assert!(Request::decode(&probe.encode()).unwrap().probe);
        let answer = Answer {
            part: request.part,
            status: AnswerStatus::Waiting,
            count: 0,
            echo_us: request.sent_us,
            window: 7 * WINDOW,
        };
        assert_eq!(Answer::decode(&answer.encode()), Some(answer));
        assert_eq!(Request::decode(&answer.encode()), None);
        assert_eq!(Answer::decode(&bytes), None);
        assert_eq!(Request::decode(&bytes[..47]), None);
        assert!(!is_part_repair_control(b"RLR1............"));
    }

    #[test]
    fn repair_is_sized_to_the_loss_at_one_part_in_ten_thousand() {
        // Clean: none (the re-request covers it).
        assert_eq!(link_repair(1_100, 0), 0);
        // 1 % measured (1.6 % planned) on an average 247 Mbit/s part.
        let one = link_repair(1_100, 10_000);
        assert!((30..=50).contains(&one), "{one}");
        // 5 % (7.6 % planned): about 11 % of K.
        let five = link_repair(1_100, 50_000);
        assert!((110..=150).contains(&five), "{five}");
        // A loop-point frame of 5,500 symbols at 1 %: a smaller share.
        let big = link_repair(5_500, 10_000);
        assert!(big * 100 / 5_500 < one * 100 / 1_100 + 1, "{big}");
        // Capped at 30 % of K.
        assert_eq!(link_repair(1_000, 400_000), 300);
        // The residual holds: at the planned loss the chosen r leaves at
        // most 1e-4, and one less does not.
        let p = planned_loss(10_000);
        let r = repair_for_loss(1_100, p, TARGET_PART_FAILURE, 10_000);
        assert_eq!(r, one);
        assert!(repair_for_loss(1_100, p, 1e-3, 10_000) < r);
    }

    #[test]
    fn windows_are_disjoint_from_the_contributor_and_inside_the_symbol_space() {
        for name in ["lon-dist-a", "lon-dist-b", "new-dist-a", "syd-dist-b", ""] {
            let window = window_for(name);
            assert!(window >= WINDOW);
            assert!(window + WINDOW + 56_403 <= 1 << 24, "{name} {window}");
        }
        assert_ne!(window_for("lon-dist-a"), window_for("lon-dist-b"));
    }
}

#[cfg(test)]
mod cost {
    use super::*;
    use raptorq::{Decoder, EncodingPacket};

    /// RaptorQ's cost for a 4K part (an ignored measurement): a plan, an
    /// encode from it, and a decode that lacks 1 % of the sources.
    #[test]
    #[ignore = "a measurement"]
    fn raptorq_cost_of_a_4k_part() {
        for k in [1_088_u16, 2_176, 4_736] {
            let size = 1_264_u16;
            let data: Vec<u8> = (0..usize::from(k) * usize::from(size))
                .map(|index| (index * 31 % 251) as u8)
                .collect();
            let oti = ObjectTransmissionInformation::new(data.len() as u64, size, 1, 1, 1);
            let started = Instant::now();
            let plan = SourceBlockEncodingPlan::generate(k);
            let plan_ms = started.elapsed().as_secs_f64() * 1e3;
            let started = Instant::now();
            let encoder = SourceBlockEncoder::with_encoding_plan(0, &oti, &data, &plan);
            let repair = encoder.repair_packets(0, u32::from(k) / 20);
            let encode_ms = started.elapsed().as_secs_f64() * 1e3;
            let sources = encoder.source_packets();
            let mut decoder = Decoder::new(oti);
            let lost = usize::from(k) / 100;
            let started = Instant::now();
            let mut out = None;
            for packet in sources.into_iter().skip(lost).chain(repair) {
                if let Some(decoded) = decoder.decode(EncodingPacket::deserialize(&packet.serialize())) {
                    out = Some(decoded);
                    break;
                }
            }
            let decode_ms = started.elapsed().as_secs_f64() * 1e3;
            assert_eq!(out.as_deref(), Some(&data[..]));
            println!(
                "K {k} ({:.1} MB): plan {plan_ms:.1} ms, encode with plan and {} repair {encode_ms:.1} ms, decode lacking {lost} sources {decode_ms:.1} ms",
                data.len() as f64 / 1e6,
                k / 20
            );
        }
    }
}
