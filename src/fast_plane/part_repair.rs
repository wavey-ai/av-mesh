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
use av_mesh::mem_guard::{Shed, StoreAccount, StoreKind};
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
/// Parts that a node keeps for answers, at most, and their bytes by
/// default (`--relay-part-store-mb`): 48 MiB holds the repair horizon (the
/// 1 s media deadline) of a 4K stream with margin, 1.6 s at 247 Mbit/s
/// (30.9 MB/s of parts) and 1.26 s at 320 (40 MB/s). The bytes are counted
/// in the memory guard's account (`memory.part_store_bytes`): each kept
/// envelope and, once a part is answered or repaired, its encoder's
/// intermediate symbols, until the last holder drops the part.
pub const STORE_PARTS: usize = 128;
pub const STORE_BYTES: usize = 48 << 20;
/// Answers to one child (4k-repair abuse bounds): a token bucket of
/// symbol bytes, this much at once and this much a second (64 Mbit/s,
/// about a quarter of a 4K stream at 247 Mbit/s; a 200 ms outage of it
/// asked again whole is 6.2 MB).
pub const ANSWER_BURST_BYTES: u64 = 16 << 20;
pub const ANSWER_BYTES_PER_SECOND: u64 = 8 << 20;
/// What one child may be sent of one part over all its asks, in symbols:
/// twice the part and a margin (a whole part asked again after a lost
/// answer).
pub const fn answer_part_limit(k: u32) -> u32 {
    2 * k + 16
}
/// The (child, part) totals kept for that bound, at most.
const ANSWERED_PARTS: usize = 4_096;
/// Streams whose epoch floor the store keeps, at most.
const FLOOR_STREAMS: usize = 256;
/// Asks waiting for a part not decoded here yet, at most, and how long one
/// waits.
const PENDING_ASKS: usize = 1_024;
/// Of them, one child's at most: its oldest goes, never another child's.
const PENDING_PER_CHILD: usize = 64;
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
/// Children whose repair datagrams the ledgers count, at most.
const EXTRA_SENT_CHILDREN: usize = 4_096;
/// Children whose loss reports are kept, at most (stale ones go first).
const CHILD_LOSS_CHILDREN: usize = 4_096;
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

/// The children of a decoded part's stream, as `PartRepair::keep` uses
/// them: those that lose datagrams from this node (with their loss, ppm),
/// and those that take this node's sources.
#[derive(Default)]
pub struct Children {
    pub lossy: Vec<(Arc<dyn SendSocket>, SocketAddr, u32)>,
    pub sources: Vec<(Arc<dyn SendSocket>, SocketAddr)>,
}

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
    /// The store's account while it is kept (`PartRepair::keep`): its
    /// envelope's bytes and its encoder's, given back when the last holder
    /// drops it.
    account: Option<Arc<StoreAccount>>,
    encoder_bytes: std::sync::atomic::AtomicU64,
    /// The cursor of an encoder dropped under memory pressure: a rebuilt
    /// one goes on from it.
    spent_before: std::sync::atomic::AtomicU32,
}

impl Drop for StoredPart {
    fn drop(&mut self) {
        if let Some(account) = &self.account {
            account.sub(self.envelope.capacity() as u64 + self.encoder_bytes.load(Ordering::Relaxed));
        }
    }
}

/// RaptorQ encoding plans by K, for the repair thread (as av-contrib's
/// `PlanCache`): `raptorq`'s own cache is one FIFO of 64 shared by the
/// process, and a plan of a 4K part's K costs milliseconds to tens of
/// milliseconds to generate. With the contributor's plan grid (16 K values
/// an octave) a 4K stream's parts need a few dozen plans.
pub struct Plans {
    plans: HashMap<u16, Arc<SourceBlockEncodingPlan>>,
    order: VecDeque<u16>,
    symbols: usize,
    pub generated: u64,
    /// Their bytes in the memory guard's account (an estimate,
    /// [`PLAN_BYTES_PER_SYMBOL`] a source symbol).
    account: Arc<StoreAccount>,
}

impl Default for Plans {
    fn default() -> Self {
        Self::new(Arc::new(StoreAccount::new(
            StoreKind::Plans,
            (PLAN_SYMBOL_BUDGET * PLAN_BYTES_PER_SYMBOL) as u64,
        )))
    }
}

/// A plan's bytes by its K (about 0.75 KiB a source symbol).
const PLAN_BYTES_PER_SYMBOL: usize = 768;

/// The source-symbol counts whose plans the repair thread makes at start:
/// those of the contributor's plan grid (16 an octave) from 768 to 2,560
/// symbols (parts of 0.9 to 3.2 MB at 1,264-byte symbols), 29 plans of about
/// 34 MB.
pub fn prewarm_symbol_counts() -> Vec<u16> {
    let mut out = Vec::new();
    let mut k: u32 = 768;
    while k <= 2_560 {
        out.push(k as u16);
        let octave = 31 - k.leading_zeros();
        k += 1 << (octave - 4);
    }
    out
}

/// Plans kept at most, and their source symbols in all (a plan of K
/// symbols is roughly 0.75 K KiB: about 48 MiB at this bound).
const PLAN_CAPACITY: usize = 96;
const PLAN_SYMBOL_BUDGET: usize = 65_536;

impl Plans {
    pub fn new(account: Arc<StoreAccount>) -> Self {
        Self {
            plans: HashMap::new(),
            order: VecDeque::new(),
            symbols: 0,
            generated: 0,
            account,
        }
    }

    /// The symbols the plans may hold now: their budget, or less while the
    /// memory guard has cut their limit.
    fn symbol_limit(&self) -> usize {
        (self.account.limit() as usize / PLAN_BYTES_PER_SYMBOL).min(PLAN_SYMBOL_BUDGET)
    }

    /// Evict the oldest plans beyond the limit (also at each job: the guard
    /// may have cut it).
    pub fn enforce(&mut self, room_for: usize) {
        let limit = self.symbol_limit();
        let adding = usize::from(room_for > 0);
        while !self.order.is_empty()
            && (self.order.len() + adding > PLAN_CAPACITY || self.symbols + room_for > limit)
        {
            if let Some(old) = self.order.pop_front() {
                self.plans.remove(&old);
                self.symbols -= usize::from(old);
                self.account.sub((usize::from(old) * PLAN_BYTES_PER_SYMBOL) as u64);
            }
        }
    }

    pub fn plan(&mut self, k: u16) -> Arc<SourceBlockEncodingPlan> {
        if let Some(plan) = self.plans.get(&k) {
            return Arc::clone(plan);
        }
        let plan = Arc::new(SourceBlockEncodingPlan::generate(k));
        self.generated += 1;
        self.enforce(usize::from(k));
        self.plans.insert(k, Arc::clone(&plan));
        self.order.push_back(k);
        self.symbols += usize::from(k);
        self.account.add((usize::from(k) * PLAN_BYTES_PER_SYMBOL) as u64);
        plan
    }
}

impl Drop for Plans {
    fn drop(&mut self) {
        self.account.sub((self.symbols * PLAN_BYTES_PER_SYMBOL) as u64);
    }
}

/// What part repair needs of a part, from one of its symbols, owned: a
/// decode on the pool completes after the symbol's bytes are gone.
#[derive(Debug, Clone)]
pub struct PartMeta {
    pub key: PartKey,
    pub generation: u64,
    pub subscription_id: u64,
    pub kind: MediaObjectKind,
    pub deadline_us: u64,
    /// One RQD2 datagram of the part (its coding profile).
    pub fec: Vec<u8>,
    pub seq_base: u32,
    pub source_symbols: u16,
    pub slim: bool,
}

impl PartMeta {
    pub fn of(view: &RlsView<'_>) -> Self {
        Self {
            key: PartKey::of(view),
            generation: view.generation,
            subscription_id: view.subscription_id,
            kind: view.kind,
            deadline_us: view.deadline_us,
            fec: view.fec.to_vec(),
            seq_base: view.packet_sequence.wrapping_sub(view.esi),
            source_symbols: view.source_symbols,
            slim: view.slim.is_some(),
        }
    }

    /// Whether part repair takes the part: two symbols or more.
    pub const fn repairable(&self) -> bool {
        !self.slim && self.source_symbols >= 2
    }
}

impl StoredPart {
    /// From its part's description and its decoded envelope.
    pub fn new(meta: &PartMeta, object_key: ObjectKey, envelope: Vec<u8>) -> Option<Self> {
        let profile = RaptorQBlockProfile::from_datagram(&meta.fec).ok()?;
        if profile.transfer_length() as usize != envelope.len() {
            return None;
        }
        Some(Self {
            key: meta.key,
            object_key,
            kind: meta.kind,
            generation: TopologyGeneration::new(meta.generation).ok()?,
            subscription: SubscriptionId::new(meta.subscription_id).ok()?,
            deadline: MediaDeadline::from_micros(meta.deadline_us),
            profile,
            seq_base: meta.seq_base,
            envelope,
            encoder: StdMutex::new(None),
            account: None,
            encoder_bytes: AtomicU64::new(0),
            spent_before: std::sync::atomic::AtomicU32::new(0),
        })
    }

    /// The source symbols `esis` as RLS1 datagrams, byte for byte those its
    /// source sent (the part's own packet sequences): a child's first-arrival
    /// table takes whichever copy comes first.
    pub fn sources(&self, esis: &[u32], limits: RelayLimits) -> Vec<Vec<u8>> {
        let (k, size) = (self.profile.source_symbols(), self.profile.symbol_size());
        let t = usize::from(size);
        esis.iter()
            .filter(|esi| **esi < u32::from(k))
            .filter_map(|&esi| {
                let at = esi as usize * t;
                let mut data = self.envelope.get(at..(at + t).min(self.envelope.len()))?.to_vec();
                data.resize(t, 0);
                let serialized =
                    raptorq::EncodingPacket::new(raptorq::PayloadId::new(0, esi), data).serialize();
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
                    MediaDatagramRole::Source,
                    MediaPathIntent::PrimarySource,
                    self.deadline,
                    fec_datagram,
                    limits,
                )
                .ok()?;
                encode_datagram(&datagram, limits).ok()
            })
            .collect()
    }

    pub fn source_symbols(&self) -> u32 {
        u32::from(self.profile.source_symbols())
    }

    /// Whether this node's window of repair ordinals for the part is spent.
    pub fn window_spent(&self) -> bool {
        self.encoder
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|(_, cursor)| *cursor >= WINDOW)
    }

    /// Drop the part's encoder (rebuilt at its next answer): its bytes.
    /// Never waits: an encoder the repair thread is using is left.
    fn drop_encoder(&self) -> u64 {
        let Ok(mut state) = self.encoder.try_lock() else {
            return 0;
        };
        // Its cursor goes with it only when nothing was sent from it: else
        // a rebuilt encoder would send the same ordinals again. Kept as a
        // number, without the encoder.
        let Some((_, cursor)) = state.take() else {
            return 0;
        };
        self.spent_before.store(cursor, Ordering::Relaxed);
        let bytes = self.encoder_bytes.swap(0, Ordering::Relaxed);
        if let Some(account) = &self.account {
            account.sub(bytes);
        }
        bytes
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
                self.spent_before.load(Ordering::Relaxed),
            ));
            // raptorq 2.0.1's encoder keeps a copy of the K source symbols
            // and the L = K' + S + H intermediate symbols (about K and a
            // twentieth, and the HDPC rows), each a Vec of T bytes: about
            // twice the part (an estimate, with each symbol's allocation).
            let bytes = (2 * u64::from(k) + u64::from(k) / 20 + 64) * (u64::from(size) + 32);
            self.encoder_bytes.store(bytes, Ordering::Relaxed);
            if let Some(account) = &self.account {
                account.add(bytes);
            }
        }
        let Some((encoder, cursor)) = state.as_mut() else {
            return Vec::new();
        };
        // Never past this node's own window: its symbols stay its own, and
        // K + window + cursor stays below RaptorQ's 24-bit symbol ids (no
        // wrap). A part whose window is spent gets no more repair here.
        let count = count.clamp(1, MAX_ANSWER_SYMBOLS).min(WINDOW.saturating_sub(*cursor));
        if count == 0 {
            return Vec::new();
        }
        debug_assert!(u64::from(k) + u64::from(window) + u64::from(*cursor) + u64::from(count) < 1 << 24);
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
    pending: Vec<PendingAsk>,
    /// Each stream's current epoch (by stream key): the floor of what is
    /// kept, answered or regenerated (a part of an older session never is).
    floors: HashMap<u64, u64>,
}

impl Store {
    /// The floor of `stream` after a part or ask of `epoch`: `false` when
    /// `epoch` is below it (stale). A newer epoch raises it and drops the
    /// older sessions' parts and waiting asks.
    fn admit_epoch(&mut self, stream: u64, epoch: u64) -> bool {
        match self.floors.get(&stream).copied() {
            Some(floor) if epoch < floor => false,
            Some(floor) if epoch == floor => true,
            _ => {
                if self.floors.len() >= FLOOR_STREAMS && !self.floors.contains_key(&stream) {
                    if let Some(old) = self.floors.keys().next().copied() {
                        self.floors.remove(&old);
                    }
                }
                self.floors.insert(stream, epoch);
                self.parts
                    .retain(|part| !(part.key.stream == stream && part.key.epoch < epoch));
                self.pending
                    .retain(|ask| !(ask.request.part.stream == stream && ask.request.part.epoch < epoch));
                true
            }
        }
    }

    fn below_floor(&self, key: &PartKey) -> bool {
        self.floors.get(&key.stream).is_some_and(|floor| key.epoch < *floor)
    }
}

/// The abuse bounds' state: what each child was sent of each part, and
/// each child's token bucket.
#[derive(Default)]
struct AnswerLimits {
    parts: HashMap<(SocketAddr, PartKey), u32>,
    order: VecDeque<(SocketAddr, PartKey)>,
    buckets: HashMap<SocketAddr, (u64, Instant)>,
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
    /// The sources this node lacked and its decode regenerated, to every
    /// child that takes its sources.
    Sources {
        part: Arc<StoredPart>,
        esis: Vec<u32>,
        sends: Vec<(Arc<dyn SendSocket>, SocketAddr)>,
    },
    /// Tests: a job that panics.
    #[cfg(test)]
    Panic,
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
    pub regenerated_parts: AtomicU64,
    pub regenerated_symbols: AtomicU64,
    pub child_loss_reports: AtomicU64,
    pub ledgers: AtomicU64,
    /// Asks and loss reports from an address that is not a child of this
    /// node (never answered), asks cut or refused by the per-part and
    /// per-second bounds, and asks, parts and answers of a session older
    /// than the stream's current one (refused, never kept or sent).
    pub refused_not_admitted: AtomicU64,
    pub refused_limited: AtomicU64,
    pub refused_stale: AtomicU64,
    /// Answers refused because the part's window of ordinals here is spent
    /// (no wrap past RaptorQ's 24-bit symbol ids), and jobs of the repair
    /// thread that panicked (caught: the thread goes on).
    pub window_spent: AtomicU64,
    pub repair_panics: AtomicU64,
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
    pub regenerated_parts: u64,
    pub regenerated_symbols: u64,
    pub child_loss_reports: u64,
    pub ledgers: u64,
    pub refused_not_admitted: u64,
    pub refused_limited: u64,
    pub refused_stale: u64,
    pub window_spent: u64,
    pub repair_panics: u64,
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
            regenerated_parts: load(&self.regenerated_parts),
            regenerated_symbols: load(&self.regenerated_symbols),
            child_loss_reports: load(&self.child_loss_reports),
            ledgers: load(&self.ledgers),
            refused_not_admitted: load(&self.refused_not_admitted),
            refused_limited: load(&self.refused_limited),
            refused_stale: load(&self.refused_stale),
            window_spent: load(&self.window_spent),
            repair_panics: load(&self.repair_panics),
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
    /// `--relay-regenerate-sources`: a part decoded here goes to the
    /// children that take this node's sources with the sources it lacked.
    pub regenerate_sources: bool,
    /// `--relay-part-store-mb`: the bytes of kept parts at most.
    pub store_bytes: usize,
}

impl Default for PartRepairConfig {
    fn default() -> Self {
        Self {
            requests: true,
            link_repair: true,
            initial_rtt_us: 50_000,
            window: WINDOW,
            deadline_us: 1_000_000,
            regenerate_sources: true,
            store_bytes: STORE_BYTES,
        }
    }
}

/// A node's part repair: its kept parts, the asks waiting for them, its
/// children's loss, and the repair thread.
pub struct PartRepair {
    pub config: PartRepairConfig,
    pub stats: Arc<PartRepairStats>,
    store: StdMutex<Store>,
    /// The kept parts' bytes in the memory guard's account.
    account: Arc<StoreAccount>,
    limits: StdMutex<AnswerLimits>,
    jobs: SyncSender<Job>,
    child_loss: StdMutex<HashMap<SocketAddr, (u32, Instant)>>,
    /// Repair datagrams sent to each child here (its ledger counts them).
    extra_sent: Arc<StdMutex<HashMap<SocketAddr, u64>>>,
    /// Tests: the parts this node asked for.
    #[cfg(test)]
    asked: StdMutex<Vec<u64>>,
    /// Tests: each first ask, as the requester saw the part then.
    #[cfg(test)]
    pub ask_log: StdMutex<Vec<AskNote>>,
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
            account: Arc::new(StoreAccount::new(StoreKind::PartStore, config.store_bytes as u64)),
            limits: StdMutex::new(AnswerLimits::default()),
            jobs,
            child_loss: StdMutex::new(HashMap::new()),
            extra_sent: Arc::clone(&extra_sent),
            #[cfg(test)]
            asked: StdMutex::new(Vec::new()),
            #[cfg(test)]
            ask_log: StdMutex::new(Vec::new()),
        });
        let weak: std::sync::Weak<dyn Shed> = Arc::downgrade(&repair) as _;
        av_mesh::mem_guard::register_store(weak);
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
        missing: &[u32],
        children: impl FnOnce(&dyn Fn(SocketAddr) -> u32) -> Children,
    ) {
        let mut part = part;
        part.account = Some(Arc::clone(&self.account));
        let part = Arc::new(part);
        self.account.add(part.envelope.capacity() as u64);
        let waiting = {
            let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
            // A part of an older session than the stream's current one is
            // neither kept nor sent anywhere (the epoch floor).
            if !store.admit_epoch(part.key.stream, part.key.epoch) {
                drop(store);
                self.stats.refused_stale.fetch_add(1, Ordering::Relaxed);
                return;
            }
            store.parts.push_back(Arc::clone(&part));
            let limit = self.account.limit();
            // Over its bytes: the oldest parts' encoders go first (rebuilt
            // if asked again), then the oldest parts.
            if self.account.held() > limit {
                for old in &store.parts {
                    if self.account.held() <= limit {
                        break;
                    }
                    old.drop_encoder();
                }
            }
            while store.parts.len() > STORE_PARTS
                || (store.parts.len() > 1 && self.account.held() > limit)
            {
                if store.parts.pop_front().is_none() {
                    break;
                }
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
        let children = children(&|child| self.child_loss(child));
        let regenerated = if self.config.regenerate_sources && !children.sources.is_empty() {
            u32::try_from(missing.len()).unwrap_or(u32::MAX)
        } else {
            0
        };
        self.stats.stored_parts.fetch_add(1, Ordering::Relaxed);
        // The sources this node lacked first: below a lossy link every
        // child lacks them too, and with them it needs no decode.
        if regenerated > 0 {
            self.stats.regenerated_parts.fetch_add(1, Ordering::Relaxed);
            self.queue(Job::Sources {
                part: Arc::clone(&part),
                esis: missing.to_vec(),
                sends: children.sources.clone(),
            });
        }
        for (via, to, mut request) in waiting {
            // An ask that waited for this decode is answered now, less the
            // regenerated sources that go to the asker anyway.
            if regenerated > 0 && children.sources.iter().any(|(_, child)| *child == to) {
                if request.wanted > 0 {
                    let left = u32::from(request.wanted).saturating_sub(regenerated);
                    if left == 0 {
                        answer(&*via, to, &request, AnswerStatus::Sending, 0, self.config.window);
                        continue;
                    }
                    request.wanted = u16::try_from(left).unwrap_or(u16::MAX);
                }
            }
            let Some(request) = self.bound_answer(to, request, &part) else {
                answer(&*via, to, &request, AnswerStatus::Busy, 0, self.config.window);
                continue;
            };
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
        let sends: Vec<_> = children
            .lossy
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

    /// What an ask from `to` may be answered with: at most what the ask
    /// says the child lacks (a whole part: K and its margin; else the
    /// symbols it names, and never more than twice K + 2 less what it says
    /// it holds), at most [`answer_part_limit`] of one part over all its
    /// asks, and within its token bucket. `None`: nothing now.
    fn bound_answer(&self, to: SocketAddr, mut request: Request, part: &StoredPart) -> Option<Request> {
        let k = part.source_symbols();
        let whole = k + 2 + k / 50;
        let claimed = if request.wanted == 0 {
            whole
        } else {
            // What it says it lacks, grown by at most half again for its
            // lane's loss (the asker grows it by 1 / (1 - loss), loss
            // capped at 0.5), and a symbol.
            let short = (k + 2).saturating_sub(request.held.min(k + 2)).max(1);
            u32::from(request.wanted).min(2 * short + 1).min(whole)
        };
        let symbol = u64::from(part.profile.symbol_size()).max(1);
        let now = Instant::now();
        let mut limits = self.limits.lock().unwrap_or_else(|e| e.into_inner());
        let sent = limits.parts.get(&(to, part.key)).copied().unwrap_or(0);
        let left_part = answer_part_limit(k).saturating_sub(sent);
        let tokens = {
            let (tokens, at) = limits
                .buckets
                .entry(to)
                .or_insert((ANSWER_BURST_BYTES, now));
            let refill = (now.duration_since(*at).as_micros() as u64)
                .saturating_mul(ANSWER_BYTES_PER_SECOND)
                / 1_000_000;
            *tokens = tokens.saturating_add(refill).min(ANSWER_BURST_BYTES);
            *at = now;
            *tokens
        };
        let granted = claimed
            .min(left_part)
            .min(u32::try_from(tokens / symbol).unwrap_or(u32::MAX))
            .min(MAX_ANSWER_SYMBOLS);
        if granted < claimed || claimed < u32::from(request.wanted) {
            self.stats.refused_limited.fetch_add(1, Ordering::Relaxed);
        }
        if granted == 0 {
            return None;
        }
        if let Some((tokens, _)) = limits.buckets.get_mut(&to) {
            *tokens = tokens.saturating_sub(u64::from(granted) * symbol);
        }
        if sent == 0 {
            if limits.order.len() >= ANSWERED_PARTS {
                if let Some(old) = limits.order.pop_front() {
                    limits.parts.remove(&old);
                }
            }
            limits.order.push_back((to, part.key));
        }
        limits.parts.insert((to, part.key), sent + granted);
        if limits.buckets.len() > ANSWERED_PARTS {
            limits
                .buckets
                .retain(|_, (_, at)| now.duration_since(*at) < Duration::from_secs(10));
        }
        request.wanted = u16::try_from(granted).unwrap_or(u16::MAX);
        Some(request)
    }

    /// A datagram that a child sent to the socket `via` (where its symbols
    /// leave from): an ask (`RLQ1`) or a loss report (`RLR1`). `false` for
    /// anything else (the caller reads it on). `admitted`: whether an
    /// address is a child of this node on this socket's lane (where this
    /// node sends it the lane's symbols); anything from another address is
    /// counted and dropped, never answered, so a forged ask cannot make
    /// this node send to a third party.
    pub fn handle_child_datagram(
        &self,
        via: &Arc<dyn SendSocket>,
        from: SocketAddr,
        datagram: &[u8],
        admitted: impl Fn(SocketAddr) -> bool,
    ) -> bool {
        let report = super::loss_report::decode_loss_report(datagram);
        if report.is_some() || Request::decode(datagram).is_some() {
            if !admitted(from) {
                self.stats.refused_not_admitted.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        if let Some((ppm, _)) = report {
            self.stats.child_loss_reports.fetch_add(1, Ordering::Relaxed);
            let mut losses = self.child_loss.lock().unwrap_or_else(|e| e.into_inner());
            if losses.len() >= CHILD_LOSS_CHILDREN && !losses.contains_key(&from) {
                losses.retain(|_, (_, at)| at.elapsed() < CHILD_LOSS_TTL);
            }
            if losses.len() < CHILD_LOSS_CHILDREN || losses.contains_key(&from) {
                losses.insert(from, (ppm, Instant::now()));
            }
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
            // An ask of an older session than the stream's current one.
            if store.below_floor(&request.part) {
                drop(store);
                self.stats.refused_stale.fetch_add(1, Ordering::Relaxed);
                answer(&**via, from, &request, AnswerStatus::Missing, 0, self.config.window);
                return true;
            }
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
                let now = Instant::now();
                store.pending.retain(|ask| now.duration_since(ask.at) <= PENDING_FOR);
                if store.pending.iter().filter(|ask| ask.to == from).count() >= PENDING_PER_CHILD {
                    if let Some(at) = store.pending.iter().position(|ask| ask.to == from) {
                        store.pending.remove(at);
                    }
                }
                if store.pending.len() >= PENDING_ASKS {
                    // Full of other children's: this ask is not kept.
                    drop(store);
                    self.stats.busy.fetch_add(1, Ordering::Relaxed);
                    answer(&**via, from, &request, AnswerStatus::Busy, 0, self.config.window);
                    return true;
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
            Some(part) => match self.bound_answer(from, request, &part) {
                Some(request) => self.queue(Job::Answer {
                    via: Arc::clone(via),
                    to: from,
                    request,
                    part,
                }),
                None => answer(&**via, from, &request, AnswerStatus::Busy, 0, self.config.window),
            },
            None => {
                self.stats.answer_waiting.fetch_add(1, Ordering::Relaxed);
                answer(&**via, from, &request, AnswerStatus::Waiting, 0, self.config.window);
            }
        }
        true
    }
}

impl Shed for PartRepair {
    fn account(&self) -> &StoreAccount {
        &self.account
    }

    /// The oldest kept parts go until the store is under its limit (the
    /// guard's task; a part an answer is being made from is freed when that
    /// answer is sent).
    fn shed_to_limit(&self) -> u64 {
        let before = self.account.held();
        let limit = self.account.limit();
        let dropped: Vec<Arc<StoredPart>> = {
            let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
            let mut dropped = Vec::new();
            for part in &store.parts {
                if self.account.held() <= limit {
                    break;
                }
                part.drop_encoder();
            }
            let mut held = self.account.held();
            while held > limit {
                let Some(part) = store.parts.pop_front() else {
                    break;
                };
                held = held.saturating_sub(
                    part.envelope.capacity() as u64 + part.encoder_bytes.load(Ordering::Relaxed),
                );
                dropped.push(part);
            }
            dropped
        };
        // Freed outside the store's lock.
        drop(dropped);
        before.saturating_sub(self.account.held())
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
    let weak: std::sync::Weak<dyn Shed> = Arc::downgrade(&plans.account) as _;
    av_mesh::mem_guard::register_store(weak);
    // The plans of the part sizes a large stream sends most, made before
    // the first job: a plan made on demand cost a part 40-80 ms (and every
    // job queued behind it).
    for k in prewarm_symbol_counts() {
        plans.plan(k);
    }
    // Each child's ledger counts this thread's datagrams to it before they
    // leave (and gives back what did not leave): a ledger sent mid-burst
    // never counts fewer than the child may already hold (4k-repair review
    // S1: counted after the burst, the child saw phantom loss). Only
    // children are sent to; the map is bounded all the same.
    let count_extra = |to: SocketAddr, add: usize, unsent: usize| {
        let mut sent = extra_sent.lock().unwrap_or_else(|e| e.into_inner());
        if sent.len() >= EXTRA_SENT_CHILDREN && !sent.contains_key(&to) {
            return;
        }
        let entry = sent.entry(to).or_default();
        *entry = entry.saturating_add(add as u64).saturating_sub(unsent as u64);
    };
    let send_counted = |via: &dyn SendSocket, to: SocketAddr, datagrams: &[Vec<u8>]| {
        count_extra(to, datagrams.len(), 0);
        let sent = send_all(via, to, datagrams);
        if sent < datagrams.len() {
            count_extra(to, 0, datagrams.len() - sent);
        }
        sent
    };
    while let Ok(job) = queue.recv() {
        plans.enforce(0);
        // A panic in one job (a malformed part, raptorq's asserts) is caught
        // and counted: the thread goes on answering.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match job {
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
                    if part.window_spent() {
                        stats.window_spent.fetch_add(1, Ordering::Relaxed);
                    }
                    stats.answer_missing.fetch_add(1, Ordering::Relaxed);
                    answer(&*via, to, &request, AnswerStatus::Missing, 0, window);
                    return;
                }
                answer(
                    &*via,
                    to,
                    &request,
                    AnswerStatus::Sending,
                    u16::try_from(datagrams.len()).unwrap_or(u16::MAX),
                    window,
                );
                let sent = send_counted(&*via, to, &datagrams);
                stats.answers.fetch_add(1, Ordering::Relaxed);
                stats.answer_symbols.fetch_add(sent as u64, Ordering::Relaxed);
            }
            Job::Sources { part, esis, sends } => {
                let datagrams = part.sources(&esis, limits);
                for (via, to) in sends {
                    let sent = send_counted(&*via, to, &datagrams);
                    stats.regenerated_symbols.fetch_add(sent as u64, Ordering::Relaxed);
                }
            }
            #[cfg(test)]
            Job::Panic => panic!("test: a repair job panics"),
            Job::Link { part, sends } => {
                let most = sends.iter().map(|(_, _, count)| *count).max().unwrap_or(0);
                if most == 0 {
                    return;
                }
                let started = Instant::now();
                let datagrams = part.repair(window, most, &mut plans, limits);
                let took = started.elapsed().as_micros() as u64;
                stats.encode_us.fetch_add(took, Ordering::Relaxed);
                stats.encode_us_max.fetch_max(took, Ordering::Relaxed);
                for (via, to, count) in sends {
                    let count = (count as usize).min(datagrams.len());
                    let sent = send_counted(&*via, to, &datagrams[..count]);
                    stats.link_repair_symbols.fetch_add(sent as u64, Ordering::Relaxed);
                }
                stats.link_repair_parts.fetch_add(1, Ordering::Relaxed);
            }
        }));
        if outcome.is_err() {
            stats.repair_panics.fetch_add(1, Ordering::Relaxed);
            warn!("a part repair job panicked: caught, the repair thread goes on");
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
/// A part's datagrams have stopped when none came for twice its lane's
/// jitter (the mean of each part's longest gap between source symbols) and
/// 0.5 ms, and not less than its own longest gap, within these bounds.
const JITTER_MIN_US: u64 = 1_000;
const JITTER_MAX_US: u64 = 20_000;
/// A part that holds enough to decode is asked for only this long after its
/// last symbol (its decode, on the pool, failed).
const DECODE_GRACE_US: u64 = 150_000;
/// The planned link repair is waited for at most this long after the
/// sources (its measured arrival, `repair_tail_us`, capped).
const LINK_REPAIR_WAIT_MAX_US: u64 = 30_000;
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
    /// The longest gap between two of its source symbols (µs).
    gap_us: u64,
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

/// Tests: what the requester knew of a part when it first asked for it.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub struct AskNote {
    pub object: u64,
    pub held: u32,
    pub k: u16,
    pub silence_us: u64,
    pub since_first_us: u64,
    pub gap_us: u64,
    pub lane_gap_us: u64,
    pub moved_on: bool,
    pub link_done: bool,
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
    /// From a part's last source to the arrival of its planned link repair
    /// (µs, a running mean, every part): the parent's decode and encode.
    repair_tail_us: u64,
    /// The expected link repair of each lane for its last (K, loss).
    expected_cache: [(u32, u32, u32); 2],
    /// Each lane's jitter: the mean longest gap within a part (µs).
    lane_gap_us: [u64; 2],
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
            expected_cache: [(u32::MAX, 0, 0); 2],
            lane_gap_us: [0; 2],
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
        if !self.follow_head(key, lane, peer, now_us) {
            // A part of an older session than the stream's current one:
            // never asked for (the epoch floor).
            return;
        }
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
            if view.esi < k && part.last_source_us != 0 {
                part.gap_us = part.gap_us.max(now_us.saturating_sub(part.last_source_us));
            }
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
            gap_us: 0,
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

    /// A symbol of a part too small for part repair (one symbol, or a
    /// compact part): it follows its stream's head and is never a gap.
    pub fn seen(&mut self, view: &RlsView<'_>, peer: SocketAddr, lane: usize, now_us: u64) {
        let key = PartKey::of(view);
        self.parent(peer, lane).last_media_us = now_us;
        let _ = self.follow_head(key, lane, peer, now_us);
        if let Some(at) = self.open.iter().position(|part| part.key == key) {
            self.open.swap_remove(at);
        }
    }

    /// A part was decoded here (any size): it ends its asks, and it is no
    /// gap of its stream.
    pub fn committed(&mut self, key: PartKey, peer: SocketAddr, now_us: u64) {
        let lane = self
            .parents
            .iter()
            .find(|parent| parent.addr == peer)
            .map_or(0, |parent| parent.lane);
        let _ = self.follow_head(key, lane, peer, now_us);
        self.done(key, now_us);
    }

    /// A part's symbol came that completed nothing new (a part
    /// decoded already): only the parent's liveness and the head.
    pub fn observe_done(&mut self, view: &RlsView<'_>, peer: SocketAddr, lane: usize, now_us: u64) {
        let key = PartKey::of(view);
        self.parent(peer, lane).last_media_us = now_us;
        let _ = self.follow_head(key, lane, peer, now_us);
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
    /// on; part numbers skipped are parts of which nothing came. `false`
    /// for a part of an older session than the stream's current one.
    fn follow_head(&mut self, key: PartKey, lane: usize, peer: SocketAddr, now_us: u64) -> bool {
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
                return true;
            }
        };
        if head.epoch != key.epoch {
            if key.epoch < head.epoch {
                return false;
            }
            *head = StreamHead {
                stream: key.stream,
                epoch: key.epoch,
                newest: key.object,
                lane,
                peer,
            };
            // A new session: the old one's open parts are never asked for
            // again.
            let before = self.open.len();
            self.open
                .retain(|part| !(part.key.stream == key.stream && part.key.epoch < key.epoch));
            let dropped = (before - self.open.len()) as u64;
            if dropped > 0 {
                self.stats().refused_stale.fetch_add(dropped, Ordering::Relaxed);
            }
            return true;
        }
        if key.object <= head.newest {
            return true;
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
            return true;
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
                gap_us: 0,
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
        true
    }

    /// The part was decoded here.
    pub fn done(&mut self, key: PartKey, now_us: u64) {
        if let Some(at) = self.open.iter().position(|part| part.key == key) {
            let part = self.open.swap_remove(at);
            // The lane's jitter as its parts see it: a running mean of each
            // part's longest gap between source symbols.
            if part.gap_us > 0 {
                let lane = lane_of(&part);
                let sample = part.gap_us.min(JITTER_MAX_US);
                self.lane_gap_us[lane] = (self.lane_gap_us[lane] * 7 + sample) / 8;
            }
            // How long after its sources a part that needed no ask kept
            // getting repair (forwarded and link repair, each made after an
            // upstream decode): the first ask waits that out.

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
    fn expected_link_repair(&mut self, k: u32, lane: usize) -> u32 {
        let ppm = u32::try_from(self.lanes.lane_loss_ppm(self.class, lane)).unwrap_or(u32::MAX);
        // A binomial search: kept for the last (K, loss) of each lane, not
        // made again for every symbol.
        let cached = &mut self.expected_cache[lane];
        if cached.0 != k || cached.1 != ppm {
            let expected = link_repair(k, ppm);
            *cached = (k, ppm, expected - expected / 10);
        }
        cached.2
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
            // A part that holds enough to decode (K + 1) is being decoded:
            // it is asked for only if that has not ended it after a while
            // (a decode that failed for want of symbols).
            let decodable = part.k > 0 && part.held > u32::from(part.k);
            let due = if decodable {
                now_us >= part.last_us + DECODE_GRACE_US
                    && (part.attempts == 0 || now_us >= part.next_due_us)
            } else if part.attempts == 0 {
                let jitter = (2 * self.lane_gap_us[lane_of(&part)] + 500)
                    .max(part.gap_us)
                    .clamp(JITTER_MIN_US, JITTER_MAX_US);
                if part.held == 0 {
                    // Nothing of it came: from when a later part came.
                    now_us >= part.moved_on_us + jitter
                } else {
                    // Its last expected datagram should have come: its
                    // datagrams stopped for the link's jitter, and the link
                    // repair this lane is planned to get has come (or had
                    // its measured time after the sources and the jitter).
                    let lane = lane_of(&part);
                    let expected = self.expected_link_repair(u32::from(part.k), lane);
                    let link_wait = self.repair_tail_us.min(LINK_REPAIR_WAIT_MAX_US);
                    now_us >= part.last_us + jitter
                        && (expected == 0
                            || part.link_done_us != 0
                            || now_us >= part.last_source_us.max(part.first_us) + link_wait + jitter)
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
            self.repair.ask_log.lock().unwrap().push(AskNote {
                object: part.key.object,
                held: part.held,
                k: part.k,
                silence_us: now_us.saturating_sub(part.last_us),
                since_first_us: now_us.saturating_sub(part.first_us),
                gap_us: part.gap_us,
                lane_gap_us: self.lane_gap_us[lane_of(&part)],
                moved_on: part.moved_on_us != 0,
                link_done: part.link_done_us != 0,
            });
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

/// Tests and benches: a kept part of `envelope`, in symbols of `t` bytes.
#[cfg(test)]
pub fn stored_part_for_test(stream: u64, epoch: u64, object: u64, envelope: Vec<u8>, t: u16) -> StoredPart {
    let bytes = envelope.len();
    let k = u16::try_from(bytes.div_ceil(usize::from(t))).unwrap();
    let packet = raptorq::EncodingPacket::new(raptorq::PayloadId::new(0, 0), vec![0; usize::from(t)]).serialize();
    let header = DatagramFecHeader::raptorq(9, u32::try_from(bytes).unwrap(), 0, k, t, &packet).unwrap();
    let mut fec = vec![0_u8; HEADER_LEN];
    header.encode(&mut fec).unwrap();
    fec.extend_from_slice(&packet);
    let meta = PartMeta {
        key: PartKey {
            stream,
            epoch,
            object,
        },
        generation: 1,
        subscription_id: 1,
        kind: MediaObjectKind::VideoDelta,
        deadline_us: 0,
        fec,
        seq_base: 0,
        source_symbols: k,
        slim: false,
    };
    let key = ObjectKey::new(
        "t",
        "7",
        "v",
        epoch,
        0,
        object,
        0,
        media_object::PayloadHash::from_bytes([0; 32]),
    )
    .unwrap();
    StoredPart::new(&meta, key, envelope).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------- 5b: bounds

    const T: u16 = 1_264;

    /// A kept part of `stream` at `epoch`, `object`, of `bytes` bytes.
    fn stored(stream: u64, epoch: u64, object: u64, bytes: usize) -> StoredPart {
        stored_part_for_test(stream, epoch, object, vec![1_u8; bytes], T)
    }

    fn repair_with(store_bytes: usize) -> Arc<PartRepair> {
        PartRepair::start(
            PartRepairConfig {
                link_repair: false,
                store_bytes,
                ..PartRepairConfig::default()
            },
            RelayLimits::default(),
        )
    }

    fn keep(repair: &PartRepair, part: StoredPart) {
        repair.keep(part, &[], |_| Children::default());
    }

    fn ask(part: PartKey, wanted: u16, held: u32, k: u16) -> [u8; REQUEST_LEN] {
        Request {
            part,
            wanted,
            probe: false,
            sent_us: 1,
            held,
            k,
        }
        .encode()
    }

    /// Datagrams `socket` takes within `wait`: (RLA1 answers, other).
    fn heard(socket: &std::net::UdpSocket, wait: Duration) -> (Vec<Answer>, usize) {
        socket.set_read_timeout(Some(Duration::from_millis(20))).unwrap();
        let (mut answers, mut other) = (Vec::new(), 0);
        let until = Instant::now() + wait;
        let mut buf = [0_u8; 2_048];
        while Instant::now() < until {
            if let Ok(len) = socket.recv(&mut buf) {
                match Answer::decode(&buf[..len]) {
                    Some(answer) => answers.push(answer),
                    None => other += 1,
                }
            }
        }
        (answers, other)
    }

    fn bound_socket() -> std::net::UdpSocket {
        std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap()
    }

    #[test]
    fn an_ask_from_an_address_that_is_not_a_child_is_never_answered() {
        let repair = repair_with(STORE_BYTES);
        let part = stored(7, 1, 10, 126_400);
        let key = part.key;
        keep(&repair, part);
        let via: Arc<dyn SendSocket> = Arc::new(bound_socket());
        let (child, victim) = (bound_socket(), bound_socket());
        let child_addr = child.local_addr().unwrap();
        let admitted = |from: SocketAddr| from == child_addr;
        // A forged ask and probe naming the victim as their sender, and a
        // loss report: counted, nothing sent to it.
        for datagram in [
            ask(key, 0, 0, 100),
            Request {
                probe: true,
                ..Request::decode(&ask(key, 0, 0, 100)).unwrap()
            }
            .encode(),
        ] {
            assert!(repair.handle_child_datagram(&via, victim.local_addr().unwrap(), &datagram, admitted));
        }
        let (answers, other) = heard(&victim, Duration::from_millis(300));
        assert!(answers.is_empty() && other == 0, "the victim got {answers:?} and {other} datagrams");
        assert_eq!(repair.stats.refused_not_admitted.load(Ordering::Relaxed), 2);
        assert_eq!(repair.stats.asks.load(Ordering::Relaxed), 0);
        // The child itself is answered.
        assert!(repair.handle_child_datagram(&via, child_addr, &ask(key, 5, 97, 100), admitted));
        let (answers, symbols) = heard(&child, Duration::from_millis(500));
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].status, AnswerStatus::Sending);
        assert_eq!(symbols, 5);
    }

    #[test]
    fn an_answer_is_at_most_what_the_ask_lacks_and_replays_get_no_more_than_twice_the_part() {
        let repair = repair_with(STORE_BYTES);
        let part = stored(7, 1, 10, 126_400);
        let (key, k) = (part.key, part.source_symbols());
        keep(&repair, part);
        let via: Arc<dyn SendSocket> = Arc::new(bound_socket());
        let child = bound_socket();
        let child_addr = child.local_addr().unwrap();
        // It says it holds 95 of K = 100 and wants 4,000: at most 2 x 7 + 1.
        repair.handle_child_datagram(&via, child_addr, &ask(key, 4_000, 95, 100), |_| true);
        let (_, symbols) = heard(&child, Duration::from_millis(400));
        assert_eq!(symbols, 15);
        // The same ask, replayed 40 times, and whole-part asks: no more
        // than 2 K + 16 of this part reach the child in all.
        for _ in 0..40 {
            repair.handle_child_datagram(&via, child_addr, &ask(key, 0, 0, 100), |_| true);
        }
        let _ = heard(&child, Duration::from_millis(800));
        // (Counted as sent: the child's socket may drop some of them.)
        assert_eq!(
            repair.stats.answer_symbols.load(Ordering::Relaxed),
            u64::from(answer_part_limit(k))
        );
        assert!(repair.stats.refused_limited.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn a_child_s_answers_are_bounded_per_second() {
        let repair = repair_with(64 << 20);
        let child: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let mut granted_bytes = 0_u64;
        let started = Instant::now();
        // Whole-part asks of ten 3.8 MB parts at once: 38 MB asked.
        for object in 0..10 {
            let part = stored(7, 1, object, 3_792_000);
            let request = Request::decode(&ask(part.key, 0, 0, 3_000)).unwrap();
            if let Some(request) = repair.bound_answer(child, request, &part) {
                granted_bytes += u64::from(request.wanted) * u64::from(T);
            }
        }
        let refill = started.elapsed().as_micros() as u64 * ANSWER_BYTES_PER_SECOND / 1_000_000;
        assert!(granted_bytes <= ANSWER_BURST_BYTES + refill + u64::from(T), "{granted_bytes}");
        assert!(granted_bytes >= ANSWER_BURST_BYTES - 4_096 * u64::from(T));
        // Another child has its own bucket.
        let other: SocketAddr = "127.0.0.1:10".parse().unwrap();
        let part = stored(7, 1, 99, 3_792_000);
        let request = Request::decode(&ask(part.key, 0, 0, 3_000)).unwrap();
        assert_eq!(repair.bound_answer(other, request, &part).unwrap().wanted, 3_062);
    }

    #[test]
    fn parts_and_asks_of_an_older_session_are_refused_and_dropped() {
        let repair = repair_with(STORE_BYTES);
        let via: Arc<dyn SendSocket> = Arc::new(bound_socket());
        let child = bound_socket();
        let child_addr = child.local_addr().unwrap();
        keep(&repair, stored(7, 2, 10, 126_400));
        // Asked before it is decoded here: waits.
        let waiting = PartKey {
            stream: 7,
            epoch: 2,
            object: 11,
        };
        repair.handle_child_datagram(&via, child_addr, &ask(waiting, 0, 0, 100), |_| true);
        let (answers, _) = heard(&child, Duration::from_millis(200));
        assert_eq!(answers.iter().map(|a| a.status).collect::<Vec<_>>(), [AnswerStatus::Waiting]);
        // The stream starts a new session: the old one's part and the
        // waiting ask go.
        keep(&repair, stored(7, 3, 1, 126_400));
        assert_eq!(repair.store.lock().unwrap().parts.len(), 1);
        assert!(repair.store.lock().unwrap().pending.is_empty());
        // A late part of the old session is neither kept nor answered from.
        keep(&repair, stored(7, 2, 11, 126_400));
        assert_eq!(repair.store.lock().unwrap().parts.len(), 1);
        assert_eq!(repair.stats.refused_stale.load(Ordering::Relaxed), 1);
        let (answers, other) = heard(&child, Duration::from_millis(300));
        assert!(answers.is_empty() && other == 0, "the late part was answered");
        // An ask of the old session: "missing", nothing else.
        repair.handle_child_datagram(
            &via,
            child_addr,
            &ask(PartKey { object: 10, ..waiting }, 0, 0, 100),
            |_| true,
        );
        let (answers, other) = heard(&child, Duration::from_millis(300));
        assert_eq!(answers.iter().map(|a| a.status).collect::<Vec<_>>(), [AnswerStatus::Missing]);
        assert_eq!(other, 0);
        assert_eq!(repair.stats.refused_stale.load(Ordering::Relaxed), 2);
        // Another stream's floor is its own.
        keep(&repair, stored(8, 1, 1, 126_400));
        assert_eq!(repair.store.lock().unwrap().parts.len(), 2);
    }

    #[test]
    fn a_requester_never_asks_for_a_part_of_an_older_session() {
        let repair = repair_with(STORE_BYTES);
        let mut requester = Requester::new(
            Arc::clone(&repair),
            [None, None],
            Arc::new(super::super::relay::FastPlaneStats::default()),
            TrafficClass::Video,
        );
        let peer: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let key = |epoch, object| PartKey {
            stream: 7,
            epoch,
            object,
        };
        assert!(requester.follow_head(key(2, 1), 0, peer, 1_000));
        // Parts 2-4 of session 2 never came: open.
        assert!(requester.follow_head(key(2, 5), 0, peer, 2_000));
        assert_eq!(requester.open.len(), 3);
        // Session 3 starts: session 2's open parts are dropped.
        assert!(requester.follow_head(key(3, 0), 0, peer, 3_000));
        assert!(requester.open.is_empty());
        // A late symbol of session 2 opens nothing.
        assert!(!requester.follow_head(key(2, 9), 0, peer, 4_000));
        assert!(requester.open.is_empty());
        requester.poll(2_000_000);
        assert_eq!(repair.stats.requests.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_part_s_repair_never_leaves_its_window_nor_the_24_bit_symbol_ids() {
        let mut part = stored(7, 1, 1, 126_400);
        part.account = None;
        let mut plans = Plans::default();
        // The highest window a node can have.
        let window = WINDOW * WINDOW_SLOTS;
        let mut sent = 0_u32;
        for _ in 0..40 {
            let datagrams = part.repair(window, MAX_ANSWER_SYMBOLS, &mut plans, RelayLimits::default());
            sent += datagrams.len() as u32;
            for datagram in &datagrams {
                let view = super::super::view::RlsView::parse(datagram, RelayLimits::default()).unwrap();
                assert!(view.esi >= 100 + window && view.esi < 100 + window + WINDOW);
                assert!(view.esi < 1 << 24);
            }
        }
        assert_eq!(sent, WINDOW);
        assert!(part.window_spent());
        assert!(part.repair(window, 1, &mut plans, RelayLimits::default()).is_empty());
    }

    #[test]
    fn a_panicking_repair_job_does_not_stop_the_repair_thread() {
        let repair = repair_with(STORE_BYTES);
        repair.queue(Job::Panic);
        let part = stored(7, 1, 10, 126_400);
        let key = part.key;
        keep(&repair, part);
        let via: Arc<dyn SendSocket> = Arc::new(bound_socket());
        let child = bound_socket();
        repair.handle_child_datagram(&via, child.local_addr().unwrap(), &ask(key, 3, 99, 100), |_| true);
        let (answers, symbols) = heard(&child, Duration::from_millis(500));
        assert_eq!(answers.len(), 1);
        assert_eq!(symbols, 3);
        assert_eq!(repair.stats.repair_panics.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn one_child_s_pending_asks_never_evict_another_s() {
        let repair = repair_with(STORE_BYTES);
        let via: Arc<dyn SendSocket> = Arc::new(bound_socket());
        let (first, second) = (bound_socket(), bound_socket());
        let key = |object| PartKey {
            stream: 7,
            epoch: 1,
            object,
        };
        repair.handle_child_datagram(&via, first.local_addr().unwrap(), &ask(key(1), 0, 0, 100), |_| true);
        for object in 2..2_000 {
            repair.handle_child_datagram(&via, second.local_addr().unwrap(), &ask(key(object), 0, 0, 100), |_| true);
        }
        let pending = &repair.store.lock().unwrap().pending;
        assert!(pending.iter().any(|ask| ask.to == first.local_addr().unwrap()));
        assert!(pending.len() <= PENDING_PER_CHILD + 1);
    }

    #[test]
    fn the_plans_are_counted_and_follow_the_guard_s_limit() {
        let mut plans = Plans::default();
        for k in [800_u16, 1_000, 1_200] {
            plans.plan(k);
        }
        assert_eq!(plans.account.held(), 3_000 * PLAN_BYTES_PER_SYMBOL as u64);
        // Above the hard limit: an eighth (6 MiB: 8,192 symbols).
        plans.account.apply(av_mesh::mem_guard::GuardState::Hard);
        plans.plan(8_000);
        assert_eq!(plans.plans.len(), 1);
        assert_eq!(plans.account.held(), 8_000 * PLAN_BYTES_PER_SYMBOL as u64);
    }

    #[test]
    fn the_store_is_bounded_in_bytes_counted_and_shed_by_the_guard() {
        let repair = repair_with(1 << 20);
        for object in 0..20 {
            keep(&repair, stored(7, 1, object, 126_400));
        }
        let held = repair.account.held();
        assert!(held <= (1 << 20) && held >= (1 << 20) - 126_400, "{held}");
        assert_eq!(repair.store.lock().unwrap().parts.len(), 8);
        // In the guard's account.
        let totals = av_mesh::mem_guard::store_totals();
        assert!(totals.part_store_bytes >= held && totals.part_store_budget_bytes >= 1 << 20);
        // Above the hard limit: an eighth of the budget.
        repair.account.apply(av_mesh::mem_guard::GuardState::Hard);
        let shed = repair.shed_to_limit();
        assert!(repair.account.held() <= (1 << 20) / 8, "{}", repair.account.held());
        assert_eq!(shed, held - repair.account.held());
        // Back to ok: the budget again; a dropped part gives its bytes back.
        repair.account.apply(av_mesh::mem_guard::GuardState::Ok);
        repair.store.lock().unwrap().parts.clear();
        assert_eq!(repair.account.held(), 0);
    }

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
