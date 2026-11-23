//! The client UDP port of an edge: RaptorQ FEC UDP to clients.
//!
//! See `av_mesh::client_udp` for the protocol. An edge keeps one sender for
//! each stream that has subscribers. The sender follows the stream in the
//! edge's cache as an LL-HLS reader of the newest part does (the same wait
//! for a part, the same repair of a hole from the parents), encodes each part
//! once for each repair policy that its subscribers asked for, and sends it to
//! them. A subscription is reader demand: it subscribes the edge to the
//! playlist at its distributors and keeps the playlist held, as playlist
//! requests do.
//!
//! An edge sends media only to an address that echoed a token that the edge
//! sent to it, so a forged source address gets at most an answer smaller than
//! the request.
//!
//! A subscriber that asks for the compact lane (`av_mesh::client_compact`,
//! `--client-udp-compact`) gets each part that fits one datagram as that
//! datagram, from whichever lane sends it first (the fast lane, cut-through
//! or the cache-fed sender); the edge keeps each stream's compact datagrams
//! in a ring ([`CompactStore`]), so that every lane sends the same bytes, the
//! sender's window repair covers them, and a `NACK` is answered from it.

use crate::fast_plane::edge_clients::{EdgeFastLane, FastClient, FastGroup};
use crate::fast_plane::{StreamClasses, TrafficClass};
use crate::hop_journal::HopJournal;
use crate::{AppRouter, ParentsLost};
use av_mesh::client_compact as compact;
use av_mesh::client_udp::{
    block_id, cut_through_block_id, cut_through_symbol_size, encode_block, encode_object, geometry,
    source_datagram, CompactPlan, Control, ObjectKind, PartObject, RepairPolicy, KEEPALIVE,
    MIN_FIRST_REQUEST_BYTES, SUBSCRIPTION_TTL,
};
use bytes::Bytes;
use playlists::mesh::{chunk_frame_slot, CacheMeshReceiveObserver};
use raptorq_datagram_fec::DatagramFecHeader;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeSet, VecDeque};
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// How long a new subscription waits for the edge to hold parts of its
/// stream before it is refused.
const ADMIT_WAIT: Duration = Duration::from_secs(4);

/// `--client-udp-fresh-head-ms`.
pub(crate) const DEFAULT_FRESH_HEAD_MS: u64 = 1_000;
/// How often a sender renews the demand for its playlist and checks that
/// the edge still serves.
const HOUSEKEEPING: Duration = Duration::from_secs(1);
/// The longest a sender waits for the next part before it looks after its
/// subscribers (keepalive, newcomers, drain).
const LIVE_WAIT: Duration = Duration::from_millis(250);
/// The longest a sender tries to repair a part that the stream has passed
/// before it sends a gap.
const HOLE_WAIT: Duration = Duration::from_millis(400);
/// Default of --client-udp-send-queue-ms: how long a datagram that found
/// the client socket full waits for it.
pub(crate) const DEFAULT_SEND_QUEUE_MS: u64 = 250;
/// Default of --client-udp-playout-ms: how far behind the newest part a part
/// is still worth sending to a client.
pub(crate) const DEFAULT_PLAYOUT_MS: u64 = 500;
/// Default of --client-udp-move-hold-down-ms.
pub(crate) const DEFAULT_MOVE_HOLD_DOWN_MS: u64 = 3_000;
/// Default of --client-udp-inline-complete-bytes: an audio part, and not a
/// 4K frame, whose encode takes milliseconds.
pub(crate) const DEFAULT_INLINE_COMPLETE_BYTES: usize = 16_384;
/// Most datagrams that the send queues of one socket hold, whatever their
/// age: about 40 MB of full datagrams.
const SEND_QUEUE_MAX_DATAGRAMS: usize = 32_768;
/// Datagrams that one drain round takes under the lock and sends, without
/// it, in one `sendmmsg`.
const DRAIN_SLICE: usize = 256;
/// Parts of each stream whose compact datagrams the edge keeps: 4 s of 1 ms
/// parts, more than any playout deadline.
const COMPACT_RING: usize = 4_096;
/// The most parts a subscriber may ask for again in one NACK interval
/// (a burst of a second of 1 ms PCM, spread over ten intervals).
const NACK_BUDGET: u32 = 200;
/// The most bytes that a subscriber's NACKs may have sent again in one NACK
/// interval (about 10 Mbit/s): 200 parts of 1 ms PCM fit, but not 200 whole
/// 4K parts from a few small NACK datagrams. A part larger than this is
/// still sent when it is the first of its interval.
const NACK_BUDGET_BYTES: u64 = 128 * 1_024;
const NACK_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy)]
pub(crate) struct EdgeUdpConfig {
    pub symbol_size: u16,
    pub max_subscribers: usize,
    /// How long a datagram that found the socket full waits in its
    /// client's queue (zero: it is dropped, or the cache-fed sender waits
    /// for the socket, as before).
    pub send_queue: Duration,
    /// Playout deadline of a subscriber, behind the newest part (zero: the
    /// whole window, and HOLE_WAIT for each hole, as before).
    pub playout: Duration,
    /// A subscriber is not moved for lost parents within this of its
    /// admission, and a client moved away for that is taken back within
    /// this of its move (zero: no hold-down).
    pub move_hold_down: Duration,
    /// Hand the datagrams of a part for all its clients to the kernel in
    /// one `sendmmsg` (`false`: one `sendto` each).
    pub batch_send: bool,
    /// A cut-through part of at most this many bytes is completed (its
    /// repair encoded and sent) where it decoded; a larger one on the
    /// blocking pool, as every part was before (zero).
    pub inline_complete_bytes: usize,
    /// A client's datagrams of a completed part leave as one UDP GSO
    /// message (`UDP_SEGMENT`) in the batch, not one message each.
    pub gso: bool,
    /// Serve the compact lane to the clients that ask for it
    /// (`--client-udp-compact`; `false`: they get the RaptorQ lane, as
    /// before).
    pub compact: bool,
    /// Send parts in the short form (a 7-byte header) to the compact clients
    /// that ask for it (`--client-udp-compact-short-header`, wave 7 item 9;
    /// `false`: the 11-byte header to every client, as before).
    pub compact_short_header: bool,
    /// Admit a subscriber as soon as the edge holds a part of its stream,
    /// woken by the part's arrival (`--client-udp-admit-on-first-part`,
    /// wave 7 item 8; `false`: once the edge's playlist copy is playable,
    /// a whole segment, looked at every 20 ms, as before).
    pub admit_on_first_part: bool,
    /// Send a new subscriber the part it starts at with its `OK`
    /// (`--client-udp-first-part-at-once`; `false`: the stream's sender
    /// sends it when it next looks, up to a part later, as before).
    pub first_part_at_once: bool,
    /// With `admit_on_first_part`: the part a subscriber is admitted at must
    /// have arrived at this edge within this long (or four of the stream's
    /// parts, if longer). A stale head, such as the last part before a
    /// rendition was held back or before an earlier demand lapsed, is not
    /// a start: the subscription waits for a newer part, and takes the
    /// stale one only at the end of its wait (`--client-udp-fresh-head-ms`,
    /// wave 7 review; zero: any part, as before).
    pub fresh_head: Duration,
    /// NACK answers are collected under the subscriber table's lock and
    /// sent after it is released (`--client-udp-repair-thread`, wave 8 S1;
    /// `false`: sent under the lock, as before).
    pub nack_send_unlocked: bool,
    /// New subscriptions rebuild the fast lane's snapshot once a batch
    /// (`--client-udp-batched-admission`, wave 8 S3; `false`: once each,
    /// under the table's lock, as before).
    pub batched_admission: bool,
    /// A moved client resumes at the part it names while the cache holds
    /// it, past the playout deadline (`--client-udp-resume-from-request`,
    /// wave 8 S2; `false`: at the playout floor, as before).
    pub resume_from_request: bool,
    /// The NACKed parts a second that the edge answers at most, all its
    /// clients together (`--client-udp-nack-edge-rate`, wave 8; 0: no cap,
    /// only each subscriber's budget, as before): a NACK storm from an
    /// overloaded edge's clients does not take the CPU its sends need.
    pub nack_edge_rate: u32,
    /// A new subscription (not a client that moves here, which names its
    /// next part) is refused `edge-busy` while the edge's egress, its
    /// client UDP lanes included, is overloaded
    /// (`--client-udp-admit-by-load`, Phase AM item 8; `false`: only
    /// `--client-udp-max-subscribers` refuses, as before).
    pub admit_by_load: bool,
    /// A draining edge sends its subscribers `MOVE draining` spread over
    /// this long, a share at each tick, not all of them at once
    /// (`--client-udp-drain-spread-ms`, Phase AM item 9; zero: all at the
    /// first housekeeping tick, as before).
    pub drain_spread: Duration,
    /// While a stream has no new part, `WAIT` goes to its subscribers this
    /// often (`--client-udp-keepalive-ms`, wave 8 review; 300 ms before):
    /// under a client's 200 ms move silence (S2), so that a quiet stream
    /// (a contributor gap, a failover promotion) is not taken for a dead
    /// edge by every listener at once.
    pub keepalive: Duration,
}

impl EdgeUdpConfig {
    /// The configuration with the defaults of the flags.
    #[cfg(test)]
    pub(crate) fn with_defaults(symbol_size: u16, max_subscribers: usize) -> Self {
        Self {
            symbol_size,
            max_subscribers,
            send_queue: Duration::from_millis(DEFAULT_SEND_QUEUE_MS),
            playout: Duration::from_millis(DEFAULT_PLAYOUT_MS),
            move_hold_down: Duration::from_millis(DEFAULT_MOVE_HOLD_DOWN_MS),
            batch_send: true,
            inline_complete_bytes: DEFAULT_INLINE_COMPLETE_BYTES,
            gso: true,
            compact: true,
            compact_short_header: true,
            admit_on_first_part: true,
            first_part_at_once: true,
            fresh_head: Duration::from_millis(DEFAULT_FRESH_HEAD_MS),
            nack_send_unlocked: true,
            batched_admission: true,
            resume_from_request: true,
            nack_edge_rate: DEFAULT_NACK_EDGE_RATE,
            admit_by_load: true,
            drain_spread: Duration::from_millis(DEFAULT_DRAIN_SPREAD_MS),
            keepalive: Duration::from_millis(DEFAULT_KEEPALIVE_MS),
        }
    }
}

/// Default of `--client-udp-keepalive-ms`: under half the client's move
/// silence (`client_session::DEFAULT_SILENCE_MS`, 200 ms), so that a lost
/// `WAIT` is not a move either.
pub(crate) const DEFAULT_KEEPALIVE_MS: u64 = 80;

/// Default of `--client-udp-drain-spread-ms`.
pub(crate) const DEFAULT_DRAIN_SPREAD_MS: u64 = 2_000;

/// Default of `--client-udp-nack-edge-rate`.
pub(crate) const DEFAULT_NACK_EDGE_RATE: u32 = 20_000;

/// The edge's budget of NACKed parts (see `EdgeUdpConfig::nack_edge_rate`):
/// a token bucket of a tenth of a second's worth, kept by the control loop.
pub(crate) struct EdgeNackBudget {
    rate: f64,
    tokens: f64,
    at: Instant,
}

impl EdgeNackBudget {
    pub(crate) fn new(rate: u32, now: Instant) -> Self {
        Self {
            rate: f64::from(rate),
            tokens: f64::from(rate) / 10.0,
            at: now,
        }
    }

    fn fill(&mut self, now: Instant) {
        let since = now.saturating_duration_since(self.at).as_secs_f64();
        self.tokens = (self.tokens + since * self.rate).min(self.rate / 10.0);
        self.at = now;
    }

    /// Whether one more part may be answered now (always, with no cap);
    /// [`Self::spend`] takes it once it is.
    pub(crate) fn has_part(&mut self, now: Instant) -> bool {
        if self.rate <= 0.0 {
            return true;
        }
        self.fill(now);
        self.tokens >= 1.0
    }

    pub(crate) fn spend(&mut self) {
        if self.rate > 0.0 {
            self.tokens -= 1.0;
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct EdgeUdpStats {
    pub subscriptions: AtomicU64,
    pub refusals: AtomicU64,
    pub moves: AtomicU64,
    pub parts: AtomicU64,
    pub gaps: AtomicU64,
    pub datagrams: AtomicU64,
    pub bytes: AtomicU64,
    pub send_errors: AtomicU64,
    pub tokens_sent: AtomicU64,
    pub bad_tokens: AtomicU64,
    /// Parts forwarded cut-through, and their datagrams.
    pub cut_through_parts: AtomicU64,
    pub cut_through_datagrams: AtomicU64,
    /// Cut-through parts completed from another parent's block.
    pub cut_through_fallbacks: AtomicU64,
    /// Datagrams not sent because the socket was full.
    pub would_block: AtomicU64,
    /// Cache-mesh datagrams that the cut-through thread could not take in
    /// time (its queue was full): their client symbols go with the part's
    /// completion instead.
    pub cut_through_queue_drops: AtomicU64,
    /// Datagrams that found the socket full and waited in their client's
    /// send queue, and those dropped from it (too old, or the queues full).
    pub send_queued: AtomicU64,
    pub send_queue_drops: AtomicU64,
    /// Parts that subscribers skipped because they were past their playout
    /// deadline (after a move or a backlog), and holes sent as gaps at once
    /// because they were.
    pub playout_skipped_parts: AtomicU64,
    pub holes_past_playout: AtomicU64,
    /// Clients kept (or taken back) within the move hold-down although the
    /// edge had lost its parents.
    pub moves_held_down: AtomicU64,
    /// Times that the edge held its readers because the silence of its
    /// parents was region-wide.
    pub region_silence_held: AtomicU64,
    /// The fast client lane, once armed, whose counters the API reports.
    pub fast_lane: OnceLock<Arc<EdgeFastLane>>,
    /// The compact lane: parts sent as compact datagrams (one per part and
    /// client, copies apart), their copies, window repair datagrams, parts
    /// asked for again (NACK) and sent again, and those refused (over the
    /// budget, or not yet due).
    pub compact_parts: AtomicU64,
    pub compact_copies: AtomicU64,
    pub compact_window_repair: AtomicU64,
    pub nack_parts: AtomicU64,
    pub nack_resent: AtomicU64,
    pub nack_refused: AtomicU64,
    /// NACKed parts refused for the edge's budget (wave 8).
    pub nack_edge_capped: AtomicU64,
    /// The subscriber table, once the edge serves clients: the live gauge
    /// of subscribers is read from it (Phase AM item 8).
    pub table: OnceLock<LiveTable>,
    /// New subscriptions refused because the edge's egress was overloaded
    /// (`--client-udp-admit-by-load`, Phase AM item 8).
    pub load_refusals: AtomicU64,
    /// Times that the region was found lost (its silence held past
    /// --client-udp-region-hold-ms and a failover region fresh), and the
    /// clients moved for it (Phase AM item 11).
    pub region_lost_checks: AtomicU64,
    pub region_lost_moves: AtomicU64,
}

/// The subscriber table, for the live gauges.
pub(crate) struct LiveTable(pub(crate) Table);

impl std::fmt::Debug for LiveTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LiveTable")
    }
}

/// IPv4 and UDP headers of a datagram: the client lanes' load counts them,
/// as a NIC's limit does (a 1 ms Opus part is ~95 bytes of payload).
pub(crate) const UDP_IP_HEADER_BYTES: u64 = 28;

impl EdgeUdpStats {
    /// Subscribers now (every stream), or `None` before the edge serves
    /// clients.
    pub(crate) fn live_subscribers(&self) -> Option<u64> {
        let table = self.table.get()?;
        Some(subscriber_count(&table.0) as u64)
    }

    /// Bytes that the client lanes sent, every lane (cache-fed,
    /// cut-through, compact, NACK answers, window repair and the fast lane),
    /// with their IP and UDP headers.
    pub(crate) fn egress_wire_bytes(&self) -> u64 {
        let mut bytes = self.bytes.load(Ordering::Relaxed)
            + self.datagrams.load(Ordering::Relaxed) * UDP_IP_HEADER_BYTES;
        if let Some(lane) = self.fast_lane.get() {
            bytes += lane.stats.bytes.load(Ordering::Relaxed)
                + lane.stats.datagrams.load(Ordering::Relaxed) * UDP_IP_HEADER_BYTES;
        }
        bytes
    }

    /// The client datagrams sent so far, every lane (the fast lane's too).
    pub(crate) fn egress_datagrams(&self) -> u64 {
        let mut datagrams = self.datagrams.load(Ordering::Relaxed);
        if let Some(lane) = self.fast_lane.get() {
            datagrams += lane.stats.datagrams.load(Ordering::Relaxed);
        }
        datagrams
    }
}

/// The compact datagrams of each stream's recent parts, in their canonical
/// form: the first lane to make a part's datagram keeps it here, and every
/// other lane, the window repair and the NACK answers use those bytes.
#[derive(Default)]
pub(crate) struct CompactStore {
    streams: dashmap::DashMap<u64, Arc<CompactRing>>,
    /// The newest source epoch of each stream that this edge has seen,
    /// rings or not (a ring made later starts there).
    epochs: dashmap::DashMap<u64, u64>,
}

pub(crate) struct CompactRing {
    slots: Box<[Mutex<Option<(u64, Bytes)>>]>,
    /// The source epoch of the parts kept (0: not known yet). A contributor
    /// restart counts its parts again: the parts of the old epoch must not
    /// answer for the new ones.
    epoch: AtomicU64,
}

impl CompactRing {
    fn new(epoch: u64) -> Self {
        Self {
            slots: (0..COMPACT_RING).map(|_| Mutex::new(None)).collect(),
            epoch: AtomicU64::new(epoch),
        }
    }

    /// The source epoch of a part about to be kept or sent: a newer one
    /// than the ring's forgets every part kept; an older one is late, and
    /// `false`.
    pub(crate) fn observe_epoch(&self, epoch: u64) -> bool {
        let mut current = self.epoch.load(Ordering::Acquire);
        loop {
            if epoch == current || epoch == 0 {
                return true;
            }
            if epoch < current {
                return false;
            }
            match self.epoch.compare_exchange_weak(
                current,
                epoch,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(now) => current = now,
            }
        }
        if current != 0 {
            for slot in self.slots.iter() {
                *slot.lock().unwrap() = None;
            }
        }
        true
    }

    /// The generation byte of the ring's epoch (0: not known).
    pub(crate) fn generation(&self) -> u8 {
        match self.epoch.load(Ordering::Acquire) {
            0 => 0,
            epoch => compact::generation_of_epoch(epoch),
        }
    }

    fn slot(&self, part: u64) -> &Mutex<Option<(u64, Bytes)>> {
        &self.slots[(part % COMPACT_RING as u64) as usize]
    }

    pub(crate) fn get(&self, part: u64) -> Option<Bytes> {
        match &*self.slot(part).lock().unwrap() {
            Some((held, datagram)) if *held == part => Some(datagram.clone()),
            _ => None,
        }
    }

    /// Keep `datagram` as the part's, unless the part has one already:
    /// returns the one kept. A gap kept is provisional: the part itself,
    /// when the edge gets it after the gap went, replaces it (so that a
    /// NACK, a later subscriber and the fast lane get the part).
    pub(crate) fn put(&self, part: u64, datagram: Bytes) -> Bytes {
        let mut slot = self.slot(part).lock().unwrap();
        match &*slot {
            Some((held, kept))
                if *held == part && !(compact::is_gap(kept) && !compact::is_gap(&datagram)) =>
            {
                kept.clone()
            }
            _ => {
                *slot = Some((part, datagram.clone()));
                datagram
            }
        }
    }

    /// The part's kept datagram, unless it is a gap.
    pub(crate) fn get_part(&self, part: u64) -> Option<Bytes> {
        self.get(part).filter(|kept| !compact::is_gap(kept))
    }
}

impl CompactStore {
    pub(crate) fn ring(&self, stream_id: u64) -> Arc<CompactRing> {
        if let Some(ring) = self.streams.get(&stream_id) {
            return Arc::clone(&ring);
        }
        let epoch = self.epochs.get(&stream_id).map_or(0, |epoch| *epoch);
        Arc::clone(
            &self
                .streams
                .entry(stream_id)
                .or_insert_with(|| Arc::new(CompactRing::new(epoch))),
        )
    }

    /// A stream's source epoch, as the cache took it up: a new one forgets
    /// the stream's kept parts.
    pub(crate) fn observe_epoch(&self, stream_id: u64, epoch: u64) {
        {
            let mut known = self.epochs.entry(stream_id).or_insert(epoch);
            if *known > epoch {
                return;
            }
            *known = epoch;
        }
        if let Some(ring) = self.streams.get(&stream_id).map(|ring| Arc::clone(&ring)) {
            ring.observe_epoch(epoch);
        }
    }

    pub(crate) fn get(&self, stream_id: u64, part: u64) -> Option<Bytes> {
        self.streams.get(&stream_id)?.get(part)
    }

    /// A stream's ring, if it has one.
    pub(crate) fn existing_ring(&self, stream_id: u64) -> Option<Arc<CompactRing>> {
        self.streams.get(&stream_id).map(|ring| Arc::clone(&ring))
    }

    /// The compact datagram of an object (a part or a gap), kept or made
    /// and kept; `None` for one that does not fit a datagram (or an init).
    pub(crate) fn of_object(&self, object: &PartObject) -> Option<Bytes> {
        let ring = self.ring(object.stream_id);
        if let Some(kept) = ring.get(object.part) {
            // A gap kept gives way to the part.
            if !(object.kind == ObjectKind::Part && compact::is_gap(&kept)) {
                return Some(kept);
            }
        }
        let mut datagram = match object.kind {
            ObjectKind::Part => {
                compact::encode_part(object.part, object.published_unix_us, &object.payload)?
            }
            ObjectKind::Gap => compact::encode_gap(object.part),
            ObjectKind::Init => return None,
        };
        compact::set_generation(&mut datagram, ring.generation());
        Some(ring.put(object.part, Bytes::from(datagram)))
    }

    /// Forget a stream that no subscriber follows any more.
    fn forget(&self, stream_id: u64) {
        self.streams.remove(&stream_id);
    }
}

/// A compact datagram as one subscriber gets it: marked with its copies (or
/// as a retransmission), a part in the short form if its plan asks for it.
pub(crate) fn compact_marked(
    canonical: &[u8],
    copies: u8,
    retransmit: bool,
    short: bool,
) -> Vec<u8> {
    let mut datagram = canonical.to_vec();
    compact::set_flags(&mut datagram, copies, retransmit);
    if short {
        if let Some(short) = compact::shorten(&datagram) {
            return short;
        }
    }
    datagram
}

/// The client socket of an edge, or a pair of them on one address: audio
/// streams' datagrams leave from the audio socket (SO_PRIORITY 6, band 0 of
/// the host's prio qdisc), video streams' from the video socket, so a 4K
/// burst never sits ahead of audio in a socket or the host queue. Both read
/// control: SO_REUSEPORT spreads a client's requests over them.
#[derive(Clone)]
pub(crate) struct EdgeSockets {
    audio: Arc<UdpSocket>,
    video: Option<Arc<UdpSocket>>,
    classes: StreamClasses,
    /// The send queues of the sockets, when --client-udp-send-queue-ms is
    /// not zero.
    audio_queue: Option<Arc<SendQueue>>,
    video_queue: Option<Arc<SendQueue>>,
    /// The fast client lane, when the edge's class threads feed one
    /// (`--client-udp-fast-lane`).
    fast_lane: Option<Arc<EdgeFastLane>>,
    /// The compact lane's datagrams, shared by every lane.
    compact: Arc<CompactStore>,
    /// The node's hop journal, for `udp_sent`.
    journal: Option<Arc<HopJournal>>,
    /// Send-only twins of the pair (audio, video): see
    /// [`Self::with_send_twins`].
    send_twins: Option<[Arc<std::net::UdpSocket>; 2]>,
}

/// Where a stream's datagrams leave from: its socket, and the socket's send
/// queue if there is one.
#[derive(Clone, Copy)]
pub(crate) struct ClientOut<'a> {
    socket: &'a Arc<UdpSocket>,
    queue: Option<&'a Arc<SendQueue>>,
    journal: Option<&'a HopJournal>,
}

/// What became of one datagram for one client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    Sent,
    /// The socket was full: it waits in the client's queue.
    Queued,
    /// The socket was full and there is no queue (or the queue is full).
    Blocked,
    Failed,
}

impl ClientOut<'_> {
    /// The hop journal's `udp_sent`: a send of `part` to `peer` (the first
    /// client of the send) was just handed to the kernel.
    pub(crate) fn journal_sent(&self, stream_id: u64, part: u64, peer: Option<SocketAddr>) {
        if let (Some(journal), Some(peer)) = (self.journal, peer) {
            journal.record_udp_sent(stream_id, part, peer, crate::now_unix_us());
        }
    }

    /// Send now, or queue if the socket is full; never waits.
    pub(crate) fn send(
        &self,
        stats: &EdgeUdpStats,
        datagram: &[u8],
        peer: SocketAddr,
    ) -> SendOutcome {
        match self.queue {
            Some(queue) => queue.send(datagram, peer),
            None => try_send_counted(self.socket, stats, datagram, peer),
        }
    }
}

/// The host refused UDP GSO on a client socket: the edge sends without it
/// from then on.
static CLIENT_GSO_REFUSED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

impl ClientOut<'_> {
    /// Send each datagram to its client now, or queue it if the socket is
    /// full; never waits. With `batch`, as many as the socket takes leave
    /// in one `sendmmsg` and the rest as [`Self::send`] would send them;
    /// with `gso` as well, a run of datagrams of one size to one client is
    /// one GSO message of the batch. Returns how many were sent or queued,
    /// and how many found the socket full with nowhere to wait.
    pub(crate) fn send_many(
        &self,
        stats: &EdgeUdpStats,
        batch: bool,
        gso: bool,
        datagrams: &[(&[u8], SocketAddr)],
    ) -> (usize, usize) {
        if !batch || datagrams.len() <= 1 {
            return self.send_each(stats, datagrams);
        }
        // A client with datagrams waiting has the new ones queued behind
        // them, under one lock; the other clients' still leave in the
        // batch (and with GSO).
        if let Some(queue) = self
            .queue
            .filter(|queue| queue.waiting.load(Ordering::Acquire) > 0)
        {
            let waiting = queue.waiting_peers();
            if !waiting.is_empty() {
                let (behind, free): (Vec<_>, Vec<_>) = datagrams
                    .iter()
                    .copied()
                    .partition(|(_, peer)| waiting.contains(peer));
                let (queued, blocked) = queue.enqueue_many(&behind, true);
                let (sent, refused) = self.send_batched(stats, gso, &free);
                return (queued + sent, blocked + refused);
            }
        }
        self.send_batched(stats, gso, datagrams)
    }

    /// Each datagram as [`Self::send`] sends it.
    fn send_each(&self, stats: &EdgeUdpStats, datagrams: &[(&[u8], SocketAddr)]) -> (usize, usize) {
        let mut delivered = 0;
        let mut blocked = 0;
        for (datagram, peer) in datagrams {
            match self.send(stats, datagram, *peer) {
                SendOutcome::Sent | SendOutcome::Queued => delivered += 1,
                SendOutcome::Blocked => blocked += 1,
                SendOutcome::Failed => {}
            }
        }
        (delivered, blocked)
    }

    /// As many as the socket takes in one `sendmmsg` (with `gso`, a run of
    /// datagrams of one size to one client is one GSO message of it); the
    /// rest, which the socket refused, queued under one lock (or, without
    /// a queue, as [`Self::send`] would send them).
    fn send_batched(
        &self,
        stats: &EdgeUdpStats,
        gso: bool,
        datagrams: &[(&[u8], SocketAddr)],
    ) -> (usize, usize) {
        if datagrams.is_empty() {
            return (0, 0);
        }
        let mut first = 0;
        if gso && !CLIENT_GSO_REFUSED.load(Ordering::Relaxed) {
            use std::os::fd::AsRawFd;
            match crate::fast_plane::sys::send_gso_batch(
                self.socket.as_raw_fd(),
                datagrams.len(),
                |index| datagrams[index],
            ) {
                Ok(sent) => first = sent.datagrams,
                Err(error) if crate::fast_plane::sys::gso_unsupported(&error) => {
                    CLIENT_GSO_REFUSED.store(true, Ordering::Relaxed);
                    warn!(error = %error, "edge client UDP: no GSO on this host, sending without it");
                    return self.send_batched(stats, false, datagrams);
                }
                Err(_) => {}
            }
        } else if let Ok(sent) =
            crate::fast_path::try_send_batch(self.socket, datagrams.len(), |index| datagrams[index])
        {
            first = sent;
        }
        let bytes: usize = datagrams[..first]
            .iter()
            .map(|(datagram, _)| datagram.len())
            .sum();
        stats.datagrams.fetch_add(first as u64, Ordering::Relaxed);
        stats.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        let rest = &datagrams[first..];
        if rest.is_empty() {
            return (first, 0);
        }
        let (delivered, blocked) = match self.queue {
            // The socket refused the batch's next datagram: the rest wait,
            // under one lock, without a syscall each (the drain sends them,
            // and drops one that fails).
            Some(queue) => {
                stats.would_block.fetch_add(1, Ordering::Relaxed);
                queue.enqueue_many(rest, true)
            }
            None => self.send_each(stats, rest),
        };
        (first + delivered, blocked)
    }
}

fn try_send_counted(
    socket: &UdpSocket,
    stats: &EdgeUdpStats,
    datagram: &[u8],
    peer: SocketAddr,
) -> SendOutcome {
    match socket.try_send_to(datagram, peer) {
        Ok(sent) => {
            stats.datagrams.fetch_add(1, Ordering::Relaxed);
            stats.bytes.fetch_add(sent as u64, Ordering::Relaxed);
            SendOutcome::Sent
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            stats.would_block.fetch_add(1, Ordering::Relaxed);
            SendOutcome::Blocked
        }
        Err(error) => {
            stats.send_errors.fetch_add(1, Ordering::Relaxed);
            debug!(%peer, error = %error, "edge client UDP send failed");
            SendOutcome::Failed
        }
    }
}

impl EdgeSockets {
    pub(crate) fn single(socket: UdpSocket) -> Self {
        Self {
            audio: Arc::new(socket),
            video: None,
            classes: StreamClasses::default(),
            audio_queue: None,
            video_queue: None,
            fast_lane: None,
            compact: Arc::default(),
            journal: None,
            send_twins: None,
        }
    }

    pub(crate) fn pair(audio: UdpSocket, video: UdpSocket, classes: StreamClasses) -> Self {
        Self {
            audio: Arc::new(audio),
            video: Some(Arc::new(video)),
            classes,
            audio_queue: None,
            video_queue: None,
            fast_lane: None,
            compact: Arc::default(),
            journal: None,
            send_twins: None,
        }
    }

    /// Send-only twins of the pair (`--client-udp-send-twins`, wave 7 item
    /// 10): two more sockets on the pair's address, in its SO_REUSEPORT
    /// group, that the fast lane's threads send from. The runtime watches
    /// the pair's sockets for writing too, and every datagram that leaves a
    /// socket so watched wakes it when the kernel frees the datagram: a
    /// fan-out of 1 ms parts to hundreds of clients kept the runtime's
    /// threads busy waking. The twins are watched for reading only (the
    /// control loop reads the clients' requests that the group hands them).
    pub(crate) fn with_send_twins(
        mut self,
        audio: std::net::UdpSocket,
        video: std::net::UdpSocket,
    ) -> Self {
        self.send_twins = Some([Arc::new(audio), Arc::new(video)]);
        self
    }

    /// The descriptor that the fast lane sends a class from: its send-only
    /// twin, else its socket.
    pub(crate) fn send_fd(&self, class: TrafficClass) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        match &self.send_twins {
            Some(twins) if self.video.is_some() => twins[class.index()].as_raw_fd(),
            _ => self.class_socket(class).as_raw_fd(),
        }
    }

    /// The hop journal that `udp_sent` goes to, if any.
    pub(crate) fn hop_journal(&self) -> Option<&Arc<HopJournal>> {
        self.journal.as_ref()
    }

    /// Journal the parts sent to clients (`udp_sent`) in `journal`, if it
    /// keeps events.
    pub(crate) fn with_hop_journal(mut self, journal: Option<Arc<HopJournal>>) -> Self {
        self.journal = journal.filter(|journal| journal.is_enabled());
        self
    }

    /// The compact lane's datagrams.
    pub(crate) fn compact(&self) -> &Arc<CompactStore> {
        &self.compact
    }

    /// Serve `lane`'s clients from these sockets (armed by `run_edge_udp`).
    pub(crate) fn with_fast_lane(mut self, lane: Option<Arc<EdgeFastLane>>) -> Self {
        self.fast_lane = lane;
        self
    }

    pub(crate) fn fast_lane(&self) -> Option<&Arc<EdgeFastLane>> {
        self.fast_lane.as_ref()
    }

    /// The socket of a class: the video socket of a pair, or the one socket.
    pub(crate) fn class_socket(&self, class: TrafficClass) -> &Arc<UdpSocket> {
        match (class, &self.video) {
            (TrafficClass::Video, Some(video)) => video,
            _ => &self.audio,
        }
    }

    /// Give each socket a send queue that holds a datagram for `limit`
    /// (none when zero), with its drain task on the current runtime.
    pub(crate) fn with_send_queues(mut self, limit: Duration, stats: &Arc<EdgeUdpStats>) -> Self {
        if limit.is_zero() {
            return self;
        }
        self.audio_queue = Some(SendQueue::start(
            Arc::clone(&self.audio),
            limit,
            Arc::clone(stats),
        ));
        self.video_queue = self
            .video
            .as_ref()
            .map(|video| SendQueue::start(Arc::clone(video), limit, Arc::clone(stats)));
        self
    }

    /// The socket that a stream's datagrams leave from.
    pub(crate) fn for_stream(&self, stream_id: u64) -> &Arc<UdpSocket> {
        self.out_for_stream(stream_id).socket
    }

    /// The socket and send queue that a stream's datagrams leave from.
    pub(crate) fn out_for_stream(&self, stream_id: u64) -> ClientOut<'_> {
        match (&self.video, self.classes.class(stream_id)) {
            (Some(video), TrafficClass::Video) => ClientOut {
                socket: video,
                queue: self.video_queue.as_ref(),
                journal: self.journal.as_deref(),
            },
            _ => ClientOut {
                socket: &self.audio,
                queue: self.audio_queue.as_ref(),
                journal: self.journal.as_deref(),
            },
        }
    }
}

// ------------------------------------------------------------------ send queue

/// The datagrams of each client that found its socket full, in order.
///
/// A datagram that the socket cannot take now is never dropped at once and
/// never holds up the sender: it waits in its client's queue, and a drain
/// task sends it when the socket can take it. A client with datagrams
/// waiting has its next ones queued behind them, so its order holds. A
/// datagram older than the limit (a part past any client's playout) is
/// dropped and counted, as is one that finds the queues full.
pub(crate) struct SendQueue {
    socket: Arc<UdpSocket>,
    limit: Duration,
    /// Datagrams waiting, all clients: zero lets a send skip the lock.
    waiting: std::sync::atomic::AtomicUsize,
    clients: Mutex<HashMap<SocketAddr, std::collections::VecDeque<(Instant, Bytes)>>>,
    /// The drain is sending copies of the queues' first datagrams without
    /// the lock: they stay at the front until it has, so nothing else may
    /// drop them.
    sending: std::sync::atomic::AtomicBool,
    wake: tokio::sync::Notify,
    stats: Arc<EdgeUdpStats>,
}

type ClientQueues = HashMap<SocketAddr, std::collections::VecDeque<(Instant, Bytes)>>;

impl SendQueue {
    fn start(socket: Arc<UdpSocket>, limit: Duration, stats: Arc<EdgeUdpStats>) -> Arc<Self> {
        let queue = Arc::new(Self {
            socket,
            limit,
            waiting: std::sync::atomic::AtomicUsize::new(0),
            clients: Mutex::new(HashMap::new()),
            sending: std::sync::atomic::AtomicBool::new(false),
            wake: tokio::sync::Notify::new(),
            stats,
        });
        tokio::spawn(drain_send_queue(Arc::downgrade(&queue)));
        queue
    }

    pub(crate) fn send(&self, datagram: &[u8], peer: SocketAddr) -> SendOutcome {
        if self.waiting.load(Ordering::Acquire) == 0 {
            match try_send_counted(&self.socket, &self.stats, datagram, peer) {
                SendOutcome::Blocked => {}
                outcome => return outcome,
            }
            return self.enqueue(datagram, peer, true);
        }
        self.enqueue(datagram, peer, false)
    }

    /// Queue behind the client's waiting datagrams; a client with none
    /// tries the socket first (unless it just did).
    fn enqueue(&self, datagram: &[u8], peer: SocketAddr, tried: bool) -> SendOutcome {
        let outcome = {
            let mut clients = self.clients.lock().unwrap();
            self.enqueue_locked(&mut clients, Instant::now(), datagram, peer, tried)
        };
        if outcome == SendOutcome::Queued {
            self.wake.notify_one();
        }
        outcome
    }

    /// [`Self::enqueue`] of several datagrams under one lock. Returns how
    /// many were sent or queued, and how many found the queues full.
    fn enqueue_many(&self, datagrams: &[(&[u8], SocketAddr)], tried: bool) -> (usize, usize) {
        let (mut delivered, mut blocked, mut queued) = (0, 0, false);
        {
            let mut clients = self.clients.lock().unwrap();
            let now = Instant::now();
            for (datagram, peer) in datagrams {
                match self.enqueue_locked(&mut clients, now, datagram, *peer, tried) {
                    SendOutcome::Sent => delivered += 1,
                    SendOutcome::Queued => {
                        delivered += 1;
                        queued = true;
                    }
                    SendOutcome::Blocked => blocked += 1,
                    SendOutcome::Failed => {}
                }
            }
        }
        if queued {
            self.wake.notify_one();
        }
        (delivered, blocked)
    }

    fn enqueue_locked(
        &self,
        clients: &mut ClientQueues,
        now: Instant,
        datagram: &[u8],
        peer: SocketAddr,
        tried: bool,
    ) -> SendOutcome {
        let waiting_here = clients.get(&peer).is_some_and(|queue| !queue.is_empty());
        if !waiting_here && !tried {
            match try_send_counted(&self.socket, &self.stats, datagram, peer) {
                SendOutcome::Blocked => {}
                outcome => return outcome,
            }
        }
        if self.waiting.load(Ordering::Acquire) >= SEND_QUEUE_MAX_DATAGRAMS {
            self.stats.send_queue_drops.fetch_add(1, Ordering::Relaxed);
            return SendOutcome::Blocked;
        }
        let queue = clients.entry(peer).or_default();
        // What the limit already condemns goes now, not at the next drain
        // (unless the drain is sending the front of the queues).
        if !self.sending.load(Ordering::Acquire) {
            while queue
                .front()
                .is_some_and(|(at, _)| now.duration_since(*at) > self.limit)
            {
                queue.pop_front();
                self.waiting.fetch_sub(1, Ordering::AcqRel);
                self.stats.send_queue_drops.fetch_add(1, Ordering::Relaxed);
            }
        }
        queue.push_back((now, Bytes::copy_from_slice(datagram)));
        self.waiting.fetch_add(1, Ordering::AcqRel);
        self.stats.send_queued.fetch_add(1, Ordering::Relaxed);
        SendOutcome::Queued
    }

    /// The clients that have datagrams waiting.
    fn waiting_peers(&self) -> std::collections::HashSet<SocketAddr> {
        let clients = self.clients.lock().unwrap();
        clients
            .iter()
            .filter(|(_, queue)| !queue.is_empty())
            .map(|(peer, _)| *peer)
            .collect()
    }

    /// Send what the socket takes, a datagram of each client in turn, and
    /// drop what is too old. `true` when the socket filled up before the
    /// queues were empty.
    ///
    /// The lock is held only to take a slice of at most [`DRAIN_SLICE`]
    /// datagrams (copies: they stay at the front of their queues) and to
    /// remove what was sent; the slice leaves in one `sendmmsg` without
    /// it, so a sender that queues meanwhile waits for neither.
    fn drain(&self) -> bool {
        let mut slice: Vec<(SocketAddr, Bytes)> = Vec::with_capacity(DRAIN_SLICE);
        loop {
            slice.clear();
            {
                let mut clients = self.clients.lock().unwrap();
                let now = Instant::now();
                for queue in clients.values_mut() {
                    while queue
                        .front()
                        .is_some_and(|(at, _)| now.duration_since(*at) > self.limit)
                    {
                        queue.pop_front();
                        self.waiting.fetch_sub(1, Ordering::AcqRel);
                        self.stats.send_queue_drops.fetch_add(1, Ordering::Relaxed);
                    }
                }
                clients.retain(|_, queue| !queue.is_empty());
                // A datagram of each client in turn.
                let mut depth = 0;
                while slice.len() < DRAIN_SLICE {
                    let before = slice.len();
                    for (peer, queue) in clients.iter() {
                        if let Some((_, datagram)) = queue.get(depth) {
                            slice.push((*peer, datagram.clone()));
                            if slice.len() == DRAIN_SLICE {
                                break;
                            }
                        }
                    }
                    if slice.len() == before {
                        break;
                    }
                    depth += 1;
                }
                if slice.is_empty() {
                    return false;
                }
                self.sending.store(true, Ordering::Release);
            }
            let mut sent = crate::fast_path::try_send_batch(&self.socket, slice.len(), |index| {
                (&slice[index].1[..], slice[index].0)
            })
            .unwrap_or(0);
            // The datagram the batch stopped at: the socket is full, or
            // this one failed (dropped, so that it does not stop the rest).
            let mut failed = None;
            let mut full = false;
            if sent < slice.len() {
                let (peer, datagram) = &slice[sent];
                match self.socket.try_send_to(datagram, *peer) {
                    Ok(_) => sent += 1,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => full = true,
                    Err(error) => {
                        debug!(%peer, error = %error, "edge client UDP queued send failed");
                        failed = Some(sent);
                    }
                }
            }
            let bytes: usize = slice[..sent]
                .iter()
                .map(|(_, datagram)| datagram.len())
                .sum();
            self.stats
                .datagrams
                .fetch_add(sent as u64, Ordering::Relaxed);
            self.stats.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
            {
                let mut clients = self.clients.lock().unwrap();
                let done = slice[..sent]
                    .iter()
                    .chain(failed.map(|index| &slice[index]));
                for (peer, _) in done {
                    if clients
                        .get_mut(peer)
                        .and_then(|queue| queue.pop_front())
                        .is_some()
                    {
                        self.waiting.fetch_sub(1, Ordering::AcqRel);
                    }
                }
                self.sending.store(false, Ordering::Release);
                clients.retain(|_, queue| !queue.is_empty());
            }
            if failed.is_some() {
                self.stats.send_errors.fetch_add(1, Ordering::Relaxed);
            }
            if full {
                return true;
            }
        }
    }
}

/// Drain a send queue whenever it holds datagrams and its socket can take
/// one. Ends when the queue is dropped.
async fn drain_send_queue(queue: std::sync::Weak<SendQueue>) {
    loop {
        let Some(strong) = queue.upgrade() else {
            return;
        };
        if strong.waiting.load(Ordering::Acquire) == 0 {
            // Wake on a queued datagram; look again now and then so that a
            // dropped queue ends this task.
            let _ = tokio::time::timeout(Duration::from_secs(1), strong.wake.notified()).await;
            continue;
        }
        if strong.drain() {
            // The socket is full: wait until it can take a datagram.
            let socket = Arc::clone(&strong.socket);
            drop(strong);
            let _ = tokio::time::timeout(Duration::from_millis(50), socket.writable()).await;
        }
    }
}

/// A request that the SO_REUSEPORT group handed a send-only twin.
async fn recv_twin(
    socket: Option<&tokio::io::unix::AsyncFd<Arc<std::net::UdpSocket>>>,
    buffer: &mut [u8],
) -> std::io::Result<(usize, SocketAddr)> {
    let Some(socket) = socket else {
        return std::future::pending().await;
    };
    loop {
        let mut guard = socket.readable().await?;
        if let Ok(result) = guard.try_io(|inner| inner.get_ref().recv_from(buffer)) {
            return result;
        }
    }
}

async fn recv_optional(
    socket: Option<&Arc<UdpSocket>>,
    buffer: &mut [u8],
) -> std::io::Result<(usize, SocketAddr)> {
    match socket {
        Some(socket) => socket.recv_from(buffer).await,
        None => std::future::pending().await,
    }
}

#[derive(Debug)]
pub(crate) struct Subscriber {
    repair: RepairPolicy,
    expires_at: Instant,
    /// The next part that this subscriber needs.
    next_part: u64,
    /// The init segment last sent to it.
    init: Option<Bytes>,
    /// Parts that cut-through carries to it: the cache sender skips them.
    cut_slots: BTreeSet<u64>,
    /// When this edge took it.
    admitted_at: Instant,
    /// It came by a move from another edge (it named the part that it needs
    /// next): only such a client is held within the move hold-down.
    moved_in: bool,
    /// It came by a move and has not yet caught up to the playout floor:
    /// only then does it skip the floor (wave 8 S2, resume from request).
    /// Cleared once it reaches the floor, so that a later stall of this
    /// edge skips it forward like any other (wave 8 review: the exemption
    /// lasted the whole session, and one exempt client made the stream's
    /// sender walk the whole stale backlog).
    catching_up: bool,
    /// It has been counted in `moves_held_down` once already.
    held_down_counted: bool,
    /// Its place on the fast lane, if it asked for it (`F1`) and the edge
    /// has one.
    fast: Option<Arc<FastClient>>,
    /// Its plan of the compact lane, if it asked for it and the edge serves
    /// it.
    compact: Option<CompactPlan>,
    /// The first part that this edge sent it: window repair goes to it only
    /// for windows from there on.
    first_part: u64,
    /// Its NACKs: the start of the current interval, and the parts granted
    /// and the bytes sent again in it.
    nack_interval: NackInterval,
    /// The windows (their first parts) whose repair went to it, newest
    /// last: whichever lane completes a window sends its repair once.
    windows_repaired: VecDeque<u64>,
}

/// A subscriber's NACK budget in the current interval: parts, and bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NackInterval {
    started: Instant,
    parts: u32,
    bytes: u64,
}

impl NackInterval {
    pub(crate) fn new(started: Instant) -> Self {
        Self {
            started,
            parts: 0,
            bytes: 0,
        }
    }

    fn roll(&mut self, now: Instant) {
        if now.duration_since(self.started) >= NACK_INTERVAL {
            *self = Self::new(now);
        }
    }

    /// Take one part of the budget.
    fn grant_part(&mut self, now: Instant) -> bool {
        self.roll(now);
        if self.parts >= NACK_BUDGET {
            return false;
        }
        self.parts += 1;
        true
    }

    /// Take `bytes` of the byte budget: refused when the interval has sent
    /// some bytes already and these would pass it.
    fn charge_bytes(&mut self, now: Instant, bytes: u64) -> bool {
        self.roll(now);
        if self.bytes > 0 && self.bytes.saturating_add(bytes) > NACK_BUDGET_BYTES {
            return false;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        true
    }
}

/// Charge `bytes` resent to a subscriber's NACK budget; `false` when the
/// budget refuses them (or the subscriber is gone).
fn charge_nack_bytes(table: &Table, stream_id: u64, peer: SocketAddr, bytes: u64) -> bool {
    table
        .lock()
        .unwrap()
        .get_mut(&stream_id)
        .and_then(|entry| entry.subscribers.get_mut(&peer))
        .is_some_and(|subscriber| subscriber.nack_interval.charge_bytes(Instant::now(), bytes))
}

#[derive(Debug, Default)]
pub(crate) struct StreamEntry {
    subscribers: HashMap<SocketAddr, Subscriber>,
    /// A sender task runs for the stream.
    running: bool,
}

pub(crate) type Table = Arc<Mutex<HashMap<u64, StreamEntry>>>;

/// The fast-lane clients of a stream, by repair policy (for the snapshot
/// that the class threads read).
pub(crate) fn fast_groups(entry: &StreamEntry) -> Vec<FastGroup> {
    let mut groups: Vec<FastGroup> = Vec::new();
    for (peer, subscriber) in &entry.subscribers {
        let Some(fast) = &subscriber.fast else {
            continue;
        };
        match groups
            .iter_mut()
            .find(|group| group.repair == subscriber.repair && group.compact == subscriber.compact)
        {
            Some(group) => group.clients.push((*peer, Arc::clone(fast))),
            None => groups.push(FastGroup {
                repair: subscriber.repair,
                compact: subscriber.compact,
                clients: vec![(*peer, Arc::clone(fast))],
            }),
        }
    }
    // A stable order, so that the rotation by part means something.
    for group in &mut groups {
        group.clients.sort_by_key(|(peer, _)| *peer);
    }
    groups
}

/// Rebuild the fast lane's snapshot after the table changed, under its lock.
fn fast_lane_changed(sockets: &EdgeSockets, table: &HashMap<u64, StreamEntry>) {
    if let Some(lane) = sockets.fast_lane() {
        lane.rebuild_locked(table);
    }
}

/// Rebuilds of the fast lane's snapshot asked for and made, so that a burst
/// of table changes (a step's subscribe spike, a region's clients moving)
/// costs a few rebuilds, not one each (`--client-udp-batched-admission`,
/// wave 8 S3). A rebuild reads every subscriber under the table's lock.
#[derive(Default)]
pub(crate) struct Rebuilds {
    asked: AtomicU64,
    made: AtomicU64,
    /// One rebuild at a time; those who wait behind it find their change
    /// taken by the next.
    turn: tokio::sync::Mutex<()>,
    /// Rebuilds made, and changes they covered (for the stats).
    pub(crate) rebuilds: AtomicU64,
}

impl Rebuilds {
    /// The snapshot includes the caller's change to the table (made
    /// before this call) when this returns: a rebuild that began after the
    /// change was asked for has run.
    pub(crate) async fn after_change(&self, sockets: &EdgeSockets, table: &Table) {
        if sockets.fast_lane().is_none() {
            return;
        }
        let mine = self.asked.fetch_add(1, Ordering::SeqCst) + 1;
        let _turn = self.turn.lock().await;
        if self.made.load(Ordering::SeqCst) >= mine {
            return;
        }
        // Every change asked for up to here was made in the table before it
        // was asked for: this rebuild covers them all.
        let covers = self.asked.load(Ordering::SeqCst);
        {
            let table = table.lock().unwrap();
            fast_lane_changed(sockets, &table);
        }
        self.rebuilds.fetch_add(1, Ordering::Relaxed);
        self.made.store(covers, Ordering::SeqCst);
    }
}
/// Clients that this edge sent away because it had lost its parents, and
/// when: one that comes back within the move hold-down is taken.
type MovedAway = Arc<Mutex<HashMap<(SocketAddr, u64), Instant>>>;
/// Subscriptions that are being admitted: a client repeats its request
/// until it is answered, and the repeats must not admit it twice.
type Admitting = Arc<Mutex<HashSet<(SocketAddr, u64)>>>;

/// Tokens that show that a client receives at its address: a keyed hash of
/// the address and the minute, valid for the minute and the next.
struct Tokens(RandomState);

impl Tokens {
    fn minute() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs() / 60)
    }

    fn token(&self, peer: SocketAddr, minute: u64) -> u64 {
        self.0.hash_one((peer, minute))
    }

    fn current(&self, peer: SocketAddr) -> u64 {
        self.token(peer, Self::minute())
    }

    fn valid(&self, peer: SocketAddr, token: u64) -> bool {
        let minute = Self::minute();
        token == self.token(peer, minute) || token == self.token(peer, minute.saturating_sub(1))
    }
}

fn subscriber_count(table: &Table) -> usize {
    table
        .lock()
        .unwrap()
        .values()
        .map(|entry| entry.subscribers.len())
        .sum()
}

pub(crate) async fn run_edge_udp(
    sockets: EdgeSockets,
    router: AppRouter,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    cut_through: Vec<Arc<CutThrough>>,
    mut shutdown_rx: watch::Receiver<()>,
) -> anyhow::Result<()> {
    let sockets = sockets.with_send_queues(config.send_queue, &stats);
    // A contributor restart counts its parts again: the compact lane
    // forgets the old epoch's parts, and stamps the new one's generation.
    let store = Arc::clone(sockets.compact());
    router
        .cache
        .on_source_epoch(Arc::new(move |stream_id, epoch| {
            store.observe_epoch(stream_id, epoch);
        }))
        .await;
    let table: Table = Arc::default();
    let _ = stats.table.set(LiveTable(Arc::clone(&table)));
    let moved: MovedAway = Arc::default();
    if let Some(lane) = sockets.fast_lane() {
        let _ = stats.fast_lane.set(Arc::clone(lane));
        // The lane's own copy of the sockets does not hold the lane.
        lane.arm(
            sockets.clone().with_fast_lane(None),
            Arc::clone(&table),
            Arc::clone(&stats),
        );
    }
    for cut in &cut_through {
        cut.arm(
            sockets.clone(),
            Arc::clone(&table),
            config,
            Arc::clone(&stats),
        );
    }
    let admitting: Admitting = Arc::default();
    let rebuilds: Arc<Rebuilds> = Arc::default();
    let mut nack_budget = EdgeNackBudget::new(config.nack_edge_rate, Instant::now());
    let tokens = Tokens(RandomState::new());
    let mut audio_buf = vec![0_u8; 2_048];
    let mut video_buf = vec![0_u8; 2_048];
    // The send-only twins' share of the requests, watched for reading only.
    let twins = match &sockets.send_twins {
        Some([audio, video]) => Some([
            tokio::io::unix::AsyncFd::with_interest(
                Arc::clone(audio),
                tokio::io::Interest::READABLE,
            )?,
            tokio::io::unix::AsyncFd::with_interest(
                Arc::clone(video),
                tokio::io::Interest::READABLE,
            )?,
        ]),
        None => None,
    };
    let mut twin_bufs = [vec![0_u8; 2_048], vec![0_u8; 2_048]];
    info!(
        bind = %sockets.audio.local_addr()?,
        class_sockets = sockets.video.is_some(),
        "edge client UDP (RaptorQ FEC) listening"
    );
    loop {
        let [twin_audio_buf, twin_video_buf] = &mut twin_bufs;
        // Which socket: 0 audio, 1 video, 2 and 3 their send-only twins.
        let (len, peer, source) = tokio::select! {
            _ = shutdown_rx.changed() => {
                // The streams' senders end with their subscribers: a stopped
                // edge sends no more WAIT (which, every 80 ms, would hold
                // its clients on it until their subscriptions expired).
                table.lock().unwrap().clear();
                return Ok(());
            }
            received = sockets.audio.recv_from(&mut audio_buf) => match received {
                Ok((len, peer)) => (len, peer, 0),
                Err(error) => {
                    // An ICMP port unreachable of a departed client surfaces
                    // here on some kernels; it is not fatal.
                    debug!(error = %error, "edge client UDP receive error");
                    continue;
                }
            },
            received = recv_optional(sockets.video.as_ref(), &mut video_buf) => match received {
                Ok((len, peer)) => (len, peer, 1),
                Err(error) => {
                    debug!(error = %error, "edge client UDP receive error");
                    continue;
                }
            },
            received = recv_twin(twins.as_ref().map(|twins| &twins[0]), twin_audio_buf) => match received {
                Ok((len, peer)) => (len, peer, 2),
                Err(error) => {
                    debug!(error = %error, "edge client UDP receive error");
                    continue;
                }
            },
            received = recv_twin(twins.as_ref().map(|twins| &twins[1]), twin_video_buf) => match received {
                Ok((len, peer)) => (len, peer, 3),
                Err(error) => {
                    debug!(error = %error, "edge client UDP receive error");
                    continue;
                }
            },
        };
        // Answer from the socket (or the twin's socket) the request came
        // to; all have the client's address.
        let (socket, request) = match source {
            1 => (
                sockets.video.as_ref().expect("received on it"),
                &video_buf[..len],
            ),
            3 => (
                sockets.video.as_ref().expect("a twin of the pair"),
                &twin_bufs[1][..len],
            ),
            2 => (&sockets.audio, &twin_bufs[0][..len]),
            _ => (&sockets.audio, &audio_buf[..len]),
        };
        let socket = Arc::clone(socket);
        let Some(control) = Control::decode(request) else {
            continue;
        };
        match control {
            Control::Subscribe {
                stream_id,
                from_part,
                repair,
                token,
                fast_lane,
                compact,
            } => {
                let compact = compact.filter(|_| config.compact).map(|plan| {
                    plan.with_short_header(plan.short_header && config.compact_short_header)
                });
                let valid = token.is_some_and(|token| tokens.valid(peer, token));
                if !valid {
                    if token.is_some() {
                        stats.bad_tokens.fetch_add(1, Ordering::Relaxed);
                    }
                    // Answer only a request at least as large as the answer.
                    if len >= MIN_FIRST_REQUEST_BYTES || token.is_some() {
                        stats.tokens_sent.fetch_add(1, Ordering::Relaxed);
                        send_control(
                            &socket,
                            peer,
                            &Control::Token {
                                stream_id,
                                token: tokens.current(peer),
                            },
                        )
                        .await;
                    }
                    continue;
                }
                let repair = repair.bounded();
                let window = router.cache.window_parts_for(stream_id).await as u64;
                let head = head_part(&router, stream_id).await;
                let refreshed = {
                    let mut guard = table.lock().unwrap();
                    let refreshed = match guard
                        .get_mut(&stream_id)
                        .and_then(|entry| entry.subscribers.get_mut(&peer))
                    {
                        Some(subscriber) => {
                            subscriber.expires_at = Instant::now() + SUBSCRIPTION_TTL;
                            // A fast-lane client whose repair changed moves
                            // to the group of its new policy.
                            let regroup = subscriber.fast.is_some()
                                && (subscriber.repair != repair || subscriber.compact != compact);
                            subscriber.repair = repair;
                            subscriber.compact = compact;
                            // A client that names a part has not yet heard
                            // the edge's answer: it resumes there.
                            if let (Some(part), Some(head)) = (from_part, head) {
                                if part < subscriber.next_part && head.saturating_sub(part) < window
                                {
                                    subscriber.next_part = part;
                                }
                            }
                            Some((subscriber.next_part, regroup))
                        }
                        None => None,
                    };
                    if refreshed.is_some_and(|(_, regroup)| regroup) {
                        if config.batched_admission {
                            let (sockets, table, rebuilds) =
                                (sockets.clone(), Arc::clone(&table), Arc::clone(&rebuilds));
                            tokio::spawn(async move {
                                rebuilds.after_change(&sockets, &table).await;
                            });
                        } else {
                            fast_lane_changed(&sockets, &guard);
                        }
                    }
                    refreshed.map(|(next_part, _)| next_part)
                };
                if let Some(next_part) = refreshed {
                    send_control(
                        &socket,
                        peer,
                        &Control::Subscribed {
                            stream_id,
                            next_part,
                            compact: compact.is_some(),
                        },
                    )
                    .await;
                    continue;
                }
                if !admitting.lock().unwrap().insert((peer, stream_id)) {
                    continue;
                }
                let pending = admitting.lock().unwrap().len();
                if subscriber_count(&table) + pending > config.max_subscribers {
                    admitting.lock().unwrap().remove(&(peer, stream_id));
                    stats.refusals.fetch_add(1, Ordering::Relaxed);
                    send_control(
                        &socket,
                        peer,
                        &Control::Refused {
                            stream_id,
                            reason: "edge-full".into(),
                        },
                    )
                    .await;
                    continue;
                }
                let admission = admit(
                    Arc::clone(&socket),
                    sockets.clone(),
                    router.clone(),
                    Arc::clone(&table),
                    Arc::clone(&moved),
                    config,
                    Arc::clone(&stats),
                    Arc::clone(&rebuilds),
                    peer,
                    stream_id,
                    from_part,
                    repair,
                    fast_lane,
                    compact,
                );
                let admitting = Arc::clone(&admitting);
                tokio::spawn(async move {
                    admission.await;
                    admitting.lock().unwrap().remove(&(peer, stream_id));
                });
            }
            Control::Unsubscribe { stream_id, token } => {
                if !tokens.valid(peer, token) {
                    stats.bad_tokens.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let mut guard = table.lock().unwrap();
                let removed = guard
                    .get_mut(&stream_id)
                    .and_then(|entry| entry.subscribers.remove(&peer));
                if removed.is_some_and(|subscriber| subscriber.fast.is_some()) {
                    if config.batched_admission {
                        drop(guard);
                        let (sockets, table, rebuilds) =
                            (sockets.clone(), Arc::clone(&table), Arc::clone(&rebuilds));
                        tokio::spawn(async move {
                            rebuilds.after_change(&sockets, &table).await;
                        });
                    } else {
                        fast_lane_changed(&sockets, &guard);
                    }
                }
            }
            Control::Nack {
                stream_id,
                token,
                parts,
            } => {
                if !tokens.valid(peer, token) {
                    stats.bad_tokens.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                // The control loop answers from the compact ring only; parts
                // that need a cache read or an encode (a 4K part takes
                // milliseconds) are answered on a task of their own, so a
                // lossy listener's NACKs do not hold up the other clients'
                // SUBs, refreshes and NACKs.
                if let Some((misses, repair)) =
                    answer_nack(
                        &sockets,
                        &table,
                        &stats,
                        peer,
                        stream_id,
                        &parts,
                        config.nack_send_unlocked,
                        &mut nack_budget,
                    )
                {
                    let sockets = sockets.clone();
                    let router = router.clone();
                    let table = Arc::clone(&table);
                    let stats = Arc::clone(&stats);
                    tokio::spawn(async move {
                        answer_nack_from_cache(
                            &sockets, &router, &table, config, &stats, peer, stream_id, misses,
                            repair,
                        )
                        .await;
                    });
                }
            }
            _ => {}
        }
    }
}

/// Send a compact-lane subscriber the parts that it asks for again: each
/// part that was sent to it already (the cache-fed sender has passed it, or
/// the fast lane or cut-through carried it; the others are still due),
/// within its NACK budget (parts and bytes), from the compact ring. Returns
/// the granted parts that the ring does not hold, with the subscriber's
/// repair, for [`answer_nack_from_cache`]; it never waits.
///
/// With `unlocked` (wave 8 S1) the datagrams are collected under the
/// subscriber table's lock and sent after it is released: a NACK storm does
/// not hold the lock across a send each (which the real-time class thread
/// and every SUB would wait behind); without, each is sent under the lock,
/// as before.
pub(crate) fn answer_nack(
    sockets: &EdgeSockets,
    table: &Table,
    stats: &EdgeUdpStats,
    peer: SocketAddr,
    stream_id: u64,
    parts: &[u64],
    unlocked: bool,
    budget: &mut EdgeNackBudget,
) -> Option<(Vec<u64>, RepairPolicy)> {
    stats
        .nack_parts
        .fetch_add(parts.len() as u64, Ordering::Relaxed);
    // The edge's own budget (wave 8) is charged only for the parts that are
    // answered: a NACK from a stranger, or for parts not sent yet or past
    // the subscriber's own budget, takes nothing from the other clients
    // (wave 8 review).
    let out = sockets.out_for_stream(stream_id);
    let mut answered = 0_usize;
    let mut misses = Vec::new();
    let mut later: Vec<Vec<u8>> = Vec::new();
    let repair = {
        let mut table = table.lock().unwrap();
        let Some(subscriber) = table
            .get_mut(&stream_id)
            .and_then(|entry| entry.subscribers.get_mut(&peer))
        else {
            stats
                .nack_refused
                .fetch_add(parts.len() as u64, Ordering::Relaxed);
            return None;
        };
        if subscriber.compact.is_none() {
            stats
                .nack_refused
                .fetch_add(parts.len() as u64, Ordering::Relaxed);
            return None;
        }
        let now = Instant::now();
        for (index, part) in parts.iter().enumerate() {
            let sent = *part < subscriber.next_part
                || subscriber.cut_slots.contains(part)
                || subscriber
                    .fast
                    .as_ref()
                    .is_some_and(|fast| fast.has_fast(*part));
            if !sent || *part < subscriber.first_part {
                continue;
            }
            if !budget.has_part(now) {
                stats
                    .nack_edge_capped
                    .fetch_add((parts.len() - index) as u64, Ordering::Relaxed);
                break;
            }
            if !subscriber.nack_interval.grant_part(now) {
                break;
            }
            // A gap kept is looked for in the cache: the part may have come
            // since (a late repair); if it has not, nothing is sent again.
            let Some(kept) = sockets
                .compact()
                .get(stream_id, *part)
                .filter(|kept| !compact::is_gap(kept))
            else {
                budget.spend();
                misses.push(*part);
                answered += 1;
                continue;
            };
            if !subscriber
                .nack_interval
                .charge_bytes(now, kept.len() as u64)
            {
                break;
            }
            budget.spend();
            answered += 1;
            let short = subscriber.compact.is_some_and(|plan| plan.short_header);
            let datagram = compact_marked(&kept, 0, true, short);
            if unlocked {
                later.push(datagram);
                continue;
            }
            if matches!(
                out.send(stats, &datagram, peer),
                SendOutcome::Sent | SendOutcome::Queued
            ) {
                stats.nack_resent.fetch_add(1, Ordering::Relaxed);
            }
        }
        subscriber.repair
    };
    if !later.is_empty() {
        // One sendmmsg for the answer's parts.
        let list: Vec<(&[u8], SocketAddr)> = later
            .iter()
            .map(|datagram| (datagram.as_slice(), peer))
            .collect();
        let (delivered, _) = out.send_many(stats, true, false, &list);
        stats
            .nack_resent
            .fetch_add(delivered as u64, Ordering::Relaxed);
    }
    stats
        .nack_refused
        .fetch_add((parts.len() - answered) as u64, Ordering::Relaxed);
    (!misses.is_empty()).then_some((misses, repair))
}

/// The granted parts of a NACK that the compact ring did not hold: read
/// from the cache and sent, compact when they fit one datagram, else whole
/// as the cache-fed lane sends them, encoded on the blocking pool. Each is
/// charged to the subscriber's byte budget before it is sent.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn answer_nack_from_cache(
    sockets: &EdgeSockets,
    router: &AppRouter,
    table: &Table,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    peer: SocketAddr,
    stream_id: u64,
    parts: Vec<u64>,
    repair: RepairPolicy,
) {
    let out = sockets.out_for_stream(stream_id);
    // One handle reads every granted part. A stream that is not in the
    // cache has none of them.
    let Some(handle) = router.cache.chunk_cache.resolve_stream(stream_id) else {
        return;
    };
    let mut refused = 0_u64;
    for part in parts {
        if refused > 0 {
            refused += 1;
            continue;
        }
        let Some((bytes, _)) = router.cache.get_part_for_handle(handle, part).await else {
            continue;
        };
        let times = router.cache.part_times_for_handle(handle, part).await;
        let object = part_object_with_times(stream_id, part, bytes, times);
        if let Some(kept) = sockets.compact().of_object(&object) {
            if !charge_nack_bytes(table, stream_id, peer, kept.len() as u64) {
                refused += 1;
                continue;
            }
            // Rare (the ring had lost it): the long form, which every
            // client reads.
            let datagram = compact_marked(&kept, 0, true, false);
            if matches!(
                out.send(stats, &datagram, peer),
                SendOutcome::Sent | SendOutcome::Queued
            ) {
                stats.nack_resent.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }
        // Too large for one datagram: the whole part with its repair, as
        // the cache-fed lane sends it.
        let estimate = (object.payload.len() as u64)
            .saturating_mul(1_000 + u64::from(repair.permille))
            / 1_000;
        if !charge_nack_bytes(table, stream_id, peer, estimate) {
            refused += 1;
            continue;
        }
        let symbol_size = config.symbol_size;
        let encoded =
            tokio::task::spawn_blocking(move || encode_object(&object, symbol_size, repair)).await;
        let Ok(Ok(datagrams)) = encoded else {
            continue;
        };
        send_datagrams(out, config, stats, &datagrams, &[peer]).await;
        stats.nack_resent.fetch_add(1, Ordering::Relaxed);
    }
    stats.nack_refused.fetch_add(refused, Ordering::Relaxed);
}

async fn send_control(socket: &UdpSocket, peer: SocketAddr, control: &Control) {
    if let Err(error) = socket.send_to(&control.encode(), peer).await {
        debug!(%peer, error = %error, "edge client UDP control send failed");
    }
}

/// Why this edge sends the clients of `stream_id` elsewhere, if it does.
async fn move_reason(router: &AppRouter, stream_id: u64) -> Option<&'static str> {
    if router.lifecycle.is_draining().await {
        return Some("draining");
    }
    if router.parents_are_lost().await {
        return Some("parents-lost");
    }
    // Phase AM item 11: a region-wide silence held long enough, with a
    // failover region fresh.
    if router.region_is_lost(stream_id).await {
        return Some("region-lost");
    }
    None
}

/// The stream's newest part once the edge holds one that arrived within
/// `fresh` (zero: any), woken by the parts' arrival; at `deadline`, a stale
/// head if there is one, else `None`.
async fn wait_for_head(
    router: &AppRouter,
    stream_id: u64,
    deadline: Instant,
    fresh: Duration,
) -> Option<u64> {
    let mut updates = router.cache.part_updates.subscribe();
    let fresh_us = u64::try_from(fresh.as_micros()).unwrap_or(u64::MAX);
    let mut stale = None;
    loop {
        if let Some(head) = head_part(router, stream_id).await {
            if fresh.is_zero()
                || router
                    .cache
                    .state
                    .read()
                    .await
                    .part_arrived_within(stream_id, head, crate::now_unix_us(), fresh_us)
            {
                return Some(head);
            }
            stale = Some(head);
        }
        let now = Instant::now();
        if now >= deadline {
            return stale;
        }
        // A bound on the wait, in case an update is missed between the
        // look and the subscription's mark.
        let _ = tokio::time::timeout(
            (deadline - now).min(Duration::from_millis(50)),
            updates.changed(),
        )
        .await;
    }
}

async fn head_part(router: &AppRouter, stream_id: u64) -> Option<u64> {
    router
        .cache
        .stream_position_for_id(stream_id)
        .await
        .map(|(_, last)| last as u64)
}

/// The oldest part of a stream that a subscriber may still be sent, by its
/// playout deadline, when `oldest` is the oldest part that a subscriber
/// wants; `None` when there is no deadline (--client-udp-playout-ms 0).
/// The deadline is a length of time, measured on the parts' times (see
/// `LiveState::playout_floor`).
async fn playout_floor(
    router: &AppRouter,
    config: EdgeUdpConfig,
    stream_id: u64,
    head: u64,
    oldest: u64,
) -> Option<u64> {
    if config.playout.is_zero() {
        return None;
    }
    let playout_us = u64::try_from(config.playout.as_micros()).unwrap_or(u64::MAX);
    Some(
        router
            .cache
            .state
            .read()
            .await
            .playout_floor(stream_id, head, playout_us, oldest),
    )
}

/// The part that a new subscriber starts at. A client that moves from
/// another edge names the part that it needs next: it resumes there if the
/// part is still in the window, and otherwise at the newest part. A part
/// past the playout deadline is not worth sending: it resumes at the oldest
/// part within it (`playout_floor`).
fn first_part_for(
    from_part: Option<u64>,
    head: u64,
    window: u64,
    playout_floor: Option<u64>,
) -> u64 {
    match from_part {
        Some(part) if part <= head.saturating_add(1) && head.saturating_sub(part) < window => {
            playout_floor.map_or(part, |floor| part.max(floor))
        }
        _ => head,
    }
}

/// How far a hole at `part` lies behind the head, in time: from the parts'
/// times, else (no record of them) as a count of the stream's longest part.
async fn hole_age(router: &AppRouter, stream_id: u64, head: Option<u64>, part: u64) -> Duration {
    let Some(head) = head else {
        return Duration::ZERO;
    };
    let state = router.cache.state.read().await;
    let age_us = state.part_age_us(stream_id, part, head).unwrap_or_else(|| {
        head.saturating_sub(part)
            .saturating_mul(state.stream_part_us(stream_id))
    });
    Duration::from_micros(age_us)
}

/// How long a hole `age` behind the head may be repaired before its
/// playout deadline passes; `None` without a deadline (HOLE_WAIT then).
/// Zero: send the gap.
fn hole_budget(age: Duration, playout: Duration) -> Option<Duration> {
    if playout.is_zero() {
        return None;
    }
    Some(playout.saturating_sub(age).min(HOLE_WAIT))
}

/// Whether the edge takes a client back that it sent away for lost parents
/// within the hold-down (and forgets the moves that are older).
fn moved_back_within_hold_down(
    moved: &MovedAway,
    peer: SocketAddr,
    stream_id: u64,
    hold_down: Duration,
    now: Instant,
) -> bool {
    if hold_down.is_zero() {
        return false;
    }
    let mut moved = moved.lock().unwrap();
    moved.retain(|_, at| now.duration_since(*at) < hold_down);
    moved.contains_key(&(peer, stream_id))
}

#[allow(clippy::too_many_arguments)]
async fn admit(
    socket: Arc<UdpSocket>,
    sockets: EdgeSockets,
    router: AppRouter,
    table: Table,
    moved: MovedAway,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    rebuilds: Arc<Rebuilds>,
    peer: SocketAddr,
    stream_id: u64,
    from_part: Option<u64>,
    repair: RepairPolicy,
    fast_lane: bool,
    compact: Option<CompactPlan>,
) {
    let admitted_in = Instant::now();
    let reason = match move_reason(&router, stream_id).await {
        // A client sent away for lost parents that comes back at once has
        // found no better edge: it is taken, not sent away again.
        Some("parents-lost")
            if moved_back_within_hold_down(
                &moved,
                peer,
                stream_id,
                config.move_hold_down,
                Instant::now(),
            ) =>
        {
            stats.moves_held_down.fetch_add(1, Ordering::Relaxed);
            None
        }
        reason => reason,
    };
    if let Some(reason) = reason {
        stats.moves.fetch_add(1, Ordering::Relaxed);
        send_control(
            &socket,
            peer,
            &Control::Move {
                stream_id,
                reason: reason.into(),
            },
        )
        .await;
        return;
    }
    // Phase AM item 8: an overloaded edge takes no new listener (one that
    // moves here is taken: refused everywhere, it would be stranded).
    if config.admit_by_load && from_part.is_none() && router.edge_load.overloaded_now() {
        stats.refusals.fetch_add(1, Ordering::Relaxed);
        stats.load_refusals.fetch_add(1, Ordering::Relaxed);
        send_control(
            &socket,
            peer,
            &Control::Refused {
                stream_id,
                reason: "edge-busy".into(),
            },
        )
        .await;
        return;
    }
    // The same demand as the first playlist request of a reader (without
    // its wait for a playable copy, when admitted on the first part).
    let demand = if config.admit_on_first_part {
        router.open_playlist_demand(stream_id).await
    } else {
        router.ensure_playlist_copy(stream_id).await
    };
    if !demand {
        stats.refusals.fetch_add(1, Ordering::Relaxed);
        send_control(
            &socket,
            peer,
            &Control::Refused {
                stream_id,
                reason: "no-playlist-slot".into(),
            },
        )
        .await;
        return;
    }
    let deadline = Instant::now() + ADMIT_WAIT;
    let head = if config.admit_on_first_part {
        wait_for_head(&router, stream_id, deadline, config.fresh_head).await
    } else {
        loop {
            if let Some(head) = head_part(&router, stream_id).await {
                break Some(head);
            }
            if Instant::now() >= deadline {
                break None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let Some(head) = head else {
        stats.refusals.fetch_add(1, Ordering::Relaxed);
        send_control(
            &socket,
            peer,
            &Control::Refused {
                stream_id,
                reason: "no-such-stream".into(),
            },
        )
        .await;
        return;
    };
    let window = router.cache.window_parts_for(stream_id).await as u64;
    let floor = playout_floor(
        &router,
        config,
        stream_id,
        head,
        from_part.unwrap_or(head).min(head),
    )
    .await;
    // Wave 8 S2: a client moving here resumes where it asks while the
    // cache holds the part, late rather than missing.
    let floor = floor.filter(|_| !(config.resume_from_request && from_part.is_some()));
    let first_part = first_part_for(from_part, head, window, floor);
    if let Some(part) = from_part.filter(|part| head.saturating_sub(*part) < window) {
        if first_part > part {
            stats
                .playout_skipped_parts
                .fetch_add(first_part - part, Ordering::Relaxed);
        }
    }
    // A client that asked for the fast lane takes it from its first part:
    // the threads send it each part from there on that they forward after
    // the snapshot below, and the cache-fed lane every part that they do not
    // (older parts, a part already on its way).
    let fast = (fast_lane && sockets.fast_lane().is_some_and(|lane| lane.is_armed()))
        .then(|| FastClient::new(first_part));
    let start_sender = {
        let mut table = table.lock().unwrap();
        let entry = table.entry(stream_id).or_default();
        entry.subscribers.insert(
            peer,
            Subscriber {
                repair,
                expires_at: Instant::now() + SUBSCRIPTION_TTL,
                next_part: first_part,
                init: None,
                cut_slots: BTreeSet::new(),
                admitted_at: Instant::now(),
                moved_in: from_part.is_some(),
                catching_up: from_part.is_some(),
                held_down_counted: false,
                fast: fast.clone(),
                compact,
                first_part,
                nack_interval: NackInterval::new(Instant::now()),
                windows_repaired: VecDeque::new(),
            },
        );
        let start = !std::mem::replace(&mut entry.running, true);
        // In the threads' snapshot before the client hears OK (batched: by
        // the next rebuild, waited for below).
        if fast.is_some() && !config.batched_admission {
            fast_lane_changed(&sockets, &table);
        }
        start
    };
    if fast.is_some() && config.batched_admission {
        rebuilds.after_change(&sockets, &table).await;
    }
    stats.subscriptions.fetch_add(1, Ordering::Relaxed);
    send_control(
        &socket,
        peer,
        &Control::Subscribed {
            stream_id,
            next_part: first_part,
            compact: compact.is_some(),
        },
    )
    .await;
    // Its first part at once, not when the stream's sender next looks (it
    // may be waiting for the part after). Every lane takes a part for a
    // subscriber once, so no lane sends it again.
    if config.first_part_at_once {
        if let Some((bytes, _)) = router
            .cache
            .get_part_for_stream_id(stream_id, first_part)
            .await
        {
            deliver(
                sockets.out_for_stream(stream_id),
                sockets.compact(),
                &router,
                &table,
                config,
                &stats,
                stream_id,
                first_part,
                Some(bytes),
            )
            .await;
        }
    }
    if config.batched_admission {
        debug!(
            %peer,
            stream_id,
            first_part,
            head,
            fast_lane = fast.is_some(),
            compact = compact.map(|plan| plan.word()),
            admit_ms = admitted_in.elapsed().as_secs_f64() * 1_000.0,
            "edge client UDP subscription"
        );
    } else {
        info!(
            %peer,
            stream_id,
            first_part,
            head,
            fast_lane = fast.is_some(),
            compact = compact.map(|plan| plan.word()),
            admit_ms = admitted_in.elapsed().as_secs_f64() * 1_000.0,
            "edge client UDP subscription"
        );
    }
    if start_sender {
        tokio::spawn(run_stream_sender(
            sockets, router, table, moved, config, stats, stream_id,
        ));
    }
}

/// Send one object to each target, encoded once for each repair policy that
/// the targets asked for: one subscriber on a lossy path does not raise the
/// repair that the others receive.
async fn send_object(
    out: ClientOut<'_>,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    object: &PartObject,
    targets: &[(SocketAddr, RepairPolicy)],
) {
    let mut groups: HashMap<RepairPolicy, Vec<SocketAddr>> = HashMap::new();
    for (peer, repair) in targets {
        groups.entry(*repair).or_default().push(*peer);
    }
    for (repair, peers) in groups {
        let datagrams = match encode_object(object, config.symbol_size, repair) {
            Ok(datagrams) => datagrams,
            Err(error) => {
                warn!(stream_id = object.stream_id, part = object.part, error = %error,
                      "edge client UDP could not encode a part");
                return;
            }
        };
        send_datagrams(out, config, stats, &datagrams, &peers).await;
    }
}

/// Send the datagrams of one encoded object to each of `peers`.
async fn send_datagrams(
    out: ClientOut<'_>,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    datagrams: &[Vec<u8>],
    peers: &[SocketAddr],
) {
    // Interleave the subscribers, so that one of them does not wait for
    // the whole of a large part to be sent to the others.
    if config.batch_send && out.queue.is_some() {
        let batch: Vec<(&[u8], SocketAddr)> = datagrams
            .iter()
            .flat_map(|datagram| peers.iter().map(move |peer| (datagram.as_slice(), *peer)))
            .collect();
        out.send_many(stats, true, false, &batch);
        return;
    }
    for datagram in datagrams {
        for peer in peers {
            if out.queue.is_some() {
                // A full socket queues the datagram for this client and
                // does not hold up the others.
                out.send(stats, datagram, *peer);
                continue;
            }
            match out.socket.send_to(datagram, *peer).await {
                Ok(sent) => {
                    stats.datagrams.fetch_add(1, Ordering::Relaxed);
                    stats.bytes.fetch_add(sent as u64, Ordering::Relaxed);
                }
                Err(error) => {
                    stats.send_errors.fetch_add(1, Ordering::Relaxed);
                    debug!(%peer, error = %error, "edge client UDP send failed");
                }
            }
        }
    }
}

async fn part_object(router: &AppRouter, stream_id: u64, part: u64, bytes: Bytes) -> PartObject {
    let times = router.cache.part_times_unix_us(stream_id, part).await;
    part_object_with_times(stream_id, part, bytes, times)
}

fn part_object_with_times(
    stream_id: u64,
    part: u64,
    bytes: Bytes,
    times: Option<crate::PartTimes>,
) -> PartObject {
    PartObject {
        kind: ObjectKind::Part,
        datagrams: 0,
        stream_id,
        part,
        available_unix_us: times.as_ref().map_or(0, |times| times.available_unix_us),
        published_unix_us: times.and_then(|times| times.published_unix_us).unwrap_or(0),
        payload: bytes,
    }
}

fn gap_object(stream_id: u64, part: u64) -> PartObject {
    PartObject {
        kind: ObjectKind::Gap,
        datagrams: 0,
        stream_id,
        part,
        available_unix_us: 0,
        published_unix_us: 0,
        payload: Bytes::new(),
    }
}

/// Send the init segment of an fMP4 stream to each target that does not
/// have this one yet.
#[allow(clippy::too_many_arguments)]
async fn send_init_if_new(
    out: ClientOut<'_>,
    router: &AppRouter,
    table: &Table,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    stream_id: u64,
    part: u64,
    targets: &[(SocketAddr, RepairPolicy)],
) {
    let Some(init) = router.cache.get_init_for_stream_id(stream_id).await else {
        return;
    };
    let needing: Vec<(SocketAddr, RepairPolicy)> = {
        let mut table = table.lock().unwrap();
        let Some(entry) = table.get_mut(&stream_id) else {
            return;
        };
        targets
            .iter()
            .filter(|(peer, _)| {
                entry.subscribers.get_mut(peer).is_some_and(|subscriber| {
                    if subscriber.init.as_ref() == Some(&init) {
                        false
                    } else {
                        subscriber.init = Some(init.clone());
                        true
                    }
                })
            })
            .copied()
            .collect()
    };
    if needing.is_empty() {
        return;
    }
    let object = PartObject {
        kind: ObjectKind::Init,
        datagrams: 0,
        stream_id,
        part,
        available_unix_us: 0,
        published_unix_us: 0,
        payload: init,
    };
    send_object(out, config, stats, &object, &needing).await;
}

/// Subscribers to send an object to, with their repair.
type Targets = Vec<(SocketAddr, RepairPolicy)>;

/// The subscribers due for a part.
#[derive(Default)]
struct Due {
    /// On the RaptorQ lane.
    blocks: Targets,
    /// On the compact lane, with their plans.
    compact: Vec<(SocketAddr, RepairPolicy, CompactPlan)>,
    /// Those that the fast lane carried the part to: they need only its
    /// init segment.
    on_fast_lane: Targets,
}

/// The subscribers due for `part`, after the part has been taken for them.
/// A subscriber that cut-through carries the part to is passed over, unless
/// `gap`: a gap goes to every subscriber due that the fast lane did not
/// carry the part to.
fn take_due(table: &Table, stream_id: u64, part: u64, gap: bool) -> Due {
    let mut table = table.lock().unwrap();
    let Some(entry) = table.get_mut(&stream_id) else {
        return Due::default();
    };
    let mut due = Due::default();
    for (peer, subscriber) in entry
        .subscribers
        .iter_mut()
        .filter(|(_, subscriber)| subscriber.next_part == part)
    {
        subscriber.next_part = part + 1;
        let cut = subscriber.cut_slots.remove(&part);
        while subscriber
            .cut_slots
            .first()
            .is_some_and(|slot| *slot < subscriber.next_part)
        {
            subscriber.cut_slots.pop_first();
        }
        if subscriber
            .fast
            .as_ref()
            .is_some_and(|fast| !fast.claim_cache(part))
        {
            due.on_fast_lane.push((*peer, subscriber.repair));
            continue;
        }
        if !cut || gap {
            match subscriber.compact {
                Some(plan) => due.compact.push((*peer, subscriber.repair, plan)),
                None => due.blocks.push((*peer, subscriber.repair)),
            }
        }
    }
    due
}

/// Send a compact datagram to each target, with the copies of its plan.
async fn send_compact(
    out: ClientOut<'_>,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    canonical: &[u8],
    targets: &[(SocketAddr, RepairPolicy, CompactPlan)],
) {
    let mut marked: HashMap<(u8, bool), Vec<u8>> = HashMap::new();
    for (_, _, plan) in targets {
        marked
            .entry((plan.copies, plan.short_header))
            .or_insert_with(|| compact_marked(canonical, plan.copies, false, plan.short_header));
    }
    let list: Vec<(&[u8], SocketAddr)> = targets
        .iter()
        .flat_map(|(peer, _, plan)| {
            let datagram = marked[&(plan.copies, plan.short_header)].as_slice();
            std::iter::repeat_n((datagram, *peer), 1 + usize::from(plan.copies))
        })
        .collect();
    stats
        .compact_parts
        .fetch_add(targets.len() as u64, Ordering::Relaxed);
    stats
        .compact_copies
        .fetch_add((list.len() - targets.len()) as u64, Ordering::Relaxed);
    send_list(out, config, stats, &list).await;
}

/// Send each datagram to its client: batched when the socket has a send
/// queue (never waits), else one send each, waiting for the socket.
async fn send_list(
    out: ClientOut<'_>,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    list: &[(&[u8], SocketAddr)],
) {
    if out.queue.is_some() {
        out.send_many(stats, config.batch_send, false, list);
        return;
    }
    for (datagram, peer) in list {
        match out.socket.send_to(datagram, *peer).await {
            Ok(sent) => {
                stats.datagrams.fetch_add(1, Ordering::Relaxed);
                stats.bytes.fetch_add(sent as u64, Ordering::Relaxed);
            }
            Err(error) => {
                stats.send_errors.fetch_add(1, Ordering::Relaxed);
                debug!(%peer, error = %error, "edge client UDP send failed");
            }
        }
    }
}

/// Windows whose repair each subscriber remembers having been sent.
const WINDOWS_REPAIRED: usize = 16;

/// After `part` went to its clients, on any lane (the fast lane's class
/// thread, cut-through or the cache-fed lane): the window repair of the
/// window of each compact plan that `part` completes in the compact ring
/// (its last part, or a late part of a window whose last part is there),
/// to each subscriber whose plan asks for it, that was sent the whole
/// window (by any lane) and that has not had it. A window with a part that
/// the edge holds no compact datagram of (a part too large for one) has
/// none. `windows`: the plans' windows, if the caller knows them (a part
/// that completes none of them costs no lock of the table).
pub(crate) fn window_repair_now(
    sockets: &EdgeSockets,
    table: &Table,
    batch: bool,
    stats: &EdgeUdpStats,
    stream_id: u64,
    part: u64,
    windows: Option<&[u8]>,
) {
    let Some(ring) = sockets.compact().existing_ring(stream_id) else {
        return;
    };
    let completes = |window: u8| {
        let window = u64::from(window.max(1));
        let first = part - part % window;
        let end = first + window - 1;
        (part == end || ring.get(end).is_some()) && (first..=end).all(|p| ring.get(p).is_some())
    };
    if let Some(windows) = windows {
        if !windows.iter().any(|window| completes(*window)) {
            return;
        }
    }
    // (peer, first part, window, rows)
    let wanted: Vec<(SocketAddr, u64, u8, u8)> = {
        let mut table = table.lock().unwrap();
        let Some(entry) = table.get_mut(&stream_id) else {
            return;
        };
        let mut complete: HashMap<u8, bool> = HashMap::new();
        let mut wanted = Vec::new();
        for (peer, subscriber) in entry.subscribers.iter_mut() {
            let Some(plan) = subscriber.compact else {
                continue;
            };
            if plan.window_repair == 0 {
                continue;
            }
            let window = u64::from(plan.window);
            let first = part - part % window;
            if subscriber.first_part > first || subscriber.windows_repaired.contains(&first) {
                continue;
            }
            if !*complete
                .entry(plan.window)
                .or_insert_with(|| completes(plan.window))
            {
                continue;
            }
            let sent = (first..first + window).all(|p| {
                p < subscriber.next_part
                    || subscriber.cut_slots.contains(&p)
                    || subscriber
                        .fast
                        .as_ref()
                        .is_some_and(|fast| fast.has_fast(p))
            });
            if !sent {
                continue;
            }
            subscriber.windows_repaired.push_back(first);
            if subscriber.windows_repaired.len() > WINDOWS_REPAIRED {
                subscriber.windows_repaired.pop_front();
            }
            wanted.push((*peer, first, plan.window, plan.window_repair));
        }
        wanted
    };
    if wanted.is_empty() {
        return;
    }
    let mut windows: HashMap<(u64, u8), Vec<(SocketAddr, u8)>> = HashMap::new();
    for (peer, first, window, rows) in wanted {
        windows
            .entry((first, window))
            .or_default()
            .push((peer, rows));
    }
    let out = sockets.out_for_stream(stream_id);
    for ((first, window), peers) in windows {
        let Some(symbols) = (first..first + u64::from(window))
            .map(|part| ring.get(part))
            .collect::<Option<Vec<Bytes>>>()
        else {
            continue;
        };
        let symbols: Vec<&[u8]> = symbols.iter().map(|symbol| symbol.as_ref()).collect();
        let rows = peers.iter().map(|(_, rows)| *rows).max().unwrap_or(0);
        let repair = compact::encode_window_repair(first, &symbols, rows);
        // Each client is told the rows that it gets (its loss count reads
        // them): the rows once for each count of rows asked for.
        let mut by_rows: HashMap<u8, Vec<Vec<u8>>> = HashMap::new();
        for (_, rows) in &peers {
            by_rows.entry(*rows).or_insert_with(|| {
                repair
                    .iter()
                    .take(usize::from(*rows))
                    .map(|datagram| compact::with_rows_sent(datagram, *rows))
                    .collect()
            });
        }
        let list: Vec<(&[u8], SocketAddr)> = peers
            .iter()
            .flat_map(|(peer, rows)| {
                by_rows[rows]
                    .iter()
                    .map(move |datagram| (datagram.as_slice(), *peer))
            })
            .collect();
        stats
            .compact_window_repair
            .fetch_add(list.len() as u64, Ordering::Relaxed);
        if out.queue.is_some() {
            out.send_many(stats, batch, false, &list);
        } else {
            out.send_each(stats, &list);
        }
    }
}

/// Send `part` to the subscribers due for it on this lane; `true` if it
/// went to any (a part that the fast lane carried to every subscriber
/// needs no more here, its window repair included).
#[allow(clippy::too_many_arguments)]
async fn deliver(
    out: ClientOut<'_>,
    store: &CompactStore,
    router: &AppRouter,
    table: &Table,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    stream_id: u64,
    part: u64,
    bytes: Option<Bytes>,
) -> bool {
    let Due {
        blocks: mut targets,
        compact: compact_targets,
        on_fast_lane,
    } = take_due(table, stream_id, part, bytes.is_none());
    if bytes.is_some() && !on_fast_lane.is_empty() {
        // The fast lane carries parts, never the init segment: it goes on
        // this lane, in space 2.
        send_init_if_new(
            out,
            router,
            table,
            config,
            stats,
            stream_id,
            part,
            &on_fast_lane,
        )
        .await;
    }
    if targets.is_empty() && compact_targets.is_empty() {
        return false;
    }
    let object = match bytes {
        Some(bytes) => {
            let everyone: Targets = targets
                .iter()
                .copied()
                .chain(
                    compact_targets
                        .iter()
                        .map(|(peer, repair, _)| (*peer, *repair)),
                )
                .collect();
            send_init_if_new(
                out, router, table, config, stats, stream_id, part, &everyone,
            )
            .await;
            stats.parts.fetch_add(1, Ordering::Relaxed);
            part_object(router, stream_id, part, bytes).await
        }
        None => {
            stats.gaps.fetch_add(1, Ordering::Relaxed);
            gap_object(stream_id, part)
        }
    };
    let is_part = object.kind == ObjectKind::Part;
    if !compact_targets.is_empty() {
        match store.of_object(&object) {
            Some(canonical) => {
                send_compact(out, config, stats, &canonical, &compact_targets).await;
                if is_part {
                    out.journal_sent(stream_id, part, compact_targets.first().map(|t| t.0));
                }
            }
            // Too large for one datagram: a RaptorQ object, as for the rest.
            None => targets.extend(
                compact_targets
                    .iter()
                    .map(|(peer, repair, _)| (*peer, *repair)),
            ),
        }
    }
    if !targets.is_empty() {
        send_object(out, config, stats, &object, &targets).await;
        if is_part {
            out.journal_sent(stream_id, part, targets.first().map(|t| t.0));
        }
    }
    true
}

async fn run_stream_sender(
    sockets: EdgeSockets,
    router: AppRouter,
    table: Table,
    moved: MovedAway,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    stream_id: u64,
) {
    let mut housekeeping_at = Instant::now();
    let mut last_sent = Instant::now();
    // The edge has lost its parents but keeps subscribers within the move
    // hold-down: it serves them from its cache alone, without repair.
    let mut held_while_lost = false;
    // Phase AM item 9: the drain in progress (its start, the subscribers
    // there were, those sent away so far).
    let mut drain: Option<DrainPace> = None;
    loop {
        let now = Instant::now();
        // The stream's class can become known after it started.
        let socket = Arc::clone(sockets.for_stream(stream_id));
        let out = sockets.out_for_stream(stream_id);
        let head = head_part(&router, stream_id).await;
        let window = router.cache.window_parts_for(stream_id).await as u64;
        let oldest_wanted = {
            let guard = table.lock().unwrap();
            guard.get(&stream_id).and_then(|entry| {
                entry
                    .subscribers
                    .values()
                    .map(|subscriber| subscriber.next_part)
                    .min()
            })
        };
        let playout = match (head, oldest_wanted) {
            (Some(head), Some(oldest)) => {
                playout_floor(&router, config, stream_id, head, oldest.min(head)).await
            }
            _ => None,
        };
        // Subscribers that stopped refreshing end; with none left, so does
        // the sender. A subscriber whose next part has left the window
        // continues at the newest part, and one whose next part is past its
        // playout deadline (after a move, or a backlog) at the oldest part
        // within it.
        let (oldest_next, newest_next, peers) = {
            let mut guard = table.lock().unwrap();
            let Some(entry) = guard.get_mut(&stream_id) else {
                return;
            };
            let mut fast_expired = false;
            entry.subscribers.retain(|_, subscriber| {
                let live = subscriber.expires_at > now;
                fast_expired |= !live && subscriber.fast.is_some();
                live
            });
            let empty = entry.subscribers.is_empty();
            if empty {
                guard.remove(&stream_id);
            }
            if fast_expired {
                fast_lane_changed(&sockets, &guard);
            }
            if empty {
                sockets.compact().forget(stream_id);
                debug!(stream_id, "edge client UDP sender stops: no subscriber");
                return;
            }
            let Some(entry) = guard.get_mut(&stream_id) else {
                return;
            };
            if let Some(head) = head {
                for subscriber in entry.subscribers.values_mut() {
                    let skipped = hold_to_playout(
                        subscriber,
                        head,
                        window,
                        playout,
                        config.resume_from_request,
                    );
                    if skipped > 0 {
                        stats
                            .playout_skipped_parts
                            .fetch_add(skipped, Ordering::Relaxed);
                    }
                }
            }
            let nexts = entry
                .subscribers
                .values()
                .map(|subscriber| subscriber.next_part);
            (
                nexts.clone().min().unwrap_or(0),
                nexts.max().unwrap_or(0),
                entry.subscribers.keys().copied().collect::<Vec<_>>(),
            )
        };

        if now >= housekeeping_at {
            housekeeping_at = now + HOUSEKEEPING;
            held_while_lost = false;
            let reason = move_reason(&router, stream_id).await;
            let paced = reason == Some("draining") && !config.drain_spread.is_zero();
            if !paced {
                drain = None;
            }
            if let Some(reason) = reason {
                // Lost parents do not move a subscriber that came by a
                // move within the hold-down: it may have just come from an
                // edge that lost its parents too. A subscriber that did not
                // move here goes at once, and draining moves every one
                // (spread over --client-udp-drain-spread-ms).
                let hold_down = if reason == "parents-lost" {
                    config.move_hold_down
                } else {
                    Duration::ZERO
                };
                let (leaving, kept, newly_held) = {
                    let mut guard = table.lock().unwrap();
                    match guard.get_mut(&stream_id) {
                        Some(entry) => {
                            let limit = paced.then(|| {
                                let pace = drain.get_or_insert_with(|| DrainPace {
                                    started: now,
                                    initial: entry.subscribers.len(),
                                    sent: 0,
                                });
                                pace.due(now, config.drain_spread)
                            });
                            let leaving = take_movable(entry, hold_down, now, limit);
                            if let Some(pace) = drain.as_mut() {
                                pace.sent += leaving.len();
                            }
                            let kept = entry.subscribers.len();
                            let newly_held = count_held_down(entry);
                            if kept == 0 {
                                guard.remove(&stream_id);
                            }
                            if !leaving.is_empty() {
                                fast_lane_changed(&sockets, &guard);
                            }
                            (leaving, kept, newly_held)
                        }
                        None => (Vec::new(), 0, 0),
                    }
                };
                if newly_held > 0 {
                    stats
                        .moves_held_down
                        .fetch_add(newly_held, Ordering::Relaxed);
                }
                if !leaving.is_empty() {
                    stats
                        .moves
                        .fetch_add(leaving.len() as u64, Ordering::Relaxed);
                    if reason == "region-lost" {
                        stats
                            .region_lost_moves
                            .fetch_add(leaving.len() as u64, Ordering::Relaxed);
                    }
                    info!(
                        stream_id,
                        reason,
                        clients = leaving.len(),
                        kept,
                        "edge client UDP moves its clients"
                    );
                    if !hold_down.is_zero() {
                        let mut moved = moved.lock().unwrap();
                        for peer in &leaving {
                            moved.insert((*peer, stream_id), now);
                        }
                    }
                }
                for peer in leaving {
                    send_control(
                        &socket,
                        peer,
                        &Control::Move {
                            stream_id,
                            reason: reason.into(),
                        },
                    )
                    .await;
                }
                if kept == 0 {
                    return;
                }
                if paced {
                    // The next share soon; those not yet sent away are
                    // served as before (their parts, repaired).
                    housekeeping_at = Instant::now() + DRAIN_TICK;
                } else {
                    held_while_lost = true;
                    continue;
                }
            } else {
                // Keep the playlist held, as a reader's playlist reloads do.
                router.ensure_playlist_copy(stream_id).await;
                router
                    .request_replica_for_stream(stream_id, "udp-client-demand", None)
                    .await;
            }
        }

        let part = oldest_next;
        let behind_head = head.is_some_and(|head| part <= head);
        if behind_head || part < newest_next {
            // A part that the stream has passed: send it from the cache, or
            // repair it from the parents while it is within the playout
            // deadline (HOLE_WAIT without one), or send a gap.
            let bytes = match router.cache.get_part_for_stream_id(stream_id, part).await {
                Some((bytes, _)) => Some(bytes),
                None if held_while_lost => None,
                None => {
                    let age = hole_age(&router, stream_id, head, part).await;
                    let mut wait = hole_budget(age, config.playout).unwrap_or(HOLE_WAIT);
                    // The repair horizon (wave 5, W2): a hole holds the
                    // stream's later parts back no longer than the cache
                    // does, and a part that the cache listed as a gap is
                    // sent as one at once.
                    if let Some(horizon) = router.cache.live_hole_wait(stream_id, part, age) {
                        wait = wait.min(horizon);
                    }
                    if wait.is_zero() {
                        stats.holes_past_playout.fetch_add(1, Ordering::Relaxed);
                        None
                    } else {
                        match tokio::time::timeout(
                            wait,
                            router.get_part_repairing_holes(stream_id, part),
                        )
                        .await
                        {
                            Ok(Ok(Some((bytes, _)))) => Some(bytes),
                            Ok(Err(ParentsLost)) => {
                                housekeeping_at = Instant::now();
                                continue;
                            }
                            _ => None,
                        }
                    }
                }
            };
            let sent = deliver(
                out,
                sockets.compact(),
                &router,
                &table,
                config,
                &stats,
                stream_id,
                part,
                bytes,
            )
            .await;
            // A part that the fast lane carried to every subscriber had its
            // window repair there.
            if sent {
                window_repair_now(
                    &sockets,
                    &table,
                    config.batch_send,
                    &stats,
                    stream_id,
                    part,
                    None,
                );
            }
            last_sent = Instant::now();
            continue;
        }

        // The newest part: wait for it as an LL-HLS part request does, for a
        // short while at a time (no longer than the keepalive, whose WAIT
        // goes when a wait ends with nothing).
        let keepalive = if config.keepalive.is_zero() {
            KEEPALIVE
        } else {
            config.keepalive
        };
        let live_wait = LIVE_WAIT.min(keepalive);
        let waited = if held_while_lost {
            Ok(Ok(router
                .cache
                .get_part_until_for_stream_id(stream_id, part, Instant::now() + live_wait)
                .await))
        } else {
            // With a repair horizon, the newest part may be a hole that the
            // cache releases as a gap within the horizon: the wait is that
            // long at a time, so that the parts behind the hole are not held
            // for LIVE_WAIT (the wait does not wake for a release).
            let mut slice = router
                .cache
                .repair_horizon_us()
                .map_or(live_wait, |horizon_us| {
                    live_wait.min(Duration::from_micros(horizon_us).max(Duration::from_millis(1)))
                });
            if drain.is_some() {
                // A paced drain's next share is due at its tick.
                slice = slice.min(
                    housekeeping_at
                        .saturating_duration_since(Instant::now())
                        .max(Duration::from_millis(1)),
                );
            }
            tokio::time::timeout(slice, router.get_part_repairing_holes(stream_id, part)).await
        };
        match waited {
            Ok(Ok(Some((bytes, _)))) => {
                let sent = deliver(
                    out,
                    sockets.compact(),
                    &router,
                    &table,
                    config,
                    &stats,
                    stream_id,
                    part,
                    Some(bytes),
                )
                .await;
                if sent {
                    window_repair_now(
                        &sockets,
                        &table,
                        config.batch_send,
                        &stats,
                        stream_id,
                        part,
                        None,
                    );
                }
                last_sent = Instant::now();
            }
            Ok(Err(ParentsLost)) => {
                // Housekeeping sends the clients away.
                housekeeping_at = Instant::now();
            }
            Ok(Ok(None)) | Err(_) => {
                if last_sent.elapsed() >= keepalive {
                    // A quiet stream is not a dead edge.
                    for peer in &peers {
                        send_control(
                            &socket,
                            *peer,
                            &Control::Waiting {
                                stream_id,
                                next_part: part,
                            },
                        )
                        .await;
                    }
                    last_sent = Instant::now();
                }
            }
        }
    }
}

/// A subscriber's next part against the stream's `head`: a part the window
/// no longer holds jumps to the head, and one older than the playout
/// `floor` to the floor, except for a client that moved here and is still
/// catching up from the part it named (wave 8 S2, with
/// `resume_from_request`). Returns the parts skipped for the floor.
fn hold_to_playout(
    subscriber: &mut Subscriber,
    head: u64,
    window: u64,
    floor: Option<u64>,
    resume_from_request: bool,
) -> u64 {
    if head.saturating_sub(subscriber.next_part) >= window {
        subscriber.next_part = head;
        subscriber.catching_up = false;
        return 0;
    }
    let Some(floor) = floor else {
        return 0;
    };
    if subscriber.next_part >= floor {
        subscriber.catching_up = false;
        return 0;
    }
    if resume_from_request && subscriber.catching_up {
        return 0;
    }
    let skipped = floor - subscriber.next_part;
    subscriber.next_part = floor;
    skipped
}

/// Subscribers of `stream_id` that no client stands behind (a test of a
/// crowd larger than its sockets): each asks for `next_part` on.
#[cfg(test)]
pub(crate) fn insert_test_subscribers(
    table: &Table,
    stream_id: u64,
    peers: &[SocketAddr],
    next_part: u64,
) {
    let now = Instant::now();
    let mut guard = table.lock().unwrap();
    let entry = guard.entry(stream_id).or_default();
    for peer in peers {
        entry.subscribers.insert(
            *peer,
            Subscriber {
                repair: RepairPolicy {
                    permille: 0,
                    min_symbols: 0,
                    loss_ppm: 0,
                },
                expires_at: now + SUBSCRIPTION_TTL,
                next_part,
                init: None,
                cut_slots: BTreeSet::new(),
                admitted_at: now,
                moved_in: false,
                catching_up: false,
                held_down_counted: false,
                fast: None,
                compact: None,
                first_part: next_part,
                nack_interval: NackInterval::new(now),
                windows_repaired: VecDeque::new(),
            },
        );
    }
}

/// How often a paced drain sends its next share (Phase AM item 9).
const DRAIN_TICK: Duration = Duration::from_millis(50);

/// A paced drain of one stream's subscribers (Phase AM item 9): by
/// `started + spread` every subscriber that was there at the start has
/// been sent away, a share at each tick, the first at once.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DrainPace {
    pub(crate) started: Instant,
    pub(crate) initial: usize,
    pub(crate) sent: usize,
}

impl DrainPace {
    /// How many to send away now: the share due by the next tick, less
    /// those sent; everyone at the end of the spread (and any who came
    /// since the start).
    pub(crate) fn due(&self, now: Instant, spread: Duration) -> usize {
        let elapsed = now.saturating_duration_since(self.started) + DRAIN_TICK;
        if spread.is_zero() || elapsed >= spread {
            return usize::MAX;
        }
        let share = (self.initial as u128 * elapsed.as_micros())
            .div_ceil(spread.as_micros().max(1)) as usize;
        share.max(1).saturating_sub(self.sent)
    }
}

/// Take the subscribers that may be moved: all of them, or with a
/// hold-down all but those that came by a move less than that long ago;
/// at most `limit` of them, if given (a paced drain).
fn take_movable(
    entry: &mut StreamEntry,
    hold_down: Duration,
    now: Instant,
    limit: Option<usize>,
) -> Vec<SocketAddr> {
    let leaving: Vec<SocketAddr> = entry
        .subscribers
        .iter()
        .filter(|(_, subscriber)| {
            !subscriber.moved_in || now.duration_since(subscriber.admitted_at) >= hold_down
        })
        .map(|(peer, _)| *peer)
        .take(limit.unwrap_or(usize::MAX))
        .collect();
    for peer in &leaving {
        entry.subscribers.remove(peer);
    }
    leaving
}

/// The subscribers left after `take_movable` are held within the
/// hold-down: count each of them once, the first time it is held.
fn count_held_down(entry: &mut StreamEntry) -> u64 {
    let mut newly = 0;
    for subscriber in entry.subscribers.values_mut() {
        if !std::mem::replace(&mut subscriber.held_down_counted, true) {
            newly += 1;
        }
    }
    newly
}

// ------------------------------------------------------------------ cut-through

/// Forwards the parts that reach this edge over the cache mesh to its UDP
/// clients as their datagrams arrive.
///
/// The cache mesh sends a part as one systematic RaptorQ block of a chunk
/// frame, so each source symbol is a slice of the frame. When a source symbol
/// arrives it is sent on at once, cut into client symbols (a divisor of the
/// mesh symbol size that fits a client datagram), with no wait for the rest
/// of the block and no timer. When the edge has decoded the block, it sends
/// each client the source symbols that it never received and the repair of
/// the client's policy, from the same frame in the same symbols. A part is
/// cut through from the first parent block that names it; if another
/// parent's block of the part decodes first, that block completes it.
///
/// It is registered with the cache mesh at start, before the client socket
/// exists, and armed by [`run_edge_udp`].
#[derive(Default)]
pub(crate) struct CutThrough {
    inner: OnceLock<CutInner>,
}

struct CutInner {
    sockets: EdgeSockets,
    table: Table,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    state: Mutex<CutState>,
}

#[derive(Default)]
struct CutState {
    /// Each block of a parent seen: where its symbols go.
    routes: HashMap<(SocketAddr, u32), Route>,
    /// Datagrams of blocks whose first symbol (which names the part) has not
    /// arrived yet.
    pending: HashMap<(SocketAddr, u32), (Instant, Vec<Vec<u8>>)>,
    /// Parts that the edge decoded before any block of theirs was claimed:
    /// a later block of the part must not claim it (the clients would wait
    /// for a block that may never decode, and the cache sender has sent it).
    decoded_unclaimed: HashMap<(u64, u64), Instant>,
    /// Each part claimed for cut-through.
    slots: HashMap<(u64, u64), SlotState>,
    pruned_at: Option<Instant>,
}

struct Route {
    stream_id: u64,
    slot: u64,
    /// This block carries the part to the clients.
    chosen: bool,
    client_symbol: u16,
    mesh_symbol: u16,
    transfer_length: u32,
    forwarded: Vec<bool>,
    sequence: u32,
    seen_at: Instant,
}

struct SlotState {
    chosen: (SocketAddr, u32),
    done: bool,
    clients: Vec<(SocketAddr, RepairPolicy)>,
    seen_at: Instant,
}

const CUT_STATE_TTL: Duration = Duration::from_secs(5);
const MAX_PENDING_PER_BLOCK: usize = 512;
const MAX_PENDING_BLOCKS: usize = 1_024;

impl CutThrough {
    pub(crate) fn arm(
        &self,
        sockets: EdgeSockets,
        table: Table,
        config: EdgeUdpConfig,
        stats: Arc<EdgeUdpStats>,
    ) {
        let _ = self.inner.set(CutInner {
            sockets,
            table,
            config,
            stats,
            state: Mutex::new(CutState::default()),
        });
    }
}

/// Send to every client now. `false` if a send found the socket full: the
/// symbol is then sent again with the part's repair.
fn send_now(
    inner: &CutInner,
    stream_id: u64,
    datagram: &[u8],
    clients: &[(SocketAddr, RepairPolicy)],
) -> bool {
    let out = inner.sockets.out_for_stream(stream_id);
    if inner.config.batch_send {
        let batch: Vec<(&[u8], SocketAddr)> =
            clients.iter().map(|(peer, _)| (datagram, *peer)).collect();
        let (delivered, blocked) = out.send_many(&inner.stats, true, false, &batch);
        inner
            .stats
            .cut_through_datagrams
            .fetch_add(delivered as u64, Ordering::Relaxed);
        return blocked == 0;
    }
    let mut all = true;
    for (peer, _) in clients {
        match out.send(&inner.stats, datagram, *peer) {
            SendOutcome::Sent | SendOutcome::Queued => {
                inner
                    .stats
                    .cut_through_datagrams
                    .fetch_add(1, Ordering::Relaxed);
            }
            SendOutcome::Blocked => all = false,
            SendOutcome::Failed => {}
        }
    }
    all
}

/// Send a compact datagram to each client now, with its copies.
fn send_compact_now(
    inner: &CutInner,
    stream_id: u64,
    part: u64,
    canonical: &[u8],
    clients: &[(SocketAddr, RepairPolicy, CompactPlan)],
) {
    if clients.is_empty() {
        return;
    }
    let forms: &[bool] = if clients.iter().any(|(_, _, plan)| plan.short_header) {
        &[false, true]
    } else {
        &[false]
    };
    let marked: Vec<Vec<u8>> = forms
        .iter()
        .copied()
        .flat_map(|short| {
            (0..=compact::MAX_COPIES)
                .map(move |copies| compact_marked(canonical, copies, false, short))
        })
        .collect();
    let variants = usize::from(compact::MAX_COPIES) + 1;
    let list: Vec<(&[u8], SocketAddr)> = clients
        .iter()
        .flat_map(|(peer, _, plan)| {
            let variant = usize::from(plan.copies) + usize::from(plan.short_header) * variants;
            let datagram = marked[variant].as_slice();
            std::iter::repeat_n((datagram, *peer), 1 + usize::from(plan.copies))
        })
        .collect();
    inner
        .stats
        .compact_parts
        .fetch_add(clients.len() as u64, Ordering::Relaxed);
    inner
        .stats
        .compact_copies
        .fetch_add((list.len() - clients.len()) as u64, Ordering::Relaxed);
    let out = inner.sockets.out_for_stream(stream_id);
    let (delivered, _) = if out.queue.is_some() {
        out.send_many(&inner.stats, inner.config.batch_send, false, &list)
    } else {
        out.send_each(&inner.stats, &list)
    };
    out.journal_sent(stream_id, part, clients.first().map(|c| c.0));
    inner
        .stats
        .cut_through_datagrams
        .fetch_add(delivered as u64, Ordering::Relaxed);
}

impl CutState {
    fn prune(&mut self, now: Instant) {
        if self
            .pruned_at
            .is_some_and(|at| now.duration_since(at) < Duration::from_secs(1))
        {
            return;
        }
        self.pruned_at = Some(now);
        self.routes
            .retain(|_, route| now.duration_since(route.seen_at) < CUT_STATE_TTL);
        self.slots
            .retain(|_, slot| now.duration_since(slot.seen_at) < CUT_STATE_TTL);
        let routes = &self.routes;
        self.pending.retain(|key, (since, _)| {
            !routes.contains_key(key) && now.duration_since(*since) < Duration::from_secs(1)
        });
        self.decoded_unclaimed
            .retain(|_, at| now.duration_since(*at) < CUT_STATE_TTL);
        if self.pending.len() > MAX_PENDING_BLOCKS {
            self.pending.clear();
        }
    }
}

/// Forward one mesh source symbol of a chosen route, cut into client symbols.
fn forward_symbol(
    inner: &CutInner,
    route: &mut Route,
    clients: &[(SocketAddr, RepairPolicy)],
    esi: u32,
    data: &[u8],
) {
    let per_mesh = u32::from(route.mesh_symbol / route.client_symbol).max(1);
    let client_size = usize::from(route.client_symbol);
    for (index, piece) in data.chunks(client_size).enumerate() {
        let client_esi = esi * per_mesh + index as u32;
        let Some(done) = route.forwarded.get_mut(client_esi as usize) else {
            break;
        };
        if *done {
            continue;
        }
        let Some(datagram) = source_datagram(
            cut_through_block_id(route.slot),
            route.transfer_length,
            route.sequence,
            route.client_symbol,
            client_esi,
            piece,
        ) else {
            continue;
        };
        route.sequence = route.sequence.wrapping_add(1);
        *done = send_now(inner, route.stream_id, &datagram, clients);
        if client_esi == 0 {
            inner.sockets.out_for_stream(route.stream_id).journal_sent(
                route.stream_id,
                route.slot,
                clients.first().map(|c| c.0),
            );
        }
    }
}

impl CacheMeshReceiveObserver for CutThrough {
    fn datagram(&self, peer: SocketAddr, datagram: &[u8]) {
        let Some(inner) = self.inner.get() else {
            return;
        };
        let Ok(header) = DatagramFecHeader::decode(datagram) else {
            return;
        };
        let Ok(payload) = header.payload(datagram) else {
            return;
        };
        if payload.len() < 4 {
            return;
        }
        let esi = u32::from_be_bytes([0, payload[1], payload[2], payload[3]]);
        if esi >= u32::from(header.source_symbols) {
            return; // a mesh repair symbol: the edge sends its own repair
        }
        let key = (peer, header.block_id);
        let now = Instant::now();
        let mut state = inner.state.lock().unwrap();
        state.prune(now);
        if !state.routes.contains_key(&key) {
            if esi != 0 {
                // The first symbol names the part; until it arrives, hold.
                let (_, held) = state
                    .pending
                    .entry(key)
                    .or_insert_with(|| (now, Vec::new()));
                if held.len() < MAX_PENDING_PER_BLOCK {
                    held.push(datagram.to_vec());
                }
                return;
            }
            let route = new_route(inner, &mut state, key, &header, &payload[4..], now);
            state.routes.insert(key, route);
            let held = state
                .pending
                .remove(&key)
                .map(|(_, held)| held)
                .unwrap_or_default();
            let state = &mut *state;
            let route = state.routes.get_mut(&key).unwrap();
            if !route.chosen {
                return;
            }
            let clients = state
                .slots
                .get(&(route.stream_id, route.slot))
                .map(|slot| slot.clients.clone())
                .unwrap_or_default();
            forward_symbol(inner, route, &clients, 0, &payload[4..]);
            for earlier in held {
                if let Ok(header) = DatagramFecHeader::decode(&earlier) {
                    if let Ok(payload) = header.payload(&earlier) {
                        if payload.len() >= 4 {
                            let esi = u32::from_be_bytes([0, payload[1], payload[2], payload[3]]);
                            forward_symbol(inner, route, &clients, esi, &payload[4..]);
                        }
                    }
                }
            }
            return;
        }
        let state = &mut *state;
        let route = state.routes.get_mut(&key).unwrap();
        route.seen_at = now;
        if !route.chosen {
            return;
        }
        let Some(slot) = state.slots.get(&(route.stream_id, route.slot)) else {
            return;
        };
        if slot.done {
            return;
        }
        let clients = slot.clients.clone();
        forward_symbol(inner, route, &clients, esi, &payload[4..]);
    }

    fn decoded(&self, peer: SocketAddr, datagram: &[u8], frame: &[u8]) {
        let Some(inner) = self.inner.get() else {
            return;
        };
        let Ok(header) = DatagramFecHeader::decode(datagram) else {
            return;
        };
        let key = (peer, header.block_id);
        let mut guard = inner.state.lock().unwrap();
        let (stream_id, slot) = match guard.routes.get(&key) {
            Some(route) => (route.stream_id, route.slot),
            None => match chunk_frame_slot(frame) {
                Some(found) => found,
                None => return,
            },
        };
        let state = &mut *guard;
        let Some(claim) = state.slots.get_mut(&(stream_id, slot)) else {
            state
                .decoded_unclaimed
                .insert((stream_id, slot), Instant::now());
            return;
        };
        if claim.done {
            return;
        }
        claim.done = true;
        let clients = claim.clients.clone();
        let chosen = claim.chosen == key;
        let route = state
            .routes
            .get(&key)
            .filter(|route| route.chosen && chosen);
        let job = match route {
            // The chosen block: the client symbols it never sent, and repair.
            Some(route) => CompleteJob::Chosen {
                client_symbol: route.client_symbol,
                missing: route
                    .forwarded
                    .iter()
                    .enumerate()
                    .filter(|(_, sent)| !**sent)
                    .map(|(esi, _)| esi)
                    .collect(),
            },
            // Another parent's block decoded first: it carries the whole part.
            None => {
                inner
                    .stats
                    .cut_through_fallbacks
                    .fetch_add(1, Ordering::Relaxed);
                CompleteJob::Whole
            }
        };
        inner
            .stats
            .cut_through_parts
            .fetch_add(1, Ordering::Relaxed);
        // Each client's repair as it asks for it now, not when the part began.
        let clients: Vec<(SocketAddr, RepairPolicy)> = {
            let table = inner.table.lock().unwrap();
            let current = table.get(&stream_id);
            clients
                .into_iter()
                .map(|(peer, repair)| {
                    let repair = current
                        .and_then(|entry| entry.subscribers.get(&peer))
                        .map_or(repair, |subscriber| subscriber.repair);
                    (peer, repair)
                })
                .collect()
        };
        let max_symbol = inner.config.symbol_size;
        let batch = Batch {
            send: inner.config.batch_send,
            gso: inner.config.gso,
        };
        // Another parent's block: the clients' first datagrams of the part.
        let first_client = matches!(job, CompleteJob::Whole)
            .then(|| clients.first().map(|c| c.0))
            .flatten();
        if completes_inline(
            frame.len(),
            inner.config.inline_complete_bytes,
            inner.sockets.out_for_stream(stream_id).queue.is_some(),
        ) {
            // A small part encodes in microseconds: complete it here rather
            // than wake a thread of the blocking pool for it, and without
            // the cut-through state locked.
            drop(guard);
            let out = inner.sockets.out_for_stream(stream_id);
            complete(
                out,
                &inner.stats,
                max_symbol,
                batch,
                slot,
                frame,
                job,
                &clients,
            );
            out.journal_sent(stream_id, slot, first_client);
            return;
        }
        let frame = frame.to_vec();
        let sockets = inner.sockets.clone();
        let stats = Arc::clone(&inner.stats);
        // Encoding a 4K part takes milliseconds: not on the receive task.
        tokio::task::spawn_blocking(move || {
            let out = sockets.out_for_stream(stream_id);
            complete(out, &stats, max_symbol, batch, slot, &frame, job, &clients);
            out.journal_sent(stream_id, slot, first_client);
        });
    }
}

/// Datagrams a cut-through thread holds before it drops one.
const CUT_LANE_CAPACITY: usize = 65_536;

enum CutEvent {
    Datagram(SocketAddr, Vec<u8>),
    Decoded(SocketAddr, Vec<u8>, Vec<u8>),
}

/// Runs a [`CutThrough`] on a thread of its own: the cache-mesh receive
/// task only copies each datagram onto a queue, so the per-client sends,
/// the cut-through state's lock and the completion's spawn never hold back
/// the decode of the next part. One lane per class: with a class-split
/// mesh, the audio and the video receive task each feed their own.
pub(crate) struct CutThroughLane {
    cut: Arc<CutThrough>,
    queue: std::sync::mpsc::SyncSender<CutEvent>,
}

impl CutThroughLane {
    /// Start the lane's thread. `runtime` is where the completions of
    /// decoded parts run (they encode on its blocking pool).
    pub(crate) fn start(
        cut: Arc<CutThrough>,
        name: &str,
        runtime: tokio::runtime::Handle,
    ) -> std::io::Result<Arc<Self>> {
        let (queue, events) = std::sync::mpsc::sync_channel::<CutEvent>(CUT_LANE_CAPACITY);
        let worker = Arc::clone(&cut);
        std::thread::Builder::new()
            .name(format!("needletail-cut-{name}"))
            .spawn(move || {
                let _runtime = runtime.enter();
                // Ends when the cache mesh drops its observer.
                while let Ok(event) = events.recv() {
                    match event {
                        CutEvent::Datagram(peer, datagram) => worker.datagram(peer, &datagram),
                        CutEvent::Decoded(peer, datagram, frame) => {
                            worker.decoded(peer, &datagram, &frame);
                        }
                    }
                }
            })?;
        Ok(Arc::new(Self { cut, queue }))
    }
}

impl CacheMeshReceiveObserver for CutThroughLane {
    fn datagram(&self, peer: SocketAddr, datagram: &[u8]) {
        let Some(inner) = self.cut.inner.get() else {
            return;
        };
        if self
            .queue
            .try_send(CutEvent::Datagram(peer, datagram.to_vec()))
            .is_err()
        {
            inner
                .stats
                .cut_through_queue_drops
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn decoded(&self, peer: SocketAddr, datagram: &[u8], frame: &[u8]) {
        if self.cut.inner.get().is_none() {
            return;
        }
        // A completion is never dropped: the clients of the part wait for
        // it. The queue is full only far behind, and then this waits.
        let _ = self
            .queue
            .send(CutEvent::Decoded(peer, datagram.to_vec(), frame.to_vec()));
    }
}

enum CompleteJob {
    Chosen {
        client_symbol: u16,
        missing: Vec<usize>,
    },
    Whole,
}

/// How [`complete`] hands its datagrams to the kernel.
#[derive(Clone, Copy)]
struct Batch {
    /// One `sendmmsg` for the part's datagrams to all its clients.
    send: bool,
    /// Each client's datagrams as one GSO message of it.
    gso: bool,
}

#[allow(clippy::too_many_arguments)]
/// Whether a cut-through part is completed where it decoded. Only a small
/// part, and only on a socket with a send queue: without one (send queue
/// 0), `complete` waits on a full socket, datagram by datagram, which may
/// not hold up the cut-through thread or the receive task. It then goes to
/// the blocking pool, as every part did before.
fn completes_inline(frame_len: usize, inline_complete_bytes: usize, send_queue: bool) -> bool {
    send_queue && frame_len <= inline_complete_bytes
}

fn complete(
    out: ClientOut<'_>,
    stats: &EdgeUdpStats,
    max_symbol: u16,
    batch: Batch,
    slot: u64,
    frame: &[u8],
    job: CompleteJob,
    clients: &[(SocketAddr, RepairPolicy)],
) {
    let mut groups: HashMap<RepairPolicy, Vec<SocketAddr>> = HashMap::new();
    for (peer, repair) in clients {
        groups.entry(*repair).or_default().push(*peer);
    }
    for (repair, peers) in groups {
        let (datagrams, send): (Vec<Vec<u8>>, Vec<usize>) = match &job {
            CompleteJob::Chosen {
                client_symbol,
                missing,
                ..
            } => {
                let sources = frame.len().div_ceil(usize::from(*client_symbol));
                let repair_symbols = repair.repair_symbols(sources as u32);
                let Ok(datagrams) = encode_block(
                    frame,
                    cut_through_block_id(slot),
                    *client_symbol,
                    repair_symbols,
                ) else {
                    continue;
                };
                let send = missing
                    .iter()
                    .copied()
                    .chain(sources..datagrams.len())
                    .collect();
                (datagrams, send)
            }
            CompleteJob::Whole => {
                let (sources, symbol) = geometry(frame.len(), max_symbol);
                let Ok(datagrams) = encode_block(
                    frame,
                    block_id(ObjectKind::Part, slot),
                    symbol,
                    repair.repair_symbols(sources),
                ) else {
                    continue;
                };
                let send = (0..datagrams.len()).collect();
                (datagrams, send)
            }
        };
        if batch.send && out.queue.is_some() {
            // Sent, or queued until the socket takes it: no wait. With GSO
            // a client's datagrams are together, one message of the batch;
            // without it the clients take turns, datagram by datagram.
            let list: Vec<(&[u8], SocketAddr)> = if batch.gso {
                peers
                    .iter()
                    .flat_map(|peer| {
                        send.iter()
                            .filter_map(|index| datagrams.get(*index))
                            .map(move |datagram| (datagram.as_slice(), *peer))
                    })
                    .collect()
            } else {
                send.iter()
                    .filter_map(|index| datagrams.get(*index))
                    .flat_map(|datagram| peers.iter().map(move |peer| (datagram.as_slice(), *peer)))
                    .collect()
            };
            let (delivered, _) = out.send_many(stats, true, batch.gso, &list);
            stats
                .cut_through_datagrams
                .fetch_add(delivered as u64, Ordering::Relaxed);
            continue;
        }
        for index in send {
            let Some(datagram) = datagrams.get(index) else {
                continue;
            };
            for peer in &peers {
                if out.queue.is_some() {
                    // Sent, or queued until the socket takes it: no wait.
                    if matches!(
                        out.send(stats, datagram, *peer),
                        SendOutcome::Sent | SendOutcome::Queued
                    ) {
                        stats.cut_through_datagrams.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
                let socket = out.socket;
                let mut attempt = 0;
                let result = loop {
                    match socket.try_send_to(datagram, *peer) {
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock && attempt < 200 =>
                        {
                            // A blocking thread: wait for the socket to drain.
                            attempt += 1;
                            std::thread::sleep(Duration::from_micros(50));
                        }
                        other => break other,
                    }
                };
                match result {
                    Ok(sent) => {
                        stats.datagrams.fetch_add(1, Ordering::Relaxed);
                        stats.cut_through_datagrams.fetch_add(1, Ordering::Relaxed);
                        stats.bytes.fetch_add(sent as u64, Ordering::Relaxed);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        stats.would_block.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        stats.send_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

/// The route of a new block, from its first source symbol, which names the
/// part. The first block of a part that has subscribers due for it is
/// chosen and claims the part for them.
fn new_route(
    inner: &CutInner,
    state: &mut CutState,
    key: (SocketAddr, u32),
    header: &DatagramFecHeader,
    first_symbol: &[u8],
    now: Instant,
) -> Route {
    let (stream_id, slot) = chunk_frame_slot(first_symbol).unwrap_or((u64::MAX, u64::MAX));
    // A part in one mesh symbol is cut through in one client symbol of its
    // own size (the mesh pads it to the mesh symbol size); a larger part in
    // client symbols that divide the mesh symbol.
    let client_symbol = if header.source_symbols == 1
        && header.transfer_length <= u32::from(inner.config.symbol_size)
    {
        Some((header.transfer_length as u16).max(16))
    } else {
        cut_through_symbol_size(header.symbol_size, inner.config.symbol_size)
    };
    let mut route = Route {
        stream_id,
        slot,
        chosen: false,
        client_symbol: client_symbol.unwrap_or(1),
        mesh_symbol: header.symbol_size,
        transfer_length: header.transfer_length,
        forwarded: Vec::new(),
        sequence: 0,
        seen_at: now,
    };
    if stream_id == u64::MAX
        || client_symbol.is_none()
        || state.slots.contains_key(&(stream_id, slot))
        || state.decoded_unclaimed.contains_key(&(stream_id, slot))
    {
        return route;
    }
    // A part in one mesh symbol is whole in this first symbol: the compact
    // lane's clients take it as its compact datagram, now.
    let wants_compact = |subscriber: &Subscriber| {
        subscriber.compact.is_some()
            && subscriber.fast.is_none()
            && subscriber.next_part <= slot
            && !subscriber.cut_slots.contains(&slot)
    };
    let compact = (header.source_symbols == 1
        && inner
            .table
            .lock()
            .unwrap()
            .get(&stream_id)
            .is_some_and(|entry| entry.subscribers.values().any(wants_compact)))
    .then(|| first_symbol.get(..header.transfer_length as usize))
    .flatten()
    .and_then(av_mesh::client_udp::cut_through_part)
    .filter(|object| object.stream_id == stream_id && object.part == slot)
    .and_then(|object| inner.sockets.compact().of_object(&object));
    let mut compact_clients: Vec<(SocketAddr, RepairPolicy, CompactPlan)> = Vec::new();
    let clients: Vec<(SocketAddr, RepairPolicy)> = {
        let mut table = inner.table.lock().unwrap();
        let Some(entry) = table.get_mut(&stream_id) else {
            return route;
        };
        // A fast-lane client takes the part from the fast lane.
        entry
            .subscribers
            .iter_mut()
            .filter(|(_, subscriber)| {
                subscriber.next_part <= slot
                    && subscriber.fast.is_none()
                    && !subscriber.cut_slots.contains(&slot)
            })
            .filter_map(|(peer, subscriber)| {
                subscriber.cut_slots.insert(slot);
                match (subscriber.compact, &compact) {
                    (Some(plan), Some(_)) => {
                        compact_clients.push((*peer, subscriber.repair, plan));
                        None
                    }
                    _ => Some((*peer, subscriber.repair)),
                }
            })
            .collect()
    };
    if let Some(canonical) = &compact {
        send_compact_now(inner, stream_id, slot, canonical, &compact_clients);
        if !compact_clients.is_empty() {
            let windows: Vec<u8> = compact_clients
                .iter()
                .map(|(_, _, plan)| plan.window)
                .collect();
            window_repair_now(
                &inner.sockets,
                &inner.table,
                inner.config.batch_send,
                &inner.stats,
                stream_id,
                slot,
                Some(&windows),
            );
        }
    }
    if clients.is_empty() {
        return route;
    }
    route.chosen = true;
    route.forwarded = vec![
        false;
        header
            .transfer_length
            .div_ceil(u32::from(route.client_symbol)) as usize
    ];
    state.slots.insert(
        (stream_id, slot),
        SlotState {
            chosen: key,
            done: false,
            clients,
            seen_at: now,
        },
    );
    route
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gap_in_the_compact_ring_gives_way_to_the_part() {
        let store = CompactStore::default();
        let gap = gap_object(9, 40);
        let kept = store.of_object(&gap).unwrap();
        assert!(compact::is_gap(&kept));
        assert_eq!(store.ring(9).get_part(40), None);
        // The part comes late (a repair after the horizon): it replaces the
        // gap, for NACKs, later subscribers and the fast lane.
        let part = PartObject {
            kind: ObjectKind::Part,
            datagrams: 0,
            stream_id: 9,
            part: 40,
            available_unix_us: 0,
            published_unix_us: 7,
            payload: Bytes::from_static(b"late part"),
        };
        let kept = store.of_object(&part).unwrap();
        assert!(!compact::is_gap(&kept));
        assert_eq!(compact::parse(&kept).unwrap().payload, b"late part");
        assert_eq!(store.get(9, 40), Some(kept.clone()));
        assert_eq!(store.ring(9).get_part(40), Some(kept.clone()));
        // A gap never replaces a part, and a part never another.
        assert_eq!(store.of_object(&gap), Some(kept.clone()));
        let other = Bytes::from(compact::encode_part(40, 7, b"other").unwrap());
        assert_eq!(store.ring(9).put(40, other), kept);
    }

    #[test]
    fn a_new_source_epoch_forgets_the_compact_parts_of_the_old() {
        let store = CompactStore::default();
        let object = |part: u64, payload: &'static [u8]| PartObject {
            kind: ObjectKind::Part,
            datagrams: 0,
            stream_id: 9,
            part,
            available_unix_us: 0,
            published_unix_us: 0,
            payload: Bytes::from_static(payload),
        };
        let old_epoch = 1_700_000_000_000_000;
        store.observe_epoch(9, old_epoch);
        let old = store.of_object(&object(40, b"old epoch")).unwrap();
        assert_eq!(
            compact::parse(&old).unwrap().generation,
            compact::generation_of_epoch(old_epoch)
        );
        // The contributor restarts and counts its parts again: part 40 of
        // the new epoch is not the old one's, and its generation is new.
        let new_epoch = old_epoch + 60_000_000;
        store.observe_epoch(9, new_epoch);
        assert_eq!(store.get(9, 40), None);
        let new = store.of_object(&object(40, b"new epoch")).unwrap();
        let view = compact::parse(&new).unwrap();
        assert_eq!(view.payload, b"new epoch");
        assert_eq!(view.generation, compact::generation_of_epoch(new_epoch));
        // A late part of the old epoch (the fast lane) is refused, and an
        // older epoch from the cache changes nothing.
        let ring = store.ring(9);
        assert!(!ring.observe_epoch(old_epoch));
        store.observe_epoch(9, old_epoch);
        assert_eq!(store.get(9, 40), Some(new));
        // A ring made after the epoch was taken up starts in it.
        store.observe_epoch(10, new_epoch);
        assert_eq!(
            store.ring(10).generation(),
            compact::generation_of_epoch(new_epoch)
        );
    }

    #[test]
    fn a_nack_interval_bounds_the_parts_and_the_bytes_sent_again() {
        let start = Instant::now();
        let mut interval = NackInterval::new(start);
        for _ in 0..NACK_BUDGET {
            assert!(interval.grant_part(start));
        }
        assert!(!interval.grant_part(start));
        // The next interval has its budget again.
        let next = start + NACK_INTERVAL;
        assert!(interval.grant_part(next));

        // Bytes: small compact parts fit many times; whole 4K parts (52 KB)
        // two at most, not the 200 parts of the part budget.
        let mut interval = NackInterval::new(start);
        let mut sent = 0;
        while interval.charge_bytes(start, 52_000) {
            sent += 1;
        }
        assert_eq!(sent, 2);
        assert!(interval.charge_bytes(next, 52_000));
        // One part larger than the budget is still sent, as the first of
        // its interval, and nothing after it.
        let mut interval = NackInterval::new(start);
        assert!(interval.charge_bytes(start, NACK_BUDGET_BYTES * 2));
        assert!(!interval.charge_bytes(start, 100));
        let mut interval = NackInterval::new(start);
        let mut small = 0;
        while interval.charge_bytes(start, 330) {
            small += 1;
        }
        assert!(small >= NACK_BUDGET as usize, "{small}");
    }

    #[test]
    fn a_subscriber_resumes_within_its_playout_deadline() {
        // The oldest part within 500 ms of 5 ms parts: 100 parts back.
        let floor = Some(1_400);
        // A client that moved names part 1000; the head is 1500 and the
        // window 2400 parts: it resumes at 1400, not 500 parts back.
        assert_eq!(first_part_for(Some(1_000), 1_500, 2_400, floor), 1_400);
        // The behaviour from before: anywhere in the window.
        assert_eq!(first_part_for(Some(1_000), 1_500, 2_400, None), 1_000);
        // A part within the deadline, the next part, and none named.
        assert_eq!(first_part_for(Some(1_450), 1_500, 2_400, floor), 1_450);
        assert_eq!(first_part_for(Some(1_501), 1_500, 2_400, floor), 1_501);
        assert_eq!(first_part_for(None, 1_500, 2_400, floor), 1_500);
        // Out of the window: the newest part, as before.
        assert_eq!(first_part_for(Some(10), 5_000, 2_400, floor), 5_000);
    }

    /// With no send queue a part is never completed inline, whatever its
    /// size: there `complete` waits on a full socket, which the cut-through
    /// thread may not do (--client-udp-send-queue-ms 0 is the behaviour
    /// from before).
    #[test]
    fn a_part_is_completed_inline_only_on_a_socket_with_a_send_queue() {
        assert!(completes_inline(1_000, 16_384, true));
        assert!(!completes_inline(20_000, 16_384, true));
        assert!(!completes_inline(1_000, 16_384, false));
        assert!(!completes_inline(1_000, 0, true));
    }

    #[test]
    fn a_hole_is_repaired_only_while_it_is_within_the_playout_deadline() {
        let playout = Duration::from_millis(500);
        let ms = Duration::from_millis;
        // No deadline: HOLE_WAIT, as before.
        assert_eq!(hole_budget(ms(25), Duration::ZERO), None);
        // 100 ms behind: 400 ms left, capped at HOLE_WAIT.
        assert_eq!(hole_budget(ms(100), playout), Some(ms(400).min(HOLE_WAIT)));
        // 400 ms behind: 100 ms left.
        assert_eq!(hole_budget(ms(400), playout), Some(ms(100)));
        // Past the deadline: the gap goes at once.
        assert_eq!(hole_budget(ms(1_000), playout), Some(Duration::ZERO));
        // A part at or ahead of the head has its whole budget.
        assert_eq!(hole_budget(Duration::ZERO, ms(50)), Some(ms(50)));
    }

    fn subscriber(admitted_at: Instant) -> Subscriber {
        moved_subscriber(admitted_at, true)
    }

    fn moved_subscriber(admitted_at: Instant, moved_in: bool) -> Subscriber {
        Subscriber {
            repair: RepairPolicy {
                permille: 0,
                min_symbols: 0,
                loss_ppm: 0,
            },
            expires_at: admitted_at + Duration::from_secs(10),
            next_part: 0,
            init: None,
            cut_slots: BTreeSet::new(),
            admitted_at,
            moved_in,
            catching_up: moved_in,
            held_down_counted: false,
            fast: None,
            compact: None,
            first_part: 0,
            nack_interval: NackInterval::new(admitted_at),
            windows_repaired: VecDeque::new(),
        }
    }

    /// Wave 8 review: a client that moved here skips the playout floor only
    /// until it has caught up to it; a later stall of this edge skips it
    /// forward like every other subscriber.
    #[test]
    fn a_mover_skips_the_playout_floor_only_until_it_has_caught_up() {
        let now = Instant::now();
        let mut mover = moved_subscriber(now, true);
        mover.next_part = 100;
        // Head 1,000, floor 500: the mover resumes at its part.
        assert_eq!(hold_to_playout(&mut mover, 1_000, 12_000, Some(500), true), 0);
        assert_eq!(mover.next_part, 100);
        assert!(mover.catching_up);
        // Caught up.
        mover.next_part = 600;
        assert_eq!(hold_to_playout(&mut mover, 1_100, 12_000, Some(600), true), 0);
        assert!(!mover.catching_up);
        // The edge stalls: the mover is skipped forward as others are.
        assert_eq!(hold_to_playout(&mut mover, 2_000, 12_000, Some(1_500), true), 900);
        assert_eq!(mover.next_part, 1_500);
        // Without resume from request every subscriber goes to the floor.
        let mut mover = moved_subscriber(now, true);
        mover.next_part = 100;
        assert_eq!(hold_to_playout(&mut mover, 1_000, 12_000, Some(500), false), 400);
        // Past the window: to the head.
        let mut other = moved_subscriber(now, false);
        other.next_part = 100;
        assert_eq!(hold_to_playout(&mut other, 20_000, 12_000, Some(19_500), true), 0);
        assert_eq!(other.next_part, 20_000);
    }

    #[test]
    fn lost_parents_do_not_move_a_subscriber_within_the_hold_down() {
        let now = Instant::now();
        let old: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let new: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let mut entry = StreamEntry::default();
        entry
            .subscribers
            .insert(old, subscriber(now - Duration::from_secs(5)));
        entry
            .subscribers
            .insert(new, subscriber(now - Duration::from_millis(500)));
        assert_eq!(
            take_movable(&mut entry, Duration::from_secs(3), now, None),
            vec![old]
        );
        assert_eq!(entry.subscribers.len(), 1);
        // Without a hold-down (or draining) every subscriber goes.
        assert_eq!(take_movable(&mut entry, Duration::ZERO, now, None), vec![new]);
        assert!(entry.subscribers.is_empty());
    }

    /// The hold-down is for clients that came by a move: a client that
    /// subscribed afresh just before the edge lost its parents is moved at
    /// once, not served from the cache for the hold-down. A held client is
    /// counted once, however many housekeeping rounds hold it.
    #[test]
    fn only_a_moved_client_is_held_down_and_counted_once() {
        let now = Instant::now();
        let fresh: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let moved: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let mut entry = StreamEntry::default();
        entry.subscribers.insert(
            fresh,
            moved_subscriber(now - Duration::from_millis(200), false),
        );
        entry.subscribers.insert(
            moved,
            moved_subscriber(now - Duration::from_millis(200), true),
        );
        assert_eq!(
            take_movable(&mut entry, Duration::from_secs(3), now, None),
            vec![fresh]
        );
        assert_eq!(count_held_down(&mut entry), 1);
        // A later round holds it again but does not count it again.
        assert!(take_movable(&mut entry, Duration::from_secs(3), now, None).is_empty());
        assert_eq!(count_held_down(&mut entry), 0);
        assert_eq!(
            take_movable(
                &mut entry,
                Duration::from_secs(3),
                now + Duration::from_secs(3),
                None
            ),
            vec![moved]
        );
    }

    /// Phase AM item 9: a paced drain of 1,000 subscribers over 2 s sends
    /// a share at each 50 ms tick, the first at once, and the last at the
    /// end of the spread: they leave across more than 1.5 s, at most a
    /// tick's share (~25) at once.
    #[test]
    fn a_paced_drain_spreads_a_thousand_moves_over_its_spread() {
        let start = Instant::now();
        let spread = Duration::from_millis(2_000);
        let mut pace = DrainPace {
            started: start,
            initial: 1_000,
            sent: 0,
        };
        let mut left = 1_000_usize;
        let (mut first, mut last, mut largest) = (None, None, 0);
        let mut at = start;
        while left > 0 {
            let due = pace.due(at, spread).min(left);
            if due > 0 {
                first.get_or_insert(at);
                last = Some(at);
                largest = largest.max(due);
            }
            pace.sent += due;
            left -= due;
            at += DRAIN_TICK;
        }
        let across = last.unwrap() - first.unwrap();
        assert_eq!(first, Some(start), "the first share goes at once");
        assert!(across >= Duration::from_millis(1_500), "{across:?}");
        assert!(across <= spread, "{across:?}");
        assert!(largest <= 26, "{largest}");
        // Newcomers since the start and those left at the end go at once.
        let pace = DrainPace {
            started: start,
            initial: 10,
            sent: 10,
        };
        assert_eq!(pace.due(start + spread, spread), usize::MAX);
        // One subscriber goes at once.
        let pace = DrainPace {
            started: start,
            initial: 1,
            sent: 0,
        };
        assert_eq!(pace.due(start, spread), 1);
    }

    #[test]
    fn a_client_moved_away_for_lost_parents_is_taken_back_within_the_hold_down() {
        let moved: MovedAway = Arc::default();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let other: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let now = Instant::now();
        let hold = Duration::from_secs(3);
        moved.lock().unwrap().insert((peer, 7), now);
        assert!(moved_back_within_hold_down(
            &moved,
            peer,
            7,
            hold,
            now + Duration::from_secs(1)
        ));
        assert!(!moved_back_within_hold_down(&moved, peer, 8, hold, now));
        assert!(!moved_back_within_hold_down(&moved, other, 7, hold, now));
        assert!(!moved_back_within_hold_down(
            &moved,
            peer,
            7,
            Duration::ZERO,
            now
        ));
        // After the hold-down the move is forgotten.
        assert!(!moved_back_within_hold_down(
            &moved,
            peer,
            7,
            hold,
            now + Duration::from_secs(4)
        ));
        assert!(moved.lock().unwrap().is_empty());
    }

    async fn receive(socket: &UdpSocket, count: usize) -> Vec<Vec<u8>> {
        let mut buf = vec![0_u8; 2_048];
        let mut got = Vec::new();
        while got.len() < count {
            let (len, _) = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut buf))
                .await
                .expect("the queued datagrams arrive")
                .unwrap();
            got.push(buf[..len].to_vec());
        }
        got
    }

    /// A part's datagrams to several clients leave in one batch, with or
    /// without GSO, with or without a send queue: each client receives its
    /// own, whole and in order, and each counts once.
    /// Wave 8 review: the edge's NACK budget is charged only for parts it
    /// answers; a stranger's NACK, or parts not sent yet, take nothing from
    /// the other clients.
    #[tokio::test]
    async fn the_edge_nack_budget_pays_only_for_answered_parts() {
        let sockets = EdgeSockets::single(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let table: Table = Arc::default();
        let stats = EdgeUdpStats::default();
        let now = Instant::now();
        let client: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let stranger: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let mut subscriber = moved_subscriber(now, false);
        subscriber.compact = Some(CompactPlan::CLEAN);
        subscriber.first_part = 0;
        subscriber.next_part = 100;
        table
            .lock()
            .unwrap()
            .entry(5)
            .or_default()
            .subscribers
            .insert(client, subscriber);
        // Ten parts' worth (a tenth of a second at 100 a second).
        let mut budget = EdgeNackBudget::new(100, now);
        let many: Vec<u64> = (0..50).collect();
        assert!(answer_nack(&sockets, &table, &stats, stranger, 5, &many, true, &mut budget).is_none());
        let unsent: Vec<u64> = (100..150).collect();
        assert!(answer_nack(&sockets, &table, &stats, client, 5, &unsent, true, &mut budget).is_none());
        assert_eq!(stats.nack_edge_capped.load(Ordering::Relaxed), 0);
        // Sent parts (not in the ring: asked of the cache): ten answered.
        let sent: Vec<u64> = (0..20).collect();
        let (misses, _) =
            answer_nack(&sockets, &table, &stats, client, 5, &sent, true, &mut budget).unwrap();
        assert_eq!(misses, (0..10).collect::<Vec<u64>>());
        assert_eq!(stats.nack_edge_capped.load(Ordering::Relaxed), 10);
    }

    #[tokio::test]
    async fn a_batch_reaches_each_client_whole_and_in_order() {
        for (queue, gso) in [(true, true), (true, false), (false, true), (false, false)] {
            let sender = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            sender.writable().await.unwrap();
            let stats = Arc::new(EdgeUdpStats::default());
            let send_queue = queue.then(|| {
                SendQueue::start(
                    Arc::clone(&sender),
                    Duration::from_millis(250),
                    Arc::clone(&stats),
                )
            });
            let out = ClientOut {
                socket: &sender,
                queue: send_queue.as_ref(),
                journal: None,
            };
            let clients = [
                UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            ];
            // Three repair datagrams of one size for each client, and a
            // shorter last one, as GSO allows; client by client.
            let datagrams: Vec<Vec<u8>> = (0..4_u8)
                .map(|index| vec![index; if index == 3 { 40 } else { 100 }])
                .collect();
            let list: Vec<(&[u8], SocketAddr)> = clients
                .iter()
                .flat_map(|client| {
                    let to = client.local_addr().unwrap();
                    datagrams
                        .iter()
                        .map(move |datagram| (datagram.as_slice(), to))
                })
                .collect();
            let (delivered, blocked) = out.send_many(&stats, true, gso, &list);
            assert_eq!((delivered, blocked), (12, 0), "queue {queue}, gso {gso}");
            for client in &clients {
                assert_eq!(
                    receive(client, 4).await,
                    datagrams,
                    "queue {queue}, gso {gso}"
                );
            }
            assert_eq!(stats.datagrams.load(Ordering::Relaxed), 12);
            assert_eq!(stats.bytes.load(Ordering::Relaxed), 3 * 340);
        }
    }

    /// A datagram that found the socket full waits in its client's queue and
    /// leaves in order when the socket can take it; the client's next
    /// datagrams queue behind it, and another client is not held up.
    #[tokio::test]
    async fn a_datagram_that_finds_the_socket_full_is_sent_later_in_order() {
        let sender = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        // Until the reactor has seen the socket writable, a try_send says
        // WouldBlock without trying.
        sender.writable().await.unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let other = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stats = Arc::new(EdgeUdpStats::default());
        let queue = SendQueue::start(
            Arc::clone(&sender),
            Duration::from_millis(250),
            Arc::clone(&stats),
        );
        let to = client.local_addr().unwrap();
        // As if the socket had refused the first two.
        assert_eq!(queue.enqueue(b"one", to, true), SendOutcome::Queued);
        assert_eq!(queue.enqueue(b"two", to, true), SendOutcome::Queued);
        // The client's next datagram goes behind them (or, if the drain has
        // already emptied the queue, straight out): never ahead.
        let third = queue.send(b"three", to);
        assert!(
            matches!(third, SendOutcome::Queued | SendOutcome::Sent),
            "{third:?}"
        );
        // Another client goes straight out.
        assert_eq!(
            queue.send(b"other", other.local_addr().unwrap()),
            SendOutcome::Sent
        );
        assert_eq!(receive(&other, 1).await, vec![b"other".to_vec()]);
        assert_eq!(
            receive(&client, 3).await,
            vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
        );
        assert!(stats.send_queued.load(Ordering::Relaxed) >= 2);
        assert_eq!(stats.send_queue_drops.load(Ordering::Relaxed), 0);
        assert_eq!(queue.waiting.load(Ordering::Relaxed), 0);
    }

    /// The drain sends in slices of at most DRAIN_SLICE, each client's
    /// datagrams in order, and leaves nothing waiting.
    #[tokio::test]
    async fn the_drain_sends_in_slices_each_client_in_order() {
        let sender = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        sender.writable().await.unwrap();
        let clients = [
            UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            UdpSocket::bind("127.0.0.1:0").await.unwrap(),
        ];
        for client in &clients {
            let _ = socket2::SockRef::from(client).set_recv_buffer_size(4 << 20);
        }
        let stats = Arc::new(EdgeUdpStats::default());
        let queue = SendQueue {
            socket: Arc::clone(&sender),
            limit: Duration::from_secs(5),
            waiting: std::sync::atomic::AtomicUsize::new(0),
            clients: Mutex::new(HashMap::new()),
            sending: std::sync::atomic::AtomicBool::new(false),
            wake: tokio::sync::Notify::new(),
            stats: Arc::clone(&stats),
        };
        let per_client = DRAIN_SLICE + 100;
        let payloads: Vec<Vec<u8>> = (0..per_client)
            .map(|index| (index as u32).to_be_bytes().to_vec())
            .collect();
        let list: Vec<(&[u8], SocketAddr)> = (0..per_client)
            .flat_map(|index| {
                clients
                    .iter()
                    .map(move |client| (index, client.local_addr().unwrap()))
            })
            .map(|(index, peer)| (payloads[index].as_slice(), peer))
            .collect();
        assert_eq!(queue.enqueue_many(&list, true), (list.len(), 0));
        assert_eq!(queue.waiting.load(Ordering::Relaxed), list.len());
        while queue.drain() {
            sender.writable().await.unwrap();
        }
        assert_eq!(queue.waiting.load(Ordering::Relaxed), 0);
        for client in &clients {
            assert_eq!(receive(client, per_client).await, payloads);
        }
        assert_eq!(stats.datagrams.load(Ordering::Relaxed), list.len() as u64);
    }

    /// A client with datagrams waiting has its new ones queued behind them;
    /// the other clients' still leave in the batch, at once.
    #[tokio::test]
    async fn a_waiting_client_does_not_take_the_batch_from_the_others() {
        let sender = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        sender.writable().await.unwrap();
        let waiting = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let free = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stats = Arc::new(EdgeUdpStats::default());
        // No drain task: the test drains by hand.
        let queue = Arc::new(SendQueue {
            socket: Arc::clone(&sender),
            limit: Duration::from_secs(5),
            waiting: std::sync::atomic::AtomicUsize::new(0),
            clients: Mutex::new(HashMap::new()),
            sending: std::sync::atomic::AtomicBool::new(false),
            wake: tokio::sync::Notify::new(),
            stats: Arc::clone(&stats),
        });
        let to_waiting = waiting.local_addr().unwrap();
        let to_free = free.local_addr().unwrap();
        // As if the socket had refused the first.
        assert_eq!(queue.enqueue(b"w1", to_waiting, true), SendOutcome::Queued);
        let out = ClientOut {
            socket: &sender,
            queue: Some(&queue),
            journal: None,
        };
        let list: Vec<(&[u8], SocketAddr)> = vec![
            (b"w2", to_waiting),
            (b"f1", to_free),
            (b"f2", to_free),
            (b"w3", to_waiting),
        ];
        assert_eq!(out.send_many(&stats, true, false, &list), (4, 0));
        // The free client's left at once; the waiting one's are queued.
        assert_eq!(
            receive(&free, 2).await,
            vec![b"f1".to_vec(), b"f2".to_vec()]
        );
        assert_eq!(queue.waiting.load(Ordering::Relaxed), 3);
        assert!(!queue.drain());
        assert_eq!(
            receive(&waiting, 3).await,
            vec![b"w1".to_vec(), b"w2".to_vec(), b"w3".to_vec()]
        );
    }

    /// A datagram older than the limit is dropped, not sent, and counted.
    #[tokio::test]
    async fn a_queued_datagram_older_than_the_limit_is_dropped_and_counted() {
        let sender = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stats = Arc::new(EdgeUdpStats::default());
        // No drain task: the test drains by hand.
        let queue = SendQueue {
            socket: sender,
            limit: Duration::from_millis(20),
            waiting: std::sync::atomic::AtomicUsize::new(0),
            clients: Mutex::new(HashMap::new()),
            sending: std::sync::atomic::AtomicBool::new(false),
            wake: tokio::sync::Notify::new(),
            stats: Arc::clone(&stats),
        };
        let to = client.local_addr().unwrap();
        queue.enqueue(b"stale", to, true);
        tokio::time::sleep(Duration::from_millis(40)).await;
        queue.enqueue(b"fresh", to, true);
        // The stale one went when the fresh one was queued behind it.
        assert_eq!(stats.send_queue_drops.load(Ordering::Relaxed), 1);
        assert!(!queue.drain());
        assert_eq!(receive(&client, 1).await, vec![b"fresh".to_vec()]);
        queue.enqueue(b"late", to, true);
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(!queue.drain());
        assert_eq!(stats.send_queue_drops.load(Ordering::Relaxed), 2);
        assert_eq!(queue.waiting.load(Ordering::Relaxed), 0);
        let mut buf = [0_u8; 64];
        assert!(
            tokio::time::timeout(Duration::from_millis(100), client.recv_from(&mut buf))
                .await
                .is_err(),
            "a dropped datagram is not sent"
        );
    }
}
