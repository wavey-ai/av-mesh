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

use super::sys;
use super::view::RlsView;
use super::TrafficClass;
use crate::edge_udp::{EdgeSockets, EdgeUdpStats, Table};
use arc_swap::ArcSwap;
use av_mesh::client_udp::{fast_block_id, rewrap_fast, RepairPolicy};
use dashmap::DashMap;
use raptorq_datagram_fec::{
    DatagramFecHeader, MediaObjectKind, RaptorQBlockProfile, RaptorQRepairEncoder,
};
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
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
    pub fn new(fast_from: u64) -> Arc<Self> {
        Arc::new(Self {
            fast_from,
            ring: (0..CLAIM_RING).map(|_| AtomicU64::new(0)).collect(),
        })
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

/// The fast-lane clients of one repair policy.
#[derive(Debug, Clone)]
pub struct FastGroup {
    pub repair: RepairPolicy,
    pub clients: Vec<(SocketAddr, Arc<FastClient>)>,
}

/// The fast-lane clients of the streams that ride the lane.
#[derive(Debug, Default)]
pub struct FastSnapshot {
    pub streams: HashMap<u64, Vec<FastGroup>>,
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
    /// Streams the tee has seen commit: `true` for a canonical slot (the
    /// lane may carry them), `false` for a reboxed one.
    eligible: DashMap<u64, bool>,
    armed: OnceLock<Armed>,
    /// The client sockets' descriptors by class, once armed.
    fds: [AtomicU64; 2],
    rank: u32,
    next_sequence: AtomicU32,
    pub stats: FastLaneStats,
}

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
        Arc::new(Self {
            snapshot: ArcSwap::from_pointee(FastSnapshot::default()),
            eligible: DashMap::new(),
            armed: OnceLock::new(),
            fds: [AtomicU64::new(u64::MAX), AtomicU64::new(u64::MAX)],
            rank,
            next_sequence: AtomicU32::new(0),
            stats: FastLaneStats::default(),
        })
    }

    /// Give the lane its client sockets and subscriber table.
    pub fn arm(&self, sockets: EdgeSockets, table: Table, stats: Arc<EdgeUdpStats>) {
        let audio = sockets.class_socket(TrafficClass::Audio).as_raw_fd();
        let video = sockets.class_socket(TrafficClass::Video).as_raw_fd();
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
        self.snapshot.store(Arc::new(FastSnapshot { streams }));
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
        }
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
        let mut wanted: Vec<(u32, Vec<SocketAddr>)> = Vec::new();
        for group in groups {
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

/// A send planned by a class thread.
#[derive(Debug, Clone, Copy)]
struct PlannedClient {
    message: u16,
    source: bool,
    part: u64,
    peer: SocketAddr,
}

/// One class thread's working state of the fast client lane: the rewrap
/// buffers of a drained batch, the sends, and the repair that each policy
/// asks of a block of K symbols. Allocated once per thread.
pub struct ClientPlan {
    buffers: Vec<u8>,
    lens: Vec<u16>,
    sends: Vec<PlannedClient>,
    /// The clients of the sends, by index in `sends` (kept apart so that a
    /// send is `Copy`).
    clients: Vec<Arc<FastClient>>,
    memo: Vec<(RepairPolicy, u32, u32)>,
    memo_next: usize,
}

const REPAIR_MEMO: usize = 64;

impl Default for ClientPlan {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientPlan {
    pub fn new() -> Self {
        Self {
            buffers: vec![0_u8; sys::RECV_BATCH * REWRAP_BYTES],
            lens: vec![0; sys::RECV_BATCH],
            sends: Vec::with_capacity(sys::RECV_BATCH * 8),
            clients: Vec::with_capacity(sys::RECV_BATCH * 8),
            memo: Vec::with_capacity(REPAIR_MEMO),
            memo_next: 0,
        }
    }

    pub fn clear(&mut self) {
        self.sends.clear();
        self.clients.clear();
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

    fn buffer(&self, message: usize) -> &[u8] {
        let start = message * REWRAP_BYTES;
        &self.buffers[start..start + usize::from(self.lens[message])]
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
        let Some(len) = rewrap_fast(
            view.fec,
            part,
            &mut self.buffers[start..start + REWRAP_BYTES],
        ) else {
            return;
        };
        self.lens[message] = len as u16;
        let k = u32::from(view.source_symbols);
        let repair_ordinal = view.esi.checked_sub(k);
        for group in groups {
            if let Some(ordinal) = repair_ordinal {
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
                if !client.claim_fast(part) {
                    continue;
                }
                if fresh {
                    lane.stats.parts.fetch_add(1, Ordering::Relaxed);
                }
                self.sends.push(PlannedClient {
                    message: message as u16,
                    source: repair_ordinal.is_none(),
                    part,
                    peer: *peer,
                });
                self.clients.push(Arc::clone(client));
            }
        }
    }

    /// Send the plan from the client socket of `class`: one non-blocking
    /// `sendmmsg`, then one more try each for what the kernel did not take.
    /// A source symbol that is still refused gives its part back to the
    /// cache-fed lane for that client.
    pub fn send(&mut self, lane: &EdgeFastLane, class: TrafficClass) {
        if self.sends.is_empty() {
            return;
        }
        let Some(fd) = lane.fd(class) else {
            self.clear();
            return;
        };
        let message = |index: usize| {
            let send = &self.sends[index];
            (self.buffer(usize::from(send.message)), send.peer)
        };
        let sent = sys::send_batch(fd, self.sends.len(), message).unwrap_or(0);
        let mut delivered = sent as u64;
        for index in sent..self.sends.len() {
            let send = self.sends[index];
            let data = self.buffer(usize::from(send.message));
            match sys::send_batch(fd, 1, |_| (data, send.peer)) {
                Ok(1) => delivered += 1,
                result => {
                    match result {
                        Err(error) if error.kind() != std::io::ErrorKind::WouldBlock => {
                            lane.stats.send_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            lane.stats.would_block.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if send.source {
                        self.clients[index].give_back(send.part);
                        lane.stats.given_back.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        lane.stats.datagrams.fetch_add(delivered, Ordering::Relaxed);
        self.clear();
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
