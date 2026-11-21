//! The hop journal: an in-memory ring of part events that every node keeps,
//! so that the time a part spends on each hop of the network can be measured
//! by joining the journals of all nodes on (stream, seq).
//!
//! Recording is cheap. An event is a fixed-size value written into a ring
//! that was allocated once, under a `std::sync::Mutex` that is held only for
//! the copy. Nothing is logged and nothing is allocated per event.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};

/// Events kept by a node unless `--hop-journal-capacity` says otherwise.
pub const DEFAULT_HOP_JOURNAL_CAPACITY: usize = 262_144;

/// Slots of the table that remembers which (stage, stream, seq) a node has
/// already recorded, for the stages that record only the first event of a
/// part or a bounded number of them. A part is in flight for milliseconds,
/// so a small table is enough; an entry that is evicted early costs at most
/// one more event for the same part, which a reader of the journal resolves
/// by taking the earliest.
const SEEN_TABLE_SLOTS: usize = 16_384;

/// Events copied out of the ring under one hold of its lock when the journal
/// is read, so that a reader never holds back the media for long. The copy
/// is a plain copy of about 90 KB; the filter runs after the lock is released.
const READ_CHUNK_EVENTS: usize = 1_024;

/// Where a part was when an event was recorded. The names are the stage
/// names of the journal format and must not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum HopStage {
    /// The first RelaySession symbol of the object arrived.
    RelayFirst = 1,
    /// The first symbol of the object was handed to the kernel for a child.
    RelayForward,
    /// The RelaySession object decoded.
    RelayDecoded,
    /// The part was stored in the ChunkCache of the node.
    Committed,
    /// The contiguous watermark of the ChunkCache reached the part.
    Watermark,
    /// A distributor handed the first datagram of the part to the kernel for
    /// the child `peer`.
    MeshSent,
    /// The first datagram of the part's cache-mesh FEC block arrived.
    MeshFirst,
    /// The cache-mesh FEC block decoded.
    MeshDecoded,
    /// The part was written into the edge ChunkCache.
    EdgeCommitted,
    /// The part entered the edge playlist.
    EdgePlaylist,
    /// An HTTP response for the part was handed to the server.
    Reply,
    /// A mode-2 UDP listener was sent the part.
    #[allow(dead_code)]
    UdpSent,
}

impl HopStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RelayFirst => "relay_first",
            Self::RelayForward => "relay_forward",
            Self::RelayDecoded => "relay_decoded",
            Self::Committed => "committed",
            Self::Watermark => "watermark",
            Self::MeshSent => "mesh_sent",
            Self::MeshFirst => "mesh_first",
            Self::MeshDecoded => "mesh_decoded",
            Self::EdgeCommitted => "edge_committed",
            Self::EdgePlaylist => "edge_playlist",
            Self::Reply => "reply",
            Self::UdpSent => "udp_sent",
        }
    }
}

/// One line of the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HopEvent {
    pub stage: HopStage,
    pub stream: u64,
    pub seq: u64,
    /// CLOCK_REALTIME in microseconds.
    pub t_us: u64,
    pub peer: Option<SocketAddr>,
    pub epoch: Option<u64>,
    pub n: Option<u32>,
}

impl HopEvent {
    pub const fn new(stage: HopStage, stream: u64, seq: u64, t_us: u64) -> Self {
        Self {
            stage,
            stream,
            seq,
            t_us,
            peer: None,
            epoch: None,
            n: None,
        }
    }

    pub const fn with_peer(mut self, peer: Option<SocketAddr>) -> Self {
        self.peer = peer;
        self
    }
}

/// Which events a reader asks for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HopQuery {
    /// Only events at or after this time.
    pub since_us: Option<u64>,
    /// Only events of this stream.
    pub stream: Option<u64>,
}

impl HopQuery {
    fn matches(&self, event: &HopEvent) -> bool {
        self.since_us.is_none_or(|since| event.t_us >= since)
            && self.stream.is_none_or(|stream| event.stream == stream)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SeenEntry {
    /// Zero for an empty slot, else the `HopStage` discriminant.
    stage: u8,
    stream: u64,
    seq: u64,
    count: u32,
}

#[derive(Debug)]
struct Ring {
    events: VecDeque<HopEvent>,
    /// Events ever pushed. The event at deque index `i` has the absolute
    /// index `pushed - events.len() + i`.
    pushed: u64,
    seen: Box<[SeenEntry]>,
}

/// The journal of one node.
#[derive(Debug)]
pub struct HopJournal {
    node: String,
    capacity: usize,
    ring: Mutex<Ring>,
}

impl HopJournal {
    /// A journal that keeps the newest `capacity` events. Zero keeps none:
    /// every record is a no-op.
    pub fn new(node: impl Into<String>, capacity: usize) -> Self {
        let (events, seen) = if capacity == 0 {
            (VecDeque::new(), Vec::new().into_boxed_slice())
        } else {
            (
                VecDeque::with_capacity(capacity),
                vec![SeenEntry::default(); SEEN_TABLE_SLOTS].into_boxed_slice(),
            )
        };
        Self {
            node: node.into(),
            capacity,
            ring: Mutex::new(Ring {
                events,
                pushed: 0,
                seen,
            }),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.capacity > 0
    }

    /// Record one event.
    pub fn record(&self, event: HopEvent) {
        if self.capacity == 0 {
            return;
        }
        let mut ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
        ring.push(event, self.capacity);
    }

    /// Record a part event without a peer.
    pub fn record_part(&self, stage: HopStage, stream: u64, seq: u64, t_us: u64) {
        self.record(HopEvent::new(stage, stream, seq, t_us));
    }

    /// Record the event only if it is the first of its stage for the part.
    /// Returns whether it was recorded.
    pub fn record_first(&self, event: HopEvent) -> bool {
        self.record_counted(event, 1).is_some()
    }

    /// Record the event if fewer than `max` events of its stage have been
    /// recorded for the part. The recorded event carries its ordinal, from
    /// 1, as `n` when `max` is above one. Returns the ordinal.
    pub fn record_counted(&self, mut event: HopEvent, max: u32) -> Option<u32> {
        if self.capacity == 0 || max == 0 {
            return None;
        }
        let mut ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
        let slot = seen_slot(event.stage, event.stream, event.seq);
        let entry = &mut ring.seen[slot];
        let stage = event.stage as u8;
        if entry.stage == stage && entry.stream == event.stream && entry.seq == event.seq {
            if entry.count >= max {
                return None;
            }
            entry.count += 1;
        } else {
            *entry = SeenEntry {
                stage,
                stream: event.stream,
                seq: event.seq,
                count: 1,
            };
        }
        let ordinal = entry.count;
        if max > 1 {
            event.n = Some(ordinal);
        }
        ring.push(event, self.capacity);
        Some(ordinal)
    }

    /// The matching events, oldest first. The ring is copied in chunks, so
    /// recorders wait for at most the copy of one chunk.
    ///
    /// A read of a full ring copies and sorts hundreds of thousands of
    /// events: call it off the async runtime (`spawn_blocking`).
    pub fn events(&self, query: HopQuery) -> Vec<HopEvent> {
        let mut out = Vec::new();
        if self.capacity == 0 {
            return out;
        }
        let mut chunk: Vec<HopEvent> = Vec::with_capacity(READ_CHUNK_EVENTS);
        // Read no further than the end of the ring when the read began, so
        // that a busy node cannot keep the reader going for ever.
        let mut bounds: Option<(u64, u64)> = None;
        loop {
            let ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
            let base = ring.pushed - ring.events.len() as u64;
            let (cursor, limit) = *bounds.get_or_insert((base, ring.pushed));
            // Events that were overwritten while the lock was released are
            // gone; carry on from the oldest that is left.
            let start = cursor.max(base);
            if start >= limit {
                break;
            }
            let end = limit.min(start + READ_CHUNK_EVENTS as u64);
            let from = (start - base) as usize;
            let to = (end - base) as usize;
            chunk.clear();
            chunk.extend(ring.events.range(from..to));
            drop(ring);
            out.extend(chunk.iter().filter(|event| query.matches(event)));
            bounds = Some((end, limit));
        }
        // Stages recorded after the fact, from times carried with the part,
        // land in the ring a little out of order.
        out.sort_by_key(|event| event.t_us);
        out
    }

    /// The matching events as NDJSON, one event per line, oldest first.
    /// Like [`Self::events`], it belongs off the async runtime.
    pub fn render_ndjson(&self, query: HopQuery) -> String {
        let events = self.events(query);
        let node = serde_json::to_string(&self.node).unwrap_or_else(|_| "\"\"".to_owned());
        let mut body = String::with_capacity(events.len() * 112);
        for event in &events {
            write_event_line(&mut body, &node, event);
        }
        body
    }
}

impl Ring {
    fn push(&mut self, event: HopEvent, capacity: usize) {
        if self.events.len() >= capacity {
            self.events.pop_front();
        }
        self.events.push_back(event);
        self.pushed += 1;
    }
}

fn seen_slot(stage: HopStage, stream: u64, seq: u64) -> usize {
    // A multiplicative mix: consecutive parts of one stream spread over the
    // table, and the stages of one part land apart.
    let mut hash = seq.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    hash ^= stream.wrapping_mul(0xC2B2_AE3D_27D4_EB4F).rotate_left(29);
    hash ^= u64::from(stage as u8).wrapping_mul(0x1656_67B1_9E37_79F9);
    hash ^= hash >> 31;
    (hash % SEEN_TABLE_SLOTS as u64) as usize
}

fn write_event_line(body: &mut String, node_json: &str, event: &HopEvent) {
    let _ = write!(
        body,
        "{{\"node\":{},\"stage\":\"{}\",\"stream\":{},\"seq\":{},\"t_us\":{}",
        node_json,
        event.stage.as_str(),
        event.stream,
        event.seq,
        event.t_us
    );
    if let Some(peer) = event.peer {
        let _ = write!(body, ",\"peer\":\"{peer}\"");
    }
    if let Some(epoch) = event.epoch {
        let _ = write!(body, ",\"epoch\":{epoch}");
    }
    if let Some(n) = event.n {
        let _ = write!(body, ",\"n\":{n}");
    }
    body.push_str("}\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(journal: &HopJournal, query: HopQuery) -> Vec<serde_json::Value> {
        journal
            .render_ndjson(query)
            .lines()
            .map(|line| serde_json::from_str(line).expect("each line is one JSON object"))
            .collect()
    }

    #[test]
    fn a_line_has_the_shared_format() {
        let journal = HopJournal::new("edge-\"a\"", 8);
        let peer: SocketAddr = "10.0.0.7:9101".parse().unwrap();
        journal.record(
            HopEvent::new(HopStage::MeshFirst, 4001, 17, 1_700_000_000_000_123)
                .with_peer(Some(peer)),
        );
        journal.record(HopEvent {
            epoch: Some(9),
            n: Some(2),
            ..HopEvent::new(HopStage::Committed, 4001, 18, 1_700_000_000_000_456)
        });
        let body = journal.render_ndjson(HopQuery::default());
        assert!(body.ends_with('\n'));
        let lines = lines(&journal, HopQuery::default());
        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0],
            serde_json::json!({
                "node": "edge-\"a\"",
                "stage": "mesh_first",
                "stream": 4001,
                "seq": 17,
                "t_us": 1_700_000_000_000_123_u64,
                "peer": "10.0.0.7:9101",
            })
        );
        assert_eq!(
            lines[1],
            serde_json::json!({
                "node": "edge-\"a\"",
                "stage": "committed",
                "stream": 4001,
                "seq": 18,
                "t_us": 1_700_000_000_000_456_u64,
                "epoch": 9,
                "n": 2,
            })
        );
    }

    #[test]
    fn stage_names_are_the_journal_names() {
        let names = [
            (HopStage::RelayFirst, "relay_first"),
            (HopStage::RelayForward, "relay_forward"),
            (HopStage::RelayDecoded, "relay_decoded"),
            (HopStage::Committed, "committed"),
            (HopStage::Watermark, "watermark"),
            (HopStage::MeshSent, "mesh_sent"),
            (HopStage::MeshFirst, "mesh_first"),
            (HopStage::MeshDecoded, "mesh_decoded"),
            (HopStage::EdgeCommitted, "edge_committed"),
            (HopStage::EdgePlaylist, "edge_playlist"),
            (HopStage::Reply, "reply"),
            (HopStage::UdpSent, "udp_sent"),
        ];
        for (stage, name) in names {
            assert_eq!(stage.as_str(), name);
        }
    }

    #[test]
    fn the_ring_keeps_the_newest_events_up_to_its_capacity() {
        let journal = HopJournal::new("n", 3);
        for seq in 0..5 {
            journal.record_part(HopStage::Committed, 1, seq, 100 + seq);
        }
        let seqs: Vec<u64> = journal
            .events(HopQuery::default())
            .iter()
            .map(|event| event.seq)
            .collect();
        assert_eq!(seqs, vec![2, 3, 4]);
    }

    #[test]
    fn a_zero_capacity_journal_records_nothing() {
        let journal = HopJournal::new("n", 0);
        assert!(!journal.is_enabled());
        journal.record_part(HopStage::Committed, 1, 1, 1);
        assert!(!journal.record_first(HopEvent::new(HopStage::RelayFirst, 1, 1, 1)));
        assert!(journal.events(HopQuery::default()).is_empty());
        assert!(journal.render_ndjson(HopQuery::default()).is_empty());
    }

    #[test]
    fn a_query_filters_by_time_and_stream_and_returns_oldest_first() {
        let journal = HopJournal::new("n", 16);
        journal.record_part(HopStage::Committed, 1, 1, 300);
        journal.record_part(HopStage::Committed, 2, 1, 100);
        journal.record_part(HopStage::Committed, 1, 2, 200);
        journal.record_part(HopStage::Committed, 1, 3, 50);
        let all: Vec<u64> = journal
            .events(HopQuery::default())
            .iter()
            .map(|event| event.t_us)
            .collect();
        assert_eq!(all, vec![50, 100, 200, 300]);
        let stream_one_since: Vec<(u64, u64)> = journal
            .events(HopQuery {
                since_us: Some(200),
                stream: Some(1),
            })
            .iter()
            .map(|event| (event.seq, event.t_us))
            .collect();
        assert_eq!(stream_one_since, vec![(2, 200), (1, 300)]);
    }

    #[test]
    fn a_read_larger_than_one_chunk_returns_every_event() {
        let total = READ_CHUNK_EVENTS * 2 + 17;
        let journal = HopJournal::new("n", total);
        for seq in 0..(total as u64 + 5) {
            journal.record_part(HopStage::Watermark, 7, seq, 1_000 + seq);
        }
        let events = journal.events(HopQuery::default());
        assert_eq!(events.len(), total);
        assert_eq!(events.first().unwrap().seq, 5);
        assert_eq!(events.last().unwrap().seq, total as u64 + 4);
    }

    #[test]
    fn first_and_counted_stages_record_once_or_up_to_their_bound() {
        let journal = HopJournal::new("n", 64);
        assert!(journal.record_first(HopEvent::new(HopStage::RelayFirst, 4001, 9, 10)));
        assert!(!journal.record_first(HopEvent::new(HopStage::RelayFirst, 4001, 9, 11)));
        // Another stage or another part is recorded on its own.
        assert!(journal.record_first(HopEvent::new(HopStage::RelayForward, 4001, 9, 12)));
        assert!(journal.record_first(HopEvent::new(HopStage::RelayFirst, 4001, 10, 13)));

        let ordinals: Vec<Option<u32>> = (0..6)
            .map(|index| {
                journal.record_counted(HopEvent::new(HopStage::Reply, 4001, 9, 20 + index), 4)
            })
            .collect();
        assert_eq!(
            ordinals,
            vec![Some(1), Some(2), Some(3), Some(4), None, None]
        );
        let replies: Vec<Option<u32>> = journal
            .events(HopQuery::default())
            .iter()
            .filter(|event| event.stage == HopStage::Reply)
            .map(|event| event.n)
            .collect();
        assert_eq!(replies, vec![Some(1), Some(2), Some(3), Some(4)]);
        let firsts = journal
            .events(HopQuery::default())
            .iter()
            .filter(|event| event.stage == HopStage::RelayFirst)
            .count();
        assert_eq!(firsts, 2);
        assert!(journal
            .events(HopQuery::default())
            .iter()
            .filter(|event| event.stage != HopStage::Reply)
            .all(|event| event.n.is_none()));
    }

    #[test]
    fn recording_from_many_threads_loses_nothing_below_capacity() {
        let journal = std::sync::Arc::new(HopJournal::new("n", 40_000));
        let threads: Vec<_> = (0..4_u64)
            .map(|stream| {
                let journal = std::sync::Arc::clone(&journal);
                std::thread::spawn(move || {
                    for seq in 0..5_000 {
                        journal.record_part(HopStage::Committed, stream, seq, seq);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(journal.events(HopQuery::default()).len(), 20_000);
        assert_eq!(
            journal
                .events(HopQuery {
                    since_us: None,
                    stream: Some(2)
                })
                .len(),
            5_000
        );
    }
}
