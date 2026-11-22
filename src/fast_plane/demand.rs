//! Rendition demand up the relay tree (wave 7 item 5).
//!
//! A source can publish several renditions of the same audio (the DAW lane's
//! FLAC, Opus and PCM, streams 2001, 3001 and 4001). Lease children (the
//! edges) already take only the streams they hold a lease for; the compiled
//! (long-haul) lanes and the contributor's own lanes carried every
//! rendition whether anyone read it or not.
//!
//! Each node with relay parents tells them, every 250 ms, which streams it
//! has demand for (`RLD1`): the streams its cache-mesh children lease from
//! it, the streams it subscribes to itself, and the streams its own compiled
//! children report. A forwarder then sends a rendition on a compiled lane
//! only when the child asks for it, or when the child asks for no rendition
//! of its group at all (so a source nobody reads stays live everywhere and
//! the first reader of a region starts from a running stream), or when the
//! child has not reported for two seconds (an older node, a lost report: the
//! lane fails open). The contributor applies the same rule to its two lanes
//! before it publishes a part at all.
//!
//! Streams outside every group (`--relay-demand-renditions`, empty by
//! default) are never held back. The gate is off by default until a held
//! rendition's first reader starts cleanly: in the local lab the first UDP
//! client of a held PCM rendition waited 3.8 s for its first part in London,
//! and in New York joined at the stale head that its distributor kept from
//! before the hold and counted every part since as missing.
//!
//! Wire format, big-endian: `RLD1`, version 1, one zero byte, stream count
//! (u16, at most [`MAX_DEMAND_STREAMS`]), then each stream id (u64).

use arc_swap::ArcSwapOption;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const DEMAND_REPORT_MAGIC: [u8; 4] = *b"RLD1";
pub const DEMAND_REPORT_VERSION: u8 = 1;
pub const MAX_DEMAND_STREAMS: usize = 128;
/// How often a node reports its demand to its parents.
pub const DEMAND_REPORT_INTERVAL: Duration = Duration::from_millis(250);
/// A report counts this long; a lane without a fresh one fails open.
pub const DEMAND_TTL: Duration = Duration::from_secs(2);

/// An `RLD1` datagram of `streams` (the first [`MAX_DEMAND_STREAMS`]).
#[must_use]
pub fn encode_demand(streams: &[u64]) -> Vec<u8> {
    let streams = &streams[..streams.len().min(MAX_DEMAND_STREAMS)];
    let mut out = Vec::with_capacity(8 + streams.len() * 8);
    out.extend_from_slice(&DEMAND_REPORT_MAGIC);
    out.push(DEMAND_REPORT_VERSION);
    out.push(0);
    out.extend_from_slice(&(streams.len() as u16).to_be_bytes());
    for stream in streams {
        out.extend_from_slice(&stream.to_be_bytes());
    }
    out
}

/// The streams of an `RLD1` datagram.
#[must_use]
pub fn decode_demand(datagram: &[u8]) -> Option<Vec<u64>> {
    if datagram.len() < 8 || datagram[..4] != DEMAND_REPORT_MAGIC || datagram[4] != DEMAND_REPORT_VERSION
    {
        return None;
    }
    let count = usize::from(u16::from_be_bytes([datagram[6], datagram[7]]));
    if count > MAX_DEMAND_STREAMS || datagram.len() != 8 + count * 8 {
        return None;
    }
    Some(
        datagram[8..]
            .chunks_exact(8)
            .map(|chunk| u64::from_be_bytes(chunk.try_into().expect("eight bytes")))
            .collect(),
    )
}

/// The rendition groups of `--relay-demand-renditions`: `;`-separated
/// groups of `,`-separated stream ids. Empty: no group, nothing held back.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RenditionGroups(Arc<Vec<Vec<u64>>>);

impl std::str::FromStr for RenditionGroups {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut groups = Vec::new();
        for group in value.split(';').map(str::trim).filter(|group| !group.is_empty()) {
            let streams = group
                .split(',')
                .map(|stream| {
                    stream
                        .trim()
                        .parse::<u64>()
                        .map_err(|_| format!("{stream:?} is not a stream id"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if streams.len() < 2 {
                return Err(format!("a rendition group needs two streams or more: {group:?}"));
            }
            groups.push(streams);
        }
        Ok(Self(Arc::new(groups)))
    }
}

impl RenditionGroups {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn group_of(&self, stream: u64) -> Option<&[u64]> {
        self.0
            .iter()
            .find(|group| group.contains(&stream))
            .map(Vec::as_slice)
    }

    /// Whether a lane whose child reports `demand` (None: no fresh report)
    /// takes `stream`: a stream outside every group, a child without a
    /// report, a child that asks for it, or a child that asks for no
    /// rendition of its group.
    pub fn allows(&self, stream: u64, demand: Option<&[u64]>) -> bool {
        let Some(demand) = demand else {
            return true;
        };
        let Some(group) = self.group_of(stream) else {
            return true;
        };
        demand.contains(&stream) || !group.iter().any(|other| demand.contains(other))
    }
}

/// The last demand report of one lane.
#[derive(Debug, Default)]
pub struct DemandSlot(ArcSwapOption<(Instant, Vec<u64>)>);

impl DemandSlot {
    pub fn record(&self, streams: Vec<u64>, now: Instant) {
        self.0.store(Some(Arc::new((now, streams))));
    }

    /// The streams of the last report, if it is fresh.
    pub fn fresh(&self, now: Instant) -> Option<Arc<(Instant, Vec<u64>)>> {
        self.0
            .load_full()
            .filter(|report| now.saturating_duration_since(report.0) <= DEMAND_TTL)
    }
}

/// What this node reports to its parents: rebuilt every
/// [`DEMAND_REPORT_INTERVAL`], read by the audio thread.
#[derive(Debug, Default)]
pub struct NodeDemand(arc_swap::ArcSwap<Vec<u8>>);

impl NodeDemand {
    /// Report `streams`. More than [`MAX_DEMAND_STREAMS`] would not fit one
    /// report: the node then reports nothing, and its parents fail open,
    /// rather than drop the highest ids and hold back renditions that are
    /// read (wave 7 review).
    pub fn set(&self, mut streams: Vec<u64>) {
        streams.sort_unstable();
        streams.dedup();
        if streams.len() > MAX_DEMAND_STREAMS {
            self.clear();
            return;
        }
        self.0.store(Arc::new(encode_demand(&streams)));
    }

    /// Report nothing: the node's demand is unknown.
    pub fn clear(&self) {
        self.0.store(Arc::new(Vec::new()));
    }

    /// The `RLD1` datagram to send; empty before the first `set`.
    pub fn report(&self) -> Arc<Vec<u8>> {
        self.0.load_full()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_report_round_trips_and_garbage_is_refused() {
        let bytes = encode_demand(&[2001, 3001]);
        assert_eq!(&bytes[..4], b"RLD1");
        assert_eq!(decode_demand(&bytes), Some(vec![2001, 3001]));
        assert_eq!(decode_demand(&encode_demand(&[])), Some(vec![]));
        assert_eq!(decode_demand(&bytes[..bytes.len() - 1]), None);
        assert_eq!(decode_demand(b"RLR1\x01\x00\x00\x00"), None);
        let many: Vec<u64> = (0..500).collect();
        assert_eq!(decode_demand(&encode_demand(&many)).unwrap().len(), MAX_DEMAND_STREAMS);
    }

    #[test]
    fn groups_parse_and_hold_back_only_unread_renditions_of_a_read_source() {
        let groups: RenditionGroups = "2001,3001,4001; 11,12".parse().unwrap();
        assert!("2001".parse::<RenditionGroups>().is_err());
        assert!("2001,x".parse::<RenditionGroups>().is_err());
        assert!("".parse::<RenditionGroups>().unwrap().is_empty());
        // No report: everything goes.
        assert!(groups.allows(4001, None));
        // Opus read: PCM and FLAC stay home, Opus goes.
        let opus = [3001_u64];
        assert!(groups.allows(3001, Some(&opus)));
        assert!(!groups.allows(4001, Some(&opus)));
        assert!(!groups.allows(2001, Some(&opus)));
        // Nothing of the source read: every rendition stays live.
        let other = [1_u64];
        assert!(groups.allows(4001, Some(&other)));
        assert!(groups.allows(4001, Some(&[])));
        // A stream of no group always goes.
        assert!(groups.allows(1, Some(&opus)));
        // The other group is its own.
        assert!(groups.allows(12, Some(&opus)));
        assert!(!groups.allows(12, Some(&[11, 3001])));
    }

    #[test]
    fn a_report_is_fresh_for_two_seconds() {
        let slot = DemandSlot::default();
        let now = Instant::now();
        assert!(slot.fresh(now).is_none());
        slot.record(vec![3001], now);
        assert_eq!(slot.fresh(now).unwrap().1, vec![3001]);
        assert!(slot.fresh(now + Duration::from_millis(2_100)).is_none());
        let node = NodeDemand::default();
        assert!(node.report().is_empty());
        node.set(vec![4001, 3001, 3001]);
        assert_eq!(decode_demand(&node.report()), Some(vec![3001, 4001]));
        // Too many streams for one report: none, so the parents fail open.
        node.set((0..=MAX_DEMAND_STREAMS as u64).collect());
        assert!(node.report().is_empty());
        node.set(vec![3001]);
        node.clear();
        assert!(node.report().is_empty());
    }
}
