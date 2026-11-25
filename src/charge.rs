//! What a session of a stream costs an edge in bytes on the wire, per lane
//! (rm/charge).
//!
//! Admission charged a session the cache's measured bit rate of its
//! playlist: the stored canonical slots' bytes (envelope, key, metadata and
//! stamps with the media) over their durations, kept as a peak. For small
//! parts the envelope dominates: 1 ms PCM (304 B of media) was charged
//! ~1,020 B a part, 3.3 times its media, and 1 ms Opus Custom (56 B) far
//! more. An edge's capacity in listeners is what admission says it is, so
//! every charge now is what the session's lane sends:
//!
//! - the compact lane (UDP, small parts): each part as one datagram of the
//!   short header and the media, IP and UDP headers included, times one plus
//!   the copies and window-repair rows the edge's compact clients take (the
//!   edge's own measured ratio over the last look; none before it has one);
//! - the RaptorQ lane (UDP objects, heavy streams): the datagrams of the
//!   part's object as the edge encodes them at the lane's base repair, IP and
//!   UDP headers included;
//! - an LL-HLS reader: the media and a response's framing for each part
//!   (HTTP/2 HEADERS and DATA frames and a TLS record, ~200 B; playlist
//!   reloads not counted).
//!
//! The media itself (bytes and part rate) is the stream's sustained figure:
//! its newest parts over their own declared durations across 2 s, never a
//! peak, never arrival times. A charge rises at once and falls only after
//! [`FALL_AFTER`] below its value ([`ChargeTracker`]), so charges do not
//! flap; existing sessions are re-charged at the next look (admission never
//! removes a session, so no switch refuses one already admitted).

use av_mesh::client_compact::SHORT_HEADER_LEN;
use av_mesh::client_udp::{encode_object, ObjectKind, PartObject, RepairPolicy};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// IPv4 and UDP headers of a datagram.
pub(crate) const IP_UDP_BYTES: u64 = 28;
/// An LL-HLS part response's framing beside its media, approximately.
pub(crate) const HTTP_PART_FRAMING_BYTES: u64 = 200;
/// A charge falls only after its looks have stayed under it this long.
pub(crate) const FALL_AFTER: Duration = Duration::from_secs(30);

/// A stream's sustained media: bits a second, parts a second, bytes a part.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SustainedMedia {
    pub(crate) media_bps: u64,
    pub(crate) parts_per_second: f64,
    pub(crate) part_bytes: u64,
}

/// What one session of a stream sends on each lane, bits a second, with
/// IP and UDP headers (UDP lanes) or the HTTP framing (readers).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LaneCharges {
    pub(crate) media_bps: u64,
    pub(crate) compact_bps: u64,
    pub(crate) raptorq_bps: u64,
    pub(crate) http_bps: u64,
}

/// The wire bytes of one part of `part_bytes` on the RaptorQ lane, as the
/// edge encodes it for a client at the lane's base repair.
pub(crate) fn raptorq_part_wire_bytes(part_bytes: u64, symbol_size: u16) -> u64 {
    let object = PartObject {
        kind: ObjectKind::Part,
        datagrams: 0,
        stream_id: 0,
        part: 0,
        available_unix_us: 0,
        published_unix_us: 0,
        generation: 0,
        payload: Bytes::from(vec![0_u8; part_bytes.min(64 << 20) as usize]),
    };
    let repair = RepairPolicy::base_for_lane(false, None, None);
    encode_object(&object, symbol_size, repair)
        .map(|datagrams| {
            datagrams
                .iter()
                .map(|datagram| datagram.len() as u64 + IP_UDP_BYTES)
                .sum()
        })
        .unwrap_or(part_bytes + IP_UDP_BYTES)
}

/// The charges of a stream's lanes, from its sustained media, the edge's
/// client symbol size and the compact lane's extra datagrams a part (copies
/// and window repair, measured at the edge).
pub(crate) fn lane_charges(media: SustainedMedia, symbol_size: u16, compact_extra: f64) -> LaneCharges {
    let parts = media.parts_per_second.max(0.0);
    let per_part = |bytes: u64| (bytes as f64 * 8.0 * parts).round() as u64;
    LaneCharges {
        media_bps: media.media_bps,
        compact_bps: (per_part(SHORT_HEADER_LEN as u64 + media.part_bytes + IP_UDP_BYTES) as f64
            * (1.0 + compact_extra.max(0.0)))
        .round() as u64,
        raptorq_bps: per_part(raptorq_part_wire_bytes(media.part_bytes, symbol_size)),
        http_bps: media.media_bps + per_part(HTTP_PART_FRAMING_BYTES),
    }
}

/// A charge with hysteresis: up at once, down only after [`FALL_AFTER`] of
/// looks under it (then to the highest of those looks).
#[derive(Debug, Clone, Default)]
pub(crate) struct ChargeTracker {
    value: LaneCharges,
    /// The looks since the value was last raised or lowered.
    lower: Option<(Instant, LaneCharges)>,
}

impl ChargeTracker {
    pub(crate) fn observe(&mut self, look: LaneCharges, now: Instant) -> LaneCharges {
        let max = |a: LaneCharges, b: LaneCharges| LaneCharges {
            media_bps: a.media_bps.max(b.media_bps),
            compact_bps: a.compact_bps.max(b.compact_bps),
            raptorq_bps: a.raptorq_bps.max(b.raptorq_bps),
            http_bps: a.http_bps.max(b.http_bps),
        };
        let raised = max(self.value, look);
        if raised != self.value {
            self.value = raised;
            self.lower = None;
            return self.value;
        }
        if look == self.value {
            self.lower = None;
            return self.value;
        }
        let (since, highest) = self.lower.get_or_insert((now, look));
        *highest = max(*highest, look);
        if now.saturating_duration_since(*since) >= FALL_AFTER {
            self.value = *highest;
            self.lower = None;
        }
        self.value
    }
}

/// A UDP listener is charged what its leg sends where the region says
/// (`udp_charge_bps`), not a reader's charge.
pub(crate) fn charge_udp(candidates: &mut [crate::assignment::CacheCandidate]) {
    for candidate in candidates {
        if let Some(bps) = candidate.udp_charge_bps {
            candidate.session_charge_bps = Some(bps);
        }
    }
}

/// The compact lane's extra datagrams a part (copies, window repair) are
/// measured over this many parts at least, else none is counted.
const COMPACT_SAMPLE_PARTS: u64 = 200;

impl crate::AppRouter {
    /// The charges of a stream this edge holds, from its sustained media
    /// (`None` before its parts cover the window), with hysteresis.
    pub(crate) async fn stream_charges(&self, stream_id: u64) -> Option<LaneCharges> {
        let media = self
            .cache
            .sustained_media(stream_id, self.heavy.config.sustain)
            .await?;
        let extra = self.compact_extra();
        let look = lane_charges(media, self.heavy.client_symbol_size(), extra);
        let mut trackers = self.heavy.charges.lock().unwrap_or_else(|p| p.into_inner());
        Some(trackers.entry(stream_id).or_default().observe(look, Instant::now()))
    }

    /// The compact lane's copies and window-repair datagrams per part sent,
    /// over the parts since the last look (the edge's measured plans).
    fn compact_extra(&self) -> f64 {
        use std::sync::atomic::Ordering;
        let stats = &self.edge_udp_stats;
        let mut parts = stats.compact_parts.load(Ordering::Relaxed);
        let mut extras = stats.compact_copies.load(Ordering::Relaxed)
            + stats.compact_window_repair.load(Ordering::Relaxed);
        if let Some(lane) = stats.fast_lane.get().and_then(std::sync::Weak::upgrade) {
            let lane = lane.snapshot_stats();
            parts += lane.compact_parts;
            extras += lane.compact_top_ups;
        }
        let mut mark = self.heavy.compact_mark.lock().unwrap_or_else(|p| p.into_inner());
        let (last_parts, last_extras, ratio) = *mark;
        let new_parts = parts.saturating_sub(last_parts);
        if new_parts >= COMPACT_SAMPLE_PARTS {
            let ratio = extras.saturating_sub(last_extras) as f64 / new_parts as f64;
            *mark = (parts, extras, ratio);
            return ratio;
        }
        ratio
    }

    /// The highest reader charge an edge of the region advertises for a
    /// playlist (its stored slots' rate from an edge that says none).
    pub(crate) async fn regional_reader_charge(&self, stream_id: u64) -> Option<u64> {
        let snapshots = self.regional_snapshots().await;
        let holdings = snapshots
            .iter()
            .filter(|snapshot| snapshot.node.region == self.node.region)
            .filter_map(|snapshot| snapshot.edge_service.as_ref())
            .flat_map(|edge| edge.playlists.iter())
            .filter(|playlist| playlist.stream_id == stream_id)
            .collect::<Vec<_>>();
        holdings
            .iter()
            .filter_map(|playlist| playlist.lane_bps.map(|lanes| lanes.http_bps))
            .max()
            .or_else(|| holdings.iter().filter_map(|playlist| playlist.bitrate_bps).max())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn media(part_bytes: u64, part_us: u64) -> SustainedMedia {
        let parts_per_second = 1e6 / part_us as f64;
        SustainedMedia {
            media_bps: (part_bytes as f64 * 8.0 * parts_per_second) as u64,
            parts_per_second,
            part_bytes,
        }
    }

    #[test]
    fn a_small_part_is_charged_its_datagram_not_its_envelope() {
        // 1 ms PCM: 304 B of media, one datagram of 304 + 7 + 28 a ms.
        let pcm = lane_charges(media(304, 1_000), 1_200, 0.0);
        assert_eq!(pcm.media_bps, 2_432_000);
        assert_eq!(pcm.compact_bps, 2_712_000);
        // 1 ms Opus Custom, 56 B: 91 B on the wire a part, 728 kbit/s.
        let opus = lane_charges(media(56, 1_000), 1_200, 0.0);
        assert_eq!(opus.compact_bps, 728_000);
        // A copy of every part doubles the compact lane.
        assert_eq!(lane_charges(media(56, 1_000), 1_200, 1.0).compact_bps, 1_456_000);
    }

    #[test]
    fn a_charge_rises_at_once_and_falls_after_its_time() {
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let mut tracker = ChargeTracker::default();
        let look = |bps: u64| LaneCharges {
            media_bps: bps,
            compact_bps: bps,
            raptorq_bps: bps,
            http_bps: bps,
        };
        assert_eq!(tracker.observe(look(100), at(0)).http_bps, 100);
        assert_eq!(tracker.observe(look(300), at(1)).http_bps, 300);
        assert_eq!(tracker.observe(look(120), at(2)).http_bps, 300);
        assert_eq!(tracker.observe(look(150), at(20)).http_bps, 300);
        assert_eq!(tracker.observe(look(110), at(32)).http_bps, 150, "the highest look of the 30 s");
    }
}
