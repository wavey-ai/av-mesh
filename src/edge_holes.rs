//! Holes found by part distance, not by a timer (`--edge-hole-distance-ms`).
//!
//! A part N of a stream is a hole at an edge when a part H later has
//! arrived and N has not, with H the parts of the stream in the distance
//! (15 ms by default, and at least two parts, so that a symbol reordered
//! between the two parents is not taken for a loss). The edge then asks its
//! cache-mesh parents for the run of missing parts at once, as the arrival
//! that shows the hole is handled: no reader waits out a grace timer before
//! the request, and no timer runs on the live path for it. Each part is
//! looked at once per stream as the distance passes it; a request that is
//! lost is left to the readers' backstop wait and the replica request.
//!
//! An edge fed by the fast plane calls [`HoleTrigger::arrived`] from its
//! tee, for each symbol it takes (in the order the class thread took them,
//! so every part before it that had all its symbols is committed). An edge
//! fed by the cache mesh alone calls it for each part the mesh applies.

use crate::LiveTsCache;
use playlists::mesh::CacheMeshHandle;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::debug;

/// Default of --edge-hole-distance-ms.
pub(crate) const DEFAULT_EDGE_HOLE_DISTANCE_MS: u64 = 15;
/// Parts of distance at the least.
const MIN_DISTANCE_PARTS: u64 = 2;
/// Parts looked at for one arrival at most: a longer run of missing parts
/// is a gap of the stream, which the replica request handles.
const MAX_SCAN_PARTS: u64 = 64;
/// A stream's part duration is read again after this many arrivals.
const PART_US_REFRESH: u32 = 256;

#[derive(Debug, Clone, Copy)]
struct StreamHoles {
    part_us: u64,
    /// The first part not yet looked at.
    next: u64,
    arrivals: u32,
}

/// Which parts of each stream are due to be looked at.
#[derive(Debug)]
struct HoleScan {
    distance_us: u64,
    streams: HashMap<u64, StreamHoles>,
}

impl HoleScan {
    fn needs_part_us(&self, stream_id: u64) -> bool {
        self.streams
            .get(&stream_id)
            .is_none_or(|entry| entry.part_us == 0 || entry.arrivals >= PART_US_REFRESH)
    }

    /// The parts to look at now that `part` arrived, if any: from the first
    /// not yet looked at to `part` less the distance, at most
    /// [`MAX_SCAN_PARTS`]. The first arrival of a stream looks only at the
    /// part the distance behind it; a part far behind the scan (a new epoch
    /// that counts from 0) starts it again.
    fn due(&mut self, stream_id: u64, part: u64, part_us: Option<u64>) -> Option<(u64, u64)> {
        let distance_us = self.distance_us;
        let distance = |part_us: u64| distance_us.div_ceil(part_us.max(1)).max(MIN_DISTANCE_PARTS);
        let entry = self.streams.entry(stream_id).or_insert(StreamHoles {
            part_us: 0,
            next: u64::MAX,
            arrivals: 0,
        });
        if let Some(part_us) = part_us {
            entry.part_us = part_us;
            entry.arrivals = 0;
        }
        entry.arrivals = entry.arrivals.saturating_add(1);
        let upto = part.checked_sub(distance(entry.part_us))?;
        if entry.next == u64::MAX || entry.next > upto.saturating_add(MAX_SCAN_PARTS) {
            entry.next = upto + 1;
            return Some((upto, upto));
        }
        if upto < entry.next {
            return None;
        }
        let from = entry.next.max(upto.saturating_sub(MAX_SCAN_PARTS - 1));
        entry.next = upto + 1;
        Some((from, upto))
    }
}

pub(crate) struct HoleTrigger {
    cache: Arc<LiveTsCache>,
    mesh: Arc<CacheMeshHandle>,
    scan: Mutex<HoleScan>,
    /// Requests sent, and the parts they asked for.
    pub requests: AtomicU64,
    pub holes: AtomicU64,
}

impl std::fmt::Debug for HoleTrigger {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HoleTrigger")
            .field("scan", &self.scan)
            .field("requests", &self.requests)
            .field("holes", &self.holes)
            .finish_non_exhaustive()
    }
}

impl HoleTrigger {
    /// `None` when `distance` is zero (the readers' grace timer, as before).
    pub(crate) fn new(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        distance: Duration,
    ) -> Option<Arc<Self>> {
        let distance_us = u64::try_from(distance.as_micros()).unwrap_or(u64::MAX);
        (distance_us > 0).then(|| {
            Arc::new(Self {
                cache,
                mesh,
                scan: Mutex::new(HoleScan {
                    distance_us,
                    streams: HashMap::new(),
                }),
                requests: AtomicU64::new(0),
                holes: AtomicU64::new(0),
            })
        })
    }

    /// A symbol (or the whole) of `part` of `stream_id` arrived. Ask the
    /// parents for the parts the distance behind it that are missing.
    pub(crate) async fn arrived(&self, stream_id: u64, part: u64) {
        let needs_part_us = self.scan.lock().unwrap().needs_part_us(stream_id);
        let part_us = if needs_part_us {
            Some(self.cache.state.read().await.stream_part_us(stream_id))
        } else {
            None
        };
        let Some((from, upto)) = self.scan.lock().unwrap().due(stream_id, part, part_us) else {
            return;
        };
        let chunk_cache = &self.cache.chunk_cache;
        // A stream this node holds nothing of yet has no holes.
        if chunk_cache.stream_idx(stream_id).is_none() {
            return;
        }
        let missing = |slot: u64| {
            usize::try_from(slot).is_ok_and(|slot| {
                chunk_cache
                    .written_unix_us_for_stream_id(stream_id, slot)
                    .is_none()
            })
        };
        let mut slot = from;
        while slot <= upto {
            if !missing(slot) {
                slot += 1;
                continue;
            }
            let start = slot;
            while slot <= upto && missing(slot) {
                slot += 1;
            }
            let count = (slot - start) as u32;
            self.holes.fetch_add(u64::from(count), Ordering::Relaxed);
            self.requests.fetch_add(1, Ordering::Relaxed);
            let mesh = Arc::clone(&self.mesh);
            // Sent beside the caller: the tee goes on with its datagrams.
            tokio::spawn(async move {
                if let Err(error) = mesh
                    .request_parts(stream_id, start as usize, count, 1)
                    .await
                {
                    debug!(stream_id, start, count, error = %error, "hole request failed");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_part_is_looked_at_once_when_the_distance_passes_it() {
        let mut scan = HoleScan {
            distance_us: 15_000,
            streams: HashMap::new(),
        };
        // 1 ms parts: 15 parts of distance.
        assert!(scan.needs_part_us(7));
        assert_eq!(scan.due(7, 10, Some(1_000)), None, "nothing 15 parts back");
        assert_eq!(scan.due(7, 115, None), Some((100, 100)), "the first look");
        assert_eq!(scan.due(7, 116, None), Some((101, 101)));
        assert_eq!(scan.due(7, 116, None), None, "looked at already");
        assert_eq!(scan.due(7, 120, None), Some((102, 105)), "the parts passed");
        assert_eq!(scan.due(7, 118, None), None, "a reordered symbol");
        // A long silence: only the newest MAX_SCAN_PARTS.
        assert_eq!(scan.due(7, 1_000, None), Some((922, 985)));
        // A new epoch counts from 0: the scan starts again.
        assert_eq!(scan.due(7, 20, None), Some((5, 5)));
        assert_eq!(scan.due(7, 21, None), Some((6, 6)));
        // 40 ms video parts: two parts at the least.
        assert_eq!(scan.due(1, 50, Some(40_000)), Some((48, 48)));
        assert!(!scan.needs_part_us(1));
    }
}
