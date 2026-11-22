//! Holes found by part distance, not by a timer (`--edge-hole-distance-ms`).
//!
//! A part N of a stream is a hole at an edge when a part H later has
//! arrived and N has not, with H the parts of the stream in the distance
//! (15 ms by default, and at least two parts, so that a symbol reordered
//! between the two parents is not taken for a loss). The edge then asks its
//! cache-mesh parents for the run of missing parts at once, as the arrival
//! that shows the hole is handled: no reader waits out a grace timer before
//! the request, and no timer runs on the live path for it. Each part is
//! looked at twice per stream: as the distance passes it (one parent is
//! asked), and as twice the distance passes it (both parents are asked for
//! a part still missing: the first request was lost, or the parent asked
//! did not have the part). A request that is lost again is left to the
//! readers' backstop wait (twice the distance) and the replica request.
//!
//! An edge fed by the fast plane calls [`HoleTrigger::arrived`] from its
//! tee, for each symbol it takes (in the order the class thread took them,
//! so every part before it that had all its symbols is committed). The
//! cache mesh calls it for each part it applies, which counts for a stream
//! that the fast plane has not fed for a while (an edge fed by the cache
//! mesh alone, a lease refused, the forwarders down).

use crate::LiveTsCache;
use playlists::mesh::CacheMeshHandle;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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
/// Each part is looked at this many times, at 1x and 2x the distance.
const LOOKS: usize = 2;
/// The cache mesh's arrivals of a stream count for its holes when the fast
/// plane has fed the stream nothing for this long (and four parts).
const FAST_PLANE_QUIET: Duration = Duration::from_millis(250);

/// Who saw a part arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Arrival {
    /// The fast-plane tee, symbol by symbol.
    FastPlane,
    /// The cache mesh, part by part.
    CacheMesh,
}

#[derive(Debug, Clone, Copy)]
struct StreamHoles {
    part_us: u64,
    /// For each look, the first part not yet looked at.
    next: [u64; LOOKS],
    arrivals: u32,
    /// When the fast plane last fed the stream.
    fast_at: Option<Instant>,
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

    /// Whether an arrival seen by `arrival` counts for the holes of the
    /// stream at `now`: the fast plane's always, the cache mesh's when the
    /// fast plane has not fed the stream for a while.
    fn counts(&mut self, stream_id: u64, arrival: Arrival, now: Instant) -> bool {
        match arrival {
            Arrival::FastPlane => {
                if let Some(entry) = self.streams.get_mut(&stream_id) {
                    entry.fast_at = Some(now);
                } else {
                    self.streams.insert(
                        stream_id,
                        StreamHoles {
                            part_us: 0,
                            next: [u64::MAX; LOOKS],
                            arrivals: 0,
                            fast_at: Some(now),
                        },
                    );
                }
                true
            }
            Arrival::CacheMesh => self.streams.get(&stream_id).is_none_or(|entry| {
                let quiet =
                    FAST_PLANE_QUIET.max(Duration::from_micros(entry.part_us.saturating_mul(4)));
                entry
                    .fast_at
                    .is_none_or(|at| now.saturating_duration_since(at) >= quiet)
            }),
        }
    }

    /// The parts to look at now that `part` arrived, for each look: from
    /// the first not yet looked at to `part` less the look's distance (1x,
    /// then 2x), at most [`MAX_SCAN_PARTS`]. The first arrival of a stream
    /// looks only at the part the distance behind it; a part far behind the
    /// scan (a new epoch that counts from 0) starts it again.
    fn due(
        &mut self,
        stream_id: u64,
        part: u64,
        part_us: Option<u64>,
    ) -> [Option<(u64, u64)>; LOOKS] {
        let distance_us = self.distance_us;
        let distance = |part_us: u64| distance_us.div_ceil(part_us.max(1)).max(MIN_DISTANCE_PARTS);
        let entry = self.streams.entry(stream_id).or_insert(StreamHoles {
            part_us: 0,
            next: [u64::MAX; LOOKS],
            arrivals: 0,
            fast_at: None,
        });
        if let Some(part_us) = part_us {
            entry.part_us = part_us;
            entry.arrivals = 0;
        }
        entry.arrivals = entry.arrivals.saturating_add(1);
        let distance = distance(entry.part_us);
        let mut due = [None; LOOKS];
        for (look, next) in entry.next.iter_mut().enumerate() {
            let Some(upto) = part.checked_sub(distance.saturating_mul(look as u64 + 1)) else {
                continue;
            };
            if *next == u64::MAX || *next > upto.saturating_add(MAX_SCAN_PARTS) {
                *next = upto + 1;
                due[look] = Some((upto, upto));
                continue;
            }
            if upto < *next {
                continue;
            }
            let from = (*next).max(upto.saturating_sub(MAX_SCAN_PARTS - 1));
            *next = upto + 1;
            due[look] = Some((from, upto));
        }
        due
    }
}

pub(crate) struct HoleTrigger {
    cache: Arc<LiveTsCache>,
    mesh: Arc<CacheMeshHandle>,
    scan: Mutex<HoleScan>,
    /// Requests sent, and the parts they asked for.
    pub requests: AtomicU64,
    pub holes: AtomicU64,
    /// Of them, the requests of the second look (to both parents).
    pub rechecks: AtomicU64,
}

impl std::fmt::Debug for HoleTrigger {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HoleTrigger")
            .field("scan", &self.scan)
            .field("requests", &self.requests)
            .field("holes", &self.holes)
            .field("rechecks", &self.rechecks)
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
                rechecks: AtomicU64::new(0),
            })
        })
    }

    /// A symbol (or the whole) of `part` of `stream_id` arrived, seen by
    /// `arrival`. Ask a parent for the parts the distance behind it that are
    /// missing, and both parents for those twice the distance behind it
    /// that are still missing.
    pub(crate) async fn arrived(&self, stream_id: u64, part: u64, arrival: Arrival) {
        let needs_part_us = {
            let mut scan = self.scan.lock().unwrap();
            if !scan.counts(stream_id, arrival, Instant::now()) {
                return;
            }
            scan.needs_part_us(stream_id)
        };
        let part_us = if needs_part_us {
            Some(self.cache.state.read().await.stream_part_us(stream_id))
        } else {
            None
        };
        let due = self.scan.lock().unwrap().due(stream_id, part, part_us);
        for (look, range) in due.into_iter().enumerate() {
            if let Some((from, upto)) = range {
                self.ask(stream_id, from, upto, look + 1);
            }
        }
    }

    /// Ask `parents` parents for the runs of parts from `from` to `upto`
    /// that are missing.
    fn ask(&self, stream_id: u64, from: u64, upto: u64, parents: usize) {
        let chunk_cache = &self.cache.chunk_cache;
        // A stream this node holds nothing of yet has no holes.
        let Some(handle) = chunk_cache.resolve_stream(stream_id) else {
            return;
        };
        let missing = |slot: u64| {
            usize::try_from(slot).is_ok_and(|slot| {
                chunk_cache
                    .written_unix_us_for_handle(handle, slot)
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
            if parents > 1 {
                self.rechecks.fetch_add(1, Ordering::Relaxed);
            }
            let mesh = Arc::clone(&self.mesh);
            // Sent beside the caller: the tee goes on with its datagrams.
            tokio::spawn(async move {
                if let Err(error) = mesh
                    .request_parts(stream_id, start as usize, count, parents)
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
        let mut first = |stream_id, part, part_us| scan.due(stream_id, part, part_us)[0];
        // 1 ms parts: 15 parts of distance.
        assert_eq!(first(7, 10, Some(1_000)), None, "nothing 15 parts back");
        assert_eq!(first(7, 115, None), Some((100, 100)), "the first look");
        assert_eq!(first(7, 116, None), Some((101, 101)));
        assert_eq!(first(7, 116, None), None, "looked at already");
        assert_eq!(first(7, 120, None), Some((102, 105)), "the parts passed");
        assert_eq!(first(7, 118, None), None, "a reordered symbol");
        // A long silence: only the newest MAX_SCAN_PARTS.
        assert_eq!(first(7, 1_000, None), Some((922, 985)));
        // A new epoch counts from 0: the scan starts again.
        assert_eq!(first(7, 20, None), Some((5, 5)));
        assert_eq!(first(7, 21, None), Some((6, 6)));
        // 40 ms video parts: two parts at the least.
        assert_eq!(first(1, 50, Some(40_000)), Some((48, 48)));
        assert!(!scan.needs_part_us(1));
        assert!(scan.needs_part_us(9));
    }

    /// The second look comes at twice the distance: a hole that the first
    /// request did not fill is asked again (of both parents) then.
    #[test]
    fn a_part_is_looked_at_again_at_twice_the_distance() {
        let mut scan = HoleScan {
            distance_us: 15_000,
            streams: HashMap::new(),
        };
        assert_eq!(
            scan.due(7, 115, Some(1_000)),
            [Some((100, 100)), Some((85, 85))]
        );
        assert_eq!(scan.due(7, 116, None), [Some((101, 101)), Some((86, 86))]);
        assert_eq!(scan.due(7, 116, None), [None, None]);
        assert_eq!(scan.due(7, 131, None), [Some((102, 116)), Some((87, 101))]);
        // Nothing twice the distance back yet: only the first look.
        assert_eq!(scan.due(8, 20, Some(1_000)), [Some((5, 5)), None]);
    }

    /// The cache mesh's arrivals count for a stream only while the fast
    /// plane has not fed it for a while.
    #[test]
    fn the_cache_mesh_looks_for_holes_when_the_fast_plane_is_quiet() {
        let mut scan = HoleScan {
            distance_us: 15_000,
            streams: HashMap::new(),
        };
        let now = Instant::now();
        assert!(scan.counts(7, Arrival::CacheMesh, now), "never fed fast");
        assert!(scan.counts(7, Arrival::FastPlane, now));
        assert!(!scan.counts(7, Arrival::CacheMesh, now + Duration::from_millis(10)));
        assert!(scan.counts(7, Arrival::CacheMesh, now + FAST_PLANE_QUIET));
        assert!(scan.counts(8, Arrival::CacheMesh, now));
    }
}
