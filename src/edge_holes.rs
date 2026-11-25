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
//!
//! send240 (C): every ask goes through the node's pull gate
//! (`crate::pull_gate`): a part this node shed itself is never asked for, a
//! lane whose tee lags or sheds asks for nothing, and the asks share a
//! budget of bytes a second. A hole is asked of one parent at the first
//! look and of the other alone at the second, which comes no sooner than
//! twice the measured time a parent takes to answer. The quiet catch-up asks
//! one parent for a few parts, from the stream's newest part (a new session
//! starts it again from the new session's head).

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
/// A stream the fast plane fed and then left quiet this long (and four
/// parts) is asked of the cache-mesh parents beyond its newest part: the
/// catch-up when both fast-plane parents are silent (4K on Linode, 24 Nov:
/// a stop of the only parent that sent an edge a video stream's sources
/// left the edge without its parts until the parent came back, 2.2 to
/// 2.65 s, although the other parent had them 35 ms after packaging).
const QUIET_CATCH_UP: Duration = Duration::from_millis(150);
/// While quiet, the ask is repeated this often (and every two parts), so
/// the parts that the parent commits meanwhile come at about this delay.
const QUIET_REPEAT: Duration = Duration::from_millis(50);
/// A quiet stream is asked for no longer than this after its last part
/// from anywhere: an ended stream costs a few small requests, no more.
const QUIET_GIVE_UP: Duration = Duration::from_secs(5);
/// Parts the quiet catch-up asks for at once (send240: it asked for 64, of
/// both parents, every 50 ms).
const QUIET_PARTS: u32 = 4;
/// Asks whose answer the trigger times (the wait before a second parent).
const TIMED_ASKS: usize = 256;

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
    /// The newest part seen from anywhere, and when it came.
    head: Option<u64>,
    head_at: Option<Instant>,
    /// When the quiet catch-up last asked, and how often (its parent
    /// alternates).
    quiet_asked: Option<Instant>,
    quiet_asks: u32,
    /// The parts' size (bytes) as they arrive: the pull gate charges it.
    part_bytes: u64,
}

/// Which parts of each stream are due to be looked at.
#[derive(Debug)]
struct HoleScan {
    distance_us: u64,
    streams: HashMap<u64, StreamHoles>,
    /// The time from an ask to its part (µs, smoothed; 0 until measured),
    /// and the asks being timed.
    fill_us: u64,
    asked: std::collections::VecDeque<(u64, u64, Instant)>,
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
                            head: None,
                            head_at: None,
                            quiet_asked: None,
                            quiet_asks: 0,
                            part_bytes: 0,
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
            head: None,
            head_at: None,
            quiet_asked: None,
            quiet_asks: 0,
            part_bytes: 0,
        });
        // A part far behind the head is a new epoch that counts from 0: the
        // quiet catch-up starts again from its head (send240).
        if entry
            .head
            .is_some_and(|head| head > part.saturating_add(MAX_SCAN_PARTS))
        {
            entry.quiet_asked = None;
            entry.quiet_asks = 0;
        }
        if entry
            .head
            .is_none_or(|head| part > head || head > part.saturating_add(MAX_SCAN_PARTS))
        {
            entry.head = Some(part);
        }
        entry.head_at = Some(Instant::now());
        if let Some(part_us) = part_us {
            entry.part_us = part_us;
            entry.arrivals = 0;
        }
        entry.arrivals = entry.arrivals.saturating_add(1);
        let distance = distance(entry.part_us);
        // send240: the second look (the other parent) comes no sooner than
        // twice the measured time an ask takes to be answered.
        let answer = (2 * self.fill_us).div_ceil(entry.part_us.max(1));
        let offsets = [distance, distance + distance.max(answer)];
        let mut due = [None; LOOKS];
        for (look, next) in entry.next.iter_mut().enumerate() {
            let Some(upto) = part.checked_sub(offsets[look]) else {
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

impl HoleScan {
    /// An ask of `part` of `stream_id` went out: its answer is timed.
    fn timing(&mut self, stream_id: u64, part: u64, now: Instant) {
        if self.asked.len() == TIMED_ASKS {
            self.asked.pop_front();
        }
        self.asked.push_back((stream_id, part, now));
    }

    /// Part `part` of `stream_id` came over the cache mesh: if it was
    /// asked for, the time it took (the wait before a second parent).
    fn answered(&mut self, stream_id: u64, part: u64, now: Instant) -> Option<u64> {
        let at = self
            .asked
            .iter()
            .position(|(stream, asked, _)| *stream == stream_id && *asked == part)?;
        let (_, _, asked_at) = self.asked.remove(at)?;
        let took = now.saturating_duration_since(asked_at).as_micros() as u64;
        self.fill_us = if self.fill_us == 0 {
            took
        } else {
            (self.fill_us * 7 + took) / 8
        };
        Some(self.fill_us)
    }

    /// The streams to ask of the cache-mesh parents now, with the first
    /// part to ask for and the parent's rank: each stream that the fast
    /// plane fed and has left quiet for [`QUIET_CATCH_UP`] (and four
    /// parts), again every [`QUIET_REPEAT`] (and two parts) while it stays
    /// quiet, until [`QUIET_GIVE_UP`] after its last part from anywhere.
    /// The parent alternates.
    fn quiet_due(&mut self, now: Instant) -> Vec<(u64, u64, usize)> {
        let mut out = Vec::new();
        for (&stream_id, entry) in &mut self.streams {
            let (Some(fast_at), Some(head)) = (entry.fast_at, entry.head) else {
                continue;
            };
            let part = Duration::from_micros(entry.part_us);
            if now.saturating_duration_since(fast_at) < QUIET_CATCH_UP.max(part * 4) {
                continue;
            }
            if entry
                .head_at
                .is_some_and(|at| now.saturating_duration_since(at) >= QUIET_GIVE_UP)
            {
                continue;
            }
            if entry
                .quiet_asked
                .is_some_and(|at| now.saturating_duration_since(at) < QUIET_REPEAT.max(part * 2))
            {
                continue;
            }
            entry.quiet_asked = Some(now);
            out.push((stream_id, head + 1, (entry.quiet_asks % 2) as usize));
            entry.quiet_asks = entry.quiet_asks.wrapping_add(1);
        }
        out
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
    /// Requests of the quiet catch-up (both parents, beyond the head).
    pub quiet_requests: AtomicU64,
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
                    fill_us: 0,
                    asked: std::collections::VecDeque::with_capacity(TIMED_ASKS),
                }),
                requests: AtomicU64::new(0),
                holes: AtomicU64::new(0),
                rechecks: AtomicU64::new(0),
                quiet_requests: AtomicU64::new(0),
            })
        })
    }

    /// A symbol (or the whole) of `part` of `stream_id` arrived, seen by
    /// `arrival`. Ask a parent for the parts the distance behind it that are
    /// missing, and both parents for those twice the distance behind it
    /// that are still missing.
    /// [`Self::arrived`] from the fast plane's tee of `class`, which took
    /// the symbol `lag_us` after its class thread (a lagging tee stops its
    /// lane's pulls), of a part of about `part_bytes` (send240).
    pub(crate) async fn arrived_from_tee(
        &self,
        stream_id: u64,
        part: u64,
        class: crate::fast_plane::TrafficClass,
        lag_us: u64,
        part_bytes: u64,
    ) {
        self.cache.pull_gate.note_tee_lag(class, lag_us);
        self.arrived_sized(stream_id, part, Arrival::FastPlane, part_bytes)
            .await;
    }

    /// [`Self::arrived`] of a part of `part_bytes` (0: unknown).
    pub(crate) async fn arrived_sized(
        &self,
        stream_id: u64,
        part: u64,
        arrival: Arrival,
        part_bytes: u64,
    ) {
        let needs_part_us = {
            let mut scan = self.scan.lock().unwrap();
            let now = Instant::now();
            if arrival == Arrival::CacheMesh {
                if let Some(fill_us) = scan.answered(stream_id, part, now) {
                    self.cache.pull_gate.fill_us.store(fill_us, Ordering::Relaxed);
                }
            }
            if let Some(entry) = scan.streams.get_mut(&stream_id) {
                if part_bytes > 0 && entry.part_bytes != part_bytes {
                    entry.part_bytes = part_bytes;
                    self.cache.pull_gate.observe_part_bytes(stream_id, part_bytes);
                }
            }
            if !scan.counts(stream_id, arrival, now) {
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

    /// The quiet catch-up (call it every few tens of milliseconds, off the
    /// live path): ask both cache-mesh parents for the parts beyond the
    /// head of each stream that the fast plane has left quiet. A parent
    /// sends what it holds of them; the parts that arrive are looked at
    /// for holes as the cache mesh's arrivals of a quiet stream are.
    pub(crate) fn sweep_quiet(&self) {
        let due = self.scan.lock().unwrap().quiet_due(Instant::now());
        for (stream_id, from, rank) in due {
            // send240: a few parts, of one parent, through the gate (a
            // lagging lane asks for nothing; the budget is shared).
            let class = self.cache.stream_classes().class(stream_id);
            if self
                .cache
                .pull_gate
                .admit(class, stream_id, from, QUIET_PARTS)
                .is_err()
            {
                continue;
            }
            self.quiet_requests.fetch_add(1, Ordering::Relaxed);
            self.cache
                .pull_gate
                .quiet_requests
                .fetch_add(1, Ordering::Relaxed);
            let mesh = Arc::clone(&self.mesh);
            tokio::spawn(async move {
                if let Err(error) = mesh
                    .request_parts_from_rank(stream_id, from as usize, QUIET_PARTS, rank)
                    .await
                {
                    debug!(stream_id, from, error = %error, "quiet catch-up request failed");
                }
            });
        }
    }

    /// Ask for the runs of parts from `from` to `upto` that are missing: at
    /// the first look (`parents` 1) of the first parent, at the second (2)
    /// of the second parent alone (send240: of both). Each run goes through
    /// the pull gate: a part shed here splits the run and is not asked for.
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
        let gate = &self.cache.pull_gate;
        let class = self.cache.stream_classes().class(stream_id);
        let rank = parents.saturating_sub(1);
        let mut slot = from;
        while slot <= upto {
            if !missing(slot) {
                slot += 1;
                continue;
            }
            if gate.is_shed(stream_id, slot) {
                // Shed here: never asked for (the readers get a 404).
                gate.not_requested_shed.fetch_add(1, Ordering::Relaxed);
                slot += 1;
                continue;
            }
            let start = slot;
            while slot <= upto && missing(slot) && !gate.is_shed(stream_id, slot) {
                slot += 1;
            }
            let count = (slot - start) as u32;
            self.holes.fetch_add(u64::from(count), Ordering::Relaxed);
            gate.holes.fetch_add(u64::from(count), Ordering::Relaxed);
            if gate.admit(class, stream_id, start, count).is_err() {
                continue;
            }
            self.requests.fetch_add(1, Ordering::Relaxed);
            if parents > 1 {
                self.rechecks.fetch_add(1, Ordering::Relaxed);
                gate.second_parent.fetch_add(1, Ordering::Relaxed);
            }
            {
                let mut scan = self.scan.lock().unwrap();
                let now = Instant::now();
                for part in start..slot {
                    scan.timing(stream_id, part, now);
                }
            }
            let mesh = Arc::clone(&self.mesh);
            // Sent beside the caller: the tee goes on with its datagrams.
            tokio::spawn(async move {
                if let Err(error) = mesh
                    .request_parts_from_rank(stream_id, start as usize, count, rank)
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
            fill_us: 0,
            asked: std::collections::VecDeque::new(),
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
            fill_us: 0,
            asked: std::collections::VecDeque::new(),
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
            fill_us: 0,
            asked: std::collections::VecDeque::new(),
        };
        let now = Instant::now();
        assert!(scan.counts(7, Arrival::CacheMesh, now), "never fed fast");
        assert!(scan.counts(7, Arrival::FastPlane, now));
        assert!(!scan.counts(7, Arrival::CacheMesh, now + Duration::from_millis(10)));
        assert!(scan.counts(7, Arrival::CacheMesh, now + FAST_PLANE_QUIET));
        assert!(scan.counts(8, Arrival::CacheMesh, now));
    }

    /// A stream the fast plane fed and then left quiet is asked of the
    /// cache-mesh parents beyond its head after 150 ms (four parts at the
    /// least), every 50 ms (two parts) while it stays quiet, and no more
    /// once it has had no part from anywhere for 5 s.
    #[test]
    fn a_quiet_stream_is_asked_beyond_its_head() {
        let mut scan = HoleScan {
            distance_us: 15_000,
            streams: HashMap::new(),
            fill_us: 0,
            asked: std::collections::VecDeque::new(),
        };
        let start = Instant::now();
        assert!(scan.counts(1, Arrival::FastPlane, start));
        scan.due(1, 500, Some(41_667));
        let at = |ms| start + Duration::from_millis(ms);
        assert!(scan.quiet_due(at(100)).is_empty(), "four 42 ms parts not yet");
        assert_eq!(scan.quiet_due(at(170)), vec![(1, 501, 0)]);
        assert!(scan.quiet_due(at(200)).is_empty(), "two parts between asks");
        assert_eq!(scan.quiet_due(at(255)), vec![(1, 501, 1)]);
        // The cache mesh brings parts: the next ask starts beyond them.
        assert!(scan.counts(1, Arrival::CacheMesh, at(260)));
        scan.due(1, 506, None);
        assert_eq!(scan.quiet_due(at(345)), vec![(1, 507, 0)]);
        // The fast plane is back: nothing to ask.
        assert!(scan.counts(1, Arrival::FastPlane, at(350)));
        scan.due(1, 507, None);
        assert!(scan.quiet_due(at(400)).is_empty());
        // A stream that ended: asked for 5 s after its last part, then not.
        assert!(!scan.quiet_due(at(600)).is_empty());
        assert!(scan.quiet_due(Instant::now() + Duration::from_secs(6)).is_empty());
        // A stream never fed by the fast plane is not asked.
        scan.due(2, 10, Some(1_000));
        assert!(scan.quiet_due(at(10_000)).iter().all(|(stream, _, _)| *stream != 2));
    }

    /// send240: the second look (the other parent) waits for twice the
    /// measured time a parent takes to answer, when that is longer than the
    /// distance.
    #[test]
    fn the_second_parent_is_asked_after_twice_the_measured_answer() {
        let mut scan = HoleScan {
            distance_us: 15_000,
            streams: HashMap::new(),
            fill_us: 0,
            asked: std::collections::VecDeque::new(),
        };
        let now = Instant::now();
        // An ask of part 3 answered 40 ms later: 80 ms before the second.
        scan.timing(7, 3, now);
        assert_eq!(scan.answered(7, 3, now + Duration::from_millis(40)), Some(40_000));
        assert_eq!(scan.answered(7, 3, now), None, "answered once");
        // 1 ms parts: the first look 15 parts back, the second 15 + 80.
        assert_eq!(scan.due(7, 115, Some(1_000)), [Some((100, 100)), Some((20, 20))]);
        // 42 ms parts: 2 parts of distance, the second look 2 + 2 back.
        assert_eq!(scan.due(9, 50, Some(41_667)), [Some((48, 48)), Some((46, 46))]);
    }

    /// send240: a new session (the part numbers start again far behind the
    /// head) starts the quiet catch-up from the new session's head, never
    /// from the old session's.
    #[test]
    fn a_new_session_starts_the_quiet_catch_up_from_its_own_head() {
        let mut scan = HoleScan {
            distance_us: 15_000,
            streams: HashMap::new(),
            fill_us: 0,
            asked: std::collections::VecDeque::new(),
        };
        let start = Instant::now();
        let at = |ms| start + Duration::from_millis(ms);
        assert!(scan.counts(1, Arrival::FastPlane, start));
        scan.due(1, 2_800, Some(41_667));
        assert_eq!(scan.quiet_due(at(200)), vec![(1, 2_801, 0)]);
        // The new session's first parts: the head is its own.
        assert!(scan.counts(1, Arrival::FastPlane, at(300)));
        scan.due(1, 3, None);
        let asked = scan.quiet_due(at(700));
        assert_eq!(asked, vec![(1, 4, 0)], "from the new head, the first parent");
        assert!(asked.iter().all(|(_, from, _)| *from < 2_800));
    }
}
