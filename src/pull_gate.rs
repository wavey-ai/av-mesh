//! What an edge may pull over the cache mesh (send240, C).
//!
//! cmpd8 (25 Nov): each part an edge's own video tee shed for want of
//! processor time became a hole that the edge pulled whole from both of its
//! parents, 175 to 330 Mbit/s over the cache mesh, 23 to 49% of a CPU plus
//! softirq on an edge that was already out of CPU; at each session start the
//! quiet sweep asked both parents for 64 parts every 50 ms. The pulls could
//! not help: the fast lane had forwarded those symbols before the tee, and a
//! node short of processor time is not helped by more work.
//!
//! The gate, one per node, read by every path that pulls:
//! - **Shed parts.** A part this node shed itself (the class thread's
//!   whole-part shed, a tee copy refused) is recorded here, lock-free, by
//!   the class thread. It is never asked of a parent, and a reader asking
//!   for it is answered "not here" at once (the cache's 404).
//! - **A lagging lane.** While a class's tee lags (behind its thread by
//!   more than [`LAG_STOP_US`]) or sheds, and for [`LANE_HOLD`] after, no
//!   hole of a stream of that class is pulled.
//! - **A budget.** Pulls of every stream share one budget of bytes a second
//!   (`--edge-pull-budget-mbit`), each request charged the parts' estimated
//!   bytes; a large part takes no budget below a reserve that small parts
//!   (at most [`SMALL_PART_BYTES`]) keep, so a 1 ms stream's holes are never
//!   starved by a heavy stream's.

use crate::fast_plane::TrafficClass;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Shed parts remembered (a direct-mapped table of fingerprints: an old
/// entry gives way to a new one; 64 KiB).
const SHED_SLOTS: usize = 8_192;
/// A tee behind its class thread by more than this lags.
pub(crate) const LAG_STOP_US: u64 = 250_000;
/// A lane that lagged or shed stays without pulls this long.
pub(crate) const LANE_HOLD: Duration = Duration::from_secs(1);
/// A part this small or smaller is a small part (a 1 ms audio part is a
/// few hundred bytes; a 4K part is 1 to 6 MB).
pub(crate) const SMALL_PART_BYTES: u64 = 64 * 1_024;
/// The share of the budget's burst that large parts leave for small ones.
const SMALL_RESERVE_PERMILLE: u64 = 250;
/// Default of `--edge-pull-budget-mbit`: 48 Mbit/s, a fifth of one 4K
/// stream at 247 Mbit/s (four or five 4K parts a second, every hole of
/// thousands of audio streams).
pub(crate) const DEFAULT_PULL_BUDGET_MBIT: u64 = 48;
/// Streams whose part size the gate keeps.
const STREAMS: usize = 256;

fn fingerprint(stream_id: u64, part: u64) -> u64 {
    // splitmix64 of the pair; 0 is an empty slot.
    let mut value = stream_id
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .rotate_left(29)
        ^ part;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (value ^ (value >> 31)) | 1
}

/// The budget's bucket: bytes it may spend now, refilled at its rate.
#[derive(Debug)]
struct Bucket {
    tokens: f64,
    at: Instant,
}

/// Why a pull was not made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refused {
    /// The part was shed here.
    Shed,
    /// Its lane's tee lags or sheds.
    Lagging,
    /// The budget is spent.
    Budget,
}

/// `/api/node`'s `pulls`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PullSnapshot {
    /// Holes the edge found (parts missing behind the distance), asks made
    /// (to one parent each) and their parts, and the asks of a second
    /// parent (the first did not answer in time).
    pub holes: u64,
    pub requests: u64,
    pub requested_parts: u64,
    pub second_parent: u64,
    /// Parts not asked for: shed here, their lane lagging, the budget
    /// spent.
    pub not_requested_shed: u64,
    pub not_requested_lagging: u64,
    pub not_requested_budget: u64,
    /// Parts recorded as shed by the class threads.
    pub shed_recorded: u64,
    /// Reader requests answered "not here" at once for a shed part.
    pub shed_answered: u64,
    /// The budget (bits a second, 0: none), the bytes charged to it, and
    /// over the last whole second.
    pub budget_bps: u64,
    pub pulled_bytes: u64,
    pub pulled_bps_1s: u64,
    /// The quiet sweep's asks (one parent, a few parts).
    pub quiet_requests: u64,
    /// The measured time from an ask to its part (µs, smoothed): the wait
    /// before the second parent is asked.
    pub fill_us: u64,
}

/// The node's gate on pulls.
#[derive(Debug)]
pub(crate) struct PullGate {
    shed: Box<[AtomicU64]>,
    /// CLOCK_MONOTONIC ns until which each class's lane takes no pull.
    lane_hold_ns: [AtomicU64; 2],
    budget_bps: AtomicU64,
    bucket: Mutex<Bucket>,
    /// Each stream's part size (bytes, smoothed): (stream id, bytes).
    part_bytes: Box<[(AtomicU64, AtomicU64)]>,
    pulled_second: Mutex<(u64, u64, u64)>,
    pub(crate) holes: AtomicU64,
    pub(crate) requests: AtomicU64,
    pub(crate) requested_parts: AtomicU64,
    pub(crate) second_parent: AtomicU64,
    pub(crate) not_requested_shed: AtomicU64,
    pub(crate) not_requested_lagging: AtomicU64,
    pub(crate) not_requested_budget: AtomicU64,
    pub(crate) shed_recorded: AtomicU64,
    pub(crate) shed_answered: AtomicU64,
    pub(crate) pulled_bytes: AtomicU64,
    pub(crate) quiet_requests: AtomicU64,
    pub(crate) fill_us: AtomicU64,
    /// Tests: the gate lets every pull through and remembers no shed part
    /// (the edge as it was before send240, for a measurement's baseline).
    #[cfg(test)]
    pub(crate) bypass: std::sync::atomic::AtomicBool,
}

impl Default for PullGate {
    fn default() -> Self {
        let mut shed: Box<[AtomicU64]> = (0..SHED_SLOTS).map(|_| AtomicU64::new(0)).collect();
        // The class threads write it: its pages are faulted in here.
        crate::memory::touch_owned(&mut *shed);
        Self {
            shed,
            lane_hold_ns: [AtomicU64::new(0), AtomicU64::new(0)],
            budget_bps: AtomicU64::new(DEFAULT_PULL_BUDGET_MBIT * 1_000_000),
            bucket: Mutex::new(Bucket {
                tokens: (DEFAULT_PULL_BUDGET_MBIT * 1_000_000 / 8) as f64,
                at: Instant::now(),
            }),
            part_bytes: (0..STREAMS)
                .map(|_| (AtomicU64::new(0), AtomicU64::new(0)))
                .collect(),
            pulled_second: Mutex::new((0, 0, 0)),
            holes: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            requested_parts: AtomicU64::new(0),
            second_parent: AtomicU64::new(0),
            not_requested_shed: AtomicU64::new(0),
            not_requested_lagging: AtomicU64::new(0),
            not_requested_budget: AtomicU64::new(0),
            shed_recorded: AtomicU64::new(0),
            shed_answered: AtomicU64::new(0),
            pulled_bytes: AtomicU64::new(0),
            quiet_requests: AtomicU64::new(0),
            fill_us: AtomicU64::new(0),
            #[cfg(test)]
            bypass: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl PullGate {
    /// `--edge-pull-budget-mbit` (0: no budget).
    pub(crate) fn set_budget_mbit(&self, mbit: u64) {
        self.budget_bps
            .store(mbit.saturating_mul(1_000_000), Ordering::Relaxed);
        let mut bucket = self.bucket.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        bucket.tokens = self.burst_bytes() as f64;
        bucket.at = Instant::now();
    }

    fn burst_bytes(&self) -> u64 {
        // A second of the budget.
        self.budget_bps.load(Ordering::Relaxed) / 8
    }

    /// The class thread shed part `part` of `stream_id` (or its tee copy
    /// was refused): never pulled, and its lane takes no pull for a while.
    /// Lock-free and allocation-free.
    #[inline]
    pub(crate) fn record_shed(&self, class: TrafficClass, stream_id: u64, part: u64) {
        let print = fingerprint(stream_id, part);
        let slot = &self.shed[(print % SHED_SLOTS as u64) as usize];
        if slot.swap(print, Ordering::Relaxed) != print {
            self.shed_recorded.fetch_add(1, Ordering::Relaxed);
        }
        self.hold_lane(class);
    }

    pub(crate) fn is_shed(&self, stream_id: u64, part: u64) -> bool {
        #[cfg(test)]
        if self.bypass.load(Ordering::Relaxed) {
            return false;
        }
        let print = fingerprint(stream_id, part);
        self.shed[(print % SHED_SLOTS as u64) as usize].load(Ordering::Relaxed) == print
    }

    fn hold_lane(&self, class: TrafficClass) {
        let until = crate::fast_plane::sys::monotonic_ns()
            .saturating_add(LANE_HOLD.as_nanos() as u64);
        self.lane_hold_ns[class.index()].fetch_max(until, Ordering::Relaxed);
    }

    /// A tee took a datagram `lag_us` after its class thread: a lane whose
    /// tee lags takes no pull.
    #[inline]
    pub(crate) fn note_tee_lag(&self, class: TrafficClass, lag_us: u64) {
        if lag_us > LAG_STOP_US {
            self.hold_lane(class);
        }
    }

    /// Whether `class`'s lane takes no pull now.
    pub(crate) fn lane_lagging(&self, class: TrafficClass) -> bool {
        self.lane_hold_ns[class.index()].load(Ordering::Relaxed)
            > crate::fast_plane::sys::monotonic_ns()
    }

    /// A part of `stream_id` of `bytes` arrived: the stream's part size.
    pub(crate) fn observe_part_bytes(&self, stream_id: u64, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let entry = &self.part_bytes[(stream_id % STREAMS as u64) as usize];
        if entry.0.swap(stream_id, Ordering::Relaxed) != stream_id {
            entry.1.store(bytes, Ordering::Relaxed);
            return;
        }
        let old = entry.1.load(Ordering::Relaxed);
        entry
            .1
            .store(if old == 0 { bytes } else { (old * 7 + bytes) / 8 }, Ordering::Relaxed);
    }

    /// A stream's part size as last seen (0: unknown).
    pub(crate) fn part_bytes(&self, stream_id: u64) -> u64 {
        let entry = &self.part_bytes[(stream_id % STREAMS as u64) as usize];
        if entry.0.load(Ordering::Relaxed) == stream_id {
            entry.1.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    /// Whether `count` parts of `stream_id` from `from` may be pulled now:
    /// the shed parts are left out (`Some(parts)` asks for the run of the
    /// rest that starts at `from`), a lagging lane and a spent budget
    /// refuse. Charges the budget on success.
    pub(crate) fn admit(
        &self,
        class: TrafficClass,
        stream_id: u64,
        part: u64,
        count: u32,
    ) -> Result<(), Refused> {
        #[cfg(test)]
        if self.bypass.load(Ordering::Relaxed) {
            self.requests.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if (part..part + u64::from(count)).any(|part| self.is_shed(stream_id, part)) {
            self.not_requested_shed
                .fetch_add(u64::from(count), Ordering::Relaxed);
            return Err(Refused::Shed);
        }
        if self.lane_lagging(class) {
            self.not_requested_lagging
                .fetch_add(u64::from(count), Ordering::Relaxed);
            return Err(Refused::Lagging);
        }
        let part_bytes = self.part_bytes(stream_id);
        let bytes = part_bytes.saturating_mul(u64::from(count));
        if !self.take(bytes, part_bytes <= SMALL_PART_BYTES) {
            self.not_requested_budget
                .fetch_add(u64::from(count), Ordering::Relaxed);
            return Err(Refused::Budget);
        }
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.requested_parts
            .fetch_add(u64::from(count), Ordering::Relaxed);
        self.book_pulled(bytes);
        Ok(())
    }

    /// Charge `bytes` to the budget: a small part may take it to empty, a
    /// large one only down to the reserve small parts keep.
    fn take(&self, bytes: u64, small: bool) -> bool {
        let rate = self.budget_bps.load(Ordering::Relaxed) / 8;
        if rate == 0 {
            return true;
        }
        let burst = rate as f64;
        let mut bucket = self.bucket.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        bucket.tokens =
            (bucket.tokens + now.duration_since(bucket.at).as_secs_f64() * rate as f64).min(burst);
        bucket.at = now;
        let floor = if small {
            0.0
        } else {
            burst * SMALL_RESERVE_PERMILLE as f64 / 1_000.0
        };
        // A part larger than all of the budget above the floor is taken
        // when the bucket is full (it may borrow the next second).
        let fits = bucket.tokens - bytes as f64 >= floor
            || (!small && bucket.tokens >= burst - 1.0 && bytes as f64 > burst - floor);
        if fits {
            bucket.tokens -= bytes as f64;
        }
        fits
    }

    fn book_pulled(&self, bytes: u64) {
        self.pulled_bytes.fetch_add(bytes, Ordering::Relaxed);
        let second = crate::now_unix_us() / 1_000_000;
        let mut pulled = self
            .pulled_second
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pulled.0 != second {
            *pulled = (second, if pulled.0 + 1 == second { pulled.1 } else { 0 }, 0);
        }
        pulled.2 += bytes;
    }

    pub(crate) fn snapshot(&self) -> PullSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let pulled = *self
            .pulled_second
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let second = crate::now_unix_us() / 1_000_000;
        PullSnapshot {
            holes: load(&self.holes),
            requests: load(&self.requests),
            requested_parts: load(&self.requested_parts),
            second_parent: load(&self.second_parent),
            not_requested_shed: load(&self.not_requested_shed),
            not_requested_lagging: load(&self.not_requested_lagging),
            not_requested_budget: load(&self.not_requested_budget),
            shed_recorded: load(&self.shed_recorded),
            shed_answered: load(&self.shed_answered),
            budget_bps: load(&self.budget_bps),
            pulled_bytes: load(&self.pulled_bytes),
            // The last whole second's bytes, in bits a second.
            pulled_bps_1s: if pulled.0 == second {
                pulled.1 * 8
            } else if pulled.0 + 1 == second {
                pulled.2 * 8
            } else {
                0
            },
            quiet_requests: load(&self.quiet_requests),
            fill_us: load(&self.fill_us),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_shed_part_is_remembered_and_never_admitted() {
        let gate = PullGate::default();
        gate.set_budget_mbit(0);
        assert!(!gate.is_shed(7, 100));
        gate.record_shed(TrafficClass::Video, 7, 100);
        assert!(gate.is_shed(7, 100));
        assert!(!gate.is_shed(7, 101) && !gate.is_shed(8, 100));
        // The video lane takes no pull for a while; audio still does.
        assert_eq!(gate.admit(TrafficClass::Video, 7, 100, 1), Err(Refused::Shed));
        assert_eq!(gate.admit(TrafficClass::Video, 7, 101, 1), Err(Refused::Lagging));
        assert_eq!(gate.admit(TrafficClass::Audio, 9, 101, 1), Ok(()));
        let snapshot = gate.snapshot();
        assert_eq!(
            (snapshot.shed_recorded, snapshot.not_requested_shed, snapshot.not_requested_lagging),
            (1, 1, 1)
        );
    }

    #[test]
    fn a_lagging_tee_stops_its_lane_s_pulls() {
        let gate = PullGate::default();
        gate.note_tee_lag(TrafficClass::Video, LAG_STOP_US);
        assert!(!gate.lane_lagging(TrafficClass::Video));
        gate.note_tee_lag(TrafficClass::Video, LAG_STOP_US + 1);
        assert!(gate.lane_lagging(TrafficClass::Video));
        assert!(!gate.lane_lagging(TrafficClass::Audio));
    }

    /// Heavy parts spend the budget down to the reserve; small parts still
    /// pass; the budget refills at its rate.
    #[test]
    fn heavy_parts_leave_the_budget_s_reserve_to_small_parts() {
        let gate = PullGate::default();
        gate.set_budget_mbit(8); // 1 MB a second
        gate.observe_part_bytes(1, 300_000);
        gate.observe_part_bytes(2, 600);
        // Two 300 kB parts fit above the 250 kB reserve, the third does not.
        assert_eq!(gate.admit(TrafficClass::Video, 1, 10, 1), Ok(()));
        assert_eq!(gate.admit(TrafficClass::Video, 1, 11, 1), Ok(()));
        assert_eq!(gate.admit(TrafficClass::Video, 1, 12, 1), Err(Refused::Budget));
        // Small parts take the reserve.
        for part in 0..100 {
            assert_eq!(gate.admit(TrafficClass::Audio, 2, part, 1), Ok(()));
        }
        assert_eq!(gate.snapshot().not_requested_budget, 1);
        std::thread::sleep(Duration::from_millis(350));
        assert_eq!(gate.admit(TrafficClass::Video, 1, 12, 1), Ok(()));
        // No budget: everything not shed and not lagging passes.
        gate.set_budget_mbit(0);
        for part in 20..40 {
            assert_eq!(gate.admit(TrafficClass::Video, 1, part, 1), Ok(()));
        }
    }
}
