//! Media clocks: how far each stream's own timeline has come at this node.
//!
//! A part's deadline is a point on its stream's timeline: the instant its
//! source sent the part's last sample, plus the contributor's budget. No
//! node compares it with a wall clock. A source whose sample clock drifts,
//! a DAW that pauses and a stream that arrives late as a whole are all on
//! time on their own timeline, and two contributors that publish the same
//! parts stamp the same deadlines whatever their own clocks say.
//!
//! A node keeps, per stream, the head of that timeline as it has seen it,
//! and sheds a datagram whose deadline is a budget or more behind the head
//! (`StaleMedia`): media that old comes from a sender that was frozen and
//! has resumed. A datagram of an epoch below the stream's belongs to a
//! session that has ended (`StaleEpoch`).
//!
//! Only validated data names a stream's epoch: an object of it that decoded
//! here (`set_epoch`), or the epoch that the node's cache took up
//! (`follow`, from the table the commit writes). A datagram's own claim of
//! a higher epoch changes nothing, and one deadline moves a head by half a
//! budget at most, so a corrupt datagram that passes the UDP checksum
//! cannot retire a stream.
//!
//! A head that starts again (a new clock's first epoch, a new epoch) takes
//! its first deadline whole, but only provisionally: it judges nothing
//! until a later deadline within a budget of it confirms it, and a deadline
//! more than a budget away, either way, takes its place. One wild deadline
//! in that window (the far future, or the year 1970) costs the next
//! datagram its judgement and no more; a confirmed head moves by half a
//! budget a datagram as before.
//!
//! The table is allocated once and never grows: a stream's clock is in one
//! of eight neighbouring slots. A stream that finds none has no clock, is
//! always fresh, and is counted. A clock that has judged nothing fresh for
//! five minutes is idle: its slot may go to another stream, and it no
//! longer calls a lower epoch stale (the node's cache forgets a stream
//! after the same five minutes, and a source that then returns with a
//! lower session id must reach the decode to be taken up again).

use std::sync::atomic::{AtomicU64, Ordering};
use xxhash_rust::xxh3::xxh3_64;

/// The neighbouring slots that a stream's clock may be in.
const PROBE: usize = 8;

/// The longest tenant, stream or track id that an RLS1 datagram may carry
/// (relay-session's hard ceiling, which `RlsView::parse` admits): the room
/// that the class threads' fixed tables give an id, so that no valid id
/// grows a table on a real-time thread.
pub const ID_ROOM_BYTES: usize = relay_session::RelayLimits::HARD.max_identifier_bytes;

/// A clock that has judged nothing fresh for this long is idle.
pub const IDLE_US: u64 = 5 * 60 * 1_000_000;

/// What a stream's clock says of a datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// On time on its stream's timeline, or not judged (no deadline, no
    /// validated epoch yet, no clock).
    Fresh,
    /// Of an epoch below the stream's validated one.
    StaleEpoch,
    /// A budget or more behind the head of its stream.
    StaleMedia,
}

/// The key of a stream's clock: a hash of the tenant and the stream, and
/// the stream's name to tell two streams of one hash apart.
#[derive(Debug, Clone, Copy)]
pub struct ClockKey<'a> {
    hash: u64,
    stream: &'a str,
}

#[derive(Debug, Default)]
struct Clock {
    /// Zero: the slot has never held a clock.
    hash: u64,
    stream: String,
    /// The stream's numeric id, as the node's caches key it.
    stream_id: Option<u64>,
    /// Zero: no epoch of the stream is validated yet.
    epoch: u64,
    /// Zero: the next deadline of the epoch is taken whole.
    head_us: u64,
    /// The head was taken whole from one deadline and no later deadline
    /// within a budget of it has confirmed it yet: it judges nothing.
    provisional: bool,
    /// When the clock last judged a datagram fresh by its own rules.
    fresh_us: u64,
    /// The change of the stream's validated epoch that `follow` took last.
    followed: u64,
}


/// A fixed-size table of media clocks, owned by one thread.
pub struct MediaClocks {
    clocks: Box<[Clock]>,
    mask: usize,
    now_us: u64,
    overflow: u64,
    created: u64,
}

impl MediaClocks {
    /// A table of `capacity` clocks (rounded up to a power of two).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let slots = capacity.max(PROBE).next_power_of_two();
        Self {
            // Each name's room made and every byte of it touched now: a new
            // stream's clock takes its name (any id the wire admits) without
            // an allocation or a first touch on the class thread (stall
            // follow-up).
            clocks: (0..slots)
                .map(|_| {
                    let mut stream = String::with_capacity(ID_ROOM_BYTES);
                    stream.extend(std::iter::repeat_n('\0', ID_ROOM_BYTES));
                    stream.clear();
                    Clock {
                        stream,
                        ..Clock::default()
                    }
                })
                .collect(),
            mask: slots - 1,
            now_us: 0,
            overflow: 0,
            created: 0,
        }
    }

    /// The key of the clock of `stream` of `tenant`.
    #[must_use]
    pub fn key<'a>(tenant: &str, stream: &'a str) -> ClockKey<'a> {
        ClockKey {
            // Zero marks an empty slot.
            hash: (xxh3_64(stream.as_bytes()) ^ xxh3_64(tenant.as_bytes()).rotate_left(32)) | 1,
            stream,
        }
    }

    /// The local time, by which a clock goes idle. Called once for a batch
    /// of datagrams; a table that is never told the time has no idle clock.
    pub fn tick(&mut self, now_us: u64) {
        self.now_us = self.now_us.max(now_us);
    }

    /// Datagrams of streams that found no slot for a clock.
    #[must_use]
    pub const fn overflow(&self) -> u64 {
        self.overflow
    }

    /// Clocks made since the table was: a change means that a stream is new
    /// here and has yet to `follow` its validated epoch.
    #[must_use]
    pub const fn created(&self) -> u64 {
        self.created
    }

    fn idle(&self, clock: &Clock) -> bool {
        self.now_us.saturating_sub(clock.fresh_us) >= IDLE_US
    }

    fn slot(&self, key: ClockKey<'_>, step: usize) -> usize {
        ((key.hash >> 8) as usize).wrapping_add(step) & self.mask
    }

    /// The slot of the stream's clock, if it has one.
    fn find(&self, key: ClockKey<'_>) -> Option<usize> {
        (0..PROBE).map(|step| self.slot(key, step)).find(|index| {
            let clock = &self.clocks[*index];
            clock.hash == key.hash && clock.stream == key.stream
        })
    }

    /// The slot of the stream's clock, made in a slot that was never used or
    /// whose clock is idle when the stream has none. `None`: no room.
    fn find_or_make(&mut self, key: ClockKey<'_>) -> Option<usize> {
        let mut free = None;
        for step in 0..PROBE {
            let index = self.slot(key, step);
            let clock = &self.clocks[index];
            if clock.hash == key.hash && clock.stream == key.stream {
                return Some(index);
            }
            if free.is_none() && (clock.hash == 0 || self.idle(clock)) {
                free = Some(index);
            }
        }
        let index = free?;
        let now_us = self.now_us;
        let clock = &mut self.clocks[index];
        clock.hash = key.hash;
        clock.stream.clear();
        clock.stream.push_str(key.stream);
        clock.stream_id = key.stream.parse().ok();
        clock.epoch = 0;
        clock.head_us = 0;
        clock.provisional = false;
        clock.fresh_us = now_us;
        clock.followed = 0;
        self.created += 1;
        Some(index)
    }

    /// Judge a datagram of `epoch` whose deadline is `deadline_us` on its
    /// stream's timeline, and move the stream's head: by half of `budget_us`
    /// at most once the head is confirmed, so that one wrong deadline cannot
    /// shed the stream.
    pub fn observe(
        &mut self,
        key: ClockKey<'_>,
        epoch: u64,
        deadline_us: u64,
        budget_us: u64,
    ) -> Verdict {
        let Some(index) = self.find_or_make(key) else {
            self.overflow += 1;
            return Verdict::Fresh;
        };
        let now_us = self.now_us;
        let idle = self.idle(&self.clocks[index]);
        let clock = &mut self.clocks[index];
        if deadline_us == 0 || clock.epoch == 0 || epoch > clock.epoch {
            // A higher epoch is only a claim until an object of it decodes.
            clock.fresh_us = now_us;
            return Verdict::Fresh;
        }
        if epoch < clock.epoch {
            // An idle clock no longer knows its stream's epoch: the decode
            // behind it decides, and tells this table what it took up.
            return if idle {
                Verdict::Fresh
            } else {
                Verdict::StaleEpoch
            };
        }
        if clock.head_us == 0
            || (clock.provisional && deadline_us.abs_diff(clock.head_us) >= budget_us)
        {
            // The head starts at this deadline, provisionally: a deadline a
            // budget or more from it, either way, is the stream's or the
            // head's was wild, and the next one tells.
            clock.head_us = deadline_us;
            clock.provisional = true;
        } else {
            // A confirmed head, or one that this deadline, within a budget
            // of it, confirms: it judges.
            clock.provisional = false;
            if deadline_us.saturating_add(budget_us) <= clock.head_us {
                return Verdict::StaleMedia;
            }
            clock.head_us = clock
                .head_us
                .max(deadline_us.min(clock.head_us.saturating_add(budget_us / 2)));
        }
        clock.fresh_us = now_us;
        Verdict::Fresh
    }

    /// What `observe` would say of the datagram, with nothing moved and no
    /// clock made: whether the head has passed something held back earlier
    /// (an assembly that waits for its last symbols).
    #[must_use]
    pub fn peek(
        &self,
        key: ClockKey<'_>,
        epoch: u64,
        deadline_us: u64,
        budget_us: u64,
    ) -> Verdict {
        let Some(clock) = self.find(key).map(|index| &self.clocks[index]) else {
            return Verdict::Fresh;
        };
        if deadline_us == 0 || clock.epoch == 0 || epoch > clock.epoch {
            Verdict::Fresh
        } else if epoch < clock.epoch {
            if self.idle(clock) {
                Verdict::Fresh
            } else {
                Verdict::StaleEpoch
            }
        } else if !clock.provisional && deadline_us.saturating_add(budget_us) <= clock.head_us {
            Verdict::StaleMedia
        } else {
            Verdict::Fresh
        }
    }

    /// An object of `epoch` of the stream decoded here: a higher epoch than
    /// the clock's is taken up and the head starts again. An idle clock
    /// takes whatever epoch decoded, a lower one too.
    pub fn set_epoch(&mut self, key: ClockKey<'_>, epoch: u64) {
        let Some(index) = self.find_or_make(key) else {
            return;
        };
        let now_us = self.now_us;
        let idle = self.idle(&self.clocks[index]);
        let clock = &mut self.clocks[index];
        if epoch > clock.epoch || (idle && epoch != clock.epoch) {
            clock.epoch = epoch;
            clock.head_us = 0;
            clock.provisional = false;
        }
        clock.fresh_us = now_us;
    }

    /// Take up, for every clock of a stream with a numeric id, the epoch
    /// that the node's cache validated for it since this table last looked:
    /// that epoch is the stream's, lower than the clock's or not, and the
    /// head starts again when it changed.
    pub fn follow(&mut self, epochs: &ValidatedEpochs) {
        for clock in self.clocks.iter_mut().filter(|clock| clock.hash != 0) {
            let Some((epoch, change)) = clock.stream_id.and_then(|id| epochs.get(id)) else {
                continue;
            };
            if change == clock.followed {
                continue;
            }
            clock.followed = change;
            if epoch != clock.epoch {
                clock.epoch = epoch;
                clock.head_us = 0;
                clock.provisional = false;
            }
        }
    }
}

/// The source epoch that this node's cache has taken up for each stream,
/// written where the cache commits and read by the class threads without a
/// lock (`MediaClocks::follow`).
///
/// The table is allocated once, and a stream's epoch is in one of eight
/// neighbouring slots, so that a look costs eight loads at most. A stream
/// stays in it for the life of the process; a stream that finds none of
/// its eight slots free is not recorded (counted in `unrecorded`), its
/// clocks never get an epoch, and they judge nothing stale.
pub struct ValidatedEpochs {
    slots: Box<[EpochSlot]>,
    mask: usize,
    version: AtomicU64,
    unrecorded: AtomicU64,
}

#[derive(Default)]
struct EpochSlot {
    /// The stream id plus one; zero: empty.
    stream: AtomicU64,
    epoch: AtomicU64,
    /// How many times the stream's epoch was written.
    change: AtomicU64,
}

impl ValidatedEpochs {
    /// A table for `capacity` streams (rounded up to a power of two).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let slots = capacity.max(1).next_power_of_two();
        let mut slots: Box<[EpochSlot]> = (0..slots).map(|_| EpochSlot::default()).collect();
        // Faulted in by the maker: the class threads read the slots (a
        // zeroed allocation is not written, and a read maps its page then).
        crate::memory::touch_owned(&mut *slots);
        Self {
            mask: slots.len() - 1,
            slots,
            version: AtomicU64::new(0),
            unrecorded: AtomicU64::new(0),
        }
    }

    fn home(stream_id: u64) -> usize {
        (stream_id.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 32) as usize
    }

    /// The slots that the stream's epoch may be in.
    fn probe(&self, stream_id: u64) -> impl Iterator<Item = &EpochSlot> {
        let home = Self::home(stream_id);
        (0..PROBE.min(self.slots.len()))
            .map(move |step| &self.slots[home.wrapping_add(step) & self.mask])
    }

    /// The cache took up `epoch` for the stream (again, after it forgot the
    /// stream, counts as well: the clocks start the stream's head anew).
    pub fn set(&self, stream_id: u64, epoch: u64) {
        let Some(tag) = stream_id.checked_add(1) else {
            return;
        };
        for slot in self.probe(stream_id) {
            let holder = match slot
                .stream
                .compare_exchange(0, tag, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => tag,
                Err(holder) => holder,
            };
            if holder == tag {
                slot.epoch.store(epoch, Ordering::Release);
                slot.change.fetch_add(1, Ordering::Release);
                // Last: a reader that saw the version before this write
                // looks again.
                self.version.fetch_add(1, Ordering::Release);
                return;
            }
        }
        self.unrecorded.fetch_add(1, Ordering::Relaxed);
    }

    /// The stream's validated epoch and how many times it was written.
    #[must_use]
    pub fn get(&self, stream_id: u64) -> Option<(u64, u64)> {
        let tag = stream_id.checked_add(1)?;
        for slot in self.probe(stream_id) {
            match slot.stream.load(Ordering::Acquire) {
                0 => return None,
                holder if holder == tag => {
                    // The change before the epoch: a write between the two
                    // reads leaves a newer epoch under an older change, and
                    // the reader takes the change again later.
                    let change = slot.change.load(Ordering::Acquire);
                    return Some((slot.epoch.load(Ordering::Acquire), change));
                }
                _ => {}
            }
        }
        None
    }

    /// Changes with every `set`: a reader whose last look saw this value
    /// has seen every epoch.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Epochs taken up for a stream that found no slot: its clocks judge
    /// nothing stale.
    #[must_use]
    pub fn unrecorded(&self) -> u64 {
        self.unrecorded.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUDGET: u64 = 1_000_000;

    /// The longest id the wire admits takes a clock in the room made for
    /// it: no allocation on the class thread (D3 review 2).
    #[test]
    fn a_clock_takes_the_longest_admitted_name_in_its_room() {
        let mut clocks = MediaClocks::new(8);
        let rooms: Vec<*const u8> = clocks.clocks.iter().map(|clock| clock.stream.as_ptr()).collect();
        let name = "s".repeat(ID_ROOM_BYTES);
        let index = clocks.find_or_make(MediaClocks::key("t", &name)).unwrap();
        assert_eq!(clocks.clocks[index].stream, name);
        assert_eq!(clocks.clocks[index].stream.as_ptr(), rooms[index]);
    }
    const EPOCH: u64 = 1_763_924_000_001_000;
    /// The deadline of the first part of the test stream.
    const START: u64 = 1_763_924_001_000_000;

    fn stream(name: &str) -> ClockKey<'_> {
        MediaClocks::key("needletail", name)
    }

    /// A table whose stream 3001 has a validated epoch and a confirmed head
    /// at START.
    fn running() -> MediaClocks {
        let mut clocks = MediaClocks::new(1_024);
        let key = stream("3001");
        assert_eq!(clocks.observe(key, EPOCH, START, BUDGET), Verdict::Fresh);
        clocks.set_epoch(key, EPOCH);
        // The first deadline of the epoch starts the head, the next one
        // confirms it.
        assert_eq!(clocks.observe(key, EPOCH, START, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.observe(key, EPOCH, START, BUDGET), Verdict::Fresh);
        clocks
    }

    #[test]
    fn nothing_is_stale_before_an_epoch_is_validated() {
        let mut clocks = MediaClocks::new(1_024);
        let key = stream("3001");
        // A node that starts mid-stream, and deadlines in any order.
        for deadline in [START, START - 3_600_000_000, 1, START + 86_400_000_000] {
            assert_eq!(clocks.observe(key, EPOCH, deadline, BUDGET), Verdict::Fresh);
            assert_eq!(clocks.observe(key, EPOCH - 5, deadline, BUDGET), Verdict::Fresh);
        }
        // A datagram without a deadline is never judged.
        clocks.set_epoch(key, EPOCH);
        assert_eq!(clocks.observe(key, EPOCH, START, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.observe(key, EPOCH - 5, 0, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.overflow(), 0);
    }

    #[test]
    fn a_part_a_budget_behind_the_head_is_stale_media() {
        let mut clocks = running();
        let key = stream("3001");
        for part in 1..=2_000_u64 {
            assert_eq!(
                clocks.observe(key, EPOCH, START + part * 1_000, BUDGET),
                Verdict::Fresh
            );
        }
        let head = START + 2_000_000;
        // Late, inside the budget: a repair copy, a burst copy, a parent
        // that is a little behind.
        assert_eq!(
            clocks.observe(key, EPOCH, head - BUDGET + 1, BUDGET),
            Verdict::Fresh
        );
        assert_eq!(clocks.peek(key, EPOCH, head - BUDGET + 1, BUDGET), Verdict::Fresh);
        // A budget behind: a sender that was frozen and resumes.
        assert_eq!(
            clocks.observe(key, EPOCH, head - BUDGET, BUDGET),
            Verdict::StaleMedia
        );
        assert_eq!(
            clocks.peek(key, EPOCH, head - BUDGET, BUDGET),
            Verdict::StaleMedia
        );
        assert_eq!(
            clocks.observe(key, EPOCH, START, BUDGET),
            Verdict::StaleMedia
        );
        // The stale burst moved nothing.
        assert_eq!(
            clocks.observe(key, EPOCH, head - BUDGET + 1, BUDGET),
            Verdict::Fresh
        );
    }

    #[test]
    fn a_source_clock_500_ppm_off_for_a_day_expires_nothing() {
        // 24 hours of 1 ms parts from a source whose sample clock runs
        // 500 ppm fast: 43.2 s ahead of this node's clock by the end. Then
        // 24 hours of 5 ms parts from one that runs as slow.
        for (part_us, ppm) in [(1_000_u64, 1_000_500_u64), (5_000, 999_500)] {
            let mut clocks = running();
            let key = stream("3001");
            let parts = 86_400_000_000 / part_us;
            for part in 1..=parts {
                clocks.tick(START + part * part_us);
                let deadline = START + part * part_us * ppm / 1_000_000;
                assert_eq!(
                    clocks.observe(key, EPOCH, deadline, BUDGET),
                    Verdict::Fresh,
                    "part {part}"
                );
            }
            // The node's clock and the stream's are 43.2 s apart now; the
            // stream's own late copies are still judged on its timeline.
            let head = START + parts * part_us * ppm / 1_000_000;
            assert_eq!(clocks.observe(key, EPOCH, head - 999_000, BUDGET), Verdict::Fresh);
            assert_eq!(
                clocks.observe(key, EPOCH, head - BUDGET, BUDGET),
                Verdict::StaleMedia
            );
        }
    }

    #[test]
    fn a_pause_of_ten_seconds_expires_nothing() {
        // A DAW that pauses: its sample clock stops, and the deadlines go
        // on where they were while 10 s pass here.
        let mut clocks = running();
        let key = stream("3001");
        clocks.tick(START);
        for part in 1..=100_u64 {
            assert_eq!(
                clocks.observe(key, EPOCH, START + part * 1_000, BUDGET),
                Verdict::Fresh
            );
        }
        clocks.tick(START + 10_000_000);
        for part in 101..=200_u64 {
            assert_eq!(
                clocks.observe(key, EPOCH, START + part * 1_000, BUDGET),
                Verdict::Fresh
            );
        }
        // A source that kept its clock running through an outage of 10 s:
        // the deadlines go on 10 s later.
        for part in 201..=300_u64 {
            assert_eq!(
                clocks.observe(key, EPOCH, START + 10_000_000 + part * 1_000, BUDGET),
                Verdict::Fresh
            );
        }
    }

    #[test]
    fn a_jump_forward_moves_the_head_half_a_budget_a_datagram() {
        let mut clocks = running();
        let key = stream("3001");
        let jump = START + 3_600_000_000;
        // The first parts after the jump are fresh, and so is a late copy
        // from before it: the head has moved half a budget.
        assert_eq!(clocks.observe(key, EPOCH, jump, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.observe(key, EPOCH, START - 400_000, BUDGET), Verdict::Fresh);
        assert_eq!(
            clocks.observe(key, EPOCH, START - 500_000, BUDGET),
            Verdict::StaleMedia
        );
        // The head reaches the new place in steps of half a budget: an hour
        // in some 7,200 datagrams, every one of them fresh.
        for part in 1..=8_000_u64 {
            assert_eq!(
                clocks.observe(key, EPOCH, jump + part * 1_000, BUDGET),
                Verdict::Fresh
            );
        }
        assert_eq!(
            clocks.observe(key, EPOCH, jump + 8_000_000 - BUDGET + 1, BUDGET),
            Verdict::Fresh
        );
        assert_eq!(
            clocks.observe(key, EPOCH, jump + 8_000_000 - BUDGET, BUDGET),
            Verdict::StaleMedia
        );
    }

    #[test]
    fn one_corrupt_deadline_sheds_nothing() {
        for corrupt in [u64::MAX, START + 86_400_000_000, START + BUDGET] {
            let mut clocks = running();
            let key = stream("3001");
            for part in 1..=10_u64 {
                clocks.observe(key, EPOCH, START + part * 1_000, BUDGET);
            }
            assert_eq!(clocks.observe(key, EPOCH, corrupt, BUDGET), Verdict::Fresh);
            // The head moved half a budget, whatever the deadline said: a
            // copy of the last part that is less than half a budget late is
            // still fresh, and that is all the datagram cost.
            let last = START + 10_000;
            assert_eq!(clocks.observe(key, EPOCH, last - 499_999, BUDGET), Verdict::Fresh);
            assert_eq!(
                clocks.observe(key, EPOCH, last - 500_000, BUDGET),
                Verdict::StaleMedia
            );
            // The stream goes on.
            for part in 11..=2_000_u64 {
                assert_eq!(
                    clocks.observe(key, EPOCH, START + part * 1_000, BUDGET),
                    Verdict::Fresh,
                    "part {part} after the deadline {corrupt}"
                );
            }
            assert_eq!(
                clocks.observe(key, EPOCH, START + 2_000_000 - BUDGET, BUDGET),
                Verdict::StaleMedia
            );
        }
        // A corrupt deadline far behind is shed, alone.
        let mut clocks = running();
        let key = stream("3001");
        assert_eq!(clocks.observe(key, EPOCH, 1, BUDGET), Verdict::StaleMedia);
        assert_eq!(clocks.observe(key, EPOCH, START + 1_000, BUDGET), Verdict::Fresh);
    }

    /// The review of AM1 (finding 1): where a head starts again, a head
    /// taken whole from one wild deadline used to shed the rest of the
    /// epoch (the far future) or disable `StaleMedia` for it (the far past).
    #[test]
    fn a_wild_deadline_where_the_head_starts_again_sheds_nothing() {
        let next = EPOCH + 1;
        let key = stream("3001");
        // The head starts again after the cache took up a new session, and
        // after an object of a higher epoch decoded here.
        let start_again = |by_decode: bool| {
            let epochs = ValidatedEpochs::new(64);
            let mut clocks = running();
            if by_decode {
                clocks.set_epoch(key, next);
            } else {
                epochs.set(3001, next);
                clocks.follow(&epochs);
            }
            clocks
        };
        for wild in [
            u64::MAX,
            START + 86_400_000_000,
            START + BUDGET,
            START - BUDGET,
            START - 3_600_000_000,
            1,
        ] {
            for by_decode in [false, true] {
                let mut clocks = start_again(by_decode);
                assert_eq!(clocks.observe(key, next, wild, BUDGET), Verdict::Fresh);
                // A provisional head judges nothing, an assembly held back
                // either.
                assert_eq!(clocks.peek(key, next, START - BUDGET, BUDGET), Verdict::Fresh);
                for part in 1..=2_000_u64 {
                    assert_eq!(
                        clocks.observe(key, next, START + part * 1_000, BUDGET),
                        Verdict::Fresh,
                        "part {part} after the wild deadline {wild}"
                    );
                }
                // The stream's own head judges again.
                assert_eq!(
                    clocks.observe(key, next, START + 2_000_000 - BUDGET, BUDGET),
                    Verdict::StaleMedia,
                    "after the wild deadline {wild}"
                );
            }
        }
        // Both ways at once: the far future, then the far past, then the
        // live stream. Its second part confirms the head, and a part a
        // budget behind is stale from the third on.
        let mut clocks = start_again(false);
        assert_eq!(clocks.observe(key, next, u64::MAX, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.observe(key, next, 1, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.observe(key, next, START + 1_000, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.observe(key, next, START + 2_000, BUDGET), Verdict::Fresh);
        assert_eq!(
            clocks.observe(key, next, START + 2_000 - BUDGET, BUDGET),
            Verdict::StaleMedia
        );
        assert_eq!(
            clocks.peek(key, next, START + 2_000 - BUDGET, BUDGET),
            Verdict::StaleMedia
        );
        // A frozen sender's burst that comes once the head is confirmed is
        // stale, as on a running stream.
        for part in 0..100_u64 {
            assert_eq!(
                clocks.observe(key, next, START - 30_000_000 + part * 1_000, BUDGET),
                Verdict::StaleMedia
            );
        }
        assert_eq!(clocks.observe(key, next, START + 3_000, BUDGET), Verdict::Fresh);
    }

    #[test]
    fn one_corrupt_epoch_retires_nothing() {
        let mut clocks = running();
        let key = stream("3001");
        // A datagram that claims a later epoch is let through (the decode
        // behind judges it) and changes nothing here.
        assert_eq!(
            clocks.observe(key, u64::MAX, START + 86_400_000_000, BUDGET),
            Verdict::Fresh
        );
        assert_eq!(clocks.observe(key, EPOCH + 1, 1, BUDGET), Verdict::Fresh);
        for part in 1..=100_u64 {
            assert_eq!(
                clocks.observe(key, EPOCH, START + part * 1_000, BUDGET),
                Verdict::Fresh
            );
        }
        // The head is where the stream put it.
        assert_eq!(
            clocks.observe(key, EPOCH, START + 100_000 - BUDGET, BUDGET),
            Verdict::StaleMedia
        );
    }

    #[test]
    fn set_epoch_raises_the_epoch_and_a_lower_epoch_is_stale() {
        let mut clocks = running();
        let key = stream("3001");
        let next = EPOCH + 30_000_000;
        // A new session of the source: parts of the new epoch flow before
        // one decodes, beside the tail of the old session.
        assert_eq!(clocks.observe(key, next, START - 5_000_000, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.observe(key, EPOCH, START + 1_000, BUDGET), Verdict::Fresh);
        clocks.set_epoch(key, next);
        assert_eq!(
            clocks.observe(key, EPOCH, START + 2_000, BUDGET),
            Verdict::StaleEpoch
        );
        assert_eq!(clocks.peek(key, EPOCH, START + 2_000, BUDGET), Verdict::StaleEpoch);
        // The head started again: the new session's timeline may be
        // anywhere, behind the old one's too.
        assert_eq!(clocks.observe(key, next, START - 5_000_000, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.observe(key, next, START - 4_999_000, BUDGET), Verdict::Fresh);
        assert_eq!(
            clocks.observe(key, next, START - 6_000_000, BUDGET),
            Verdict::StaleMedia
        );
        // A lower epoch never comes back, and the same epoch again does not
        // start the head again.
        clocks.set_epoch(key, EPOCH);
        clocks.set_epoch(key, next);
        assert_eq!(
            clocks.observe(key, EPOCH, START + 3_000, BUDGET),
            Verdict::StaleEpoch
        );
        assert_eq!(
            clocks.observe(key, next, START - 6_000_000, BUDGET),
            Verdict::StaleMedia
        );
    }

    #[test]
    fn streams_past_the_table_are_fresh_and_counted_and_none_is_retired() {
        let mut clocks = MediaClocks::new(1_024);
        let names: Vec<String> = (0..1_025_u64).map(|id| (10_000 + id).to_string()).collect();
        let mut without_clock = 0_u64;
        for name in &names {
            let key = stream(name);
            let before = clocks.overflow();
            assert_eq!(clocks.observe(key, EPOCH, START, BUDGET), Verdict::Fresh);
            without_clock += clocks.overflow() - before;
            clocks.set_epoch(key, EPOCH);
            assert_eq!(clocks.observe(key, EPOCH, START, BUDGET), Verdict::Fresh);
        }
        // One stream more than the table has slots: at least one found no
        // slot among its eight.
        assert!(without_clock >= 1, "{without_clock}");
        assert_eq!(clocks.created() + without_clock, 1_025);
        // Every stream is still fresh at its head. The streams with a clock
        // kept it (old media is stale there); those without shed nothing.
        let mut judged = 0_u64;
        for name in &names {
            let key = stream(name);
            assert_eq!(clocks.observe(key, EPOCH, START + 1_000, BUDGET), Verdict::Fresh);
            match clocks.observe(key, EPOCH, START - BUDGET, BUDGET) {
                Verdict::StaleMedia => judged += 1,
                Verdict::Fresh => {}
                Verdict::StaleEpoch => panic!("stream {name} lost its epoch"),
            }
        }
        assert_eq!(judged, clocks.created());
        assert_eq!(clocks.overflow(), without_clock * 4);
    }

    #[test]
    fn two_streams_an_hour_apart_keep_their_own_heads() {
        let mut clocks = MediaClocks::new(1_024);
        let (early, late) = (stream("3001"), stream("4001"));
        let hour = 3_600_000_000_u64;
        for key in [early, late] {
            clocks.observe(key, EPOCH, START, BUDGET);
            clocks.set_epoch(key, EPOCH);
        }
        for part in 0..5_000_u64 {
            assert_eq!(
                clocks.observe(early, EPOCH, START + part * 1_000, BUDGET),
                Verdict::Fresh
            );
            assert_eq!(
                clocks.observe(late, EPOCH, START + hour + part * 1_000, BUDGET),
                Verdict::Fresh
            );
        }
        // The same stream name under another tenant is another clock.
        let other = MediaClocks::key("other-tenant", "3001");
        assert_eq!(clocks.observe(other, EPOCH - 1, 1, BUDGET), Verdict::Fresh);
        // What is fresh for the early stream is an hour old for the late
        // one, and neither head moved the other.
        assert_eq!(
            clocks.observe(early, EPOCH, START + 4_999_000 - BUDGET + 1, BUDGET),
            Verdict::Fresh
        );
        assert_eq!(
            clocks.observe(early, EPOCH, START + 4_999_000 - BUDGET, BUDGET),
            Verdict::StaleMedia
        );
        assert_eq!(
            clocks.observe(late, EPOCH, START + 4_999_000, BUDGET),
            Verdict::StaleMedia
        );
        assert_eq!(
            clocks.observe(late, EPOCH, START + hour + 4_999_000 - BUDGET + 1, BUDGET),
            Verdict::Fresh
        );
    }

    #[test]
    fn an_idle_clock_gives_its_slot_away_and_judges_no_lower_epoch() {
        let mut clocks = MediaClocks::new(8);
        // Eight streams fill the eight slots; a ninth has no clock.
        let names: Vec<String> = (0..9_u64).map(|id| (20_000 + id).to_string()).collect();
        clocks.tick(START);
        for name in &names[..8] {
            clocks.observe(stream(name), EPOCH, START, BUDGET);
            clocks.set_epoch(stream(name), EPOCH);
            clocks.observe(stream(name), EPOCH, START, BUDGET);
        }
        assert_eq!(clocks.observe(stream(&names[8]), EPOCH, START, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.overflow(), 1);
        // Just short of five minutes later the first stream alone still
        // flows: nothing is idle yet.
        clocks.tick(START + IDLE_US - 1);
        assert_eq!(
            clocks.observe(stream(&names[0]), EPOCH, START + 1_000, BUDGET),
            Verdict::Fresh
        );
        assert_eq!(
            clocks.observe(stream(&names[1]), EPOCH - 1, START, BUDGET),
            Verdict::StaleEpoch
        );
        assert_eq!(clocks.observe(stream(&names[8]), EPOCH, START, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.overflow(), 2);
        // At five minutes the seven silent clocks are idle. A source that
        // returns with a lower session id is not held at the door (stale
        // datagrams did not keep its clock busy), and what decodes is taken
        // up.
        clocks.tick(START + IDLE_US);
        let returned = stream(&names[1]);
        assert_eq!(clocks.observe(returned, EPOCH - 1, START, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.observe(returned, EPOCH - 1, START, BUDGET), Verdict::Fresh);
        clocks.set_epoch(returned, EPOCH - 1);
        assert_eq!(clocks.observe(returned, EPOCH - 1, START, BUDGET), Verdict::Fresh);
        assert_eq!(
            clocks.observe(returned, EPOCH - 2, START, BUDGET),
            Verdict::StaleEpoch
        );
        // The ninth stream takes an idle slot, and the first stream, never
        // idle, kept its clock.
        let created = clocks.created();
        assert_eq!(clocks.observe(stream(&names[8]), EPOCH, START, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.created(), created + 1);
        assert_eq!(clocks.overflow(), 2);
        assert_eq!(
            clocks.observe(stream(&names[0]), EPOCH, START - BUDGET, BUDGET),
            Verdict::StaleMedia
        );
    }

    #[test]
    fn clocks_follow_the_epochs_that_the_cache_validated() {
        let epochs = ValidatedEpochs::new(1_024);
        let mut clocks = MediaClocks::new(1_024);
        let key = stream("3001");
        assert_eq!(epochs.get(3001), None);
        assert_eq!(epochs.version(), 0);
        // Datagrams first, then the commit of the first object.
        assert_eq!(clocks.observe(key, EPOCH, START, BUDGET), Verdict::Fresh);
        clocks.follow(&epochs);
        assert_eq!(clocks.observe(key, EPOCH - 1, START, BUDGET), Verdict::Fresh);
        epochs.set(3001, EPOCH);
        assert_eq!(epochs.version(), 1);
        clocks.follow(&epochs);
        assert_eq!(clocks.observe(key, EPOCH, START, BUDGET), Verdict::Fresh);
        assert_eq!(
            clocks.observe(key, EPOCH - 1, START, BUDGET),
            Verdict::StaleEpoch
        );
        assert_eq!(clocks.observe(key, EPOCH, START + 1_000, BUDGET), Verdict::Fresh);
        assert_eq!(
            clocks.observe(key, EPOCH, START - BUDGET, BUDGET),
            Verdict::StaleMedia
        );
        // Following again changes nothing: the head stays.
        clocks.follow(&epochs);
        assert_eq!(
            clocks.observe(key, EPOCH, START - BUDGET, BUDGET),
            Verdict::StaleMedia
        );
        // A new session: the epoch rises and the head starts again.
        epochs.set(3001, EPOCH + 9);
        clocks.follow(&epochs);
        assert_eq!(clocks.observe(key, EPOCH, START, BUDGET), Verdict::StaleEpoch);
        assert_eq!(
            clocks.observe(key, EPOCH + 9, START - 5_000_000, BUDGET),
            Verdict::Fresh
        );
        // The cache forgot the stream and took it up again at a lower
        // epoch: the cache's word is the stream's epoch.
        epochs.set(3001, EPOCH + 3);
        clocks.follow(&epochs);
        assert_eq!(clocks.observe(key, EPOCH + 3, START, BUDGET), Verdict::Fresh);
        assert_eq!(
            clocks.observe(key, EPOCH + 2, START, BUDGET),
            Verdict::StaleEpoch
        );
        // A stream that this table sees only after its epoch was validated
        // (an edge whose cache took the epoch from the mesh) follows it at
        // its first look, and a stream without a numeric id follows none.
        epochs.set(4001, EPOCH);
        let (late, named) = (stream("4001"), stream("talkback"));
        let created = clocks.created();
        assert_eq!(clocks.observe(late, EPOCH - 1, START, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.observe(named, EPOCH - 1, START, BUDGET), Verdict::Fresh);
        assert_eq!(clocks.created(), created + 2);
        clocks.follow(&epochs);
        assert_eq!(
            clocks.observe(late, EPOCH - 1, START, BUDGET),
            Verdict::StaleEpoch
        );
        assert_eq!(clocks.observe(named, EPOCH - 1, START, BUDGET), Verdict::Fresh);
        assert_eq!(epochs.get(4001), Some((EPOCH, 1)));
        assert_eq!(epochs.get(3001), Some((EPOCH + 3, 3)));
        assert_eq!(epochs.version(), 4);
    }

    #[test]
    fn the_validated_epochs_take_no_more_streams_than_they_have_slots() {
        let epochs = ValidatedEpochs::new(4);
        for stream_id in 1..=5_u64 {
            epochs.set(stream_id, EPOCH + stream_id);
        }
        for stream_id in 1..=4_u64 {
            assert_eq!(epochs.get(stream_id), Some((EPOCH + stream_id, 1)));
        }
        assert_eq!(epochs.get(5), None);
        assert_eq!(epochs.version(), 4);
        assert_eq!(epochs.unrecorded(), 1);
        // The last stream id has no tag, and is not recorded.
        epochs.set(u64::MAX, EPOCH);
        assert_eq!(epochs.get(u64::MAX), None);
    }

    /// The review of AM1 (finding 3): a full table was probed to its end on
    /// every look, 4,096 loads on the audio thread for each unrecorded
    /// stream at each `follow`.
    #[test]
    fn a_stream_looks_at_eight_slots_and_one_without_room_is_counted() {
        let epochs = ValidatedEpochs::new(4_096);
        // Nine streams whose epochs would go in the same slot.
        let home = ValidatedEpochs::home(1) & epochs.mask;
        let crowd: Vec<u64> = (1..)
            .filter(|id| ValidatedEpochs::home(*id) & epochs.mask == home)
            .take(10)
            .collect();
        for (index, stream_id) in crowd.iter().take(9).enumerate() {
            epochs.set(*stream_id, EPOCH + index as u64);
        }
        for (index, stream_id) in crowd.iter().take(8).enumerate() {
            assert_eq!(epochs.get(*stream_id), Some((EPOCH + index as u64, 1)));
        }
        // The ninth found its eight slots taken, though the table has room.
        assert_eq!(epochs.get(crowd[8]), None);
        assert_eq!(epochs.unrecorded(), 1);
        assert_eq!(epochs.version(), 8);
        // A recorded stream's epoch is written again where it is.
        epochs.set(crowd[0], EPOCH + 100);
        assert_eq!(epochs.get(crowd[0]), Some((EPOCH + 100, 2)));
        assert_eq!(epochs.unrecorded(), 1);
        // A look for a stream never set stops at its eighth slot.
        assert_eq!(epochs.get(crowd[9]), None);
        // Its clocks follow nothing and judge nothing stale.
        let mut clocks = MediaClocks::new(64);
        let name = crowd[8].to_string();
        let key = stream(&name);
        clocks.observe(key, EPOCH, START, BUDGET);
        clocks.follow(&epochs);
        assert_eq!(clocks.observe(key, EPOCH - 1, START, BUDGET), Verdict::Fresh);
    }

    #[test]
    fn a_reader_on_another_thread_sees_every_epoch_once_it_has_seen_the_version() {
        let epochs = std::sync::Arc::new(ValidatedEpochs::new(64));
        let writer = {
            let epochs = std::sync::Arc::clone(&epochs);
            std::thread::spawn(move || {
                for round in 1..=2_000_u64 {
                    for stream_id in 1..=8_u64 {
                        epochs.set(stream_id, round);
                    }
                }
            })
        };
        let mut clocks = MediaClocks::new(64);
        let names: Vec<String> = (1..=8_u64).map(|id| id.to_string()).collect();
        for name in &names {
            clocks.observe(stream(name), 1, START, BUDGET);
        }
        let mut seen = u64::MAX;
        loop {
            let done = writer.is_finished();
            let version = epochs.version();
            if version != seen {
                clocks.follow(&epochs);
                seen = version;
            } else if done {
                break;
            }
        }
        writer.join().unwrap();
        assert_eq!(epochs.version(), 16_000);
        for name in &names {
            // The last epoch written is the clock's: the one before it is
            // stale.
            assert_eq!(
                clocks.observe(stream(name), 1_999, START, BUDGET),
                Verdict::StaleEpoch,
                "stream {name}"
            );
            assert_eq!(clocks.observe(stream(name), 2_000, START, BUDGET), Verdict::Fresh);
        }
    }
}
