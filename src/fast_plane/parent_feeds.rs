//! What each parent delivered to this node, as the class threads saw it.
//!
//! A forwarder may be stopped only when each of its children has every
//! audio object from its other parent, on time. "An arrival within 100 ms"
//! does not show that: a class thread admits and forwards before anything
//! looks at a deadline, so a parent that runs late, or that drops one object
//! in a hundred, still gives its children a fresh arrival every millisecond.
//! So every child keeps, per parent, stream and track, a record of what
//! arrived: how many objects, how many were skipped, and how much time each
//! had left before its deadline. A gate reads the record on the child
//! (`GET /api/node`, `parents[]`) and compares two parents of one node,
//! which needs no clock: both forward the same bytes, so the same object
//! carries the same deadline from each.
//!
//! The key is the sender's IP address, not its socket address: the flows
//! of one parent from several source ports, and a second contributor at
//! another address, each land where a gate wants them, one entry per
//! parent node.
//!
//! The class threads find an entry in an `ArcSwap` table and write it
//! without a lock: an entry is written by one thread (the class thread of
//! its stream), under a sequence number that lets a reader take a
//! consistent copy. A sampler (`crate::fast_control`, once a second) keeps
//! twelve copies per entry and publishes the figures of the last second and
//! of the last ten.

use super::view::RlsView;
use arc_swap::ArcSwap;
use raptorq_datagram_fec::MediaDatagramRole;
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::net::IpAddr;
use std::sync::atomic::{fence, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

/// Seconds of the per-second slack minimum that an entry keeps.
const SLACK_SLOTS: usize = 16;
/// Copies of an entry's counters that the sampler keeps.
const SNAPSHOTS: usize = 12;
/// Whole seconds that the long figures cover.
pub const WINDOW_SECONDS: u64 = 10;
/// An object this far behind the head is a stream that started again.
const RESET_BEHIND: u64 = 4_096;
/// Entries kept at most. Further (address, stream, track) are not recorded.
const MAX_FEEDS: usize = 4_096;
/// An entry without a datagram for this long is dropped by the sampler.
const RETIRE_AFTER_US: u64 = 300_000_000;

/// The counters of one entry: cumulative since the entry was made, but for
/// the head and the slack minima.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Counters {
    datagrams: u64,
    sources: u64,
    repairs: u64,
    /// Unix µs of the last source and repair datagram (0: none yet).
    last_source_us: u64,
    last_repair_us: u64,
    epoch: u64,
    /// The highest object seen, once `has_head`.
    head: u64,
    has_head: bool,
    advances: u64,
    skipped: u64,
    resets: u64,
    multi_symbol: u64,
    /// Of the advances: the time each had left before its deadline.
    slack_sum_us: i64,
    slack_n: u64,
    /// The least slack of each Unix second (the second plus one; 0: empty).
    slack_min: [(u64, i64); SLACK_SLOTS],
}

/// One (address, stream, track). Its cells are written by one class thread
/// between two stores of `seq` (odd while it writes).
struct Feed {
    ip: IpAddr,
    stream: Box<str>,
    track: Box<str>,
    stream_id: Option<u64>,
    seq: AtomicU64,
    datagrams: AtomicU64,
    sources: AtomicU64,
    repairs: AtomicU64,
    last_source_us: AtomicU64,
    last_repair_us: AtomicU64,
    epoch: AtomicU64,
    head: AtomicU64,
    has_head: AtomicU64,
    advances: AtomicU64,
    skipped: AtomicU64,
    resets: AtomicU64,
    multi_symbol: AtomicU64,
    slack_sum_us: AtomicI64,
    slack_n: AtomicU64,
    slack_second: [AtomicU64; SLACK_SLOTS],
    slack_min_us: [AtomicI64; SLACK_SLOTS],
    /// The sampler's copies and figures. Never taken by a class thread.
    history: StdMutex<History>,
}

/// One more, by the entry's only writer: a load and a store, no locked
/// instruction.
fn bump(cell: &AtomicU64, by: u64) {
    cell.store(
        cell.load(Ordering::Relaxed).wrapping_add(by),
        Ordering::Relaxed,
    );
}

impl Feed {
    fn new(ip: IpAddr, stream: &str, track: &str) -> Self {
        Self {
            ip,
            stream: stream.into(),
            track: track.into(),
            stream_id: stream.parse().ok(),
            seq: AtomicU64::new(0),
            datagrams: AtomicU64::new(0),
            sources: AtomicU64::new(0),
            repairs: AtomicU64::new(0),
            last_source_us: AtomicU64::new(0),
            last_repair_us: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
            head: AtomicU64::new(0),
            has_head: AtomicU64::new(0),
            advances: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            resets: AtomicU64::new(0),
            multi_symbol: AtomicU64::new(0),
            slack_sum_us: AtomicI64::new(0),
            slack_n: AtomicU64::new(0),
            slack_second: std::array::from_fn(|_| AtomicU64::new(0)),
            slack_min_us: std::array::from_fn(|_| AtomicI64::new(0)),
            history: StdMutex::new(History::default()),
        }
    }

    fn is(&self, ip: IpAddr, stream: &str, track: &str) -> bool {
        self.ip == ip && &*self.stream == stream && &*self.track == track
    }

    /// Book one admitted datagram. Called by the entry's one writer.
    fn record(&self, view: &Sample) {
        let rx_us = view.rx_us;
        let seq = self.seq.load(Ordering::Relaxed);
        self.seq.store(seq.wrapping_add(1), Ordering::Relaxed);
        // What follows is not seen without the odd sequence before it.
        fence(Ordering::Release);
        bump(&self.datagrams, 1);
        match view.role() {
            MediaDatagramRole::Source => {
                bump(&self.sources, 1);
                self.last_source_us.store(rx_us, Ordering::Relaxed);
            }
            MediaDatagramRole::Repair => {
                bump(&self.repairs, 1);
                self.last_repair_us.store(rx_us, Ordering::Relaxed);
            }
        }
        if view.source_symbols > 1 {
            bump(&self.multi_symbol, 1);
        }
        let has_head = self.has_head.load(Ordering::Relaxed) != 0;
        let head = self.head.load(Ordering::Relaxed);
        let starts_again = has_head
            && (view.epoch != self.epoch.load(Ordering::Relaxed)
                || view.object.saturating_add(RESET_BEHIND) < head);
        if starts_again {
            bump(&self.resets, 1);
        }
        // A source or a repair alike: a one-symbol object decodes from
        // either.
        if !has_head || starts_again || view.object > head {
            if has_head && !starts_again {
                bump(&self.skipped, view.object - head - 1);
            }
            self.epoch.store(view.epoch, Ordering::Relaxed);
            self.head.store(view.object, Ordering::Relaxed);
            self.has_head.store(1, Ordering::Relaxed);
            bump(&self.advances, 1);
            // Slack on an advance only: a compiled lane carries each source
            // again 20 ms later, and a copy is an advance only when the
            // original was lost, when its lateness is the truth.
            let slack = (i128::from(view.deadline_us) - i128::from(rx_us))
                .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64;
            self.slack_sum_us.store(
                self.slack_sum_us
                    .load(Ordering::Relaxed)
                    .wrapping_add(slack),
                Ordering::Relaxed,
            );
            bump(&self.slack_n, 1);
            let second = rx_us / 1_000_000;
            let slot = (second % SLACK_SLOTS as u64) as usize;
            let least = if self.slack_second[slot].load(Ordering::Relaxed) == second + 1 {
                self.slack_min_us[slot].load(Ordering::Relaxed).min(slack)
            } else {
                slack
            };
            self.slack_min_us[slot].store(least, Ordering::Relaxed);
            self.slack_second[slot].store(second + 1, Ordering::Relaxed);
        }
        self.seq.store(seq.wrapping_add(2), Ordering::Release);
    }

    /// A consistent copy of the counters.
    fn read(&self) -> Counters {
        loop {
            let seq = self.seq.load(Ordering::Acquire);
            if seq & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let load = |cell: &AtomicU64| cell.load(Ordering::Relaxed);
            let counters = Counters {
                datagrams: load(&self.datagrams),
                sources: load(&self.sources),
                repairs: load(&self.repairs),
                last_source_us: load(&self.last_source_us),
                last_repair_us: load(&self.last_repair_us),
                epoch: load(&self.epoch),
                head: load(&self.head),
                has_head: load(&self.has_head) != 0,
                advances: load(&self.advances),
                skipped: load(&self.skipped),
                resets: load(&self.resets),
                multi_symbol: load(&self.multi_symbol),
                slack_sum_us: self.slack_sum_us.load(Ordering::Relaxed),
                slack_n: load(&self.slack_n),
                slack_min: std::array::from_fn(|slot| {
                    (
                        load(&self.slack_second[slot]),
                        self.slack_min_us[slot].load(Ordering::Relaxed),
                    )
                }),
            };
            fence(Ordering::Acquire);
            if self.seq.load(Ordering::Relaxed) == seq {
                return counters;
            }
        }
    }
}

/// The figures of one entry over the sampler's last second and last ten.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Figures {
    window_s: u64,
    objects_1s: u64,
    objects_10s: u64,
    datagrams_1s: u64,
    skipped_10s: u64,
    resets_10s: u64,
    multi_symbol_10s: u64,
    slack_mean_us_1s: Option<i64>,
    slack_mean_us_10s: Option<i64>,
    slack_min_us_10s: Option<i64>,
}

/// The sampler's copies of one entry, newest first. An entry's counters
/// start at zero, so the copy before the first is all zeros.
#[derive(Debug, Default)]
struct History {
    samples: VecDeque<Counters>,
    /// Whole seconds of record since the entry was made or last reset.
    whole_seconds: u64,
    figures: Figures,
}

impl History {
    fn take(&mut self, now: Counters, now_us: u64) {
        let previous = self.samples.front().copied();
        self.whole_seconds = match previous {
            // The first copy of an entry, or the first after it started
            // again: the second it was made or reset in is not a whole one.
            None => 0,
            Some(previous) if previous.resets != now.resets => 0,
            Some(_) => self.whole_seconds + 1,
        };
        self.samples.push_front(now);
        self.samples.truncate(SNAPSHOTS);
        let before = |seconds: usize| self.samples.get(seconds).copied().unwrap_or_default();
        let second = before(1);
        let long = before(WINDOW_SECONDS as usize);
        let mean = |older: Counters| {
            let objects = now.slack_n.wrapping_sub(older.slack_n);
            i64::try_from(objects)
                .ok()
                .filter(|objects| *objects > 0)
                .map(|objects| now.slack_sum_us.wrapping_sub(older.slack_sum_us) / objects)
        };
        let now_s = now_us / 1_000_000;
        self.figures = Figures {
            window_s: self.whole_seconds.min(WINDOW_SECONDS),
            objects_1s: now.advances.wrapping_sub(second.advances),
            objects_10s: now.advances.wrapping_sub(long.advances),
            datagrams_1s: now.datagrams.wrapping_sub(second.datagrams),
            skipped_10s: now.skipped.wrapping_sub(long.skipped),
            resets_10s: now.resets.wrapping_sub(long.resets),
            multi_symbol_10s: now.multi_symbol.wrapping_sub(long.multi_symbol),
            slack_mean_us_1s: mean(second),
            slack_mean_us_10s: mean(long),
            // The Unix seconds that hold the last ten seconds: the second
            // now and the ten before it.
            slack_min_us_10s: now
                .slack_min
                .iter()
                .filter(|(second, _)| {
                    *second > 0 && (now_s.saturating_sub(WINDOW_SECONDS)..=now_s).contains(&(second - 1))
                })
                .map(|(_, least)| *least)
                .min(),
        };
    }
}

/// One (parent address, stream, track) as `GET /api/node` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentFeed {
    pub address: IpAddr,
    /// The stream as RLS1 names it, and as the node's caches key it when it
    /// is a number.
    pub stream: String,
    pub stream_id: Option<u64>,
    pub track: String,
    /// Since the last admitted source and repair datagram; `None` before
    /// the first.
    pub source_age_ms: Option<u64>,
    pub repair_age_ms: Option<u64>,
    pub head_object: Option<u64>,
    /// Whole seconds of record since the entry was made or last started
    /// again, at most [`WINDOW_SECONDS`].
    pub window_s: u64,
    /// Objects (head advances) in the sampler's last second and last ten.
    pub objects_1s: u64,
    pub objects_10s: u64,
    pub skipped_10s: u64,
    pub resets_10s: u64,
    /// Datagrams of objects of more than one source symbol.
    pub multi_symbol_10s: u64,
    /// Deadline minus arrival of the objects, in µs (signed): the mean of
    /// the last second and of the last ten, and the least of the last ten.
    pub slack_mean_us_1s: Option<i64>,
    pub slack_mean_us_10s: Option<i64>,
    pub slack_min_us_10s: Option<i64>,
}

/// The datagrams of the fast plane's sockets over the sampler's last
/// second, and how long ago the last one came.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlaneFigures {
    pub received_1s: u64,
    pub admitted_1s: u64,
    pub not_admitted_1s: u64,
    /// Since the count of received datagrams was last seen to rise; `None`
    /// when none was ever received.
    pub received_age_ms: Option<u64>,
}

#[derive(Debug, Default)]
struct Plane {
    received: u64,
    not_admitted: u64,
    figures: PlaneFigures,
}

/// A hash that is its input: the table is keyed by a hash already.
#[derive(Default)]
struct Passed(u64);

impl Hasher for Passed {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 << 8) | u64::from(*byte);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

#[derive(Clone, Default)]
struct Table {
    /// By the hash of (address, stream, track); the few entries of one hash.
    by_hash: HashMap<u64, Vec<Arc<Feed>>, BuildHasherDefault<Passed>>,
    len: usize,
}

impl Table {
    fn find(&self, hash: u64, ip: IpAddr, stream: &str, track: &str) -> Option<&Arc<Feed>> {
        self.by_hash
            .get(&hash)?
            .iter()
            .find(|feed| feed.is(ip, stream, track))
    }

    fn feeds(&self) -> impl Iterator<Item = &Arc<Feed>> {
        self.by_hash.values().flatten()
    }
}

/// What an entry books of one datagram (the fields of its view it reads).
#[derive(Debug, Clone, Copy, Default)]
struct Sample {
    source: bool,
    source_symbols: u16,
    epoch: u64,
    object: u64,
    deadline_us: u64,
    rx_us: u64,
}

impl Sample {
    fn of(view: &RlsView<'_>, rx_us: u64) -> Self {
        Self {
            source: matches!(view.role, MediaDatagramRole::Source),
            source_symbols: view.source_symbols,
            epoch: view.epoch,
            object: view.object,
            deadline_us: view.deadline_us,
            rx_us,
        }
    }

    fn role(&self) -> MediaDatagramRole {
        if self.source {
            MediaDatagramRole::Source
        } else {
            MediaDatagramRole::Repair
        }
    }
}

/// The room of a held key's stream and track ids: any id the wire admits.
const KEY_BYTES: usize = super::media_clock::ID_ROOM_BYTES;

/// The (address, stream, track) of an entry to make, held without an
/// allocation.
#[derive(Debug, Clone, Copy)]
struct InlineKey {
    ip: IpAddr,
    stream: [u8; KEY_BYTES],
    stream_len: u8,
    track: [u8; KEY_BYTES],
    track_len: u8,
}

impl InlineKey {
    fn new(ip: IpAddr, stream: &str, track: &str) -> Option<Self> {
        if stream.len() > KEY_BYTES || track.len() > KEY_BYTES {
            return None;
        }
        let mut key = Self {
            ip,
            stream: [0; KEY_BYTES],
            stream_len: stream.len() as u8,
            track: [0; KEY_BYTES],
            track_len: track.len() as u8,
        };
        key.stream[..stream.len()].copy_from_slice(stream.as_bytes());
        key.track[..track.len()].copy_from_slice(track.as_bytes());
        Some(key)
    }

    fn stream(&self) -> &str {
        std::str::from_utf8(&self.stream[..usize::from(self.stream_len)]).unwrap_or_default()
    }

    fn track(&self) -> &str {
        std::str::from_utf8(&self.track[..usize::from(self.track_len)]).unwrap_or_default()
    }

    fn is(&self, ip: IpAddr, stream: &str, track: &str) -> bool {
        self.ip == ip && self.stream() == stream && self.track() == track
    }
}

/// Keys a class thread holds datagrams of while their entries are made.
const MISSES: usize = 16;
/// Datagrams held per key: a few milliseconds of a 1 ms stream, more than
/// the tee takes to make the entry.
const MISS_SAMPLES: usize = 64;
/// A key whose entry has not come this long after its first datagram is
/// given up (its datagrams count as untracked).
const MISS_GIVE_UP_US: u64 = 1_000_000;
/// Entries asked for and not yet made, at most.
const REQUESTS: usize = 32;

#[derive(Clone, Copy)]
struct Miss {
    hash: u64,
    key: InlineKey,
    samples: [Sample; MISS_SAMPLES],
    count: usize,
    first_us: u64,
    /// The tee has been asked to make the entry.
    requested: bool,
}

/// One class thread's datagrams of keys that have no entry yet (stall
/// follow-up): the thread does not make an entry (an allocation and a
/// copy of the table on the real-time thread) but asks the tee to
/// (`ParentFeeds::make_requested`) and holds the key's datagrams here, then
/// books them in order once the entry exists, before anything newer of
/// the key, so the record is the same as if the entry had been there.
/// Fixed size, made before the thread runs.
pub struct FeedMisses {
    misses: Box<[Option<Miss>]>,
    held: usize,
}

impl FeedMisses {
    pub fn new() -> Self {
        Self {
            misses: vec![None; MISSES].into_boxed_slice(),
            held: 0,
        }
    }
}

impl FeedMisses {
    /// Keys held now.
    #[cfg(test)]
    pub fn held(&self) -> usize {
        self.held
    }
}

impl Default for FeedMisses {
    fn default() -> Self {
        Self::new()
    }
}

/// The entries the class threads asked for.
struct Requests {
    keys: [Option<(u64, InlineKey)>; REQUESTS],
}

impl Requests {
    /// Whether the key is asked for and not yet taken by the tee.
    fn asked(&self, hash: u64, key: &InlineKey) -> bool {
        self.keys
            .iter()
            .flatten()
            .any(|(other, asked)| *other == hash && asked.is(key.ip, key.stream(), key.track()))
    }

    /// Ask for the key's entry; `false` when the requests are full.
    fn ask(&mut self, hash: u64, key: &InlineKey) -> bool {
        if self.asked(hash, key) {
            return true;
        }
        match self.keys.iter_mut().find(|slot| slot.is_none()) {
            Some(slot) => {
                *slot = Some((hash, *key));
                true
            }
            None => false,
        }
    }
}

/// The feed records of a node: one entry per (parent address, stream,
/// track) that an admitted datagram came from.
pub struct ParentFeeds {
    table: ArcSwap<Table>,
    /// Tables replaced while a class thread read them (see `retired`).
    retired: super::retired::Retired<Table>,
    /// Entries the class threads asked for: they `try_lock` it, never wait.
    requests: StdMutex<Requests>,
    hasher: std::collections::hash_map::RandomState,
    /// Datagrams not recorded because the table was full.
    untracked: AtomicU64,
    plane: StdMutex<Plane>,
    /// The count of received datagrams when it was last looked at, and when
    /// it was last seen to have risen (Unix µs; 0: never).
    received_seen: AtomicU64,
    received_changed_us: AtomicU64,
}

impl Default for ParentFeeds {
    fn default() -> Self {
        Self {
            table: ArcSwap::from_pointee(Table::default()),
            retired: super::retired::Retired::default(),
            requests: StdMutex::new(Requests {
                keys: [None; REQUESTS],
            }),
            hasher: std::collections::hash_map::RandomState::new(),
            untracked: AtomicU64::new(0),
            plane: StdMutex::new(Plane::default()),
            received_seen: AtomicU64::new(0),
            received_changed_us: AtomicU64::new(0),
        }
    }
}

fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn age_ms(now_us: u64, at_us: u64) -> Option<u64> {
    (at_us > 0).then(|| now_us.saturating_sub(at_us) / 1_000)
}

impl ParentFeeds {
    fn hash(&self, ip: IpAddr, stream: &str, track: &str) -> u64 {
        let mut hasher = self.hasher.build_hasher();
        match ip {
            IpAddr::V4(ip) => hasher.write(&ip.octets()),
            IpAddr::V6(ip) => hasher.write(&ip.octets()),
        }
        hasher.write(stream.as_bytes());
        hasher.write_u8(0xff);
        hasher.write(track.as_bytes());
        hasher.finish()
    }

    /// Book one admitted datagram from the parent at `ip`, received at
    /// `rx_us` (Unix µs). Called by the class thread of the datagram's
    /// stream for every admitted datagram, before the first-arrival test:
    /// no lock, and no allocation. A key with no entry yet is held in the
    /// thread's `misses` and its entry asked of the tee (`make_requested`);
    /// a full table takes no new key (`untracked`).
    pub fn observe(&self, ip: IpAddr, view: &RlsView<'_>, rx_us: u64, misses: &mut FeedMisses) {
        let hash = self.hash(ip, view.stream, view.track);
        let table = self.table.load();
        let sample = Sample::of(view, rx_us);
        if let Some(feed) = table.find(hash, ip, view.stream, view.track) {
            if misses.held > 0 {
                Self::book_held(feed, hash, misses);
            }
            feed.record(&sample);
            return;
        }
        if table.len >= MAX_FEEDS {
            self.untracked.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.hold(hash, ip, view, sample, misses);
    }

    /// Book what `misses` holds of the key whose entry `feed` is, in order.
    fn book_held(feed: &Feed, hash: u64, misses: &mut FeedMisses) {
        for slot in misses.misses.iter_mut() {
            let Some(miss) = slot else {
                continue;
            };
            if miss.hash == hash && miss.key.is(feed.ip, &feed.stream, &feed.track) {
                for sample in &miss.samples[..miss.count] {
                    feed.record(sample);
                }
                *slot = None;
                misses.held -= 1;
                return;
            }
        }
    }

    /// Hold a datagram of a key without an entry, and ask for the entry.
    fn hold(
        &self,
        hash: u64,
        ip: IpAddr,
        view: &RlsView<'_>,
        sample: Sample,
        misses: &mut FeedMisses,
    ) {
        // A key given up is freed after the pass (`book_misses`).
        let mut free = None;
        let mut found = None;
        for (index, slot) in misses.misses.iter().enumerate() {
            match slot {
                Some(miss) if miss.hash == hash && miss.key.is(ip, view.stream, view.track) => {
                    found = Some(index);
                    break;
                }
                None => free = free.or(Some(index)),
                Some(_) => {}
            }
        }
        let index = match (found, free) {
            (Some(index), _) => index,
            (None, Some(index)) => {
                let Some(key) = InlineKey::new(ip, view.stream, view.track) else {
                    self.untracked.fetch_add(1, Ordering::Relaxed);
                    return;
                };
                misses.misses[index] = Some(Miss {
                    hash,
                    key,
                    samples: [Sample::default(); MISS_SAMPLES],
                    count: 0,
                    first_us: sample.rx_us,
                    requested: false,
                });
                misses.held += 1;
                index
            }
            (None, None) => {
                self.untracked.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let Some(miss) = misses.misses[index].as_mut() else {
            return;
        };
        if miss.count == MISS_SAMPLES {
            self.untracked.fetch_add(1, Ordering::Relaxed);
        } else {
            miss.samples[miss.count] = sample;
            miss.count += 1;
        }
        if !miss.requested {
            miss.requested = self.request(miss.hash, miss.key);
        }
    }

    /// Ask the tee to make an entry; `false` when the requests are in use
    /// or full (the key asks again after the pass, `book_misses`).
    fn request(&self, hash: u64, key: InlineKey) -> bool {
        let Ok(mut requests) = self.requests.try_lock() else {
            return false;
        };
        requests.ask(hash, &key)
    }

    /// After each of a class thread's passes: book what it holds of keys
    /// whose entries now exist; give up (as untracked) a key whose entry has
    /// not come [`MISS_GIVE_UP_US`] after its first datagram, whether the
    /// tee was not asked, refused it (a full table) or is late; and ask
    /// again for a key whose request the tee took without making its entry.
    pub fn book_misses(&self, misses: &mut FeedMisses, now_us: u64) {
        if misses.held == 0 {
            return;
        }
        let table = self.table.load();
        // The requests not yet taken by the tee (`None`: in use by another
        // thread, so this pass asks for nothing).
        let mut requests = self.requests.try_lock().ok();
        for index in 0..misses.misses.len() {
            let Some((hash, key, first_us, count, requested)) = misses.misses[index]
                .as_ref()
                .map(|miss| (miss.hash, miss.key, miss.first_us, miss.count, miss.requested))
            else {
                continue;
            };
            if let Some(feed) = table.find(hash, key.ip, key.stream(), key.track()) {
                Self::book_held(feed, hash, misses);
                continue;
            }
            if now_us.saturating_sub(first_us) > MISS_GIVE_UP_US {
                self.untracked.fetch_add(count as u64, Ordering::Relaxed);
                misses.misses[index] = None;
                misses.held -= 1;
                continue;
            }
            let Some(requests) = requests.as_mut() else {
                continue;
            };
            if !requested || !requests.asked(hash, &key) {
                if let Some(held) = misses.misses[index].as_mut() {
                    held.requested = requests.ask(hash, &key);
                }
            }
        }
    }

    /// Make the entries the class threads asked for (the tee, after each of
    /// its batches).
    pub fn make_requested(&self) {
        let asked = {
            let mut requests = lock(&self.requests);
            if requests.keys.iter().all(Option::is_none) {
                return;
            }
            std::mem::replace(&mut requests.keys, [None; REQUESTS])
        };
        for (hash, key) in asked.into_iter().flatten() {
            self.insert(hash, key.ip, key.stream(), key.track());
        }
    }

    /// The class thread's first look at the table, before it runs: the
    /// lock-free snapshots register a thread on first use.
    pub fn warm_up(&self) {
        drop(self.table.load());
    }

    /// Make the entry of a new (address, stream, track), unless it exists
    /// or the table is full: the table is copied with it.
    fn insert(&self, hash: u64, ip: IpAddr, stream: &str, track: &str) {
        let made = Arc::new(Feed::new(ip, stream, track));
        // Whether the closure's last run replaced the table (rcu may run it
        // again after a race): one that kept it has nothing to retire.
        let replaced = std::cell::Cell::new(false);
        let old = self.table.rcu(|table| {
            if table.find(hash, ip, stream, track).is_some() || table.len >= MAX_FEEDS {
                replaced.set(false);
                return Arc::clone(table);
            }
            let mut next = Table::clone(table);
            next.by_hash
                .entry(hash)
                .or_default()
                .push(Arc::clone(&made));
            next.len += 1;
            replaced.set(true);
            Arc::new(next)
        });
        if replaced.get() {
            self.retired.keep(old);
        }
    }

    /// Datagrams that found the table full.
    pub fn untracked(&self) -> u64 {
        self.untracked.load(Ordering::Relaxed)
    }

    /// Note the count of received datagrams as it is now.
    fn note_received(&self, received: u64, now_us: u64) {
        if self.received_seen.swap(received, Ordering::Relaxed) != received {
            self.received_changed_us
                .store(now_us.max(1), Ordering::Relaxed);
        }
    }

    /// The sampler's pass, once a second: a copy of every entry and its
    /// figures, the plane's figures from the counts of datagrams received
    /// and not admitted so far, and the entries without a datagram for five
    /// minutes dropped.
    pub fn sample(&self, received: u64, not_admitted: u64, now_us: u64) {
        self.note_received(received, now_us);
        let table = self.table.load_full();
        let mut admitted_1s = 0_u64;
        let mut idle = false;
        for feed in table.feeds() {
            let counters = feed.read();
            let mut history = lock(&feed.history);
            history.take(counters, now_us);
            admitted_1s += history.figures.datagrams_1s;
            idle |= now_us.saturating_sub(counters.last_source_us.max(counters.last_repair_us))
                > RETIRE_AFTER_US;
        }
        {
            let mut plane = lock(&self.plane);
            plane.figures = PlaneFigures {
                received_1s: received.wrapping_sub(plane.received),
                admitted_1s,
                not_admitted_1s: not_admitted.wrapping_sub(plane.not_admitted),
                received_age_ms: None,
            };
            plane.received = received;
            plane.not_admitted = not_admitted;
        }
        if idle {
            let old = self.table.rcu(|table| {
                let mut next = Table::default();
                for feed in table.feeds() {
                    let counters = feed.read();
                    if now_us.saturating_sub(counters.last_source_us.max(counters.last_repair_us))
                        <= RETIRE_AFTER_US
                    {
                        let hash = self.hash(feed.ip, &feed.stream, &feed.track);
                        next.by_hash.entry(hash).or_default().push(Arc::clone(feed));
                        next.len += 1;
                    }
                }
                Arc::new(next)
            });
            self.retired.keep(old);
        }
    }

    /// The plane's figures, with the age of the last received datagram
    /// judged against the count of received datagrams as it is now.
    pub fn plane(&self, received: u64, now_us: u64) -> PlaneFigures {
        self.note_received(received, now_us);
        PlaneFigures {
            received_age_ms: age_ms(now_us, self.received_changed_us.load(Ordering::Relaxed)),
            ..lock(&self.plane).figures
        }
    }

    /// Every entry: its ages and head as they are now, and the sampler's
    /// figures. Sorted by address, stream and track.
    pub fn snapshot(&self, now_us: u64) -> Vec<ParentFeed> {
        let table = self.table.load_full();
        let mut feeds = table
            .feeds()
            .map(|feed| {
                let counters = feed.read();
                let figures = lock(&feed.history).figures;
                ParentFeed {
                    address: feed.ip,
                    stream: feed.stream.to_string(),
                    stream_id: feed.stream_id,
                    track: feed.track.to_string(),
                    source_age_ms: age_ms(now_us, counters.last_source_us),
                    repair_age_ms: age_ms(now_us, counters.last_repair_us),
                    head_object: counters.has_head.then_some(counters.head),
                    window_s: figures.window_s,
                    objects_1s: figures.objects_1s,
                    objects_10s: figures.objects_10s,
                    skipped_10s: figures.skipped_10s,
                    resets_10s: figures.resets_10s,
                    multi_symbol_10s: figures.multi_symbol_10s,
                    slack_mean_us_1s: figures.slack_mean_us_1s,
                    slack_mean_us_10s: figures.slack_mean_us_10s,
                    slack_min_us_10s: figures.slack_min_us_10s,
                }
            })
            .collect::<Vec<_>>();
        feeds.sort_by(|a, b| {
            (a.address, a.stream_id, &a.stream, &a.track).cmp(&(
                b.address,
                b.stream_id,
                &b.stream,
                &b.track,
            ))
        });
        feeds
    }

    /// Whether the parent at `ip` delivers stream `stream_id` now: an
    /// admitted source of it came from that address within `within_us`.
    /// The release of a parent that left a ranked set waits for this of its
    /// replacement (the lifecycle unit's parent release task).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn delivering(&self, ip: IpAddr, stream_id: u64, now_us: u64, within_us: u64) -> bool {
        self.table.load().feeds().any(|feed| {
            feed.ip == ip
                && feed.stream_id == Some(stream_id)
                && Some(feed.read().last_source_us)
                    .filter(|at_us| *at_us > 0)
                    .is_some_and(|at_us| now_us.saturating_sub(at_us) <= within_us)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raptorq_datagram_fec::{MediaObjectKind, MediaPathIntent};
    use std::net::{Ipv4Addr, SocketAddr};

    /// The allocations of the calling thread so far (the test binary's
    /// counting allocator, `crate::alloc_count`).
    fn allocations() -> u64 {
        crate::alloc_count::allocations()
    }

    thread_local! {
        static MISSES: std::cell::RefCell<FeedMisses> = std::cell::RefCell::new(FeedMisses::new());
    }

    /// The class thread's call, with the entry it asks for made at once
    /// (as the tee makes it after its batch) and what it held booked (as
    /// the thread books it after its pass).
    fn observe(feeds: &ParentFeeds, ip: IpAddr, view: &RlsView<'_>, rx_us: u64) {
        MISSES.with(|misses| {
            let mut misses = misses.borrow_mut();
            feeds.observe(ip, view, rx_us, &mut misses);
            feeds.make_requested();
            feeds.book_misses(&mut misses, rx_us);
        });
    }

    static NO_HASH: [u8; 32] = [0; 32];
    /// A time well into a Unix second, so that a few µs either way stay in
    /// it.
    const T0: u64 = 1_763_900_000_500_000;
    const SECOND: u64 = 1_000_000;

    fn view(
        stream: &'static str,
        epoch: u64,
        object: u64,
        role: MediaDatagramRole,
        deadline_us: u64,
    ) -> RlsView<'static> {
        RlsView {
            generation: 1,
            subscription_id: 1,
            deadline_us,
            role,
            kind: MediaObjectKind::Audio,
            path_intent: match role {
                MediaDatagramRole::Source => MediaPathIntent::PrimarySource,
                MediaDatagramRole::Repair => MediaPathIntent::SecondaryRepair,
            },
            tenant: "default",
            stream,
            track: "opaque-audio",
            epoch,
            group: 0,
            object,
            version: 1,
            payload_hash: &NO_HASH,
            fec: &[],
            block_id: 0,
            packet_sequence: 0,
            source_symbols: 1,
            esi: 0,
            slim: None,
        }
    }

    fn source(object: u64, deadline_us: u64) -> RlsView<'static> {
        view("3001", 7, object, MediaDatagramRole::Source, deadline_us)
    }

    fn parent(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 80, 3, last))
    }

    fn only(feeds: &ParentFeeds, now_us: u64) -> ParentFeed {
        let mut rows = feeds.snapshot(now_us);
        assert_eq!(rows.len(), 1, "{rows:?}");
        rows.remove(0)
    }

    #[test]
    fn an_advance_counts_what_it_skipped() {
        let feeds = ParentFeeds::default();
        let ip = parent(11);
        for object in [10, 11, 12] {
            observe(&feeds, ip, &source(object, T0 + SECOND), T0);
        }
        // 13 and 14 never come from this parent; 15 comes as its repair
        // (a one-symbol object decodes from either), then 16.
        observe(
            &feeds,
            ip,
            &view("3001", 7, 15, MediaDatagramRole::Repair, T0 + SECOND),
            T0 + 3_000,
        );
        observe(&feeds, ip, &source(16, T0 + SECOND), T0 + 4_000);
        // One that comes late and out of order is neither an advance nor a
        // skip taken back.
        observe(&feeds, ip, &source(13, T0 + SECOND), T0 + 5_000);
        feeds.sample(6, 0, T0 + 10_000);
        let row = only(&feeds, T0 + 10_000);
        assert_eq!(row.head_object, Some(16));
        assert_eq!(row.objects_1s, 5, "10, 11, 12, 15 and 16");
        assert_eq!(row.objects_10s, 5);
        assert_eq!(row.skipped_10s, 2, "13 and 14");
        assert_eq!(row.resets_10s, 0);
        assert_eq!(row.source_age_ms, Some(5));
        assert_eq!(row.repair_age_ms, Some(7));
        assert_eq!((row.stream_id, row.track.as_str()), (Some(3001), "opaque-audio"));
        // The first object of an entry skipped nothing, whatever its number.
        assert_eq!(feeds.plane(6, T0 + 10_000).admitted_1s, 6);
    }

    #[test]
    fn a_burst_copy_is_not_an_advance_and_does_not_move_the_slack() {
        let feeds = ParentFeeds::default();
        let ip = parent(11);
        // Each object arrives 7 ms before its deadline, and again 20 ms
        // later (the compiled lane's burst copy), 13 ms past it.
        for object in 0..100_u64 {
            let sent = T0 + object * 1_000;
            observe(&feeds, ip, &source(object, sent + 7_000), sent);
        }
        for object in 0..100_u64 {
            let sent = T0 + object * 1_000;
            observe(&feeds, ip, &source(object, sent + 7_000), sent + 20_000);
        }
        feeds.sample(200, 0, T0 + 200_000);
        let row = only(&feeds, T0 + 200_000);
        assert_eq!(row.objects_1s, 100, "the copies are not objects");
        assert_eq!(row.slack_mean_us_1s, Some(7_000));
        assert_eq!(row.slack_mean_us_10s, Some(7_000));
        assert_eq!(row.slack_min_us_10s, Some(7_000));
        assert_eq!(row.skipped_10s, 0);
        // The copies are datagrams of this parent all the same.
        assert_eq!(feeds.plane(200, T0 + 200_000).admitted_1s, 200);

        // An object whose original was lost: its copy is the advance, and
        // its lateness is the truth.
        let sent = T0 + 100_000;
        observe(&feeds, ip, &source(100, sent + 7_000), sent + 20_000);
        feeds.sample(201, 0, T0 + SECOND + 200_000);
        let row = only(&feeds, T0 + SECOND + 200_000);
        assert_eq!(row.objects_1s, 1);
        assert_eq!(row.slack_mean_us_1s, Some(-13_000));
        assert_eq!(row.slack_min_us_10s, Some(-13_000));
    }

    #[test]
    fn a_new_epoch_resets_the_entry() {
        let feeds = ParentFeeds::default();
        let ip = parent(11);
        for object in 5_000..5_010_u64 {
            observe(&feeds, ip, &source(object, T0 + SECOND), T0);
        }
        feeds.sample(10, 0, T0 + 1_000);
        feeds.sample(10, 0, T0 + SECOND + 1_000);
        assert_eq!(only(&feeds, T0 + SECOND + 1_000).window_s, 1);

        // The contributor starts again and counts from zero in a new epoch:
        // not 5,009 objects skipped backwards, and nothing skipped forwards.
        for object in 0..3_u64 {
            observe(
                &feeds,
                ip,
                &view("3001", 8, object, MediaDatagramRole::Source, T0 + 3 * SECOND),
                T0 + SECOND + 2_000,
            );
        }
        feeds.sample(13, 0, T0 + 2 * SECOND + 1_000);
        let row = only(&feeds, T0 + 2 * SECOND + 1_000);
        assert_eq!(row.head_object, Some(2));
        assert_eq!(row.resets_10s, 1);
        assert_eq!(row.skipped_10s, 0);
        assert_eq!(row.objects_1s, 3);
        assert_eq!(row.window_s, 0, "the record starts again");

        // The same epoch far behind the head is a stream that started
        // again too; a little behind it is a late datagram.
        observe(
            &feeds,
            ip,
            &view("3001", 8, 50_000, MediaDatagramRole::Source, T0 + 3 * SECOND),
            T0 + 2 * SECOND + 2_000,
        );
        observe(
            &feeds,
            ip,
            &view("3001", 8, 49_990, MediaDatagramRole::Source, T0 + 3 * SECOND),
            T0 + 2 * SECOND + 3_000,
        );
        observe(
            &feeds,
            ip,
            &view("3001", 8, 40_000, MediaDatagramRole::Source, T0 + 3 * SECOND),
            T0 + 2 * SECOND + 4_000,
        );
        feeds.sample(16, 0, T0 + 3 * SECOND + 1_000);
        let row = only(&feeds, T0 + 3 * SECOND + 1_000);
        assert_eq!(row.head_object, Some(40_000));
        assert_eq!(row.resets_10s, 2);
        assert_eq!(row.skipped_10s, 49_997, "from 2 to 50,000, once");
    }

    #[test]
    fn two_senders_of_one_address_are_one_parent() {
        let feeds = ParentFeeds::default();
        // Two flows of one parent (two source ports), and a second parent.
        let flow_a: SocketAddr = "10.80.3.11:22401".parse().unwrap();
        let flow_b: SocketAddr = "10.80.3.11:24007".parse().unwrap();
        let other: SocketAddr = "10.80.3.12:22401".parse().unwrap();
        for object in 0..10_u64 {
            let flow = if object % 2 == 0 { flow_a } else { flow_b };
            observe(&feeds, flow.ip(), &source(object, T0 + SECOND), T0 + object);
            observe(&feeds, other.ip(), &source(object, T0 + SECOND), T0 + object);
        }
        observe(
            &feeds,
            other.ip(),
            &view("4001", 7, 0, MediaDatagramRole::Source, T0 + SECOND),
            T0 + 10,
        );
        feeds.sample(21, 0, T0 + 1_000);
        let rows = feeds.snapshot(T0 + 1_000);
        assert_eq!(
            rows.iter()
                .map(|row| (row.address, row.stream_id, row.objects_1s, row.skipped_10s))
                .collect::<Vec<_>>(),
            vec![
                (flow_a.ip(), Some(3001), 10, 0),
                (other.ip(), Some(3001), 10, 0),
                (other.ip(), Some(4001), 1, 0),
            ],
            "the two flows of one address are one complete feed"
        );
        assert!(feeds.delivering(flow_b.ip(), 3001, T0 + 1_000, 100_000));
        assert!(!feeds.delivering(flow_a.ip(), 4001, T0 + 1_000, 100_000));
        assert!(!feeds.delivering(other.ip(), 4001, T0 + SECOND, 100_000));
    }

    #[test]
    fn the_window_is_ten_whole_seconds() {
        let feeds = ParentFeeds::default();
        let ip = parent(11);
        let mut object = 0_u64;
        let mut received = 0_u64;
        // 100 objects a second for 14 seconds, sampled at each second's end.
        // In second 2 one object has 3 ms of slack, the others 9 ms.
        for second in 0..14_u64 {
            for index in 0..100_u64 {
                let at = T0 + second * SECOND + index * 1_000;
                let slack = if second == 2 && index == 40 { 3_000 } else { 9_000 };
                observe(&feeds, ip, &source(object, at + slack), at);
                object += 1;
                received += 1;
            }
            let sampled = T0 + second * SECOND + 400_000;
            feeds.sample(received, 0, sampled);
            let row = only(&feeds, sampled);
            assert_eq!(row.window_s, second.min(10), "after sample {second}");
            assert_eq!(row.objects_1s, 100);
            assert_eq!(row.objects_10s, 100 * (second + 1).min(10));
            assert_eq!(row.skipped_10s, 0);
            // The object of second 2 is in the ten seconds until the sample
            // of second 12 (the seconds 2 to 12), and gone after it.
            let least = if (2..=12).contains(&second) { 3_000 } else { 9_000 };
            assert_eq!(row.slack_min_us_10s, Some(least), "after sample {second}");
            let plane = feeds.plane(received, sampled);
            assert_eq!((plane.received_1s, plane.admitted_1s), (100, 100));
            assert_eq!(plane.received_age_ms, Some(0));
        }
        // Nothing more arrives: the figures empty second by second, the ages
        // grow, and the record stays ten whole seconds long.
        let last = T0 + 13 * SECOND + 99_000;
        for second in 14..26_u64 {
            feeds.sample(received, 0, T0 + second * SECOND + 400_000);
        }
        let now = T0 + 25 * SECOND + 400_000;
        let row = only(&feeds, now);
        assert_eq!(row.window_s, 10);
        assert_eq!((row.objects_1s, row.objects_10s), (0, 0));
        assert_eq!(row.slack_mean_us_10s, None);
        assert_eq!(row.slack_min_us_10s, None);
        assert_eq!(row.source_age_ms, Some((now - last) / 1_000));
        assert_eq!(row.repair_age_ms, None);
        let plane = feeds.plane(received, now);
        assert_eq!((plane.received_1s, plane.admitted_1s), (0, 0));
        assert_eq!(plane.received_age_ms, Some(12_000));

        // An entry silent for five minutes is dropped.
        feeds.sample(received, 0, last + RETIRE_AFTER_US + 1);
        assert!(feeds.snapshot(last + RETIRE_AFTER_US + 1).is_empty());
    }

    #[test]
    fn a_new_key_is_held_without_allocating_and_booked_in_order_once_made() {
        // Stall follow-up: the class thread makes no entry. It holds a new
        // key's datagrams, asks the tee for the entry, and books them in
        // order when the entry exists, so the record is the one it would
        // have been.
        let feeds = ParentFeeds::default();
        let mut misses = FeedMisses::new();
        let ip = parent(31);
        // As the class thread does before it runs (arc-swap registers a
        // thread at its first load).
        feeds.warm_up();
        let before = allocations();
        for object in [40, 41, 43] {
            feeds.observe(ip, &source(object, T0 + SECOND), T0 + object, &mut misses);
        }
        assert_eq!(allocations() - before, 0, "allocations on the class thread");
        assert_eq!(misses.held(), 1);
        assert!(feeds.snapshot(T0 + 100).is_empty(), "no entry made on the class thread");
        // The tee makes it; the thread's next datagram of the key books the
        // held ones first.
        feeds.make_requested();
        feeds.observe(ip, &source(44, T0 + SECOND), T0 + 44, &mut misses);
        assert_eq!(misses.held(), 0);
        feeds.sample(4, 0, T0 + 200);
        let feed = only(&feeds, T0 + 200);
        assert_eq!(feed.head_object, Some(44));
        assert_eq!(feed.objects_1s, 4);
        assert_eq!(feed.skipped_10s, 1, "object 42 was skipped, in order");
        assert_eq!(feeds.untracked(), 0);
        // A key whose entry is made after its last datagram is booked after
        // the thread's pass.
        let other = parent(32);
        feeds.observe(other, &source(7, T0 + SECOND), T0 + 300, &mut misses);
        feeds.make_requested();
        feeds.book_misses(&mut misses, T0 + 301);
        assert_eq!(misses.held(), 0);
        assert_eq!(feeds.snapshot(T0 + 400).len(), 2);
    }

    #[test]
    fn the_longest_admitted_ids_are_held_without_allocating_and_get_their_entry() {
        // D3 review 2: a held key has room for any id the wire admits
        // (128 bytes), so such a stream gets its entry like any other.
        static LONG: std::sync::OnceLock<(String, String)> = std::sync::OnceLock::new();
        let (stream, track) =
            LONG.get_or_init(|| ("s".repeat(KEY_BYTES), "t".repeat(KEY_BYTES)));
        assert_eq!(KEY_BYTES, relay_session::RelayLimits::HARD.max_identifier_bytes);
        let feeds = ParentFeeds::default();
        let mut misses = FeedMisses::new();
        let ip = parent(33);
        let mut long = source(40, T0 + SECOND);
        long.stream = stream;
        long.track = track;
        // As the class thread does before it runs.
        feeds.warm_up();
        let before = allocations();
        feeds.observe(ip, &long, T0 + 40, &mut misses);
        assert_eq!(allocations() - before, 0, "allocations on the class thread");
        assert_eq!(misses.held(), 1);
        feeds.make_requested();
        long.object = 41;
        feeds.observe(ip, &long, T0 + 41, &mut misses);
        assert_eq!(misses.held(), 0);
        assert_eq!(feeds.untracked(), 0);
        let feed = only(&feeds, T0 + 100);
        assert_eq!((feed.stream.as_str(), feed.track.as_str()), (stream.as_str(), track.as_str()));
        assert_eq!(feed.head_object, Some(41));
    }

    #[test]
    fn a_held_key_whose_entry_is_refused_is_asked_again_then_given_up() {
        // D3 review 5: the tee took the request but the table filled first.
        // The key is asked for again after the pass, and given up (its
        // datagrams counted untracked) a second after its first datagram,
        // so the thread stops holding it.
        let feeds = ParentFeeds::default();
        let mut misses = FeedMisses::new();
        let ip = parent(51);
        feeds.observe(ip, &source(1, T0 + SECOND), T0, &mut misses);
        feeds.observe(ip, &source(2, T0 + SECOND), T0 + 1, &mut misses);
        assert_eq!(misses.held(), 1);
        for last in 0..MAX_FEEDS as u32 {
            let other = IpAddr::V4(Ipv4Addr::from(0x0c00_0000 + last));
            feeds.insert(feeds.hash(other, "3001", "opaque-audio"), other, "3001", "opaque-audio");
        }
        feeds.make_requested();
        assert!(lock(&feeds.requests).keys.iter().all(Option::is_none));
        feeds.book_misses(&mut misses, T0 + 10);
        assert_eq!(misses.held(), 1);
        assert!(
            lock(&feeds.requests).keys.iter().flatten().count() == 1,
            "asked again"
        );
        feeds.make_requested();
        feeds.book_misses(&mut misses, T0 + MISS_GIVE_UP_US + 1);
        assert_eq!(misses.held(), 0);
        assert_eq!(feeds.untracked(), 2);
        assert_eq!(feeds.snapshot(T0).len(), MAX_FEEDS);
    }

    #[test]
    fn an_insert_that_changes_nothing_retires_nothing() {
        // D3 review 4: an insert of a key that exists (or into a full
        // table) leaves the table as it is; it used to hand the current
        // table to `retired`, where it and its feeds stayed for good.
        let feeds = ParentFeeds::default();
        let ip = parent(41);
        let hash = feeds.hash(ip, "3001", "opaque-audio");
        feeds.insert(hash, ip, "3001", "opaque-audio");
        assert_eq!(feeds.retired.len(), 0);
        for _ in 0..10 {
            feeds.insert(hash, ip, "3001", "opaque-audio");
        }
        assert_eq!(feeds.retired.len(), 0);
        assert_eq!(Arc::strong_count(&feeds.table.load_full()), 2, "the swap's and this one");
    }

    #[test]
    fn a_full_table_records_no_new_entry_and_counts_it() {
        let feeds = ParentFeeds::default();
        for last in 0..MAX_FEEDS as u32 + 3 {
            let ip = IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + last));
            observe(&feeds, ip, &source(0, T0 + SECOND), T0);
        }
        assert_eq!(feeds.snapshot(T0).len(), MAX_FEEDS);
        assert_eq!(feeds.untracked(), 3);

        // On the class thread, neither a tracked key nor an untracked one
        // allocates once the table is full.
        let tracked = IpAddr::V4(Ipv4Addr::from(0x0a00_0000));
        let untracked = IpAddr::V4(Ipv4Addr::from(0x0b00_0000));
        let before = allocations();
        for object in 1..=100 {
            observe(&feeds, tracked, &source(object, T0 + SECOND), T0);
            observe(&feeds, untracked, &source(object, T0 + SECOND), T0);
        }
        assert_eq!(allocations() - before, 0, "allocations on the class thread");
        assert_eq!(feeds.untracked(), 103);
    }
}
