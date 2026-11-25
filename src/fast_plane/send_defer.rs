//! Datagrams that a full socket refused, kept by the class thread and sent
//! as soon as the socket takes them again (send240, A).
//!
//! cmpd8 (25 Nov): an edge's public NIC stopped taking datagrams for 1 to 3
//! s. The fast lane met `WouldBlock` sending a 4K symbol to a client and
//! handed the client's whole part to the cache-fed lane, a tokio task that
//! sends whole parts again: the stall became holds of 3 to 5.7 s and 11
//! parts lost. A forwarder's class thread dropped what its children's
//! sockets refused (4k.md fix 5), leaving it to their hole repair.
//!
//! Now the class thread keeps what a socket refused, per destination (a
//! client of the edge, a child of the forwarder), in order, and sends it
//! before anything newer for that destination as soon as the socket takes
//! datagrams again. It is bounded twice:
//! - in time: a part's datagrams share one deadline, set when the first of
//!   them is kept (`--send-defer-ms`, 200 ms by default). What is still
//!   unsent then is given up: at the edge, the cache-fed lane is told which
//!   symbols of the part the client lacks, and sends those (never the
//!   whole part again); at a forwarder the child's own repair covers it.
//! - in bytes: one pool of fixed slots per class thread, made and touched
//!   before the thread runs (`--send-defer-mb`), a destination taking at
//!   most a share of it. A destination whose share is full loses what it
//!   cannot keep; no other destination does.
//!
//! The class thread allocates, locks and logs nothing here: the pool, the
//! queues and their lists are fixed, the queues are linked lists through
//! the slots. On the normal path (no datagram kept) a send costs one test
//! of a counter.

use super::sys;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Bytes of one slot of the pool: the largest datagram a class thread sends
/// (an RLS1 datagram is at most 1,472 bytes, a rewrapped RQD2 less).
pub const SLOT_BYTES: usize = 1_536;
/// Default of `--send-defer-ms`.
pub const DEFAULT_SEND_DEFER_MS: u64 = 200;
/// The pool of a class thread made without the node's budget (tests, a
/// default configuration): 8 MiB.
pub const DEFAULT_POOL_BYTES: usize = 8 << 20;
/// `--send-defer-mb auto`: this share of the node's memory (permille), and
/// its bounds: 31 MB on a 3.9 GB g6-dedicated-2, about 200 ms of 4K at 247
/// Mbit/s to five destinations at once.
pub const AUTO_POOL_PERMILLE: u64 = 8;
pub const AUTO_POOL_MIN_BYTES: u64 = 8 << 20;
pub const AUTO_POOL_MAX_BYTES: u64 = 64 << 20;

/// The pool of `--send-defer-mb`: `mb`, or `auto` (`None`) from the
/// node's memory.
pub fn pool_bytes(mb: Option<u64>, node_memory_bytes: u64) -> usize {
    let bytes = match mb {
        Some(mb) => mb.saturating_mul(1 << 20),
        None => (node_memory_bytes / 1_000 * AUTO_POOL_PERMILLE)
            .clamp(AUTO_POOL_MIN_BYTES, AUTO_POOL_MAX_BYTES),
    };
    usize::try_from(bytes).unwrap_or(usize::MAX)
}
/// Destinations with datagrams kept at once, at most.
pub const QUEUES: usize = 256;
/// A destination keeps at most this share of the pool (permille).
pub const QUEUE_SHARE_PERMILLE: u64 = 500;
/// Datagrams one drain hands the kernel at most: a few milliseconds of the
/// class thread, then its sockets again.
const DRAIN_MAX: usize = 4_096;
/// Datagrams of one destination gathered for one `sendmmsg`.
const GATHER: usize = 64;
const NONE: u32 = u32::MAX;
/// After a socket was found full, the kept datagrams are tried again this
/// much later at the soonest.
const RETRY_FULL_NS: u64 = 250_000;

/// The bound of a pool: its time, its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeferConfig {
    /// A part's datagrams are kept at most this long (ns).
    pub bound_ns: u64,
    /// The pool's bytes (a whole number of [`SLOT_BYTES`] slots).
    pub pool_bytes: usize,
}

impl DeferConfig {
    /// `ms` of keeping (0: off) in a pool of `pool_bytes`.
    pub fn new(ms: u64, pool_bytes: usize) -> Option<Self> {
        (ms > 0 && pool_bytes >= SLOT_BYTES).then(|| Self {
            bound_ns: ms.saturating_mul(1_000_000),
            pool_bytes,
        })
    }

    pub fn slots(self) -> usize {
        self.pool_bytes / SLOT_BYTES
    }
}

/// What a kept datagram carries, for its hand-off when it expires.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tag {
    pub stream: u64,
    pub part: u64,
    pub esi: u32,
    /// The part's source symbols (K).
    pub k: u16,
    /// The repair the destination's policy asks of a block of K.
    pub repair: u16,
    pub source: bool,
}

impl Tag {
    /// A datagram with a deadline of its own (a forwarder's: its child's
    /// repair covers what is given up, so no part is handed off).
    pub const UNTAGGED: Self = Self {
        stream: u64::MAX,
        part: 0,
        esi: 0,
        k: 0,
        repair: 0,
        source: false,
    };

    fn untagged(&self) -> bool {
        self.stream == u64::MAX
    }
}

/// A part whose kept datagrams expired (or found no room) for a
/// destination: the source symbols from `from_esi` on are not sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Expired {
    pub target: SocketAddr,
    pub stream: u64,
    pub part: u64,
    pub from_esi: u32,
    pub k: u16,
    pub repair: u16,
    /// The RQD2 geometry of the part (from its datagram's header), 0 when
    /// the datagram carried none.
    pub transfer_length: u32,
    pub symbol_size: u16,
    /// Datagrams of the part given up.
    pub datagrams: u32,
    /// When the first of them was kept (CLOCK_MONOTONIC ns).
    pub kept_ns: u64,
}

#[derive(Clone, Copy)]
struct Entry {
    len: u16,
    next: u32,
    tag: Tag,
    deadline_ns: u64,
}

#[derive(Clone, Copy)]
struct Queue {
    fd: RawFd,
    target: SocketAddr,
    head: u32,
    tail: u32,
    count: u32,
    bytes: u64,
    /// The part of the newest kept entry, and its deadline: a part's
    /// entries share the deadline of the first of them.
    part: (u64, u64),
    part_deadline_ns: u64,
}

const EMPTY_QUEUE: Queue = Queue {
    fd: -1,
    target: SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0)),
    head: NONE,
    tail: NONE,
    count: 0,
    bytes: 0,
    part: (u64::MAX, u64::MAX),
    part_deadline_ns: 0,
};

/// The counters of a pool (`/api/node`).
#[derive(Debug, Default)]
pub struct DeferStats {
    /// Sends that found the socket full (a `WouldBlock`, once per send
    /// call).
    pub would_block: AtomicU64,
    /// Datagrams kept, and of them those sent later.
    pub deferred: AtomicU64,
    pub deferred_sent: AtomicU64,
    /// Datagrams given up at their part's deadline, and the parts.
    pub expired: AtomicU64,
    pub expired_parts: AtomicU64,
    /// Datagrams that found no room (their destination's share or the pool
    /// full): lost for that destination only.
    pub refused: AtomicU64,
    /// Datagrams dropped by an error of their send.
    pub errors: AtomicU64,
    /// Bytes kept now and the most kept at once.
    pub kept_bytes: AtomicU64,
    pub high_water_bytes: AtomicU64,
    /// The pool's bytes, and its bound (ms).
    pub pool_bytes: AtomicU64,
    pub bound_ms: AtomicU64,
}

/// [`DeferStats`] as `/api/node` shows it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DeferSnapshot {
    pub would_block: u64,
    pub deferred: u64,
    pub deferred_sent: u64,
    pub expired: u64,
    pub expired_parts: u64,
    pub refused: u64,
    pub errors: u64,
    pub kept_bytes: u64,
    pub high_water_bytes: u64,
    pub pool_bytes: u64,
    pub bound_ms: u64,
    /// Edge only: parts handed to the cache-fed lane with the symbols the
    /// client lacks, those symbols, and hand-offs the lane's ring refused.
    pub handed_parts: u64,
    pub handed_symbols: u64,
    pub handed_dropped: u64,
}

impl DeferStats {
    pub fn snapshot(&self) -> DeferSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        DeferSnapshot {
            would_block: load(&self.would_block),
            deferred: load(&self.deferred),
            deferred_sent: load(&self.deferred_sent),
            expired: load(&self.expired),
            expired_parts: load(&self.expired_parts),
            refused: load(&self.refused),
            errors: load(&self.errors),
            kept_bytes: load(&self.kept_bytes),
            high_water_bytes: load(&self.high_water_bytes),
            pool_bytes: load(&self.pool_bytes),
            bound_ms: load(&self.bound_ms),
            ..DeferSnapshot::default()
        }
    }
}

/// The pools' bytes of the whole process: the memory guard's account
/// (`/api/node` `memory.send_defer_bytes`).
use av_mesh::mem_guard::SEND_DEFER_BYTES as POOL_BYTES;

/// One class thread's kept datagrams.
pub struct SendDefer {
    config: DeferConfig,
    data: Box<[u8]>,
    entries: Box<[Entry]>,
    free: Box<[u32]>,
    free_len: usize,
    queues: Box<[Queue]>,
    /// Queues in use, by index into `queues`; `spare` the unused ones.
    active: Box<[u16]>,
    active_len: usize,
    spare: Box<[u16]>,
    spare_len: usize,
    /// The queue the next drain starts with.
    turn: usize,
    /// A drain found a socket full: the next does not try before this
    /// (CLOCK_MONOTONIC ns), so a stalled socket costs a few failed sends a
    /// millisecond, not one at every batch.
    retry_after_ns: u64,
    queue_cap_bytes: u64,
    kept_bytes: u64,
    gathered: Box<[u32]>,
    runs: Vec<(usize, usize)>,
    pub stats: Arc<DeferStats>,
}

impl std::fmt::Debug for SendDefer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SendDefer")
            .field("config", &self.config)
            .field("active", &self.active_len)
            .field("kept_bytes", &self.kept_bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for SendDefer {
    fn drop(&mut self) {
        POOL_BYTES.fetch_sub(self.data.len() as u64, Ordering::Relaxed);
    }
}

/// The RQD2 header fields a hand-off needs, read in place.
fn geometry(datagram: &[u8]) -> (u32, u16) {
    if datagram.len() < raptorq_datagram_fec::HEADER_LEN
        || datagram[..4] != raptorq_datagram_fec::DATAGRAM_MAGIC
    {
        return (0, 0);
    }
    let transfer_length = u32::from_le_bytes([datagram[12], datagram[13], datagram[14], datagram[15]]);
    let symbol_size = u16::from_le_bytes([datagram[22], datagram[23]]);
    (transfer_length, symbol_size)
}

impl SendDefer {
    /// A pool, made and touched here (by the thread that makes it, before
    /// the class thread runs).
    pub fn new(config: DeferConfig) -> Self {
        let slots = config.slots().clamp(1, NONE as usize - 1);
        let mut data = vec![0_u8; slots * SLOT_BYTES].into_boxed_slice();
        crate::memory::touch_owned(&mut *data);
        let empty = Entry {
            len: 0,
            next: NONE,
            tag: Tag::default(),
            deadline_ns: 0,
        };
        let mut entries = vec![empty; slots].into_boxed_slice();
        crate::memory::touch_owned(&mut *entries);
        let mut free: Box<[u32]> = (0..slots as u32).rev().collect();
        crate::memory::touch_owned(&mut *free);
        let mut queues = vec![EMPTY_QUEUE; QUEUES].into_boxed_slice();
        crate::memory::touch_owned(&mut *queues);
        let mut active = vec![0_u16; QUEUES].into_boxed_slice();
        crate::memory::touch_owned(&mut *active);
        let mut spare: Box<[u16]> = (0..QUEUES as u16).rev().collect();
        crate::memory::touch_owned(&mut *spare);
        let mut gathered = vec![0_u32; GATHER].into_boxed_slice();
        crate::memory::touch_owned(&mut *gathered);
        let mut runs = Vec::with_capacity(GATHER);
        crate::memory::touch_spare(&mut runs);
        let stats = Arc::new(DeferStats::default());
        stats
            .pool_bytes
            .store(data.len() as u64, Ordering::Relaxed);
        stats
            .bound_ms
            .store(config.bound_ns / 1_000_000, Ordering::Relaxed);
        POOL_BYTES.fetch_add(data.len() as u64, Ordering::Relaxed);
        Self {
            config,
            queue_cap_bytes: (data.len() as u64 * QUEUE_SHARE_PERMILLE / 1_000)
                .max(SLOT_BYTES as u64),
            data,
            entries,
            free_len: free.len(),
            free,
            queues,
            active,
            active_len: 0,
            spare_len: QUEUES,
            spare,
            turn: 0,
            retry_after_ns: 0,
            kept_bytes: 0,
            gathered,
            runs,
            stats,
        }
    }

    /// Whether any datagram is kept: the class thread then drains at its
    /// next pass, and its wait is short.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.active_len == 0
    }

    pub fn kept_bytes(&self) -> u64 {
        self.kept_bytes
    }

    /// The queue of `(fd, target)`, if it holds datagrams.
    #[inline]
    fn find(&self, fd: RawFd, target: SocketAddr) -> Option<usize> {
        self.active[..self.active_len]
            .iter()
            .map(|index| usize::from(*index))
            .find(|index| {
                let queue = &self.queues[*index];
                queue.fd == fd && queue.target == target
            })
    }

    /// Whether `target` has datagrams kept on `fd`: a new datagram for it
    /// goes behind them ([`Self::keep`]), not to the socket.
    #[inline]
    pub fn is_waiting(&self, fd: RawFd, target: SocketAddr) -> bool {
        self.active_len > 0 && self.find(fd, target).is_some()
    }

    /// Keep, in order, each of `sends` whose destination has datagrams kept
    /// (or, in a test, whose path is stalled), and take it out of `sends`:
    /// a destination's datagrams stay in order. `at` gives a send's socket,
    /// destination and bytes. In place; nothing to do while nothing is kept.
    pub fn divert<'a, S>(
        &mut self,
        sends: &mut Vec<S>,
        now_ns: u64,
        at: impl Fn(&S) -> (RawFd, SocketAddr, &'a [u8]),
    ) {
        if self.is_empty() && !any_blocked() {
            return;
        }
        sends.retain(|send| {
            let (fd, target, datagram) = at(send);
            if !(self.is_waiting(fd, target) || blocked(fd, target)) {
                return true;
            }
            self.keep(fd, target, datagram, Tag::UNTAGGED, now_ns, |_| {});
            false
        });
    }

    /// Count a send that found its socket full.
    pub fn note_would_block(&self) {
        self.stats.would_block.fetch_add(1, Ordering::Relaxed);
    }

    /// Keep `datagram` for `target` on `fd`, behind what it has kept. When
    /// there is no room for it (the destination's share, or the pool, is
    /// full), it is lost for this destination alone and `refused` is told
    /// of it (a source symbol, for its hand-off). Returns whether it was
    /// kept.
    pub fn keep(
        &mut self,
        fd: RawFd,
        target: SocketAddr,
        datagram: &[u8],
        tag: Tag,
        now_ns: u64,
        refused: impl FnOnce(Expired),
    ) -> bool {
        let queue_index = match self.find(fd, target) {
            Some(index) => Some(index),
            None if self.spare_len > 0 => {
                self.spare_len -= 1;
                let index = usize::from(self.spare[self.spare_len]);
                self.queues[index] = Queue {
                    fd,
                    target,
                    ..EMPTY_QUEUE
                };
                self.active[self.active_len] = index as u16;
                self.active_len += 1;
                Some(index)
            }
            None => None,
        };
        let room = queue_index.is_some_and(|index| {
            self.queues[index].bytes + SLOT_BYTES as u64 <= self.queue_cap_bytes
        }) && self.free_len > 0
            && datagram.len() <= SLOT_BYTES;
        let Some(index) = queue_index.filter(|_| room) else {
            self.stats.refused.fetch_add(1, Ordering::Relaxed);
            if let Some(index) = queue_index {
                if self.queues[index].count == 0 {
                    self.release_queue(index);
                }
            }
            if tag.source {
                let (transfer_length, symbol_size) = geometry(datagram);
                refused(Expired {
                    target,
                    stream: tag.stream,
                    part: tag.part,
                    from_esi: tag.esi,
                    k: tag.k,
                    repair: tag.repair,
                    transfer_length,
                    symbol_size,
                    datagrams: 1,
                    kept_ns: now_ns,
                });
            }
            return false;
        };
        self.free_len -= 1;
        let slot = self.free[self.free_len];
        let at = slot as usize * SLOT_BYTES;
        self.data[at..at + datagram.len()].copy_from_slice(datagram);
        let queue = &mut self.queues[index];
        let key = (tag.stream, tag.part);
        if queue.part != key || tag.untagged() {
            queue.part = key;
            queue.part_deadline_ns = now_ns.saturating_add(self.config.bound_ns);
        }
        self.entries[slot as usize] = Entry {
            len: datagram.len() as u16,
            next: NONE,
            tag,
            deadline_ns: queue.part_deadline_ns,
        };
        if queue.tail == NONE {
            queue.head = slot;
        } else {
            self.entries[queue.tail as usize].next = slot;
        }
        queue.tail = slot;
        queue.count += 1;
        queue.bytes += SLOT_BYTES as u64;
        self.kept_bytes += SLOT_BYTES as u64;
        self.stats.deferred.fetch_add(1, Ordering::Relaxed);
        self.stats
            .kept_bytes
            .store(self.kept_bytes, Ordering::Relaxed);
        self.stats
            .high_water_bytes
            .fetch_max(self.kept_bytes, Ordering::Relaxed);
        true
    }

    /// The front entry of queue `index` leaves (sent, expired or failed).
    fn pop_front(&mut self, index: usize) {
        let queue = &mut self.queues[index];
        let slot = queue.head;
        debug_assert_ne!(slot, NONE);
        queue.head = self.entries[slot as usize].next;
        if queue.head == NONE {
            queue.tail = NONE;
        }
        queue.count -= 1;
        queue.bytes -= SLOT_BYTES as u64;
        self.kept_bytes -= SLOT_BYTES as u64;
        self.free[self.free_len] = slot;
        self.free_len += 1;
    }

    /// An empty queue goes back to the spares.
    fn release_queue(&mut self, index: usize) {
        if let Some(at) = self.active[..self.active_len]
            .iter()
            .position(|queue| usize::from(*queue) == index)
        {
            self.active_len -= 1;
            self.active[at] = self.active[self.active_len];
            self.queues[index] = EMPTY_QUEUE;
            self.spare[self.spare_len] = index as u16;
            self.spare_len += 1;
        }
    }

    /// Give up every part whose deadline has passed: each part of each
    /// destination is told to `expired` once, with the first source symbol
    /// it lacks.
    pub fn expire(&mut self, now_ns: u64, mut expired: impl FnMut(Expired)) {
        let mut position = 0;
        while position < self.active_len {
            let index = usize::from(self.active[position]);
            let mut current: Option<Expired> = None;
            loop {
                let head = self.queues[index].head;
                if head == NONE {
                    break;
                }
                let entry = self.entries[head as usize];
                let same_part = !entry.tag.untagged()
                    && current.as_ref().is_some_and(|run| {
                        (run.stream, run.part) == (entry.tag.stream, entry.tag.part)
                    });
                if entry.deadline_ns > now_ns && !same_part {
                    break;
                }
                if !same_part {
                    if let Some(run) = current.take() {
                        self.stats.expired_parts.fetch_add(1, Ordering::Relaxed);
                        expired(run);
                    }
                }
                let at = head as usize * SLOT_BYTES;
                let (transfer_length, symbol_size) =
                    geometry(&self.data[at..at + usize::from(entry.len)]);
                let run = current.get_or_insert(Expired {
                    target: self.queues[index].target,
                    stream: entry.tag.stream,
                    part: entry.tag.part,
                    from_esi: u32::MAX,
                    k: entry.tag.k,
                    repair: entry.tag.repair,
                    transfer_length,
                    symbol_size,
                    datagrams: 0,
                    kept_ns: entry.deadline_ns.saturating_sub(self.config.bound_ns),
                });
                run.datagrams += 1;
                if entry.tag.source {
                    run.from_esi = run.from_esi.min(entry.tag.esi);
                }
                self.stats.expired.fetch_add(1, Ordering::Relaxed);
                self.pop_front(index);
            }
            if let Some(run) = current.take() {
                self.stats.expired_parts.fetch_add(1, Ordering::Relaxed);
                expired(run);
            }
            if self.queues[index].count == 0 {
                self.release_queue(index);
                // The last queue took this place: look at it next.
                continue;
            }
            position += 1;
        }
        self.stats
            .kept_bytes
            .store(self.kept_bytes, Ordering::Relaxed);
    }

    /// Expire what is past its deadline, then send what the sockets take,
    /// each destination in turn, a [`GATHER`] of its datagrams at a time in
    /// order (as UDP GSO messages with `gso`), until a socket is full or
    /// all is sent. Returns the datagrams sent and their bytes.
    pub fn drain(&mut self, now_ns: u64, gso: bool, expired: impl FnMut(Expired)) -> (usize, u64) {
        self.expire(now_ns, expired);
        if self.active_len == 0 || now_ns < self.retry_after_ns {
            return (0, 0);
        }
        // Sockets found full in this drain (a class thread sends from a few).
        let mut full: [RawFd; 8] = [-1; 8];
        let mut full_len = 0;
        let mut sent_total = 0;
        let mut bytes_total = 0;
        loop {
            let mut progressed = false;
            let rounds = self.active_len;
            let mut position = self.turn % rounds.max(1);
            for _ in 0..rounds {
                if position >= self.active_len {
                    position = 0;
                }
                let index = usize::from(self.active[position]);
                let fd = self.queues[index].fd;
                if full[..full_len].contains(&fd) || blocked(fd, self.queues[index].target) {
                    position += 1;
                    continue;
                }
                let (taken, bytes, socket_full) = self.send_front(index, gso);
                sent_total += taken;
                bytes_total += bytes;
                progressed |= taken > 0;
                if socket_full && full_len < full.len() {
                    full[full_len] = fd;
                    full_len += 1;
                }
                if self.queues[index].count == 0 {
                    self.release_queue(index);
                    // The last queue took this place.
                } else {
                    position += 1;
                }
                if sent_total >= DRAIN_MAX {
                    break;
                }
            }
            self.turn = self.turn.wrapping_add(1);
            if !progressed || sent_total >= DRAIN_MAX || self.active_len == 0 {
                break;
            }
        }
        if full_len > 0 {
            self.retry_after_ns = now_ns.saturating_add(RETRY_FULL_NS);
        }
        self.stats
            .kept_bytes
            .store(self.kept_bytes, Ordering::Relaxed);
        (sent_total, bytes_total)
    }

    /// Send the front of queue `index` (up to [`GATHER`]): the datagrams
    /// the kernel took, their bytes, and whether the socket was full.
    fn send_front(&mut self, index: usize, gso: bool) -> (usize, u64, bool) {
        let queue = self.queues[index];
        let mut count = 0;
        let mut slot = queue.head;
        while slot != NONE && count < GATHER {
            self.gathered[count] = slot;
            count += 1;
            slot = self.entries[slot as usize].next;
        }
        let (data, entries, gathered) = (&self.data, &self.entries, &self.gathered);
        let message = |position: usize| {
            let slot = gathered[position] as usize;
            let at = slot * SLOT_BYTES;
            (
                &data[at..at + usize::from(entries[slot].len)],
                queue.target,
            )
        };
        let mut taken = if gso {
            match sys::send_gso_batch_with(queue.fd, &mut self.runs, count, message) {
                Ok(sent) => sent.datagrams,
                Err(error) if sys::gso_unsupported(&error) => {
                    sys::send_batch(queue.fd, count, message).unwrap_or(0)
                }
                Err(_) => 0,
            }
        } else {
            sys::send_batch(queue.fd, count, message).unwrap_or(0)
        };
        let mut socket_full = false;
        let bytes_of = |taken: usize| -> u64 {
            gathered[..taken]
                .iter()
                .map(|slot| u64::from(entries[*slot as usize].len))
                .sum()
        };
        if taken < count {
            // Full, or this one datagram failed (dropped, so that it does
            // not stop the rest): one more try tells which.
            match sys::send_batch(queue.fd, 1, |_| message(taken)) {
                Ok(1) => taken += 1,
                Err(error) if error.kind() != std::io::ErrorKind::WouldBlock => {
                    let bytes = bytes_of(taken);
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    for _ in 0..=taken {
                        self.pop_front(index);
                    }
                    self.stats
                        .deferred_sent
                        .fetch_add(taken as u64, Ordering::Relaxed);
                    return (taken, bytes, false);
                }
                _ => socket_full = true,
            }
        }
        let bytes = bytes_of(taken);
        for _ in 0..taken {
            self.pop_front(index);
        }
        if taken > 0 {
            self.stats
                .deferred_sent
                .fetch_add(taken as u64, Ordering::Relaxed);
        }
        (taken, bytes, socket_full)
    }
}

/// Tests: a stall of one destination's path (or of every destination's),
/// set by a test: sends to it are refused as a full socket refuses them.
#[cfg(test)]
pub mod stall {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The stalled sockets (the fast lane's and the cache-fed lane's: a
    /// host's NIC stalls both), the destination's port on them (0: every
    /// destination), and the CLOCK_MONOTONIC ns until which they are
    /// stalled (0: none).
    static FDS: [AtomicU64; 2] = [AtomicU64::new(u64::MAX), AtomicU64::new(u64::MAX)];
    static PORT: AtomicU64 = AtomicU64::new(0);
    static UNTIL_NS: AtomicU64 = AtomicU64::new(0);
    /// Tests that stall a path hold this, one at a time.
    pub static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Stall `port` (0: every destination) of sockets `fds` (one or two)
    /// for `ms` from now.
    pub fn set(fds: &[std::os::fd::RawFd], port: u16, ms: u64) {
        for (slot, stalled) in FDS.iter().enumerate() {
            let value = fds.get(slot).or(fds.first()).map_or(u64::MAX, |fd| *fd as u64);
            stalled.store(value, Ordering::Relaxed);
        }
        PORT.store(u64::from(port), Ordering::Relaxed);
        UNTIL_NS.store(
            super::sys::monotonic_ns() + ms * 1_000_000,
            Ordering::Release,
        );
    }

    /// Stall `port` of whichever socket sends to it (a forwarder's child),
    /// for `ms` from now.
    pub fn set_port(port: u16, ms: u64) {
        for stalled in &FDS {
            stalled.store(ANY_FD, Ordering::Relaxed);
        }
        PORT.store(u64::from(port), Ordering::Relaxed);
        UNTIL_NS.store(
            super::sys::monotonic_ns() + ms * 1_000_000,
            Ordering::Release,
        );
    }

    const ANY_FD: u64 = u64::MAX - 1;

    pub fn clear() {
        UNTIL_NS.store(0, Ordering::Release);
    }

    #[inline]
    pub fn active() -> bool {
        UNTIL_NS.load(Ordering::Relaxed) != 0
    }

    #[inline]
    pub fn blocks(fd: std::os::fd::RawFd, target: SocketAddr) -> bool {
        let until = UNTIL_NS.load(Ordering::Acquire);
        if until == 0
            || !FDS.iter().any(|slot| {
                let stalled = slot.load(Ordering::Relaxed);
                stalled == fd as u64 || stalled == ANY_FD
            })
        {
            return false;
        }
        let port = PORT.load(Ordering::Relaxed);
        (port == 0 || port == u64::from(target.port())) && super::sys::monotonic_ns() < until
    }
}

/// Whether a test may be stalling a path (one load in a test build; false
/// otherwise).
#[inline]
pub fn any_blocked() -> bool {
    #[cfg(test)]
    {
        stall::active()
    }
    #[cfg(not(test))]
    {
        false
    }
}

/// Whether a test stalls `target`'s path from socket `fd` now.
#[inline]
pub fn blocked(fd: RawFd, target: SocketAddr) -> bool {
    #[cfg(test)]
    {
        stall::blocks(fd, target)
    }
    #[cfg(not(test))]
    {
        let _ = (fd, target);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket;
    use std::os::fd::AsRawFd;

    fn tag(part: u64, esi: u32) -> Tag {
        Tag {
            stream: 7,
            part,
            esi,
            k: 4,
            repair: 1,
            source: esi < 4,
        }
    }

    fn pair() -> (UdpSocket, UdpSocket) {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver.set_nonblocking(true).unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        sender.set_nonblocking(true).unwrap();
        (sender, receiver)
    }

    fn received(receiver: &UdpSocket) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut buf = [0_u8; 2_048];
        while let Ok(len) = receiver.recv(&mut buf) {
            out.push(buf[..len].to_vec());
        }
        out
    }

    /// Kept datagrams leave in order, before nothing else of theirs, once
    /// the destination takes them; they expire a part at a time with the
    /// first source symbol not sent.
    #[test]
    fn kept_datagrams_leave_in_order_and_expire_by_part() {
        let (sender, receiver) = pair();
        let target = receiver.local_addr().unwrap();
        let fd = sender.as_raw_fd();
        let mut defer = SendDefer::new(DeferConfig::new(100, 64 * SLOT_BYTES).unwrap());
        assert!(defer.is_empty());
        let now = 1_000_000_000;
        for esi in 0..6_u32 {
            assert!(defer.keep(fd, target, &[esi as u8; 100], tag(1, esi), now, |_| {}));
        }
        assert!(defer.is_waiting(fd, target));
        assert_eq!(defer.kept_bytes(), 6 * SLOT_BYTES as u64);
        // A later part keeps a deadline of its own.
        defer.keep(fd, target, &[9; 100], tag(2, 0), now + 50_000_000, |_| {});
        let _lock = stall::LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        stall::set(&[fd], target.port(), 1_000);
        assert_eq!(defer.drain(now, false, |_| panic!("nothing expired")).0, 0);
        stall::clear();
        assert_eq!(defer.drain(now, false, |_| panic!("nothing expired")), (7, 700));
        let got = received(&receiver);
        assert_eq!(got.len(), 7);
        assert_eq!(
            got.iter().map(|datagram| datagram[0]).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5, 9]
        );
        assert!(defer.is_empty());
        let snapshot = defer.stats.snapshot();
        assert_eq!((snapshot.deferred, snapshot.deferred_sent), (7, 7));

        // Part 3 expires whole once its first datagram's deadline passes:
        // its entry kept later goes with it, and part 4 stays.
        for esi in 2..6_u32 {
            defer.keep(fd, target, &[esi as u8; 100], tag(3, esi), now, |_| {});
        }
        defer.keep(fd, target, &[1; 100], tag(4, 1), now + 90_000_000, |_| {});
        let mut expired = Vec::new();
        defer.expire(now + 100_000_001, |run| expired.push(run));
        assert_eq!(expired.len(), 1);
        assert_eq!((expired[0].part, expired[0].from_esi, expired[0].datagrams), (3, 2, 4));
        assert!(defer.is_waiting(fd, target));
        defer.expire(now + 200_000_000, |run| expired.push(run));
        assert_eq!(expired.len(), 2);
        assert!(defer.is_empty());
    }

    /// A destination whose share is full loses what it cannot keep, and
    /// only it: another destination keeps its datagrams.
    #[test]
    fn a_full_destination_loses_alone() {
        let (sender, receiver) = pair();
        let (_, other) = pair();
        let (a, b) = (receiver.local_addr().unwrap(), other.local_addr().unwrap());
        let fd = sender.as_raw_fd();
        // A pool of 8 slots: a destination keeps at most 4.
        let mut defer = SendDefer::new(DeferConfig::new(100, 8 * SLOT_BYTES).unwrap());
        let mut refused = Vec::new();
        let source = |esi| Tag {
            k: 8,
            source: true,
            ..tag(1, esi)
        };
        for esi in 0..6 {
            defer.keep(fd, a, &[1; 10], source(esi), 0, |run| refused.push(run));
        }
        assert_eq!(refused.len(), 2, "two of six refused: {refused:?}");
        assert_eq!(refused[0].from_esi, 4);
        for esi in 0..4 {
            assert!(defer.keep(fd, b, &[2; 10], tag(1, esi), 0, |_| panic!("b keeps")));
        }
        assert_eq!(defer.stats.snapshot().refused, 2);
        assert_eq!(defer.drain(0, true, |_| {}).0, 8);
    }
}
