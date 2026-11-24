//! The hand-off from a class thread to its tee: one producer, one consumer,
//! no allocation, no lock (stall follow-up to wave 9).
//!
//! The class thread copied every teed datagram into a `Bytes` of its own
//! (an allocation per datagram from its glibc arena), and the tee and the
//! HTTP threads freed those copies back into that arena, so the real-time
//! thread took the arena's lock and grew the arena (an `mprotect` under the
//! process's mmap lock, fresh pages) in the first session: the leading
//! in-process candidate for the 0.3 to 2 s stops of its ingest measured on
//! Linode. The ring is made, faulted in and locked (`memory::pin_region`)
//! before the thread is made real-time. The class thread copies a datagram
//! into the next free bytes and publishes an entry; the tee reads the entry
//! and its bytes in place and releases them when it is done, which is the
//! return of the buffer: two indexes, nothing freed by anyone. A full ring
//! refuses the copy (the class thread counts `tee_dropped`, as it did for a
//! full channel) and the forwarding never waits.
//!
//! Bytes are taken contiguously: a datagram that would cross the end of the
//! byte ring starts again at its beginning, and the bytes skipped are
//! released with the entry.
//!
//! A ring that grows ([`tee_ring_growing`], the video class's 96 MiB) has
//! all its bytes reserved as address space at start, but only the first
//! part faulted in and locked (16 MB at the defaults, what every node
//! pinned for video before the 4K round): a node that carries no video
//! never has the rest resident or locked. The class thread's first push
//! asks for the rest (one atomic, nothing else); the tee's next wait starts
//! a helper thread that faults the rest in and locks it [`GROW_STEP`] bytes
//! at a time, publishing how far it has got after each step (Release); at
//! the end of a lap the class thread takes all that is ready, once it sees
//! it (Acquire) and no byte of the lap before is still held. So the first
//! lap (half a second of 247 Mbit/s) is the only one at the first part's
//! size, however long the whole growth takes; the class thread never
//! touches a page that is not faulted in and locked, never allocates and
//! never waits for the growth. Once the rest is ready the size is taken at
//! the next lap's end: a lap ends either in the larger size or in a refused
//! push, never in a wrap (a wrap would need the lap before released, which
//! is the condition of the larger size).

use futures_util::task::AtomicWaker;
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Bytes of the byte ring for each entry the ring holds.
pub const BYTES_PER_ENTRY: usize = 256;
/// The fewest bytes a byte ring has.
const MIN_BYTES: usize = 64 * 1024;
/// Bytes a growing ring's helper faults in and locks before it publishes
/// them (about 5 ms of faults each).
pub const GROW_STEP: usize = 8 << 20;

/// `Shared::grow`: the ring has all its bytes from the start.
const FIXED: u8 = 0;
/// It may grow; nothing has asked yet.
const IDLE: u8 = 1;
/// The class thread pushed: the tee's next wait starts the growth.
const WANTED: u8 = 2;
/// The helper thread is faulting in and locking the rest.
const GROWING: u8 = 3;
/// The rest is faulted in (and locked, where the limit allows).
const GROWN: u8 = 4;

/// The byte ring's memory: zeroed address space that nobody touches until
/// it is pinned or written.
struct RingBytes {
    ptr: *mut u8,
    len: usize,
    mapped: bool,
}

impl RingBytes {
    fn new(len: usize) -> Self {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: a fresh private anonymous mapping, no file.
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                    -1,
                    0,
                )
            };
            if ptr != libc::MAP_FAILED {
                return Self {
                    ptr: ptr.cast(),
                    len,
                    mapped: true,
                };
            }
        }
        let layout = Self::layout(len);
        // SAFETY: `len` is at least MIN_BYTES, so the layout is not empty.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Self {
            ptr,
            len,
            mapped: false,
        }
    }

    fn layout(len: usize) -> std::alloc::Layout {
        std::alloc::Layout::from_size_align(len, 4096).expect("tee ring bytes fit a layout")
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl Drop for RingBytes {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        if self.mapped {
            // SAFETY: the mapping made in `new`, unmapped once.
            unsafe { libc::munmap(self.ptr.cast(), self.len) };
            return;
        }
        // SAFETY: allocated in `new` with this layout.
        unsafe { std::alloc::dealloc(self.ptr, Self::layout(self.len)) };
    }
}

struct Shared<M> {
    /// The pins, first so that they unlock before the buffers are freed.
    _pins: [crate::memory::Pinned; 2],
    /// The pins of the bytes past `initial`, a step each, as grown.
    grown_pins: Mutex<Vec<crate::memory::Pinned>>,
    /// Bytes faulted in (and locked, where allowed) from the start: the
    /// most the producer may use.
    ready: AtomicUsize,
    entries: Box<[UnsafeCell<Entry<M>>]>,
    bytes: RingBytes,
    /// Bytes faulted in and locked at start; the ring may use only these
    /// until `ready` passes them.
    initial: usize,
    grow: AtomicU8,
    /// Entries published by the producer, and released by the consumer.
    head: AtomicU64,
    tail: AtomicU64,
    /// The byte position up to which the consumer has released.
    released: AtomicU64,
    closed: AtomicBool,
    waker: AtomicWaker,
}

// SAFETY: an entry and its bytes are written only by the producer before it
// publishes them (`head`, Release) and read only by the consumer after it
// sees them (Acquire), until it releases them (`tail`, `released`,
// Release), after which only the producer touches them again (Acquire).
// The bytes past `ready` are touched by the helper thread alone until it
// stores a `ready` past them (Release), and by the producer only after it
// loads that (Acquire).
unsafe impl<M: Send> Send for Shared<M> {}
// SAFETY: as above.
unsafe impl<M: Send> Sync for Shared<M> {}

#[derive(Clone, Copy)]
struct Entry<M> {
    meta: Option<M>,
    /// Byte position (monotonic) of the datagram's first byte, which its
    /// release returns, and where its bytes are in the byte ring.
    start: u64,
    offset: u32,
    len: u32,
}

/// The class thread's end.
pub struct TeeSender<M> {
    shared: Arc<Shared<M>>,
    /// The next free byte position (monotonic), and where it is in the
    /// byte ring.
    at: u64,
    offset: u64,
    /// Bytes of the ring in use: `initial` until the ring grows.
    capacity: u64,
    /// The first push asked for the rest of a ring that grows.
    asked: bool,
    /// Entries pushed since the last wake.
    unwoken: bool,
}

/// The tee's end.
pub struct TeeReceiver<M> {
    shared: Arc<Shared<M>>,
}

/// A ring of `entries` datagrams and `entries * BYTES_PER_ENTRY` bytes,
/// pinned now.
pub fn tee_ring<M: Copy + Send>(entries: usize) -> (TeeSender<M>, TeeReceiver<M>) {
    tee_ring_with_bytes(entries, entries.saturating_mul(BYTES_PER_ENTRY))
}

/// A ring of `entries` datagrams and `bytes` bytes (at least 64 KiB),
/// pinned now: the video class's, whose datagrams are a symbol each
/// (1,316 B and its headers), against the 256 B a datagram that
/// [`tee_ring`] allows.
pub fn tee_ring_with_bytes<M: Copy + Send>(
    entries: usize,
    bytes: usize,
) -> (TeeSender<M>, TeeReceiver<M>) {
    tee_ring_growing(entries, bytes, bytes)
}

/// A ring of `entries` datagrams whose first `initial` bytes are pinned now
/// and which grows to `bytes` bytes (at most 4 GiB) once a datagram is
/// pushed: the video class's, which a node that carries no video never
/// grows (module docs).
pub fn tee_ring_growing<M: Copy + Send>(
    entries: usize,
    initial: usize,
    bytes: usize,
) -> (TeeSender<M>, TeeReceiver<M>) {
    let entries = entries.max(1);
    let byte_len = bytes.max(MIN_BYTES).min(u32::MAX as usize);
    let initial = initial.max(MIN_BYTES).min(byte_len);
    let mut entry_cells: Box<[UnsafeCell<Entry<M>>]> = (0..entries)
        .map(|_| {
            UnsafeCell::new(Entry {
                meta: None,
                start: 0,
                offset: 0,
                len: 0,
            })
        })
        .collect();
    let byte_cells = RingBytes::new(byte_len);
    let pins = [
        // SAFETY: the cells are owned here, valid and not shared yet.
        unsafe {
            crate::memory::pin_region(
                "tee ring entries",
                entry_cells.as_mut_ptr().cast(),
                std::mem::size_of_val(&*entry_cells),
            )
        },
        // SAFETY: as above, for the first `initial` bytes.
        unsafe { crate::memory::pin_region("tee ring bytes", byte_cells.ptr, initial) },
    ];
    let shared = Arc::new(Shared {
        _pins: pins,
        grown_pins: Mutex::new(Vec::new()),
        ready: AtomicUsize::new(initial),
        entries: entry_cells,
        bytes: byte_cells,
        initial,
        grow: AtomicU8::new(if initial < byte_len { IDLE } else { FIXED }),
        head: AtomicU64::new(0),
        tail: AtomicU64::new(0),
        released: AtomicU64::new(0),
        closed: AtomicBool::new(false),
        waker: AtomicWaker::new(),
    });
    (
        TeeSender {
            shared: Arc::clone(&shared),
            at: 0,
            offset: 0,
            capacity: initial as u64,
            asked: false,
            unwoken: false,
        },
        TeeReceiver { shared },
    )
}

impl<M> Shared<M> {
    fn byte_ptr(&self, offset: usize) -> *mut u8 {
        debug_assert!(offset <= self.bytes.len());
        // SAFETY: within the mapping (callers pass an offset of the ring).
        unsafe { self.bytes.ptr.add(offset) }
    }

    fn entry(&self, index: u64) -> &UnsafeCell<Entry<M>> {
        &self.entries[(index % self.entries.len() as u64) as usize]
    }

    /// Fault in and lock the bytes past `initial` a step at a time, saying
    /// how far after each, then that all are. Off the class thread (a
    /// helper thread, or the tee).
    fn grow_now(&self) {
        let started = std::time::Instant::now();
        let mut locked = true;
        let mut end = self.ready.load(Ordering::Relaxed);
        while end < self.bytes.len() {
            end = (end + GROW_STEP).min(self.bytes.len());
            locked &= self.grow_to(end);
        }
        self.grow.store(GROWN, Ordering::Release);
        tracing::info!(
            initial_bytes = self.initial,
            bytes = self.bytes.len(),
            locked,
            took_ms = started.elapsed().as_millis() as u64,
            "tee ring grown: a stream arrived on its class"
        );
    }

    /// Fault in and lock the bytes up to `end`, then publish them
    /// (Release). Whether they are locked.
    fn grow_to(&self, end: usize) -> bool {
        let from = self.ready.load(Ordering::Relaxed);
        if end <= from {
            return true;
        }
        // SAFETY: the bytes from `ready` on belong to the ring, and nobody
        // reads or writes them until `ready` passes them, which is stored
        // after this.
        let pin = unsafe {
            crate::memory::pin_region("tee ring bytes (grown)", self.byte_ptr(from), end - from)
        };
        let locked = pin.is_locked();
        self.grown_pins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(pin);
        self.ready.store(end, Ordering::Release);
        locked
    }
}

impl<M: Copy> TeeSender<M> {
    /// Copy `data` into the ring with `meta`. `false` when the ring has no
    /// room for it (or it is larger than the byte ring): nothing is copied.
    /// Wakes nobody: call [`Self::wake`] once the batch is in.
    pub fn push(&mut self, meta: M, data: &[u8]) -> bool {
        let shared = &*self.shared;
        if !self.asked {
            // A stream on this class: a ring that grows asks for the rest
            // (the tee's next wait starts it).
            self.asked = true;
            let _ = shared
                .grow
                .compare_exchange(IDLE, WANTED, Ordering::Relaxed, Ordering::Relaxed);
        }
        let head = shared.head.load(Ordering::Relaxed);
        if head - shared.tail.load(Ordering::Acquire) >= shared.entries.len() as u64 {
            return false;
        }
        let len = data.len() as u64;
        let mut start = self.at;
        let mut offset = self.offset;
        if offset + len > self.capacity {
            // The end of the lap. A ring whose rest is faulted in takes it
            // here, if no byte of the lap before (all past `offset`) is
            // still held: from the lap's start on, the bytes are those of
            // a ring of the new size.
            let ready = shared.ready.load(Ordering::Acquire) as u64;
            if self.capacity < ready && shared.released.load(Ordering::Acquire) >= start - offset {
                self.capacity = ready;
            }
            if offset + len > self.capacity {
                if len > self.capacity {
                    return false;
                }
                // Contiguous bytes: start again at the beginning.
                start += self.capacity - offset;
                offset = 0;
            }
        }
        if start + len - shared.released.load(Ordering::Acquire) > self.capacity {
            return false;
        }
        // SAFETY: the bytes at `offset` are released by the consumer (the
        // check above), within the part of the ring in use (faulted in and
        // locked), and not yet published: only this end touches them.
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                shared.byte_ptr(offset as usize),
                data.len(),
            )
        };
        // SAFETY: the entry is released (the first check) and unpublished.
        unsafe {
            *shared.entry(head).get() = Entry {
                meta: Some(meta),
                start,
                offset: offset as u32,
                len: data.len() as u32,
            };
        }
        shared.head.store(head + 1, Ordering::Release);
        self.at = start + len;
        self.offset = offset + len;
        self.unwoken = true;
        true
    }

    /// What the ring could take now: free entries, and free bytes of the
    /// part of the ring in use (its first part until it grows). A datagram
    /// that would cross the end skips the bytes left there, so a run of
    /// datagrams finds up to one datagram's bytes fewer.
    pub fn room(&self) -> (u64, u64) {
        let shared = &*self.shared;
        let held = shared.head.load(Ordering::Relaxed) - shared.tail.load(Ordering::Acquire);
        let entries = (shared.entries.len() as u64).saturating_sub(held);
        let bytes = self
            .capacity
            .saturating_sub(self.at - shared.released.load(Ordering::Acquire));
        (entries, bytes)
    }

    /// Bytes of the ring in use now.
    pub const fn capacity(&self) -> u64 {
        self.capacity
    }

    /// The ring is not yet at its full size (its first lap, or the laps
    /// before the rest is ready): a refused push then counts apart.
    pub fn growing(&self) -> bool {
        self.capacity < self.shared.bytes.len() as u64
    }

    /// Wake the tee if anything was pushed since the last wake.
    pub fn wake(&mut self) {
        if std::mem::take(&mut self.unwoken) {
            self.shared.waker.wake();
        }
    }
}

impl<M> Drop for TeeSender<M> {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.waker.wake();
    }
}

/// What [`TeeReceiver::ready`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ready {
    /// An entry is waiting.
    Entry,
    /// The class thread has gone and every entry is released.
    Closed,
}

impl<M: Copy + Send + 'static> TeeReceiver<M> {
    /// The oldest entry not yet released, read in place.
    pub fn peek(&self) -> Option<(M, &[u8])> {
        let shared = &*self.shared;
        let tail = shared.tail.load(Ordering::Relaxed);
        if tail == shared.head.load(Ordering::Acquire) {
            return None;
        }
        // SAFETY: published (Acquire above) and not released: the
        // producer does not write it until `release`.
        let entry = unsafe { *shared.entry(tail).get() };
        // SAFETY: as above, for its bytes, which are contiguous.
        let bytes = unsafe {
            std::slice::from_raw_parts(
                shared.byte_ptr(entry.offset as usize).cast_const(),
                entry.len as usize,
            )
        };
        Some((entry.meta?, bytes))
    }

    /// Release the entry `peek` showed, and its bytes, to the producer.
    pub fn release(&self) {
        let shared = &*self.shared;
        let tail = shared.tail.load(Ordering::Relaxed);
        if tail == shared.head.load(Ordering::Acquire) {
            return;
        }
        // SAFETY: published and not released.
        let entry = unsafe { *shared.entry(tail).get() };
        shared
            .released
            .store(entry.start + u64::from(entry.len), Ordering::Release);
        shared.tail.store(tail + 1, Ordering::Release);
    }

    /// Wait for an entry, or for the end.
    pub fn ready(&self) -> ReadyFuture<'_, M> {
        ReadyFuture { receiver: self }
    }

    /// Bytes of the ring now faulted in (and locked, where allowed): its
    /// first part, or all of them once grown. (The node shows it in
    /// `/api/mesh/local` `memory.locked_bytes`, and logs the growth.)
    #[cfg(test)]
    pub fn pinned_bytes(&self) -> usize {
        self.shared.ready.load(Ordering::Acquire)
    }

    /// The class thread asked for the rest of the ring: fault it in on a
    /// helper thread (on this one if none can be made).
    fn grow_if_asked(&self) {
        let shared = &self.shared;
        if shared.grow.load(Ordering::Relaxed) != WANTED
            || shared
                .grow
                .compare_exchange(WANTED, GROWING, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        let helper = Arc::clone(shared);
        let spawned = std::thread::Builder::new()
            .name("needletail-tee-grow".into())
            .spawn(move || helper.grow_now());
        if let Err(error) = spawned {
            tracing::warn!(error = %error, "no thread to grow the tee ring: grown on the tee");
            shared.grow_now();
        }
    }

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Ready> {
        self.grow_if_asked();
        let check = || {
            let shared = &*self.shared;
            let tail = shared.tail.load(Ordering::Relaxed);
            if tail != shared.head.load(Ordering::Acquire) {
                return Some(Ready::Entry);
            }
            if shared.closed.load(Ordering::Acquire) {
                // The producer's last push precedes its close.
                return Some(if tail != shared.head.load(Ordering::Acquire) {
                    Ready::Entry
                } else {
                    Ready::Closed
                });
            }
            None
        };
        if let Some(ready) = check() {
            return Poll::Ready(ready);
        }
        self.shared.waker.register(cx.waker());
        match check() {
            Some(ready) => Poll::Ready(ready),
            None => Poll::Pending,
        }
    }
}

/// [`TeeReceiver::ready`].
pub struct ReadyFuture<'a, M> {
    receiver: &'a TeeReceiver<M>,
}

impl<M: Copy + Send + 'static> Future for ReadyFuture<'_, M> {
    type Output = Ready;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Ready> {
        self.receiver.poll_ready(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn entries_come_out_in_order_with_their_bytes() {
        let (mut tx, rx) = tee_ring::<u32>(4);
        assert!(rx.peek().is_none());
        for index in 0..4_u32 {
            assert!(tx.push(index, &[index as u8; 3]));
        }
        assert!(!tx.push(9, b"full"), "four entries at most");
        for index in 0..4_u32 {
            let (meta, bytes) = rx.peek().unwrap();
            assert_eq!((meta, bytes), (index, &[index as u8; 3][..]));
            rx.release();
        }
        assert!(rx.peek().is_none());
        assert!(tx.push(5, b"again"));
        assert_eq!(rx.peek().unwrap(), (5, &b"again"[..]));
    }

    /// The video ring is bounded by its bytes: 4K symbols of 1,400 B fill
    /// a ring of 256 B an entry after a fifth of its entries.
    #[test]
    fn a_ring_with_bytes_of_its_own_holds_that_many_symbols() {
        let symbol = [7_u8; 1_400];
        let count = |(mut tx, _rx): (TeeSender<u32>, TeeReceiver<u32>)| {
            let mut pushed = 0_u32;
            while tx.push(pushed, &symbol) {
                pushed += 1;
            }
            pushed
        };
        assert_eq!(count(tee_ring::<u32>(1_000)), 182, "256,000 B / 1,400 B");
        assert_eq!(count(tee_ring_with_bytes::<u32>(1_000, 1_000 * 1_400)), 1_000);
        assert_eq!(count(tee_ring_with_bytes::<u32>(10, 1_000 * 1_400)), 10, "entries bound");
    }

    /// Pages of `ptr..ptr + len` that are resident (mincore).
    #[cfg(target_os = "linux")]
    fn resident_pages(ptr: *mut u8, len: usize) -> usize {
        let pages = len.div_ceil(4096);
        let mut vec = vec![0_u8; pages];
        // SAFETY: a page-aligned range of the ring's own mapping.
        let result = unsafe { libc::mincore(ptr.cast(), len, vec.as_mut_ptr().cast()) };
        assert_eq!(result, 0, "mincore");
        vec.iter().filter(|page| **page & 1 == 1).count()
    }

    /// A ring that grows (the video class's) has only its first part
    /// resident until a datagram is pushed and the tee waits; then the
    /// class thread takes the rest at the end of a lap, and every datagram
    /// comes out whole and in order across the change.
    #[test]
    fn a_growing_ring_touches_its_rest_only_once_a_datagram_comes() {
        const INITIAL: usize = 4 << 20;
        const FULL: usize = 16 << 20;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let (mut tx, rx) = tee_ring_growing::<u64>(65_536, INITIAL, FULL);
        assert_eq!(rx.pinned_bytes(), INITIAL);
        assert_eq!(rx.shared.grow.load(Ordering::Relaxed), IDLE);
        // Past the first part, leaving a huge page's room for the kernel's
        // transparent huge page at its end.
        #[cfg(target_os = "linux")]
        {
            let rest = rx.shared.byte_ptr(INITIAL + (2 << 20));
            assert_eq!(resident_pages(rest, FULL - INITIAL - (2 << 20)), 0, "untouched");
        }
        // A tee that waits with nothing pushed starts nothing.
        let nothing = runtime.block_on(async {
            tokio::time::timeout(Duration::from_millis(20), rx.ready()).await
        });
        assert!(nothing.is_err());
        assert_eq!(rx.shared.grow.load(Ordering::Relaxed), IDLE);

        let datagram = |index: u64| vec![(index % 251) as u8; 1_316];
        let mut next_out = 0_u64;
        let drain = |rx: &TeeReceiver<u64>, next_out: &mut u64| {
            while let Some((meta, bytes)) = rx.peek() {
                assert_eq!(meta, *next_out);
                assert_eq!(bytes, &datagram(meta)[..]);
                rx.release();
                *next_out += 1;
            }
        };
        assert!(tx.push(0, &datagram(0)));
        assert_eq!(rx.shared.grow.load(Ordering::Relaxed), WANTED);
        assert_eq!(runtime.block_on(rx.ready()), Ready::Entry);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while rx.shared.grow.load(Ordering::Acquire) != GROWN {
            assert!(std::time::Instant::now() < deadline, "grown");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(rx.pinned_bytes(), FULL);
        #[cfg(target_os = "linux")]
        {
            let rest = rx.shared.byte_ptr(INITIAL);
            assert_eq!(
                resident_pages(rest, FULL - INITIAL),
                (FULL - INITIAL) / 4096,
                "faulted in"
            );
        }
        // Still the first part until the end of its lap; then all of it.
        let mut index = 1_u64;
        while tx.offset + 1_316 <= INITIAL as u64 {
            assert!(tx.push(index, &datagram(index)));
            assert_eq!(tx.capacity, INITIAL as u64);
            index += 1;
            if index % 512 == 0 {
                drain(&rx, &mut next_out);
            }
        }
        drain(&rx, &mut next_out);
        assert!(tx.push(index, &datagram(index)));
        index += 1;
        assert_eq!(tx.capacity, FULL as u64, "taken at the end of the lap");
        // Many laps of the whole ring, held up to its bytes.
        for _ in 0..40_000 {
            while !tx.push(index, &datagram(index)) {
                drain(&rx, &mut next_out);
            }
            assert!(tx.at - rx.shared.released.load(Ordering::Relaxed) <= FULL as u64);
            index += 1;
        }
        drain(&rx, &mut next_out);
        assert_eq!(next_out, index);
    }

    /// A lap that ends while a datagram of the lap before is still held
    /// (the tee a lap behind) does not grow: its rest would land on that
    /// datagram. The class thread wraps as before and grows at the end of
    /// a lap once the tee has caught up.
    #[test]
    fn a_ring_grows_only_at_a_lap_with_nothing_held_from_the_one_before() {
        const SMALL: usize = 64 * 1024;
        let (mut tx, rx) = tee_ring_growing::<u64>(1_024, SMALL, 4 * SMALL);
        let mut index = 0_u64;
        let mut push = |tx: &mut TeeSender<u64>, len: usize| {
            index += 1;
            tx.push(index, &vec![1; len])
        };
        // Lap 0: 65 datagrams of 1,000 bytes, then one of 500 at 65,000.
        for _ in 0..65 {
            assert!(push(&mut tx, 1_000));
        }
        assert!(push(&mut tx, 500));
        for _ in 0..65 {
            rx.release();
        }
        // Lap 1 starts before the rest is faulted in.
        assert!(push(&mut tx, 1_000));
        assert_eq!(tx.at, SMALL as u64 + 1_000, "wrapped");
        rx.grow_if_asked();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while rx.shared.grow.load(Ordering::Acquire) != GROWN {
            assert!(std::time::Instant::now() < deadline, "grown");
            std::thread::sleep(Duration::from_millis(1));
        }
        // Lap 1 up to the held 500 bytes of lap 0.
        for _ in 0..64 {
            assert!(push(&mut tx, 1_000));
        }
        assert!(!push(&mut tx, 1_000), "the lap before is held: no growth");
        assert_eq!(tx.capacity, SMALL as u64);
        // The tee takes lap 0's last datagram and lap 1's first.
        rx.release();
        rx.release();
        assert!(push(&mut tx, 1_000));
        assert_eq!(tx.capacity, 4 * SMALL as u64, "grown at the lap's end");
        assert_eq!(tx.offset, SMALL as u64 - 536 + 1_000, "on past the old end");
    }

    /// The helper publishes the rest a step at a time: a lap that ends with
    /// part of it ready takes that part (fix4-video), and the next lap's
    /// end the rest.
    #[test]
    fn a_lap_takes_what_is_ready_of_the_rest() {
        const SMALL: usize = 64 * 1024;
        let (mut tx, rx) = tee_ring_growing::<u64>(1_024, SMALL, 4 * SMALL);
        let mut index = 0_u64;
        let mut push = |tx: &mut TeeSender<u64>| {
            index += 1;
            let pushed = tx.push(index, &[1; 1_000]);
            rx.release();
            pushed
        };
        assert!(tx.growing());
        // Half of the rest ready during the first lap.
        rx.shared.grow_to(SMALL + SMALL / 2);
        assert_eq!(rx.pinned_bytes(), SMALL + SMALL / 2);
        while tx.capacity() == SMALL as u64 {
            assert!(push(&mut tx));
        }
        assert_eq!(tx.capacity(), (SMALL + SMALL / 2) as u64, "what was ready");
        assert!(tx.growing());
        rx.shared.grow_to(4 * SMALL);
        while tx.capacity() == (SMALL + SMALL / 2) as u64 {
            assert!(push(&mut tx));
        }
        assert_eq!(tx.capacity(), 4 * SMALL as u64);
        assert!(!tx.growing());
        // A growth asked for later finds nothing left to do.
        rx.shared.grow_now();
        assert_eq!(rx.pinned_bytes(), 4 * SMALL);
    }

    #[test]
    fn a_datagram_never_crosses_the_end_and_bytes_are_returned_in_order() {
        // 300 entries: 76,800 bytes. 1,000-byte datagrams wrap many times.
        let (mut tx, rx) = tee_ring::<u64>(300);
        let capacity = rx.shared.bytes.len();
        let mut next_out = 0_u64;
        for index in 0..2_000_u64 {
            let data = vec![(index % 251) as u8; 1_000];
            while !tx.push(index, &data) {
                // Full of bytes: the consumer returns the oldest.
                let (meta, bytes) = rx.peek().unwrap();
                assert_eq!(meta, next_out);
                assert!(bytes.iter().all(|byte| *byte == (meta % 251) as u8));
                rx.release();
                next_out += 1;
            }
            let held = tx.at - rx.shared.released.load(Ordering::Relaxed);
            assert!(held <= capacity as u64);
        }
        assert!(
            next_out > 1_000,
            "the bytes, not the entries, bound the ring"
        );
        assert!(!tx.push(0, &vec![0; capacity + 1]), "larger than the ring");
    }

    /// The class thread's cost per teed datagram, the ring's push against
    /// the copy and channel it replaced (`Bytes::copy_from_slice` and a
    /// bounded tokio channel's `try_send`), with a consumer on another
    /// thread taking each item and freeing it, as the tee did.
    #[test]
    #[ignore = "a measurement: run it alone, in release"]
    fn a_push_costs_the_class_thread_less_than_the_copy_and_channel() {
        const ITEMS: u32 = 1_000_000;
        let datagram = [7_u8; 300];
        let (mut tx, rx) = tee_ring::<u64>(65_536);
        let consumer = std::thread::spawn(move || {
            let mut taken = 0;
            while taken < ITEMS {
                if rx.peek().is_some() {
                    rx.release();
                    taken += 1;
                }
            }
        });
        let started = std::time::Instant::now();
        let mut pushed = 0;
        while pushed < ITEMS {
            if tx.push(u64::from(pushed), &datagram) {
                pushed += 1;
            }
        }
        let ring = started.elapsed() / ITEMS;
        consumer.join().unwrap();

        let (channel_tx, mut channel_rx) =
            tokio::sync::mpsc::channel::<(u64, bytes::Bytes)>(65_536);
        let consumer = std::thread::spawn(move || {
            let mut taken = 0;
            while taken < ITEMS {
                if let Ok(item) = channel_rx.try_recv() {
                    drop(item);
                    taken += 1;
                }
            }
        });
        let started = std::time::Instant::now();
        let mut sent = 0;
        while sent < ITEMS {
            if channel_tx
                .try_send((u64::from(sent), bytes::Bytes::copy_from_slice(&datagram)))
                .is_ok()
            {
                sent += 1;
            }
        }
        let channel = started.elapsed() / ITEMS;
        consumer.join().unwrap();
        eprintln!("per 300-byte datagram: ring push {ring:?}, Bytes copy + channel {channel:?}");
    }

    #[test]
    fn the_tee_wakes_for_an_entry_and_ends_when_the_thread_goes() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (mut tx, rx) = tee_ring::<u32>(64);
        let producer = std::thread::spawn(move || {
            for index in 0..1_000_u32 {
                while !tx.push(index, &index.to_le_bytes()) {
                    tx.wake();
                    std::thread::yield_now();
                }
                if index % 7 == 0 {
                    tx.wake();
                }
            }
            tx.wake();
        });
        let received = runtime.block_on(async {
            let mut received = Vec::new();
            loop {
                match rx.ready().await {
                    Ready::Closed => break,
                    Ready::Entry => {
                        while let Some((meta, bytes)) = rx.peek() {
                            assert_eq!(bytes, meta.to_le_bytes());
                            received.push(meta);
                            rx.release();
                        }
                    }
                }
            }
            received
        });
        producer.join().unwrap();
        assert_eq!(received, (0..1_000).collect::<Vec<_>>());
    }
}
