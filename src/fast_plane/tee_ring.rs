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

use futures_util::task::AtomicWaker;
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

/// Bytes of the byte ring for each entry the ring holds.
pub const BYTES_PER_ENTRY: usize = 256;

struct Shared<M> {
    /// The pins, first so that they unlock before the buffers are freed.
    _pins: [crate::memory::Pinned; 2],
    entries: Box<[UnsafeCell<Entry<M>>]>,
    bytes: Box<[UnsafeCell<u8>]>,
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
unsafe impl<M: Send> Send for Shared<M> {}
// SAFETY: as above.
unsafe impl<M: Send> Sync for Shared<M> {}

#[derive(Clone, Copy)]
struct Entry<M> {
    meta: Option<M>,
    /// Byte position (monotonic) of the datagram's first byte, and its
    /// length.
    start: u64,
    len: u32,
}

/// The class thread's end.
pub struct TeeSender<M> {
    shared: Arc<Shared<M>>,
    /// The next free byte position (monotonic).
    at: u64,
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
    let entries = entries.max(1);
    let byte_len = bytes.max(64 * 1024);
    let mut entry_cells: Box<[UnsafeCell<Entry<M>>]> = (0..entries)
        .map(|_| {
            UnsafeCell::new(Entry {
                meta: None,
                start: 0,
                len: 0,
            })
        })
        .collect();
    let mut byte_cells: Box<[UnsafeCell<u8>]> = (0..byte_len).map(|_| UnsafeCell::new(0)).collect();
    let pins = [
        // SAFETY: the cells are owned here, valid and not shared yet.
        unsafe {
            crate::memory::pin_region(
                "tee ring entries",
                entry_cells.as_mut_ptr().cast(),
                std::mem::size_of_val(&*entry_cells),
            )
        },
        // SAFETY: as above.
        unsafe {
            crate::memory::pin_region("tee ring bytes", byte_cells.as_mut_ptr().cast(), byte_len)
        },
    ];
    let shared = Arc::new(Shared {
        _pins: pins,
        entries: entry_cells,
        bytes: byte_cells,
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
            unwoken: false,
        },
        TeeReceiver { shared },
    )
}

impl<M> Shared<M> {
    fn byte_ptr(&self, position: u64) -> *mut u8 {
        let offset = (position % self.bytes.len() as u64) as usize;
        self.bytes[offset].get()
    }

    fn entry(&self, index: u64) -> &UnsafeCell<Entry<M>> {
        &self.entries[(index % self.entries.len() as u64) as usize]
    }
}

impl<M: Copy> TeeSender<M> {
    /// Copy `data` into the ring with `meta`. `false` when the ring has no
    /// room for it (or it is larger than the byte ring): nothing is copied.
    /// Wakes nobody: call [`Self::wake`] once the batch is in.
    pub fn push(&mut self, meta: M, data: &[u8]) -> bool {
        let shared = &*self.shared;
        let head = shared.head.load(Ordering::Relaxed);
        if head - shared.tail.load(Ordering::Acquire) >= shared.entries.len() as u64 {
            return false;
        }
        let capacity = shared.bytes.len() as u64;
        let len = data.len() as u64;
        if len > capacity {
            return false;
        }
        let mut start = self.at;
        let offset = start % capacity;
        if offset + len > capacity {
            // Contiguous bytes: start again at the beginning.
            start += capacity - offset;
        }
        if start + len - shared.released.load(Ordering::Acquire) > capacity {
            return false;
        }
        // SAFETY: the bytes from `start` are released by the consumer (the
        // check above) and not yet published: only this end touches them.
        unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), shared.byte_ptr(start), data.len()) };
        // SAFETY: the entry is released (the first check) and unpublished.
        unsafe {
            *shared.entry(head).get() = Entry {
                meta: Some(meta),
                start,
                len: data.len() as u32,
            };
        }
        shared.head.store(head + 1, Ordering::Release);
        self.at = start + len;
        self.unwoken = true;
        true
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

impl<M: Copy> TeeReceiver<M> {
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
                shared.byte_ptr(entry.start).cast_const(),
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

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Ready> {
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

impl<M: Copy> Future for ReadyFuture<'_, M> {
    type Output = Ready;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Ready> {
        self.receiver.poll_ready(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
