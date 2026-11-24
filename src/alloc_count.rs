//! Tests: the test binary's allocator, which is the system's (as the
//! node's is) and counts allocations and frees per thread, so that a test
//! can show that a path allocates nothing: the calling thread's own count
//! (`allocations`), and those of a thread that a test watches from
//! another (`AllocCounter`, which a class thread installs once it runs,
//! with the minor faults it takes from then).
//! Set `NEEDLETAIL_ALLOC_TRACE=1` to print the backtrace of each watched
//! thread's allocation or free.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// What a watched thread allocated and freed, and the minor faults it
/// took, from when it installed the counter.
#[derive(Debug, Default)]
pub struct AllocCounter {
    pub allocs: AtomicU64,
    pub frees: AtomicU64,
    /// A thread installed the counter (a test that read zeros from a
    /// counter nobody installed would prove nothing).
    pub installed: AtomicBool,
    /// The passes of its loop that it reported (`pass_done`).
    pub passes: AtomicU64,
    /// Its minor faults (getrusage RUSAGE_THREAD) when it installed the
    /// counter, and at its last reported pass.
    pub minflt_start: AtomicU64,
    pub minflt_now: AtomicU64,
}

impl AllocCounter {
    /// Minor faults the thread took between installing the counter and its
    /// last reported pass.
    pub fn faults(&self) -> u64 {
        self.minflt_now
            .load(Ordering::Relaxed)
            .saturating_sub(self.minflt_start.load(Ordering::Relaxed))
    }
}

impl AllocCounter {
    /// A counter that lives as long as the test binary.
    pub fn leaked() -> &'static Self {
        Box::leak(Box::default())
    }
}

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static WATCHED: Cell<Option<&'static AllocCounter>> = const { Cell::new(None) };
    /// Inside `note` (its trace allocates): nothing more is counted.
    static NOTING: Cell<bool> = const { Cell::new(false) };
}

/// Allocations (reallocations among them) of the calling thread so far.
pub fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

/// Count the calling thread's allocations, frees and minor faults in
/// `counter` from now.
pub fn watch_this_thread(counter: Option<&'static AllocCounter>) {
    let Some(counter) = counter else {
        return;
    };
    let (minor, _) = crate::memory::faults(true);
    counter.minflt_start.store(minor, Ordering::Relaxed);
    counter.minflt_now.store(minor, Ordering::Relaxed);
    counter.installed.store(true, Ordering::Release);
    WATCHED.with(|watched| watched.set(Some(counter)));
}

/// A watched thread's loop finished a pass: count it, and note the
/// thread's minor faults now.
pub fn pass_done() {
    let Some(counter) = WATCHED.with(Cell::get) else {
        return;
    };
    let (minor, _) = crate::memory::faults(true);
    counter.minflt_now.store(minor, Ordering::Relaxed);
    counter.passes.fetch_add(1, Ordering::Relaxed);
}

fn note(free: bool) {
    if NOTING
        .try_with(|noting| noting.replace(true))
        .unwrap_or(true)
    {
        return;
    }
    if !free {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
    }
    let _ = WATCHED.try_with(|watched| {
        let Some(counter) = watched.get() else {
            return;
        };
        if free {
            counter.frees.fetch_add(1, Ordering::Relaxed);
        } else {
            counter.allocs.fetch_add(1, Ordering::Relaxed);
        }
        if std::env::var_os("NEEDLETAIL_ALLOC_TRACE").is_some() {
            eprintln!(
                "watched thread {}:\n{}",
                if free { "free" } else { "allocation" },
                std::backtrace::Backtrace::force_capture()
            );
        }
    });
    let _ = NOTING.try_with(|noting| noting.set(false));
}

struct Counting;

// SAFETY: every call is passed on to the system allocator unchanged.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(false);
        // SAFETY: as the caller's.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(false);
        // SAFETY: as the caller's.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        note(true);
        // SAFETY: as the caller's.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(false);
        // SAFETY: as the caller's.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;
