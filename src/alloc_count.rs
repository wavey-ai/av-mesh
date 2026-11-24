//! Tests: the test binary's allocator, which is the system's (as the
//! node's is) and counts allocations and frees per thread, so that a test
//! can show that a path allocates nothing: the calling thread's own count
//! (`allocations`), and those of a thread that a test watches from
//! another (`AllocCounter`, which a class thread installs once it runs).
//! Set `NEEDLETAIL_ALLOC_TRACE=1` to print the backtrace of each watched
//! thread's allocation or free.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

/// What a watched thread allocated and freed.
#[derive(Debug, Default)]
pub struct AllocCounter {
    pub allocs: AtomicU64,
    pub frees: AtomicU64,
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

/// Count the calling thread's allocations and frees in `counter` from now.
pub fn watch_this_thread(counter: Option<&'static AllocCounter>) {
    if counter.is_some() {
        WATCHED.with(|watched| watched.set(counter));
    }
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
