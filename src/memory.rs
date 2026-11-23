//! Memory at start-up (wave 9, cold start): the first session after a
//! deploy pays for memory that every later session finds ready. Each first
//! touch of a page is a fault (a 2 MB huge page is zeroed in it), and
//! glibc's allocator hands memory back to the kernel when a heap's top
//! grows past its trim threshold and takes it again with fresh faults.
//!
//! - `lock_all`: mlockall(MCL_CURRENT | MCL_FUTURE) where the process may
//!   lock without a limit (RLIMIT_MEMLOCK infinite, or root): every mapped
//!   page is faulted in now and every later mapping when it is made, so no
//!   thread (the real-time class threads included) takes a fault for a page
//!   of its own. Off by default (`--mlock-all`, wave 9 review: its memory
//!   cost is unmeasured); a deploy that asks for it gives the service
//!   `LimitMEMLOCK=infinity`. With
//!   a finite limit (the namespace lab's 8 MB) it is not tried: MCL_FUTURE
//!   would make allocations past the limit fail.
//! - `tune_allocator`: a fixed trim threshold and mmap threshold, so that
//!   freed memory stays in the heaps for the next allocation instead of
//!   going back to the kernel and coming back as page faults.
//! - `faults`: the process's minor and major faults (getrusage), and those
//!   of the calling thread, for /api/stats.

use std::sync::atomic::{AtomicBool, Ordering};

/// What the start-up did, for /api/stats.
static LOCKED: AtomicBool = AtomicBool::new(false);
static TUNED: AtomicBool = AtomicBool::new(false);

/// The heap trim threshold set by `tune_allocator`: a heap's top is given
/// back to the kernel only past this.
pub const TRIM_THRESHOLD: i32 = 256 << 20;
/// Allocations from this size on are mmapped (glibc's largest: 32 MB on
/// 64-bit), not the default 128 KB that rises only as such blocks are
/// freed.
pub const MMAP_THRESHOLD: i32 = 32 << 20;

/// Lock every page of the process now and from now on, where it may do so
/// without a limit. Returns why it did not.
pub fn lock_all(enabled: bool) -> Result<(), String> {
    if !enabled {
        return Err("--mlock-all false".into());
    }
    #[cfg(target_os = "linux")]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes the struct it is given.
        if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) } != 0 {
            return Err(format!(
                "getrlimit(RLIMIT_MEMLOCK): {}",
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: geteuid has no preconditions.
        let root = unsafe { libc::geteuid() } == 0;
        if limit.rlim_cur != libc::RLIM_INFINITY && !root {
            return Err(format!(
                "RLIMIT_MEMLOCK is {} bytes (LimitMEMLOCK=infinity lets the service lock its memory)",
                limit.rlim_cur
            ));
        }
        // SAFETY: mlockall takes flags only.
        if unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) } != 0 {
            return Err(format!("mlockall: {}", std::io::Error::last_os_error()));
        }
        LOCKED.store(true, Ordering::Relaxed);
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("mlockall is Linux only".into())
    }
}

/// Keep freed memory in the allocator's heaps (see the module comment).
pub fn tune_allocator(enabled: bool) -> Result<(), String> {
    if !enabled {
        return Err("--malloc-tune false".into());
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: mallopt takes plain integers; it fails by returning 0.
        let trim = unsafe { libc::mallopt(libc::M_TRIM_THRESHOLD, TRIM_THRESHOLD) };
        // SAFETY: as above.
        let mmap = unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD) };
        if trim == 0 || mmap == 0 {
            return Err(format!("mallopt refused (trim {trim}, mmap {mmap})"));
        }
        TUNED.store(true, Ordering::Relaxed);
        Ok(())
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        Err("mallopt is glibc only".into())
    }
}

/// Whether `lock_all` locked the process's memory.
pub fn locked() -> bool {
    LOCKED.load(Ordering::Relaxed)
}

/// Whether `tune_allocator` set the thresholds.
pub fn tuned() -> bool {
    TUNED.load(Ordering::Relaxed)
}

/// Minor and major page faults: of the process (`thread` false) or of the
/// calling thread.
pub fn faults(thread: bool) -> (u64, u64) {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: a zeroed rusage is valid; getrusage writes it.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        let who = if thread {
            libc::RUSAGE_THREAD
        } else {
            libc::RUSAGE_SELF
        };
        // SAFETY: as above.
        if unsafe { libc::getrusage(who, &mut usage) } != 0 {
            return (0, 0);
        }
        (
            u64::try_from(usage.ru_minflt).unwrap_or(0),
            u64::try_from(usage.ru_majflt).unwrap_or(0),
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = thread;
        (0, 0)
    }
}

/// Write a zero to the first byte of every page of `bytes` bytes at `ptr`,
/// so that their faults are taken now. The memory is the caller's scratch
/// space: spare capacity not yet initialized (a `Vec`'s), whose bytes it
/// does not need. Nothing is read (reading uninitialized memory, even with
/// `read_volatile`, is undefined behaviour; wave 9 review).
///
/// # Safety
/// `ptr..ptr + bytes` must be valid for writes, and the caller must not
/// need the bytes it holds.
pub unsafe fn prefault(ptr: *mut u8, bytes: usize) {
    const PAGE: usize = 4096;
    let mut offset = 0;
    while offset < bytes {
        // SAFETY: within the range the caller vouches for.
        unsafe {
            std::ptr::write_volatile(ptr.add(offset), 0);
        }
        offset += PAGE;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn faults_count_a_first_touch_and_prefault_takes_them_early() {
        let bytes = 8 << 20;
        let mut block: Vec<u8> = Vec::with_capacity(bytes);
        let (before, _) = faults(true);
        // SAFETY: the vector's capacity is owned and writable.
        unsafe { prefault(block.as_mut_ptr(), bytes) };
        let (after, _) = faults(true);
        // 2,048 pages of 4 KB, or a few huge pages: at least one fault.
        assert!(after > before, "no fault counted ({before} -> {after})");
        let (again, _) = faults(true);
        // SAFETY: as above.
        unsafe { prefault(block.as_mut_ptr(), bytes) };
        let (last, _) = faults(true);
        assert!(
            last - again <= 2,
            "a prefaulted block faulted again: {} faults",
            last - again
        );
        block.clear();
    }

    #[test]
    fn locking_is_refused_without_an_unlimited_memlock() {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes the struct it is given.
        unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) };
        // SAFETY: geteuid has no preconditions.
        let may = limit.rlim_cur == libc::RLIM_INFINITY || unsafe { libc::geteuid() } == 0;
        if !may {
            let refused = lock_all(true).unwrap_err();
            assert!(refused.contains("RLIMIT_MEMLOCK"), "{refused}");
            assert!(!locked());
        }
        assert_eq!(lock_all(false).unwrap_err(), "--mlock-all false");
    }
}
