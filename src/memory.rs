//! Memory at start-up (wave 9, cold start; D 2.3): the first session after a
//! deploy pays for memory that every later session finds ready. Each first
//! touch of a page is a fault (a 2 MB huge page is zeroed in it), and
//! glibc's allocator hands memory back to the kernel when a heap's top
//! grows past its trim threshold and takes it again with fresh faults.
//!
//! - `pin_region`: the owner of a long-lived buffer that a class thread or
//!   a journal writes (the hop and stall journals' rings, the first-arrival
//!   tables, the receive batches, the tee rings) faults its pages in at
//!   construction (`--prefault`) and locks them (`--mlock rings`,
//!   `mlock(ptr, len)`), before any class thread runs. A refused lock (a
//!   memlock limit below the region) is counted and logged once per region
//!   and changes nothing else: the region stays faulted in and the node
//!   runs. This replaces `mlockall` (`--mlock-all`, removed): under the
//!   unit's finite LimitMEMLOCK it could not lock the process, and with an
//!   unlimited one it would hold every mapping resident (D's review 8).
//!   Not covered: the cache's parts, which fill on the tee's thread.
//! - `tune_allocator`: a fixed trim threshold and mmap threshold, so that
//!   freed memory stays in the heaps for the next allocation instead of
//!   going back to the kernel and coming back as page faults.
//! - `faults`: the process's minor and major faults (getrusage), and those
//!   of the calling thread, for /api/stats.
//! - `report`: what the above did, with the process's virtual, resident and
//!   locked sizes, for `/api/mesh/local` `memory`.

use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;

static TUNED: AtomicBool = AtomicBool::new(false);
/// `--mlock`, as `LockMode as u8`.
static MODE: AtomicU8 = AtomicU8::new(LockMode::Rings as u8);
/// `--prefault`.
static PREFAULT: AtomicBool = AtomicBool::new(true);
/// Bytes that `pin_region` faulted in, since start.
static PREFAULTED: AtomicU64 = AtomicU64::new(0);
/// Bytes locked by `pin_region` now (a region's lock ends with it).
static LOCKED: AtomicU64 = AtomicU64::new(0);
/// Bytes whose lock the kernel refused, since start.
static REFUSED: AtomicU64 = AtomicU64::new(0);
/// The regions whose refusal was logged already.
static REFUSALS_LOGGED: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

/// The heap trim threshold set by `tune_allocator`: a heap's top is given
/// back to the kernel only past this.
pub const TRIM_THRESHOLD: i32 = 256 << 20;
/// Allocations from this size on are mmapped (glibc's largest: 32 MB on
/// 64-bit), not the default 128 KB that rises only as such blocks are
/// freed.
pub const MMAP_THRESHOLD: i32 = 32 << 20;
/// The page that a prefault writes one byte of.
const PAGE: usize = 4096;

/// `--mlock`: what `pin_region` locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LockMode {
    /// Nothing is locked; regions are still prefaulted (with `--prefault`).
    Off = 0,
    /// Every pinned region is locked, where the memlock limit allows.
    Rings = 1,
}

/// Set `--mlock` and `--prefault`, before any region is made.
pub fn configure(mode: LockMode, prefault: bool) {
    MODE.store(mode as u8, Ordering::Relaxed);
    PREFAULT.store(prefault, Ordering::Relaxed);
}

/// `--mlock` as configured.
pub fn mode() -> LockMode {
    if MODE.load(Ordering::Relaxed) == LockMode::Off as u8 {
        LockMode::Off
    } else {
        LockMode::Rings
    }
}

/// A region that `pin_region` faulted in and perhaps locked. Its lock ends
/// when it is dropped: an owner keeps it as its first field, so that it
/// drops (and unlocks) before the buffer it covers is freed. Locks do not
/// nest and a heap region need not start or end on a page, so a region
/// shares its first and last pages with its neighbours: a drop unlocks only
/// the pages wholly inside it, and never a live neighbour's page.
#[derive(Debug)]
pub struct Pinned {
    ptr: usize,
    len: usize,
    locked: bool,
}

impl Pinned {
    /// Whether the region is locked in memory.
    pub fn is_locked(&self) -> bool {
        self.locked
    }

    /// A region of nothing (an owner with no buffer).
    pub const fn none() -> Self {
        Self {
            ptr: 0,
            len: 0,
            locked: false,
        }
    }
}

impl Drop for Pinned {
    fn drop(&mut self) {
        if !self.locked {
            return;
        }
        #[cfg(target_os = "linux")]
        {
            let start = self.ptr.next_multiple_of(PAGE);
            let end = (self.ptr + self.len) / PAGE * PAGE;
            if end > start {
                // SAFETY: whole pages inside the range that `pin_region`
                // locked, whose owner has not freed it yet (see above).
                unsafe { libc::munlock(start as *const libc::c_void, end - start) };
            }
        }
        LOCKED.fetch_sub(self.len as u64, Ordering::Relaxed);
    }
}

/// Fault in `len` bytes at `ptr` (with `--prefault`) and lock them (with
/// `--mlock rings`). Called by the owner of a long-lived buffer when it
/// makes it, before any class thread uses it. A refused lock is counted in
/// `lock_refused_bytes`, logged once for `name`, and changes nothing else.
///
/// # Safety
/// `ptr..ptr + len` must be valid for reads and writes for as long as the
/// returned value lives, initialized or not, and no other thread may write
/// it during the call (each page's first byte is written back with the
/// value it holds).
pub unsafe fn pin_region(name: &'static str, ptr: *mut u8, len: usize) -> Pinned {
    if len == 0 {
        return Pinned::none();
    }
    if PREFAULT.load(Ordering::Relaxed) {
        // SAFETY: the caller vouches for the range.
        unsafe { prefault(ptr, len) };
        PREFAULTED.fetch_add(len as u64, Ordering::Relaxed);
    }
    let locked = mode() == LockMode::Rings && lock(ptr, len);
    if locked {
        LOCKED.fetch_add(len as u64, Ordering::Relaxed);
    } else if mode() == LockMode::Rings {
        REFUSED.fetch_add(len as u64, Ordering::Relaxed);
        let mut logged = REFUSALS_LOGGED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !logged.contains(&name) {
            logged.push(name);
            tracing::info!(
                region = name,
                bytes = len,
                memlock_limit = memlock_limit(),
                "memory region not locked (the memlock limit is below it): it stays faulted in"
            );
        }
    }
    Pinned {
        ptr: ptr as usize,
        len,
        locked,
    }
}

/// [`pin_region`] over a slice the caller owns.
pub fn pin_slice<T: Copy>(name: &'static str, slice: &mut [T]) -> Pinned {
    // SAFETY: the slice's bytes are valid and exclusively borrowed.
    unsafe {
        pin_region(
            name,
            slice.as_mut_ptr().cast(),
            std::mem::size_of_val(slice),
        )
    }
}

fn lock(ptr: *mut u8, len: usize) -> bool {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: the caller of `pin_region` vouches for the range.
        unsafe { libc::mlock(ptr as *const libc::c_void, len) == 0 }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (ptr, len);
        false
    }
}

/// RLIMIT_MEMLOCK of the process (`None`: unlimited or unknown).
fn memlock_limit() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes the struct it is given.
        if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) } != 0
            || limit.rlim_cur == libc::RLIM_INFINITY
        {
            return None;
        }
        Some(limit.rlim_cur)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
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

/// Whether the pinned regions are locked: some bytes are, and no lock was
/// refused (`FastPlaneSnapshot.memory_locked`).
pub fn locked() -> bool {
    LOCKED.load(Ordering::Relaxed) > 0 && REFUSED.load(Ordering::Relaxed) == 0
}

/// Whether `tune_allocator` set the thresholds.
pub fn tuned() -> bool {
    TUNED.load(Ordering::Relaxed)
}

/// `/api/mesh/local` `memory`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MemoryReport {
    pub mode: LockMode,
    pub prefaulted_bytes: u64,
    pub locked_bytes: u64,
    pub lock_refused_bytes: u64,
    pub vm_size_kb: u64,
    pub vm_rss_kb: u64,
    pub vm_lck_kb: u64,
    /// The system's transparent huge page setting (`always`, `madvise`,
    /// `never`; empty when unknown).
    pub thp: String,
    pub allocator_tuned: bool,
}

/// What the regions and the allocator are now.
pub fn report() -> MemoryReport {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let thp =
        std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled").unwrap_or_default();
    MemoryReport {
        mode: mode(),
        prefaulted_bytes: PREFAULTED.load(Ordering::Relaxed),
        locked_bytes: LOCKED.load(Ordering::Relaxed),
        lock_refused_bytes: REFUSED.load(Ordering::Relaxed),
        vm_size_kb: status_kb(&status, "VmSize:"),
        vm_rss_kb: status_kb(&status, "VmRSS:"),
        vm_lck_kb: status_kb(&status, "VmLck:"),
        thp: thp_setting(&thp).to_owned(),
        allocator_tuned: tuned(),
    }
}

/// A `kB` line of `/proc/self/status`; 0 when absent.
fn status_kb(status: &str, key: &str) -> u64 {
    status
        .lines()
        .find_map(|line| {
            let rest = line.strip_prefix(key)?;
            rest.trim().trim_end_matches("kB").trim().parse().ok()
        })
        .unwrap_or(0)
}

/// The bracketed word of `always [madvise] never`.
fn thp_setting(text: &str) -> &str {
    text.split_whitespace()
        .find_map(|word| word.strip_prefix('[')?.strip_suffix(']'))
        .unwrap_or("")
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

/// Write the first byte of every page of `bytes` bytes at `ptr` back with
/// the value it holds, so that their faults are taken now. The byte is
/// moved as `MaybeUninit<u8>`: the memory may be uninitialized (a `Vec`'s
/// spare capacity) without the read being undefined behaviour, and
/// initialized memory keeps its contents.
///
/// # Safety
/// `ptr..ptr + bytes` must be valid for reads and writes, and no other
/// thread may write it during the call.
pub unsafe fn prefault(ptr: *mut u8, bytes: usize) {
    let ptr = ptr.cast::<std::mem::MaybeUninit<u8>>();
    let mut offset = 0;
    while offset < bytes {
        // SAFETY: within the range the caller vouches for; a MaybeUninit
        // may hold any byte, or none.
        unsafe {
            let byte = std::ptr::read_volatile(ptr.add(offset));
            std::ptr::write_volatile(ptr.add(offset), byte);
        }
        offset += PAGE;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh mapping of `len` bytes that no allocator has touched.
    struct Fresh(*mut u8, usize);

    impl Fresh {
        fn new(len: usize) -> Self {
            // SAFETY: an anonymous private mapping, checked below.
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                )
            };
            assert_ne!(ptr, libc::MAP_FAILED);
            // No huge page: each 4 KB page is a fault of its own.
            // SAFETY: the mapping is ours.
            unsafe { libc::madvise(ptr, len, libc::MADV_NOHUGEPAGE) };
            Self(ptr.cast(), len)
        }
    }

    impl Drop for Fresh {
        fn drop(&mut self) {
            // SAFETY: the mapping is ours.
            unsafe { libc::munmap(self.0.cast(), self.1) };
        }
    }

    #[test]
    fn faults_count_a_first_touch_and_prefault_takes_them_early() {
        // A mapping of its own, not the allocator's: a block freed by an
        // earlier test on this thread can come back already touched
        // (integration 1's order-dependent failure).
        let bytes = 8 << 20;
        let block = Fresh::new(bytes);
        let (before, _) = faults(true);
        // SAFETY: the mapping is owned and writable.
        unsafe { prefault(block.0, bytes) };
        let (after, _) = faults(true);
        assert!(after > before, "no fault counted ({before} -> {after})");
        let (again, _) = faults(true);
        // SAFETY: as above.
        unsafe { prefault(block.0, bytes) };
        let (last, _) = faults(true);
        assert!(
            last - again <= 2,
            "a prefaulted block faulted again: {} faults",
            last - again
        );
    }

    #[test]
    fn prefault_keeps_what_the_memory_holds() {
        let mut held: Vec<u8> = (0..3 * PAGE).map(|index| (index % 251) as u8).collect();
        let copy = held.clone();
        // SAFETY: the vector's bytes are owned and writable.
        unsafe { prefault(held.as_mut_ptr(), held.len()) };
        assert_eq!(held, copy);
    }

    /// RLIMIT_MEMLOCK of this process (soft), or `None` when unlimited.
    fn soft_limit() -> Option<u64> {
        memlock_limit()
    }

    #[test]
    fn a_region_is_locked_under_a_finite_limit() {
        // A small region fits any limit the box gives (8 MB by default;
        // one page at least): it is locked, VmLck shows it, and the lock
        // ends with the region.
        let len = 16 * PAGE;
        if soft_limit().is_some_and(|limit| limit < len as u64 * 4) {
            eprintln!("skipped: RLIMIT_MEMLOCK below {} bytes", len * 4);
            return;
        }
        let block = Fresh::new(len);
        // SAFETY: the mapping is owned and writable while `pinned` lives.
        let pinned = unsafe { pin_region("test region", block.0, len) };
        assert!(pinned.is_locked(), "a {len}-byte region within the limit");
        let report = report();
        assert!(report.locked_bytes >= len as u64, "{report:?}");
        assert!(report.vm_lck_kb >= (len / 1024) as u64, "{report:?}");
        assert!(report.prefaulted_bytes >= len as u64, "{report:?}");
        drop(pinned);
    }

    /// The kB that `/proc/self/smaps` shows locked in `start..start + len`.
    fn locked_kb_in(start: usize, len: usize) -> u64 {
        let smaps = std::fs::read_to_string("/proc/self/smaps").unwrap();
        let mut inside = false;
        let mut kb = 0;
        for line in smaps.lines() {
            let range = line.split_whitespace().next().unwrap_or("");
            // A mapping's header line starts with its range; its fields
            // follow, one per line.
            if let Some((from, to)) = range.split_once('-') {
                if let (Ok(from), Ok(to)) = (
                    usize::from_str_radix(from, 16),
                    usize::from_str_radix(to, 16),
                ) {
                    inside = from >= start && to <= start + len;
                    continue;
                }
            }
            if inside {
                if let Some(rest) = line.strip_prefix("Locked:") {
                    kb += rest
                        .trim()
                        .trim_end_matches("kB")
                        .trim()
                        .parse::<u64>()
                        .unwrap_or(0);
                }
            }
        }
        kb
    }

    #[test]
    fn a_dropped_region_leaves_the_page_it_shares_with_a_live_one_locked() {
        // D3 review 10: two regions share page 1 of three. Dropping the
        // second unlocks only its own page 2; page 1 stays locked for the
        // first.
        let len = 3 * PAGE;
        if soft_limit().is_some_and(|limit| limit < 16 * PAGE as u64) {
            eprintln!("skipped: RLIMIT_MEMLOCK below 16 pages");
            return;
        }
        let block = Fresh::new(len);
        let split = PAGE + 100;
        // SAFETY: both halves are inside the owned mapping, which outlives
        // the pins.
        let first = unsafe { pin_region("test first half", block.0, split) };
        // SAFETY: as above.
        let second = unsafe { pin_region("test second half", block.0.add(split), len - split) };
        assert!(first.is_locked() && second.is_locked());
        let base = block.0 as usize;
        assert_eq!(locked_kb_in(base, len), 12);
        drop(second);
        assert_eq!(
            locked_kb_in(base, len),
            8,
            "page 1 is still the first region's"
        );
        drop(first);
        assert_eq!(
            locked_kb_in(base, len),
            4,
            "the shared page is left locked, not a neighbour's unlocked"
        );
    }

    #[test]
    fn a_refused_lock_is_counted_and_the_node_runs() {
        // A region over the limit (the namespace lab's 8 MB, any finite
        // box limit): the lock is refused, counted, and the region stays
        // faulted in and usable.
        let Some(limit) = soft_limit() else {
            eprintln!("skipped: RLIMIT_MEMLOCK is unlimited here");
            return;
        };
        let len = (usize::try_from(limit).unwrap() + (4 << 20)) / PAGE * PAGE;
        let block = Fresh::new(len);
        let refused_before = REFUSED.load(Ordering::Relaxed);
        // SAFETY: the mapping is owned and writable while `pinned` lives.
        let pinned = unsafe { pin_region("test region over the limit", block.0, len) };
        assert!(!pinned.is_locked());
        assert!(REFUSED.load(Ordering::Relaxed) - refused_before >= len as u64);
        let (before, _) = faults(true);
        // SAFETY: as above.
        unsafe { prefault(block.0, len) };
        let (after, _) = faults(true);
        assert!(
            after - before <= 2,
            "the refused region was faulted in all the same ({} faults)",
            after - before
        );
        assert!(!locked(), "a refusal makes memory_locked false");
        // The memory is the region's to use.
        // SAFETY: inside the mapping.
        unsafe { block.0.add(len - 1).write(7) };
    }

    #[test]
    fn the_huge_page_setting_is_the_bracketed_word() {
        assert_eq!(thp_setting("always [madvise] never\n"), "madvise");
        assert_eq!(thp_setting("[always] madvise never"), "always");
        assert_eq!(thp_setting(""), "");
        assert_eq!(
            status_kb("VmLck:\t    128 kB\nVmRSS:\t 9 kB\n", "VmLck:"),
            128
        );
        assert_eq!(status_kb("", "VmLck:"), 0);
    }
}
