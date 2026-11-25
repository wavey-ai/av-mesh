//! The node's memory: what it may spend on video, and the guard that keeps
//! the kernel from having to act (memory round, 24 Nov).
//!
//! On the referee's Linode lab (cmpd7) every forwarder and edge of a 4K sweep
//! was killed by the kernel's out-of-memory killer at 240 Mbit/s: the cache
//! kept a 4K stream's 46.5 s window (1.4 GB at 247 Mbit/s) and left each old
//! epoch's window behind. The caches are now bounded in bytes. This module
//! holds what bounds them and the last line of defence:
//!
//! - [`NodeMemory`]: the node's memory, the smaller of `MemTotal` and the
//!   cgroup's `memory.max` / `memory.high`, from which the defaults follow.
//! - [`Budgets`]: the video cache's bytes per playlist and per node, the
//!   guard's limits and the sockets' share, each `auto` (a share of the
//!   node's memory) or a number of MB.
//! - [`Guard`]: read the process's RSS once a second off the hot path. Above
//!   the soft limit the video caches are shed to a floor and no new video
//!   playlist is taken; above the hard limit every video cache is shed. Audio
//!   is never shed. Its state and counters are `/api/node`'s `memory`.
//! - [`trim`]: give the allocator's free pages back to the kernel, after a
//!   large release (glibc keeps freed memory in its heaps: with
//!   `--malloc-tune`'s 256 MB trim threshold a 4K edge stayed at 3.0 GB for
//!   minutes after it released its copy).
//! - [`SocketBudget`]: the growable receive buffers of the video class and
//!   cache-mesh sockets share one node-level budget, each class with a
//!   share of its own that no other class can take (send240, D).

use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

const MB: u64 = 1 << 20;

/// A size that is `auto` (a share of the node's memory) or a number of MB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemSetting {
    Auto,
    Mb(u64),
}

impl MemSetting {
    /// Bytes: `auto` is `permille` of the node's memory.
    pub fn bytes(self, node: u64, permille: u64) -> u64 {
        match self {
            Self::Auto => node / 1_000 * permille,
            Self::Mb(mb) => mb.saturating_mul(MB),
        }
    }
}

impl FromStr for MemSetting {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.eq_ignore_ascii_case("auto") {
            return Ok(Self::Auto);
        }
        value
            .parse::<u64>()
            .map(Self::Mb)
            .map_err(|_| format!("{value:?} is neither auto nor a number of MB"))
    }
}

impl std::fmt::Display for MemSetting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => f.write_str("auto"),
            Self::Mb(mb) => write!(f, "{mb}"),
        }
    }
}

/// The node's memory as this process may use it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct NodeMemory {
    /// `MemTotal` of /proc/meminfo.
    pub total_bytes: u64,
    /// The cgroup's `memory.max`, if one is set.
    pub cgroup_max_bytes: Option<u64>,
    /// The cgroup's `memory.high`, if one is set.
    pub cgroup_high_bytes: Option<u64>,
    /// The host's UDP memory pressure threshold (`net.ipv4.udp_mem`'s
    /// second value, in pages), in bytes.
    pub udp_pressure_bytes: Option<u64>,
}

impl NodeMemory {
    /// Read /proc/meminfo and the process's cgroup (v2).
    pub fn read() -> Self {
        let total_bytes = std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|meminfo| meminfo_kb(&meminfo, "MemTotal:"))
            .map_or(4_096 * MB, |kb| kb * 1_024);
        let cgroup = std::fs::read_to_string("/proc/self/cgroup")
            .ok()
            .and_then(|text| {
                text.lines()
                    .find_map(|line| line.strip_prefix("0::").map(str::to_owned))
            });
        let read_limit = |file: &str| {
            let path = format!("/sys/fs/cgroup{}/{file}", cgroup.as_deref()?);
            parse_cgroup_limit(&std::fs::read_to_string(path).ok()?)
        };
        let udp_pressure_bytes = std::fs::read_to_string("/proc/sys/net/ipv4/udp_mem")
            .ok()
            .and_then(|text| text.split_whitespace().nth(1)?.parse::<u64>().ok())
            .map(|pages| pages * 4_096);
        Self {
            total_bytes,
            cgroup_max_bytes: read_limit("memory.max"),
            cgroup_high_bytes: read_limit("memory.high"),
            udp_pressure_bytes,
        }
    }

    /// The memory the node's shares are taken of: the smallest of the
    /// machine's memory and the cgroup's limits.
    pub fn usable_bytes(self) -> u64 {
        self.total_bytes
            .min(self.cgroup_max_bytes.unwrap_or(u64::MAX))
            .min(self.cgroup_high_bytes.unwrap_or(u64::MAX))
    }
}

fn meminfo_kb(meminfo: &str, key: &str) -> Option<u64> {
    meminfo
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|kb| kb.parse().ok())
}

/// A cgroup v2 limit file: a number of bytes, or `max` (none).
pub fn parse_cgroup_limit(text: &str) -> Option<u64> {
    let text = text.trim();
    if text == "max" {
        return None;
    }
    text.parse().ok()
}

/// Resident bytes of this process (/proc/self/statm), or 0.
pub fn rss_bytes() -> u64 {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|pages| pages.parse().ok())
        .unwrap_or(0);
    // SAFETY: sysconf takes a plain constant.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    pages.saturating_mul(u64::try_from(page).unwrap_or(4_096))
}

/// The shares of the node's memory, in permille, that `auto` takes.
pub mod share {
    /// The chunk caches of every playlist together: 10% (390 MB of a
    /// g6-dedicated-2's 3.9 GB).
    pub const VIDEO_CACHE: u64 = 100;
    /// The guard's soft limit on RSS: 50%.
    pub const SOFT: u64 = 500;
    /// The guard's hard limit on RSS: 65%.
    pub const HARD: u64 = 650;
    /// The growable sockets' receive buffers' growth together, as asked (the
    /// kernel books twice this): 4%, and at most a quarter of the host's UDP
    /// pressure threshold (`net.ipv4.udp_mem`'s second value), so that the
    /// video sockets filling in a stall never push the host's UDP memory to
    /// where the kernel drops every socket's datagrams, audio's among them.
    pub const SOCKETS: u64 = 40;
    /// send240 (D): what each socket class keeps of the sockets' budget for
    /// itself, permille: the fast plane's video receive sockets half, the
    /// cache mesh's receive sockets a quarter; the last quarter goes to
    /// whichever asks first. On a 4 GB node (112 MB asked): 56 MB, 28 MB
    /// and 28 MB. cmpd8: the video sockets asked all 118 MB and the cache
    /// mesh's :30101 socket stayed at its floor and overflowed.
    pub const SOCKETS_VIDEO_RECEIVE: u64 = 500;
    pub const SOCKETS_CACHE_MESH: u64 = 250;
}

/// The socket classes of the sockets' budget (send240, D).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SocketClass {
    /// The fast plane's video receive sockets (primary and secondary).
    VideoReceive = 0,
    /// The cache mesh's receive sockets (main and class lanes).
    CacheMesh = 1,
}

impl SocketClass {
    pub const ALL: [Self; 2] = [Self::VideoReceive, Self::CacheMesh];

    /// Its share of the budget, permille.
    pub const fn reserve_permille(self) -> u64 {
        match self {
            Self::VideoReceive => share::SOCKETS_VIDEO_RECEIVE,
            Self::CacheMesh => share::SOCKETS_CACHE_MESH,
        }
    }
}

/// What the class threads' pools of kept datagrams hold, all together
/// (send240, A: `fast_plane::send_defer`), for the memory's account.
pub static SEND_DEFER_BYTES: AtomicU64 = AtomicU64::new(0);

/// What the node may spend, resolved from its flags and its memory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Budgets {
    pub node_memory_bytes: u64,
    /// The chunk caches of every playlist together.
    pub video_cache_bytes: u64,
    /// One playlist's chunk cache.
    pub video_cache_playlist_bytes: u64,
    /// The guard's limits on RSS (0: no guard).
    pub soft_bytes: u64,
    pub hard_bytes: u64,
    /// What the large stores keep together above the soft limit, the
    /// largest cut first ([`shed_level`]).
    pub shed_floor_bytes: u64,
    /// The growable sockets' receive buffers together, as asked.
    pub socket_bytes: u64,
}

impl Budgets {
    /// Resolve the flags against the node's memory. A cache bound of 0 MB is
    /// no bound; a guard limit of 0 MB is no guard.
    pub fn resolve(
        memory: NodeMemory,
        video_cache: MemSetting,
        video_cache_playlist: MemSetting,
        soft: MemSetting,
        hard: MemSetting,
        sockets: MemSetting,
    ) -> Self {
        let node = memory.usable_bytes();
        let video_cache_bytes = video_cache.bytes(node, share::VIDEO_CACHE);
        let video_cache_playlist_bytes = match video_cache_playlist {
            MemSetting::Auto => video_cache_bytes,
            setting => setting.bytes(node, share::VIDEO_CACHE),
        };
        let soft_bytes = soft.bytes(node, share::SOFT);
        let hard_bytes = hard.bytes(node, share::HARD).max(soft_bytes);
        let playlist = if video_cache_playlist_bytes == 0 {
            video_cache_bytes
        } else {
            video_cache_playlist_bytes
        };
        Self {
            node_memory_bytes: node,
            video_cache_bytes,
            video_cache_playlist_bytes,
            soft_bytes,
            hard_bytes,
            // Half the cache's bound: 5 s of one 4K stream at 320 Mbit/s
            // with the defaults on a 4 GB node, and never less than 32 MB.
            shed_floor_bytes: (video_cache_bytes.max(playlist) / 2).max(32 * MB),
            socket_bytes: match sockets {
                MemSetting::Auto => sockets
                    .bytes(node, share::SOCKETS)
                    .min(memory.udp_pressure_bytes.map_or(u64::MAX, |bytes| bytes / 4)),
                setting => setting.bytes(node, share::SOCKETS),
            },
        }
    }
}

/// The guard's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum GuardState {
    Ok = 0,
    /// Above the soft limit: video caches shed to their floor, no new video
    /// playlist.
    Soft = 1,
    /// Above the hard limit: every video cache shed.
    Hard = 2,
}

impl GuardState {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Soft,
            2 => Self::Hard,
            _ => Self::Ok,
        }
    }
}

/// What the guard does in one tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuardTick {
    pub state: GuardState,
    /// The state changed in this tick.
    pub changed: bool,
    /// The allocator should give its free pages back now.
    pub trim: bool,
}

/// The node-level memory guard: its limits, its state and its counters.
/// [`Guard::tick`] decides; the cache does the shedding.
#[derive(Debug, Default)]
pub struct Guard {
    soft_bytes: AtomicU64,
    hard_bytes: AtomicU64,
    state: AtomicU8,
    rss_bytes: AtomicU64,
    rss_peak_bytes: AtomicU64,
    cache_bytes: AtomicU64,
    /// The cache's bytes when the allocator was last trimmed (or the most
    /// it has held since).
    cache_high_bytes: AtomicU64,
    soft_entries: AtomicU64,
    hard_entries: AtomicU64,
    shed_bytes: AtomicU64,
    refused_large_playlists: AtomicU64,
    idle_releases: AtomicU64,
    trims: AtomicU64,
    trimmed_bytes: AtomicU64,
}

/// The cache bytes released since the last trim that make the guard trim
/// the allocator: 64 MB (two seconds of 4K at 247 Mbit/s).
pub const TRIM_AFTER_RELEASE_BYTES: u64 = 64 * MB;
/// The guard leaves a state when RSS falls below this share of its limit.
const HYSTERESIS_PERMILLE: u64 = 900;

impl Guard {
    pub fn new(soft_bytes: u64, hard_bytes: u64) -> Self {
        let guard = Self::default();
        guard.soft_bytes.store(soft_bytes, Ordering::Relaxed);
        guard.hard_bytes.store(hard_bytes.max(soft_bytes), Ordering::Relaxed);
        guard
    }

    pub fn state(&self) -> GuardState {
        GuardState::from_u8(self.state.load(Ordering::Relaxed))
    }

    /// Whether a new video playlist may be taken.
    pub fn takes_new_large_playlist(&self) -> bool {
        self.state() == GuardState::Ok
    }

    pub fn note_refused_large_playlist(&self) {
        self.refused_large_playlists.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_shed(&self, bytes: u64) {
        self.shed_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn note_idle_release(&self) {
        self.idle_releases.fetch_add(1, Ordering::Relaxed);
    }

    /// One tick: the process's RSS and the cache's bytes now. Leaves a state
    /// only below 90% of its limit, and asks for a trim after a release of
    /// [`TRIM_AFTER_RELEASE_BYTES`] or on entering a shed.
    pub fn tick(&self, rss_bytes: u64, cache_bytes: u64) -> GuardTick {
        self.rss_bytes.store(rss_bytes, Ordering::Relaxed);
        self.rss_peak_bytes.fetch_max(rss_bytes, Ordering::Relaxed);
        self.cache_bytes.store(cache_bytes, Ordering::Relaxed);
        let high = self.cache_high_bytes.fetch_max(cache_bytes, Ordering::Relaxed).max(cache_bytes);
        let soft = self.soft_bytes.load(Ordering::Relaxed);
        let hard = self.hard_bytes.load(Ordering::Relaxed);
        let before = self.state();
        let below = |limit: u64| rss_bytes < limit / 1_000 * HYSTERESIS_PERMILLE;
        let state = if soft == 0 {
            GuardState::Ok
        } else if hard > 0 && rss_bytes >= hard {
            GuardState::Hard
        } else if rss_bytes >= soft {
            if before == GuardState::Hard && !below(hard) {
                GuardState::Hard
            } else {
                GuardState::Soft
            }
        } else if before == GuardState::Hard && !below(hard) {
            GuardState::Hard
        } else if before != GuardState::Ok && !below(soft) {
            GuardState::Soft
        } else {
            GuardState::Ok
        };
        let changed = state != before;
        if changed {
            self.state.store(state as u8, Ordering::Relaxed);
            match state {
                GuardState::Soft if before == GuardState::Ok => {
                    self.soft_entries.fetch_add(1, Ordering::Relaxed);
                }
                GuardState::Hard => {
                    self.hard_entries.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
        let released = high.saturating_sub(cache_bytes);
        let trim = released >= TRIM_AFTER_RELEASE_BYTES
            || (changed && state != GuardState::Ok)
            // Above the soft limit, every tick: what was shed must leave
            // the process.
            || (state != GuardState::Ok && released > 0);
        if trim {
            self.cache_high_bytes.store(cache_bytes, Ordering::Relaxed);
        }
        GuardTick {
            state,
            changed,
            trim,
        }
    }

    /// Count a trim and what it gave back (RSS before less RSS after).
    pub fn note_trim(&self, returned_bytes: u64) {
        self.trims.fetch_add(1, Ordering::Relaxed);
        self.trimmed_bytes.fetch_add(returned_bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self, budgets: &Budgets, sockets: &SocketBudget) -> MemorySnapshot {
        MemorySnapshot {
            state: self.state(),
            rss_bytes: self.rss_bytes.load(Ordering::Relaxed),
            rss_peak_bytes: self.rss_peak_bytes.load(Ordering::Relaxed),
            node_memory_bytes: budgets.node_memory_bytes,
            soft_bytes: self.soft_bytes.load(Ordering::Relaxed),
            hard_bytes: self.hard_bytes.load(Ordering::Relaxed),
            video_cache_bytes: self.cache_bytes.load(Ordering::Relaxed),
            video_cache_budget_bytes: budgets.video_cache_bytes,
            video_cache_playlist_budget_bytes: budgets.video_cache_playlist_bytes,
            shed_floor_bytes: budgets.shed_floor_bytes,
            soft_entries: self.soft_entries.load(Ordering::Relaxed),
            hard_entries: self.hard_entries.load(Ordering::Relaxed),
            shed_bytes: self.shed_bytes.load(Ordering::Relaxed),
            refused_large_playlists: self.refused_large_playlists.load(Ordering::Relaxed),
            idle_releases: self.idle_releases.load(Ordering::Relaxed),
            trims: self.trims.load(Ordering::Relaxed),
            trimmed_bytes: self.trimmed_bytes.load(Ordering::Relaxed),
            socket_budget_bytes: sockets.budget(),
            socket_asked_bytes: sockets.asked(),
            socket_clamped: sockets.clamped(),
            socket_classes: SocketClass::ALL.map(|class| sockets.class_snapshot(class)),
            send_defer_bytes: SEND_DEFER_BYTES.load(Ordering::Relaxed),
        }
    }
}

/// `/api/node`'s `memory`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemorySnapshot {
    pub state: GuardState,
    pub rss_bytes: u64,
    pub rss_peak_bytes: u64,
    pub node_memory_bytes: u64,
    pub soft_bytes: u64,
    pub hard_bytes: u64,
    /// The chunk cache's payload bytes now, hidden streams' included.
    pub video_cache_bytes: u64,
    pub video_cache_budget_bytes: u64,
    pub video_cache_playlist_budget_bytes: u64,
    pub shed_floor_bytes: u64,
    pub soft_entries: u64,
    pub hard_entries: u64,
    /// Cache bytes released by sheds.
    pub shed_bytes: u64,
    pub refused_large_playlists: u64,
    /// Video streams quiet long enough that their parts were released.
    pub idle_releases: u64,
    pub trims: u64,
    /// RSS that the trims gave back.
    pub trimmed_bytes: u64,
    pub socket_budget_bytes: u64,
    pub socket_asked_bytes: u64,
    /// Socket growths cut short by the budget.
    pub socket_clamped: u64,
    /// send240 (D): each socket class's own share of the budget and what
    /// it asked (video receive, cache mesh).
    #[serde(default)]
    pub socket_classes: [SocketClassSnapshot; 2],
    /// send240 (A): the class threads' pools of kept datagrams, made and
    /// touched at start (in RSS).
    #[serde(default)]
    pub send_defer_bytes: u64,
}

/// One socket class of the sockets' budget.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SocketClassSnapshot {
    pub reserve_bytes: u64,
    pub asked_bytes: u64,
    pub clamped: u64,
}

/// The node's budgets and guard, installed once at start.
#[derive(Debug)]
pub struct Installed {
    pub budgets: Budgets,
    pub guard: Arc<Guard>,
}

impl Installed {
    pub fn snapshot(&self) -> MemorySnapshot {
        self.guard.snapshot(&self.budgets, &SOCKETS)
    }
}

static INSTALLED: OnceLock<Installed> = OnceLock::new();

/// Install the node's budgets: the guard's limits and the sockets' budget.
/// The first call wins.
pub fn install(budgets: Budgets) -> &'static Installed {
    INSTALLED.get_or_init(|| {
        SOCKETS.set_budget(budgets.socket_bytes);
        Installed {
            budgets,
            guard: Arc::new(Guard::new(budgets.soft_bytes, budgets.hard_bytes)),
        }
    })
}

pub fn installed() -> Option<&'static Installed> {
    INSTALLED.get()
}

/// The level the largest stores are cut to so that all of them together
/// hold at most `target` bytes: the highest level L with the sum of
/// min(held, L) at most `target`. Stores below L keep what they hold.
pub fn shed_level(held: &[u64], target: u64) -> u64 {
    if held.iter().sum::<u64>() <= target {
        return held.iter().copied().max().unwrap_or(0);
    }
    let (mut low, mut high) = (0_u64, held.iter().copied().max().unwrap_or(0));
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        let sum: u64 = held.iter().map(|bytes| (*bytes).min(mid)).sum();
        if sum <= target {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    low
}

/// What a trim leaves at the top of each allocator arena: more than the
/// 32 MiB that a class thread warms in its own arena before it is made
/// real-time (`--relay-class-arena-warm-mb`), so that a trim never takes
/// those pages back and the class thread never faults them in again.
pub const TRIM_PAD_BYTES: usize = 40 << 20;

/// Give the allocator's free pages back to the kernel (glibc's
/// `malloc_trim`: the free pages inside every arena's heaps, and each
/// arena's top beyond [`TRIM_PAD_BYTES`]). Returns what RSS fell by. Run
/// off the hot path: it holds each arena's lock while it walks it.
pub fn trim() -> u64 {
    let before = rss_bytes();
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: malloc_trim takes a plain integer.
    unsafe {
        libc::malloc_trim(TRIM_PAD_BYTES);
    }
    before.saturating_sub(rss_bytes())
}

/// The growable receive buffers of the node's sockets share one budget: the
/// sum of what they ask (the kernel books twice that). A socket asks for
/// what its traffic needs; it is given what is left of its class's share
/// and of the common rest beside the others' current sizes, and never less
/// than its floor. Each class keeps its share ([`SocketClass`]): no class
/// is squeezed to its floor by another (send240, D).
#[derive(Debug, Default)]
pub struct SocketBudget {
    budget: AtomicU64,
    asked: AtomicU64,
    clamped: AtomicU64,
    /// What each class asked, and its growths cut short.
    class_asked: [AtomicU64; 2],
    class_clamped: [AtomicU64; 2],
    /// Growths are rare (a socket's look, once a second, off the hot
    /// path): one at a time.
    lock: std::sync::Mutex<()>,
}

impl SocketBudget {
    pub fn set_budget(&self, bytes: u64) {
        self.budget.store(bytes, Ordering::Relaxed);
    }

    pub fn budget(&self) -> u64 {
        self.budget.load(Ordering::Relaxed)
    }

    pub fn asked(&self) -> u64 {
        self.asked.load(Ordering::Relaxed)
    }

    pub fn clamped(&self) -> u64 {
        self.clamped.load(Ordering::Relaxed)
    }

    /// A class's own share of the budget.
    pub fn reserve(&self, class: SocketClass) -> u64 {
        self.budget().saturating_mul(class.reserve_permille()) / 1_000
    }

    /// The budget's rest, which no class keeps: whichever asks first.
    fn common(&self) -> u64 {
        self.budget().saturating_sub(
            SocketClass::ALL
                .iter()
                .map(|class| self.reserve(*class))
                .sum(),
        )
    }

    pub fn class_snapshot(&self, class: SocketClass) -> SocketClassSnapshot {
        SocketClassSnapshot {
            reserve_bytes: self.reserve(class),
            asked_bytes: self.class_asked[class as usize].load(Ordering::Relaxed),
            clamped: self.class_clamped[class as usize].load(Ordering::Relaxed),
        }
    }

    /// A socket of `class` asked `current` bytes so far and now wants
    /// `wanted`: what it may ask the kernel for. The budget bounds the
    /// sockets' growth beyond the sizes they were made with (each
    /// `--udp-socket-buffer-bytes` or cache-mesh floor): first from the
    /// class's own share, then from the common rest. A buffer never
    /// shrinks. No budget (0) gives what is asked.
    pub fn grow(&self, class: SocketClass, current: usize, wanted: usize) -> usize {
        if wanted <= current {
            return current;
        }
        let extra = (wanted - current) as u64;
        let budget = self.budget();
        let _one = self.lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let granted = if budget == 0 {
            extra
        } else {
            let asked = |class: SocketClass| self.class_asked[class as usize].load(Ordering::Relaxed);
            let own = asked(class);
            let from_reserve = extra.min(self.reserve(class).saturating_sub(own));
            // What every class took beyond its own share is the common
            // rest's.
            let common_used: u64 = SocketClass::ALL
                .iter()
                .map(|other| asked(*other).saturating_sub(self.reserve(*other)))
                .sum();
            let from_common =
                (extra - from_reserve).min(self.common().saturating_sub(common_used));
            from_reserve + from_common
        };
        self.class_asked[class as usize].fetch_add(granted, Ordering::Relaxed);
        self.asked.fetch_add(granted, Ordering::Relaxed);
        if granted < extra {
            self.clamped.fetch_add(1, Ordering::Relaxed);
            self.class_clamped[class as usize].fetch_add(1, Ordering::Relaxed);
        }
        current.saturating_add(usize::try_from(granted).unwrap_or(usize::MAX))
    }
}

/// The node's socket budget, set once at start (`--socket-buffer-mb`).
pub static SOCKETS: SocketBudget = SocketBudget {
    budget: AtomicU64::new(0),
    asked: AtomicU64::new(0),
    clamped: AtomicU64::new(0),
    class_asked: [AtomicU64::new(0), AtomicU64::new(0)],
    class_clamped: [AtomicU64::new(0), AtomicU64::new(0)],
    lock: std::sync::Mutex::new(()),
};

#[cfg(test)]
mod tests {
    use super::*;

    const GB: u64 = 1 << 30;

    fn four_gb() -> NodeMemory {
        NodeMemory {
            total_bytes: 3_921 * MB,
            cgroup_max_bytes: None,
            cgroup_high_bytes: None,
            // A 4 GB guest's default: free buffer pages / 8.
            udp_pressure_bytes: Some(115_000 * 4_096),
        }
    }

    #[test]
    fn auto_budgets_follow_the_node_s_memory_and_its_cgroup() {
        let auto = MemSetting::Auto;
        let small = Budgets::resolve(four_gb(), auto, auto, auto, auto, auto);
        assert_eq!(small.video_cache_bytes, 3_921 * MB / 1_000 * 100);
        assert_eq!(small.video_cache_playlist_bytes, small.video_cache_bytes);
        assert!(small.soft_bytes > 1_900 * MB && small.soft_bytes < 2_000 * MB);
        assert!(small.hard_bytes > 2_500 * MB && small.hard_bytes < 2_600 * MB);
        assert_eq!(small.shed_floor_bytes, small.video_cache_bytes / 2);
        // 4% of 3.9 GB is 157 MB; a quarter of the UDP pressure threshold
        // (449 MB) is 112 MB.
        assert_eq!(small.socket_bytes, 115_000 * 4_096 / 4);
        let large = Budgets::resolve(
            NodeMemory {
                total_bytes: 8 * GB,
                ..NodeMemory::default()
            },
            auto,
            auto,
            auto,
            auto,
            auto,
        );
        assert!(large.video_cache_bytes > 2 * small.video_cache_bytes - 64 * MB);
        // A cgroup limit below the machine's memory sets the shares.
        let limited = NodeMemory {
            cgroup_max_bytes: Some(2 * GB),
            ..four_gb()
        };
        assert_eq!(limited.usable_bytes(), 2 * GB);
        let explicit = Budgets::resolve(
            four_gb(),
            MemSetting::Mb(600),
            MemSetting::Mb(300),
            MemSetting::Mb(0),
            MemSetting::Mb(0),
            MemSetting::Mb(64),
        );
        assert_eq!(explicit.video_cache_bytes, 600 * MB);
        assert_eq!(explicit.video_cache_playlist_bytes, 300 * MB);
        assert_eq!(explicit.soft_bytes, 0);
        assert_eq!(explicit.socket_bytes, 64 * MB);
        assert_eq!("auto".parse(), Ok(MemSetting::Auto));
        assert_eq!("512".parse(), Ok(MemSetting::Mb(512)));
        assert!("lots".parse::<MemSetting>().is_err());
        assert_eq!(parse_cgroup_limit("max\n"), None);
        assert_eq!(parse_cgroup_limit("2147483648\n"), Some(2 * GB));
    }

    /// Integration 5: the budgets of a g6-dedicated-2 (MemTotal 3,921 MB on
    /// Linode) as the lab runs it. Under the mesh unit's `MemoryHigh=75%`
    /// and `MemoryMax=85%`, `auto` is taken of the cgroup's memory.high (75%
    /// of MemTotal): every share shrinks by a quarter. The deploy therefore
    /// renders the shares of MemTotal explicitly, which gives back the
    /// budgets of a node without the unit's limits. Printed with
    /// `--nocapture` for the hand-over.
    #[test]
    fn a_four_gb_linode_s_budgets_with_and_without_the_unit_s_limits() {
        let auto = MemSetting::Auto;
        let node = four_gb();
        let unit = NodeMemory {
            cgroup_high_bytes: Some(node.total_bytes / 100 * 75),
            cgroup_max_bytes: Some(node.total_bytes / 100 * 85),
            ..node
        };
        let plain = Budgets::resolve(node, auto, auto, auto, auto, auto);
        let under_unit = Budgets::resolve(unit, auto, auto, auto, auto, auto);
        // What the deploy renders on a g6-dedicated-2: 10%, 50% and 65% of
        // 3,921 MB.
        let rendered = Budgets::resolve(
            unit,
            MemSetting::Mb(392),
            auto,
            MemSetting::Mb(1_960),
            MemSetting::Mb(2_548),
            auto,
        );
        for (name, budgets) in [
            ("auto, no cgroup limit", plain),
            ("auto, MemoryHigh=75% MemoryMax=85%", under_unit),
            ("rendered 392/1960/2548, same unit", rendered),
        ] {
            println!(
                "{name}: node {} MB, video cache {} MB, per playlist {} MB, shed floor {} MB, \
                 soft {} MB, hard {} MB, sockets {} MB asked",
                budgets.node_memory_bytes / MB,
                budgets.video_cache_bytes / MB,
                budgets.video_cache_playlist_bytes / MB,
                budgets.shed_floor_bytes / MB,
                budgets.soft_bytes / MB,
                budgets.hard_bytes / MB,
                budgets.socket_bytes / MB,
            );
        }
        assert_eq!(under_unit.node_memory_bytes, unit.cgroup_high_bytes.unwrap());
        assert!(under_unit.video_cache_bytes < plain.video_cache_bytes * 8 / 10);
        assert!(under_unit.hard_bytes < unit.cgroup_high_bytes.unwrap());
        // Rendered, the budgets are the plain node's (within a MB of
        // rounding) and the hard limit stays under memory.high.
        assert!(rendered.video_cache_bytes.abs_diff(plain.video_cache_bytes) < MB);
        assert!(rendered.soft_bytes.abs_diff(plain.soft_bytes) < MB);
        assert!(rendered.hard_bytes.abs_diff(plain.hard_bytes) < MB);
        assert!(rendered.hard_bytes < unit.cgroup_high_bytes.unwrap());
        // The sockets' budget is the UDP threshold's quarter either way.
        assert_eq!(rendered.socket_bytes, plain.socket_bytes);
    }

    #[test]
    fn the_guard_sheds_at_its_limits_and_leaves_them_below_ninety_percent() {
        let guard = Guard::new(2 * GB, 3 * GB);
        assert_eq!(guard.tick(GB, 300 * MB).state, GuardState::Ok);
        let soft = guard.tick(2 * GB + MB, 300 * MB);
        assert_eq!(soft.state, GuardState::Soft);
        assert!(soft.changed && soft.trim);
        assert!(!guard.takes_new_large_playlist());
        // Between 90% and 100% of the soft limit it stays.
        assert_eq!(guard.tick(2 * GB - 100 * MB, 200 * MB).state, GuardState::Soft);
        let hard = guard.tick(3 * GB, 200 * MB);
        assert_eq!(hard.state, GuardState::Hard);
        assert_eq!(guard.tick(3 * GB - 200 * MB, 0).state, GuardState::Hard);
        assert_eq!(guard.tick(2 * GB, 0).state, GuardState::Soft);
        let back = guard.tick(GB, 0);
        assert_eq!(back.state, GuardState::Ok);
        assert!(guard.takes_new_large_playlist());
        let snapshot = guard.snapshot(&Budgets::default(), &SocketBudget::default());
        assert_eq!((snapshot.soft_entries, snapshot.hard_entries), (1, 1));
        // A release of 64 MB or more asks for a trim once.
        assert!(!guard.tick(GB, 400 * MB).trim);
        assert!(guard.tick(GB, 330 * MB).trim);
        assert!(!guard.tick(GB, 330 * MB).trim);
        // No soft limit: no guard.
        let off = Guard::new(0, 0);
        assert_eq!(off.tick(100 * GB, 0).state, GuardState::Ok);
    }

    #[test]
    fn the_largest_stores_are_cut_first() {
        // Two 4K stores and one smaller: the smaller is kept whole.
        assert_eq!(shed_level(&[300 * MB, 200 * MB, 20 * MB], 220 * MB), 100 * MB);
        assert_eq!(shed_level(&[300 * MB], 196 * MB), 196 * MB);
        assert_eq!(shed_level(&[100 * MB, 50 * MB], 400 * MB), 100 * MB);
        assert_eq!(shed_level(&[], 0), 0);
    }

    #[test]
    fn the_sockets_share_one_budget() {
        use SocketClass::{CacheMesh, VideoReceive};
        let budget = SocketBudget::default();
        budget.set_budget(100 * MB);
        let floor = 8 * MB as usize;
        // Growth beyond the sizes the sockets were made with shares it: the
        // video sockets take their half (50 MB) and the common quarter.
        let a = budget.grow(VideoReceive, floor, 48 * MB as usize);
        assert_eq!(a, 48 * MB as usize);
        let b = budget.grow(VideoReceive, floor, 48 * MB as usize);
        assert_eq!(b, floor + 35 * MB as usize, "10 of its share, 25 common");
        assert_eq!(budget.clamped(), 1);
        // Spent: the others keep what they were made with, and none shrinks.
        assert_eq!(budget.grow(VideoReceive, floor, 64 * MB as usize), floor);
        assert_eq!(budget.grow(VideoReceive, a, 120 * MB as usize), a);
        assert_eq!(budget.grow(VideoReceive, a, 10 * MB as usize), a);
        assert_eq!(budget.asked(), 75 * MB);
        let open = SocketBudget::default();
        assert_eq!(
            open.grow(VideoReceive, floor, 256 * MB as usize),
            256 * MB as usize
        );
        // The cache mesh's share is its own: the video sockets took all
        // they could, and its socket still grows by its quarter.
        let mesh = budget.grow(CacheMesh, floor, 64 * MB as usize);
        assert_eq!(mesh, floor + 25 * MB as usize);
        assert_eq!(budget.asked(), 100 * MB);
        let snapshot = budget.class_snapshot(CacheMesh);
        assert_eq!((snapshot.reserve_bytes, snapshot.asked_bytes, snapshot.clamped), (25 * MB, 25 * MB, 1));
    }

    /// send240 (D): cmpd8's edge, whose two video receive sockets asked two
    /// seconds of 4K (62 MB each at 247 Mbit/s) before the cache mesh's
    /// asked anything: the cache mesh's socket still gets its share, in
    /// whichever order they ask. A 4 GB node's shares are printed.
    #[test]
    fn a_4k_edge_s_video_sockets_leave_the_cache_mesh_its_share() {
        use SocketClass::{CacheMesh, VideoReceive};
        let budgets = Budgets::resolve(
            four_gb(),
            MemSetting::Auto,
            MemSetting::Auto,
            MemSetting::Auto,
            MemSetting::Auto,
            MemSetting::Auto,
        );
        let floor = 8 * MB as usize;
        let two_seconds = 62 * MB as usize;
        for video_first in [true, false] {
            let budget = SocketBudget::default();
            budget.set_budget(budgets.socket_bytes);
            let (mut primary, mut secondary, mut mesh) = (floor, floor, floor);
            for _ in 0..3 {
                if video_first {
                    primary = budget.grow(VideoReceive, primary, two_seconds);
                    secondary = budget.grow(VideoReceive, secondary, two_seconds);
                    mesh = budget.grow(CacheMesh, mesh, two_seconds);
                } else {
                    mesh = budget.grow(CacheMesh, mesh, two_seconds);
                    primary = budget.grow(VideoReceive, primary, two_seconds);
                    secondary = budget.grow(VideoReceive, secondary, two_seconds);
                }
            }
            let reserve = budget.reserve(CacheMesh) as usize;
            assert!(mesh >= floor + reserve, "the cache mesh keeps its share");
            assert!(
                primary - floor + secondary - floor >= budget.reserve(VideoReceive) as usize,
                "the video sockets keep theirs"
            );
            assert!(budget.asked() <= budgets.socket_bytes);
            println!(
                "4 GB node, sockets {} MB asked: video receive share {} MB, cache mesh share {} MB, \
                 common {} MB; {} first: video sockets grew to {} + {} MB, cache mesh to {} MB",
                budgets.socket_bytes / MB,
                budget.reserve(VideoReceive) / MB,
                budget.reserve(CacheMesh) / MB,
                budget.common() / MB,
                if video_first { "video" } else { "cache mesh" },
                primary as u64 / MB,
                secondary as u64 / MB,
                mesh as u64 / MB,
            );
        }
    }

    #[test]
    fn rss_and_the_node_s_memory_are_read() {
        assert!(rss_bytes() > 0);
        let memory = NodeMemory::read();
        assert!(memory.total_bytes > 0);
        assert!(memory.usable_bytes() <= memory.total_bytes);
    }
}
