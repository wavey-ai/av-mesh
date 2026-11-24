use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Lowest outbound limit for the Linode dedicated plan used by Needletail.
pub const DEFAULT_EGRESS_CAPACITY_BPS: u64 = 4_000_000_000;
pub const DEFAULT_ADMISSION_PERCENT: u8 = 85;
pub const DEFAULT_RECOVERY_PERCENT: u8 = 75;
pub const DEFAULT_WINDOW_SECONDS: u64 = 10;
pub const DEFAULT_MIN_SUSTAINED_SECONDS: u64 = 3;
/// A live reader asks for a part or a playlist several times a second. One
/// that has asked for nothing for this long has gone, and its share of the
/// node is free for another reader.
pub const DEFAULT_SESSION_IDLE_SECONDS: u64 = 15;
pub const DEFAULT_MAX_SESSIONS: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeAdmissionConfig {
    pub capacity_bps: u64,
    pub admission_percent: u8,
    pub recovery_percent: u8,
    pub window_seconds: u64,
    pub min_sustained_seconds: u64,
    pub session_idle_seconds: u64,
    pub max_sessions: usize,
    /// Most playback sessions that the node serves at one time. Zero sets no
    /// limit. This is the reader limit of a small node, where the processor
    /// is exhausted before the network.
    pub session_limit: usize,
    /// Requests that the node answers in a second when its processor is
    /// full. A reader of 5 ms parts asks 200 times a second, and a small
    /// node runs out of processor long before it runs out of network: one
    /// vCPU serves about forty such readers whatever their bit rate. Each
    /// session is therefore charged the larger of its bit rate and the share
    /// of the node that its requests take. Zero charges the bit rate alone.
    pub request_capacity_per_sec: u64,
}

impl Default for EdgeAdmissionConfig {
    fn default() -> Self {
        Self {
            capacity_bps: DEFAULT_EGRESS_CAPACITY_BPS,
            admission_percent: DEFAULT_ADMISSION_PERCENT,
            recovery_percent: DEFAULT_RECOVERY_PERCENT,
            window_seconds: DEFAULT_WINDOW_SECONDS,
            min_sustained_seconds: DEFAULT_MIN_SUSTAINED_SECONDS,
            session_idle_seconds: DEFAULT_SESSION_IDLE_SECONDS,
            max_sessions: DEFAULT_MAX_SESSIONS,
            session_limit: 0,
            request_capacity_per_sec: 0,
        }
    }
}

impl EdgeAdmissionConfig {
    pub fn normalized(mut self) -> Self {
        self.capacity_bps = self.capacity_bps.max(1);
        self.admission_percent = self.admission_percent.clamp(1, 100);
        self.recovery_percent = self
            .recovery_percent
            .clamp(1, self.admission_percent.saturating_sub(1).max(1));
        self.window_seconds = self.window_seconds.max(1);
        self.min_sustained_seconds = self.min_sustained_seconds.clamp(1, self.window_seconds);
        self.session_idle_seconds = self.session_idle_seconds.max(1);
        self.max_sessions = self.max_sessions.max(1);
        self
    }

    fn admission_bps(self) -> u64 {
        percent_of(self.capacity_bps, self.admission_percent)
    }

    fn recovery_bps(self) -> u64 {
        percent_of(self.capacity_bps, self.recovery_percent)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeAdmissionSnapshot {
    pub capacity_bps: u64,
    pub observed_bps: u64,
    pub admission_bps: u64,
    pub recovery_bps: u64,
    pub observation_seconds: u64,
    pub window_seconds: u64,
    pub overloaded: bool,
    pub rejected_requests: u64,
    pub active_sessions: u64,
    pub admitted_sessions: u64,
    /// Most sessions served at one time. Zero means no limit.
    #[serde(default)]
    pub session_limit: u64,
    /// Bit rate that the admitted sessions are entitled to: for each session,
    /// the bit rate of the playlist that it reads.
    #[serde(default)]
    pub committed_bps: u64,
    /// Load in thousandths of the admission limit: the largest of observed
    /// egress, committed egress, sessions and the process's CPU (wave 9
    /// C2). At 1,000 the node admits no new session.
    #[serde(default)]
    pub load_permille: u32,
    /// The CPU term of the load (wave 9 C2, `--cpu-load`): the process's
    /// CPU time over its available CPUs in the last few seconds, in
    /// thousandths of `--cpu-load-full-percent` of them. 0 without it.
    #[serde(default)]
    pub cpu_permille: u32,
}

/// The process's CPU time so far (user and system), in microseconds;
/// `None` when it cannot be read (wave 9 C2).
pub type CpuCounter = Arc<dyn Fn() -> Option<u64> + Send + Sync>;

/// Default of `--cpu-load-full-percent`: the share of the available CPUs
/// at which the CPU term reads 1,000 permille, as the egress term reads
/// 1,000 at the admission percent of its capacity.
pub const DEFAULT_CPU_LOAD_FULL_PERCENT: u8 = 85;

/// Default of `--cpu-load-window-ms`: the CPU term is the mean over this
/// long, so that one busy second (a crowd's admission, a GC of the cache)
/// does not read as a full node.
pub const DEFAULT_CPU_LOAD_WINDOW_MS: u64 = 5_000;

/// Samples of the CPU counter closer together than this are not kept.
const CPU_SAMPLE_SPACING_MS: u64 = 200;

/// The CPU term of the load (wave 9 C2): samples of the process's CPU
/// time, and the mean over the window from them.
struct CpuSource {
    read: CpuCounter,
    cpus: u32,
    full_percent: u8,
    window_ms: u64,
    window: CpuWindow,
}

impl std::fmt::Debug for CpuSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CpuSource")
            .field("cpus", &self.cpus)
            .field("full_percent", &self.full_percent)
            .field("window_ms", &self.window_ms)
            .finish()
    }
}

/// Samples of a cumulative CPU time: (ms since start, CPU µs).
#[derive(Debug, Default, Clone)]
pub(crate) struct CpuWindow {
    samples: VecDeque<(u64, u64)>,
}

impl CpuWindow {
    /// Keep a sample (one every `CPU_SAMPLE_SPACING_MS` at most), and the
    /// samples of the last `window_ms` with the one just before them.
    pub(crate) fn record(&mut self, at_ms: u64, cpu_us: u64, window_ms: u64) {
        if let Some(&(last_ms, last_us)) = self.samples.back() {
            if at_ms < last_ms.saturating_add(CPU_SAMPLE_SPACING_MS) || cpu_us < last_us {
                return;
            }
        }
        self.samples.push_back((at_ms, cpu_us));
        let oldest = at_ms.saturating_sub(window_ms);
        while self.samples.len() > 2 && self.samples[1].0 <= oldest {
            self.samples.pop_front();
        }
    }

    /// The CPU term: CPU time over wall time between the first and last
    /// samples, over `cpus` CPUs, in thousandths of `full_percent` of them.
    /// `None` until the samples span half a second.
    pub(crate) fn permille(&self, cpus: u32, full_percent: u8) -> Option<u32> {
        let (first_ms, first_us) = *self.samples.front()?;
        let (last_ms, last_us) = *self.samples.back()?;
        let wall_ms = last_ms.checked_sub(first_ms).filter(|wall| *wall >= 500)?;
        Some(cpu_permille(
            last_us.saturating_sub(first_us),
            wall_ms.saturating_mul(1_000),
            cpus,
            full_percent,
        ))
    }
}

/// CPU time `cpu_us` in `wall_us` of wall time, as thousandths of
/// `full_percent` of `cpus` CPUs.
pub(crate) fn cpu_permille(cpu_us: u64, wall_us: u64, cpus: u32, full_percent: u8) -> u32 {
    let full = u128::from(wall_us)
        * u128::from(cpus.max(1))
        * u128::from(full_percent.clamp(1, 100))
        / 100;
    if full == 0 {
        return 0;
    }
    (u128::from(cpu_us) * 1_000 / full).min(u128::from(u32::MAX)) as u32
}

/// The process's CPU time so far, user and system, in microseconds.
pub fn process_cpu_us() -> Option<u64> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime writes the timespec it is given.
    if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) } != 0 {
        return None;
    }
    Some(
        u64::try_from(time.tv_sec).ok()?.saturating_mul(1_000_000)
            + u64::try_from(time.tv_nsec).ok()? / 1_000,
    )
}

/// The calling thread's CPU time so far, in microseconds; `None` when it
/// cannot be read.
pub fn thread_cpu_us() -> Option<u64> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime writes the timespec it is given.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) } != 0 {
        return None;
    }
    Some(
        u64::try_from(time.tv_sec).ok()?.saturating_mul(1_000_000)
            + u64::try_from(time.tv_nsec).ok()? / 1_000,
    )
}

/// CPU time this process spent on work that one playlist costs whatever
/// its listeners: the fast-plane tees' decode and commit of every symbol
/// of the streams the node holds (4K: 60 to 115 % of a CPU at 247 to 300
/// Mbit/s on a lab edge, with one listener or a hundred). The CPU term of
/// the load leaves it out (`process_cpu_us_for_listeners`), so that a 4K
/// edge does not read as full, leave assignment and shed its audio
/// listeners because of its playlists; what a listener costs (its fan-out,
/// its repair, its HTTP) stays in the term.
pub static PLAYLIST_CPU_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The process's CPU time less [`PLAYLIST_CPU_US`]: the CPU counter of
/// the load's CPU term.
pub fn process_cpu_us_for_listeners() -> Option<u64> {
    Some(process_cpu_us()?.saturating_sub(PLAYLIST_CPU_US.load(Ordering::Relaxed)))
}

/// The CPUs this process may run on: its affinity and its cgroup's quota
/// (a lab node's CPUQuota, an instance's vCPUs).
pub fn available_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|count| u32::try_from(count.get()).unwrap_or(u32::MAX))
        .unwrap_or(1)
}

/// What a new playback session would add to the node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlaybackRequest<'a> {
    pub session_id: Option<&'a str>,
    /// Playlist that the request reads, when the path names one.
    pub stream_id: Option<u64>,
    /// The node is draining and takes no new session.
    pub draining: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaybackAdmission {
    Anonymous,
    Admitted,
    Existing,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct EdgeAdmission {
    inner: Arc<EdgeAdmissionInner>,
}

/// A cumulative count of bytes that the node sends without passing them
/// through [`EdgeAdmission::record_bytes`]: the client UDP lanes, whose
/// real-time senders count into atomics and take no lock (Phase AM item 8).
/// It is read when the load is read, and what it rose by since the last
/// read is spread over the seconds in between.
pub type EgressCounter = Arc<dyn Fn() -> u64 + Send + Sync>;

struct CounterSource {
    read: EgressCounter,
    last: u64,
    last_second: u64,
}

impl std::fmt::Debug for CounterSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CounterSource")
            .field("last", &self.last)
            .field("last_second", &self.last_second)
            .finish()
    }
}

#[derive(Debug)]
struct EdgeAdmissionInner {
    config: EdgeAdmissionConfig,
    started: Instant,
    window: Mutex<RollingEgressWindow>,
    counters: Mutex<Vec<CounterSource>>,
    /// The CPU term (wave 9 C2), once set.
    cpu: Mutex<Option<CpuSource>>,
    overloaded: AtomicBool,
    rejected_requests: AtomicU64,
    admitted_sessions: AtomicU64,
    sessions: Mutex<HashMap<String, PlaybackSession>>,
    /// When a request last pruned the idle sessions (elapsed ms + 1; 0:
    /// never). A request prunes at most every SESSION_PRUNE_INTERVAL_MS.
    sessions_pruned_ms: AtomicU64,
    playlist_bps: Mutex<HashMap<u64, u64>>,
    /// Requests that a reader of each playlist makes in a second.
    playlist_request_rate: Mutex<HashMap<u64, u64>>,
}

#[derive(Debug, Clone, Copy)]
struct PlaybackSession {
    last_seen_ms: u64,
    stream_id: Option<u64>,
    /// The node has sent the session to another cache. It no longer counts
    /// towards the load of this node, but requests that it still makes here
    /// are served.
    moved: bool,
}

impl Default for EdgeAdmission {
    fn default() -> Self {
        Self::new(EdgeAdmissionConfig::default())
    }
}

impl EdgeAdmission {
    pub fn new(config: EdgeAdmissionConfig) -> Self {
        Self {
            inner: Arc::new(EdgeAdmissionInner {
                config: config.normalized(),
                started: Instant::now(),
                window: Mutex::new(RollingEgressWindow::default()),
                counters: Mutex::new(Vec::new()),
                cpu: Mutex::new(None),
                overloaded: AtomicBool::new(false),
                rejected_requests: AtomicU64::new(0),
                admitted_sessions: AtomicU64::new(0),
                sessions: Mutex::new(HashMap::new()),
                sessions_pruned_ms: AtomicU64::new(0),
                playlist_bps: Mutex::new(HashMap::new()),
                playlist_request_rate: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn record_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let second = self.inner.started.elapsed().as_secs();
        if let Ok(mut window) = self.inner.window.lock() {
            window.record(second, bytes as u64, self.inner.config.window_seconds);
        }
    }

    pub fn snapshot(&self) -> EdgeAdmissionSnapshot {
        let second = self.inner.started.elapsed().as_secs();
        self.snapshot_at(second)
    }

    /// Count the egress of `read` (cumulative bytes) in the observed rate,
    /// and so in `load_permille` and the free capacity that the node
    /// advertises, and in its overload. What it counted before now is not
    /// egress of the window.
    pub fn add_egress_counter(&self, read: EgressCounter) {
        let second = self.inner.started.elapsed().as_secs();
        let last = read();
        self.inner
            .counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(CounterSource {
                read,
                last,
                last_second: second,
            });
    }

    /// Count the process's CPU in the load (wave 9 C2): `read` gives its
    /// CPU time, `cpus` the CPUs it may use, and the mean over `window_ms`
    /// at `full_percent` of them reads 1,000 permille. An edge's LL-HLS
    /// readers (and any per-listener work) cost CPU that their bytes do
    /// not show: 3 readers of 1 ms parts and ~50 UDP listeners took a lab
    /// edge to 135-148 % of its 200 % while its bytes read 300-570 permille.
    pub fn set_cpu_counter(
        &self,
        read: CpuCounter,
        cpus: u32,
        full_percent: u8,
        window_ms: u64,
    ) {
        let mut window = CpuWindow::default();
        if let Some(cpu_us) = read() {
            window.record(self.elapsed_ms(), cpu_us, window_ms);
        }
        *self
            .inner
            .cpu
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(CpuSource {
            read,
            cpus: cpus.max(1),
            full_percent: full_percent.clamp(1, 100),
            window_ms: window_ms.max(1_000),
            window,
        });
    }

    /// The CPU term now (a sample taken), 0 without a CPU counter.
    fn cpu_permille_now(&self) -> u32 {
        let elapsed_ms = self.elapsed_ms();
        let mut cpu = self
            .inner
            .cpu
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(source) = cpu.as_mut() else {
            return 0;
        };
        if let Some(cpu_us) = (source.read)() {
            source.window.record(elapsed_ms, cpu_us, source.window_ms);
        }
        source
            .window
            .permille(source.cpus, source.full_percent)
            .unwrap_or(0)
    }

    /// Whether the node is overloaded now (the measured egress, with the
    /// snapshot's hysteresis): a new client is refused while it is.
    pub fn overloaded_now(&self) -> bool {
        self.overloaded_at(self.inner.started.elapsed().as_secs())
    }

    /// The counters' rise since their last read into the window, spread
    /// evenly over the seconds since then (within the window).
    fn fold_counters(&self, second: u64) {
        let mut counters = self
            .inner
            .counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if counters.is_empty() {
            return;
        }
        let window_seconds = self.inner.config.window_seconds;
        let mut window = self
            .inner
            .window
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for source in counters.iter_mut() {
            let value = (source.read)();
            let delta = value.saturating_sub(source.last);
            source.last = value;
            let from = source.last_second;
            source.last_second = source.last_second.max(second);
            if delta == 0 {
                continue;
            }
            // The seconds after the last read up to this one; a read in the
            // same second books it there.
            let seconds = second.saturating_sub(from).max(1);
            let first = second + 1 - seconds;
            let share = delta / seconds;
            let mut rest = delta - share * seconds;
            for at in first.max(second.saturating_sub(window_seconds.saturating_sub(1)))..=second {
                let bytes = share + std::mem::take(&mut rest);
                window.record(at, bytes, window_seconds);
            }
        }
    }

    pub fn record_rejection(&self) {
        self.inner.rejected_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn admit_playback(&self, request: PlaybackRequest<'_>) -> PlaybackAdmission {
        // Every playback request asks: only the overload state, not the
        // whole snapshot, which walks every session twice.
        let overloaded = self.overloaded_at(self.inner.started.elapsed().as_secs());
        let elapsed_ms = self.elapsed_ms();
        self.admit_playback_at(request, overloaded, elapsed_ms)
    }

    /// Whether the measured egress overloads the node at `second`, with the
    /// hysteresis of the snapshot, which it updates.
    fn overloaded_at(&self, second: u64) -> bool {
        self.fold_counters(second);
        let (observed_bps, observation_seconds) = self
            .inner
            .window
            .lock()
            .map(|mut window| window.rate(second, self.inner.config.window_seconds))
            .unwrap_or((0, 0));
        let was_overloaded = self.inner.overloaded.load(Ordering::Relaxed);
        let overloaded = if was_overloaded {
            observed_bps >= self.inner.config.recovery_bps()
        } else {
            observation_seconds >= self.inner.config.min_sustained_seconds
                && observed_bps >= self.inner.config.admission_bps()
        };
        self.inner.overloaded.store(overloaded, Ordering::Relaxed);
        overloaded
    }

    /// Record the measured bit rate of a playlist that this node holds. A new
    /// session is admitted against the bit rate of the playlist that it asks
    /// for.
    ///
    /// The node keeps the highest rate that it has measured for the playlist.
    /// The measured rate follows the content, and a session admitted during a
    /// quiet passage still needs the rate of a busy one.
    pub fn set_playlist_bps(&self, stream_id: u64, bps: u64) {
        if bps == 0 {
            return;
        }
        let mut playlist_bps = self
            .inner
            .playlist_bps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let peak = playlist_bps.entry(stream_id).or_insert(bps);
        *peak = (*peak).max(bps);
    }

    /// Record how many requests a reader of a playlist makes in a second:
    /// one for each part, and a playlist reload now and then.
    pub fn set_playlist_request_rate(&self, stream_id: u64, requests_per_sec: u64) {
        self.inner
            .playlist_request_rate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(stream_id, requests_per_sec);
    }

    /// Forget the bit rate of a playlist that this node no longer holds.
    pub fn clear_playlist_bps(&self, stream_id: u64) {
        self.inner
            .playlist_bps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&stream_id);
        self.inner
            .playlist_request_rate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&stream_id);
    }

    /// Requests that a reader of a playlist makes in a second, when known.
    pub fn playlist_request_rate(&self, stream_id: u64) -> Option<u64> {
        self.inner
            .playlist_request_rate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&stream_id)
            .copied()
    }

    /// Requests that this node answers in a second with its processor full.
    pub fn request_capacity_per_sec(&self) -> u64 {
        self.inner.config.request_capacity_per_sec
    }

    /// The highest bit rate that this node has measured for a playlist.
    pub fn measured_playlist_bps(&self, stream_id: u64) -> Option<u64> {
        self.inner
            .playlist_bps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&stream_id)
            .copied()
    }

    /// What one session of a playlist is charged, in bits per second: its
    /// highest measured bit rate, or the share of the node that its requests
    /// take, whichever is larger. `None` while neither is known.
    pub fn playlist_bps(&self, stream_id: u64) -> Option<u64> {
        let measured = self
            .inner
            .playlist_bps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&stream_id)
            .copied();
        let requests = self.request_cost_bps(stream_id);
        match (measured, requests) {
            (Some(measured), Some(requests)) => Some(measured.max(requests)),
            (measured, requests) => measured.or(requests),
        }
    }

    /// The share of the capacity of the node that the requests of one
    /// session take, in bits per second.
    fn request_cost_bps(&self, stream_id: u64) -> Option<u64> {
        let capacity = self.inner.config.request_capacity_per_sec;
        if capacity == 0 {
            return None;
        }
        let rate = self
            .inner
            .playlist_request_rate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&stream_id)
            .copied()?;
        Some(
            (u128::from(self.inner.config.capacity_bps) * u128::from(rate) / u128::from(capacity))
                .min(u128::from(u64::MAX)) as u64,
        )
    }

    /// Record that this node has sent a session to another cache. Its
    /// commitment is free for another reader at once. The parts that the
    /// session was already told about are still served to it.
    pub fn release_session(&self, session_id: &str) -> bool {
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match sessions.get_mut(session_id) {
            Some(session) if !session.moved => {
                session.moved = true;
                true
            }
            _ => false,
        }
    }

    /// Sessions already sent to another cache that made a request here in
    /// the last `within_ms`: readers that still fetch the parts they were
    /// told about. A draining edge waits for them, as the session gauge no
    /// longer counts them.
    pub fn moved_seen_within(&self, within_ms: u64) -> u64 {
        self.moved_seen_within_at(within_ms, self.elapsed_ms())
    }

    fn moved_seen_within_at(&self, within_ms: u64, elapsed_ms: u64) -> u64 {
        self.inner
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|session| {
                session.moved && elapsed_ms.saturating_sub(session.last_seen_ms) <= within_ms
            })
            .count() as u64
    }

    /// Active sessions of each playlist.
    pub fn sessions_by_playlist(&self) -> HashMap<u64, u64> {
        let elapsed_ms = self.elapsed_ms();
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_sessions(
            &mut sessions,
            elapsed_ms,
            self.inner.config.session_idle_seconds,
        );
        let mut by_playlist = HashMap::new();
        for session in sessions.values().filter(|session| !session.moved) {
            if let Some(stream_id) = session.stream_id {
                *by_playlist.entry(stream_id).or_insert(0_u64) += 1;
            }
        }
        by_playlist
    }

    fn elapsed_ms(&self) -> u64 {
        self.inner
            .started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }

    fn committed_bps(&self, sessions: &HashMap<String, PlaybackSession>) -> u64 {
        let mut charges: HashMap<u64, u64> = HashMap::new();
        sessions
            .values()
            .filter(|session| !session.moved)
            .filter_map(|session| session.stream_id)
            .filter_map(|stream_id| {
                if let Some(charge) = charges.get(&stream_id) {
                    return Some(*charge);
                }
                let charge = self.playlist_bps(stream_id)?;
                charges.insert(stream_id, charge);
                Some(charge)
            })
            .fold(0_u64, |total, bps| total.saturating_add(bps))
    }

    fn snapshot_at(&self, second: u64) -> EdgeAdmissionSnapshot {
        self.fold_counters(second);
        let (observed_bps, observation_seconds) = self
            .inner
            .window
            .lock()
            .map(|mut window| window.rate(second, self.inner.config.window_seconds))
            .unwrap_or((0, 0));
        let admission_bps = self.inner.config.admission_bps();
        let recovery_bps = self.inner.config.recovery_bps();
        let was_overloaded = self.inner.overloaded.load(Ordering::Relaxed);
        let overloaded = if was_overloaded {
            observed_bps >= recovery_bps
        } else {
            observation_seconds >= self.inner.config.min_sustained_seconds
                && observed_bps >= admission_bps
        };
        self.inner.overloaded.store(overloaded, Ordering::Relaxed);

        let (active_sessions, committed_bps) = {
            let elapsed_ms = self.elapsed_ms();
            let mut sessions = self
                .inner
                .sessions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            prune_sessions(
                &mut sessions,
                elapsed_ms,
                self.inner.config.session_idle_seconds,
            );
            (
                active_session_count(&sessions),
                self.committed_bps(&sessions),
            )
        };
        let session_limit = self.inner.config.session_limit as u64;
        let mut load_permille = permille(observed_bps.max(committed_bps), admission_bps);
        if session_limit > 0 {
            load_permille = load_permille.max(permille(active_sessions, session_limit));
        }
        let cpu_permille = self.cpu_permille_now();
        load_permille = load_permille.max(cpu_permille);
        if overloaded {
            load_permille = load_permille.max(1_000);
        }

        EdgeAdmissionSnapshot {
            capacity_bps: self.inner.config.capacity_bps,
            observed_bps,
            admission_bps,
            recovery_bps,
            observation_seconds,
            window_seconds: self.inner.config.window_seconds,
            overloaded,
            rejected_requests: self.inner.rejected_requests.load(Ordering::Relaxed),
            active_sessions,
            admitted_sessions: self.inner.admitted_sessions.load(Ordering::Relaxed),
            session_limit,
            committed_bps,
            load_permille,
            cpu_permille,
        }
    }

    fn admit_playback_at(
        &self,
        request: PlaybackRequest<'_>,
        overloaded: bool,
        elapsed_ms: u64,
    ) -> PlaybackAdmission {
        let Some(session_id) = request.session_id else {
            return if overloaded || request.draining {
                PlaybackAdmission::Rejected
            } else {
                PlaybackAdmission::Anonymous
            };
        };
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // A walk of every session for every request is quadratic in the
        // readers: prune now and then. A session is idle after seconds.
        let pruned = self.inner.sessions_pruned_ms.load(Ordering::Relaxed);
        if pruned == 0
            || elapsed_ms < pruned - 1
            || elapsed_ms - (pruned - 1) >= SESSION_PRUNE_INTERVAL_MS
        {
            prune_sessions(
                &mut sessions,
                elapsed_ms,
                self.inner.config.session_idle_seconds,
            );
            self.inner
                .sessions_pruned_ms
                .store(elapsed_ms + 1, Ordering::Relaxed);
        }
        if let Some(session) = sessions.get_mut(session_id) {
            session.last_seen_ms = elapsed_ms;
            if request.stream_id.is_some() {
                session.stream_id = request.stream_id;
            }
            return PlaybackAdmission::Existing;
        }
        if overloaded || request.draining {
            return PlaybackAdmission::Rejected;
        }
        let session_limit = self.inner.config.session_limit;
        if session_limit > 0 && active_session_count(&sessions) >= session_limit as u64 {
            return PlaybackAdmission::Rejected;
        }
        // The session is entitled to the bit rate of its playlist. Admit it
        // only when the sessions already admitted leave that much room. This
        // acts before the measured egress has risen.
        if let Some(playlist_bps) = request
            .stream_id
            .and_then(|stream_id| self.playlist_bps(stream_id))
        {
            let committed_bps = self.committed_bps(&sessions);
            if committed_bps.saturating_add(playlist_bps) > self.inner.config.admission_bps() {
                return PlaybackAdmission::Rejected;
            }
        }
        if sessions.len() >= self.inner.config.max_sessions {
            if let Some(oldest) = sessions
                .iter()
                .min_by_key(|(_, session)| session.last_seen_ms)
                .map(|(session_id, _)| session_id.clone())
            {
                sessions.remove(&oldest);
            }
        }
        sessions.insert(
            session_id.to_owned(),
            PlaybackSession {
                last_seen_ms: elapsed_ms,
                stream_id: request.stream_id,
                moved: false,
            },
        );
        self.inner.admitted_sessions.fetch_add(1, Ordering::Relaxed);
        PlaybackAdmission::Admitted
    }

    #[cfg(test)]
    fn active_sessions_at(&self, elapsed_ms: u64) -> u64 {
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_sessions(
            &mut sessions,
            elapsed_ms,
            self.inner.config.session_idle_seconds,
        );
        active_session_count(&sessions)
    }

    #[cfg(test)]
    fn record_bytes_at(&self, second: u64, bytes: u64) {
        self.inner
            .window
            .lock()
            .unwrap()
            .record(second, bytes, self.inner.config.window_seconds);
    }
}

#[derive(Debug, Default)]
struct RollingEgressWindow {
    buckets: VecDeque<EgressBucket>,
}

#[derive(Debug, Clone, Copy)]
struct EgressBucket {
    second: u64,
    bytes: u64,
}

impl RollingEgressWindow {
    fn record(&mut self, second: u64, bytes: u64, window_seconds: u64) {
        let newest = self.buckets.back().map_or(second, |last| last.second.max(second));
        self.prune(newest, window_seconds);
        if newest.saturating_sub(second) >= window_seconds {
            return;
        }
        // Mostly the newest second; the counters folded at a read can book
        // a few seconds before it (the buckets stay in order).
        match self
            .buckets
            .iter()
            .rposition(|bucket| bucket.second <= second)
        {
            Some(index) if self.buckets[index].second == second => {
                let bucket = &mut self.buckets[index];
                bucket.bytes = bucket.bytes.saturating_add(bytes);
            }
            Some(index) => self
                .buckets
                .insert(index + 1, EgressBucket { second, bytes }),
            None => self.buckets.push_front(EgressBucket { second, bytes }),
        }
    }

    fn rate(&mut self, second: u64, window_seconds: u64) -> (u64, u64) {
        self.prune(second, window_seconds);
        let Some(first) = self.buckets.front() else {
            return (0, 0);
        };
        let observation_seconds = second
            .saturating_sub(first.second)
            .saturating_add(1)
            .min(window_seconds);
        let bytes = self
            .buckets
            .iter()
            .map(|bucket| bucket.bytes)
            .fold(0_u64, u64::saturating_add);
        (
            bytes.saturating_mul(8) / observation_seconds.max(1),
            observation_seconds,
        )
    }

    fn prune(&mut self, second: u64, window_seconds: u64) {
        let oldest = second.saturating_sub(window_seconds.saturating_sub(1));
        while self
            .buckets
            .front()
            .is_some_and(|bucket| bucket.second < oldest)
        {
            self.buckets.pop_front();
        }
    }
}

fn percent_of(value: u64, percent: u8) -> u64 {
    value.saturating_mul(u64::from(percent)).saturating_div(100)
}

fn active_session_count(sessions: &HashMap<String, PlaybackSession>) -> u64 {
    sessions.values().filter(|session| !session.moved).count() as u64
}

fn permille(value: u64, limit: u64) -> u32 {
    if limit == 0 {
        return u32::MAX;
    }
    let scaled = u128::from(value) * 1_000 / u128::from(limit);
    scaled.min(u128::from(u32::MAX)) as u32
}

/// How often a playback request prunes the idle sessions at most.
const SESSION_PRUNE_INTERVAL_MS: u64 = 250;

fn prune_sessions(sessions: &mut HashMap<String, PlaybackSession>, now_ms: u64, idle_seconds: u64) {
    let idle_ms = idle_seconds.saturating_mul(1_000);
    sessions.retain(|_, session| now_ms.saturating_sub(session.last_seen_ms) < idle_ms);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_is_charged_for_its_requests_when_they_cost_more_than_its_bits() {
        // 100 Mbit/s of network, admission at 85%, and 9,000 requests a
        // second of processor.
        let admission = EdgeAdmission::new(EdgeAdmissionConfig {
            capacity_bps: 100_000_000,
            request_capacity_per_sec: 9_000,
            ..EdgeAdmissionConfig::default()
        });
        // 4K video in 200 ms parts: seven requests a second. The bit rate
        // is what a session costs.
        admission.set_playlist_bps(1, 16_500_000);
        admission.set_playlist_request_rate(1, 7);
        assert_eq!(admission.playlist_bps(1), Some(16_500_000));
        // Opus in 5 ms parts: 96 kbit/s and 202 requests a second. The
        // requests cost 2.2% of the node, which is 2.24 Mbit/s of 100.
        admission.set_playlist_bps(3001, 96_000);
        admission.set_playlist_request_rate(3001, 202);
        assert_eq!(admission.playlist_bps(3001), Some(2_244_444));

        // The node admits 37 such readers, not the 885 that fit its network.
        let mut admitted = 0;
        for reader in 0..100 {
            let session = format!("reader-{reader}");
            let request = PlaybackRequest {
                session_id: Some(&session),
                stream_id: Some(3001),
                draining: false,
            };
            if matches!(
                admission.admit_playback(request),
                PlaybackAdmission::Admitted
            ) {
                admitted += 1;
            }
        }
        assert_eq!(admitted, 37);
        let snapshot = admission.snapshot();
        assert!(snapshot.load_permille >= 950, "{}", snapshot.load_permille);

        // Without a request capacity the bit rate alone counts.
        let by_bits = EdgeAdmission::new(EdgeAdmissionConfig {
            capacity_bps: 100_000_000,
            ..EdgeAdmissionConfig::default()
        });
        by_bits.set_playlist_bps(3001, 96_000);
        by_bits.set_playlist_request_rate(3001, 202);
        assert_eq!(by_bits.playlist_bps(3001), Some(96_000));
    }

    fn admission() -> EdgeAdmission {
        EdgeAdmission::new(EdgeAdmissionConfig {
            capacity_bps: 1_000,
            admission_percent: 80,
            recovery_percent: 60,
            window_seconds: 4,
            min_sustained_seconds: 3,
            session_idle_seconds: 5,
            max_sessions: 2,
            session_limit: 0,
            request_capacity_per_sec: 0,
        })
    }

    fn session(session_id: &str) -> PlaybackRequest<'_> {
        PlaybackRequest {
            session_id: Some(session_id),
            ..PlaybackRequest::default()
        }
    }

    fn reader(session_id: &str, stream_id: u64) -> PlaybackRequest<'_> {
        PlaybackRequest {
            session_id: Some(session_id),
            stream_id: Some(stream_id),
            draining: false,
        }
    }

    /// Phase AM item 8: egress that the node counts in an atomic (the
    /// client UDP lanes) is in the observed rate, the load and the free
    /// capacity, spread over the seconds since the last read.
    #[test]
    fn a_counter_of_udp_egress_is_in_the_load() {
        let admission = EdgeAdmission::new(EdgeAdmissionConfig {
            capacity_bps: 100_000,
            admission_percent: 80,
            recovery_percent: 60,
            window_seconds: 4,
            min_sustained_seconds: 2,
            ..EdgeAdmissionConfig::default()
        });
        let sent = Arc::new(AtomicU64::new(5_000));
        let read = Arc::clone(&sent);
        admission.add_egress_counter(Arc::new(move || read.load(Ordering::Relaxed)));
        // What it had counted before it was added is not egress.
        assert_eq!(admission.snapshot_at(0).observed_bps, 0);
        // 4,000 bytes over 4 seconds read at once: 1,000 bytes a second.
        sent.fetch_add(4_000, Ordering::Relaxed);
        let snapshot = admission.snapshot_at(4);
        assert_eq!(snapshot.observation_seconds, 4);
        assert_eq!(snapshot.observed_bps, 8_000);
        assert_eq!(snapshot.load_permille, 100, "8 kbit/s of an 80 kbit/s limit");
        // HTTP bytes add to it in the same seconds.
        admission.record_bytes_at(4, 4_000);
        assert_eq!(admission.snapshot_at(4).observed_bps, 16_000);
        // A rise past the limit, sustained, overloads the node.
        sent.fetch_add(80_000, Ordering::Relaxed);
        let snapshot = admission.snapshot_at(6);
        assert!(snapshot.overloaded, "{snapshot:?}");
        assert!(snapshot.load_permille >= 1_000, "{snapshot:?}");
        // A read long after books only the window's share.
        sent.fetch_add(100_000, Ordering::Relaxed);
        let snapshot = admission.snapshot_at(106);
        assert_eq!(snapshot.observed_bps, 100_000 * 8 / 100);
    }

    #[test]
    fn rejects_only_after_sustained_high_egress() {
        let admission = admission();
        admission.record_bytes_at(0, 100);
        admission.record_bytes_at(1, 100);
        assert!(!admission.snapshot_at(1).overloaded);

        admission.record_bytes_at(2, 100);
        let snapshot = admission.snapshot_at(2);
        assert_eq!(snapshot.observed_bps, 800);
        assert_eq!(snapshot.observation_seconds, 3);
        assert!(snapshot.overloaded);
    }

    #[test]
    fn hysteresis_keeps_rejecting_until_recovery_boundary() {
        let admission = admission();
        for second in 0..3 {
            admission.record_bytes_at(second, 100);
        }
        assert!(admission.snapshot_at(2).overloaded);

        assert!(admission.snapshot_at(3).overloaded);
        assert!(!admission.snapshot_at(4).overloaded);
    }

    #[test]
    fn old_buckets_leave_the_bounded_window() {
        let admission = admission();
        admission.record_bytes_at(0, 1_000);
        admission.record_bytes_at(10, 10);
        let snapshot = admission.snapshot_at(10);
        assert_eq!(snapshot.observed_bps, 80);
        assert_eq!(snapshot.observation_seconds, 1);
        assert!(!snapshot.overloaded);
    }

    #[test]
    fn config_keeps_recovery_at_or_below_admission() {
        let config = EdgeAdmissionConfig {
            capacity_bps: 0,
            admission_percent: 0,
            recovery_percent: 100,
            window_seconds: 0,
            min_sustained_seconds: 100,
            session_idle_seconds: 0,
            max_sessions: 0,
            session_limit: 0,
            request_capacity_per_sec: 0,
        }
        .normalized();
        assert_eq!(config.capacity_bps, 1);
        assert_eq!(config.admission_percent, 1);
        assert_eq!(config.recovery_percent, 1);
        assert_eq!(config.window_seconds, 1);
        assert_eq!(config.min_sustained_seconds, 1);
        assert_eq!(config.session_idle_seconds, 1);
        assert_eq!(config.max_sessions, 1);
    }

    #[test]
    fn preserves_admitted_sessions_during_overload() {
        let admission = admission();
        assert_eq!(
            admission.admit_playback_at(session("existing"), false, 0),
            PlaybackAdmission::Admitted
        );
        assert_eq!(
            admission.admit_playback_at(session("existing"), true, 1_000),
            PlaybackAdmission::Existing
        );
        assert_eq!(
            admission.admit_playback_at(session("new"), true, 1_000),
            PlaybackAdmission::Rejected
        );
        assert_eq!(
            admission.admit_playback_at(PlaybackRequest::default(), true, 1_000),
            PlaybackAdmission::Rejected
        );
    }

    #[test]
    fn session_limit_rejects_a_new_session_and_keeps_admitted_ones() {
        let admission = EdgeAdmission::new(EdgeAdmissionConfig {
            capacity_bps: 1_000_000,
            session_limit: 2,
            request_capacity_per_sec: 0,
            max_sessions: 100,
            ..EdgeAdmissionConfig::default()
        });
        assert_eq!(
            admission.admit_playback_at(session("one"), false, 0),
            PlaybackAdmission::Admitted
        );
        assert_eq!(
            admission.admit_playback_at(session("two"), false, 0),
            PlaybackAdmission::Admitted
        );
        assert_eq!(
            admission.admit_playback_at(session("three"), false, 0),
            PlaybackAdmission::Rejected
        );
        assert_eq!(
            admission.admit_playback_at(session("one"), false, 1_000),
            PlaybackAdmission::Existing
        );
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.session_limit, 2);
        assert_eq!(snapshot.load_permille, 1_000);
    }

    #[test]
    fn sessions_are_admitted_against_the_bit_rate_of_their_playlist() {
        // 100 Mbit/s node, 85 Mbit/s admission limit, one 16 Mbit/s playlist
        // and one 2 Mbit/s playlist.
        let admission = EdgeAdmission::new(EdgeAdmissionConfig {
            capacity_bps: 100_000_000,
            ..EdgeAdmissionConfig::default()
        });
        admission.set_playlist_bps(1, 16_000_000);
        admission.set_playlist_bps(2001, 2_000_000);
        for index in 0..5 {
            let id = format!("video-{index}");
            assert_eq!(
                admission.admit_playback_at(reader(&id, 1), false, 0),
                PlaybackAdmission::Admitted
            );
        }
        // 80 Mbit/s is committed. A sixth 4K reader does not fit. Two audio
        // readers do, and a third does not.
        assert_eq!(
            admission.admit_playback_at(reader("video-5", 1), false, 0),
            PlaybackAdmission::Rejected
        );
        assert_eq!(
            admission.admit_playback_at(reader("audio-0", 2001), false, 0),
            PlaybackAdmission::Admitted
        );
        assert_eq!(
            admission.admit_playback_at(reader("audio-1", 2001), false, 0),
            PlaybackAdmission::Admitted
        );
        assert_eq!(
            admission.admit_playback_at(reader("audio-2", 2001), false, 0),
            PlaybackAdmission::Rejected
        );
        // A quiet passage does not lower the rate that a session is held to.
        admission.set_playlist_bps(1, 9_000_000);
        assert_eq!(admission.playlist_bps(1), Some(16_000_000));
        admission.clear_playlist_bps(7);
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.committed_bps, 84_000_000);
        assert_eq!(snapshot.load_permille, 988);
        assert_eq!(admission.sessions_by_playlist()[&1], 5);
        assert_eq!(admission.sessions_by_playlist()[&2001], 2);
    }

    #[test]
    fn a_draining_node_keeps_its_sessions_and_takes_no_new_one() {
        let admission = admission();
        assert_eq!(
            admission.admit_playback_at(session("existing"), false, 0),
            PlaybackAdmission::Admitted
        );
        let draining = |session_id| PlaybackRequest {
            session_id,
            stream_id: None,
            draining: true,
        };
        assert_eq!(
            admission.admit_playback_at(draining(Some("existing")), false, 1_000),
            PlaybackAdmission::Existing
        );
        assert_eq!(
            admission.admit_playback_at(draining(Some("new")), false, 1_000),
            PlaybackAdmission::Rejected
        );
        assert_eq!(
            admission.admit_playback_at(draining(None), false, 1_000),
            PlaybackAdmission::Rejected
        );
        // The node sends the session to another cache. The session no longer
        // counts, and what it still asks for here is served.
        assert!(admission.release_session("existing"));
        assert!(!admission.release_session("existing"));
        assert_eq!(admission.active_sessions_at(1_000), 0);
        assert_eq!(
            admission.admit_playback_at(draining(Some("existing")), false, 1_500),
            PlaybackAdmission::Existing
        );
    }

    #[test]
    fn expires_idle_sessions_and_bounds_the_registry() {
        let admission = admission();
        admission.admit_playback_at(session("one"), false, 0);
        admission.admit_playback_at(session("two"), false, 1_000);
        admission.admit_playback_at(session("three"), false, 2_000);
        assert_eq!(admission.active_sessions_at(2_000), 2);
        assert_eq!(
            admission.admit_playback_at(session("one"), true, 2_000),
            PlaybackAdmission::Rejected
        );
        assert_eq!(admission.active_sessions_at(7_000), 0);
    }

    #[test]
    fn a_moved_session_that_still_fetches_is_counted() {
        let admission = admission();
        assert_eq!(
            admission.admit_playback_at(session("one"), false, 1_000),
            PlaybackAdmission::Admitted
        );
        assert_eq!(
            admission.admit_playback_at(session("two"), false, 1_000),
            PlaybackAdmission::Admitted
        );
        assert_eq!(admission.moved_seen_within_at(1_000, 1_000), 0);
        // Both are sent away: the session gauge no longer counts them.
        assert!(admission.release_session("one"));
        assert!(admission.release_session("two"));
        assert_eq!(admission.active_sessions_at(1_000), 0);
        assert_eq!(admission.moved_seen_within_at(1_000, 1_500), 2);
        // One still fetches a part it was told about.
        assert_eq!(
            admission.admit_playback_at(session("one"), false, 2_400),
            PlaybackAdmission::Existing
        );
        assert_eq!(admission.moved_seen_within_at(1_000, 2_500), 1);
        assert_eq!(admission.moved_seen_within_at(1_000, 3_401), 0);
        assert_eq!(admission.moved_seen_within(60_000), 2);
    }

    /// The CPU term's counter leaves out what the tees booked: a 4K edge
    /// whose tee spends 1.2 CPUs on its playlist and 0.3 on its listeners
    /// reads 0.3 of its 2 CPUs (176 at 85 %), not 1.5 (882).
    #[test]
    fn the_cpu_term_leaves_out_the_playlists_cpu() {
        // The process has run for well under a second here, and other
        // tests' tees book into the same counter: compare with what it
        // holds now, not with fixed figures.
        let all = process_cpu_us().unwrap();
        let booked = PLAYLIST_CPU_US.load(Ordering::Relaxed);
        let for_listeners = process_cpu_us_for_listeners().unwrap();
        assert!(for_listeners <= all.max(process_cpu_us().unwrap()));
        assert!(for_listeners <= process_cpu_us().unwrap().saturating_sub(booked));
        assert!(thread_cpu_us().is_some());
        assert_eq!(cpu_permille(1_500_000 - 1_200_000, 1_000_000, 2, 85), 176);
        assert_eq!(cpu_permille(1_500_000, 1_000_000, 2, 85), 882);
    }

    #[test]
    fn the_cpu_term_is_the_mean_over_its_window_of_the_cpus_at_the_full_percent() {
        // 1.7 CPUs busy of 2, full at 85 %: 1,000.
        assert_eq!(cpu_permille(1_700_000, 1_000_000, 2, 85), 1_000);
        // 0.85 of 2 at 85 %: 500; any time over no wall time: 0.
        assert_eq!(cpu_permille(850_000, 1_000_000, 2, 85), 500);
        assert_eq!(cpu_permille(5, 0, 2, 85), 0);
        let mut window = CpuWindow::default();
        assert_eq!(window.permille(2, 85), None);
        // A sample every 250 ms of 1 CPU busy for 4 s, then 2 CPUs busy
        // for one second: the 5 s mean, not the last second's.
        for step in 0..=16 {
            window.record(step * 250, step * 250_000, 5_000);
        }
        for step in 17..=20 {
            window.record(step * 250, 4_000_000 + (step - 16) * 500_000, 5_000);
        }
        // (4 s x 1 CPU + 1 s x 2 CPUs) / 5 s = 1.2 CPUs of 1.7: 705.
        assert_eq!(window.permille(2, 85), Some(705));
        // Samples closer than the spacing, or that go back, are not kept.
        let kept = window.samples.len();
        window.record(20 * 250 + 10, 9_000_000, 5_000);
        window.record(21 * 250, 5_999_999, 5_000);
        assert_eq!(window.samples.len(), kept);
        // Only the window and the sample before it stay.
        assert!(window.samples.front().unwrap().0 <= 20 * 250 - 5_000);
        assert!(window.samples.len() <= 22);
    }

    #[test]
    fn the_load_is_the_cpu_term_when_the_cpu_is_busier_than_the_bytes() {
        let admission = admission();
        let cpu = Arc::new(AtomicU64::new(0));
        let read = Arc::clone(&cpu);
        admission.set_cpu_counter(
            Arc::new(move || Some(read.load(Ordering::Relaxed))),
            2,
            DEFAULT_CPU_LOAD_FULL_PERCENT,
            DEFAULT_CPU_LOAD_WINDOW_MS,
        );
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.cpu_permille, 0);
        assert_eq!(snapshot.load_permille, 0);
        // 1.02 CPUs busy for 0.6 s of wall time (a sample at the start and
        // one now): 600 permille of 1.7 CPUs.
        std::thread::sleep(std::time::Duration::from_millis(600));
        let elapsed_us = admission.elapsed_ms() * 1_000;
        cpu.store(elapsed_us * 102 / 100, Ordering::Relaxed);
        let snapshot = admission.snapshot();
        assert!(
            (560..=640).contains(&snapshot.cpu_permille),
            "{snapshot:?}"
        );
        assert_eq!(snapshot.load_permille, snapshot.cpu_permille);
        // Bytes above it are the load again.
        admission.record_bytes_at(admission.inner.started.elapsed().as_secs(), 1_000_000_000);
        let snapshot = admission.snapshot();
        assert!(snapshot.load_permille >= 1_000, "{snapshot:?}");
        // Without a counter the term is 0, as before.
        assert_eq!(EdgeAdmission::default().snapshot().cpu_permille, 0);
        assert!(process_cpu_us().is_some());
        assert!(available_cpus() >= 1);
    }
}
