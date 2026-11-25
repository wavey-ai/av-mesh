//! CPU admission (rm/cpuadmit): an edge refuses or spills a new leg before
//! any admitted listener loses parts, by the CPU that actually saturates.
//!
//! Diagnosis (25 Nov). The load's CPU term of wave 9 C2 (the process's CPU
//! time over all its CPUs, full at 85 %) never fired in the lab's ramp of 1
//! ms Opus (300 legs an edge, 54 % of parts missing, edge processes at
//! 115-121 % of their 200 %). In process, with an edge's threads on two
//! CPUs of their own: the class thread takes 0.08-0.11 of a core whatever
//! the legs; the sends run on the two fan-out threads, which float over
//! both CPUs; and the receivers' kernel path of each datagram (loopback or
//! a lab veth) runs in the sender's softirq, which no thread's CPU clock
//! counts (`CONFIG_IRQ_TIME_ACCOUNTING`). At 300 legs the two CPUs were 1.91
//! busy (0.51 of it softirq) while the threads' clocks said 1.25, and the
//! fan-out began to shed; at 400 the CPUs were 2.00 busy, the threads 1.43
//! (the process term 841 permille, under its line), and 27 % of the parts
//! were missing. What fills is the node's CPUs as a whole, with the sends'
//! softirq; a pool thread can fill on a larger node before the CPUs do.
//!
//! So the term is the busiest of:
//! - `cpus`: the node's CPUs (its affinity, or `--cpu-admit-cpus`), busy
//!   time (user, nice, system, irq, softirq, steal) over their time, from
//!   `/proc/stat`, full at `--cpu-admit-cpus-full-percent` (80);
//! - each registered send thread (the class threads, the fan-out pool),
//!   full at `--cpu-admit-thread-full-percent` (70) of one core: a class
//!   thread's run and run-queue time (`schedstat`), a fan-out thread's wall
//!   time from a plan's start to its end (its own clock reads, once a plan,
//!   never a datagram: softirq and preemption inside its sends included).
//!
//! sampled every `--cpu-admit-sample-ms` (500) off the class threads and
//! taken over `--cpu-admit-window-ms` (2,000). The node's load is at least
//! this term plus the legs it admitted since the samples could show them
//! (each charged its datagrams at the measured cost a datagram, never less
//! than `--cpu-admit-datagram-ns`), so a wave of joins cannot overshoot
//! between samples. At 1,000 a new leg is refused `edge-busy` (a lone mover
//! is taken, as for bytes) and the entry spills it; over 1,000 for
//! `--cpu-admit-shed-after-ms` (2,000) the edge asks two-leg listeners to
//! give up their leg here (`MOVE shed`) until it is back under.
//!
//! Bytes, datagrams and measured times only: nothing here knows what a
//! stream carries.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(crate) const DEFAULT_CPUS_FULL_PERCENT: u32 = 80;
pub(crate) const DEFAULT_THREAD_FULL_PERCENT: u32 = 70;
pub(crate) const DEFAULT_WINDOW_MS: u64 = 2_000;
pub(crate) const DEFAULT_SAMPLE_MS: u64 = 500;
/// The floor of a client datagram's cost, ns: charge.md's edge process time
/// a listener-part (1.4 µs, PCM, loopback) and about half its host-wide
/// softirq a datagram (3.0-3.3 µs, both ends on loopback). Only a floor:
/// the measured cost (6-7 µs in process with the receivers' path on the
/// edge's CPUs) replaces it once the edge has sent 2,000 datagrams.
pub(crate) const DEFAULT_DATAGRAM_NS: u64 = 3_000;
/// While the edge has not measured what a datagram costs it (its fan-out
/// has not sent 2,000 in a window), the legs admitted and not yet in the
/// measured term may add at most this (thousandths of full): a burst of
/// joins at a cold edge, priced at the floor, cannot fill it. The lab's
/// veths cost 18-30 µs a datagram, ten times the floor: at 300 the cold
/// burst of 150 legs an edge took both CPUs to 1,000.
pub(crate) const DEFAULT_PENDING_MAX_PERMILLE: u32 = 100;
pub(crate) const DEFAULT_SHED_AFTER_MS: u64 = 2_000;
/// Legs asked to leave in one round at most.
pub(crate) const DEFAULT_SHED_PER_ROUND: usize = 32;
/// A measured cost a datagram is taken from this many fan-out sends in the
/// window at least (the lab's first look at 2,000, a window of a cold
/// burst's first sends, read a few µs and let 86 legs past a cap that the
/// next sample, at 22.7 µs, would have held).
const MIN_DATAGRAMS_FOR_COST: u64 = 20_000;
/// Legs are priced at the highest cost a datagram of this many of the
/// latest measurements (a cost read low once does not open the edge).
const COST_LOOKS: usize = 4;

/// The `--cpu-admit-*` settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CpuAdmitConfig {
    /// Busy permille of the node's CPUs (their mean) at which the term
    /// reads 1,000.
    pub(crate) cpus_full_permille: u32,
    /// Busy permille of one core at which a send thread's term reads 1,000.
    pub(crate) thread_full_permille: u32,
    pub(crate) window: Duration,
    pub(crate) sample_every: Duration,
    pub(crate) datagram_ns: u64,
    /// Before the edge has measured a datagram's cost, the legs admitted
    /// since the samples may add at most this.
    pub(crate) pending_max_permille: u32,
    /// Over full this long, the edge sheds (zero: it never does).
    pub(crate) shed_after: Duration,
    pub(crate) shed_per_round: usize,
    /// The node's CPUs; empty: the process's affinity.
    pub(crate) cpus: Vec<usize>,
    /// Tests on a shared box: the CPUs' busy time is this process's
    /// threads pinned to them (run time) and the CPUs' irq and softirq,
    /// not every task's (other tenants' builds land on any CPU). A node
    /// alone on its guest reads `/proc/stat` (`false`).
    pub(crate) own_threads_only: bool,
}

impl Default for CpuAdmitConfig {
    fn default() -> Self {
        Self {
            cpus_full_permille: DEFAULT_CPUS_FULL_PERCENT * 10,
            thread_full_permille: DEFAULT_THREAD_FULL_PERCENT * 10,
            window: Duration::from_millis(DEFAULT_WINDOW_MS),
            sample_every: Duration::from_millis(DEFAULT_SAMPLE_MS),
            datagram_ns: DEFAULT_DATAGRAM_NS,
            pending_max_permille: DEFAULT_PENDING_MAX_PERMILLE,
            shed_after: Duration::from_millis(DEFAULT_SHED_AFTER_MS),
            shed_per_round: DEFAULT_SHED_PER_ROUND,
            cpus: Vec::new(),
            own_threads_only: false,
        }
    }
}

/// What a registered thread does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ThreadGroup {
    /// A class thread (pinned on a deploy): in the term.
    Class,
    /// A fan-out thread: in the term.
    Fanout,
    /// A tee: reported, not in the term (its work is per part, not per
    /// leg: refusing legs does not lighten it; the CPUs term sees it).
    Tee,
    /// Another thread of the edge's (the window repair): reported.
    Other,
}

impl ThreadGroup {
    fn in_term(self) -> bool {
        matches!(self, Self::Class | Self::Fanout)
    }
}

#[derive(Debug, Clone)]
struct TrackedThread {
    tid: i32,
    name: String,
    group: ThreadGroup,
    /// Wall time busy, ns, kept by the thread itself (fan-out); `None`: its
    /// run and run-queue time are read from `schedstat`.
    busy_ns: Option<Arc<AtomicU64>>,
}

/// The threads of one edge that the term reads: each registers itself once,
/// when it starts.
#[derive(Debug, Clone, Default)]
pub(crate) struct ThreadRegistry(Arc<Mutex<Vec<TrackedThread>>>);

impl ThreadRegistry {
    /// The calling thread, as `name` of `group`; `busy_ns` if it keeps its
    /// own wall time busy.
    pub(crate) fn register_current(
        &self,
        name: impl Into<String>,
        group: ThreadGroup,
        busy_ns: Option<Arc<AtomicU64>>,
    ) {
        let tid = current_tid();
        let mut threads = self.0.lock().unwrap_or_else(|p| p.into_inner());
        threads.retain(|thread| thread.tid != tid);
        threads.push(TrackedThread {
            tid,
            name: name.into(),
            group,
            busy_ns,
        });
    }

    fn list(&self) -> Vec<TrackedThread> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

pub(crate) fn current_tid() -> i32 {
    // SAFETY: gettid takes no argument.
    (unsafe { libc::syscall(libc::SYS_gettid) }) as i32
}

/// A thread's run time and run-queue time so far, ns (`schedstat`).
fn thread_demand_ns(tid: i32) -> Option<u64> {
    let text = std::fs::read_to_string(format!("/proc/self/task/{tid}/schedstat")).ok()?;
    let mut fields = text.split_whitespace();
    let run: u64 = fields.next()?.parse().ok()?;
    let wait: u64 = fields.next()?.parse().ok()?;
    Some(run + wait)
}

/// Busy and total jiffies of `cpus` together (busy: user, nice, system,
/// irq, softirq and steal; total adds idle and iowait), from `/proc/stat`.
pub(crate) fn cpus_jiffies(cpus: &[usize]) -> Option<(u64, u64)> {
    let text = std::fs::read_to_string("/proc/stat").ok()?;
    let (mut busy, mut total) = (0_u64, 0_u64);
    let mut seen = false;
    for line in text.lines().skip(1) {
        let mut fields = line.split_whitespace();
        let Some(cpu) = fields
            .next()
            .and_then(|name| name.strip_prefix("cpu"))
            .and_then(|index| index.parse::<usize>().ok())
        else {
            break;
        };
        if !cpus.contains(&cpu) {
            continue;
        }
        let values: Vec<u64> = fields.take(8).filter_map(|v| v.parse().ok()).collect();
        if values.len() < 8 {
            continue;
        }
        let idle = values[3] + values[4];
        let all: u64 = values.iter().sum();
        busy += all - idle;
        total += all;
        seen = true;
    }
    seen.then_some((busy, total))
}

/// Irq and softirq jiffies of `cpus` together.
fn cpus_irq_jiffies(cpus: &[usize]) -> Option<u64> {
    let text = std::fs::read_to_string("/proc/stat").ok()?;
    let mut irq = 0;
    for line in text.lines().skip(1) {
        let mut fields = line.split_whitespace();
        let Some(cpu) = fields
            .next()
            .and_then(|name| name.strip_prefix("cpu"))
            .and_then(|index| index.parse::<usize>().ok())
        else {
            break;
        };
        if cpus.contains(&cpu) {
            let values: Vec<u64> = fields.take(7).filter_map(|v| v.parse().ok()).collect();
            irq += values.get(5).copied().unwrap_or(0) + values.get(6).copied().unwrap_or(0);
        }
    }
    Some(irq)
}

/// Run time, ns, of this process's threads whose affinity is within
/// `cpus`.
fn own_threads_ns(cpus: &[usize]) -> u64 {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return 0;
    };
    let mut total = 0;
    for task in tasks.flatten() {
        let Some(tid) = task.file_name().to_str().and_then(|tid| tid.parse::<i32>().ok()) else {
            continue;
        };
        // SAFETY: a zeroed cpu_set_t is valid; sched_getaffinity writes it.
        let within = unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::sched_getaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0
                && (0..libc::CPU_SETSIZE as usize)
                    .filter(|cpu| libc::CPU_ISSET(*cpu, &set))
                    .all(|cpu| cpus.contains(&cpu))
        };
        if !within {
            continue;
        }
        if let Some(run) = std::fs::read_to_string(task.path().join("schedstat"))
            .ok()
            .and_then(|text| text.split_whitespace().next()?.parse::<u64>().ok())
        {
            total += run;
        }
    }
    total
}

/// The CPUs of this process's affinity.
pub(crate) fn affinity_cpus() -> Vec<usize> {
    // SAFETY: a zeroed cpu_set_t is valid; sched_getaffinity writes it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Vec::new();
        }
        (0..libc::CPU_SETSIZE as usize)
            .filter(|cpu| libc::CPU_ISSET(*cpu, &set))
            .collect()
    }
}

fn jiffy_ns() -> u64 {
    // SAFETY: plain sysconf.
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as u64;
    1_000_000_000 / ticks
}

/// Cumulative counters the sampler reads at each sample.
pub(crate) type Counter = Arc<dyn Fn() -> u64 + Send + Sync>;

#[derive(Debug, Clone)]
struct Sample {
    at: Instant,
    cpus: Option<(u64, u64)>,
    /// (tid, ns) of each registered thread.
    threads: Vec<(i32, u64)>,
    datagrams: u64,
}

/// One registered thread's busy time over the window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ThreadBusy {
    pub(crate) name: String,
    /// Thousandths of one core.
    pub(crate) busy_permille: u32,
}

/// The term over the last window.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CpuVerdict {
    /// The busiest term, thousandths of its full point (1,000: full).
    pub(crate) permille: u32,
    /// Which: `cpus`, or a thread's name.
    pub(crate) term: String,
    /// The node's CPUs busy, thousandths (their mean).
    pub(crate) cpus_busy_permille: u32,
    pub(crate) cpus: u32,
    pub(crate) threads: Vec<ThreadBusy>,
    /// What one client datagram costs the send threads, ns (measured,
    /// never under the floor).
    pub(crate) datagram_ns: u64,
    /// The send capacity a leg's datagrams are priced against, ns of busy
    /// time a second at the full point: the smaller of the CPUs' and the
    /// send threads'.
    pub(crate) capacity_ns: u64,
}

impl CpuVerdict {
    /// What a leg of `datagrams_per_s` adds to the term, thousandths.
    pub(crate) fn leg_permille(&self, datagrams_per_s: f64) -> u32 {
        leg_permille(self.datagram_ns, self.capacity_ns, datagrams_per_s)
    }
}

/// A leg of `datagrams_per_s` at `datagram_ns` a datagram against
/// `capacity_ns`, thousandths (at least 1 for a leg that sends).
pub(crate) fn leg_permille(datagram_ns: u64, capacity_ns: u64, datagrams_per_s: f64) -> u32 {
    if capacity_ns == 0 || datagrams_per_s <= 0.0 {
        return 0;
    }
    let permille = datagrams_per_s * datagram_ns as f64 * 1_000.0 / capacity_ns as f64;
    (permille.ceil() as u32).max(1)
}

/// What an edge advertises of its CPU admission (`cpu_admit` of its edge
/// service), for the entry's charges and its counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CpuAdmitAdvert {
    /// The term with the legs admitted since (thousandths of full).
    pub(crate) permille: u32,
    pub(crate) term: String,
    pub(crate) datagram_ns: u64,
    pub(crate) capacity_ns: u64,
    /// The edge is over its full point and sheds.
    #[serde(default)]
    pub(crate) shedding: bool,
}

/// Counters of every decision (cumulative).
#[derive(Debug, Default)]
pub(crate) struct CpuAdmitStats {
    /// New legs refused at the edge, by the binding term.
    pub(crate) refused_cpus: AtomicU64,
    pub(crate) refused_thread: AtomicU64,
    /// New LL-HLS sessions refused at full.
    pub(crate) refused_http: AtomicU64,
    /// New legs refused while the legs admitted since the samples were at
    /// their cap (a burst at an edge with room: they come back or spill).
    pub(crate) refused_pending: AtomicU64,
    /// Legs admitted and charged.
    pub(crate) admitted: AtomicU64,
    /// Rounds over full, and legs asked to leave (`MOVE shed`).
    pub(crate) shed_rounds: AtomicU64,
    pub(crate) shed_hints: AtomicU64,
    /// Entry: caches passed over for a new leg because their CPU term
    /// (with this entry's charges) was full.
    pub(crate) spilled: AtomicU64,
    /// Samples taken.
    pub(crate) samples: AtomicU64,
}

#[derive(Debug, Default)]
struct State {
    samples: VecDeque<Sample>,
    verdict: CpuVerdict,
    /// The legs admitted lately: when, and their charge.
    charges: VecDeque<(Instant, u32)>,
    /// Over full since (measured term alone).
    over_since: Option<Instant>,
    /// Legs asked to leave lately: when, and their charge.
    shed: VecDeque<(Instant, u32)>,
    /// The highest of the latest measured costs a datagram (`None`: none
    /// measured yet: the edge is cold).
    last_cost: Option<u64>,
    costs: VecDeque<u64>,
}

/// One edge's CPU admission.
pub(crate) struct CpuAdmit {
    pub(crate) config: CpuAdmitConfig,
    cpus: Vec<usize>,
    threads: ThreadRegistry,
    datagrams: Counter,
    state: Mutex<State>,
    pub(crate) stats: CpuAdmitStats,
    started: Instant,
    /// Refusals and sheds on (the default); off, every leg is admitted and
    /// charged and none is shed, the term still measured (tests inject an
    /// overload so).
    enforce: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for CpuAdmit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CpuAdmit")
            .field("config", &self.config)
            .field("cpus", &self.cpus)
            .finish()
    }
}

/// Why a new leg is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CpuRefusal {
    Cpus,
    Thread,
    /// The legs admitted since the samples are at their cap.
    Pending,
}

impl CpuAdmit {
    /// `threads`: the edge's send threads (its fast lane's registry);
    /// `datagrams`: the sends its fan-out threads made so far.
    pub(crate) fn new(config: CpuAdmitConfig, threads: ThreadRegistry, datagrams: Counter) -> Self {
        let cpus = if config.cpus.is_empty() {
            affinity_cpus()
        } else {
            config.cpus.clone()
        };
        Self {
            config,
            cpus,
            threads,
            datagrams,
            state: Mutex::default(),
            stats: CpuAdmitStats::default(),
            started: Instant::now(),
            enforce: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Refuse and shed (`true`, the default), or only measure.
    pub(crate) fn set_enforce(&self, on: bool) {
        self.enforce.store(on, Ordering::Relaxed);
    }

    fn enforcing(&self) -> bool {
        self.enforce.load(Ordering::Relaxed)
    }

    /// The registered threads: (tid, name).
    pub(crate) fn threads(&self) -> Vec<(i32, String)> {
        self.threads
            .list()
            .into_iter()
            .map(|thread| (thread.tid, thread.name))
            .collect()
    }

    pub(crate) fn cpus(&self) -> &[usize] {
        &self.cpus
    }

    /// Take a sample now and judge the window.
    pub(crate) fn sample(&self) {
        let now = Instant::now();
        let threads = self.threads.list();
        let sample = Sample {
            at: now,
            cpus: if self.config.own_threads_only {
                // In ns: the threads' run time and the CPUs' irq time,
                // over the CPUs' wall time.
                cpus_irq_jiffies(&self.cpus).map(|irq| {
                    let elapsed = now.saturating_duration_since(self.started).as_nanos() as u64;
                    (
                        own_threads_ns(&self.cpus) + irq * jiffy_ns(),
                        elapsed * self.cpus.len().max(1) as u64,
                    )
                })
            } else {
                cpus_jiffies(&self.cpus)
            },
            threads: threads
                .iter()
                .filter_map(|thread| {
                    let ns = match &thread.busy_ns {
                        Some(busy) => Some(busy.load(Ordering::Relaxed)),
                        None => thread_demand_ns(thread.tid),
                    }?;
                    Some((thread.tid, ns))
                })
                .collect(),
            datagrams: (self.datagrams)(),
        };
        self.stats.samples.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.samples.push_back(sample);
        // The window, and the sample just before it.
        while state.samples.len() > 2
            && now.saturating_duration_since(state.samples[1].at) >= self.config.window
        {
            state.samples.pop_front();
        }
        let verdict = self.judge(&state, &threads);
        if let Some((verdict, measured)) = verdict {
            // Only a measured cost (never the floor) ends the cold cap.
            if let Some(measured) = measured {
                state.costs.push_back(measured);
                while state.costs.len() > COST_LOOKS {
                    state.costs.pop_front();
                }
                state.last_cost = state.costs.iter().copied().max();
            }
            if verdict.permille >= 1_000 {
                state.over_since.get_or_insert(now);
            } else {
                state.over_since = None;
            }
            state.verdict = verdict;
        }
        let keep = self.config.window + self.config.sample_every * 2;
        while state
            .charges
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) >= keep)
        {
            state.charges.pop_front();
        }
        while state
            .shed
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) >= keep)
        {
            state.shed.pop_front();
        }
    }

    /// The verdict over the window, and the cost of a datagram measured in
    /// it (`None`: too few sends to tell).
    fn judge(&self, state: &State, threads: &[TrackedThread]) -> Option<(CpuVerdict, Option<u64>)> {
        let first = state.samples.front()?;
        let last = state.samples.back()?;
        let wall_ns = last.at.saturating_duration_since(first.at).as_nanos() as u64;
        if wall_ns < 400_000_000 {
            return None;
        }
        let cpus = self.cpus.len().max(1) as u64;
        let cpus_busy_permille = match (first.cpus, last.cpus) {
            (Some((b0, t0)), Some((b1, t1))) if t1 > t0 => {
                (b1.saturating_sub(b0) * 1_000 / (t1 - t0)) as u32
            }
            _ => 0,
        };
        let cpus_full = self.config.cpus_full_permille.max(1);
        let thread_full = self.config.thread_full_permille.max(1);
        let mut permille = cpus_busy_permille * 1_000 / cpus_full;
        let mut term = String::from("cpus");
        let mut busy = Vec::new();
        let (mut send_ns, mut send_threads) = (0_u64, 0_u64);
        for thread in threads {
            let before = first.threads.iter().find(|(tid, _)| *tid == thread.tid);
            let after = last.threads.iter().find(|(tid, _)| *tid == thread.tid);
            let (Some((_, before)), Some((_, after))) = (before, after) else {
                continue;
            };
            let ns = after.saturating_sub(*before);
            let thread_permille = (ns.saturating_mul(1_000) / wall_ns).min(u64::from(u32::MAX)) as u32;
            busy.push(ThreadBusy {
                name: thread.name.clone(),
                busy_permille: thread_permille,
            });
            if !thread.group.in_term() {
                continue;
            }
            send_threads += 1;
            // The cost of a datagram: the fan-out pool's wall time over its
            // sends (a class thread's time is mostly its receipt of the
            // stream, whatever its few inline sends).
            if thread.group == ThreadGroup::Fanout {
                send_ns += ns;
            }
            let thread_term = thread_permille.saturating_mul(1_000) / thread_full;
            if thread_term > permille {
                permille = thread_term;
                term = thread.name.clone();
            }
        }
        let datagrams = last.datagrams.saturating_sub(first.datagrams);
        let measured = (datagrams >= MIN_DATAGRAMS_FOR_COST).then(|| send_ns / datagrams.max(1));
        let datagram_ns = measured
            .into_iter()
            .chain(state.costs.iter().copied())
            .max()
            .unwrap_or(0)
            .max(self.config.datagram_ns);
        let cpus_capacity = cpus * 1_000_000 * u64::from(cpus_full);
        let threads_capacity = if send_threads > 0 {
            send_threads * 1_000_000 * u64::from(thread_full)
        } else {
            u64::MAX
        };
        Some((
            CpuVerdict {
                permille,
                term,
                cpus_busy_permille,
                cpus: cpus as u32,
                threads: busy,
                datagram_ns,
                capacity_ns: cpus_capacity.min(threads_capacity),
            },
            measured,
        ))
    }

    pub(crate) fn verdict(&self) -> CpuVerdict {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .verdict
            .clone()
    }

    /// The legs admitted lately that the window does not show yet,
    /// thousandths: each fades as the window takes it in.
    fn pending_permille(&self, state: &State, now: Instant) -> u32 {
        let fade = (self.config.window + self.config.sample_every).as_secs_f64();
        state
            .charges
            .iter()
            .map(|(at, permille)| {
                let age = now.saturating_duration_since(*at).as_secs_f64();
                f64::from(*permille) * (1.0 - age / fade).max(0.0)
            })
            .sum::<f64>()
            .round() as u32
    }

    /// The legs asked to leave lately, thousandths: each counts in full
    /// until the window holds only samples taken after it (the window and
    /// two samples: the client's leave takes a round trip), for the
    /// window's mean lags its going (a fade let a shed edge shed again what
    /// its window still showed: 360 legs shed to 151 where 226 were full).
    fn shed_permille(&self, state: &State, now: Instant) -> u32 {
        let shown = self.config.window + self.config.sample_every * 2;
        state
            .shed
            .iter()
            .filter(|(at, _)| now.saturating_duration_since(*at) < shown)
            .map(|(_, permille)| *permille)
            .sum()
    }

    /// A new reader is refused now (the term with the legs admitted since
    /// is at full, and refusals are on).
    pub(crate) fn full(&self) -> bool {
        self.enforcing() && self.load_permille() >= 1_000
    }

    /// A new leg refused at full before it was priced: counted by the
    /// binding term.
    pub(crate) fn note_full_refusal(&self) {
        let cpus = self.state.lock().unwrap_or_else(|p| p.into_inner()).verdict.term == "cpus";
        let counter = if cpus {
            &self.stats.refused_cpus
        } else {
            &self.stats.refused_thread
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// The node's load by this term now: the measured term and the legs
    /// admitted since.
    pub(crate) fn load_permille(&self) -> u32 {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state
            .verdict
            .permille
            .saturating_add(self.pending_permille(&state, Instant::now()))
    }

    /// A new leg of `datagrams_per_s`: admitted and charged at once (so
    /// that the next admission sees it), or refused when the term with it
    /// would pass full.
    pub(crate) fn admit_leg(&self, datagrams_per_s: f64) -> Result<u32, CpuRefusal> {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let charge = state.verdict.leg_permille(datagrams_per_s).max(
            leg_permille(
                self.config.datagram_ns,
                self.floor_capacity_ns(),
                datagrams_per_s,
            ),
        );
        let pending = self.pending_permille(&state, now);
        let load = state.verdict.permille.saturating_add(pending);
        if self.enforcing()
            && state.last_cost.is_none()
            && pending.saturating_add(charge) > self.config.pending_max_permille.max(charge)
            && load.saturating_add(charge) <= 1_000
        {
            self.stats.refused_pending.fetch_add(1, Ordering::Relaxed);
            return Err(CpuRefusal::Pending);
        }
        if self.enforcing() && load.saturating_add(charge) > 1_000 {
            if state.verdict.term == "cpus" {
                self.stats.refused_cpus.fetch_add(1, Ordering::Relaxed);
                return Err(CpuRefusal::Cpus);
            }
            self.stats.refused_thread.fetch_add(1, Ordering::Relaxed);
            return Err(CpuRefusal::Thread);
        }
        state.charges.push_back((now, charge));
        self.stats.admitted.fetch_add(1, Ordering::Relaxed);
        Ok(charge)
    }

    /// A leg that is taken whatever the load (a lone mover): charged, so
    /// the next admissions see it.
    pub(crate) fn charge_leg(&self, datagrams_per_s: f64) -> u32 {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let charge = state.verdict.leg_permille(datagrams_per_s).max(leg_permille(
            self.config.datagram_ns,
            self.floor_capacity_ns(),
            datagrams_per_s,
        ));
        state.charges.push_back((Instant::now(), charge));
        self.stats.admitted.fetch_add(1, Ordering::Relaxed);
        charge
    }

    /// What one leg of `datagrams_per_s` is charged now, thousandths.
    pub(crate) fn leg_charge(&self, datagrams_per_s: f64) -> u32 {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.verdict.leg_permille(datagrams_per_s).max(leg_permille(
            self.config.datagram_ns,
            self.floor_capacity_ns(),
            datagrams_per_s,
        ))
    }

    /// The node's CPUs busy with the charges not yet measured, thousandths
    /// of ONE CPU (place240's unit).
    pub(crate) fn one_cpu_permille(&self) -> u32 {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let cpus = self.cpus.len().max(1) as u64;
        let busy = u64::from(state.verdict.cpus_busy_permille) * cpus;
        let pending = u64::from(self.pending_permille(&state, Instant::now()))
            * u64::from(self.config.cpus_full_permille)
            * cpus
            / 1_000;
        (busy + pending).min(u64::from(u32::MAX)) as u32
    }

    /// A heavy session admitted (place240's model cost, thousandths of one
    /// CPU): charged against the CPUs until the samples show it.
    pub(crate) fn charge_one_cpu_permille(&self, permille: u32) {
        if permille == 0 {
            return;
        }
        let cpus = self.cpus.len().max(1) as u64;
        let term = u64::from(permille) * 1_000
            / (cpus * u64::from(self.config.cpus_full_permille.max(1)));
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state
            .charges
            .push_back((Instant::now(), term.min(u64::from(u32::MAX)) as u32));
    }

    /// The capacity a leg is priced against before any verdict: the CPUs'.
    fn floor_capacity_ns(&self) -> u64 {
        self.cpus.len().max(1) as u64 * 1_000_000 * u64::from(self.config.cpus_full_permille.max(1))
    }

    /// How many legs of `leg_permille` each to ask to leave now: none
    /// unless the measured term has been over full for the shed delay;
    /// then enough to bring it back under (with a margin of one leg), less
    /// what the legs asked lately will take off, at most a round's worth.
    pub(crate) fn shed_quota(&self, leg_permille: u32) -> usize {
        if self.config.shed_after.is_zero() || !self.enforcing() {
            return 0;
        }
        let now = Instant::now();
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let Some(since) = state.over_since else {
            return 0;
        };
        if now.saturating_duration_since(since) < self.config.shed_after {
            return 0;
        }
        let leg = leg_permille.max(1);
        let excess = state
            .verdict
            .permille
            .saturating_add(leg)
            .saturating_sub(1_000)
            .saturating_sub(self.shed_permille(&state, now));
        (excess.div_ceil(leg) as usize).min(self.config.shed_per_round)
    }

    /// `count` legs of `leg_permille` each were asked to leave.
    pub(crate) fn shed_sent(&self, count: usize, leg_permille: u32) {
        if count == 0 {
            return;
        }
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.shed.push_back((now, leg_permille.saturating_mul(count as u32)));
        self.stats.shed_rounds.fetch_add(1, Ordering::Relaxed);
        self.stats
            .shed_hints
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    /// The measured term has been over full for the shed delay.
    pub(crate) fn shedding(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        self.enforcing()
            && !self.config.shed_after.is_zero()
            && state
                .over_since
                .is_some_and(|since| Instant::now().saturating_duration_since(since) >= self.config.shed_after)
    }

    pub(crate) fn advert(&self) -> CpuAdmitAdvert {
        let verdict = self.verdict();
        CpuAdmitAdvert {
            permille: self.load_permille(),
            term: verdict.term,
            datagram_ns: verdict.datagram_ns.max(self.config.datagram_ns),
            capacity_ns: if verdict.capacity_ns > 0 {
                verdict.capacity_ns
            } else {
                self.floor_capacity_ns()
            },
            shedding: self.shedding(),
        }
    }

    /// `/api/node` and `/api/assignment`: the term, its parts and the
    /// counters.
    pub(crate) fn status(&self) -> serde_json::Value {
        let verdict = self.verdict();
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        serde_json::json!({
            "load_permille": self.load_permille(),
            "measured_permille": verdict.permille,
            "term": verdict.term,
            "cpus": self.cpus,
            "cpus_busy_permille": verdict.cpus_busy_permille,
            "threads": verdict.threads,
            "datagram_ns": verdict.datagram_ns,
            "capacity_ns": verdict.capacity_ns,
            "shedding": self.shedding(),
            "full": {
                "cpus_permille": self.config.cpus_full_permille,
                "thread_permille": self.config.thread_full_permille,
            },
            "refused": {
                "cpus": load(&self.stats.refused_cpus),
                "thread": load(&self.stats.refused_thread),
                "http": load(&self.stats.refused_http),
                "pending": load(&self.stats.refused_pending),
            },
            "admitted": load(&self.stats.admitted),
            "shed_rounds": load(&self.stats.shed_rounds),
            "shed_hints": load(&self.stats.shed_hints),
            "spilled": load(&self.stats.spilled),
            "samples": load(&self.stats.samples),
        })
    }

    /// Test only: set the verdict as if measured.
    #[cfg(test)]
    pub(crate) fn force_verdict(&self, verdict: CpuVerdict, over_for: Option<Duration>) {
        let mut state = self.state.lock().unwrap();
        state.over_since = over_for.and_then(|over| Instant::now().checked_sub(over));
        state.verdict = verdict;
    }
}

/// Sample `admit` every `sample_every` until `shutdown` changes.
pub(crate) fn spawn_sampler(
    admit: Arc<CpuAdmit>,
    mut shutdown: tokio::sync::watch::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(admit.config.sample_every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let admit = Arc::clone(&admit);
                    // /proc reads off the runtime's workers.
                    let _ = tokio::task::spawn_blocking(move || admit.sample()).await;
                }
                _ = shutdown.changed() => break,
            }
        }
    })
}

/// The `--cpu-admit-*` flags.
#[derive(clap::Args, Debug, Clone)]
pub(crate) struct CpuAdmitArgs {
    /// rm/cpuadmit: an edge's load counts the busiest of its CPUs (with the
    /// sends' softirq) and its send threads, sampled off the class threads;
    /// at full a new leg is refused `edge-busy` and the entry spills it.
    #[arg(id = "cpu_admit", long = "cpu-admit", env = "NEEDLETAIL_CPU_ADMIT", default_value_t = true, action = clap::ArgAction::Set)]
    pub(crate) enabled: bool,
    /// The node's CPUs' mean busy percent at which the term is full.
    #[arg(id = "cpu_admit_cpus_full_percent", long = "cpu-admit-cpus-full-percent", default_value_t = DEFAULT_CPUS_FULL_PERCENT,
          value_parser = clap::value_parser!(u32).range(1..=100))]
    pub(crate) cpus_full_percent: u32,
    /// A send thread's busy percent of one core at which the term is full.
    #[arg(id = "cpu_admit_thread_full_percent", long = "cpu-admit-thread-full-percent", default_value_t = DEFAULT_THREAD_FULL_PERCENT,
          value_parser = clap::value_parser!(u32).range(1..=100))]
    pub(crate) thread_full_percent: u32,
    #[arg(id = "cpu_admit_window_ms", long = "cpu-admit-window-ms", default_value_t = DEFAULT_WINDOW_MS)]
    pub(crate) window_ms: u64,
    #[arg(id = "cpu_admit_sample_ms", long = "cpu-admit-sample-ms", default_value_t = DEFAULT_SAMPLE_MS)]
    pub(crate) sample_ms: u64,
    /// A client datagram costs the send threads at least this (ns): the
    /// floor of a leg's charge before the edge has measured its own.
    #[arg(id = "cpu_admit_datagram_ns", long = "cpu-admit-datagram-ns", default_value_t = DEFAULT_DATAGRAM_NS)]
    pub(crate) datagram_ns: u64,
    /// Before the edge has measured what a datagram costs it, the legs
    /// admitted since the last samples may add at most this (thousandths
    /// of full): a burst of joins at a cold edge waits for the samples.
    #[arg(id = "cpu_admit_pending_max_permille", long = "cpu-admit-pending-max-permille", default_value_t = DEFAULT_PENDING_MAX_PERMILLE)]
    pub(crate) pending_max_permille: u32,
    /// Over full this long, the edge asks two-leg listeners to give up
    /// their leg here (`MOVE shed`); 0: never.
    #[arg(id = "cpu_admit_shed_after_ms", long = "cpu-admit-shed-after-ms", default_value_t = DEFAULT_SHED_AFTER_MS)]
    pub(crate) shed_after_ms: u64,
    /// The node's CPUs (`0,1`); empty: the process's affinity.
    #[arg(id = "cpu_admit_cpus", long = "cpu-admit-cpus", default_value = "", value_parser = parse_cpu_list)]
    pub(crate) cpus: CpuList,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CpuList(pub(crate) Vec<usize>);

fn parse_cpu_list(text: &str) -> Result<CpuList, String> {
    let mut cpus = Vec::new();
    for item in text.split(',').map(str::trim).filter(|item| !item.is_empty()) {
        match item.split_once('-') {
            Some((from, to)) => {
                let from: usize = from.parse().map_err(|_| format!("not a CPU: {item}"))?;
                let to: usize = to.parse().map_err(|_| format!("not a CPU: {item}"))?;
                cpus.extend(from..=to);
            }
            None => cpus.push(item.parse().map_err(|_| format!("not a CPU: {item}"))?),
        }
    }
    Ok(CpuList(cpus))
}

impl CpuAdmitArgs {
    pub(crate) fn config(&self) -> Option<CpuAdmitConfig> {
        self.enabled.then(|| CpuAdmitConfig {
            cpus_full_permille: self.cpus_full_percent * 10,
            thread_full_permille: self.thread_full_percent * 10,
            window: Duration::from_millis(self.window_ms.max(500)),
            sample_every: Duration::from_millis(self.sample_ms.max(100)),
            datagram_ns: self.datagram_ns,
            pending_max_permille: self.pending_max_permille,
            shed_after: Duration::from_millis(self.shed_after_ms),
            shed_per_round: DEFAULT_SHED_PER_ROUND,
            cpus: self.cpus.0.clone(),
            own_threads_only: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admit(cpus: usize) -> CpuAdmit {
        CpuAdmit::new(
            CpuAdmitConfig {
                cpus: (0..cpus).collect(),
                ..CpuAdmitConfig::default()
            },
            ThreadRegistry::default(),
            Arc::new(|| 0),
        )
    }

    fn verdict(permille: u32, term: &str) -> CpuVerdict {
        CpuVerdict {
            permille,
            term: term.into(),
            cpus_busy_permille: permille * 8 / 10,
            cpus: 2,
            threads: Vec::new(),
            // 5 µs a datagram against 2 CPUs at 80 %.
            datagram_ns: 5_000,
            capacity_ns: 1_600_000_000,
        }
    }

    #[test]
    fn a_leg_is_priced_by_its_datagrams_against_the_capacity() {
        // 1,000 datagrams a second at 5 µs: 5 ms a second of 1.6 s.
        assert_eq!(verdict(0, "cpus").leg_permille(1_000.0), 4);
        assert_eq!(leg_permille(3_000, 1_600_000_000, 1_000.0), 2);
        assert_eq!(leg_permille(1_500, 0, 1_000.0), 0);
        assert_eq!(leg_permille(1_500, 1_600_000_000, 0.0), 0);
    }

    /// A wave of joins between two samples: each admitted leg is charged
    /// at once, so the wave stops at full, not past it.
    #[test]
    fn joins_between_samples_stop_at_full() {
        let admit = admit(2);
        // A burst at an edge with room waits for the samples past the cap.
        admit.force_verdict(verdict(0, "cpus"), None);
        let mut burst = 0;
        while admit.admit_leg(1_000.0).is_ok() {
            burst += 1;
        }
        assert_eq!(burst, DEFAULT_PENDING_MAX_PERMILLE / 4);
        assert_eq!(admit.admit_leg(1_000.0), Err(CpuRefusal::Pending));
        // Measured, a leg is priced at its cost and only full refuses.
        admit.state.lock().unwrap().last_cost = Some(5_000);
        assert!(admit.admit_leg(1_000.0).is_ok());
        let admit = super::tests::admit(2);
        admit.force_verdict(verdict(900, "cpus"), None);
        let mut admitted = 0;
        while admit.admit_leg(1_000.0).is_ok() {
            admitted += 1;
            assert!(admitted < 1_000);
        }
        // 100 permille of room at 4 a leg.
        assert_eq!(admitted, 25);
        assert!(admit.load_permille() <= 1_000);
        assert_eq!(admit.stats.refused_cpus.load(Ordering::Relaxed), 1);
        // A thread's term refuses as itself.
        admit.force_verdict(verdict(1_000, "nt-fanout-audio0"), None);
        assert_eq!(admit.admit_leg(1_000.0), Err(CpuRefusal::Thread));
    }

    #[test]
    fn an_edge_sheds_only_after_the_delay_and_only_its_excess() {
        let admit = admit(2);
        admit.force_verdict(verdict(1_100, "cpus"), Some(Duration::from_millis(500)));
        assert_eq!(admit.shed_quota(4), 0, "not yet");
        assert!(!admit.shedding());
        admit.force_verdict(verdict(1_100, "cpus"), Some(Duration::from_secs(3)));
        assert!(admit.shedding());
        // 104 over (one leg's margin): 26 legs.
        assert_eq!(admit.shed_quota(4), 26);
        admit.shed_sent(10, 4);
        // What it asked lately counts: 104 - 40 = 64 left.
        assert_eq!(admit.shed_quota(4), 16);
        admit.shed_sent(16, 4);
        assert_eq!(admit.shed_quota(4), 0, "it does not empty itself");
        // 3,000 over: a round's worth at most.
        admit.force_verdict(verdict(4_000, "cpus"), Some(Duration::from_secs(3)));
        assert_eq!(admit.shed_quota(4), DEFAULT_SHED_PER_ROUND);
        admit.force_verdict(verdict(900, "cpus"), None);
        assert_eq!(admit.shed_quota(4), 0);
        assert_eq!(admit.stats.shed_hints.load(Ordering::Relaxed), 26);
    }

    #[test]
    fn the_cpus_are_read_and_a_registered_thread_is_measured() {
        let cpus = affinity_cpus();
        assert!(!cpus.is_empty());
        let (busy, total) = cpus_jiffies(&cpus).unwrap();
        assert!(total >= busy && total > 0);
        let registry = ThreadRegistry::default();
        let busy_ns = Arc::new(AtomicU64::new(0));
        registry.register_current("spinner", ThreadGroup::Fanout, Some(Arc::clone(&busy_ns)));
        let admit = CpuAdmit::new(
            CpuAdmitConfig {
                cpus: cpus.clone(),
                ..CpuAdmitConfig::default()
            },
            registry,
            Arc::new(|| 0),
        );
        admit.sample();
        std::thread::sleep(Duration::from_millis(500));
        // As if the thread had been busy 0.6 s of the 0.5 s: over its 70 %.
        busy_ns.store(600_000_000, Ordering::Relaxed);
        admit.sample();
        assert_eq!(admit.state.lock().unwrap().last_cost, None, "the floor is not a measurement");
        let verdict = admit.verdict();
        assert_eq!(verdict.term, "spinner", "{verdict:?}");
        assert!(verdict.permille >= 1_000, "{verdict:?}");
        assert_eq!(verdict.threads.len(), 1);
        assert_eq!(verdict.datagram_ns, DEFAULT_DATAGRAM_NS, "no datagrams: the floor");
        assert_eq!(parse_cpu_list("0,2-3").unwrap(), CpuList(vec![0, 2, 3]));
        assert!(parse_cpu_list("x").is_err());
    }
}
