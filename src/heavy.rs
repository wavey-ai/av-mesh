//! Heavy sessions on small edges (place240): placement by what a session
//! costs an edge, the honest cap, and the edge's distress.
//!
//! cmpd8 (four regions, 4K at 240 Mbit/s): the entry placed UDP viewers and
//! LL-HLS readers by a rendezvous hash weighted by load, and every viewer of
//! a step joined within 0.1 s while every edge read load 23 to 36, so the
//! hash alone decided and both of a region's 4K viewers and its readers
//! landed on one 2-vCPU edge again and again while the other was healthy.
//!
//! A session is *heavy* when the edges class its stream heavy: its
//! sustained rate (its newest parts' media bytes over their own declared
//! durations, 2 s; never a peak, never arrival times) above
//! `--heavy-session-bps` (8 Mbit/s, the two-path rate) for 500 ms, and
//! small again only after 10 s under 80 % of it ([`RateClass`]). The figure
//! the cache measured before (`playlist_bitrate_bps`, kept as a peak by
//! `EdgeAdmission::set_playlist_bps`) counts the stored canonical slot,
//! envelope and all: 8.15 Mbit/s for 1 ms PCM, whose media is 2.43. Streams
//! are opaque bytes: every rule here keys on byte rates and measured
//! conditions, never on what the stream carries.
//!
//! What a heavy session costs an edge is a linear model ([`CostModel`]), in
//! thousandths of one CPU, its coefficients settings whose defaults are
//! cmpd8's budget of a 2-vCPU edge at the reference rate of 240 Mbit/s
//! (analysis section 6), each scaled by the stream's rate:
//!
//! | what | default | measured at 240 Mbit/s |
//! | --- | --- | --- |
//! | a UDP fast-lane viewer | 100 | 8-10 % of a CPU (+ its NET_TX softirq) |
//! | a UDP cache-fed viewer | 200 | the cache-fed lane on the tokio workers |
//! | an LL-HLS (HTTP) reader | 240 | 17-24 % of a CPU per reader |
//! | a heavy stream held (its tee) | 650 | tee 41-58 % + blocking pool 13-24 % |
//! | the edge's own work | 200 | tokio, cache mesh 0-5 %, softirq |
//!
//! An edge's heavy budget is `cpus x 1000 x --heavy-budget-percent` (85)
//! less `--heavy-reserve-permille-per-cpu` (100 a vCPU) kept for small
//! sessions, and its heavy egress at most `--heavy-egress-max-mbps` (600:
//! cmpd8's advice, a guest's heavy egress at most ~600 Mbit/s until the NIC
//! stalls are explained). The edge advertises its committed cost
//! ([`HeavyAdvert`]); the entry adds the sessions it placed since
//! ([`PendingHeavy`]) and the cost of the new one, and the rule of
//! `assignment::assign` places it.

use crate::assignment::{HeavyRoom, HeavySession};
use crate::{edge_admission, AppRouter, MeshSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Default of `--heavy-session-bps`: the two-path rate
/// (`assignment::DEFAULT_TWO_PATH_MAX_BPS`); 1 ms PCM is 2.6 Mbit/s.
pub(crate) const DEFAULT_HEAVY_SESSION_BPS: u64 = 8_000_000;
/// The rate the cost coefficients are given at.
pub(crate) const REFERENCE_BPS: u64 = 240_000_000;

/// How a session is delivered at the edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Lane {
    /// A UDP client on the fast lane (the class threads forward to it).
    UdpFast,
    /// A UDP client fed from the cache (whole parts sent by a task).
    UdpCache,
    /// An LL-HLS reader.
    Http,
}

/// The linear model of what heavy work costs an edge: thousandths of one
/// CPU at [`REFERENCE_BPS`], each scaled by the stream's rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CostModel {
    pub(crate) udp_fast_permille: u32,
    pub(crate) udp_cache_permille: u32,
    pub(crate) http_permille: u32,
    pub(crate) stream_permille: u32,
    pub(crate) base_permille: u32,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            udp_fast_permille: 100,
            udp_cache_permille: 200,
            http_permille: 240,
            stream_permille: 650,
            base_permille: 200,
        }
    }
}

fn scaled(permille: u32, bps: u64) -> u32 {
    (u128::from(permille) * u128::from(bps) / u128::from(REFERENCE_BPS)).min(u128::from(u32::MAX))
        as u32
}

impl CostModel {
    /// One session of a stream at `bps` on `lane`.
    pub(crate) fn session(&self, lane: Lane, bps: u64) -> u32 {
        let permille = match lane {
            Lane::UdpFast => self.udp_fast_permille,
            Lane::UdpCache => self.udp_cache_permille,
            Lane::Http => self.http_permille,
        };
        scaled(permille, bps)
    }

    /// A heavy stream held (its tee, whatever its sessions).
    pub(crate) fn stream(&self, bps: u64) -> u32 {
        scaled(self.stream_permille, bps)
    }
}

/// What makes an edge in distress (rule 4). Each is measured over the
/// distress monitor's sample (at most every [`DISTRESS_SAMPLE`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DistressConfig {
    /// Client sends that found the socket full (`would_block` of every lane,
    /// the fast lane's parts given back, send-queue drops, and the hook's
    /// send-side counters), a second (`--heavy-distress-blocked-per-s`, 20;
    /// 0: not judged).
    pub(crate) send_blocked_per_s: u64,
    /// Tee copies shed, a second (`--heavy-distress-tee-per-s`, 1,000; 0:
    /// not judged). cmpd8's hot edges shed 1.5k to 20k a second.
    pub(crate) tee_dropped_per_s: u64,
    /// The memory guard at soft or hard (`--heavy-distress-memory`).
    pub(crate) memory: bool,
    /// The busiest of the node's CPUs over this (thousandths) for
    /// `core_busy_for` (`--heavy-distress-core-permille`, 970; 0: not
    /// judged).
    pub(crate) core_busy_permille: u32,
    pub(crate) core_busy_for: Duration,
    /// Distress ends after this long with every input under its threshold.
    pub(crate) clear_after: Duration,
}

impl Default for DistressConfig {
    /// The rules on, but the busiest CPU's: in a test process on a shared
    /// box every CPU it may use is someone's busy CPU (the flag's default,
    /// 970, turns it on for a node alone on its guest).
    fn default() -> Self {
        Self {
            send_blocked_per_s: 20,
            tee_dropped_per_s: 1_000,
            memory: true,
            core_busy_permille: 0,
            core_busy_for: Duration::from_secs(3),
            clear_after: Duration::from_secs(3),
        }
    }
}

/// The settings of heavy sessions (`--heavy-*`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct HeavyConfig {
    /// A session of a stream above this rate is heavy (`--heavy-session-bps`).
    pub(crate) session_bps: u64,
    pub(crate) model: CostModel,
    /// The share of the CPUs heavy work and its reserve may take.
    pub(crate) budget_percent: u8,
    /// Kept for small sessions, a vCPU (audio first).
    pub(crate) reserve_permille_per_cpu: u32,
    /// Heavy egress of one edge at most.
    pub(crate) egress_max_bps: u64,
    /// The rate a stream that no cache has measured yet is reckoned at; such
    /// a session is placed by the heavy rule but never refused by it.
    pub(crate) unmeasured_bps: u64,
    /// The node's CPUs (0: as the process may use them).
    pub(crate) cpus: u32,
    /// A session this entry placed counts at its edge until an
    /// advertisement written this long after it (the client subscribes
    /// within a round trip), or `pending_ttl`.
    pub(crate) pending_settle: Duration,
    pub(crate) pending_ttl: Duration,
    /// A charge whose source is not proven (a UDP `ASSIGN` answered, not
    /// yet confirmed) lapses after this, and at most `max_unproven` of
    /// them count at one edge: a forged `ASSIGN` fills no edge for long.
    pub(crate) unproven_for: Duration,
    pub(crate) max_unproven: usize,
    pub(crate) distress: DistressConfig,
    /// A stream's class (heavy or small) follows its sustained rate: its
    /// newest parts' bytes over their own declared durations once they
    /// cover `sustain` (`--heavy-sustain-ms`, 2 s), never a peak and
    /// never arrival times. Heavy once that rate is over `session_bps` for
    /// `heavy_after` (`--heavy-after-ms`, 500 ms: two looks); small again
    /// only after it is under `small_below_permille` (800) of it for
    /// `small_after` (`--heavy-small-after-ms`, 10 s).
    pub(crate) sustain: Duration,
    pub(crate) heavy_after: Duration,
    pub(crate) small_after: Duration,
    pub(crate) small_below_permille: u32,
}

impl Default for HeavyConfig {
    fn default() -> Self {
        Self {
            session_bps: DEFAULT_HEAVY_SESSION_BPS,
            model: CostModel::default(),
            budget_percent: 85,
            reserve_permille_per_cpu: 100,
            egress_max_bps: 600_000_000,
            unmeasured_bps: 25_000_000,
            cpus: 0,
            pending_settle: Duration::from_millis(1_000),
            pending_ttl: Duration::from_secs(15),
            unproven_for: Duration::from_millis(1_500),
            max_unproven: 8,
            distress: DistressConfig::default(),
            sustain: Duration::from_secs(2),
            heavy_after: Duration::from_millis(500),
            small_after: Duration::from_secs(10),
            small_below_permille: 800,
        }
    }
}

impl HeavyConfig {
    /// Whether a session of a stream at `bps` is heavy.
    pub(crate) fn is_heavy(&self, bps: u64) -> bool {
        bps > self.session_bps
    }

    pub(crate) fn cpus(&self) -> u32 {
        if self.cpus > 0 {
            self.cpus
        } else {
            edge_admission::available_cpus()
        }
    }

    /// The CPU heavy sessions may commit on an edge of `cpus`: the budget
    /// less the small sessions' reserve (thousandths of one CPU).
    pub(crate) fn heavy_budget(&self, cpus: u32) -> u32 {
        let cpus = cpus.max(1);
        (cpus * 1_000 * u32::from(self.budget_percent.clamp(1, 100)) / 100)
            .saturating_sub(self.reserve_permille_per_cpu.saturating_mul(cpus))
    }

    /// The heavy session that a request for a stream at `bps` (`None`:
    /// unmeasured) on `lane` is, or `None` for a small one.
    pub(crate) fn session(&self, bps: Option<u64>, lane: Lane) -> Option<HeavySession> {
        let (bps, measured) = match bps {
            Some(bps) => (bps, true),
            None => (self.unmeasured_bps, false),
        };
        (bps > 0 && self.is_heavy(bps)).then(|| HeavySession {
            bps,
            cpu: self.model.session(lane, bps),
            stream_cpu: self.model.stream(bps),
            refuse: measured,
        })
    }
}

/// One heavy stream an edge holds, with its sessions by lane.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HeavyStream {
    pub(crate) stream_id: u64,
    pub(crate) bps: u64,
    #[serde(default)]
    pub(crate) udp_fast: u64,
    #[serde(default)]
    pub(crate) udp_cache: u64,
    #[serde(default)]
    pub(crate) http: u64,
}

/// What an edge advertises of its heavy work (`heavy` in its edge service
/// advertisement).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HeavyAdvert {
    pub(crate) cpus: u32,
    /// The CPU heavy sessions may commit (budget less the reserve),
    /// thousandths of one CPU.
    pub(crate) budget_permille: u32,
    /// The model's cost of what the edge holds and serves now.
    pub(crate) committed_permille: u32,
    /// The process's CPU over the last second, thousandths of one CPU.
    pub(crate) measured_permille: u32,
    pub(crate) egress_max_bps: u64,
    /// The heavy sessions' bytes.
    pub(crate) committed_bps: u64,
    /// The streams it classes heavy (their tee is paid).
    #[serde(default)]
    pub(crate) streams: Vec<HeavyStream>,
    /// The streams it classes small (a sustained rate under the line).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) small: Vec<u64>,
    /// Why the edge is in distress, if it is (rule 4): placement avoids it
    /// for heavy sessions at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) distress: Option<String>,
}

impl HeavyAdvert {
    /// The room an entry sees at this edge for `stream_id`, with the
    /// sessions it placed there since the advertisement (`pending`).
    pub(crate) fn room(
        &self,
        stream_id: u64,
        pending: &[PendingCharge],
        model: &CostModel,
    ) -> HeavyRoom {
        let mut cpu_used = self.committed_permille.max(self.measured_permille);
        let mut bps_used = self.committed_bps;
        let mut paid: Vec<u64> = self.streams.iter().map(|stream| stream.stream_id).collect();
        for charge in pending {
            cpu_used = cpu_used.saturating_add(model.session(charge.lane, charge.bps));
            bps_used = bps_used.saturating_add(charge.bps);
            if !paid.contains(&charge.stream_id) {
                paid.push(charge.stream_id);
                cpu_used = cpu_used.saturating_add(model.stream(charge.bps));
            }
        }
        HeavyRoom {
            cpu_budget: self.budget_permille,
            cpu_used,
            bps_max: self.egress_max_bps,
            bps_used,
            holds: paid.contains(&stream_id),
            distress: self.distress.is_some(),
        }
    }
}

/// A heavy session an entry placed at an edge and the edge has not
/// advertised yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingCharge {
    pub(crate) stream_id: u64,
    pub(crate) lane: Lane,
    pub(crate) bps: u64,
}

#[derive(Debug, Clone)]
struct Charge {
    node: String,
    key: String,
    stream_id: u64,
    lane: Lane,
    bps: u64,
    at: Instant,
    at_unix_ms: u64,
    proven: bool,
}

/// Charges kept at most (the oldest go first).
const MAX_CHARGES: usize = 4_096;

/// The heavy sessions this entry placed lately (rule 1: concurrent joins
/// count their bytes at once).
#[derive(Debug, Default)]
pub(crate) struct PendingHeavy {
    charges: Mutex<Vec<Charge>>,
}

impl PendingHeavy {
    /// A heavy session `key` (stream/session) placed at `node` now; a
    /// charge of the same key at that node is replaced. `proven`: its
    /// source is proven (HTTPS, a confirmation).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn charge(
        &self,
        node: &str,
        key: &str,
        stream_id: u64,
        lane: Lane,
        bps: u64,
        proven: bool,
        now: Instant,
        now_unix_ms: u64,
    ) {
        let mut charges = self.charges.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(charge) = charges
            .iter_mut()
            .find(|charge| charge.node == node && charge.key == key)
        {
            charge.proven |= proven;
            return;
        }
        if charges.len() >= MAX_CHARGES {
            charges.remove(0);
        }
        charges.push(Charge {
            node: node.to_owned(),
            key: key.to_owned(),
            stream_id,
            lane,
            bps,
            at: now,
            at_unix_ms: now_unix_ms,
            proven,
        });
    }

    /// The source of `key` at `node` is proven: its charge stands its whole
    /// time. Returns whether there was one.
    pub(crate) fn confirm(&self, node: &str, key: &str) -> bool {
        let mut charges = self.charges.lock().unwrap_or_else(|p| p.into_inner());
        match charges
            .iter_mut()
            .find(|charge| charge.node == node && charge.key == key)
        {
            Some(charge) => {
                charge.proven = true;
                true
            }
            None => false,
        }
    }

    /// The charges that still count at `node`, whose advertisement was
    /// written at `advert_unix_ms`: those it may not show yet.
    pub(crate) fn at(
        &self,
        node: &str,
        advert_unix_ms: u64,
        config: &HeavyConfig,
        now: Instant,
    ) -> Vec<PendingCharge> {
        let settle_ms = u64::try_from(config.pending_settle.as_millis()).unwrap_or(u64::MAX);
        let mut charges = self.charges.lock().unwrap_or_else(|p| p.into_inner());
        charges.retain(|charge| {
            let age = now.saturating_duration_since(charge.at);
            age < config.pending_ttl && (charge.proven || age < config.unproven_for)
        });
        let mut unproven = 0;
        charges
            .iter()
            .filter(|charge| {
                charge.node == node
                    && advert_unix_ms < charge.at_unix_ms.saturating_add(settle_ms)
            })
            .filter(|charge| {
                if charge.proven {
                    return true;
                }
                unproven += 1;
                unproven <= config.max_unproven
            })
            .map(|charge| PendingCharge {
                stream_id: charge.stream_id,
                lane: charge.lane,
                bps: charge.bps,
            })
            .collect()
    }
}

/// How often the distress monitor samples at most.
pub(crate) const DISTRESS_SAMPLE: Duration = Duration::from_millis(500);

/// The counters the distress monitor reads (cumulative).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DistressCounters {
    pub(crate) send_blocked: u64,
    pub(crate) tee_dropped: u64,
    /// The memory guard: 0 ok, 1 soft, 2 hard.
    pub(crate) memory: u8,
}

/// Busy and total jiffies of each CPU the process may use.
fn core_jiffies() -> Option<Vec<(u64, u64)>> {
    let allowed = allowed_cpus();
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let mut cores = Vec::new();
    for line in stat.lines().skip(1).take_while(|line| line.starts_with("cpu")) {
        let mut fields = line.split_whitespace();
        let Some(index) = fields
            .next()
            .and_then(|name| name.strip_prefix("cpu"))
            .and_then(|index| index.parse::<usize>().ok())
        else {
            continue;
        };
        if allowed.as_ref().is_some_and(|allowed| !allowed.contains(&index)) {
            continue;
        }
        let values: Vec<u64> = fields.filter_map(|value| value.parse().ok()).collect();
        let total: u64 = values.iter().take(8).sum();
        let idle = values.get(3).copied().unwrap_or(0) + values.get(4).copied().unwrap_or(0);
        cores.push((total.saturating_sub(idle), total));
    }
    Some(cores)
}

/// The CPUs of the process's affinity.
fn allowed_cpus() -> Option<Vec<usize>> {
    // SAFETY: a zeroed cpu_set_t is valid; sched_getaffinity writes it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return None;
        }
        Some(
            (0..libc::CPU_SETSIZE as usize)
                .filter(|cpu| libc::CPU_ISSET(*cpu, &set))
                .collect(),
        )
    }
}

#[derive(Debug, Default)]
struct DistressState {
    sampled: Option<(Instant, DistressCounters)>,
    cores: Option<Vec<(u64, u64)>>,
    core_busy_since: Option<Instant>,
    /// The reason now and since when, and when an input was last over its
    /// threshold.
    active: Option<&'static str>,
    last_bad: Option<Instant>,
}

/// One sample's verdict of the inputs (`None`: every input under its
/// threshold), from the counters' rise over `seconds`.
fn judge_inputs(
    config: &DistressConfig,
    before: DistressCounters,
    now: DistressCounters,
    seconds: f64,
    core_busy: bool,
) -> Option<&'static str> {
    let per_s = |after: u64, before: u64| after.saturating_sub(before) as f64 / seconds.max(0.001);
    if config.memory && now.memory >= 2 {
        return Some("memory-hard");
    }
    if config.memory && now.memory == 1 {
        return Some("memory-soft");
    }
    if config.send_blocked_per_s > 0
        && per_s(now.send_blocked, before.send_blocked) >= config.send_blocked_per_s as f64
    {
        return Some("send-blocked");
    }
    if config.tee_dropped_per_s > 0
        && per_s(now.tee_dropped, before.tee_dropped) >= config.tee_dropped_per_s as f64
    {
        return Some("tee-shed");
    }
    core_busy.then_some("core-busy")
}

/// A counter that the send side adds to the distress monitor's
/// `send_blocked` (see [`Heavy::register_send_side_counter`]).
pub(crate) type SendSideCounter = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Counters of the heavy rules, by reason (cumulative).
#[derive(Debug, Default)]
pub(crate) struct HeavyStats {
    /// Entry: heavy sessions placed, and those refused for want of room in
    /// the region (the busy answer: 429, or a spill to an edge that
    /// refuses `edge-busy`).
    pub(crate) placed: AtomicU64,
    pub(crate) entry_refused_http: AtomicU64,
    pub(crate) entry_refused_udp: AtomicU64,
    /// Edge: new heavy sessions refused by its own cap, by reason.
    pub(crate) refused_cpu: AtomicU64,
    pub(crate) refused_bytes: AtomicU64,
    pub(crate) refused_distress: AtomicU64,
    /// Edge: times it entered distress, and `MOVE distress-*` hints sent
    /// to one-leg clients of heavy streams.
    pub(crate) distress_episodes: AtomicU64,
    pub(crate) distress_hints: AtomicU64,
}

/// Why an edge refuses a new heavy session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeavyRefusal {
    Cpu,
    Bytes,
    Distress,
}

impl HeavyRefusal {
    pub(crate) fn word(self) -> &'static str {
        match self {
            Self::Cpu => "heavy-cpu",
            Self::Bytes => "heavy-bytes",
            Self::Distress => "distress",
        }
    }
}

/// A stream's class at an edge (place240): heavy or small by its
/// sustained rate, with hysteresis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RateClass {
    pub(crate) heavy: bool,
    /// Its last sustained rate; `None` before the parts covered the window.
    pub(crate) bps: Option<u64>,
    above_since: Option<Instant>,
    below_since: Option<Instant>,
}

impl RateClass {
    /// A look at the stream's sustained rate (`None`: not yet known).
    pub(crate) fn observe(&mut self, sample: Option<u64>, now: Instant, config: &HeavyConfig) {
        let Some(bps) = sample else {
            return;
        };
        self.bps = Some(bps);
        if config.is_heavy(bps) {
            self.below_since = None;
            let since = *self.above_since.get_or_insert(now);
            if now.saturating_duration_since(since) >= config.heavy_after {
                self.heavy = true;
            }
            return;
        }
        self.above_since = None;
        let low = u128::from(bps) * 1_000
            < u128::from(config.session_bps) * u128::from(config.small_below_permille);
        if !self.heavy || !low {
            self.below_since = None;
            return;
        }
        let since = *self.below_since.get_or_insert(now);
        if now.saturating_duration_since(since) >= config.small_after {
            self.heavy = false;
            self.below_since = None;
        }
    }
}

/// A heavy session an edge admits and has not counted yet (place240: two
/// admissions at once each counted the other's absence).
#[derive(Debug, Clone, Copy)]
struct Reservation {
    id: u64,
    stream_id: u64,
    lane: Lane,
    bps: u64,
    at: Instant,
}

/// A reservation lapses after this however its admission ended.
const RESERVATION_TTL: Duration = Duration::from_secs(10);

/// An admission's reservation of its heavy cost at an edge, released when
/// it is dropped (the subscriber is in the table by then, or refused).
pub(crate) struct HeavyHold {
    heavy: Arc<Heavy>,
    id: u64,
}

impl Drop for HeavyHold {
    fn drop(&mut self) {
        self.heavy
            .reservations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|reservation| reservation.id != self.id);
    }
}

/// A node's heavy-session state: its settings, the entry's pending
/// charges, the edge's distress and the counters.
pub(crate) struct Heavy {
    pub(crate) config: HeavyConfig,
    pub(crate) pending: PendingHeavy,
    pub(crate) stats: HeavyStats,
    distress: Mutex<DistressState>,
    /// The heavy streams held at the last advertisement, with their rates
    /// (the edge's own cap reads them between advertisements).
    held: Mutex<Vec<(u64, u64)>>,
    /// HOOK (send-side counters): the edge's send path, pacing and socket
    /// budgets are another engineer's work (25 Nov), adding counters of
    /// sends deferred or blocked. Register each here; their rises count as
    /// `send_blocked` in the distress monitor beside the counters that
    /// exist today.
    send_side: Mutex<Vec<SendSideCounter>>,
    /// Test only: a distress reason forced on (`None`: measured).
    forced: Mutex<Option<&'static str>>,
    /// The class of each stream this edge holds.
    classes: Mutex<HashMap<u64, RateClass>>,
    /// Heavy admissions in progress, and the lock that makes an admission's
    /// check and its reservation one step.
    reservations: Mutex<Vec<Reservation>>,
    admit: Mutex<()>,
    next_reservation: AtomicU64,
    /// rm/charge: each stream's lane charges with their hysteresis, the
    /// compact lane's last count of parts and extra datagrams (and the ratio
    /// it gave), and the client symbol size.
    pub(crate) charges: Mutex<HashMap<u64, crate::charge::ChargeTracker>>,
    pub(crate) compact_mark: Mutex<(u64, u64, f64)>,
    symbol_size: std::sync::atomic::AtomicU16,
}

impl std::fmt::Debug for Heavy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Heavy").field("config", &self.config).finish()
    }
}

impl Default for Heavy {
    fn default() -> Self {
        Self::new(HeavyConfig::default())
    }
}

impl Heavy {
    pub(crate) fn new(config: HeavyConfig) -> Self {
        Self {
            config,
            pending: PendingHeavy::default(),
            stats: HeavyStats::default(),
            distress: Mutex::default(),
            held: Mutex::default(),
            send_side: Mutex::default(),
            forced: Mutex::default(),
            classes: Mutex::default(),
            reservations: Mutex::default(),
            admit: Mutex::default(),
            next_reservation: AtomicU64::new(1),
            charges: Mutex::default(),
            compact_mark: Mutex::new((0, 0, 0.0)),
            symbol_size: std::sync::atomic::AtomicU16::new(av_mesh::client_udp::DEFAULT_SYMBOL_SIZE),
        }
    }

    /// rm/charge: the client UDP symbol size (`--client-udp-symbol-size`).
    pub(crate) fn client_symbol_size(&self) -> u16 {
        self.symbol_size.load(Ordering::Relaxed)
    }

    pub(crate) fn set_client_symbol_size(&self, size: u16) {
        self.symbol_size.store(size, Ordering::Relaxed);
    }

    /// The reservations that still stand.
    fn reserved(&self, now: Instant) -> Vec<Reservation> {
        let mut reservations = self.reservations.lock().unwrap_or_else(|p| p.into_inner());
        reservations.retain(|reservation| now.saturating_duration_since(reservation.at) < RESERVATION_TTL);
        reservations.clone()
    }

    /// HOOK: add a cumulative counter of client sends that the send side
    /// deferred or found blocked (see the field `send_side`).
    #[allow(dead_code)]
    pub(crate) fn register_send_side_counter(&self, read: SendSideCounter) {
        self.send_side
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(read);
    }

    fn send_side_total(&self) -> u64 {
        self.send_side
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|read| read())
            .sum()
    }

    /// Test only: class a stream heavy at `bps` (a look with no sample
    /// keeps it).
    #[cfg(test)]
    pub(crate) fn force_heavy(&self, stream_id: u64, bps: u64) {
        let mut classes = self.classes.lock().unwrap();
        let class = classes.entry(stream_id).or_default();
        class.heavy = true;
        class.bps = Some(bps);
    }

    /// Test only: force distress on (`Some`) or back to measured.
    #[cfg(test)]
    pub(crate) fn force_distress(&self, reason: Option<&'static str>) {
        *self.forced.lock().unwrap() = reason;
    }

    /// The distress verdict now, sampling the counters if the last sample
    /// is older than [`DISTRESS_SAMPLE`].
    fn distress_at(&self, now: Instant, read: impl FnOnce() -> DistressCounters) -> Option<&'static str> {
        if let Some(reason) = *self.forced.lock().unwrap_or_else(|p| p.into_inner()) {
            return Some(reason);
        }
        let config = self.config.distress;
        let mut state = self.distress.lock().unwrap_or_else(|p| p.into_inner());
        if state
            .sampled
            .is_some_and(|(at, _)| now.saturating_duration_since(at) < DISTRESS_SAMPLE)
        {
            return state.active;
        }
        let counters = read();
        let core_busy = if config.core_busy_permille > 0 {
            let cores = core_jiffies();
            let busiest = match (&state.cores, &cores) {
                (Some(before), Some(after)) => after
                    .iter()
                    .zip(before)
                    .filter_map(|((busy, total), (busy0, total0))| {
                        let total = total.saturating_sub(*total0);
                        (total > 0).then(|| busy.saturating_sub(*busy0) * 1_000 / total)
                    })
                    .max(),
                _ => None,
            };
            state.cores = cores;
            match busiest {
                Some(busiest) if busiest >= u64::from(config.core_busy_permille) => {
                    let since = *state.core_busy_since.get_or_insert(now);
                    now.saturating_duration_since(since) >= config.core_busy_for
                }
                _ => {
                    state.core_busy_since = None;
                    false
                }
            }
        } else {
            false
        };
        let verdict = match state.sampled {
            Some((at, before)) => judge_inputs(
                &config,
                before,
                counters,
                now.saturating_duration_since(at).as_secs_f64(),
                core_busy,
            ),
            None => None,
        };
        state.sampled = Some((now, counters));
        match verdict {
            Some(reason) => {
                if state.active.is_none() {
                    self.stats.distress_episodes.fetch_add(1, Ordering::Relaxed);
                }
                state.active = Some(reason);
                state.last_bad = Some(now);
            }
            None => {
                if state
                    .last_bad
                    .is_none_or(|bad| now.saturating_duration_since(bad) >= config.clear_after)
                {
                    state.active = None;
                }
            }
        }
        state.active
    }
}

/// Default of the distress hint's repeat to a client that stays.
pub(crate) const DISTRESS_HINT_EVERY: Duration = Duration::from_secs(1);
/// The reason word of a distress hint: `MOVE <stream> distress-<why>`.
pub(crate) const DISTRESS_PREFIX: &str = av_mesh::client_udp::DISTRESS_MOVE_PREFIX;

impl AppRouter {
    /// The distress counters now (rule 4): what exists today, and the
    /// hook's send-side counters.
    fn distress_counters(&self) -> DistressCounters {
        let stats = &self.edge_udp_stats;
        let mut send_blocked = stats.would_block.load(Ordering::Relaxed)
            + stats.send_queue_drops.load(Ordering::Relaxed);
        if let Some(lane) = stats.fast_lane.get().and_then(std::sync::Weak::upgrade) {
            let lane = lane.snapshot_stats();
            send_blocked += lane.would_block + lane.given_back;
        }
        send_blocked += self.heavy.send_side_total();
        DistressCounters {
            send_blocked,
            tee_dropped: self.cache.relay_ingress_snapshot().fast_plane.tee_dropped,
            memory: av_mesh::mem_guard::installed()
                .map_or(0, |installed| installed.guard.state() as u8),
        }
    }

    /// Why this edge is in distress now, if it is.
    pub(crate) fn heavy_distress(&self) -> Option<&'static str> {
        if self.cache_mesh_role != crate::CacheMeshRole::Edge && self.client_udp_port.is_none() {
            return None;
        }
        self.heavy
            .distress_at(Instant::now(), || self.distress_counters())
    }

    /// The class of a stream this edge holds, after a look at its
    /// sustained rate (place240).
    pub(crate) async fn heavy_class(&self, stream_id: u64) -> RateClass {
        let sample = self
            .cache
            .sustained_bitrate_bps(stream_id, self.heavy.config.sustain)
            .await;
        let mut classes = self.heavy.classes.lock().unwrap_or_else(|p| p.into_inner());
        let class = classes.entry(stream_id).or_default();
        class.observe(sample, Instant::now(), &self.heavy.config);
        *class
    }

    /// The distress reason for a stream's one-leg clients: the edge is in
    /// distress and the stream is classed heavy.
    pub(crate) async fn heavy_distress_for(&self, stream_id: u64) -> Option<&'static str> {
        if !self.heavy_class(stream_id).await.heavy {
            return None;
        }
        self.heavy_distress()
    }

    /// The process's CPU over the last second, thousandths of one CPU.
    fn heavy_measured_permille(&self, cpus: u32) -> u32 {
        self.node_status
            .cpu_permille
            .load(Ordering::Relaxed)
            .saturating_mul(cpus)
    }

    /// The heavy streams this edge holds with their sessions by lane, and
    /// the model's committed cost and bytes, from `held` (stream, rate),
    /// the admissions in progress (`reserved`) counted too.
    fn heavy_streams(
        &self,
        held: &[(u64, u64)],
        reserved: &[Reservation],
    ) -> (Vec<HeavyStream>, u32, u64) {
        let model = self.heavy.config.model;
        let udp = self.edge_udp_stats.subscribers_by_stream_and_lane();
        let http = self.edge_load.egress.sessions_by_playlist();
        let mut committed = model.base_permille;
        let mut bytes = 0_u64;
        let mut streams = Vec::new();
        for &(stream_id, bps) in held {
            let (udp_fast, udp_cache) = udp.get(&stream_id).copied().unwrap_or((0, 0));
            let http = http.get(&stream_id).copied().unwrap_or(0);
            committed = committed
                .saturating_add(model.stream(bps))
                .saturating_add(model.session(Lane::UdpFast, bps).saturating_mul(udp_fast as u32))
                .saturating_add(
                    model
                        .session(Lane::UdpCache, bps)
                        .saturating_mul(udp_cache as u32),
                )
                .saturating_add(model.session(Lane::Http, bps).saturating_mul(http as u32));
            bytes = bytes.saturating_add(bps.saturating_mul(udp_fast + udp_cache + http));
            streams.push(HeavyStream {
                stream_id,
                bps,
                udp_fast,
                udp_cache,
                http,
            });
        }
        for reservation in reserved {
            committed = committed.saturating_add(model.session(reservation.lane, reservation.bps));
            bytes = bytes.saturating_add(reservation.bps);
            if !held.iter().any(|(stream_id, _)| *stream_id == reservation.stream_id) {
                committed = committed.saturating_add(model.stream(reservation.bps));
            }
        }
        (streams, committed, bytes)
    }

    /// The heavy part of this edge's advertisement, from the streams it
    /// holds: those it classes heavy with their sustained rates and
    /// sessions (admissions in progress counted), and those it classes
    /// small. `None` on a node that serves no reader.
    pub(crate) async fn heavy_advert(&self, streams: Vec<u64>) -> Option<HeavyAdvert> {
        if self.cache_mesh_role != crate::CacheMeshRole::Edge {
            return None;
        }
        let mut held = Vec::new();
        let mut small = Vec::new();
        for stream_id in streams {
            let class = self.heavy_class(stream_id).await;
            match (class.heavy, class.bps) {
                (true, Some(bps)) => held.push((stream_id, bps)),
                (false, Some(_)) => small.push(stream_id),
                _ => {}
            }
        }
        *self.heavy.held.lock().unwrap_or_else(|p| p.into_inner()) = held.clone();
        let config = self.heavy.config;
        let cpus = config.cpus();
        let reserved = self.heavy.reserved(Instant::now());
        let (streams, committed, bytes) = self.heavy_streams(&held, &reserved);
        Some(HeavyAdvert {
            cpus,
            budget_permille: config.heavy_budget(cpus),
            committed_permille: committed,
            measured_permille: self.heavy_measured_permille(cpus),
            egress_max_bps: config.egress_max_bps,
            committed_bps: bytes,
            streams,
            small,
            distress: self.heavy_distress().map(str::to_owned),
        })
    }

    /// The edge's own cap (rule 2): one more session of `stream_id` on
    /// `lane`, its cost reserved at once (released when the hold is
    /// dropped: the admission ended), so two admissions at once see each
    /// other. A session of a stream this edge does not class heavy is
    /// always taken (`Ok(None)`).
    pub(crate) async fn heavy_reserve(
        &self,
        stream_id: u64,
        lane: Lane,
    ) -> Result<Option<HeavyHold>, HeavyRefusal> {
        self.heavy_check(stream_id, lane, true).await
    }

    /// [`Self::heavy_reserve`]; `add`: the session is not counted in the
    /// edge's sessions yet (it is reserved when it fits).
    async fn heavy_check(
        &self,
        stream_id: u64,
        lane: Lane,
        add: bool,
    ) -> Result<Option<HeavyHold>, HeavyRefusal> {
        let config = self.heavy.config;
        let class = self.heavy_class(stream_id).await;
        let (true, Some(bps)) = (class.heavy, class.bps) else {
            return Ok(None);
        };
        // An edge in distress is avoided by placement (its advertisement)
        // and tells its one-leg clients to move; it does not refuse a
        // session the entry sent it (a hedge, a move in a stale view):
        // that made a loop of edge-busy refusals.
        let _admit = self.heavy.admit.lock().unwrap_or_else(|p| p.into_inner());
        let mut held = self.heavy.held.lock().unwrap_or_else(|p| p.into_inner()).clone();
        if !held.iter().any(|(held, _)| *held == stream_id) {
            held.push((stream_id, bps));
        }
        let now = Instant::now();
        let reserved = self.heavy.reserved(now);
        let cpus = config.cpus();
        let (_, committed, bytes) = self.heavy_streams(&held, &reserved);
        let (cpu_add, bps_add) = if add {
            (config.model.session(lane, bps), bps)
        } else {
            (0, 0)
        };
        let cpu = committed
            .max(self.heavy_measured_permille(cpus))
            .saturating_add(cpu_add);
        if bytes.saturating_add(bps_add) > config.egress_max_bps {
            self.heavy.stats.refused_bytes.fetch_add(1, Ordering::Relaxed);
            return Err(HeavyRefusal::Bytes);
        }
        if cpu > config.heavy_budget(cpus) {
            self.heavy.stats.refused_cpu.fetch_add(1, Ordering::Relaxed);
            return Err(HeavyRefusal::Cpu);
        }
        if !add {
            return Ok(None);
        }
        let id = self.heavy.next_reservation.fetch_add(1, Ordering::Relaxed);
        self.heavy
            .reservations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(Reservation {
                id,
                stream_id,
                lane,
                bps,
                at: now,
            });
        Ok(Some(HeavyHold {
            heavy: Arc::clone(&self.heavy),
            id,
        }))
    }

    /// The edge's cap for an LL-HLS reader: a session just admitted (and
    /// so counted in its playlist's sessions) that passes the heavy budget
    /// is forgotten and refused (429).
    pub(crate) async fn heavy_admits_reader(
        &self,
        stream_id: Option<u64>,
        session_id: Option<&str>,
        admission: edge_admission::PlaybackAdmission,
    ) -> edge_admission::PlaybackAdmission {
        let (edge_admission::PlaybackAdmission::Admitted, Some(stream_id), Some(session_id)) =
            (admission, stream_id, session_id)
        else {
            return admission;
        };
        if self.heavy_check(stream_id, Lane::Http, false).await.is_err() {
            self.edge_load.egress.forget_session(session_id);
            return edge_admission::PlaybackAdmission::Rejected;
        }
        admission
    }

    /// The heavy session a reader of `stream_id` is, from the classes the
    /// region's edges advertise (place240), and the rate its legs are cut
    /// by: heavy at the highest sustained rate an edge classes it heavy
    /// at; small (two-path rate 0) when an edge classes it small and none
    /// heavy; else not known yet: the unmeasured estimate, placed by room
    /// and never refused, its legs as asked (`None`).
    pub(crate) fn heavy_session(
        &self,
        snapshots: &[MeshSnapshot],
        stream_id: u64,
        udp: bool,
    ) -> (Option<HeavySession>, Option<u64>) {
        let lane = if udp { Lane::UdpFast } else { Lane::Http };
        let mut heavy_bps: Option<u64> = None;
        let mut small = false;
        for advert in snapshots
            .iter()
            .filter(|snapshot| snapshot.node.region == self.node.region)
            .filter_map(|snapshot| snapshot.edge_service.as_ref()?.heavy.as_ref())
        {
            if let Some(stream) = advert.streams.iter().find(|stream| stream.stream_id == stream_id) {
                heavy_bps = Some(heavy_bps.map_or(stream.bps, |bps| bps.max(stream.bps)));
            } else if advert.small.contains(&stream_id) {
                small = true;
            }
        }
        match heavy_bps {
            Some(bps) => (self.heavy.config.session(Some(bps), lane), Some(bps)),
            None if small => (None, Some(0)),
            None => (self.heavy.config.session(None, lane), None),
        }
    }

    /// The room an entry sees at a cache for `stream_id` (`None` for a cache
    /// that advertises no heavy work: an older node).
    pub(crate) fn heavy_room(
        &self,
        node_id: &str,
        advert: Option<&HeavyAdvert>,
        advert_unix_ms: u64,
        stream_id: u64,
    ) -> Option<HeavyRoom> {
        let advert = advert?;
        let pending = self
            .heavy
            .pending
            .at(node_id, advert_unix_ms, &self.heavy.config, Instant::now());
        Some(advert.room(stream_id, &pending, &self.heavy.config.model))
    }

    /// Book a heavy session placed at `node` (`proven`: its source is).
    pub(crate) fn heavy_charge(
        &self,
        node: &str,
        key: &str,
        stream_id: u64,
        session: HeavySession,
        udp: bool,
        proven: bool,
    ) {
        self.heavy.stats.placed.fetch_add(1, Ordering::Relaxed);
        self.heavy.pending.charge(
            node,
            key,
            stream_id,
            if udp { Lane::UdpFast } else { Lane::Http },
            session.bps,
            proven,
            Instant::now(),
            crate::now_unix_ms(),
        );
    }

    /// A heavy session refused for want of room in the region.
    pub(crate) fn heavy_refused(&self, udp: bool) {
        let counter = if udp {
            &self.heavy.stats.entry_refused_udp
        } else {
            &self.heavy.stats.entry_refused_http
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// The counters for `/api/assignment`.
    pub(crate) fn heavy_counters(&self) -> serde_json::Value {
        let stats = &self.heavy.stats;
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        serde_json::json!({
            "placed": load(&stats.placed),
            "entry_refused": {
                "http": load(&stats.entry_refused_http),
                "udp": load(&stats.entry_refused_udp),
            },
            "edge_refused": {
                "heavy-cpu": load(&stats.refused_cpu),
                "heavy-bytes": load(&stats.refused_bytes),
                "distress": load(&stats.refused_distress),
            },
            "distress_episodes": load(&stats.distress_episodes),
            "distress_hints": load(&stats.distress_hints),
            "distress": self.heavy_distress(),
        })
    }
}

/// The `--heavy-*` flags.
#[derive(clap::Args, Debug, Clone)]
pub(crate) struct HeavyArgs {
    /// A session of a stream whose measured rate is above this is heavy:
    /// placed by what it costs an edge, capped by the edge's budget, given
    /// one leg (the two-path rate).
    #[arg(long = "heavy-session-bps", env = "NEEDLETAIL_HEAVY_SESSION_BPS", default_value_t = DEFAULT_HEAVY_SESSION_BPS)]
    pub(crate) session_bps: u64,
    /// The share of an edge's CPUs that heavy work and the small sessions'
    /// reserve may take.
    #[arg(long = "heavy-budget-percent", env = "NEEDLETAIL_HEAVY_BUDGET_PERCENT", default_value_t = 85)]
    pub(crate) budget_percent: u8,
    /// CPU kept for small sessions (audio first), thousandths of one CPU a
    /// vCPU: heavy sessions never take it.
    #[arg(long = "heavy-reserve-permille-per-cpu", env = "NEEDLETAIL_HEAVY_RESERVE_PERMILLE_PER_CPU", default_value_t = 100)]
    pub(crate) reserve_permille_per_cpu: u32,
    /// Heavy egress of one edge at most, Mbit/s (cmpd8: keep a guest's
    /// heavy egress at most ~600 Mbit/s until the NIC stalls are explained).
    #[arg(long = "heavy-egress-max-mbps", env = "NEEDLETAIL_HEAVY_EGRESS_MAX_MBPS", default_value_t = 600)]
    pub(crate) egress_max_mbps: u64,
    /// The rate a stream no cache has measured yet is reckoned at: such a
    /// session is placed by room but never refused.
    #[arg(long = "heavy-unmeasured-bps", env = "NEEDLETAIL_HEAVY_UNMEASURED_BPS", default_value_t = 25_000_000)]
    pub(crate) unmeasured_bps: u64,
    /// The node's CPUs for the budget (0: those the process may use).
    #[arg(long = "heavy-cpus", env = "NEEDLETAIL_HEAVY_CPUS", default_value_t = 0)]
    pub(crate) cpus: u32,
    /// The cost model, thousandths of one CPU at 240 Mbit/s (scaled by the
    /// stream's rate): a UDP fast-lane viewer, a UDP cache-fed viewer, an
    /// LL-HLS reader, a heavy stream held (its tee) and the edge's own.
    #[arg(long = "heavy-cost-udp-fast-permille", default_value_t = 100)]
    pub(crate) cost_udp_fast: u32,
    #[arg(long = "heavy-cost-udp-cache-permille", default_value_t = 200)]
    pub(crate) cost_udp_cache: u32,
    #[arg(long = "heavy-cost-http-permille", default_value_t = 240)]
    pub(crate) cost_http: u32,
    #[arg(long = "heavy-cost-stream-permille", default_value_t = 650)]
    pub(crate) cost_stream: u32,
    #[arg(long = "heavy-cost-base-permille", default_value_t = 200)]
    pub(crate) cost_base: u32,
    /// Edges whose fill with a heavy session is within this (thousandths
    /// of their budget) of the emptiest are comparable: the session's
    /// rendezvous hash chooses among them.
    #[arg(long = "heavy-place-band-permille", default_value_t = crate::assignment::DEFAULT_HEAVY_BAND_PERMILLE)]
    pub(crate) band_permille: u32,
    /// Distress (rule 4): client sends blocked a second, tee copies shed a
    /// second, the memory guard at soft or hard, the busiest CPU over this
    /// permille for this long (0: a rule off); distress ends after this
    /// long with every input under its threshold.
    #[arg(long = "heavy-distress-blocked-per-s", default_value_t = 20)]
    pub(crate) distress_blocked_per_s: u64,
    #[arg(long = "heavy-distress-tee-per-s", default_value_t = 1_000)]
    pub(crate) distress_tee_per_s: u64,
    #[arg(long = "heavy-distress-memory", default_value_t = true, action = clap::ArgAction::Set)]
    pub(crate) distress_memory: bool,
    #[arg(long = "heavy-distress-core-permille", default_value_t = 970)]
    pub(crate) distress_core_permille: u32,
    #[arg(long = "heavy-distress-core-ms", default_value_t = 3_000)]
    pub(crate) distress_core_ms: u64,
    #[arg(long = "heavy-distress-clear-ms", default_value_t = 3_000)]
    pub(crate) distress_clear_ms: u64,
    /// A stream's class follows its sustained rate over this (its newest
    /// parts' declared durations), heavy after it is over the line this
    /// long, small again after it is under 80 % of it this long.
    #[arg(long = "heavy-sustain-ms", default_value_t = 2_000)]
    pub(crate) sustain_ms: u64,
    #[arg(long = "heavy-after-ms", default_value_t = 500)]
    pub(crate) heavy_after_ms: u64,
    #[arg(long = "heavy-small-after-ms", default_value_t = 10_000)]
    pub(crate) small_after_ms: u64,
}

impl HeavyArgs {
    pub(crate) fn config(&self) -> HeavyConfig {
        HeavyConfig {
            session_bps: self.session_bps,
            model: CostModel {
                udp_fast_permille: self.cost_udp_fast,
                udp_cache_permille: self.cost_udp_cache,
                http_permille: self.cost_http,
                stream_permille: self.cost_stream,
                base_permille: self.cost_base,
            },
            budget_percent: self.budget_percent,
            reserve_permille_per_cpu: self.reserve_permille_per_cpu,
            egress_max_bps: self.egress_max_mbps.saturating_mul(1_000_000),
            unmeasured_bps: self.unmeasured_bps,
            cpus: self.cpus,
            distress: DistressConfig {
                send_blocked_per_s: self.distress_blocked_per_s,
                tee_dropped_per_s: self.distress_tee_per_s,
                memory: self.distress_memory,
                core_busy_permille: self.distress_core_permille,
                core_busy_for: Duration::from_millis(self.distress_core_ms),
                clear_after: Duration::from_millis(self.distress_clear_ms),
            },
            sustain: Duration::from_millis(self.sustain_ms.max(100)),
            heavy_after: Duration::from_millis(self.heavy_after_ms),
            small_after: Duration::from_millis(self.small_after_ms),
            ..HeavyConfig::default()
        }
    }
}

/// UDP subscribers of a stream by lane: (fast, cache-fed).
pub(crate) type LaneCounts = HashMap<u64, (u64, u64)>;

#[cfg(test)]
mod tests {
    use super::*;

    fn counters(send_blocked: u64, tee_dropped: u64, memory: u8) -> DistressCounters {
        DistressCounters {
            send_blocked,
            tee_dropped,
            memory,
        }
    }

    #[test]
    fn the_model_at_the_reference_rate_is_its_coefficients_and_scales_with_the_rate() {
        let model = CostModel::default();
        assert_eq!(model.session(Lane::UdpFast, REFERENCE_BPS), 100);
        assert_eq!(model.session(Lane::Http, REFERENCE_BPS), 240);
        assert_eq!(model.stream(REFERENCE_BPS), 650);
        assert_eq!(model.session(Lane::UdpFast, 320_000_000), 133);
        assert_eq!(model.session(Lane::Http, 2_600_000), 2);
    }

    #[test]
    fn a_two_vcpu_edge_has_a_heavy_budget_of_1500_and_four_of_3000() {
        let config = HeavyConfig::default();
        assert_eq!(config.heavy_budget(2), 2 * 850 - 200);
        assert_eq!(config.heavy_budget(4), 4 * 850 - 400);
        assert!(config.session(Some(2_600_000), Lane::UdpFast).is_none());
        let heavy = config.session(Some(240_000_000), Lane::UdpFast).unwrap();
        assert_eq!((heavy.cpu, heavy.stream_cpu, heavy.refuse), (100, 650, true));
        let unmeasured = config.session(None, Lane::Http).unwrap();
        assert!(!unmeasured.refuse, "an unmeasured stream is never refused");
    }

    /// The cap's default numbers (rule 2), from the measured budget, on a
    /// 2-vCPU edge: at 240 Mbit/s one heavy HTTP reader beside a UDP heavy
    /// viewer, two UDP viewers and no reader; at 320 one viewer and no
    /// reader beside it.
    #[test]
    fn the_default_cap_of_a_two_vcpu_edge() {
        let config = HeavyConfig::default();
        let fits = |bps: u64, udp: u64, http: u64| {
            let model = config.model;
            let cpu = model.base_permille
                + model.stream(bps)
                + model.session(Lane::UdpFast, bps) * udp as u32
                + model.session(Lane::Http, bps) * http as u32;
            cpu <= config.heavy_budget(2) && bps * (udp + http) <= config.egress_max_bps
        };
        assert!(fits(240_000_000, 1, 1));
        assert!(fits(240_000_000, 2, 0));
        assert!(!fits(240_000_000, 2, 1));
        assert!(!fits(240_000_000, 1, 2));
        assert!(fits(320_000_000, 1, 0));
        assert!(!fits(320_000_000, 1, 1));
        // Two heavy streams' tees fill a 2-vCPU edge with one viewer each.
        let model = config.model;
        let two = model.base_permille + 2 * model.stream(240_000_000) + 2 * 100;
        assert!(two > config.heavy_budget(2));
    }

    #[test]
    fn an_entry_counts_its_pending_sessions_until_an_advertisement_shows_them() {
        let config = HeavyConfig::default();
        let pending = PendingHeavy::default();
        let t0 = Instant::now();
        pending.charge("edge-1", "7/a", 7, Lane::UdpFast, 240_000_000, true, t0, 10_000);
        pending.charge("edge-1", "7/a", 7, Lane::UdpFast, 240_000_000, true, t0, 10_000);
        pending.charge("edge-1", "7/b", 7, Lane::UdpFast, 240_000_000, false, t0, 10_050);
        assert_eq!(pending.at("edge-1", 9_000, &config, t0).len(), 2, "a repeat charges once");
        assert_eq!(pending.at("edge-2", 9_000, &config, t0).len(), 0);
        // An advertisement written a settle after the charge shows it.
        assert_eq!(pending.at("edge-1", 11_000, &config, t0).len(), 1);
        assert_eq!(pending.at("edge-1", 11_050, &config, t0).len(), 0);
        // An unproven charge lapses after its time; a confirmed one stays.
        let later = t0 + Duration::from_millis(1_600);
        assert_eq!(pending.at("edge-1", 9_000, &config, later).len(), 1);
        assert!(pending.confirm("edge-1", "7/a"));
        let advert = HeavyAdvert {
            cpus: 2,
            budget_permille: config.heavy_budget(2),
            committed_permille: 200,
            measured_permille: 0,
            egress_max_bps: 600_000_000,
            committed_bps: 0,
            streams: Vec::new(),
            small: Vec::new(),
            distress: None,
        };
        let room = advert.room(7, &pending.at("edge-1", 9_000, &config, later), &config.model);
        assert_eq!(room.cpu_used, 200 + 650 + 100, "the first session there pays the tee");
        assert!(room.holds && room.bps_used == 240_000_000);
    }

    #[test]
    fn at_most_a_few_unproven_charges_count_at_one_edge() {
        let config = HeavyConfig::default();
        let pending = PendingHeavy::default();
        let now = Instant::now();
        for index in 0..20 {
            pending.charge("edge-1", &format!("7/{index}"), 7, Lane::UdpFast, 240_000_000, false, now, 0);
        }
        assert_eq!(pending.at("edge-1", 0, &config, now).len(), config.max_unproven);
    }

    #[test]
    fn distress_comes_with_an_input_over_its_threshold_and_clears_after_a_quiet_spell() {
        let config = DistressConfig {
            core_busy_permille: 0,
            ..DistressConfig::default()
        };
        assert_eq!(judge_inputs(&config, counters(0, 0, 0), counters(5, 300, 0), 1.0, false), None);
        assert_eq!(
            judge_inputs(&config, counters(0, 0, 0), counters(25, 0, 0), 1.0, false),
            Some("send-blocked")
        );
        assert_eq!(
            judge_inputs(&config, counters(0, 0, 0), counters(0, 600, 0), 0.5, false),
            Some("tee-shed")
        );
        assert_eq!(judge_inputs(&config, counters(0, 0, 0), counters(0, 0, 1), 1.0, false), Some("memory-soft"));
        assert_eq!(judge_inputs(&config, counters(0, 0, 0), counters(0, 0, 0), 1.0, true), Some("core-busy"));

        let heavy = Heavy::new(HeavyConfig {
            distress: config,
            ..HeavyConfig::default()
        });
        let t0 = Instant::now();
        assert_eq!(heavy.distress_at(t0, || counters(0, 0, 0)), None);
        let t1 = t0 + Duration::from_secs(1);
        assert_eq!(heavy.distress_at(t1, || counters(100, 0, 0)), Some("send-blocked"));
        // Within a sample it holds; quiet for less than the clear time it holds.
        assert_eq!(heavy.distress_at(t1 + Duration::from_millis(100), || counters(100, 0, 0)), Some("send-blocked"));
        let t2 = t1 + Duration::from_secs(1);
        assert_eq!(heavy.distress_at(t2, || counters(100, 0, 0)), Some("send-blocked"));
        let t3 = t1 + Duration::from_secs(3);
        assert_eq!(heavy.distress_at(t3, || counters(100, 0, 0)), None);
        assert_eq!(heavy.stats.distress_episodes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_send_side_counter_of_the_hook_counts_as_blocked_sends() {
        let heavy = Heavy::default();
        let value = Arc::new(AtomicU64::new(7));
        let read = Arc::clone(&value);
        heavy.register_send_side_counter(Arc::new(move || read.load(Ordering::Relaxed)));
        assert_eq!(heavy.send_side_total(), 7);
    }

    /// Place240 follow-up 1: a stream's class follows its sustained rate
    /// with hysteresis: heavy only once over the line for `heavy_after`,
    /// small again only after `small_after` under 80 % of it; a sample
    /// that is not known yet changes nothing.
    #[test]
    fn a_class_needs_a_sustained_rate_to_change_and_longer_to_change_back() {
        let config = HeavyConfig::default();
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let mut pcm = RateClass::default();
        for ms in (0..20_000).step_by(100) {
            pcm.observe(Some(2_400_000), at(ms), &config);
            assert!(!pcm.heavy);
        }
        // One look over the line (a rate that is not sustained) is not heavy.
        pcm.observe(Some(9_000_000), at(20_000), &config);
        pcm.observe(Some(2_400_000), at(20_100), &config);
        pcm.observe(Some(9_000_000), at(20_200), &config);
        assert!(!pcm.heavy);
        let mut video = RateClass::default();
        video.observe(None, at(0), &config);
        video.observe(Some(40_000_000), at(2_000), &config);
        assert!(!video.heavy);
        video.observe(Some(40_000_000), at(2_500), &config);
        assert!(video.heavy);
        // A dip into the band, and a short one under it: still heavy.
        video.observe(Some(7_000_000), at(3_000), &config);
        video.observe(Some(1_000_000), at(4_000), &config);
        video.observe(Some(40_000_000), at(5_000), &config);
        video.observe(Some(1_000_000), at(6_000), &config);
        video.observe(Some(1_000_000), at(15_900), &config);
        assert!(video.heavy);
        video.observe(Some(1_000_000), at(16_000), &config);
        assert!(!video.heavy, "10 s under 80 %");
    }
}
