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
pub const DEFAULT_SESSION_IDLE_SECONDS: u64 = 60;
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

#[derive(Debug)]
struct EdgeAdmissionInner {
    config: EdgeAdmissionConfig,
    started: Instant,
    window: Mutex<RollingEgressWindow>,
    overloaded: AtomicBool,
    rejected_requests: AtomicU64,
    admitted_sessions: AtomicU64,
    sessions: Mutex<HashMap<String, u64>>,
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
                overloaded: AtomicBool::new(false),
                rejected_requests: AtomicU64::new(0),
                admitted_sessions: AtomicU64::new(0),
                sessions: Mutex::new(HashMap::new()),
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

    pub fn record_rejection(&self) {
        self.inner.rejected_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn admit_playback(&self, session_id: Option<&str>) -> PlaybackAdmission {
        let overloaded = self.snapshot().overloaded;
        let elapsed_ms = self
            .inner
            .started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        self.admit_playback_at(session_id, overloaded, elapsed_ms)
    }

    fn snapshot_at(&self, second: u64) -> EdgeAdmissionSnapshot {
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

        let active_sessions = self.active_sessions_at(
            self.inner
                .started
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        );

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
        }
    }

    fn admit_playback_at(
        &self,
        session_id: Option<&str>,
        overloaded: bool,
        elapsed_ms: u64,
    ) -> PlaybackAdmission {
        let Some(session_id) = session_id else {
            return if overloaded {
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
        prune_sessions(
            &mut sessions,
            elapsed_ms,
            self.inner.config.session_idle_seconds,
        );
        if let Some(last_seen_ms) = sessions.get_mut(session_id) {
            *last_seen_ms = elapsed_ms;
            return PlaybackAdmission::Existing;
        }
        if overloaded {
            return PlaybackAdmission::Rejected;
        }
        if sessions.len() >= self.inner.config.max_sessions {
            if let Some(oldest) = sessions
                .iter()
                .min_by_key(|(_, last_seen_ms)| **last_seen_ms)
                .map(|(session_id, _)| session_id.clone())
            {
                sessions.remove(&oldest);
            }
        }
        sessions.insert(session_id.to_owned(), elapsed_ms);
        self.inner.admitted_sessions.fetch_add(1, Ordering::Relaxed);
        PlaybackAdmission::Admitted
    }

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
        sessions.len().min(u64::MAX as usize) as u64
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
        self.prune(second, window_seconds);
        if let Some(last) = self.buckets.back_mut() {
            if last.second == second {
                last.bytes = last.bytes.saturating_add(bytes);
                return;
            }
        }
        self.buckets.push_back(EgressBucket { second, bytes });
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

fn prune_sessions(sessions: &mut HashMap<String, u64>, now_ms: u64, idle_seconds: u64) {
    let idle_ms = idle_seconds.saturating_mul(1_000);
    sessions.retain(|_, last_seen_ms| now_ms.saturating_sub(*last_seen_ms) < idle_ms);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission() -> EdgeAdmission {
        EdgeAdmission::new(EdgeAdmissionConfig {
            capacity_bps: 1_000,
            admission_percent: 80,
            recovery_percent: 60,
            window_seconds: 4,
            min_sustained_seconds: 3,
            session_idle_seconds: 5,
            max_sessions: 2,
        })
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
            admission.admit_playback_at(Some("existing"), false, 0),
            PlaybackAdmission::Admitted
        );
        assert_eq!(
            admission.admit_playback_at(Some("existing"), true, 1_000),
            PlaybackAdmission::Existing
        );
        assert_eq!(
            admission.admit_playback_at(Some("new"), true, 1_000),
            PlaybackAdmission::Rejected
        );
        assert_eq!(
            admission.admit_playback_at(None, true, 1_000),
            PlaybackAdmission::Rejected
        );
    }

    #[test]
    fn expires_idle_sessions_and_bounds_the_registry() {
        let admission = admission();
        admission.admit_playback_at(Some("one"), false, 0);
        admission.admit_playback_at(Some("two"), false, 1_000);
        admission.admit_playback_at(Some("three"), false, 2_000);
        assert_eq!(admission.active_sessions_at(2_000), 2);
        assert_eq!(
            admission.admit_playback_at(Some("one"), true, 2_000),
            PlaybackAdmission::Rejected
        );
        assert_eq!(admission.active_sessions_at(7_000), 0);
    }
}
