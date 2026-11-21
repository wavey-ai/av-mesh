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
    /// egress, committed egress and sessions. At 1,000 the node admits no new
    /// session.
    #[serde(default)]
    pub load_permille: u32,
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

#[derive(Debug)]
struct EdgeAdmissionInner {
    config: EdgeAdmissionConfig,
    started: Instant,
    window: Mutex<RollingEgressWindow>,
    overloaded: AtomicBool,
    rejected_requests: AtomicU64,
    admitted_sessions: AtomicU64,
    sessions: Mutex<HashMap<String, PlaybackSession>>,
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
                overloaded: AtomicBool::new(false),
                rejected_requests: AtomicU64::new(0),
                admitted_sessions: AtomicU64::new(0),
                sessions: Mutex::new(HashMap::new()),
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

    pub fn record_rejection(&self) {
        self.inner.rejected_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn admit_playback(&self, request: PlaybackRequest<'_>) -> PlaybackAdmission {
        let overloaded = self.snapshot().overloaded;
        let elapsed_ms = self.elapsed_ms();
        self.admit_playback_at(request, overloaded, elapsed_ms)
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
        prune_sessions(
            &mut sessions,
            elapsed_ms,
            self.inner.config.session_idle_seconds,
        );
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
}
