//! The node's lifecycle (C 3.1, 3.2): why it drains, how it starts and how
//! it stops.
//!
//! Draining is a set of reasons, not a flag. `start` is held from the start
//! of a process that starts drained until it is warm (an edge also gives it
//! up after `--start-warm-ms`, and says so: `ready_forced`); `operator` is an
//! operator's `drain-node` or `close-node` until `resume-node`, kept across a
//! restart in the drain-state file; `stopping` is SIGTERM or SIGINT and is
//! never cleared. The cache mesh's hello says draining while any reason
//! holds. An edge drains (moves its listeners, takes none) while any reason
//! holds; a distributor only for `operator` or `stopping`, so that a
//! starting forwarder is ranked last by its children's cache mesh but still
//! answers as an entry point.
//!
//! SIGTERM and SIGINT are both a graceful stop ([`stop_signal`]): an edge
//! drains its listeners and readers, at most its stop bound, releases its
//! leases and stops ([`stop_edge`]); a forwarder says it is leaving for
//! `--stop-linger-ms` and stops ([`stop_forwarder`]). A second signal skips
//! what is left of the waits.

use crate::node_status::{DrainReason, DrainReasons, NodeState};
use crate::AppRouter;
use playlists::mesh::CacheMeshHandle;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio::sync::{watch, Notify};
use tracing::{info, warn};

const START: u8 = 1;
const OPERATOR: u8 = 2;
const STOPPING: u8 = 4;

/// How often an edge reads whether it has drained on a stop, and how long
/// it must have been empty.
pub(crate) const STOP_POLL: Duration = Duration::from_millis(50);
pub(crate) const STOP_HOLD: Duration = Duration::from_secs(1);
/// How often a starting node reads whether it is warm (its status is kept
/// for 100 ms).
pub(crate) const START_POLL: Duration = Duration::from_millis(100);
/// Added to B's drain spread and move grace for an edge's stop bound when
/// `--stop-drain-ms` is 0.
pub(crate) const STOP_DRAIN_SPARE_MS: u64 = 2_000;

impl DrainReason {
    const fn bit(self) -> u8 {
        match self {
            Self::Start => START,
            Self::Operator => OPERATOR,
            Self::Stopping => STOPPING,
        }
    }
}

/// The state byte of the cache mesh's hello (`with_hello_state`).
pub(crate) const fn hello_state(state: NodeState) -> u8 {
    match state {
        NodeState::Starting => 0,
        NodeState::Ready => 1,
        NodeState::Degraded => 2,
        NodeState::Draining => 3,
        NodeState::Stopping => 4,
    }
}

/// A hello's state byte in words, as `/api/node` shows a peer's: the
/// names of [`NodeState`], or `unknown` for a value a later build defines.
pub(crate) const fn hello_state_name(state: u8) -> &'static str {
    match state {
        0 => "starting",
        1 => "ready",
        2 => "degraded",
        3 => "draining",
        4 => "stopping",
        _ => "unknown",
    }
}

/// How a node's lifecycle begins.
#[derive(Debug, Clone, Default)]
pub(crate) struct LifecycleConfig {
    /// An edge drains for any reason; a distributor for `operator` and
    /// `stopping` only.
    pub(crate) edge: bool,
    /// `--start-drained`: the node holds `start` until it is warm.
    pub(crate) start_drained: bool,
    /// `--drain-state-file`: where an operator's drain is kept.
    pub(crate) state_file: Option<PathBuf>,
    /// The start opens what must be warm (an edge that replicates on
    /// demand: E5; a slot: F7), and so waits until the region's moving heads
    /// are known. Otherwise there is nothing to judge.
    pub(crate) warm_pass: bool,
}

/// The node's drain reasons and what follows from them. Clones share one
/// state.
#[derive(Clone)]
pub(crate) struct NodeLifecycle {
    inner: Arc<Inner>,
}

struct Inner {
    edge: bool,
    reasons: AtomicU8,
    /// The edge gave up `start` at its start timeout, not warm.
    forced: AtomicBool,
    /// What the cache mesh's hello says of this node's state.
    hello: Arc<AtomicU8>,
    /// The cache mesh whose hello says whether this node drains.
    mesh: OnceLock<Arc<CacheMeshHandle>>,
    state_file: Option<PathBuf>,
    /// Changes are made one at a time, and with them when the node last
    /// began to drain (`None` while it serves).
    draining_since: StdMutex<Option<Instant>>,
    /// Woken when the node begins or stops draining: the telemetry
    /// publisher advertises the change at once instead of at its next tick.
    changed: Arc<Notify>,
    /// What the start's warm pass opened (C CR8).
    warm: StdMutex<WarmPass>,
}

/// What a start's warm pass opened (C 3.1 E5, F7), for the readiness
/// evaluator.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WarmPass {
    /// An edge's warm copies: each audio playlist with a moving head in the
    /// region that it opened, with the highest head advertised for it then.
    pub(crate) copies: Vec<(u64, u64)>,
    /// A slot's start demand: the playlists it subscribed to at its start.
    pub(crate) slot_streams: Vec<u64>,
    /// The region's moving heads were known once: every fresh node of the
    /// region and every playlist of every fresh holder was seen in two
    /// advertisements (kept, as a regional view is). True from the start of
    /// a node that opens nothing at its start.
    pub(crate) judged: bool,
}

impl Default for NodeLifecycle {
    /// An edge that serves from its start and keeps no drain state.
    fn default() -> Self {
        Self::new(LifecycleConfig {
            edge: true,
            ..LifecycleConfig::default()
        })
    }
}

impl NodeLifecycle {
    pub(crate) fn new(config: LifecycleConfig) -> Self {
        let mut reasons = if config.start_drained { START } else { 0 };
        if let Some(path) = &config.state_file {
            match DrainState::read(path) {
                Ok(Some(state)) => {
                    reasons |= OPERATOR;
                    info!(
                        path = %path.display(),
                        since_unix_ms = state.since_unix_ms,
                        "an operator's drain holds from before the restart"
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    // A file that is there and cannot be read is a drain
                    // all the same: an operator put this node out.
                    reasons |= OPERATOR;
                    warn!(
                        path = %path.display(),
                        error = %error,
                        "the drain-state file cannot be read; the node stays drained"
                    );
                }
            }
        }
        let draining = draining(config.edge, reasons);
        Self {
            inner: Arc::new(Inner {
                edge: config.edge,
                reasons: AtomicU8::new(reasons),
                forced: AtomicBool::new(false),
                hello: Arc::new(AtomicU8::new(reasons_hello_state(reasons, 0))),
                mesh: OnceLock::new(),
                state_file: config.state_file,
                draining_since: StdMutex::new(draining.then(Instant::now)),
                changed: Arc::new(Notify::new()),
                warm: StdMutex::new(WarmPass {
                    judged: !(config.start_drained && config.warm_pass),
                    ..WarmPass::default()
                }),
            }),
        }
    }

    /// The cache mesh whose hello says whether this node drains: its bit is
    /// set now and on every change of the reasons.
    pub(crate) fn attach_mesh(&self, mesh: &Arc<CacheMeshHandle>) {
        let _change = self.lock_change();
        if self.inner.mesh.set(Arc::clone(mesh)).is_ok() {
            mesh.set_draining(self.inner.reasons.load(Ordering::Acquire) != 0);
        }
    }

    /// The state byte the cache mesh's hello reads.
    pub(crate) fn hello(&self) -> Arc<AtomicU8> {
        Arc::clone(&self.inner.hello)
    }

    /// The state the readiness evaluator found, for the hello.
    pub(crate) fn set_hello_state(&self, state: NodeState) {
        self.inner
            .hello
            .store(hello_state(state), Ordering::Relaxed);
    }

    pub(crate) fn reasons(&self) -> DrainReasons {
        let reasons = self.inner.reasons.load(Ordering::Acquire);
        DrainReasons {
            start: reasons & START != 0,
            operator: reasons & OPERATOR != 0,
            stopping: reasons & STOPPING != 0,
        }
    }

    pub(crate) fn holds(&self, reason: DrainReason) -> bool {
        self.inner.reasons.load(Ordering::Acquire) & reason.bit() != 0
    }

    /// Whether this node drains: on an edge any reason, on a distributor
    /// `operator` or `stopping`.
    pub(crate) async fn is_draining(&self) -> bool {
        draining(self.inner.edge, self.inner.reasons.load(Ordering::Acquire))
    }

    /// An edge gave up `start` at its start timeout, before it was warm.
    pub(crate) fn ready_forced(&self) -> bool {
        self.inner.forced.load(Ordering::Acquire)
    }

    /// Woken when [`Self::is_draining`] changes.
    pub(crate) fn changed(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.changed)
    }

    pub(crate) fn warm_pass(&self) -> WarmPass {
        self.lock_warm().clone()
    }

    /// An edge's warm pass: the copies it opened and whether the region's
    /// heads are known.
    pub(crate) fn set_warm_copies(&self, copies: Vec<(u64, u64)>, judged: bool) {
        let mut warm = self.lock_warm();
        warm.copies = copies;
        warm.judged |= judged;
    }

    /// A slot's start demand and whether the region's heads are known.
    pub(crate) fn set_slot_streams(&self, streams: Vec<u64>, judged: bool) {
        let mut warm = self.lock_warm();
        warm.slot_streams = streams;
        warm.judged |= judged;
    }

    fn lock_warm(&self) -> std::sync::MutexGuard<'_, WarmPass> {
        self.inner
            .warm
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn drain(&self, reason: DrainReason) {
        self.update(|reasons| reasons | reason.bit());
    }

    /// Remove one reason; the others hold. `stopping` is never removed.
    pub(crate) fn resume(&self, reason: DrainReason) {
        if reason == DrainReason::Stopping {
            return;
        }
        self.update(|reasons| reasons & !reason.bit());
    }

    /// An edge gives up `start` at its start timeout, not warm.
    pub(crate) fn force_start(&self) {
        self.inner.forced.store(true, Ordering::Release);
        self.resume(DrainReason::Start);
    }

    /// How much longer a draining node waits before its first moves, so
    /// that the advertisement saying it drains (sent at once on the change)
    /// reaches the entry points first: otherwise the movers of an edge
    /// drained at the same moment are sent to this one, and moved again.
    pub(crate) fn drain_advert_wait(&self, now: Instant, grace: Duration) -> Option<Duration> {
        let since = (*self.lock_change())?;
        grace
            .checked_sub(now.saturating_duration_since(since))
            .filter(|wait| !wait.is_zero())
    }

    fn lock_change(&self) -> std::sync::MutexGuard<'_, Option<Instant>> {
        self.inner
            .draining_since
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The one place the reasons change, and with them the hello's bit and
    /// state, the drain-state file and the advertisement.
    fn update(&self, change: impl FnOnce(u8) -> u8) {
        let mut since = self.lock_change();
        let before = self.inner.reasons.load(Ordering::Acquire);
        let after = change(before);
        if after == before {
            return;
        }
        self.inner.reasons.store(after, Ordering::Release);
        if let Some(mesh) = self.inner.mesh.get() {
            mesh.set_draining(after != 0);
        }
        let held = self.inner.hello.load(Ordering::Relaxed);
        self.inner
            .hello
            .store(reasons_hello_state(after, held), Ordering::Relaxed);
        if (before ^ after) & OPERATOR != 0 {
            if let Some(path) = &self.inner.state_file {
                let written = if after & OPERATOR != 0 {
                    DrainState::write(path)
                } else {
                    DrainState::remove(path)
                };
                if let Err(error) = written {
                    warn!(
                        path = %path.display(),
                        error = %error,
                        "the drain-state file was not changed: an operator's drain may not survive a restart as it should"
                    );
                }
            }
        }
        let (was, is) = (
            draining(self.inner.edge, before),
            draining(self.inner.edge, after),
        );
        if was != is {
            *since = is.then(Instant::now);
            // A stored permit when the publisher is not waiting yet.
            self.inner.changed.notify_one();
        }
    }
}

fn draining(edge: bool, reasons: u8) -> bool {
    if edge {
        reasons != 0
    } else {
        reasons & (OPERATOR | STOPPING) != 0
    }
}

/// The hello state that the reasons alone decide; with none, `held` (what
/// the readiness evaluator last found), or ready after a drain.
fn reasons_hello_state(reasons: u8, held: u8) -> u8 {
    if reasons & STOPPING != 0 {
        hello_state(NodeState::Stopping)
    } else if reasons & START != 0 {
        hello_state(NodeState::Starting)
    } else if reasons & OPERATOR != 0 {
        hello_state(NodeState::Draining)
    } else if held == hello_state(NodeState::Degraded) {
        held
    } else {
        hello_state(NodeState::Ready)
    }
}

/// An operator's drain, in `--drain-state-file`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DrainState {
    pub(crate) reason: String,
    pub(crate) since_unix_ms: u64,
}

impl DrainState {
    /// The drain kept in `path`, if there is one.
    pub(crate) fn read(path: &Path) -> std::io::Result<Option<Self>> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(std::io::Error::other),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Keep an operator's drain from now: written whole or not at all.
    fn write(path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let state = Self {
            reason: DrainReason::Operator.as_str().to_owned(),
            since_unix_ms: crate::now_unix_ms(),
        };
        let mut staged = path.as_os_str().to_owned();
        staged.push(".next");
        let staged = PathBuf::from(staged);
        std::fs::write(
            &staged,
            serde_json::to_vec(&state).map_err(std::io::Error::other)?,
        )?;
        std::fs::rename(&staged, path)
    }

    fn remove(path: &Path) -> std::io::Result<()> {
        match std::fs::remove_file(path) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }
}

/// SIGTERM and SIGINT, both a graceful stop.
pub(crate) struct StopSignal {
    terminate: Signal,
    interrupt: Signal,
}

/// Listen for SIGTERM and SIGINT from now on: neither ends the process any
/// more, each is a [`StopSignal::recv`].
pub(crate) fn stop_signal() -> std::io::Result<StopSignal> {
    Ok(StopSignal {
        terminate: signal(SignalKind::terminate())?,
        interrupt: signal(SignalKind::interrupt())?,
    })
}

impl StopSignal {
    /// The next signal, by name.
    pub(crate) async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.terminate.recv() => "SIGTERM",
            _ = self.interrupt.recv() => "SIGINT",
        }
    }
}

/// What a stop did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Stopped {
    /// The edge was empty for [`STOP_HOLD`] (a forwarder: it lingered its
    /// whole time).
    pub(crate) drained: bool,
    /// A second signal cut the wait short.
    pub(crate) skipped: bool,
    pub(crate) waited: Duration,
    /// Playlist subscriptions released at the node's parents.
    pub(crate) released: usize,
}

impl AppRouter {
    /// An edge has nobody left: no UDP subscriber (a leaving one counts
    /// until its `UNSUB` or its grace), no LL-HLS session that was not
    /// moved, no moved session that still fetches, no read in progress.
    fn edge_is_empty(&self) -> bool {
        self.edge_udp_stats.live_subscribers().unwrap_or(0) == 0
            && self.edge_load.admission_snapshot().active_sessions == 0
            && self.edge_load.moved_seen_within(1_000) == 0
            && self.edge_load.requests_in_flight() == 0
    }

    /// Release every playlist this node subscribes to at its parents: they
    /// drop it as a lease child now, not when its lease lapses.
    async fn release_subscriptions(&self) -> usize {
        // Reader demand would renew what is released.
        self.playlist_demand.streams.clear();
        let mut released = 0;
        for stream_id in self.mesh.subscribed_streams() {
            match self.mesh.unsubscribe(stream_id).await {
                Ok(_) => released += 1,
                Err(error) => warn!(
                    stream_id,
                    error = %error,
                    "a playlist subscription was not released at the stop"
                ),
            }
        }
        released
    }
}

/// SIGTERM on an edge (C 3.2, B 3.12): `stopping`, advertised at once, so
/// that the entry points assign it nobody and its UDP subscribers are moved
/// (spread over the drain spread, each kept until its `UNSUB` or the grace)
/// and its LL-HLS readers redirected at their next reload. It waits until
/// it has been empty for a second, read every 50 ms, at most `bound`, or a
/// second signal (`skip`); then it releases its playlist subscriptions.
pub(crate) async fn stop_edge(
    router: &AppRouter,
    bound: Duration,
    skip: impl Future<Output = ()>,
) -> Stopped {
    let started = Instant::now();
    router.lifecycle.drain(DrainReason::Stopping);
    let empty = async {
        let deadline = started + bound;
        let mut empty_since: Option<Instant> = None;
        let mut tick = tokio::time::interval(STOP_POLL);
        loop {
            tick.tick().await;
            let now = Instant::now();
            if router.edge_is_empty() {
                if now.saturating_duration_since(*empty_since.get_or_insert(now)) >= STOP_HOLD {
                    return true;
                }
            } else {
                empty_since = None;
            }
            if now >= deadline {
                return false;
            }
        }
    };
    let (drained, skipped) = tokio::select! {
        drained = empty => (drained, false),
        () = skip => (false, true),
    };
    let waited = started.elapsed();
    let released = router.release_subscriptions().await;
    Stopped {
        drained,
        skipped,
        waited,
        released,
    }
}

/// SIGTERM on a forwarder (C 3.2): `stopping`, so its hellos say it is
/// leaving and its children re-rank at once; it keeps forwarding for
/// `linger` (or until a second signal), then a slot (`--cache-mesh-upstream-
/// demand`) releases its own subscriptions at its parents. There is nothing
/// to hand over: each child takes every audio part from its other parent
/// already.
pub(crate) async fn stop_forwarder(
    router: &AppRouter,
    linger: Duration,
    upstream_demand: bool,
    skip: impl Future<Output = ()>,
) -> Stopped {
    let started = Instant::now();
    router.lifecycle.drain(DrainReason::Stopping);
    let (drained, skipped) = tokio::select! {
        () = tokio::time::sleep(linger) => (true, false),
        () = skip => (false, true),
    };
    let waited = started.elapsed();
    let released = if upstream_demand {
        router.release_subscriptions().await
    } else {
        0
    };
    Stopped {
        drained,
        skipped,
        waited,
        released,
    }
}

/// An edge's start (C 3.1): it holds `start` until it is warm, or for
/// `warm_bound` at most, and then serves cold and says so
/// (`ready_forced`): a cold edge serves, an edge that never resumes does
/// not. Meanwhile it opens a copy of each audio playlist whose head moves
/// in its region (`warm_playlists`, E5).
pub(crate) async fn start_edge(
    router: AppRouter,
    warm_bound: Duration,
    shutdown_rx: watch::Receiver<()>,
) {
    run_start(router, Some(warm_bound), shutdown_rx).await;
}

/// A forwarder's start (C 3.1): it holds `start`, and its hellos say
/// draining, until it is warm. There is no forced end: while it is not warm
/// it is ranked last and harms nothing.
pub(crate) async fn start_forwarder(router: AppRouter, shutdown_rx: watch::Receiver<()>) {
    run_start(router, None, shutdown_rx).await;
}

async fn run_start(
    router: AppRouter,
    warm_bound: Option<Duration>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let lifecycle = router.lifecycle.clone();
    let mut tick = tokio::time::interval(START_POLL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut heads = crate::RegionHeads::default();
    while lifecycle.holds(DrainReason::Start) {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = tick.tick() => {}
        }
        // E5: an edge opens the copies it must have warm before it serves.
        if warm_bound.is_some() {
            crate::warm_playlists(&router, &mut heads).await;
        }
        let status = router.node_status().await;
        if status.warm {
            lifecycle.resume(DrainReason::Start);
            info!(
                uptime_ms = status.process.uptime_ms,
                state = ?status.state,
                "warm: the node takes its place (drain reason start dropped)"
            );
        } else if let Some(bound) =
            warm_bound.filter(|bound| Duration::from_millis(status.process.uptime_ms) >= *bound)
        {
            lifecycle.force_start();
            warn!(
                uptime_ms = status.process.uptime_ms,
                start_warm_ms = bound.as_millis() as u64,
                not_ready = ?status.not_ready,
                "not warm at the start timeout: the edge serves cold (ready_forced)"
            );
        }
    }
}
