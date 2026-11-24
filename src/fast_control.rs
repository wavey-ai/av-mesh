//! The control plane of the fast plane: everything about it that runs on a
//! timer. The fast plane itself (`src/fast_plane/`) has none: its threads
//! wait only on their sockets, and its tees only on their datagrams (a
//! test fails on any `tokio::time` there). What must happen now and then
//! happens here, and reaches the fast plane through atomics, snapshots and
//! notifications:
//!
//! - the tees' housekeeping: the cache rotation every 10 ms, the release of
//!   holes past the repair horizon every 5 ms, and once a second the expiry
//!   of receive state, which the tee runs when told;
//! - the relay failover controller, which reads the threads' atomics;
//! - the lease children: their snapshot's backstop rebuild, their failover
//!   listener (whose promotions lapse), and the replay of a promoted
//!   child's warm sources, which waits for a full socket;
//! - the sampler of the parent feeds: once a second, the figures of what
//!   each parent delivered in the last second and the last ten.

use crate::fast_plane::lease::{LeaseChild, LeaseChildren};
use crate::fast_plane::parent_feeds::ParentFeeds;
use crate::fast_plane::part_repair::{PartRepair, SendSocket};
use crate::fast_plane::relay::{FailoverInputs, FastPlaneStats};
use crate::fast_plane::{sys, TrafficClass};
use crate::{
    LiveTsCache, RelayDatagramObservation, RelayFailoverController, RelayIngressParentPath,
};
use playlists::mesh::CacheMeshHandle;
use raptorq_datagram_fec::MediaDatagramRole;
use relay_session::{
    FailoverForwardMode, FailoverLeaseCommand, SubscriptionId, TopologyGeneration,
};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{watch, Notify};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, warn};

/// How often the lease children are rebuilt when no lease changes, as a
/// backstop.
const REBUILD_BACKSTOP: Duration = Duration::from_secs(1);
/// How long a warm-source replay waits for a full socket in all.
const REPLAY_WAIT: Duration = Duration::from_millis(50);
/// How often the legacy TS cache rotates.
const ROTATE_EVERY: Duration = Duration::from_millis(10);
/// How often the tees expire their receive state.
const EXPIRE_EVERY: Duration = Duration::from_secs(1);
/// How often holes past the repair horizon that no commit released are
/// released (`--canonical-repair-horizon`, at least 10 ms).
const HORIZON_EVERY: Duration = Duration::from_millis(5);

/// How often the parent feeds are sampled.
const FEEDS_SAMPLE_EVERY: Duration = Duration::from_secs(1);

/// The sampler of the parent feeds: once a second it copies every entry's
/// counters and publishes the figures of the last second and the last ten,
/// with the plane's own counts of datagrams received and not admitted.
pub(crate) async fn run_parent_feeds_sampler(
    feeds: Arc<ParentFeeds>,
    stats: Arc<FastPlaneStats>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    // No tick is shorter than a second: the figures are of whole seconds.
    let mut tick = interval(FEEDS_SAMPLE_EVERY);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut untracked = 0;
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = tick.tick() => {
                feeds.sample(
                    stats.received.iter().map(|class| class.load(Ordering::Relaxed)).sum(),
                    stats.not_admitted.load(Ordering::Relaxed),
                    crate::now_unix_us(),
                );
                let now = feeds.untracked();
                if now != untracked {
                    warn!(
                        datagrams = now - untracked,
                        "the parent feed table is full: datagrams of further streams are not recorded"
                    );
                    untracked = now;
                }
            }
        }
    }
}

/// The tees' housekeeping, on its own task: the cache's rotation (when
/// `rotate`), and a nudge to each tee to expire its receive state. A tee
/// never waits on a timer for it.
pub(crate) async fn run_tee_housekeeping(
    cache: Arc<LiveTsCache>,
    rotate: bool,
    expire: Vec<Arc<Notify>>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut rotation = interval(ROTATE_EVERY);
    rotation.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut expiry = interval(EXPIRE_EVERY);
    expiry.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut horizon = interval(HORIZON_EVERY);
    horizon.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = rotation.tick(), if rotate => {
                if let Err(error) = cache.rotate_if_due(false).await {
                    warn!(error = %format!("{error:#}"), "fast-plane cache rotation failed");
                }
            }
            _ = horizon.tick() => {
                cache.release_expired_repair_holes().await;
            }
            _ = expiry.tick() => {
                for tee in &expire {
                    tee.notify_one();
                }
            }
        }
    }
}

/// Replay a promoted child's warm video sources from the video socket,
/// waiting up to [`REPLAY_WAIT`] in all for a full socket. Returns the
/// datagrams sent.
pub(crate) async fn replay_warm(lease: &LeaseChildren, child: &LeaseChild) -> usize {
    let datagrams = lease.take_warm(child);
    let target = child.targets[TrafficClass::Video.index()];
    let fd = lease.fd(TrafficClass::Video);
    let mut sent = 0;
    let started = Instant::now();
    while sent < datagrams.len() {
        let left = datagrams.len() - sent;
        let taken =
            sys::send_batch(fd, left, |index| (&datagrams[sent + index][..], target)).unwrap_or(0);
        sent += taken;
        if taken == 0 {
            if started.elapsed() >= REPLAY_WAIT {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    lease
        .stats
        .warm_replayed
        .fetch_add(sent as u64, Ordering::Relaxed);
    lease
        .stats
        .warm_dropped
        .fetch_add((datagrams.len() - sent) as u64, Ordering::Relaxed);
    sent
}

/// Rebuild the lease children whenever the served leases change, and once a
/// second in any case.
pub(crate) async fn run_lease_children(
    lease: Arc<LeaseChildren>,
    mesh: Arc<CacheMeshHandle>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut changed = mesh.served_changed();
    let mut backstop = tokio::time::interval(REBUILD_BACKSTOP);
    backstop.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            result = changed.changed() => {
                if result.is_err() {
                    return;
                }
            }
            _ = backstop.tick() => {}
        }
        lease.rebuild(&mesh.served_subscriptions());
    }
}

/// The failover listener of the lease children: an edge's failover
/// controller promotes this forwarder (RLF1) for the edge's video once its
/// primary went silent. The child is the controller's address; a command
/// from an address without a lease is rejected. A promotion replays the
/// child's warm ring; the lease of a promotion ends it unless renewed.
pub(crate) async fn run_lease_failover_listener(
    socket: UdpSocket,
    lease: Arc<LeaseChildren>,
    generation: TopologyGeneration,
    subscription_id: SubscriptionId,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut buffer = [0_u8; relay_session::FAILOVER_CONTROL_WIRE_LEN + 1];
    // By child: the last transition taken, and when a promotion lapses.
    let mut transitions: HashMap<IpAddr, (u64, FailoverForwardMode, Option<Instant>)> =
        HashMap::new();
    let mut tick = tokio::time::interval(Duration::from_millis(25));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = tick.tick() => {
                let now = Instant::now();
                for (ip, (_, mode, deadline)) in &mut transitions {
                    if deadline.is_some_and(|deadline| now >= deadline) {
                        let _ = lease.apply_failover(*ip, FailoverForwardMode::RepairOnly);
                        *deadline = None;
                        *mode = FailoverForwardMode::RepairOnly;
                    }
                }
                transitions.retain(|ip, _| lease.child(*ip).is_some());
            }
            received = socket.recv_from(&mut buffer) => {
                let Ok((len, peer)) = received else {
                    lease.stats.commands_rejected.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let Ok(command) = FailoverLeaseCommand::decode(&buffer[..len]) else {
                    lease.stats.commands_rejected.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let ip = peer.ip();
                let (last_id, last_mode, _) = transitions
                    .get(&ip)
                    .copied()
                    .unwrap_or((0, FailoverForwardMode::RepairOnly, None));
                let stale = command.generation != generation
                    || command.subscription_id != subscription_id
                    || command.transition_id < last_id
                    || (command.transition_id == last_id && command.mode != last_mode);
                if stale {
                    lease.stats.commands_rejected.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let Some(promoted_now) = lease.apply_failover(ip, command.mode) else {
                    lease.stats.commands_rejected.fetch_add(1, Ordering::Relaxed);
                    debug!(%peer, "failover command from an address without a lease");
                    continue;
                };
                let deadline = (command.mode == FailoverForwardMode::SourceAndRepair)
                    .then(|| Instant::now() + Duration::from_micros(command.lease_duration_us));
                transitions.insert(ip, (command.transition_id, command.mode, deadline));
                if promoted_now {
                    if let Some(child) = lease.child(ip) {
                        let replayed = replay_warm(&lease, &child).await;
                        debug!(child = %ip, replayed, "replayed a promoted lease child's warm sources");
                    }
                }
            }
        }
    }
}

pub(crate) async fn run_failover_controller(
    mut controller: RelayFailoverController,
    inputs: Arc<FailoverInputs>,
    heartbeat: Duration,
    cache: Arc<LiveTsCache>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut tick = interval(heartbeat);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut seen = [0_u64; 4];
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = tick.tick() => {
                let mut events = Vec::with_capacity(4);
                for (index, (slot, path, role, decoded)) in [
                    (&inputs.primary_source, RelayIngressParentPath::Primary, MediaDatagramRole::Source, false),
                    (&inputs.secondary_repair, RelayIngressParentPath::Secondary, MediaDatagramRole::Repair, false),
                    (&inputs.secondary_source, RelayIngressParentPath::Secondary, MediaDatagramRole::Source, false),
                    // A decode alone: a (path, role) pair that observe()
                    // reads as nothing but the decode.
                    (&inputs.decoded, RelayIngressParentPath::Primary, MediaDatagramRole::Repair, true),
                ]
                .into_iter()
                .enumerate()
                {
                    let value = slot.load(Ordering::Relaxed);
                    if value != seen[index] {
                        seen[index] = value;
                        events.push((value, path, RelayDatagramObservation { role, decoded }));
                    }
                }
                events.sort_by_key(|(value, _, _)| *value);
                for (value, path, observation) in events {
                    let at = inputs.base + Duration::from_micros(value - 1);
                    controller.observe(path, observation, at);
                }
                controller.tick(Instant::now()).await;
                cache.update_relay_failover_controller(controller.snapshot());
            }
        }
    }
}

/// Read and count whatever arrives on the lease sockets (wave 8 review,
/// F-I1). They only send; a datagram sent to them (an edge's demand or loss
/// report to its lease parent) queued in their receive buffers until the
/// kernel dropped it, counted as UdpRcvbufErrors, which hid any real drop.
/// One task for each socket of the pair (SO_REUSEPORT spreads what arrives
/// over both).
pub(crate) fn spawn_lease_inbound_drain(
    lease: &Arc<LeaseChildren>,
    shutdown_rx: watch::Receiver<()>,
) -> Vec<tokio::task::JoinHandle<()>> {
    lease
        .inbound_sockets()
        .into_iter()
        .filter_map(|socket| tokio::net::UdpSocket::from_std(socket).ok())
        .map(|socket| {
            let lease = Arc::clone(lease);
            let socket = Arc::new(socket);
            let mut shutdown = shutdown_rx.clone();
            tokio::spawn(async move {
                let mut buf = vec![0_u8; 2_048];
                loop {
                    tokio::select! {
                        _ = shutdown.changed() => return,
                        received = socket.recv_from(&mut buf) => {
                            if let Ok((len, from)) = received {
                                lease.stats.inbound.fetch_add(1, Ordering::Relaxed);
                                // A child's ask or loss report (4k-repair).
                                if let Some(repair) = lease.part_repair.get() {
                                    let via: Arc<dyn SendSocket> = Arc::clone(&socket) as _;
                                    repair.handle_child_datagram(&via, from, &buf[..len]);
                                }
                            } else {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                }
            })
        })
        .collect()
}

/// The video sockets' receive buffers sized to their streams once a second
/// until shutdown (`fast_plane::relay::VideoSocketBuffers`, fix4-video).
pub(crate) async fn run_video_socket_buffers(
    mut buffers: crate::fast_plane::relay::VideoSocketBuffers,
    stats: Arc<crate::fast_plane::relay::FastPlaneStats>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last = Instant::now();
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = tick.tick() => {
                let now = Instant::now();
                buffers.turn(&stats, now.duration_since(last));
                last = now;
            }
        }
    }
}

/// The cache mesh's receive sockets sized to their traffic (integration 4),
/// as the fast plane's video sockets are (`relay::VideoSocketBuffers`).
/// The cache mesh's sockets were made with --cache-mesh-socket-buffer-bytes
/// (else --udp-socket-buffer-bytes) and dropped what a stall overflowed:
/// the main (audio) socket 1,864 datagrams at one edge of the local gate in
/// a 4K stream's first second, the class (video) socket 122k at one edge
/// during Linode node stops. Once a second, each socket whose busiest
/// second so far, times `ms`, is more than it was asked for is asked for
/// that, up to `max`; never less. Off every hot path: a counter read and,
/// when a socket grows, a setsockopt.
#[derive(Debug)]
pub(crate) struct MeshSocketBuffers {
    ms: u64,
    /// What the sockets were made with, and the least they are grown to at
    /// the first look, before any traffic.
    configured: usize,
    floor: usize,
    max: usize,
    /// Per socket (`main`, `class`): bytes asked, wire bytes at the last
    /// look, and the busiest second's bytes.
    lanes: HashMap<&'static str, MeshSocketLane>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct MeshSocketLane {
    pub asked: usize,
    pub last_bytes: u64,
    pub busiest_per_second: u64,
    /// The buffer as the kernel reports it (twice what was asked).
    pub granted: usize,
}

impl MeshSocketBuffers {
    pub(crate) fn new(ms: u64, configured: usize, floor: usize, max: usize) -> Self {
        Self {
            ms,
            configured,
            floor: floor.max(configured),
            max,
            lanes: HashMap::new(),
        }
    }

    /// One look: `sockets` as `CacheMeshHandle::receive_sockets` gives them.
    pub(crate) fn turn<S: std::os::fd::AsRawFd>(
        &mut self,
        sockets: &[(&'static str, S, u64)],
        elapsed: Duration,
    ) {
        for (lane, socket, bytes) in sockets {
            let state = self.lanes.entry(*lane).or_insert(MeshSocketLane {
                asked: self.configured,
                last_bytes: *bytes,
                ..MeshSocketLane::default()
            });
            let delta = bytes.saturating_sub(state.last_bytes);
            state.last_bytes = *bytes;
            let per_second = (delta as f64 / elapsed.as_secs_f64().max(1e-3)) as u64;
            state.busiest_per_second = state.busiest_per_second.max(per_second);
            let target = crate::fast_plane::relay::video_socket_buffer_target(
                state.busiest_per_second,
                self.ms,
                self.floor,
                self.max,
            );
            if self.ms > 0 && target > state.asked {
                let granted = sys::grow_receive_buffer(socket, target);
                tracing::info!(
                    lane = *lane,
                    bytes_per_second = state.busiest_per_second,
                    asked_bytes = target,
                    granted_bytes = granted,
                    "a cache-mesh socket's receive buffer grows to its traffic"
                );
                state.asked = target;
            }
            state.granted = sys::receive_buffer(socket).unwrap_or(0);
        }
    }

    pub(crate) fn lane(&self, lane: &str) -> Option<MeshSocketLane> {
        self.lanes.get(lane).copied()
    }
}

/// [`MeshSocketBuffers`] once a second until shutdown.
pub(crate) async fn run_cache_mesh_socket_buffers(
    mesh: Arc<CacheMeshHandle>,
    mut buffers: MeshSocketBuffers,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last = Instant::now();
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = tick.tick() => {
                let now = Instant::now();
                buffers.turn(&mesh.receive_sockets(), now.duration_since(last));
                last = now;
            }
        }
    }
}

#[cfg(test)]
mod mesh_socket_buffer_tests {
    use super::*;

    /// Integration 4: a cache-mesh socket grows to 2 s of its busiest
    /// second, from the configured size up to the cap, and never shrinks.
    #[test]
    fn a_cache_mesh_socket_grows_to_two_seconds_of_its_busiest_second() {
        let socket = Arc::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
        let floor = 256 << 10;
        let max = 4 << 20;
        let _ = sys::grow_receive_buffer(&*socket, 128 << 10);
        let mut buffers = MeshSocketBuffers::new(2_000, 128 << 10, floor, max);
        let second = Duration::from_secs(1);
        // The first look, before any traffic: the floor.
        buffers.turn(&[("main", Arc::clone(&socket), 0)], second);
        assert_eq!(buffers.lane("main").unwrap().asked, floor);
        // An audio second: 300 kB/s wants 600 kB.
        buffers.turn(&[("main", Arc::clone(&socket), 300_000)], second);
        let lane = buffers.lane("main").unwrap();
        assert_eq!((lane.busiest_per_second, lane.asked), (300_000, 600_000));
        // A quiet second asks for nothing less.
        buffers.turn(&[("main", Arc::clone(&socket), 300_100)], second);
        assert_eq!(buffers.lane("main").unwrap().asked, 600_000);
        // A burst of 10 MB/s asks for the cap, and no more.
        buffers.turn(&[("main", Arc::clone(&socket), 10_300_100)], second);
        let lane = buffers.lane("main").unwrap();
        assert_eq!((lane.busiest_per_second, lane.asked), (10_000_000, max));
        // The kernel grants at least what rmem_max allows (doubled).
        assert!(lane.granted >= 2 * floor, "{lane:?}");
        // With ms 0 nothing is asked.
        let mut off = MeshSocketBuffers::new(0, floor, floor, max);
        off.turn(&[("class", Arc::clone(&socket), 0)], second);
        off.turn(&[("class", Arc::clone(&socket), 50_000_000)], second);
        assert_eq!(off.lane("class").unwrap().asked, floor);
    }
}

/// Wake each tee's part repair asks every 2 ms (4k-repair): an ask is
/// due a few ms after its part's parent moved on, which a tee that has no
/// datagram to take would not see.
pub(crate) async fn run_part_repair_tick(
    ticks: Vec<Arc<tokio::sync::Notify>>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut every = tokio::time::interval(Duration::from_millis(2));
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = every.tick() => {
                for tick in &ticks {
                    tick.notify_one();
                }
            }
        }
    }
}

/// How often each child gets its ledger.
const CHILD_LEDGER_EVERY: Duration = Duration::from_millis(250);

/// Send each fast-plane child its ledger on each lane (`RLL1`, 4k-repair): the
/// datagrams handed to the kernel for it so far (forwarded, and the repair
/// thread's), from the socket they leave from, four times a second. The
/// child's video thread measures the lane's loss against it and reports it
/// (`RLR1`), which sizes the child's link repair here.
pub(crate) async fn run_child_ledgers(
    forwarder: Option<Arc<crate::RelayDownstreamForwarder>>,
    lease: Option<Arc<LeaseChildren>>,
    repair: Arc<PartRepair>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut every = tokio::time::interval(CHILD_LEDGER_EVERY);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = every.tick() => {}
        }
        let mut sent = 0_u64;
        if let Some(forwarder) = &forwarder {
            for (fd, target, count) in forwarder.ledgers() {
                let count = count + repair.extra_sent(target);
                let ledger = crate::fast_plane::loss_report::encode_ledger(count);
                if sys::send_batch(fd, 1, |_| (&ledger[..], target)).unwrap_or(0) == 1 {
                    sent += 1;
                }
            }
        }
        if let Some(lease) = &lease {
            let snapshot = lease.load();
            let mut done: Vec<std::net::SocketAddr> = Vec::new();
            for class in TrafficClass::ALL {
                let fd = lease.fd(class);
                for child in snapshot.children.iter() {
                    let target = child.targets[class.index()];
                    if done.contains(&target) {
                        continue;
                    }
                    done.push(target);
                    let count = child.sent[class.index()].load(Ordering::Relaxed)
                        + repair.extra_sent(target);
                    let ledger = crate::fast_plane::loss_report::encode_ledger(count);
                    if sys::send_batch(fd, 1, |_| (&ledger[..], target)).unwrap_or(0) == 1 {
                        sent += 1;
                    }
                }
            }
        }
        repair.stats.ledgers.fetch_add(sent, Ordering::Relaxed);
    }
}
