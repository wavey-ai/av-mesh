//! The control plane of the fast plane: everything about it that runs on a
//! timer. The fast plane itself (`src/fast_plane/`) has none: its threads
//! wait only on their sockets, and its tees only on their datagrams (a
//! test fails on any `tokio::time` there). What must happen now and then
//! happens here, and reaches the fast plane through atomics, snapshots and
//! notifications:
//!
//! - the tees' housekeeping: the cache rotation every 10 ms, and once a
//!   second the expiry of receive state, which the tee runs when told;
//! - the relay failover controller, which reads the threads' atomics;
//! - the lease children: their snapshot's backstop rebuild, their failover
//!   listener (whose promotions lapse), and the replay of a promoted
//!   child's warm sources, which waits for a full socket.

use crate::fast_plane::lease::{LeaseChild, LeaseChildren};
use crate::fast_plane::relay::FailoverInputs;
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
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = rotation.tick(), if rotate => {
                if let Err(error) = cache.rotate_if_due(false).await {
                    warn!(error = %format!("{error:#}"), "fast-plane cache rotation failed");
                }
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
