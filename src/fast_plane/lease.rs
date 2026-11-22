//! Lease-driven fast-plane children (`--relay-lease-children`).
//!
//! A forwarder (a distributor with class threads) already serves cache-mesh
//! playlist leases: an edge that has a reader or a UDP client for a stream
//! subscribes to it at each of its parents, and renews the lease while the
//! demand lasts. Each served lease (child, stream) also makes the child a
//! fast-plane child for that stream: the class threads hand it the
//! contributor's RLS1 symbols of the stream as they arrive, byte for byte,
//! after the compiled (long-haul) children and before the tee.
//!
//! The child's RLS1 address is its cache-mesh address with the port
//! replaced: the first of `--relay-lease-child-ports` when this forwarder is
//! its primary (`--relay-lease-primary-children`), the second when it is its
//! secondary, so the edge tells its two parents apart by the socket a symbol
//! arrives on, as a relay tells its two contributor lanes apart. Video goes
//! to that port plus the class offset. The symbols leave from one socket
//! pair bound at `--relay-lease-bind`, the address that the edge binds as
//! its parent.
//!
//! The per-class lane rule: an audio stream goes in full to every lease
//! child. A video stream goes in full to the children this forwarder is
//! primary for and as repair only to the others, which keep the sources in
//! a warm ring and get them, and the ring, once their failover controller
//! promotes this forwarder (RLF1 on `--relay-lease-failover-bind`).
//!
//! The threads read the children from an `ArcSwap` snapshot that a tokio
//! task rebuilds from the served leases whenever they change
//! (`CacheMeshHandle::served_changed`), so a child joins within one cache
//! mesh sync interval and the hot path takes no lock.

use super::sys;
use super::{class_addr, TrafficClass};
use crate::{now_unix_us, WarmSourceReplayBuffer};
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use playlists::mesh::{CacheMeshHandle, ServedSubscription};
use raptorq_datagram_fec::MediaDatagramRole;
use relay_session::{
    FailoverForwardMode, FailoverLeaseCommand, SubscriptionId, TopologyGeneration,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Lease children a forwarder feeds at most (one per edge).
pub const DEFAULT_MAX_LEASE_CHILDREN: usize = 16;
/// How often the children are rebuilt when no lease changes, as a backstop.
const REBUILD_BACKSTOP: Duration = Duration::from_secs(1);
/// How long a warm-source replay waits for a full socket in all.
const REPLAY_WAIT: Duration = Duration::from_millis(50);

/// How a forwarder feeds its lease children.
#[derive(Debug, Clone)]
pub struct LeaseChildConfig {
    /// The source address of every lease child's symbols: an edge binds it
    /// as its parent.
    pub bind: SocketAddr,
    /// The child's RLS1 port when this forwarder is its primary, and when it
    /// is its secondary.
    pub child_ports: [u16; 2],
    /// Video goes to the child's port plus this (0: the same port).
    pub class_port_offset: u16,
    /// Children (by address) that this forwarder is the primary for: they
    /// take a video stream in full; the others take its repair.
    pub primary_children: Vec<IpAddr>,
    /// Children fed at most. Further children are refused and logged.
    pub max_children: usize,
}

/// What a lease child takes of a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseTake {
    Send,
    /// A video source held back from a repair-only child: kept warm for its
    /// promotion.
    Warm,
}

/// One child of the lease snapshot. Its promotion and warm ring outlive a
/// rebuild of the snapshot.
#[derive(Debug)]
pub struct LeaseChild {
    pub ip: IpAddr,
    /// This forwarder is the child's primary: video in full.
    pub primary: bool,
    /// Where each class goes, by [`TrafficClass::index`].
    pub targets: [SocketAddr; 2],
    promoted: AtomicBool,
    warm: StdMutex<WarmSourceReplayBuffer>,
}

impl LeaseChild {
    /// The lane rule: audio in full to every child; video in full to the
    /// children this forwarder is primary for (or that promoted it), and
    /// repair only to the others, whose sources are kept warm.
    pub fn takes(&self, class: TrafficClass, role: MediaDatagramRole) -> LeaseTake {
        match (class, role) {
            (TrafficClass::Audio, _) | (TrafficClass::Video, MediaDatagramRole::Repair) => {
                LeaseTake::Send
            }
            (TrafficClass::Video, MediaDatagramRole::Source) => {
                if self.primary || self.promoted.load(Ordering::Relaxed) {
                    LeaseTake::Send
                } else {
                    LeaseTake::Warm
                }
            }
        }
    }
}

/// The children of each stream, as the class threads read them.
#[derive(Debug, Default)]
pub struct LeaseSnapshot {
    /// Indexes into `children`, by the stream id as RLS1 carries it.
    streams: HashMap<Box<str>, Box<[u16]>>,
    pub children: Box<[Arc<LeaseChild>]>,
}

impl LeaseSnapshot {
    /// The children of a stream, in a stable order.
    pub fn children_of(&self, stream: &str) -> &[u16] {
        self.streams.get(stream).map_or(&[], |children| children)
    }

    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    /// (child, stream) pairs.
    pub fn pairs(&self) -> usize {
        self.streams.values().map(|children| children.len()).sum()
    }
}

/// Counters of the lease children.
#[derive(Debug, Default)]
pub struct LeaseStats {
    pub children: AtomicU64,
    pub pairs: AtomicU64,
    /// Children refused because the forwarder had --relay-max-lease-children.
    pub refused: AtomicU64,
    pub rebuilds: AtomicU64,
    pub sent: [AtomicU64; 2],
    pub would_block: AtomicU64,
    pub send_errors: AtomicU64,
    pub warm_buffered: AtomicU64,
    pub warm_replayed: AtomicU64,
    pub warm_dropped: AtomicU64,
    pub promotions: AtomicU64,
    pub demotions: AtomicU64,
    pub commands_rejected: AtomicU64,
}

/// The lease children of a forwarder: their snapshot, their send sockets,
/// and their counters.
pub struct LeaseChildren {
    config: LeaseChildConfig,
    /// By class: the audio socket at SO_PRIORITY 6, its video twin on the
    /// same address.
    sockets: [std::net::UdpSocket; 2],
    snapshot: ArcSwap<LeaseSnapshot>,
    /// Every child fed, by address, kept across rebuilds while it has a
    /// lease (its promotion and warm ring), and the children refused.
    known: StdMutex<Known>,
    pub stats: LeaseStats,
}

#[derive(Default)]
struct Known {
    children: BTreeMap<IpAddr, Arc<LeaseChild>>,
    refused: HashSet<IpAddr>,
}

impl std::fmt::Debug for LeaseChildren {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaseChildren")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl LeaseChildren {
    /// Bind the send sockets at the configured address.
    pub fn bind(config: LeaseChildConfig, buffer_bytes: usize) -> Result<Arc<Self>> {
        let audio = sys::bind_reuseport(config.bind).with_context(|| {
            format!(
                "failed to bind the fast-plane lease children's socket {}",
                config.bind
            )
        })?;
        if let Err(error) = sys::set_priority(&audio, sys::AUDIO_SO_PRIORITY) {
            debug!(error = %error, "lease children's audio socket keeps its priority");
        }
        let local = audio.local_addr()?;
        let video = sys::bind_reuseport(local).with_context(|| {
            format!("failed to bind the video twin of the lease children's socket {local}")
        })?;
        for (socket, purpose) in [
            (&audio, "lease children audio"),
            (&video, "lease children video"),
        ] {
            if let Err(error) = crate::configure_udp_socket_buffers(socket, buffer_bytes, purpose) {
                warn!(error = %error, purpose, "could not size a lease children's socket");
            }
        }
        info!(
            bind = %local,
            child_ports = ?config.child_ports,
            primary_children = ?config.primary_children,
            max_children = config.max_children,
            "fast-plane lease children ready"
        );
        Ok(Arc::new(Self {
            config: LeaseChildConfig {
                bind: local,
                ..config
            },
            sockets: [audio, video],
            snapshot: ArcSwap::from_pointee(LeaseSnapshot::default()),
            known: StdMutex::new(Known::default()),
            stats: LeaseStats::default(),
        }))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.config.bind
    }

    pub fn fd(&self, class: TrafficClass) -> RawFd {
        self.sockets[class.index()].as_raw_fd()
    }

    pub fn load(&self) -> arc_swap::Guard<Arc<LeaseSnapshot>> {
        self.snapshot.load()
    }

    /// The child at an address, if it has a lease.
    pub fn child(&self, ip: IpAddr) -> Option<Arc<LeaseChild>> {
        self.known
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .children
            .get(&ip)
            .cloned()
    }

    fn new_child(&self, ip: IpAddr) -> Option<Arc<LeaseChild>> {
        let primary = self.config.primary_children.contains(&ip);
        let port = self.config.child_ports[usize::from(!primary)];
        let audio = SocketAddr::new(ip, port);
        let video = class_addr(audio, TrafficClass::Video, self.config.class_port_offset).ok()?;
        Some(Arc::new(LeaseChild {
            ip,
            primary,
            targets: [audio, video],
            promoted: AtomicBool::new(false),
            warm: StdMutex::new(WarmSourceReplayBuffer::default()),
        }))
    }

    /// Rebuild the snapshot from the leases the cache mesh serves. Children
    /// already fed keep their place; new ones are taken in address order
    /// up to the cap, and the rest are refused (logged once each).
    pub fn rebuild(&self, served: &[ServedSubscription]) {
        let mut by_child: BTreeMap<IpAddr, Vec<u64>> = BTreeMap::new();
        for subscription in served {
            by_child
                .entry(subscription.child.ip())
                .or_default()
                .push(subscription.stream_id);
        }
        let max = self.config.max_children;
        let mut known = self
            .known
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Children whose leases have all gone are forgotten, with their
        // warm rings.
        known.children.retain(|ip, _| by_child.contains_key(ip));
        known.refused.retain(|ip| by_child.contains_key(ip));
        for ip in by_child.keys() {
            if known.children.contains_key(ip) {
                continue;
            }
            if known.children.len() >= max {
                if known.refused.insert(*ip) {
                    self.stats.refused.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        child = %ip,
                        max_children = max,
                        "fast-plane lease child refused: the forwarder feeds --relay-max-lease-children already; it keeps the cache mesh"
                    );
                }
                continue;
            }
            let Some(child) = self.new_child(*ip) else {
                continue;
            };
            known.refused.remove(ip);
            info!(
                child = %ip,
                primary = child.primary,
                audio = %child.targets[0],
                video = %child.targets[1],
                "fast-plane lease child added"
            );
            known.children.insert(*ip, child);
        }
        let children: Vec<Arc<LeaseChild>> = known.children.values().cloned().collect();
        let index: HashMap<IpAddr, u16> = children
            .iter()
            .enumerate()
            .map(|(index, child)| (child.ip, index as u16))
            .collect();
        let mut streams: HashMap<Box<str>, Vec<u16>> = HashMap::new();
        for (ip, stream_ids) in &by_child {
            let Some(child) = index.get(ip) else {
                continue;
            };
            for stream_id in stream_ids {
                let entry = streams
                    .entry(stream_id.to_string().into_boxed_str())
                    .or_default();
                if !entry.contains(child) {
                    entry.push(*child);
                }
            }
        }
        drop(known);
        let snapshot = LeaseSnapshot {
            streams: streams
                .into_iter()
                .map(|(stream, mut children)| {
                    children.sort_unstable();
                    (stream, children.into_boxed_slice())
                })
                .collect(),
            children: children.into_boxed_slice(),
        };
        self.stats
            .children
            .store(snapshot.children.len() as u64, Ordering::Relaxed);
        self.stats
            .pairs
            .store(snapshot.pairs() as u64, Ordering::Relaxed);
        self.stats.rebuilds.fetch_add(1, Ordering::Relaxed);
        self.snapshot.store(Arc::new(snapshot));
    }

    /// Keep a video source held back from a repair-only child.
    pub fn buffer_warm(
        &self,
        child: &LeaseChild,
        object: u64,
        expires_at_us: u64,
        datagram: &[u8],
        now_us: u64,
    ) {
        let mutation = child
            .warm
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(object, expires_at_us, datagram, now_us);
        self.stats
            .warm_buffered
            .fetch_add(mutation.added_datagrams as u64, Ordering::Relaxed);
        self.stats
            .warm_dropped
            .fetch_add(mutation.removed_datagrams() as u64, Ordering::Relaxed);
    }

    /// Promote or demote a child's video. `Some(true)` when the child has
    /// just been promoted; `None` when no child has that address.
    pub fn apply_failover(&self, ip: IpAddr, mode: FailoverForwardMode) -> Option<bool> {
        let child = self.child(ip)?;
        let promoted = mode == FailoverForwardMode::SourceAndRepair;
        let previous = child.promoted.swap(promoted, Ordering::Relaxed);
        if previous != promoted {
            if promoted {
                self.stats.promotions.fetch_add(1, Ordering::Relaxed);
            } else {
                self.stats.demotions.fetch_add(1, Ordering::Relaxed);
            }
            info!(child = %ip, ?mode, "fast-plane lease child's video forwarding mode changed");
        }
        Some(!previous && promoted)
    }

    /// Replay a promoted child's warm video sources from the video socket.
    /// Returns the datagrams sent.
    pub async fn replay_warm(&self, child: &LeaseChild) -> usize {
        let batch = child
            .warm
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_live(now_unix_us());
        self.stats
            .warm_dropped
            .fetch_add(batch.expired_datagrams as u64, Ordering::Relaxed);
        let target = child.targets[TrafficClass::Video.index()];
        let fd = self.fd(TrafficClass::Video);
        let mut sent = 0;
        let started = Instant::now();
        while sent < batch.datagrams.len() {
            let left = batch.datagrams.len() - sent;
            let taken = sys::send_batch(fd, left, |index| {
                (&batch.datagrams[sent + index][..], target)
            })
            .unwrap_or(0);
            sent += taken;
            if taken == 0 {
                if started.elapsed() >= REPLAY_WAIT {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        self.stats
            .warm_replayed
            .fetch_add(sent as u64, Ordering::Relaxed);
        self.stats
            .warm_dropped
            .fetch_add((batch.datagrams.len() - sent) as u64, Ordering::Relaxed);
        sent
    }
}

/// Rebuild the lease children whenever the served leases change, and once a
/// second in any case.
pub async fn run_lease_children(
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
pub async fn run_lease_failover_listener(
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
                        let replayed = lease.replay_warm(&child).await;
                        debug!(child = %ip, replayed, "replayed a promoted lease child's warm sources");
                    }
                }
            }
        }
    }
}

/// The lease children's counters, for `/api/mesh`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct LeaseSnapshotStats {
    pub enabled: bool,
    pub children: u64,
    pub pairs: u64,
    pub refused: u64,
    pub rebuilds: u64,
    pub audio_sent: u64,
    pub video_sent: u64,
    pub would_block: u64,
    pub send_errors: u64,
    pub warm_buffered: u64,
    pub warm_replayed: u64,
    pub warm_dropped: u64,
    pub promotions: u64,
    pub demotions: u64,
    pub commands_rejected: u64,
}

impl LeaseStats {
    pub fn snapshot(&self) -> LeaseSnapshotStats {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        LeaseSnapshotStats {
            enabled: true,
            children: load(&self.children),
            pairs: load(&self.pairs),
            refused: load(&self.refused),
            rebuilds: load(&self.rebuilds),
            audio_sent: load(&self.sent[0]),
            video_sent: load(&self.sent[1]),
            would_block: load(&self.would_block),
            send_errors: load(&self.send_errors),
            warm_buffered: load(&self.warm_buffered),
            warm_replayed: load(&self.warm_replayed),
            warm_dropped: load(&self.warm_dropped),
            promotions: load(&self.promotions),
            demotions: load(&self.demotions),
            commands_rejected: load(&self.commands_rejected),
        }
    }
}

/// Parse `--relay-lease-child-ports PRIMARY,SECONDARY`.
pub fn parse_child_ports(value: &str) -> std::result::Result<[u16; 2], String> {
    let (primary, secondary) = value
        .split_once(',')
        .ok_or_else(|| "expected PRIMARY_PORT,SECONDARY_PORT".to_owned())?;
    let parse = |port: &str| {
        port.trim()
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .ok_or_else(|| format!("invalid port {port}"))
    };
    let ports = [parse(primary)?, parse(secondary)?];
    if ports[0] == ports[1] {
        return Err("the primary and secondary child ports must differ".to_owned());
    }
    Ok(ports)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn served(child: &str, stream_id: u64) -> ServedSubscription {
        ServedSubscription {
            child: child.parse().unwrap(),
            stream_id,
            backfill_parts_pending: 0,
        }
    }

    fn lease(max_children: usize, primary: &[&str]) -> Arc<LeaseChildren> {
        LeaseChildren::bind(
            LeaseChildConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                child_ports: [22_001, 22_002],
                class_port_offset: 1_000,
                primary_children: primary.iter().map(|ip| ip.parse().unwrap()).collect(),
                max_children,
            },
            1 << 20,
        )
        .unwrap()
    }

    #[test]
    fn a_lease_makes_its_child_a_child_of_its_stream_only() {
        let lease = lease(16, &["10.0.0.5"]);
        lease.rebuild(&[
            served("10.0.0.5:9101", 4001),
            served("10.0.0.6:9101", 4001),
            served("10.0.0.6:9101", 1),
        ]);
        let snapshot = lease.load();
        assert_eq!(snapshot.children.len(), 2);
        assert_eq!(snapshot.pairs(), 3);
        let of = |stream: &str| {
            snapshot
                .children_of(stream)
                .iter()
                .map(|index| snapshot.children[usize::from(*index)].ip.to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(of("4001"), ["10.0.0.5", "10.0.0.6"]);
        assert_eq!(of("1"), ["10.0.0.6"]);
        assert!(of("2").is_empty());
        // The primary child takes the primary port; the other the secondary.
        let five = lease.child("10.0.0.5".parse().unwrap()).unwrap();
        assert!(five.primary);
        assert_eq!(five.targets[0], "10.0.0.5:22001".parse().unwrap());
        assert_eq!(five.targets[1], "10.0.0.5:23001".parse().unwrap());
        let six = lease.child("10.0.0.6".parse().unwrap()).unwrap();
        assert!(!six.primary);
        assert_eq!(six.targets[0], "10.0.0.6:22002".parse().unwrap());
        assert_eq!(six.targets[1], "10.0.0.6:23002".parse().unwrap());
        // A lease that ends removes the child of that stream, and a child
        // with no lease left.
        lease.rebuild(&[served("10.0.0.6:9101", 1)]);
        let snapshot = lease.load();
        assert!(snapshot.children_of("4001").is_empty());
        assert_eq!(snapshot.children.len(), 1);
        assert!(lease.child("10.0.0.5".parse().unwrap()).is_none());
        lease.rebuild(&[]);
        assert!(lease.load().is_empty());
        assert_eq!(lease.stats.snapshot().children, 0);
    }

    #[test]
    fn children_above_the_cap_are_refused_and_counted_once() {
        let lease = lease(2, &[]);
        let leases = [
            served("10.0.0.3:9101", 7),
            served("10.0.0.1:9101", 7),
            served("10.0.0.2:9101", 7),
        ];
        lease.rebuild(&leases);
        lease.rebuild(&leases);
        let snapshot = lease.load();
        assert_eq!(snapshot.children.len(), 2);
        assert_eq!(
            lease.stats.snapshot().refused,
            1,
            "counted once, not per rebuild"
        );
        assert!(lease.child("10.0.0.3".parse().unwrap()).is_none());
        // A child that is fed keeps its place when a lower address comes.
        lease.rebuild(&[served("10.0.0.0:9101", 7), leases[1], leases[2], leases[0]]);
        assert!(lease.child("10.0.0.1".parse().unwrap()).is_some());
        assert!(lease.child("10.0.0.2".parse().unwrap()).is_some());
        assert!(lease.child("10.0.0.0".parse().unwrap()).is_none());
        // A place that frees up goes to a refused child.
        lease.rebuild(&[leases[0], leases[1]]);
        assert!(lease.child("10.0.0.3".parse().unwrap()).is_some());
    }

    #[test]
    fn the_lane_rule_sends_audio_to_all_and_video_sources_to_primaries() {
        let lease = lease(16, &["10.0.0.5"]);
        lease.rebuild(&[served("10.0.0.5:9101", 1), served("10.0.0.6:9101", 1)]);
        let primary = lease.child("10.0.0.5".parse().unwrap()).unwrap();
        let other = lease.child("10.0.0.6".parse().unwrap()).unwrap();
        for child in [&primary, &other] {
            for role in [MediaDatagramRole::Source, MediaDatagramRole::Repair] {
                assert_eq!(child.takes(TrafficClass::Audio, role), LeaseTake::Send);
            }
            assert_eq!(
                child.takes(TrafficClass::Video, MediaDatagramRole::Repair),
                LeaseTake::Send
            );
        }
        assert_eq!(
            primary.takes(TrafficClass::Video, MediaDatagramRole::Source),
            LeaseTake::Send
        );
        assert_eq!(
            other.takes(TrafficClass::Video, MediaDatagramRole::Source),
            LeaseTake::Warm
        );
        // A promotion of this forwarder sends the other child sources too,
        // and survives a rebuild.
        assert_eq!(
            lease.apply_failover(other.ip, FailoverForwardMode::SourceAndRepair),
            Some(true)
        );
        lease.rebuild(&[served("10.0.0.5:9101", 1), served("10.0.0.6:9101", 1)]);
        assert_eq!(
            lease
                .child(other.ip)
                .unwrap()
                .takes(TrafficClass::Video, MediaDatagramRole::Source),
            LeaseTake::Send
        );
        assert_eq!(
            lease.apply_failover(other.ip, FailoverForwardMode::SourceAndRepair),
            Some(false)
        );
        assert_eq!(
            lease.apply_failover("10.0.0.9".parse().unwrap(), FailoverForwardMode::RepairOnly),
            None
        );
    }

    #[test]
    fn child_ports_parse_as_primary_then_secondary() {
        assert_eq!(parse_child_ports("22001,22002").unwrap(), [22_001, 22_002]);
        assert!(parse_child_ports("22001").is_err());
        assert!(parse_child_ports("22001,22001").is_err());
        assert!(parse_child_ports("0,22002").is_err());
    }
}
