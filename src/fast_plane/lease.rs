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
//! Which of its two parents a forwarder is for a stream (wave 8 stage 5,
//! AM 12, `--relay-lease-ranked`): the child ranks the live distributors
//! of its region for each playlist and subscribes at the first two; its
//! Subscribe says where this forwarder stands (`ServedSubscription::rank`).
//! Rank 0 makes it the child's primary for that stream, any other rank its
//! secondary, so the fast-plane parents of an edge follow its ranking of
//! the live distributors stream by stream, a third distributor (dist-c)
//! included, and not the fixed pair of the deploy. A lease without a rank
//! (a child from before) keeps the fixed rule:
//! `--relay-lease-primary-children`. A child can so be this forwarder's
//! primary for one stream and its secondary for another: the snapshot
//! holds one entry for each (child, role).
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
//! mesh sync interval and the hot path takes no lock. That task, the
//! failover listener and the warm replay are in `crate::fast_control`,
//! with every other timer of the fast plane.

use super::sys;
use super::{class_addr, TrafficClass};
use crate::{now_unix_us, WarmSourceReplayBuffer};
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use playlists::mesh::ServedSubscription;
use raptorq_datagram_fec::MediaDatagramRole;
use relay_session::FailoverForwardMode;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tracing::{debug, info, warn};

/// Lease children a forwarder feeds at most (one per edge).
pub const DEFAULT_MAX_LEASE_CHILDREN: usize = 16;

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
    /// `--relay-lease-ranked`: a lease's rank (when the child gives one)
    /// says whether this forwarder is the child's primary for the stream.
    pub ranked: bool,
    /// `--relay-media-deadline-ms`, in µs: how long a source held back from
    /// a warm child is kept from its receipt.
    pub warm_hold_us: u64,
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
    /// Datagrams each class thread handed the kernel for this child, by
    /// [`TrafficClass::index`]: the counts of its ledgers (`RLL1`, 4k-repair
    /// link repair, on each lane).
    pub sent: [AtomicU64; 2],
}

impl LeaseChild {
    /// The child's failover controller promoted this forwarder: it takes
    /// video in full here.
    pub fn is_promoted(&self) -> bool {
        self.promoted.load(Ordering::Relaxed)
    }

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

/// The children of one stream, and when the fast plane last fed them.
#[derive(Debug)]
struct LeaseStream {
    /// Indexes into the snapshot's `children`.
    children: Box<[u16]>,
    /// When the class thread last planned a datagram of the stream to a
    /// child (unix µs), kept across rebuilds while the stream has a lease.
    fed_us: Arc<AtomicU64>,
}

/// The children of each stream, as the class threads read them.
#[derive(Debug, Default)]
pub struct LeaseSnapshot {
    /// By the stream id as RLS1 carries it.
    streams: HashMap<Box<str>, LeaseStream>,
    pub children: Box<[Arc<LeaseChild>]>,
}

/// The decimal form of a stream id, as RLS1 carries it, without allocating.
fn stream_digits(stream_id: u64, digits: &mut [u8; 20]) -> &str {
    let mut at = digits.len();
    let mut value = stream_id;
    loop {
        at -= 1;
        digits[at] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    std::str::from_utf8(&digits[at..]).unwrap_or_default()
}

impl LeaseSnapshot {
    /// Whether the child at `ip` takes stream `stream_id` here, and if so
    /// whether this forwarder is its primary. No allocation.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn feeds(&self, ip: IpAddr, stream_id: u64) -> Option<bool> {
        self.child_feeding(ip, stream_id).map(|child| child.primary)
    }

    /// The child at `ip`, if it takes stream `stream_id` here. No
    /// allocation.
    pub fn child_feeding(&self, ip: IpAddr, stream_id: u64) -> Option<&LeaseChild> {
        let mut digits = [0_u8; 20];
        self.children_of(stream_digits(stream_id, &mut digits))
            .iter()
            .map(|&child| &*self.children[usize::from(child)])
            .find(|child| child.ip == ip)
    }

    /// When the fast plane last fed stream `stream_id` to its children here
    /// (unix µs; 0: never, or no lease). No allocation.
    pub fn fed_us(&self, stream_id: u64) -> u64 {
        let mut digits = [0_u8; 20];
        self.streams
            .get(stream_digits(stream_id, &mut digits))
            .map_or(0, |stream| stream.fed_us.load(Ordering::Relaxed))
    }

    /// The children of a stream, in a stable order.
    pub fn children_of(&self, stream: &str) -> &[u16] {
        self.streams
            .get(stream)
            .map_or(&[], |stream| &stream.children)
    }

    /// The children of a stream, and the clock of its last feed, which the
    /// class thread sets when it plans a datagram to them.
    pub fn children_and_clock_of(&self, stream: &str) -> Option<(&[u16], &AtomicU64)> {
        self.streams
            .get(stream)
            .map(|stream| (&*stream.children, &*stream.fed_us))
    }

    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    /// (child, stream) pairs.
    pub fn pairs(&self) -> usize {
        self.streams
            .values()
            .map(|stream| stream.children.len())
            .sum()
    }
}

/// What a forwarder still pushes over the cache mesh to a lease child that
/// its fast plane feeds a stream to (`--cache-mesh-fast-plane-push` for
/// audio, `--cache-mesh-fast-plane-video-push` for video).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LivePush {
    /// Every leased pair is pushed, as before wave 7.
    All,
    /// Only the forwarder that does not feed the child the stream in full
    /// pushes: the edge's secondary (for video, until the edge promotes it).
    Secondary,
    /// No pair that the fast plane feeds is pushed.
    Nothing,
}

/// The cache mesh's live push filter of a forwarder (wave 7 item 4; video
/// wave 8 stage 3): a stream's parts are not pushed over the cache mesh to
/// a lease child that the fast plane feeds them to. With
/// [`LivePush::Secondary`] only the children that take the stream in full
/// from this forwarder are filtered (audio: those it is the primary of;
/// video: those it is the primary of or that promoted it), so their other
/// forwarder still pushes. The lease children are set in `slot` once they
/// run; until then nothing is filtered.
///
/// Video (wave 8 stage 3): an edge with a lease at both of its forwarders
/// took a 4K stream four ways, the primary's RLS1 sources and the
/// secondary's repair on the fast plane and both forwarders' cache-mesh
/// pushes (about 3.1 times the stream on its VLAN link locally), and each
/// forwarder pushed every edge a full copy. The referee's edges dropped at
/// the cache-mesh video socket (wave 7, 100-440 k datagrams each).
///
/// A lease says the fast plane should feed the pair, not that it does: a
/// pair is filtered only while the class thread fed the stream to the lease
/// children within the last [`FAST_PLANE_FED_WINDOW_US`]. If this
/// forwarder's relay ingest of the stream stops (its parents lost, a
/// subscription or generation mismatch, a stalled class thread) while its
/// cache still gets the parts another way, the push resumes, and the edge's
/// hole repair has arrivals to measure from (wave 7 review).
pub fn live_push_filter(
    slot: Arc<std::sync::OnceLock<Arc<LeaseChildren>>>,
    classes: super::StreamClasses,
    audio: LivePush,
    video: LivePush,
) -> playlists::mesh::CacheMeshLivePushFilter {
    Arc::new(move |peer: SocketAddr, stream_id: u64| {
        let is_video = classes.class(stream_id) == TrafficClass::Video;
        let mode = if is_video { video } else { audio };
        if mode == LivePush::All {
            return false;
        }
        let Some(lease) = slot.get() else {
            return false;
        };
        let snapshot = lease.load();
        let Some(child) = snapshot.child_feeding(peer.ip(), stream_id) else {
            return false;
        };
        let in_full = child.primary || (is_video && child.is_promoted());
        (mode == LivePush::Nothing || in_full)
            && crate::now_unix_us().saturating_sub(snapshot.fed_us(stream_id))
                <= FAST_PLANE_FED_WINDOW_US
    })
}

/// A pair counts as fed by the fast plane for this long after its stream's
/// last datagram to the lease children.
pub const FAST_PLANE_FED_WINDOW_US: u64 = 100_000;

/// Counters of the lease children.
#[derive(Debug, Default)]
pub struct LeaseStats {
    /// Children (addresses) fed, and (child, stream) pairs.
    pub children: AtomicU64,
    pub pairs: AtomicU64,
    /// Pairs this forwarder is the child's primary for, and pairs whose
    /// role came from the child's rank (AM 12).
    pub primary_pairs: AtomicU64,
    pub ranked_pairs: AtomicU64,
    /// Children refused because the forwarder had --relay-max-lease-children.
    pub refused: AtomicU64,
    pub rebuilds: AtomicU64,
    pub sent: [AtomicU64; 2],
    pub would_block: AtomicU64,
    pub send_errors: AtomicU64,
    pub warm_buffered: AtomicU64,
    pub warm_replayed: AtomicU64,
    pub warm_dropped: AtomicU64,
    /// Video sources not kept warm because a replay held the ring's lock
    /// at that moment (the class thread never waits for it).
    pub warm_refused: AtomicU64,
    pub promotions: AtomicU64,
    pub demotions: AtomicU64,
    pub commands_rejected: AtomicU64,
    /// Datagrams read (and dropped) from the lease sockets, which only send
    /// (`--relay-lease-drain-inbound`, wave 8 review F-I1).
    pub inbound: AtomicU64,
}

/// The lease children of a forwarder: their snapshot, their send sockets,
/// and their counters.
pub struct LeaseChildren {
    config: LeaseChildConfig,
    /// By class: the audio socket at SO_PRIORITY 6, its video twin on the
    /// same address.
    sockets: [std::net::UdpSocket; 2],
    snapshot: ArcSwap<LeaseSnapshot>,
    /// Snapshots replaced while a class thread read them (see `retired`).
    retired: super::retired::Retired<LeaseSnapshot>,
    /// Every child fed, by address, kept across rebuilds while it has a
    /// lease (its promotion and warm ring), and the children refused.
    known: StdMutex<Known>,
    pub stats: LeaseStats,
    /// The node's part repair (4k-repair): what children send to the lease
    /// sockets (asks, loss reports) goes to it.
    pub part_repair: std::sync::OnceLock<Arc<super::part_repair::PartRepair>>,
}

#[derive(Default)]
struct Known {
    /// By (address, primary): a child that ranks this forwarder first for
    /// one stream and second for another has an entry for each.
    children: BTreeMap<(IpAddr, bool), Arc<LeaseChild>>,
    refused: HashSet<IpAddr>,
    /// The feed clock of each leased stream, kept across rebuilds.
    fed: HashMap<Box<str>, Arc<AtomicU64>>,
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
        // Faulted in here: the class threads write its counters.
        Ok(crate::memory::touched_arc(Self {
            config: LeaseChildConfig {
                bind: local,
                ..config
            },
            sockets: [audio, video],
            snapshot: ArcSwap::from_pointee(LeaseSnapshot::default()),
            retired: super::retired::Retired::default(),
            known: StdMutex::new(Known::default()),
            stats: LeaseStats::default(),
            part_repair: std::sync::OnceLock::new(),
        }))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.config.bind
    }

    /// Children (addresses) fed at most (`--relay-max-lease-children`).
    pub fn max_children(&self) -> usize {
        self.config.max_children
    }

    /// Copies of the lease sockets for a reader of what arrives on them
    /// (`crate::fast_control::spawn_lease_inbound_drain`): they only send.
    pub fn inbound_sockets(&self) -> Vec<std::net::UdpSocket> {
        self.sockets
            .iter()
            .filter_map(|socket| {
                socket
                    .try_clone()
                    .map_err(|error| warn!(error = %error, "lease socket not drained"))
                    .ok()
            })
            .collect()
    }

    pub fn fd(&self, class: TrafficClass) -> RawFd {
        self.sockets[class.index()].as_raw_fd()
    }

    pub fn load(&self) -> arc_swap::Guard<Arc<LeaseSnapshot>> {
        self.snapshot.load()
    }

    /// The child at an address, if it has a lease: its entry as a child
    /// that takes video repair here (the one a promotion acts on), else its
    /// entry as a primary child.
    pub fn child(&self, ip: IpAddr) -> Option<Arc<LeaseChild>> {
        let known = self
            .known
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        known
            .children
            .get(&(ip, false))
            .or_else(|| known.children.get(&(ip, true)))
            .cloned()
    }

    /// Whether this forwarder is the primary of a lease's child for its
    /// stream: by the child's rank when it gives one (and the rule is on),
    /// else by --relay-lease-primary-children.
    fn is_primary(&self, subscription: &ServedSubscription) -> bool {
        match subscription.rank.filter(|_| self.config.ranked) {
            Some(rank) => rank == 0,
            None => self
                .config
                .primary_children
                .contains(&subscription.child.ip()),
        }
    }

    fn new_child(&self, ip: IpAddr, primary: bool) -> Option<Arc<LeaseChild>> {
        let port = self.config.child_ports[usize::from(!primary)];
        let audio = SocketAddr::new(ip, port);
        let video = class_addr(audio, TrafficClass::Video, self.config.class_port_offset).ok()?;
        Some(Arc::new(LeaseChild {
            ip,
            primary,
            targets: [audio, video],
            promoted: AtomicBool::new(false),
            // A repair-only child's video sources are kept warm by the
            // video thread: in a buffer made whole here, off that thread
            // (fix4-video). A primary child takes them all and keeps none.
            warm: StdMutex::new(if primary {
                WarmSourceReplayBuffer::default()
            } else {
                WarmSourceReplayBuffer::preallocated()
            }),
            sent: [AtomicU64::new(0), AtomicU64::new(0)],
        }))
    }

    /// Rebuild the snapshot from the leases the cache mesh serves. Children
    /// already fed keep their place; new ones are taken in address order
    /// up to the cap, and the rest are refused (logged once each).
    pub fn rebuild(&self, served: &[ServedSubscription]) {
        let mut by_child: BTreeMap<(IpAddr, bool), Vec<u64>> = BTreeMap::new();
        let mut ranked_pairs = 0_u64;
        for subscription in served {
            if self.config.ranked && subscription.rank.is_some() {
                ranked_pairs += 1;
            }
            by_child
                .entry((subscription.child.ip(), self.is_primary(subscription)))
                .or_default()
                .push(subscription.stream_id);
        }
        let ips: HashSet<IpAddr> = by_child.keys().map(|(ip, _)| *ip).collect();
        let max = self.config.max_children;
        let mut known = self
            .known
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Children whose leases have all gone are forgotten, with their
        // warm rings (and an entry for a role the child no longer gives).
        known.children.retain(|key, _| by_child.contains_key(key));
        known.refused.retain(|ip| ips.contains(ip));
        for &(ip, primary) in by_child.keys() {
            if known.children.contains_key(&(ip, primary)) {
                continue;
            }
            // The cap counts children (addresses), not their roles.
            let fed: HashSet<IpAddr> = known.children.keys().map(|(ip, _)| *ip).collect();
            if !fed.contains(&ip) && fed.len() >= max {
                if known.refused.insert(ip) {
                    self.stats.refused.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        child = %ip,
                        max_children = max,
                        "fast-plane lease child refused: the forwarder feeds --relay-max-lease-children already; it keeps the cache mesh"
                    );
                }
                continue;
            }
            let Some(child) = self.new_child(ip, primary) else {
                continue;
            };
            known.refused.remove(&ip);
            info!(
                child = %ip,
                primary = child.primary,
                audio = %child.targets[0],
                video = %child.targets[1],
                "fast-plane lease child added"
            );
            known.children.insert((ip, primary), child);
        }
        let children: Vec<Arc<LeaseChild>> = known.children.values().cloned().collect();
        let index: HashMap<(IpAddr, bool), u16> = children
            .iter()
            .enumerate()
            .map(|(index, child)| ((child.ip, child.primary), index as u16))
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
        known.fed.retain(|stream, _| streams.contains_key(stream));
        let streams = streams
            .into_iter()
            .map(|(stream, mut children)| {
                children.sort_unstable();
                let fed_us = Arc::clone(known.fed.entry(stream.clone()).or_default());
                (
                    stream,
                    LeaseStream {
                        children: children.into_boxed_slice(),
                        fed_us,
                    },
                )
            })
            .collect();
        drop(known);
        let snapshot = LeaseSnapshot {
            streams,
            children: children.into_boxed_slice(),
        };
        let addresses: HashSet<IpAddr> = snapshot.children.iter().map(|child| child.ip).collect();
        let primary_pairs: usize = snapshot
            .streams
            .values()
            .map(|stream| {
                stream
                    .children
                    .iter()
                    .filter(|&&child| snapshot.children[usize::from(child)].primary)
                    .count()
            })
            .sum();
        self.stats
            .children
            .store(addresses.len() as u64, Ordering::Relaxed);
        self.stats
            .primary_pairs
            .store(primary_pairs as u64, Ordering::Relaxed);
        self.stats.ranked_pairs.store(ranked_pairs, Ordering::Relaxed);
        self.stats
            .pairs
            .store(snapshot.pairs() as u64, Ordering::Relaxed);
        self.stats.rebuilds.fetch_add(1, Ordering::Relaxed);
        self.retired.keep(self.snapshot.swap(Arc::new(snapshot)));
    }

    /// Keep a video source held back from a repair-only child, for the
    /// media budget from `now_us`.
    ///
    /// On the video thread: it never waits for the lock (a replay holds it
    /// for a swap) and keeps the datagram only in a buffer made whole
    /// before (fix4-video); else the datagram is counted in
    /// `warm_refused`.
    pub fn buffer_warm(&self, child: &LeaseChild, object: u64, datagram: &[u8], now_us: u64) {
        let mut warm = match child.warm.try_lock() {
            Ok(warm) => warm,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                self.stats.warm_refused.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        if !warm.is_fixed() {
            drop(warm);
            self.stats.warm_refused.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mutation = warm.push(object, self.config.warm_hold_us, datagram, now_us);
        drop(warm);
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

    /// Take a promoted child's warm video sources that are still live, for
    /// a replay (`fast_control::replay_warm`), and count the expired ones.
    ///
    /// A buffer made whole leaves with its datagrams: a new one, made here
    /// before the lock, takes its place in one swap, so the video thread
    /// never allocates nor finds the lock held for more than the swap.
    pub fn take_warm(&self, child: &LeaseChild) -> Vec<Vec<u8>> {
        let fixed = child
            .warm
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_fixed();
        let mut taken = if fixed {
            WarmSourceReplayBuffer::preallocated()
        } else {
            WarmSourceReplayBuffer::default()
        };
        std::mem::swap(
            &mut *child
                .warm
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            &mut taken,
        );
        let batch = taken.take_live(now_unix_us());
        self.stats
            .warm_dropped
            .fetch_add(batch.expired_datagrams as u64, Ordering::Relaxed);
        batch.datagrams
    }
}

/// The lease children's counters, for `/api/mesh`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct LeaseSnapshotStats {
    pub enabled: bool,
    pub children: u64,
    pub pairs: u64,
    pub primary_pairs: u64,
    pub ranked_pairs: u64,
    pub refused: u64,
    pub rebuilds: u64,
    pub audio_sent: u64,
    pub video_sent: u64,
    pub would_block: u64,
    pub send_errors: u64,
    pub warm_buffered: u64,
    pub warm_replayed: u64,
    pub warm_dropped: u64,
    pub warm_refused: u64,
    pub promotions: u64,
    pub demotions: u64,
    pub commands_rejected: u64,
    pub inbound: u64,
}

impl LeaseStats {
    pub fn snapshot(&self) -> LeaseSnapshotStats {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        LeaseSnapshotStats {
            enabled: true,
            children: load(&self.children),
            pairs: load(&self.pairs),
            primary_pairs: load(&self.primary_pairs),
            ranked_pairs: load(&self.ranked_pairs),
            refused: load(&self.refused),
            rebuilds: load(&self.rebuilds),
            audio_sent: load(&self.sent[0]),
            video_sent: load(&self.sent[1]),
            would_block: load(&self.would_block),
            send_errors: load(&self.send_errors),
            warm_buffered: load(&self.warm_buffered),
            warm_replayed: load(&self.warm_replayed),
            warm_dropped: load(&self.warm_dropped),
            warm_refused: load(&self.warm_refused),
            promotions: load(&self.promotions),
            demotions: load(&self.demotions),
            commands_rejected: load(&self.commands_rejected),
            inbound: load(&self.inbound),
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
            rank: None,
        }
    }

    fn ranked(child: &str, stream_id: u64, rank: u8) -> ServedSubscription {
        ServedSubscription {
            rank: Some(rank),
            ..served(child, stream_id)
        }
    }

    fn lease_with(max_children: usize, primary: &[&str], ranked: bool) -> Arc<LeaseChildren> {
        LeaseChildren::bind(
            LeaseChildConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                child_ports: [22_001, 22_002],
                class_port_offset: 1_000,
                primary_children: primary.iter().map(|ip| ip.parse().unwrap()).collect(),
                max_children,
                ranked,
                warm_hold_us: 1_000_000,
            },
            1 << 20,
        )
        .unwrap()
    }

    fn lease(max_children: usize, primary: &[&str]) -> Arc<LeaseChildren> {
        lease_with(max_children, primary, true)
    }

    /// Wave 8 review (F-I1): what arrives at the lease sockets (an edge's
    /// demand report to its lease parent) is read and counted, not left to
    /// fill their receive buffers.
    #[tokio::test]
    async fn what_arrives_at_the_lease_sockets_is_read_and_counted() {
        let lease = lease(16, &[]);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        let tasks = crate::fast_control::spawn_lease_inbound_drain(&lease, shutdown_rx);
        assert_eq!(tasks.len(), 2);
        let child = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        for _ in 0..20 {
            child.send_to(b"RLD1....", lease.local_addr()).unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while lease.stats.snapshot().inbound < 20 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("every datagram read");
        let _ = shutdown_tx.send(());
        for task in tasks {
            task.await.unwrap();
        }
    }

    #[test]
    fn a_ranked_lease_makes_this_forwarder_primary_for_its_first_parent_streams() {
        // The deploy names 10.0.0.5 a primary child; its leases say rank 1
        // for stream 1 and rank 0 for stream 2: the rank wins.
        let lease = lease(16, &["10.0.0.5"]);
        lease.rebuild(&[
            ranked("10.0.0.5:9101", 1, 1),
            ranked("10.0.0.5:9101", 2, 0),
            ranked("10.0.0.6:9101", 1, 0),
            // A lease without a rank keeps the fixed rule (not primary).
            served("10.0.0.7:9101", 1),
        ]);
        let snapshot = lease.load();
        let five: IpAddr = "10.0.0.5".parse().unwrap();
        let six: IpAddr = "10.0.0.6".parse().unwrap();
        let seven: IpAddr = "10.0.0.7".parse().unwrap();
        assert_eq!(snapshot.feeds(five, 1), Some(false));
        assert_eq!(snapshot.feeds(five, 2), Some(true));
        assert_eq!(snapshot.feeds(six, 1), Some(true));
        assert_eq!(snapshot.feeds(seven, 1), Some(false));
        // The primary entry sends to the first port, the other to the second.
        let as_primary = snapshot.child_feeding(five, 2).unwrap();
        assert_eq!(as_primary.targets[0], "10.0.0.5:22001".parse().unwrap());
        assert_eq!(as_primary.targets[1], "10.0.0.5:23001".parse().unwrap());
        let as_secondary = snapshot.child_feeding(five, 1).unwrap();
        assert_eq!(as_secondary.targets[0], "10.0.0.5:22002".parse().unwrap());
        assert_eq!(
            as_secondary.takes(TrafficClass::Video, MediaDatagramRole::Source),
            LeaseTake::Warm
        );
        assert_eq!(
            as_primary.takes(TrafficClass::Video, MediaDatagramRole::Source),
            LeaseTake::Send
        );
        // Children count addresses, not roles.
        let stats = lease.stats.snapshot();
        assert_eq!((stats.children, stats.pairs), (3, 4));
        assert_eq!((stats.primary_pairs, stats.ranked_pairs), (2, 3));
        // A promotion acts on the entry that takes repair here.
        assert_eq!(
            lease.apply_failover(five, FailoverForwardMode::SourceAndRepair),
            Some(true)
        );
        assert_eq!(
            lease
                .load()
                .child_feeding(five, 1)
                .unwrap()
                .takes(TrafficClass::Video, MediaDatagramRole::Source),
            LeaseTake::Send
        );
        // The child re-ranks: this forwarder is now its first parent for
        // stream 1 as well; the secondary entry goes, with its warm ring.
        lease.rebuild(&[
            ranked("10.0.0.5:9101", 1, 0),
            ranked("10.0.0.5:9101", 2, 0),
        ]);
        assert_eq!(lease.load().feeds(five, 1), Some(true));
        assert_eq!(lease.load().children.len(), 1);
        assert!(lease.child(five).unwrap().primary);
    }

    #[test]
    fn with_the_ranked_rule_off_the_fixed_primary_list_decides() {
        let lease = lease_with(16, &["10.0.0.5"], false);
        lease.rebuild(&[ranked("10.0.0.5:9101", 1, 1), ranked("10.0.0.6:9101", 1, 0)]);
        let snapshot = lease.load();
        assert_eq!(snapshot.feeds("10.0.0.5".parse().unwrap(), 1), Some(true));
        assert_eq!(snapshot.feeds("10.0.0.6".parse().unwrap(), 1), Some(false));
        assert_eq!(lease.stats.snapshot().ranked_pairs, 0);
    }

    #[test]
    fn the_cap_counts_a_child_with_two_roles_once() {
        let lease = lease(1, &[]);
        lease.rebuild(&[
            ranked("10.0.0.5:9101", 1, 0),
            ranked("10.0.0.5:9101", 2, 1),
            ranked("10.0.0.6:9101", 1, 0),
        ]);
        let snapshot = lease.load();
        assert_eq!(snapshot.children.len(), 2, "both roles of 10.0.0.5");
        assert!(snapshot.feeds("10.0.0.6".parse().unwrap(), 1).is_none());
        assert_eq!(lease.stats.snapshot().refused, 1);
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
    fn the_push_filter_names_the_audio_pairs_that_the_fast_plane_feeds() {
        let lease = lease(16, &["10.0.0.5"]);
        lease.rebuild(&[
            served("10.0.0.5:9101", 4001),
            served("10.0.0.6:9101", 4001),
            served("10.0.0.6:9101", 1),
            served("10.0.0.6:9101", 18_446_744_073_709_551_615),
        ]);
        let snapshot = lease.load();
        let five: IpAddr = "10.0.0.5".parse().unwrap();
        let six: IpAddr = "10.0.0.6".parse().unwrap();
        assert_eq!(snapshot.feeds(five, 4001), Some(true));
        assert_eq!(snapshot.feeds(six, 4001), Some(false));
        assert_eq!(snapshot.feeds(five, 1), None);
        assert_eq!(snapshot.feeds(six, u64::MAX), Some(false));
        assert_eq!(snapshot.feeds("10.0.0.7".parse().unwrap(), 4001), None);

        let classes = crate::fast_plane::StreamClasses::default();
        classes.mark_video(1);
        let slot = Arc::new(std::sync::OnceLock::new());
        let none = live_push_filter(
            Arc::clone(&slot),
            classes.clone(),
            LivePush::Nothing,
            LivePush::All,
        );
        let primary = live_push_filter(
            Arc::clone(&slot),
            classes,
            LivePush::Secondary,
            LivePush::All,
        );
        let peer = |ip: &str| -> SocketAddr { format!("{ip}:9101").parse().unwrap() };
        // Nothing is filtered before the lease children run.
        assert!(!none(peer("10.0.0.5"), 4001));
        slot.set(Arc::clone(&lease)).unwrap();
        // Nor while the fast plane has not fed the stream: a lease is not
        // a delivery.
        assert!(!none(peer("10.0.0.5"), 4001));
        let feed = |stream: &str, at_us: u64| {
            lease
                .load()
                .children_and_clock_of(stream)
                .unwrap()
                .1
                .store(at_us, Ordering::Relaxed);
        };
        feed("4001", crate::now_unix_us());
        feed("18446744073709551615", crate::now_unix_us());
        // `none`: no audio push to any child the fast plane feeds.
        assert!(none(peer("10.0.0.5"), 4001));
        assert!(none(peer("10.0.0.6"), 4001));
        // `secondary`: the primary forwarder stops; the secondary pushes.
        assert!(primary(peer("10.0.0.5"), 4001));
        assert!(!primary(peer("10.0.0.6"), 4001));
        // Video keeps the push; a child without a lease here is no pair.
        assert!(!none(peer("10.0.0.6"), 1));
        assert!(!none(peer("10.0.0.7"), 4001));
        // The feed stops (the relay ingest of the stream stopped): the
        // push resumes once the window has passed, for both modes.
        feed(
            "4001",
            crate::now_unix_us() - FAST_PLANE_FED_WINDOW_US - 1_000,
        );
        assert!(!none(peer("10.0.0.5"), 4001));
        assert!(!primary(peer("10.0.0.5"), 4001));
        assert!(none(peer("10.0.0.6"), u64::MAX), "another stream still fed");
        // The clock outlives a rebuild of the snapshot.
        feed("4001", crate::now_unix_us());
        lease.rebuild(&[
            served("10.0.0.5:9101", 4001),
            served("10.0.0.6:9101", 4001),
        ]);
        assert!(none(peer("10.0.0.5"), 4001));
    }

    #[test]
    fn the_video_push_filter_keeps_the_push_of_a_child_that_takes_only_repair_here() {
        // This forwarder is 10.0.0.5's primary and 10.0.0.6's secondary.
        let lease = lease(16, &["10.0.0.5"]);
        lease.rebuild(&[served("10.0.0.5:9101", 1), served("10.0.0.6:9101", 1)]);
        let classes = crate::fast_plane::StreamClasses::default();
        classes.mark_video(1);
        let slot = Arc::new(std::sync::OnceLock::new());
        slot.set(Arc::clone(&lease)).unwrap();
        let filter = |video| {
            live_push_filter(Arc::clone(&slot), classes.clone(), LivePush::All, video)
        };
        let (all, secondary, none) = (
            filter(LivePush::All),
            filter(LivePush::Secondary),
            filter(LivePush::Nothing),
        );
        let peer = |ip: &str| -> SocketAddr { format!("{ip}:9101").parse().unwrap() };
        let fed = |at_us: u64| {
            lease
                .load()
                .children_and_clock_of("1")
                .unwrap()
                .1
                .store(at_us, Ordering::Relaxed);
        };
        // Not fed yet: every pair is pushed.
        assert!(!secondary(peer("10.0.0.5"), 1));
        assert!(!none(peer("10.0.0.5"), 1));
        fed(crate::now_unix_us());
        // `all`: video is pushed as before.
        assert!(!all(peer("10.0.0.5"), 1));
        assert!(!all(peer("10.0.0.6"), 1));
        // `secondary`: the child that takes the stream in full here (its
        // primary) is not pushed; the repair-only child still is.
        assert!(secondary(peer("10.0.0.5"), 1));
        assert!(!secondary(peer("10.0.0.6"), 1));
        // Once the repair-only child promotes this forwarder it takes the
        // sources here, and its push stops too; a demotion brings it back.
        let six: IpAddr = "10.0.0.6".parse().unwrap();
        assert_eq!(
            lease.apply_failover(six, FailoverForwardMode::SourceAndRepair),
            Some(true)
        );
        assert!(secondary(peer("10.0.0.6"), 1));
        lease.apply_failover(six, FailoverForwardMode::RepairOnly);
        assert!(!secondary(peer("10.0.0.6"), 1));
        // `none`: no pair the fast plane feeds is pushed.
        assert!(none(peer("10.0.0.5"), 1));
        assert!(none(peer("10.0.0.6"), 1));
        // The audio mode does not touch video, nor the video mode audio.
        lease.rebuild(&[served("10.0.0.5:9101", 4001)]);
        lease
            .load()
            .children_and_clock_of("4001")
            .unwrap()
            .1
            .store(crate::now_unix_us(), Ordering::Relaxed);
        assert!(!none(peer("10.0.0.5"), 4001));
        // The feed stops: the push resumes once the window has passed.
        lease.rebuild(&[served("10.0.0.5:9101", 1)]);
        fed(crate::now_unix_us() - FAST_PLANE_FED_WINDOW_US - 1_000);
        assert!(!secondary(peer("10.0.0.5"), 1));
        assert!(!none(peer("10.0.0.5"), 1));
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
