//! Relay and distributor ingest on the fast plane: per-class forward-first
//! OS threads and the tee behind them (see the module above).
//!
//! `--relay-class-threads true` runs this in place of `run_udp_fec_ingest`;
//! `false` keeps that loop, which validates and decodes every symbol before
//! it forwards it.

use super::burst_copy::BurstCopies;
use super::dedup::FirstArrival;
use super::edge_clients::{ClientPlan, EdgeFastLane, UpstreamRepair};
use super::lease::{LeaseChildren, LeaseSnapshot, LeaseSnapshotStats, LeaseTake};
use super::loss_report::LossMeter;
use super::sys::{self, RecvBatch, RECV_BATCH};
use super::view::RlsView;
use super::{class_addr, StreamClasses, TrafficClass};
use crate::link_telemetry::LinkTelemetry;
use crate::{
    commit_relay_object, configure_udp_socket_buffers, journal_relay_symbol, now_unix_ms,
    now_unix_us, process_relay_audio_epoch_datagram, process_udp_fec_ingest_datagram_inner,
    relay_publication_clock, take_relay_deadline_drop_delta, AudioEpochDatagram, LiveTsCache,
    NativeAudioRelay, RelayDownstreamForwarder, RelayForwardRole, RelayIngestRuntime,
    RelayIngressParentPath, CANONICAL_STREAM_IDLE_RETENTION,
};
use anyhow::{Context, Result};
use av_mesh::relay_ingress::{
    is_relay_session_datagram, PeerAdmission, RelayIngressError, RelayIngressOutcome,
    RelayIngressSnapshot, RelayUdpDispatch, RelayUdpDispatchOutcome,
};
use av_mesh::udp_fec::UdpFecReceiver;
use bytes::Bytes;
use media_object::ObjectKey;
use raptorq_datagram_fec::{MediaDatagramRole, MediaObjectKind};
use relay_session::RelayLimits;
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, mpsc, watch, Notify};
use tracing::{debug, info, warn};

/// Datagrams a class's tee holds before the thread drops its tee copy.
pub const DEFAULT_TEE_CAPACITY: usize = 65_536;
/// Symbols the first-arrival table remembers per class.
const DEDUP_CAPACITY: usize = 65_536;
/// How often an idle class thread looks at the stop flag.
const STOP_POLL_MS: i32 = 50;
/// SCHED_FIFO priority of the audio thread.
const AUDIO_FIFO_PRIORITY: i32 = 10;
/// Nice value of the video thread.
const VIDEO_NICE: i32 = -10;

/// How the class threads run.
#[derive(Debug, Clone)]
pub struct RelayClassConfig {
    /// Video sockets at the audio ports plus this offset; 0 binds audio
    /// sockets only (every class arrives there).
    pub port_offset: u16,
    /// CPU of each class's thread, by [`TrafficClass::index`].
    pub cpus: [Option<usize>; 2],
    /// Audio at SCHED_FIFO 10, video at nice -10, where the host allows.
    pub realtime: bool,
    pub tee_capacity: usize,
    /// `--relay-video-gso`: the video thread hands each child the symbols
    /// of a drained batch as UDP GSO messages (one pass through the stack
    /// for up to 64 symbols) instead of one `sendmmsg` entry per symbol.
    pub video_gso: bool,
    /// `--relay-lease-children`: the children that the served cache-mesh
    /// leases make, fed after the compiled children (see `lease`).
    pub lease: Option<Arc<LeaseChildren>>,
    /// `--client-udp-fast-lane`: the edge's UDP clients that take the fast
    /// lane, fed after the children and before the tee (see
    /// `edge_clients`).
    pub edge_clients: Option<Arc<EdgeFastLane>>,
    /// `--edge-hole-distance-ms`: the tee finds an edge's holes by part
    /// distance as it takes each symbol (see `edge_holes`).
    pub holes: Option<Arc<crate::edge_holes::HoleTrigger>>,
    /// `--relay-burst-copy-ms` (wave 5, W2): the audio thread sends each
    /// audio source symbol to the compiled children again this long after
    /// it forwarded it (see `burst_copy`); 0 sends no copy.
    pub burst_copy_ms: u64,
    /// `--relay-loss-report` (wave 7 item 2): the audio thread measures the
    /// loss of each parent lane and reports it to the parent (see
    /// `loss_report`).
    pub loss_report: bool,
    /// Tests: the tee of this class panics on its first datagram.
    #[cfg(test)]
    pub test_tee_panic: Option<TrafficClass>,
}

impl Default for RelayClassConfig {
    fn default() -> Self {
        Self {
            port_offset: 1_000,
            cpus: [None, None],
            realtime: true,
            tee_capacity: DEFAULT_TEE_CAPACITY,
            video_gso: true,
            lease: None,
            edge_clients: None,
            holes: None,
            burst_copy_ms: 0,
            loss_report: false,
            #[cfg(test)]
            test_tee_panic: None,
        }
    }
}

/// One receive socket of the fast plane.
pub struct ClassSocket {
    pub class: TrafficClass,
    pub path: RelayIngressParentPath,
    pub socket: std::net::UdpSocket,
}

/// Bind the receive sockets: the primary (and secondary) port for audio,
/// and each at `port_offset` higher for video.
pub fn bind_class_sockets(
    primary: SocketAddr,
    secondary: Option<SocketAddr>,
    port_offset: u16,
    buffer_bytes: usize,
) -> Result<Vec<ClassSocket>> {
    let mut sockets = Vec::new();
    let classes: &[TrafficClass] = if port_offset == 0 {
        &[TrafficClass::Audio]
    } else {
        &TrafficClass::ALL
    };
    for (path, bind) in [
        (RelayIngressParentPath::Primary, Some(primary)),
        (RelayIngressParentPath::Secondary, secondary),
    ] {
        let Some(bind) = bind else {
            continue;
        };
        for &class in classes {
            let address = class_addr(bind, class, port_offset)?;
            let socket = sys::bind_plain(address).with_context(|| {
                format!(
                    "failed to bind the {} RelaySession ingest on {address}",
                    class.as_str()
                )
            })?;
            let purpose = format!("{} {:?} relay ingest", class.as_str(), path);
            let state = configure_udp_socket_buffers(&socket, buffer_bytes, &purpose)?;
            if !state.target_met() {
                warn!(
                    purpose,
                    requested_bytes = buffer_bytes,
                    actual_receive_bytes = state.receive_bytes,
                    "UDP socket buffer target was limited by the operating system"
                );
            }
            if let Err(error) = sys::enable_receive_metadata(&socket) {
                debug!(error = %error, "kernel receive timestamps unavailable");
            }
            info!(
                bind = %socket.local_addr()?,
                class = class.as_str(),
                ?path,
                "fast-plane RelaySession ingest listening"
            );
            sockets.push(ClassSocket {
                class,
                path,
                socket,
            });
        }
    }
    Ok(sockets)
}

/// Counters of the fast plane, summed over its threads.
#[derive(Debug, Default)]
pub struct FastPlaneStats {
    pub received: [AtomicU64; 2],
    pub forwarded: [AtomicU64; 2],
    pub duplicates: AtomicU64,
    pub not_admitted: AtomicU64,
    pub invalid: AtomicU64,
    pub tee_dropped: AtomicU64,
    pub would_block: AtomicU64,
    pub send_errors: AtomicU64,
    pub truncated: AtomicU64,
    /// Datagrams the kernel dropped on a full receive queue (SO_RXQ_OVFL).
    pub kernel_dropped: AtomicU64,
    pub realtime_threads: AtomicU64,
    pub pinned_threads: AtomicU64,
    pub threads: AtomicU64,
    /// Video datagrams sent inside UDP GSO messages.
    pub gso_datagrams: AtomicU64,
    /// UDP GSO messages (each one pass through the stack).
    pub gso_messages: AtomicU64,
    /// Threads that found UDP GSO unavailable and send without it.
    pub gso_fallbacks: AtomicU64,
    /// Burst copies sent to compiled children, and dropped (a full ring, a
    /// full socket).
    pub burst_copies: AtomicU64,
    pub burst_copies_dropped: AtomicU64,
    /// Loss reports (RLR1) sent to parents, and the highest loss (ppm) that
    /// the audio thread measures on a parent lane now.
    pub loss_reports: AtomicU64,
    pub parent_loss_ppm: AtomicU64,
    /// The lease children, whose counters the snapshot reports.
    pub lease: OnceLock<Arc<LeaseChildren>>,
}

/// What `/api/mesh` reports of the fast plane.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(default)]
pub struct FastPlaneSnapshot {
    pub enabled: bool,
    pub threads: u64,
    pub realtime_threads: u64,
    pub pinned_threads: u64,
    pub audio_received: u64,
    pub video_received: u64,
    pub audio_forwarded: u64,
    pub video_forwarded: u64,
    pub duplicates: u64,
    pub not_admitted: u64,
    pub invalid: u64,
    pub tee_dropped: u64,
    pub would_block: u64,
    pub send_errors: u64,
    pub truncated: u64,
    pub kernel_dropped: u64,
    pub gso_datagrams: u64,
    pub gso_messages: u64,
    pub gso_fallbacks: u64,
    pub burst_copies: u64,
    pub burst_copies_dropped: u64,
    pub loss_reports: u64,
    pub parent_loss_ppm: u64,
    pub lease_children: LeaseSnapshotStats,
}

impl FastPlaneStats {
    pub fn snapshot(&self) -> FastPlaneSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        FastPlaneSnapshot {
            enabled: true,
            threads: load(&self.threads),
            realtime_threads: load(&self.realtime_threads),
            pinned_threads: load(&self.pinned_threads),
            audio_received: load(&self.received[0]),
            video_received: load(&self.received[1]),
            audio_forwarded: load(&self.forwarded[0]),
            video_forwarded: load(&self.forwarded[1]),
            duplicates: load(&self.duplicates),
            not_admitted: load(&self.not_admitted),
            invalid: load(&self.invalid),
            tee_dropped: load(&self.tee_dropped),
            would_block: load(&self.would_block),
            send_errors: load(&self.send_errors),
            truncated: load(&self.truncated),
            kernel_dropped: load(&self.kernel_dropped),
            gso_datagrams: load(&self.gso_datagrams),
            gso_messages: load(&self.gso_messages),
            gso_fallbacks: load(&self.gso_fallbacks),
            burst_copies: load(&self.burst_copies),
            burst_copies_dropped: load(&self.burst_copies_dropped),
            loss_reports: load(&self.loss_reports),
            parent_loss_ppm: load(&self.parent_loss_ppm),
            lease_children: self
                .lease
                .get()
                .map(|lease| lease.stats.snapshot())
                .unwrap_or_default(),
        }
    }
}

/// What the class threads saw last, for the failover controller: times in
/// µs since `base`, plus one (zero is never).
pub(crate) struct FailoverInputs {
    pub(crate) base: Instant,
    pub(crate) primary_source: AtomicU64,
    pub(crate) secondary_repair: AtomicU64,
    pub(crate) secondary_source: AtomicU64,
    pub(crate) decoded: AtomicU64,
}

impl FailoverInputs {
    fn new() -> Self {
        Self {
            base: Instant::now(),
            primary_source: AtomicU64::new(0),
            secondary_repair: AtomicU64::new(0),
            secondary_source: AtomicU64::new(0),
            decoded: AtomicU64::new(0),
        }
    }

    fn stamp(&self, slot: &AtomicU64, now: Instant) {
        let us = now.saturating_duration_since(self.base).as_micros() as u64;
        slot.store(us.saturating_add(1), Ordering::Relaxed);
    }

    fn observe(&self, path: RelayIngressParentPath, role: MediaDatagramRole, now: Instant) {
        match (path, role) {
            (RelayIngressParentPath::Primary, MediaDatagramRole::Source) => {
                self.stamp(&self.primary_source, now);
            }
            (RelayIngressParentPath::Secondary, MediaDatagramRole::Repair) => {
                self.stamp(&self.secondary_repair, now);
            }
            (RelayIngressParentPath::Secondary, MediaDatagramRole::Source) => {
                self.stamp(&self.secondary_source, now);
            }
            (RelayIngressParentPath::Primary, MediaDatagramRole::Repair) => {}
        }
    }
}

/// One datagram handed from a class thread to its tee.
struct TeeItem {
    peer: SocketAddr,
    path: RelayIngressParentPath,
    bytes: Bytes,
    /// Arrival time: the kernel's when the socket reports it.
    rx_us: u64,
    /// When the thread read it off the socket (its batch's wake-up).
    woken_us: u64,
    /// The edge's fast client lane handed the symbol to the kernel for this
    /// client (the first of its send) at this time.
    client_sent: Option<(SocketAddr, u64)>,
    kind: TeeKind,
}

enum TeeKind {
    /// An RLS1 symbol that passed the session checks: forwarded (to the
    /// first child at that time, if any child took it).
    Admitted {
        forwarded: Option<(SocketAddr, u64)>,
        video_kind: bool,
    },
    /// Anything that is not RLS1: legacy RQD2, AEP1, native-audio control.
    /// The tee runs today's path for it.
    Unchecked,
}

#[derive(Debug, Clone, Copy)]
struct PlannedSend {
    message: u16,
    path: u16,
    role: MediaDatagramRole,
    /// The send takes a burst copy (a source to a compiled child; not the
    /// one-symbol source of a repair lane).
    copy: bool,
}

/// A source symbol held back from a warm repair-only child, buffered for
/// its promotion once the batch's sends are done.
#[derive(Debug, Clone, Copy)]
struct PlannedWarm {
    message: u16,
    path: u16,
    object: u64,
    expires_at_us: u64,
}

/// A send to a lease child: `child` indexes the snapshot's children.
#[derive(Debug, Clone, Copy)]
struct PlannedLease {
    message: u16,
    child: u16,
}

/// The sends of one drained batch, by source socket.
pub(crate) struct ForwardPlan {
    groups: Vec<Vec<PlannedSend>>,
    retry: Vec<(usize, PlannedSend)>,
    warm: Vec<PlannedWarm>,
    /// Sends to lease children, after every compiled child's.
    lease: Vec<PlannedLease>,
    /// Video sources held back from repair-only lease children: `path` is
    /// the child's index in the snapshot.
    lease_warm: Vec<PlannedWarm>,
    first: [Option<(SocketAddr, u64)>; RECV_BATCH],
}

impl ForwardPlan {
    fn new(groups: usize, warm_paths: usize) -> Self {
        Self {
            groups: (0..groups)
                .map(|_| Vec::with_capacity(RECV_BATCH))
                .collect(),
            retry: Vec::with_capacity(RECV_BATCH),
            warm: Vec::with_capacity(RECV_BATCH * warm_paths),
            lease: Vec::with_capacity(RECV_BATCH * 4),
            lease_warm: Vec::new(),
            first: [None; RECV_BATCH],
        }
    }

    fn clear(&mut self) {
        for group in &mut self.groups {
            group.clear();
        }
        self.retry.clear();
        self.warm.clear();
        self.lease.clear();
        self.lease_warm.clear();
        self.first = [None; RECV_BATCH];
    }
}

/// Plan the sends of one admitted symbol to the lease children of its
/// stream, by the lane rule.
fn plan_lease(
    snapshot: &LeaseSnapshot,
    class: TrafficClass,
    message: usize,
    view: &RlsView<'_>,
    plan: &mut ForwardPlan,
) {
    let mut object = None;
    for &child in snapshot.children_of(view.stream) {
        match snapshot.children[usize::from(child)].takes(class, view.role) {
            LeaseTake::Send => plan.lease.push(PlannedLease {
                message: message as u16,
                child,
            }),
            LeaseTake::Warm => plan.lease_warm.push(PlannedWarm {
                message: message as u16,
                path: child,
                object: *object.get_or_insert_with(|| view_object_id(view)),
                expires_at_us: view.deadline_us,
            }),
        }
    }
}

/// Send the lease plan from the lease children's socket of the class: one
/// non-blocking `sendmmsg` (video as UDP GSO messages, each child's symbols
/// together), then one more try each for what the kernel did not take.
/// Never waits. Then keep the held-back video sources warm.
#[allow(clippy::too_many_arguments)]
fn send_lease_plan(
    lease: &LeaseChildren,
    snapshot: &LeaseSnapshot,
    class: TrafficClass,
    batch: &RecvBatch,
    plan: &mut ForwardPlan,
    stats: &FastPlaneStats,
    gso: &mut bool,
    now_us: u64,
) {
    let ForwardPlan {
        lease: sends,
        lease_warm,
        first,
        ..
    } = plan;
    if !sends.is_empty() {
        let fd = lease.fd(class);
        let target =
            |send: &PlannedLease| snapshot.children[usize::from(send.child)].targets[class.index()];
        let use_gso = *gso && class == TrafficClass::Video;
        if use_gso {
            // A child's symbols in a row make one GSO message; the order of
            // a child's own symbols is kept.
            sends.sort_by_key(|send| send.child);
        }
        let message = |index: usize| {
            let send = &sends[index];
            (batch.data(usize::from(send.message)), target(send))
        };
        let mut sent = None;
        if use_gso {
            match sys::send_gso_batch(fd, sends.len(), message) {
                Ok(gso_sent) => {
                    stats
                        .gso_datagrams
                        .fetch_add(gso_sent.datagrams as u64, Ordering::Relaxed);
                    stats
                        .gso_messages
                        .fetch_add(gso_sent.messages as u64, Ordering::Relaxed);
                    sent = Some(gso_sent.datagrams);
                }
                Err(error) if sys::gso_unsupported(&error) => {
                    *gso = false;
                    stats.gso_fallbacks.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        error = %error,
                        "UDP GSO is unavailable: the video thread sends each symbol on its own"
                    );
                }
                Err(_) => sent = Some(0),
            }
        }
        let sent = match sent {
            Some(sent) => sent,
            None => sys::send_batch(fd, sends.len(), message).unwrap_or(0),
        };
        let sent_us = now_unix_us();
        for send in &sends[..sent] {
            first[usize::from(send.message)].get_or_insert((target(send), sent_us));
        }
        let mut delivered = sent as u64;
        let (mut would_block, mut errors) = (0_u64, 0_u64);
        // One more try each for what the kernel did not take, until the
        // socket is full. Refused symbols are counted, never waited for:
        // the child's FEC or its other parent covers them.
        sys::send_rest(
            sent,
            sends.len(),
            |index| {
                let send = &sends[index];
                sys::send_batch(fd, 1, |_| {
                    (batch.data(usize::from(send.message)), target(send))
                })
            },
            |index, outcome| match outcome {
                sys::RestOutcome::Sent => {
                    delivered += 1;
                    let send = &sends[index];
                    first[usize::from(send.message)].get_or_insert((target(send), now_unix_us()));
                }
                sys::RestOutcome::WouldBlock => would_block += 1,
                sys::RestOutcome::Error => errors += 1,
            },
        );
        if would_block > 0 {
            lease
                .stats
                .would_block
                .fetch_add(would_block, Ordering::Relaxed);
        }
        if errors > 0 {
            lease.stats.send_errors.fetch_add(errors, Ordering::Relaxed);
        }
        lease.stats.sent[class.index()].fetch_add(delivered, Ordering::Relaxed);
    }
    for warm in lease_warm.iter() {
        lease.buffer_warm(
            &snapshot.children[usize::from(warm.path)],
            warm.object,
            warm.expires_at_us,
            batch.data(usize::from(warm.message)),
            now_us,
        );
    }
}

/// The warm-buffer identity of the object of a symbol, read in place.
pub(crate) fn view_object_id(view: &RlsView<'_>) -> u64 {
    crate::warm_object_id(
        view.tenant,
        view.stream,
        view.track,
        view.epoch,
        view.group,
        view.object,
        view.version,
        view.payload_hash,
    )
}

impl RelayDownstreamForwarder {
    /// Plan the sends of one admitted symbol: every path whose lane takes
    /// its role, in compiled order. A source symbol held back from a warm
    /// repair-only child is noted, and kept for a later promotion only once
    /// the batch is sent (`buffer_fast_warm`), as `forward` keeps it.
    fn plan_fast(&self, message: usize, view: &RlsView<'_>, plan: &mut ForwardPlan) {
        let role = view.role;
        let mut object = None;
        // A one-symbol audio object has nothing a repair lane could carry
        // but its symbol when its parent sent no repair (wave 7 item 2):
        // the repair-lane child takes the source, so it still has a copy
        // from this forwarder beside its primary's.
        let single_symbol = matches!(role, MediaDatagramRole::Source)
            && view.source_symbols == 1
            && view.kind == MediaObjectKind::Audio
            && self.repair_lane_single_symbol();
        for (index, path) in self.paths.iter().enumerate() {
            let promoted = path.promoted.load(Ordering::Relaxed);
            let repair_lane_source =
                path.role == RelayForwardRole::Repair && matches!(role, MediaDatagramRole::Source);
            let failover_source = promoted && repair_lane_source;
            let single_symbol_source = single_symbol && repair_lane_source && !failover_source;
            if path.role.permits(role) || failover_source || single_symbol_source {
                plan.groups[path.group].push(PlannedSend {
                    message: message as u16,
                    path: index as u16,
                    role,
                    copy: !single_symbol_source,
                });
                continue;
            }
            if path.role == RelayForwardRole::Repair && matches!(role, MediaDatagramRole::Source) {
                plan.warm.push(PlannedWarm {
                    message: message as u16,
                    path: index as u16,
                    object: *object.get_or_insert_with(|| view_object_id(view)),
                    expires_at_us: view.deadline_us,
                });
            }
            self.filtered_datagrams.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Buffer the batch's held-back sources, in the warm buffers of the
    /// thread's class.
    fn buffer_fast_warm(
        &self,
        class: TrafficClass,
        batch: &RecvBatch,
        plan: &ForwardPlan,
        now_us: u64,
    ) {
        for warm in &plan.warm {
            self.buffer_warm_source_class(
                &self.paths[usize::from(warm.path)],
                class,
                warm.object,
                warm.expires_at_us,
                batch.data(usize::from(warm.message)),
                now_us,
            );
        }
    }

    /// Repair-only children, whose held-back sources a plan notes.
    fn warm_paths(&self) -> usize {
        self.paths
            .iter()
            .filter(|path| path.role == RelayForwardRole::Repair)
            .count()
    }

    fn class_fd(&self, class: TrafficClass, group: usize) -> std::os::fd::RawFd {
        match (class, self.video_sockets.get(group)) {
            (TrafficClass::Video, Some(twin)) => twin.as_raw_fd(),
            _ => self.paths[self.socket_groups[group][0]].socket.as_raw_fd(),
        }
    }

    fn class_target(&self, class: TrafficClass, path: usize) -> SocketAddr {
        match class {
            TrafficClass::Audio => self.paths[path].target,
            TrafficClass::Video => self.paths[path].video_target,
        }
    }

    /// Send a plan: one non-blocking `sendmmsg` per source socket, then one
    /// more try for what the kernel did not take. Never waits. With `gso`
    /// (video only) a child's run of equal symbols is one UDP GSO message;
    /// a host without UDP GSO clears `gso` and the batch goes without it.
    fn send_fast_plan(
        &self,
        class: TrafficClass,
        batch: &RecvBatch,
        plan: &mut ForwardPlan,
        stats: &FastPlaneStats,
        gso: &mut bool,
    ) {
        let ForwardPlan {
            groups,
            retry,
            first,
            ..
        } = plan;
        let use_gso = *gso && class == TrafficClass::Video;
        for (group, sends) in groups.iter_mut().enumerate() {
            if sends.is_empty() {
                continue;
            }
            if use_gso && self.socket_groups[group].len() > 1 {
                // Children that share a source socket: the plan takes them
                // in turn symbol by symbol, which leaves GSO nothing to join.
                // Each child's symbols together (its own order kept, the
                // children in compiled order) make one message per child.
                sends.sort_unstable_by_key(|send| (send.path, send.message));
            }
            let sends = &*sends;
            let fd = self.class_fd(class, group);
            let started = Instant::now();
            let message = |index: usize| {
                let send = sends[index];
                (
                    batch.data(usize::from(send.message)),
                    self.class_target(class, usize::from(send.path)),
                )
            };
            let mut sent = None;
            if use_gso && *gso {
                match sys::send_gso_batch(fd, sends.len(), message) {
                    Ok(gso_sent) => {
                        stats
                            .gso_datagrams
                            .fetch_add(gso_sent.datagrams as u64, Ordering::Relaxed);
                        stats
                            .gso_messages
                            .fetch_add(gso_sent.messages as u64, Ordering::Relaxed);
                        sent = Some(gso_sent.datagrams);
                    }
                    Err(error) if sys::gso_unsupported(&error) => {
                        *gso = false;
                        stats.gso_fallbacks.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            error = %error,
                            "UDP GSO is unavailable: the video thread sends each symbol on its own"
                        );
                    }
                    Err(_) => sent = Some(0),
                }
            }
            let sent = match sent {
                Some(sent) => sent,
                None => sys::send_batch(fd, sends.len(), message).unwrap_or(0),
            };
            let sent_us = now_unix_us();
            let elapsed = started.elapsed();
            let (mut sources, mut repairs, mut bytes) = (0_u64, 0_u64, 0_u64);
            for send in &sends[..sent] {
                bytes += batch.data(usize::from(send.message)).len() as u64;
                match send.role {
                    MediaDatagramRole::Source => sources += 1,
                    MediaDatagramRole::Repair => repairs += 1,
                }
                first[usize::from(send.message)]
                    .get_or_insert((self.class_target(class, usize::from(send.path)), sent_us));
            }
            self.record_forward_batch(sources, repairs, bytes, elapsed);
            stats.forwarded[class.index()].fetch_add(sent as u64, Ordering::Relaxed);
            retry.extend(sends[sent..].iter().map(|send| (group, *send)));
        }
        for (group, send) in retry.drain(..) {
            let data = batch.data(usize::from(send.message));
            let target = self.class_target(class, usize::from(send.path));
            let started = Instant::now();
            match sys::send_batch(self.class_fd(class, group), 1, |_| (data, target)) {
                Ok(1) => {
                    self.record_forward_success(send.role, data.len(), started.elapsed());
                    stats.forwarded[class.index()].fetch_add(1, Ordering::Relaxed);
                    first[usize::from(send.message)].get_or_insert((target, now_unix_us()));
                }
                Err(error) if error.kind() != std::io::ErrorKind::WouldBlock => {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                    stats.send_errors.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    // Counted, never waited for: the child's FEC or its
                    // other parent covers the symbol.
                    self.errors.fetch_add(1, Ordering::Relaxed);
                    stats.would_block.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

/// One class's forwarding thread.
struct ClassThread {
    class: TrafficClass,
    sockets: Vec<(RelayIngressParentPath, std::net::UdpSocket)>,
    admissions: Arc<HashMap<SocketAddr, PeerAdmission>>,
    limits: RelayLimits,
    forwarder: Option<Arc<RelayDownstreamForwarder>>,
    link_telemetry: Option<Arc<LinkTelemetry>>,
    tee: mpsc::Sender<TeeItem>,
    stats: Arc<FastPlaneStats>,
    failover: Arc<FailoverInputs>,
    stop: Arc<AtomicBool>,
    cpu: Option<usize>,
    realtime: bool,
    video_gso: bool,
    lease: Option<Arc<LeaseChildren>>,
    edge_clients: Option<Arc<EdgeFastLane>>,
    burst_copy_ms: u64,
    loss_report: bool,
}

/// How often the audio thread looks at its loss meters.
const LOSS_POLL: Duration = Duration::from_millis(100);

struct Pending {
    message: usize,
    peer: SocketAddr,
    path: RelayIngressParentPath,
    rx_us: u64,
    admitted: Option<bool>,
}

impl ClassThread {
    fn schedule(&self) {
        let mut normal = Vec::new();
        if let Some(cpu) = self.cpu {
            match crate::fast_path::pin_current_thread(cpu) {
                Ok(()) => {
                    self.stats.pinned_threads.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => normal.push(error),
            }
        }
        if self.realtime {
            let result = match self.class {
                TrafficClass::Audio => sys::set_fifo(AUDIO_FIFO_PRIORITY),
                TrafficClass::Video => sys::set_nice(VIDEO_NICE),
            };
            match result {
                Ok(()) => {
                    self.stats.realtime_threads.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => normal.push(error),
            }
        }
        if !normal.is_empty() {
            info!(
                class = self.class.as_str(),
                reasons = ?normal,
                "fast-plane thread runs with normal scheduling"
            );
        }
    }

    fn run(self) {
        self.schedule();
        self.stats.threads.fetch_add(1, Ordering::Relaxed);
        let fds: Vec<_> = self
            .sockets
            .iter()
            .map(|(_, socket)| socket.as_raw_fd())
            .collect();
        let mut batch = RecvBatch::new();
        let mut dedup = FirstArrival::new(DEDUP_CAPACITY);
        let mut plan = self.forwarder.as_ref().map_or_else(
            || ForwardPlan::new(0, 0),
            |forwarder| ForwardPlan::new(forwarder.socket_groups.len(), forwarder.warm_paths()),
        );
        let mut pending = Vec::with_capacity(RECV_BATCH);
        // UDP GSO for video until the host refuses it once (the edge's
        // clients and the children apart).
        let mut gso = self.video_gso && self.class == TrafficClass::Video;
        let mut clients = ClientPlan::new()
            .with_gso(gso)
            .with_video(self.class == TrafficClass::Video);
        let mut kernel_dropped = vec![0_u32; self.sockets.len()];
        // Burst copies of the audio sources for the compiled children.
        let mut copies = (self.class == TrafficClass::Audio && self.forwarder.is_some())
            .then(|| BurstCopies::new(self.burst_copy_ms.saturating_mul(1_000)))
            .flatten();
        // The loss of each parent lane, on the audio thread.
        let mut meters: Vec<LossMeter> = if self.loss_report && self.class == TrafficClass::Audio {
            self.sockets.iter().map(|_| LossMeter::new()).collect()
        } else {
            Vec::new()
        };
        let mut last_loss_poll = Instant::now();
        while !self.stop.load(Ordering::Relaxed) {
            // No longer than the next copy's due time.
            let timeout = copies
                .as_ref()
                .and_then(|copies| copies.wait_ms(now_unix_us()))
                .map_or(STOP_POLL_MS, |wait| wait.min(STOP_POLL_MS));
            let ready = match sys::wait_readable(&fds, timeout) {
                Ok(ready) => ready,
                Err(error) => {
                    warn!(class = self.class.as_str(), error = %error, "fast-plane wait failed");
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
            };
            for (index, (path, socket)) in self.sockets.iter().enumerate() {
                if ready & (1 << index) == 0 {
                    continue;
                }
                loop {
                    let received = match batch.recv(socket) {
                        Ok(received) => received,
                        Err(error) => {
                            debug!(error = %error, "fast-plane receive error");
                            0
                        }
                    };
                    if received == 0 {
                        break;
                    }
                    self.drain(
                        &batch,
                        *path,
                        &mut kernel_dropped[index],
                        &mut dedup,
                        &mut plan,
                        &mut pending,
                        &mut clients,
                        &mut gso,
                        copies.as_mut(),
                        meters.get_mut(index),
                    );
                    if received < RECV_BATCH {
                        break;
                    }
                }
            }
            if let Some(copies) = copies.as_mut() {
                self.send_copies(copies);
            }
            if !meters.is_empty() && last_loss_poll.elapsed() >= LOSS_POLL {
                last_loss_poll = Instant::now();
                self.report_loss(&fds, &mut meters, last_loss_poll);
            }
        }
    }

    /// Send each parent whose lane's loss changed (or is still above zero)
    /// its report, from the socket its symbols come to.
    fn report_loss(&self, fds: &[std::os::fd::RawFd], meters: &mut [LossMeter], now: Instant) {
        let mut worst = 0_u32;
        for (index, meter) in meters.iter_mut().enumerate() {
            if let Some((peer, report)) = meter.poll(now) {
                if sys::send_batch(fds[index], 1, |_| (&report[..], peer)).unwrap_or(0) == 1 {
                    self.stats.loss_reports.fetch_add(1, Ordering::Relaxed);
                }
            }
            worst = worst.max(meter.loss_ppm());
        }
        self.stats
            .parent_loss_ppm
            .store(u64::from(worst), Ordering::Relaxed);
    }

    /// Send the burst copies that are due, after the batch's own sends.
    fn send_copies(&self, copies: &mut BurstCopies) {
        let Some(forwarder) = &self.forwarder else {
            return;
        };
        if copies.is_empty() {
            return;
        }
        let dropped = copies.dropped;
        let sent = copies.send_due(now_unix_us(), |group, run| {
            sys::send_batch(forwarder.class_fd(self.class, group), run.len(), |index| {
                run[index]
            })
            .unwrap_or(0)
        });
        if sent > 0 {
            self.stats.burst_copies.fetch_add(sent, Ordering::Relaxed);
        }
        if copies.dropped > dropped {
            self.stats
                .burst_copies_dropped
                .fetch_add(copies.dropped - dropped, Ordering::Relaxed);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn drain(
        &self,
        batch: &RecvBatch,
        path: RelayIngressParentPath,
        kernel_dropped: &mut u32,
        dedup: &mut FirstArrival,
        plan: &mut ForwardPlan,
        pending: &mut Vec<Pending>,
        clients: &mut ClientPlan,
        gso: &mut bool,
        copies: Option<&mut BurstCopies>,
        mut meter: Option<&mut LossMeter>,
    ) {
        let now_us = now_unix_us();
        let now = Instant::now();
        pending.clear();
        plan.clear();
        let class = self.class;
        // The lease children as they are now, without a lock.
        let lease = self
            .lease
            .as_deref()
            .map(|lease| (lease, lease.load()))
            .filter(|(_, snapshot)| !snapshot.is_empty());
        // The edge's fast-lane clients as they are now, without a lock.
        let edge_clients = self
            .edge_clients
            .as_deref()
            .map(|lane| (lane, lane.load()))
            .filter(|(_, snapshot)| !snapshot.streams.is_empty());
        clients.clear();
        self.stats.received[class.index()].fetch_add(batch.len() as u64, Ordering::Relaxed);
        for message in 0..batch.len() {
            let received = batch.get(message);
            if let Some(dropped) = received.dropped {
                if dropped != *kernel_dropped {
                    self.stats.kernel_dropped.fetch_add(
                        u64::from(dropped.wrapping_sub(*kernel_dropped)),
                        Ordering::Relaxed,
                    );
                    *kernel_dropped = dropped;
                }
            }
            if received.truncated {
                self.stats.truncated.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let Some(peer) = received.peer else {
                continue;
            };
            if let Some(telemetry) = &self.link_telemetry {
                match path {
                    RelayIngressParentPath::Primary => {
                        telemetry.record_primary_bytes(peer, received.data.len());
                    }
                    RelayIngressParentPath::Secondary => {
                        telemetry.record_secondary_bytes(peer, received.data.len());
                    }
                }
            }
            let rx_us = received.kernel_us.unwrap_or(now_us);
            let unchecked = Pending {
                message,
                peer,
                path,
                rx_us,
                admitted: None,
            };
            if !is_relay_session_datagram(received.data) {
                pending.push(unchecked);
                continue;
            }
            // An RLS1 datagram that is malformed, or that its peer's session
            // does not admit, is counted and dropped here: the tee could only
            // reject it, and a flood of them must not fill the tee ahead of
            // admitted symbols.
            let Ok(view) = RlsView::parse(received.data, self.limits) else {
                self.stats.invalid.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            let admitted = self.admissions.get(&peer).is_some_and(|admission| {
                admission.admits(view.generation, view.subscription_id, view.path_intent)
            });
            if !admitted {
                self.stats.not_admitted.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            self.failover.observe(path, view.role, now);
            if let Some(meter) = meter.as_deref_mut() {
                meter.observe(peer, view.packet_sequence, now);
            }
            let key = dedup.key(
                view.tenant,
                view.stream,
                view.track,
                view.group,
                view.version,
                view.block_id,
                view.epoch,
                view.object,
                view.packet_sequence,
            );
            if !dedup.first(key) {
                self.stats.duplicates.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if let Some(forwarder) = &self.forwarder {
                forwarder.plan_fast(message, &view, plan);
            }
            if let Some((_, snapshot)) = &lease {
                plan_lease(snapshot, class, message, &view, plan);
            }
            if let Some((lane, snapshot)) = &edge_clients {
                clients.plan(lane, snapshot, message, &view);
            }
            pending.push(Pending {
                admitted: Some(view.is_video_kind()),
                ..unchecked
            });
        }
        if let Some(forwarder) = &self.forwarder {
            forwarder.send_fast_plan(class, batch, plan, &self.stats, gso);
            // Held-back sources are kept only once every child has its
            // datagrams, in this class's own buffers.
            forwarder.buffer_fast_warm(class, batch, plan, now_us);
            // The sources' burst copies, due their delay from now.
            if let Some(copies) = copies {
                let dropped = copies.dropped;
                for (group, sends) in plan.groups.iter().enumerate() {
                    for send in sends
                        .iter()
                        .filter(|send| matches!(send.role, MediaDatagramRole::Source) && send.copy)
                    {
                        copies.push(
                            batch.data(usize::from(send.message)),
                            group,
                            forwarder.class_target(class, usize::from(send.path)),
                            now_us,
                        );
                    }
                }
                if copies.dropped > dropped {
                    self.stats
                        .burst_copies_dropped
                        .fetch_add(copies.dropped - dropped, Ordering::Relaxed);
                }
            }
        }
        // The lease children (the edges) after the compiled children (the
        // long-haul forwarders).
        if let Some((lease, snapshot)) = &lease {
            send_lease_plan(
                lease,
                snapshot,
                class,
                batch,
                plan,
                &self.stats,
                gso,
                now_us,
            );
        }
        // The edge's clients after its children, in one sendmmsg from the
        // client socket of the class.
        if let Some((lane, _)) = &edge_clients {
            clients.send(lane, class);
        }
        // The tee copies come after every child has its datagrams.
        for entry in pending.drain(..) {
            let kind = match entry.admitted {
                Some(video_kind) => TeeKind::Admitted {
                    forwarded: plan.first[entry.message],
                    video_kind,
                },
                None => TeeKind::Unchecked,
            };
            let item = TeeItem {
                peer: entry.peer,
                path: entry.path,
                bytes: Bytes::copy_from_slice(batch.data(entry.message)),
                rx_us: entry.rx_us,
                woken_us: now_us,
                client_sent: clients.sent_to(entry.message),
                kind,
            };
            if self.tee.try_send(item).is_err() {
                self.stats.tee_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// `relay_woken`: when the class thread read the part's first symbol off its
/// socket, beside `relay_first`, the kernel's receive time of it.
fn journal_woken(cache: &LiveTsCache, key: &ObjectKey, peer: SocketAddr, woken_us: u64) {
    let Some(journal) = cache.hops() else {
        return;
    };
    let Ok(stream) = key.stream().parse::<u64>() else {
        return;
    };
    journal.record_first(
        crate::hop_journal::HopEvent::new(
            crate::hop_journal::HopStage::RelayWoken,
            stream,
            key.object(),
            woken_us,
        )
        .with_peer(Some(peer)),
    );
}

/// The relay ingress snapshots of both tees, merged for `/api/mesh`.
fn merge_snapshots(snapshots: &[RelayIngressSnapshot; 2]) -> RelayIngressSnapshot {
    let [audio, video] = snapshots;
    let mut merged = *audio;
    merged.active_objects += video.active_objects;
    merged.completed_objects += video.completed_objects;
    merged.active_object_bytes += video.active_object_bytes;
    merged.buffered_datagrams += video.buffered_datagrams;
    let (a, v) = (&mut merged.counters, &video.counters);
    a.datagrams_received += v.datagrams_received;
    a.datagrams_rejected += v.datagrams_rejected;
    a.source_datagrams += v.source_datagrams;
    a.repair_datagrams += v.repair_datagrams;
    a.duplicate_datagrams += v.duplicate_datagrams;
    a.decoded_objects += v.decoded_objects;
    a.repair_assisted_objects += v.repair_assisted_objects;
    a.fec_recovered_objects += v.fec_recovered_objects;
    a.fec_recovered_source_symbols += v.fec_recovered_source_symbols;
    a.expired_objects += v.expired_objects;
    a.conflict_drops += v.conflict_drops;
    a.authentication_drops += v.authentication_drops;
    a.deadline_drops += v.deadline_drops;
    merged
}

/// One class's tee: the unchanged receive path behind the forward.
struct Tee {
    class: TrafficClass,
    cache: Arc<LiveTsCache>,
    dispatch: RelayUdpDispatch,
    receiver: UdpFecReceiver,
    audio_block_sessions: HashMap<u32, (u64, Instant)>,
    native_audio_relay: NativeAudioRelay,
    reply_socket: Option<Arc<UdpSocket>>,
    forwarder: Option<Arc<RelayDownstreamForwarder>>,
    audio_epochs: Option<broadcast::Sender<AudioEpochDatagram>>,
    failover: Arc<FailoverInputs>,
    classes: StreamClasses,
    snapshots: Arc<StdMutex<[RelayIngressSnapshot; 2]>>,
    stats: Arc<FastPlaneStats>,
    /// The audio tee also retires idle streams, as the single ingest loop
    /// did (the cache's rotation is `fast_control`'s, on its own task).
    housekeeping: bool,
    /// The edge's fast client lane: the tee tells it which streams commit
    /// as canonical slots, and tops its clients' repair up after a decode.
    edge_clients: Option<Arc<EdgeFastLane>>,
    upstream_repair: UpstreamRepair,
    limits: RelayLimits,
    holes: Option<Arc<crate::edge_holes::HoleTrigger>>,
    reported_deadline_drops: u64,
    /// Tests: the tee panics on its first datagram.
    #[cfg(test)]
    panic_on_item: bool,
}

impl Tee {
    /// Runs until the thread's channel closes. It waits on nothing but its
    /// datagrams and the housekeeping task's nudge to expire its state
    /// (`fast_control::run_tee_housekeeping`, which also rotates the cache):
    /// no timer of its own sits beside the datagrams.
    async fn run(mut self, mut items: mpsc::Receiver<TeeItem>, expire: Arc<Notify>) -> Result<()> {
        loop {
            tokio::select! {
                item = items.recv() => {
                    let Some(item) = item else {
                        return Ok(());
                    };
                    self.process(item).await;
                    // Whatever else is queued, then one snapshot.
                    for _ in 0..RECV_BATCH {
                        let Ok(item) = items.try_recv() else {
                            break;
                        };
                        self.process(item).await;
                    }
                    self.publish();
                }
                () = expire.notified() => {
                    self.expire().await;
                }
            }
        }
    }

    fn publish(&self) {
        let snapshot = self.dispatch.receiver().snapshot();
        let mut merged = {
            let mut snapshots = self
                .snapshots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            snapshots[self.class.index()] = snapshot;
            merge_snapshots(&snapshots)
        };
        // RLS1 datagrams that the threads dropped as malformed or not
        // admitted never reach a dispatch; they count as its rejections did.
        let dropped = self.stats.invalid.load(Ordering::Relaxed)
            + self.stats.not_admitted.load(Ordering::Relaxed);
        merged.counters.datagrams_received += dropped;
        merged.counters.datagrams_rejected += dropped;
        self.cache.update_relay_runtime(
            merged,
            self.forwarder
                .as_ref()
                .map(|forwarder| forwarder.snapshot()),
        );
    }

    async fn expire(&mut self) {
        self.native_audio_relay.expire(Instant::now());
        let expired = self.receiver.expire_inactive();
        if expired.objects > 0 || expired.flows > 0 {
            debug!(
                expired_objects = expired.objects,
                expired_flows = expired.flows,
                "expired inactive UDP-FEC receive state"
            );
        }
        let relay_expired = self.dispatch.receiver_mut().expire(now_unix_us());
        if relay_expired.objects > 0 {
            debug!(
                class = self.class.as_str(),
                expired_objects = relay_expired.objects,
                "expired deadline-bound RelaySession receive state"
            );
        }
        let snapshot = self.dispatch.receiver().snapshot();
        if let Some(deadline_drops) = take_relay_deadline_drop_delta(
            &mut self.reported_deadline_drops,
            snapshot.counters.deadline_drops,
        ) {
            warn!(
                class = self.class.as_str(),
                deadline_drops,
                deadline_drops_total = self.reported_deadline_drops,
                "dropped expired RelaySession datagrams since previous ingress report"
            );
        }
        self.publish();
        if self.housekeeping {
            self.cache.flush_idle_canonical_gaps(now_unix_ms()).await;
            let retired_streams = self
                .cache
                .retire_streams_idle_before(
                    now_unix_ms()
                        .saturating_sub(CANONICAL_STREAM_IDLE_RETENTION.as_millis() as u64),
                )
                .await;
            if retired_streams > 0 {
                info!(retired_streams, "retired idle canonical stream cache state");
            }
        }
    }

    fn mark_class(&self, stream: &str, video_kind: bool) {
        if self.class == TrafficClass::Video || video_kind {
            if let Ok(stream_id) = stream.parse() {
                self.classes.mark_video(stream_id);
            }
        }
    }

    async fn process(&mut self, item: TeeItem) {
        #[cfg(test)]
        assert!(!self.panic_on_item, "test: the tee panics");
        let started = Instant::now();
        match item.kind {
            TeeKind::Admitted {
                forwarded,
                video_kind,
            } => {
                self.process_symbol(&item, forwarded, video_kind).await;
            }
            TeeKind::Unchecked => {
                self.process_unchecked(&item).await;
            }
        }
        self.cache.record_relay_processing(started.elapsed());
    }

    async fn process_unchecked(&mut self, item: &TeeItem) {
        if item.path == RelayIngressParentPath::Primary {
            if let Some(socket) = self.reply_socket.as_deref() {
                if self
                    .native_audio_relay
                    .handle_control(socket, item.peer, &item.bytes)
                    .await
                {
                    return;
                }
            }
        }
        let native = self
            .reply_socket
            .as_deref()
            .map(|socket| (socket, &self.native_audio_relay));
        if process_relay_audio_epoch_datagram(
            item.peer,
            &item.bytes,
            &mut self.audio_block_sessions,
            self.audio_epochs.as_ref(),
            self.forwarder.as_deref(),
            native,
        )
        .await
        {
            return;
        }
        // Legacy RQD2 (RLS1 never comes here: the thread admits it or drops
        // it). Nothing here forwards.
        let _ = process_udp_fec_ingest_datagram_inner(
            item.peer,
            &item.bytes,
            &self.cache,
            &mut self.receiver,
            &mut self.dispatch,
            None,
        )
        .await;
    }

    async fn process_symbol(
        &mut self,
        item: &TeeItem,
        forwarded: Option<(SocketAddr, u64)>,
        video_kind: bool,
    ) {
        let peer = item.peer;
        // The upstream repair of the stream's objects, for the fast lane's
        // top-up.
        // The symbol's header, read again in place, for the fast lane's
        // top-up (the upstream repair of the stream's objects) and the hole
        // trigger.
        let fast_view = (self.edge_clients.is_some() || self.holes.is_some())
            .then(|| RlsView::parse(&item.bytes, self.limits).ok())
            .flatten();
        if let (Some(view), Some(_)) = (&fast_view, &self.edge_clients) {
            if let Some(stream_id) = view.stream_id() {
                self.upstream_repair
                    .observe(stream_id, view.object, view.esi, view.source_symbols);
            }
        }
        let journal_forward = |key: &ObjectKey| {
            if let (Some(forwarder), Some((child, sent_us))) = (&self.forwarder, forwarded) {
                forwarder.journal_forward(key, child, sent_us);
            }
        };
        // The deadline is checked against the arrival time: a symbol that
        // waited in the tee is not dropped for it.
        match self.dispatch.push(peer, &item.bytes, item.rx_us) {
            Ok(RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Buffered { key, .. })) => {
                journal_relay_symbol(&self.cache, &key, peer, item.rx_us, None);
                journal_woken(&self.cache, &key, peer, item.woken_us);
                journal_forward(&key);
                self.mark_class(key.stream(), video_kind);
            }
            Ok(RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Decoded {
                object,
                envelope,
                parent_count,
                accepted_datagrams,
                ..
            })) => {
                journal_relay_symbol(
                    &self.cache,
                    object.key(),
                    peer,
                    item.rx_us,
                    Some(now_unix_us()),
                );
                journal_woken(&self.cache, object.key(), peer, item.woken_us);
                journal_forward(object.key());
                self.mark_class(object.key().stream(), video_kind);
                let stream = object.key().stream().to_owned();
                let sequence = object.key().object();
                let publication_clock = relay_publication_clock(&object);
                let fast_lane = self.edge_clients.clone().zip(stream.parse::<u64>().ok());
                let canonical = !av_mesh::canonical_slot::is_soundkit_v2_bundle(&object)
                    && object.kind() == media_object::ObjectKind::Media;
                let media = object.kind() == media_object::ObjectKind::Media;
                // A copy of the envelope only for a stream with clients.
                let top_up_envelope = fast_lane
                    .as_ref()
                    .filter(|(lane, stream_id)| lane.load().streams.contains_key(stream_id))
                    .map(|_| envelope.clone());
                match commit_relay_object(&self.cache, *object, envelope).await {
                    Ok(_) => {
                        if let Some((lane, stream_id)) = fast_lane {
                            if media {
                                lane.mark_committed(stream_id, canonical);
                            }
                            if let (Some(view), Some(envelope), true) =
                                (&fast_view, top_up_envelope, canonical)
                            {
                                lane.top_up(
                                    stream_id,
                                    sequence,
                                    view.fec,
                                    envelope.as_ref(),
                                    self.upstream_repair.expected(stream_id, sequence),
                                );
                            }
                        }
                        if let Some(clock) = publication_clock {
                            self.cache
                                .record_relay_availability(clock.observe(now_unix_us()));
                        }
                        self.failover.stamp(&self.failover.decoded, Instant::now());
                        debug!(
                            %peer,
                            stream,
                            sequence,
                            parent_count,
                            accepted_datagrams,
                            class = self.class.as_str(),
                            "committed canonical RelaySession RaptorQ object"
                        );
                    }
                    Err(error) => {
                        warn!(
                            %peer,
                            stream,
                            sequence,
                            error = %error,
                            "failed to cache canonical RelaySession object"
                        );
                    }
                }
            }
            Ok(RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Duplicate { key, .. })) => {
                journal_forward(&key);
            }
            Ok(RelayUdpDispatchOutcome::Legacy) | Err(RelayIngressError::DeadlineExpired) => {}
            Err(error) => {
                warn!(
                    %peer,
                    datagram_bytes = item.bytes.len(),
                    error = %error,
                    "RelaySession datagram rejected at configured dispatch seam"
                );
            }
        }
        // The fast lane's send of the symbol to its clients (udp_sent).
        if let (Some((client, sent_us)), Some(view), Some(journal)) =
            (item.client_sent, &fast_view, self.cache.hops())
        {
            if let Some(stream_id) = view.stream_id() {
                journal.record_udp_sent(stream_id, view.object, client, sent_us);
            }
        }
        // The part this symbol is of shows the holes the distance behind it,
        // once whatever it completed is committed.
        if let (Some(holes), Some(view)) = (&self.holes, &fast_view) {
            let media = !matches!(
                view.kind,
                raptorq_datagram_fec::MediaObjectKind::Initialization
                    | raptorq_datagram_fec::MediaObjectKind::CodecConfig
            );
            if let Some(stream_id) = view.stream_id().filter(|_| media) {
                holes
                    .arrived(
                        stream_id,
                        view.object,
                        crate::edge_holes::Arrival::FastPlane,
                    )
                    .await;
            }
        }
    }
}

/// The relay ingest on the fast plane. Takes the place of
/// `run_udp_fec_ingest`; `runtime.secondary_socket` is unused (the
/// secondary port's sockets are among `sockets`).
pub async fn run_relay_class_ingest(
    sockets: Vec<ClassSocket>,
    cache: Arc<LiveTsCache>,
    mut shutdown_rx: watch::Receiver<()>,
    runtime: RelayIngestRuntime,
    config: RelayClassConfig,
) -> Result<()> {
    let RelayIngestRuntime {
        dispatch,
        secondary_socket: _,
        class_twins: _,
        forwarder,
        audio_epochs,
        failover_controller,
        failover_heartbeat,
        link_telemetry,
    } = runtime;
    let stats = Arc::new(FastPlaneStats::default());
    if let Some(lease) = &config.lease {
        let _ = stats.lease.set(Arc::clone(lease));
    }
    cache.set_fast_plane_stats(Arc::clone(&stats));
    let admissions = Arc::new(dispatch.peer_admissions());
    let limits = dispatch.receiver().config().relay_limits;
    let video_dispatch = dispatch
        .twin()
        .context("failed to copy the relay dispatch")?;
    let snapshots = Arc::new(StdMutex::new([
        dispatch.receiver().snapshot(),
        video_dispatch.receiver().snapshot(),
    ]));
    cache.update_relay_ingress(dispatch.receiver().snapshot());
    let failover = Arc::new(FailoverInputs::new());
    let stop = Arc::new(AtomicBool::new(false));
    let reply_socket = match sockets.iter().find(|socket| {
        socket.class == TrafficClass::Audio && socket.path == RelayIngressParentPath::Primary
    }) {
        Some(socket) => Some(Arc::new(UdpSocket::from_std(socket.socket.try_clone()?)?)),
        None => None,
    };
    let mut by_class: [Vec<(RelayIngressParentPath, std::net::UdpSocket)>; 2] = Default::default();
    for socket in sockets {
        by_class[socket.class.index()].push((socket.path, socket.socket));
    }
    let mut dispatches = [Some(dispatch), Some(video_dispatch)];
    let mut threads = Vec::new();
    let mut tees = tokio::task::JoinSet::new();
    let mut expire = Vec::new();
    // The audio tee's node rotates the cache, as the single loop did.
    let mut audio_tee = false;
    for class in TrafficClass::ALL {
        let class_sockets = std::mem::take(&mut by_class[class.index()]);
        if class_sockets.is_empty() {
            continue;
        }
        let (tx, rx) = mpsc::channel(config.tee_capacity.max(1));
        let thread = ClassThread {
            class,
            sockets: class_sockets,
            admissions: Arc::clone(&admissions),
            limits,
            forwarder: forwarder.clone(),
            link_telemetry: link_telemetry.clone(),
            tee: tx,
            stats: Arc::clone(&stats),
            failover: Arc::clone(&failover),
            stop: Arc::clone(&stop),
            cpu: config.cpus[class.index()],
            realtime: config.realtime,
            video_gso: config.video_gso,
            lease: config.lease.clone(),
            edge_clients: config.edge_clients.clone(),
            burst_copy_ms: config.burst_copy_ms,
            loss_report: config.loss_report,
        };
        match std::thread::Builder::new()
            .name(format!("needletail-fast-{}", class.as_str()))
            .spawn(move || thread.run())
        {
            Ok(handle) => threads.push(handle),
            Err(error) => {
                // The threads already started stop before the error returns.
                stop.store(true, Ordering::Relaxed);
                join_threads(threads).await;
                tees.shutdown().await;
                return Err(error).context("failed to start a fast-plane thread");
            }
        }
        let tee = Tee {
            class,
            cache: Arc::clone(&cache),
            dispatch: dispatches[class.index()]
                .take()
                .expect("one dispatch per class"),
            receiver: UdpFecReceiver::new(),
            audio_block_sessions: HashMap::new(),
            native_audio_relay: NativeAudioRelay::default(),
            reply_socket: (class == TrafficClass::Audio)
                .then(|| reply_socket.clone())
                .flatten(),
            forwarder: forwarder.clone(),
            audio_epochs: audio_epochs.clone(),
            failover: Arc::clone(&failover),
            classes: cache.stream_classes().clone(),
            snapshots: Arc::clone(&snapshots),
            stats: Arc::clone(&stats),
            housekeeping: class == TrafficClass::Audio,
            edge_clients: config.edge_clients.clone(),
            upstream_repair: UpstreamRepair::default(),
            limits,
            holes: config.holes.clone(),
            reported_deadline_drops: 0,
            #[cfg(test)]
            panic_on_item: config.test_tee_panic == Some(class),
        };
        audio_tee |= class == TrafficClass::Audio;
        let nudge = Arc::new(Notify::new());
        expire.push(Arc::clone(&nudge));
        tees.spawn(async move { (class, tee.run(rx, nudge).await) });
    }
    let housekeeping = tokio::spawn(crate::fast_control::run_tee_housekeeping(
        Arc::clone(&cache),
        audio_tee,
        expire,
        shutdown_rx.clone(),
    ));
    let controller = failover_controller.map(|controller| {
        tokio::spawn(crate::fast_control::run_failover_controller(
            controller,
            Arc::clone(&failover),
            failover_heartbeat,
            Arc::clone(&cache),
            shutdown_rx.clone(),
        ))
    });
    info!(
        threads = threads.len(),
        port_offset = config.port_offset,
        video_gso = config.video_gso,
        children = forwarder
            .as_ref()
            .map_or(0, |forwarder| forwarder.paths.len()),
        lease_children = config.lease.is_some(),
        "RelaySession ingest runs on the fast plane"
    );

    // Until shutdown, a tee that ends is an error of the whole ingest: the
    // threads would forward on while nothing commits, so they stop, and the
    // error goes to the caller as the single loop's did.
    let mut result = tokio::select! {
        _ = shutdown_rx.changed() => Ok(()),
        Some(ended) = tees.join_next() => Err(tee_ended(ended)),
    };
    if let Err(error) = &result {
        tracing::error!(
            error = %format!("{error:#}"),
            "a fast-plane tee ended: RelaySession ingest stops"
        );
        if let Some(controller) = &controller {
            controller.abort();
        }
    }
    housekeeping.abort();
    stop.store(true, Ordering::Relaxed);
    join_threads(threads).await;
    // With the threads gone the tees' channels close and they end.
    while let Some(ended) = tees.join_next().await {
        if !matches!(ended, Ok((_, Ok(())))) && result.is_ok() {
            result = Err(tee_ended(ended));
        }
    }
    if let Some(controller) = controller {
        let _ = controller.await;
    }
    let _ = housekeeping.await;
    if result.is_ok() {
        cache.rotate_if_due(true).await?;
    }
    info!("UDP-FEC mesh byte ingest shutting down");
    result
}

async fn join_threads(threads: Vec<std::thread::JoinHandle<()>>) {
    for thread in threads {
        let _ = tokio::task::spawn_blocking(move || thread.join()).await;
    }
}

/// Why a tee ended: its error, a panic, or (before shutdown) its channel
/// closing.
fn tee_ended(ended: Result<(TrafficClass, Result<()>), tokio::task::JoinError>) -> anyhow::Error {
    match ended {
        Ok((class, Ok(()))) => anyhow::anyhow!("the fast-plane {} tee ended", class.as_str()),
        Ok((class, Err(error))) => {
            error.context(format!("the fast-plane {} tee failed", class.as_str()))
        }
        Err(error) => anyhow::anyhow!("a fast-plane tee ended: {error}"),
    }
}
