//! Relay and distributor ingest on the fast plane: per-class forward-first
//! OS threads and the tee behind them (see the module above).
//!
//! `--relay-class-threads true` runs this in place of `run_udp_fec_ingest`;
//! `false` keeps that loop, which validates and decodes every symbol before
//! it forwards it.

use super::dedup::FirstArrival;
use super::sys::{self, RecvBatch, RECV_BATCH};
use super::view::RlsView;
use super::{class_addr, StreamClasses, TrafficClass};
use crate::link_telemetry::LinkTelemetry;
use crate::{
    commit_relay_object, configure_udp_socket_buffers, journal_relay_symbol, now_unix_ms,
    now_unix_us, process_relay_audio_epoch_datagram, process_udp_fec_ingest_datagram_inner,
    relay_publication_clock, take_relay_deadline_drop_delta, AudioEpochDatagram, LiveTsCache,
    NativeAudioRelay, RelayDatagramObservation, RelayDownstreamForwarder, RelayFailoverController,
    RelayForwardRole, RelayIngestRuntime, RelayIngressParentPath, CANONICAL_STREAM_IDLE_RETENTION,
};
use anyhow::{Context, Result};
use av_mesh::relay_ingress::{
    is_relay_session_datagram, PeerAdmission, RelayIngressError, RelayIngressOutcome,
    RelayIngressSnapshot, RelayUdpDispatch, RelayUdpDispatchOutcome,
};
use av_mesh::udp_fec::UdpFecReceiver;
use bytes::Bytes;
use media_object::ObjectKey;
use raptorq_datagram_fec::MediaDatagramRole;
use relay_session::RelayLimits;
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{interval, MissedTickBehavior};
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
        }
    }
}

/// What the class threads saw last, for the failover controller: times in
/// µs since `base`, plus one (zero is never).
struct FailoverInputs {
    base: Instant,
    primary_source: AtomicU64,
    secondary_repair: AtomicU64,
    secondary_source: AtomicU64,
    decoded: AtomicU64,
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

/// The sends of one drained batch, by source socket.
pub(crate) struct ForwardPlan {
    groups: Vec<Vec<PlannedSend>>,
    retry: Vec<(usize, PlannedSend)>,
    warm: Vec<PlannedWarm>,
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
            first: [None; RECV_BATCH],
        }
    }

    fn clear(&mut self) {
        for group in &mut self.groups {
            group.clear();
        }
        self.retry.clear();
        self.warm.clear();
        self.first = [None; RECV_BATCH];
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
        for (index, path) in self.paths.iter().enumerate() {
            let promoted = path.promoted.load(Ordering::Relaxed);
            let failover_source = promoted
                && path.role == RelayForwardRole::Repair
                && matches!(role, MediaDatagramRole::Source);
            if path.role.permits(role) || failover_source {
                plan.groups[path.group].push(PlannedSend {
                    message: message as u16,
                    path: index as u16,
                    role,
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
    /// more try for what the kernel did not take. Never waits.
    fn send_fast_plan(
        &self,
        class: TrafficClass,
        batch: &RecvBatch,
        plan: &mut ForwardPlan,
        stats: &FastPlaneStats,
    ) {
        let ForwardPlan {
            groups,
            retry,
            first,
            ..
        } = plan;
        for (group, sends) in groups.iter().enumerate() {
            if sends.is_empty() {
                continue;
            }
            let fd = self.class_fd(class, group);
            let started = Instant::now();
            let sent = sys::send_batch(fd, sends.len(), |index| {
                let send = sends[index];
                (
                    batch.data(usize::from(send.message)),
                    self.class_target(class, usize::from(send.path)),
                )
            })
            .unwrap_or(0);
            let sent_us = now_unix_us();
            let elapsed = started.elapsed();
            for send in &sends[..sent] {
                let len = batch.data(usize::from(send.message)).len();
                self.record_forward_success(send.role, len, elapsed);
                first[usize::from(send.message)]
                    .get_or_insert((self.class_target(class, usize::from(send.path)), sent_us));
            }
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
}

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
        let mut kernel_dropped = vec![0_u32; self.sockets.len()];
        while !self.stop.load(Ordering::Relaxed) {
            let ready = match sys::wait_readable(&fds, STOP_POLL_MS) {
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
                    );
                    if received < RECV_BATCH {
                        break;
                    }
                }
            }
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
    ) {
        let now_us = now_unix_us();
        let now = Instant::now();
        pending.clear();
        plan.clear();
        let class = self.class;
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
            pending.push(Pending {
                admitted: Some(view.is_video_kind()),
                ..unchecked
            });
        }
        if let Some(forwarder) = &self.forwarder {
            forwarder.send_fast_plan(class, batch, plan, &self.stats);
            // Held-back sources are kept only once every child has its
            // datagrams, in this class's own buffers.
            forwarder.buffer_fast_warm(class, batch, plan, now_us);
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
                kind,
            };
            if self.tee.try_send(item).is_err() {
                self.stats.tee_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
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
    /// The audio tee also rotates the cache and retires idle streams, as
    /// the single ingest loop did.
    housekeeping: bool,
    reported_deadline_drops: u64,
    /// Tests: the tee panics on its first datagram.
    #[cfg(test)]
    panic_on_item: bool,
}

impl Tee {
    async fn run(mut self, mut items: mpsc::Receiver<TeeItem>) -> Result<()> {
        let mut rotate = interval(Duration::from_millis(10));
        rotate.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut expire = interval(Duration::from_secs(1));
        expire.set_missed_tick_behavior(MissedTickBehavior::Skip);
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
                _ = rotate.tick(), if self.housekeeping => {
                    self.cache.rotate_if_due(false).await?;
                }
                _ = expire.tick() => {
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
                journal_forward(object.key());
                self.mark_class(object.key().stream(), video_kind);
                let stream = object.key().stream().to_owned();
                let sequence = object.key().object();
                let publication_clock = relay_publication_clock(&object);
                match commit_relay_object(&self.cache, *object, envelope).await {
                    Ok(_) => {
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
    }
}

async fn run_failover_controller(
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
            reported_deadline_drops: 0,
            #[cfg(test)]
            panic_on_item: config.test_tee_panic == Some(class),
        };
        tees.spawn(async move { (class, tee.run(rx).await) });
    }
    let controller = failover_controller.map(|controller| {
        tokio::spawn(run_failover_controller(
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
        children = forwarder
            .as_ref()
            .map_or(0, |forwarder| forwarder.paths.len()),
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
