//! Relay and distributor ingest on the fast plane: per-class forward-first
//! OS threads and the tee behind them (see the module above).
//!
//! `--relay-class-threads true` runs this in place of `run_udp_fec_ingest`;
//! `false` keeps that loop, which validates and decodes every symbol before
//! it forwards it.

use super::burst_copy::BurstCopies;
use super::dedup::{self, Arrival, FirstArrival};
use super::demand::{NodeDemand, DEMAND_REPORT_INTERVAL};
use super::edge_clients::{ClientPlan, ClientSender, EdgeFastLane, UpstreamRepair};
use super::lease::{LeaseChildren, LeaseSnapshot, LeaseSnapshotStats, LeaseTake};
use super::loss_report::{decode_ledger, LossMeter};
use super::media_clock::{MediaClocks, ValidatedEpochs, Verdict};
use super::parent_feeds::{FeedMisses, ParentFeeds};
use super::decode_pool::{DecodeDone, DecodePool, DecodePoolSnapshot, DecodedCtx};
use super::part_repair::{
    Answer, Children, PartKey, PartMeta, PartRepair, PartRepairSnapshot, Requester, SendSocket,
    StoredPart,
};
use super::sys::{self, RecvBatch, RECV_BATCH};
use super::tee_ring::{self, tee_ring, tee_ring_growing, Ready, TeeReceiver, TeeSender};
use super::view::RlsView;
use super::{class_addr, StreamClasses, TrafficClass};
use crate::link_telemetry::LinkTelemetry;
use crate::{
    commit_relay_object, configure_udp_socket_buffers, journal_relay_decoded, journal_relay_symbol,
    now_unix_ms, now_unix_us, process_relay_audio_epoch_datagram,
    process_udp_fec_ingest_datagram_inner, relay_publication_clock, warn_relay_rejection,
    AudioEpochDatagram, Committed, LiveTsCache, NativeAudioRelay, RelayDownstreamForwarder,
    RelayForwardRole, RelayIngestRuntime, RelayIngressParentPath, CANONICAL_STREAM_IDLE_RETENTION,
};
use anyhow::{Context, Result};
use av_mesh::relay_ingress::{
    is_relay_datagram, ParsedOutcome, PeerAdmission, RelayIngressOutcome, RelayIngressSnapshot,
    RelayUdpDispatch, RelayUdpDispatchOutcome,
};
use av_mesh::stall_journal::{self, StallKind, StallThread};
use av_mesh::udp_fec::UdpFecReceiver;
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
use tokio::sync::{broadcast, watch, Notify};
use tracing::{debug, info, warn};

/// Datagrams a class's tee holds before the thread drops its tee copy (its
/// ring also holds `tee_ring::BYTES_PER_ENTRY` bytes for each: 16 MB at
/// this, pinned at start).
pub const DEFAULT_TEE_CAPACITY: usize = 65_536;
/// Bytes of the video class's tee ring (`--relay-video-tee-mb`): 96 MiB,
/// as many bytes as its [`DEFAULT_TEE_CAPACITY`] entries take of 4K
/// symbols (1,316 bytes and RLS1's header: 65,536 x about 1,390 = 91 MB).
/// That is 2.56 s at 247 Mbit/s of media (25,600 datagrams a second, the
/// entries bind first; the bytes would last 2.8 s) and 2.1 s at 300 Mbit/s
/// of 50 frames a second (31,150 a second). fix4-video measured the tee at
/// 2.7 to 3.3 times those rates on one CPU, and its longest single stall
/// in the local gate at 0.9 s: the ring covers that twice, and a longer lag
/// loses whole parts (`TeeParts`). It was 192 MiB (rm/4k): its entries ran
/// out at about 89 MB, but the ring writes its bytes in turn, so all 192
/// MiB became resident and locked for nothing. The ring of 256 B an entry
/// (16 MB, 12,000 symbols, half a second at 247) dropped copies in every
/// lab run. Only the first 16 MB is faulted in and locked at start; the
/// rest when a video datagram first comes (`tee_ring::tee_ring_growing`).
pub const DEFAULT_VIDEO_TEE_BYTES: usize = 96 << 20;
/// `--relay-video-socket-buffer-ms`: a video socket holds two seconds of
/// its stream's busiest second (`VideoSocketBuffers`).
pub const DEFAULT_VIDEO_SOCKET_BUFFER_MS: u64 = 2_000;
/// `--relay-video-socket-buffer-max-mb`: 128 MiB asked (the kernel books
/// twice that in buffer memory: about 2.7 s at 300 Mbit/s of 4K).
pub const DEFAULT_VIDEO_SOCKET_BUFFER_MAX_BYTES: usize = 128 << 20;
/// Symbols the first-arrival table remembers per class.
const DEDUP_CAPACITY: usize = 65_536;
/// How often an idle class thread looks at the stop flag.
const STOP_POLL_MS: i32 = 50;
/// Media clocks per class thread: streams beyond it are never stale.
const MEDIA_CLOCKS: usize = 1_024;
/// Streams whose validated epoch the class threads follow.
const VALIDATED_EPOCH_STREAMS: usize = 4_096;
/// `--relay-media-deadline-ms` by default, in µs.
pub const DEFAULT_MEDIA_DEADLINE_US: u64 = 1_000_000;
/// Receive batches a class thread takes from one socket before it looks at
/// the next (A 2.7): one parent's burst cannot hold another's live parts
/// back by more than this many batches.
const SOCKET_TURN_BATCHES: usize = 4;
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
    /// Bytes of the video class's tee ring.
    pub video_tee_bytes: usize,
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
    /// `--relay-demand-report` (wave 7 item 5): the audio thread sends each
    /// relay parent this node's demand (`RLD1`, see `demand`).
    pub demand: Option<Arc<NodeDemand>>,
    /// `--relay-ranked-parents` (wave 8 stage 5, AM 12): the distributors
    /// of the region that this lease child admits beside its two configured
    /// parents. They are lease parents, which read no demand report: none
    /// is sent to them.
    pub ranked_parents: Vec<SocketAddr>,
    /// `--relay-lease-descriptors` (wave 9, cold start): the audio thread
    /// sends a lease child that is new to a stream the stream's last whole
    /// one-symbol part (RLS1, which carries the descriptor of the compact
    /// RLC1 parts) before its first compact part (see `LeaseDescriptors`).
    pub lease_descriptors: bool,
    /// `--relay-video-gro` (wave 9 D): the video sockets take UDP GRO, so a
    /// parent's GSO burst (or what the NIC joined) is one receive message cut
    /// apart in the thread, not one kernel datagram each (see
    /// `sys::enable_gro`).
    pub video_gro: bool,
    /// `--relay-media-deadline-ms` in µs (A 2.5): a datagram whose deadline
    /// is this far behind the head of its stream's timeline here is stale.
    pub media_deadline_us: u64,
    /// The receive buffer the class sockets were made with
    /// (`--udp-socket-buffer-bytes`): the least a video socket keeps.
    pub socket_buffer_bytes: usize,
    /// `--relay-video-socket-buffer-ms` (fix4-video): a video socket's
    /// receive buffer grows to hold this long of the most its stream
    /// brought in a second, up to `video_socket_buffer_max_bytes`; 0 leaves
    /// it as made.
    pub video_socket_buffer_ms: u64,
    pub video_socket_buffer_max_bytes: usize,
    /// 4k-repair (`--relay-part-repair`, `--relay-link-repair`): asks for
    /// what a video part lacks, answers to children, link repair.
    pub part_repair: Option<Arc<PartRepair>>,
    /// `--relay-decode-workers` (4k-repair): the pool that decodes damaged
    /// parts off the tees (`decode_pool`); `None` decodes on the tees.
    pub decode_pool: Option<Arc<DecodePool>>,
    /// `--relay-trim-to-margin` (4k-repair): with regenerated sources, the
    /// trim forwards source-taking children only its margin of upstream
    /// repair (the children wait for this node's decode and need none of
    /// their own); off, it forwards what the part lacks here too (each node
    /// can decode as soon as it holds K, in parallel along the path).
    pub trim_to_margin: bool,
    /// `--relay-trim-forwarded-repair` (4k-repair): a class thread forwards
    /// the upstream repair of a part of at least `TRIM_MIN_SYMBOLS` to the
    /// children that take this node's sources only up to what the part
    /// lacks here and a margin (`RepairTrim`); repair-only children take
    /// all of it.
    pub trim_forwarded_repair: bool,
    /// `--reader-slice-us` (hls4k): the small-part lane's tee asks the
    /// kernel for the same slice as the LL-HLS readers' threads, so that at
    /// its nice 0 it keeps its precedence over them (nice 5) when a part
    /// wakes both: with the kernel's slice the readers' 200 µs gave them the
    /// earlier deadline. 0 keeps the kernel's. The large-part lane's tee
    /// keeps the kernel's slice at nice 10.
    pub small_lane_tee_slice_us: u64,
    /// Tests: the tee of this class panics on its first datagram.
    #[cfg(test)]
    pub test_tee_panic: Option<TrafficClass>,
    /// Tests: both tees on the caller's runtime, as before each had a
    /// thread (the baseline of the isolation test).
    #[cfg(test)]
    pub test_tees_on_caller_runtime: bool,
    /// Tests: the video tee spends this much more processor time on each
    /// datagram (a tee short of processor time).
    #[cfg(test)]
    pub test_video_tee_spin_ns: u64,
    /// Tests: the audio thread counts its allocations and frees here once
    /// it runs (`alloc_count`).
    #[cfg(test)]
    pub test_alloc_counter: Option<&'static crate::alloc_count::AllocCounter>,
    /// Tests: the same for the video thread.
    #[cfg(test)]
    pub test_video_alloc_counter: Option<&'static crate::alloc_count::AllocCounter>,
}

impl Default for RelayClassConfig {
    fn default() -> Self {
        Self {
            port_offset: 1_000,
            cpus: [None, None],
            realtime: true,
            tee_capacity: DEFAULT_TEE_CAPACITY,
            video_tee_bytes: DEFAULT_VIDEO_TEE_BYTES,
            video_gso: true,
            lease: None,
            edge_clients: None,
            holes: None,
            burst_copy_ms: 0,
            loss_report: false,
            demand: None,
            ranked_parents: Vec::new(),
            lease_descriptors: true,
            video_gro: true,
            media_deadline_us: DEFAULT_MEDIA_DEADLINE_US,
            socket_buffer_bytes: 8 << 20,
            video_socket_buffer_ms: DEFAULT_VIDEO_SOCKET_BUFFER_MS,
            video_socket_buffer_max_bytes: DEFAULT_VIDEO_SOCKET_BUFFER_MAX_BYTES,
            part_repair: None,
            decode_pool: None,
            trim_to_margin: false,
            trim_forwarded_repair: true,
            small_lane_tee_slice_us: 0,
            #[cfg(test)]
            test_tee_panic: None,
            #[cfg(test)]
            test_tees_on_caller_runtime: false,
            #[cfg(test)]
            test_video_tee_spin_ns: 0,
            #[cfg(test)]
            test_alloc_counter: None,
            #[cfg(test)]
            test_video_alloc_counter: None,
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
    /// Receive batches (one `recvmmsg` that returned datagrams) per class:
    /// received / batches is what a wake-up reads, and what a send can join
    /// (UDP GSO joins a child's symbols of one batch only).
    pub batches: [AtomicU64; 2],
    pub forwarded: [AtomicU64; 2],
    pub duplicates: AtomicU64,
    pub not_admitted: AtomicU64,
    pub invalid: AtomicU64,
    pub tee_dropped: AtomicU64,
    /// Datagrams each class's tee dropped as past their deadline, as its
    /// dispatch counts them.
    pub deadline_drops: [AtomicU64; 2],
    pub would_block: AtomicU64,
    pub send_errors: AtomicU64,
    pub truncated: AtomicU64,
    /// Datagrams the kernel dropped on a full receive queue (SO_RXQ_OVFL).
    pub kernel_dropped: AtomicU64,
    /// The same by socket: [class][path] (audio, video; primary, secondary),
    /// so that a full queue names its lane (wave 8: the referee's 4K losses
    /// were at one socket of each node, the secondary's video).
    pub kernel_dropped_lanes: [[AtomicU64; 2]; 2],
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
    /// the audio thread measures on a parent lane now (only lanes whose
    /// parent sends ledgers, RLL1, are measured: the contributor's).
    pub loss_reports: AtomicU64,
    pub parent_loss_ppm: AtomicU64,
    /// Demand reports (RLD1) sent to parents.
    pub demand_reports: AtomicU64,
    /// The lease children, whose counters the snapshot reports.
    pub lease: OnceLock<Arc<LeaseChildren>>,
    /// Loop lag per class (wave 9, spec8 F-I2): the longest pass of a
    /// class thread's loop (from a wait's return to the next wait), the
    /// passes longer than 1, 5 and 50 ms, and the longest gap between two
    /// returns of its wait that both brought datagrams with no idle timeout
    /// between them (a timeout is STOP_POLL_MS, 50 ms). A pass is the
    /// thread's own work (sends, a lock, a page fault); a gap past it is
    /// time the thread did not run (preemption, a stalled host).
    pub loop_busy_max_us: [AtomicU64; 2],
    pub loop_busy_over: [[AtomicU64; 3]; 2],
    pub loop_gap_max_us: [AtomicU64; 2],
    /// Page faults (minor, major) of each class thread so far, read by the
    /// thread itself about once a second (wave 9: a first session's faults
    /// against a later one's).
    pub thread_faults: [[AtomicU64; 2]; 2],
    /// Whole audio parts sent to lease children new to their stream, for
    /// the descriptor of the compact parts (wave 9, `LeaseDescriptors`).
    pub lease_descriptors: AtomicU64,
    /// Video sockets with UDP GRO on, receive messages that carried more
    /// than one datagram and the datagrams they carried (wave 9 D).
    pub gro_sockets: AtomicU64,
    pub gro_messages: AtomicU64,
    pub gro_datagrams: AtomicU64,
    /// The audio class's parent lanes (A 2.7), by [`lane_index`]: what each
    /// socket received, and how its copies fared in the dedup.
    pub parent_lanes: [ParentLaneStats; 2],
    /// Admitted datagrams that the media clocks held stale (A 2.5): of an
    /// epoch below their stream's validated one, or a budget behind their
    /// stream's head. Neither forwarded nor teed.
    pub stale_epoch: AtomicU64,
    pub stale_media: AtomicU64,
    /// Datagrams of streams that found no media clock, per class.
    pub media_clock_overflow: [AtomicU64; 2],
    /// Each class's tee (fix4-video): the wakes that took datagrams, its
    /// processor time over them (µs, its thread's clock), and the longest
    /// it took a datagram after the class thread read it (µs).
    pub tee_batches: [AtomicU64; 2],
    pub tee_cpu_us: [AtomicU64; 2],
    pub tee_lag_max_us: [AtomicU64; 2],
    /// Video parts the video thread did not tee because the ring had no
    /// room for the whole part when its first symbol came (`TeeParts`);
    /// their symbols count in `tee_dropped`.
    pub tee_parts_shed: AtomicU64,
    /// Tee copies refused while the class's ring was not yet at its full
    /// size (they count in `tee_dropped` too), and the bytes of each
    /// class's ring in use.
    pub tee_dropped_growing: AtomicU64,
    pub tee_ring_bytes: [AtomicU64; 2],
    /// Bytes the video sockets received, by `lane_index` (primary,
    /// secondary), and their receive buffers as the kernel reports them
    /// (twice what was asked: its booking of buffer memory).
    pub video_rx_bytes: [AtomicU64; 2],
    pub video_socket_buffer_bytes: [AtomicU64; 2],
    /// Validated epochs that found no slot in the class threads' table
    /// (`ValidatedEpochs::unrecorded`): their streams are never stale.
    pub media_clock_unrecorded: AtomicU64,
    /// The loss (ppm) the video thread measures on each parent lane, by
    /// `lane_index`, against its parent's ledgers (4k-repair: the
    /// contributor's video lanes, and a forwarder's to its children).
    pub video_parent_loss_ppm: [AtomicU64; 2],
    /// Upstream repair symbols not forwarded to source-taking
    /// children: beyond what the part lacked here (`RepairTrim`).
    pub repair_trimmed: AtomicU64,
    /// The node's part repair (4k-repair), whose counters the snapshot
    /// reports.
    pub part_repair: OnceLock<Arc<PartRepair>>,
    pub decode_pool: OnceLock<Arc<DecodePool>>,
}

/// The counters of one parent lane (one receive socket) of the audio class.
#[derive(Debug, Default)]
pub struct ParentLaneStats {
    pub received: AtomicU64,
    pub not_admitted: AtomicU64,
    /// Symbols whose first copy came on this lane.
    pub first_arrivals: AtomicU64,
    /// Further copies that came on this lane, and those whose bytes were
    /// not the first copy's.
    pub duplicates: AtomicU64,
    pub duplicate_mismatch: AtomicU64,
    /// The largest lag of a duplicate behind its first copy in the second
    /// under way, and in the last whole second.
    lag_now_us: AtomicU64,
    lag_last_us: AtomicU64,
    /// When the lane's parent last sent its ledger (RLL1), in Unix µs (zero:
    /// never), and the loss its meter measures.
    ledger_us: AtomicU64,
    pub(crate) loss_ppm: AtomicU64,
    /// The parent whose ledgers arrive on the lane: written when a ledger
    /// arrives (a few times a second), never per datagram.
    peer: StdMutex<Option<SocketAddr>>,
}

impl ParentLaneStats {
    /// The lane's parent sent its ledger (a few times a second). Its address
    /// is kept without waiting on a snapshot that holds the lock.
    fn ledger(&self, peer: SocketAddr, now_us: u64) {
        self.ledger_us.store(now_us, Ordering::Relaxed);
        if let Ok(mut kept) = self.peer.try_lock() {
            *kept = Some(peer);
        }
    }
}

/// The index of a parent lane: primary 0, secondary 1.
const fn lane_index(path: RelayIngressParentPath) -> usize {
    match path {
        RelayIngressParentPath::Primary => 0,
        RelayIngressParentPath::Secondary => 1,
    }
}

/// A lane's name in the snapshot and in log lines.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParentLane {
    #[default]
    Primary,
    Secondary,
}

impl ParentLane {
    const fn of(index: usize) -> Self {
        if index == 0 {
            Self::Primary
        } else {
            Self::Secondary
        }
    }
}

/// What `/api/mesh/local` reports of a parent lane of the audio class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ParentLaneSnapshot {
    pub lane: ParentLane,
    /// The parent whose ledgers arrive on the lane (a contributor), if any.
    pub peer: Option<SocketAddr>,
    pub received: u64,
    pub first_arrivals: u64,
    pub duplicates: u64,
    pub duplicate_mismatch: u64,
    /// The largest lag of a duplicate in the last whole second or the one
    /// under way.
    pub duplicate_lag_max_us: u64,
    pub not_admitted: u64,
    pub kernel_dropped: u64,
    pub ledger_age_ms: Option<u64>,
    /// `None` once the last ledger is older than [`LEDGER_STALE_MS`]: the
    /// meter keeps its last estimate until a new parent's ledger.
    pub loss_ppm: Option<u64>,
}

/// A lane's loss is not reported once its last ledger is this old.
pub const LEDGER_STALE_MS: u64 = 3_000;

/// What the media clocks of the class threads held stale.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(default)]
pub struct MediaClockSnapshot {
    pub stale_epoch: u64,
    pub stale_media: u64,
    pub overflow: u64,
    /// Epochs the cache took up for a stream that the class threads' table
    /// of validated epochs had no slot for.
    pub unrecorded_streams: u64,
}

/// The thresholds of `FastPlaneStats::loop_busy_over`, in µs.
pub const LOOP_BUSY_OVER_US: [u64; 3] = [1_000, 5_000, 50_000];

impl FastPlaneStats {
    /// Book one pass of a class thread's loop (see `loop_busy_max_us`).
    fn record_loop(&self, class: TrafficClass, busy_us: u64, gap_us: Option<u64>) {
        let index = class.index();
        self.loop_busy_max_us[index].fetch_max(busy_us, Ordering::Relaxed);
        for (threshold, counter) in LOOP_BUSY_OVER_US.iter().zip(&self.loop_busy_over[index]) {
            if busy_us > *threshold {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Some(gap_us) = gap_us {
            self.loop_gap_max_us[index].fetch_max(gap_us, Ordering::Relaxed);
        }
    }

    /// The loss (ppm) measured on the parent lane `lane` (`lane_index`) of
    /// `class`, against its parent's ledgers (4k-repair).
    pub(crate) fn lane_loss_ppm(&self, class: TrafficClass, lane: usize) -> u64 {
        match class {
            TrafficClass::Audio => self.parent_lanes[lane].loss_ppm.load(Ordering::Relaxed),
            TrafficClass::Video => self.video_parent_loss_ppm[lane].load(Ordering::Relaxed),
        }
    }

    /// The counters of the parent lane of `path`: the audio class's only.
    fn lane(&self, class: TrafficClass, path: RelayIngressParentPath) -> Option<&ParentLaneStats> {
        (class == TrafficClass::Audio).then(|| &self.parent_lanes[lane_index(path)])
    }

    /// Book a copy of a symbol already seen, which the thread drops, on the
    /// lane it came on. A copy whose bytes differ from the first one's is a
    /// fault of the senders: logged once per power of two a lane.
    fn record_duplicate(
        &self,
        class: TrafficClass,
        path: RelayIngressParentPath,
        arrival: Arrival,
        view: &RlsView<'_>,
    ) {
        self.duplicates.fetch_add(1, Ordering::Relaxed);
        let (
            Some(lane),
            Arrival::Duplicate {
                equal,
                lag_us,
                first_lane,
            },
        ) = (self.lane(class, path), arrival)
        else {
            return;
        };
        lane.duplicates.fetch_add(1, Ordering::Relaxed);
        lane.lag_now_us
            .fetch_max(u64::from(lag_us), Ordering::Relaxed);
        if equal {
            return;
        }
        let mismatches = lane.duplicate_mismatch.fetch_add(1, Ordering::Relaxed) + 1;
        if mismatches.is_power_of_two() {
            warn!(
                stream = view.stream,
                object = view.object,
                sequence = view.packet_sequence,
                lane = ?ParentLane::of(lane_index(path)),
                first_lane = ?ParentLane::of(usize::from(first_lane)),
                mismatches,
                "a copy of a symbol differs from its first copy: two senders put other bytes under one key"
            );
        }
    }

    /// A second has passed: its largest lags become the last whole second's.
    fn roll_duplicate_lag(&self) {
        for lane in &self.parent_lanes {
            let lag = lane.lag_now_us.swap(0, Ordering::Relaxed);
            lane.lag_last_us.store(lag, Ordering::Relaxed);
        }
    }

    fn parent_lane_snapshot(&self, index: usize, now_us: u64) -> ParentLaneSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let lane = &self.parent_lanes[index];
        let ledger_age_ms = match load(&lane.ledger_us) {
            0 => None,
            at => Some(now_us.saturating_sub(at) / 1_000),
        };
        ParentLaneSnapshot {
            lane: ParentLane::of(index),
            peer: *lane
                .peer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            received: load(&lane.received),
            first_arrivals: load(&lane.first_arrivals),
            duplicates: load(&lane.duplicates),
            duplicate_mismatch: load(&lane.duplicate_mismatch),
            duplicate_lag_max_us: load(&lane.lag_now_us).max(load(&lane.lag_last_us)),
            not_admitted: load(&lane.not_admitted),
            kernel_dropped: load(&self.kernel_dropped_lanes[TrafficClass::Audio.index()][index]),
            ledger_age_ms,
            loss_ppm: ledger_age_ms
                .filter(|age| *age < LEDGER_STALE_MS)
                .map(|_| load(&lane.loss_ppm)),
        }
    }
}

/// The fast plane's lines of the stall journal (D1). Each is given a
/// duration that its caller has measured anyway; under the kind's threshold
/// it is that comparison and nothing else, and at the threshold an event
/// (`stall_journal::note`). A class thread's events carry its page faults
/// since its once-a-second reading (`FastPlaneStats::thread_faults`).
#[inline]
fn stall_line(stats: &FastPlaneStats, kind: StallKind, class: TrafficClass, dur_us: u64) {
    let thread = match class {
        TrafficClass::Audio => StallThread::Audio,
        TrafficClass::Video => StallThread::Video,
    };
    let faults = &stats.thread_faults[class.index()];
    stall_journal::note(kind, thread, dur_us, Some(faults));
}

/// `class_pass`: a pass of a class thread's loop.
#[inline]
fn stall_pass(stats: &FastPlaneStats, class: TrafficClass, busy_us: u64) {
    stall_line(stats, StallKind::ClassPass, class, busy_us);
}

/// `class_gap`: the time between two data-bearing wakes.
#[inline]
fn stall_gap(stats: &FastPlaneStats, class: TrafficClass, gap_us: Option<u64>) {
    stall_line(stats, StallKind::ClassGap, class, gap_us.unwrap_or(0));
}

/// `rx_late`: how long the oldest datagram of a batch (its first) waited
/// between its kernel stamp and the drain.
#[inline]
fn stall_rx(stats: &FastPlaneStats, class: TrafficClass, message: usize, waited_us: u64) {
    if message == 0 {
        stall_line(stats, StallKind::RxLate, class, waited_us);
    }
}

/// `send_slow`: one `sendmmsg` of a class thread.
#[inline]
fn stall_send(stats: &FastPlaneStats, class: TrafficClass, took_us: u64) {
    stall_line(stats, StallKind::SendSlow, class, took_us);
}

/// `tee_lag`: how long after its batch's wake an item reached the tee.
#[inline]
fn stall_tee(class: TrafficClass, lag_us: u64) {
    let thread = match class {
        TrafficClass::Audio => StallThread::TeeAudio,
        TrafficClass::Video => StallThread::TeeVideo,
    };
    stall_journal::note(StallKind::TeeLag, thread, lag_us, None);
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
    pub audio_batches: u64,
    pub video_batches: u64,
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
    pub audio_primary_kernel_dropped: u64,
    pub audio_secondary_kernel_dropped: u64,
    pub video_primary_kernel_dropped: u64,
    pub video_secondary_kernel_dropped: u64,
    pub gso_datagrams: u64,
    pub gso_messages: u64,
    pub gso_fallbacks: u64,
    pub burst_copies: u64,
    pub burst_copies_dropped: u64,
    pub loss_reports: u64,
    pub parent_loss_ppm: u64,
    pub demand_reports: u64,
    pub lease_children: LeaseSnapshotStats,
    /// Loop lag (wave 9): see `FastPlaneStats::loop_busy_max_us`.
    pub audio_loop_busy_max_us: u64,
    pub audio_loop_busy_over_1ms: u64,
    pub audio_loop_busy_over_5ms: u64,
    pub audio_loop_busy_over_50ms: u64,
    pub audio_loop_gap_max_us: u64,
    pub video_loop_busy_max_us: u64,
    pub video_loop_busy_over_1ms: u64,
    pub video_loop_busy_over_5ms: u64,
    pub video_loop_busy_over_50ms: u64,
    pub video_loop_gap_max_us: u64,
    /// Page faults (minor, major) of the class threads and of the process,
    /// and whether its memory is locked and its allocator tuned (wave 9).
    pub audio_thread_minflt: u64,
    pub audio_thread_majflt: u64,
    pub video_thread_minflt: u64,
    pub video_thread_majflt: u64,
    pub process_minflt: u64,
    pub process_majflt: u64,
    pub memory_locked: bool,
    pub allocator_tuned: bool,
    pub lease_descriptors: u64,
    pub gro_sockets: u64,
    pub gro_messages: u64,
    pub gro_datagrams: u64,
    /// The audio class's parent lanes (A 2.7).
    pub parent_lanes: [ParentLaneSnapshot; 2],
    pub media_clock: MediaClockSnapshot,
    /// The tees (fix4-video): processor time (µs), the video tee's wakes,
    /// its longest lag behind its thread (µs), and the video parts not
    /// teed for want of room in the ring.
    pub audio_tee_cpu_us: u64,
    pub video_tee_cpu_us: u64,
    pub video_tee_batches: u64,
    pub video_tee_lag_max_us: u64,
    /// The small-part lane's tee: its longest lag (µs).
    pub audio_tee_lag_max_us: u64,
    pub video_tee_parts_shed: u64,
    /// Tee copies refused while a ring was growing (in `tee_dropped` too),
    /// and the bytes of the video tee ring in use.
    pub tee_dropped_growing: u64,
    pub video_tee_ring_bytes: u64,
    /// The video sockets' receive buffers as the kernel reports them, the
    /// larger of the two (fix4-video: sized to two seconds of the stream).
    pub video_socket_buffer_bytes: u64,
    /// The loss the video thread measures on each parent lane (ppm).
    pub video_parent_loss_ppm: [u64; 2],
    pub repair_trimmed: u64,
    /// Part repair (4k-repair): asks, answers, link repair.
    pub part_repair: PartRepairSnapshot,
    /// The decode pool (4k-repair).
    pub decode_pool: DecodePoolSnapshot,
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
            audio_batches: load(&self.batches[0]),
            video_batches: load(&self.batches[1]),
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
            audio_primary_kernel_dropped: load(&self.kernel_dropped_lanes[0][0]),
            audio_secondary_kernel_dropped: load(&self.kernel_dropped_lanes[0][1]),
            video_primary_kernel_dropped: load(&self.kernel_dropped_lanes[1][0]),
            video_secondary_kernel_dropped: load(&self.kernel_dropped_lanes[1][1]),
            gso_datagrams: load(&self.gso_datagrams),
            gso_messages: load(&self.gso_messages),
            gso_fallbacks: load(&self.gso_fallbacks),
            burst_copies: load(&self.burst_copies),
            burst_copies_dropped: load(&self.burst_copies_dropped),
            loss_reports: load(&self.loss_reports),
            parent_loss_ppm: load(&self.parent_loss_ppm),
            demand_reports: load(&self.demand_reports),
            lease_children: self
                .lease
                .get()
                .map(|lease| lease.stats.snapshot())
                .unwrap_or_default(),
            audio_loop_busy_max_us: load(&self.loop_busy_max_us[0]),
            audio_loop_busy_over_1ms: load(&self.loop_busy_over[0][0]),
            audio_loop_busy_over_5ms: load(&self.loop_busy_over[0][1]),
            audio_loop_busy_over_50ms: load(&self.loop_busy_over[0][2]),
            audio_loop_gap_max_us: load(&self.loop_gap_max_us[0]),
            video_loop_busy_max_us: load(&self.loop_busy_max_us[1]),
            video_loop_busy_over_1ms: load(&self.loop_busy_over[1][0]),
            video_loop_busy_over_5ms: load(&self.loop_busy_over[1][1]),
            video_loop_busy_over_50ms: load(&self.loop_busy_over[1][2]),
            video_loop_gap_max_us: load(&self.loop_gap_max_us[1]),
            audio_thread_minflt: load(&self.thread_faults[0][0]),
            audio_thread_majflt: load(&self.thread_faults[0][1]),
            video_thread_minflt: load(&self.thread_faults[1][0]),
            video_thread_majflt: load(&self.thread_faults[1][1]),
            process_minflt: crate::memory::faults(false).0,
            process_majflt: crate::memory::faults(false).1,
            memory_locked: crate::memory::locked(),
            allocator_tuned: crate::memory::tuned(),
            lease_descriptors: load(&self.lease_descriptors),
            gro_sockets: load(&self.gro_sockets),
            gro_messages: load(&self.gro_messages),
            gro_datagrams: load(&self.gro_datagrams),
            parent_lanes: {
                let now_us = now_unix_us();
                [0, 1].map(|index| self.parent_lane_snapshot(index, now_us))
            },
            media_clock: MediaClockSnapshot {
                stale_epoch: load(&self.stale_epoch),
                stale_media: load(&self.stale_media),
                overflow: self.media_clock_overflow.iter().map(load).sum(),
                unrecorded_streams: load(&self.media_clock_unrecorded),
            },
            audio_tee_cpu_us: load(&self.tee_cpu_us[0]),
            video_tee_cpu_us: load(&self.tee_cpu_us[1]),
            video_tee_batches: load(&self.tee_batches[1]),
            video_tee_lag_max_us: load(&self.tee_lag_max_us[1]),
            audio_tee_lag_max_us: load(&self.tee_lag_max_us[0]),
            video_tee_parts_shed: load(&self.tee_parts_shed),
            tee_dropped_growing: load(&self.tee_dropped_growing),
            video_tee_ring_bytes: load(&self.tee_ring_bytes[1]),
            video_socket_buffer_bytes: self
                .video_socket_buffer_bytes
                .iter()
                .map(load)
                .max()
                .unwrap_or(0),
            video_parent_loss_ppm: [
                load(&self.video_parent_loss_ppm[0]),
                load(&self.video_parent_loss_ppm[1]),
            ],
            repair_trimmed: load(&self.repair_trimmed),
            part_repair: self
                .part_repair
                .get()
                .map(|repair| repair.stats.snapshot())
                .unwrap_or_default(),
            decode_pool: self
                .decode_pool
                .get()
                .map(|pool| pool.snapshot())
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

/// What a class thread hands its tee with a datagram (whose bytes go in
/// the tee ring beside it).
#[derive(Clone, Copy)]
struct TeeMeta {
    peer: SocketAddr,
    path: RelayIngressParentPath,
    /// Arrival time: the kernel's when the socket reports it.
    rx_us: u64,
    /// When the thread read it off the socket (its batch's wake-up).
    woken_us: u64,
    /// The edge's fast client lane handed the symbol to the kernel for this
    /// client (the first of its send) at this time.
    client_sent: Option<(SocketAddr, u64)>,
    kind: TeeKind,
}

#[derive(Clone, Copy)]
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
    /// The UDP GSO runs of a send, worked out in place (fix4-video: a
    /// list made per send was the video thread's allocation per batch).
    runs: Vec<(usize, usize)>,
}

impl ForwardPlan {
    /// A plan with room for a batch to every child: `paths` compiled
    /// children (in `groups` socket groups, `warm_paths` of them repair
    /// only) and `lease_children` lease children, so that no list grows on
    /// the class thread (stall follow-up).
    fn new(groups: usize, paths: usize, warm_paths: usize, lease_children: usize) -> Self {
        Self {
            groups: (0..groups)
                .map(|_| Vec::with_capacity(RECV_BATCH * paths.max(1)))
                .collect(),
            retry: Vec::with_capacity(RECV_BATCH * paths.max(1)),
            warm: Vec::with_capacity(RECV_BATCH * warm_paths),
            lease: Vec::with_capacity(RECV_BATCH * lease_children),
            lease_warm: Vec::with_capacity(RECV_BATCH * lease_children),
            first: [None; RECV_BATCH],
            // A run per send at most: the largest list of sends.
            runs: Vec::with_capacity(RECV_BATCH * paths.max(lease_children).max(1)),
        }
    }

    /// Fault in every list's room, on the thread that makes the plan: a
    /// reserved list's pages are untouched until a batch first fills it,
    /// which would be on the real-time thread (D3 review 3).
    fn touch(&mut self) {
        for group in &mut self.groups {
            crate::memory::touch_spare(group);
        }
        crate::memory::touch_spare(&mut self.retry);
        crate::memory::touch_spare(&mut self.warm);
        crate::memory::touch_spare(&mut self.lease);
        crate::memory::touch_spare(&mut self.lease_warm);
        crate::memory::touch_spare(&mut self.runs);
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
    now_us: u64,
    trim: bool,
) {
    let mut object = None;
    let Some((children, fed_us)) = snapshot.children_and_clock_of(view.stream) else {
        return;
    };
    if !children.is_empty() {
        // The cache mesh's push filter reads it (wave 7 item 4).
        fed_us.store(now_us, Ordering::Relaxed);
    }
    for &child in children {
        let entry = &snapshot.children[usize::from(child)];
        if trim && (entry.primary || entry.is_promoted()) {
            continue;
        }
        match entry.takes(class, view.role) {
            LeaseTake::Send => plan.lease.push(PlannedLease {
                message: message as u16,
                child,
            }),
            LeaseTake::Warm => plan.lease_warm.push(PlannedWarm {
                message: message as u16,
                path: child,
                object: *object.get_or_insert_with(|| view_object_id(view)),
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
        runs,
        ..
    } = plan;
    if !sends.is_empty() {
        let fd = lease.fd(class);
        let target =
            |send: &PlannedLease| snapshot.children[usize::from(send.child)].targets[class.index()];
        let use_gso = *gso && class == TrafficClass::Video;
        if use_gso {
            // A child's symbols in a row make one GSO message; the order of
            // a child's own symbols is kept. In place: a stable sort takes
            // a buffer.
            sends.sort_unstable_by_key(|send| (send.child, send.message));
        }
        let message = |index: usize| {
            let send = &sends[index];
            (batch.data(usize::from(send.message)), target(send))
        };
        let started = Instant::now();
        let mut sent = None;
        if use_gso {
            match sys::send_gso_batch_with(fd, runs, sends.len(), message) {
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
        stall_send(stats, class, started.elapsed().as_micros() as u64);
        let sent_us = now_unix_us();
        for send in &sends[..sent] {
            first[usize::from(send.message)].get_or_insert((target(send), sent_us));
            snapshot.children[usize::from(send.child)].sent[class.index()]
                .fetch_add(1, Ordering::Relaxed);
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
                    snapshot.children[usize::from(send.child)].sent[class.index()]
                        .fetch_add(1, Ordering::Relaxed);
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
    fn plan_fast(
        &self,
        message: usize,
        view: &RlsView<'_>,
        plan: &mut ForwardPlan,
        now: Instant,
        trim: bool,
    ) {
        let role = view.role;
        let mut object = None;
        // Renditions that a child's subtree does not read stay off its lane
        // (wave 7 item 5).
        let gate = self
            .demand_groups()
            .and_then(|groups| view.stream_id().map(|stream| (groups, stream)));
        // A one-symbol audio object has nothing a repair lane could carry
        // but its symbol when its parent sent no repair (wave 7 item 2):
        // the repair-lane child takes the source, so it still has a copy
        // from this forwarder beside its primary's, and its burst copy
        // unless --relay-repair-lane-burst-copy is off.
        let single_symbol = matches!(role, MediaDatagramRole::Source)
            && view.source_symbols == 1
            && view.kind == MediaObjectKind::Audio
            && self.repair_lane_single_symbol();
        let single_symbol_copy = self.repair_lane_burst_copy();
        for (index, path) in self.paths.iter().enumerate() {
            if let Some((groups, stream)) = gate {
                let report = path.demand.fresh(now);
                if !groups.allows(stream, report.as_ref().map(|report| report.1.as_slice())) {
                    self.filtered_datagrams.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
            if trim && path.role != RelayForwardRole::Repair {
                self.filtered_datagrams.fetch_add(1, Ordering::Relaxed);
                continue;
            }
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
                    copy: !single_symbol_source || single_symbol_copy,
                });
                continue;
            }
            if path.role == RelayForwardRole::Repair && matches!(role, MediaDatagramRole::Source) {
                plan.warm.push(PlannedWarm {
                    message: message as u16,
                    path: index as u16,
                    object: *object.get_or_insert_with(|| view_object_id(view)),
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

    /// Each child's ledger on each lane: the socket its datagrams of the
    /// lane leave from, where it takes them, and how many were sent it so
    /// far (4k-repair).
    pub(crate) fn ledgers(&self) -> Vec<(std::os::fd::RawFd, SocketAddr, u64)> {
        let mut out = Vec::with_capacity(2 * self.paths.len());
        for class in TrafficClass::ALL {
            for (index, path) in self.paths.iter().enumerate() {
                out.push((
                    self.class_fd(class, path.group),
                    self.class_target(class, index),
                    path.sent[class.index()].load(Ordering::Relaxed),
                ));
            }
        }
        out
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
            runs,
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
                match sys::send_gso_batch_with(fd, runs, sends.len(), message) {
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
            stall_send(stats, class, elapsed.as_micros() as u64);
            let (mut sources, mut repairs, mut bytes) = (0_u64, 0_u64, 0_u64);
            for send in &sends[..sent] {
                // The child's ledger of the lane (4k-repair link repair).
                self.paths[usize::from(send.path)].sent[class.index()]
                    .fetch_add(1, Ordering::Relaxed);
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
        // A socket that refused one datagram (its send buffer is full) is not
        // tried again in this window: one more failing syscall per symbol,
        // up to 64 for each child, is kernel time on the video thread while
        // the queue drains (the lease path's `send_rest` stops the same way).
        let mut full: Option<std::os::fd::RawFd> = None;
        for (group, send) in retry.drain(..) {
            let data = batch.data(usize::from(send.message));
            let target = self.class_target(class, usize::from(send.path));
            let fd = self.class_fd(class, group);
            if full == Some(fd) {
                self.errors.fetch_add(1, Ordering::Relaxed);
                stats.would_block.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let started = Instant::now();
            match sys::send_batch(fd, 1, |_| (data, target)) {
                Ok(1) => {
                    self.paths[usize::from(send.path)].sent[class.index()]
                        .fetch_add(1, Ordering::Relaxed);
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
                    full = Some(fd);
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
    /// The tee ring's producer end, which `run` takes.
    tee: Option<TeeSender<TeeMeta>>,
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
    demand: Option<Arc<NodeDemand>>,
    /// The parents that demand reports go to: every admitted peer but the
    /// ranked lease parents.
    demand_peers: Arc<[SocketAddr]>,
    lease_descriptors: bool,
    video_gro: bool,
    /// What each parent delivers, for `GET /api/node` (see `parent_feeds`).
    feeds: Arc<ParentFeeds>,
    /// The media budget (A 2.5), in µs.
    media_deadline_us: u64,
    /// `RelayClassConfig::trim_forwarded_repair`.
    trim_repair: bool,
    /// This node regenerates the sources it lacked for its children: the
    /// trim forwards them only the margin of upstream repair.
    trim_regenerated: bool,
    /// The epoch that the cache validated for each stream, which the
    /// thread's media clocks follow.
    epochs: Arc<ValidatedEpochs>,
    /// Tests: the counter of this thread's allocations once it runs.
    #[cfg(test)]
    test_alloc_counter: Option<&'static crate::alloc_count::AllocCounter>,
}

/// Wave 9 (cold start): each stream's last whole one-symbol audio part
/// (RLS1: it carries the descriptor that the compact RLC1 parts are read
/// against) and the lease children that had a whole part of the stream
/// since. A child new to the stream (an edge taking a new copy) gets that
/// part before its first compact part. Without it the child dropped every
/// compact part ("unknown descriptor") until the contributor's next whole
/// part, up to --relay-compact-whole-ms (100 ms) later, and served its new
/// listeners from the cache mesh meanwhile, 7-18 ms late (wave 9 trace).
///
/// A fixed table made before the thread runs (stall follow-up): a new
/// stream takes a free entry, not an insert and two buffers' allocations on
/// the real-time thread.
struct LeaseDescriptors {
    /// The table's pin, first so that it unlocks before the table is freed.
    _pin: crate::memory::Pinned,
    streams: Box<[LastWhole]>,
}

#[derive(Clone, Copy)]
struct LastWhole {
    /// The stream id's bytes as RLS1 carries them (`key_len` 0: free).
    key: [u8; DESCRIPTOR_KEY_BYTES],
    key_len: u8,
    datagram: [u8; DESCRIPTOR_BYTES],
    len: u16,
    /// When it arrived, on this node's clock.
    kept_us: u64,
    had: [SocketAddr; DESCRIPTOR_CHILDREN],
    had_len: u8,
}

/// Streams whose last whole part the audio thread keeps, at most.
const MAX_DESCRIPTOR_STREAMS: usize = 1_024;
/// How long a stream's last whole part is sent to a new child, from its
/// receipt.
const DESCRIPTOR_HOLD_US: u64 = 1_000_000;
/// A whole part larger than this (an RLS1 datagram is at most 1,472 bytes)
/// is not kept.
const DESCRIPTOR_BYTES: usize = 1_472;
/// The room of a stream id: any id the wire admits.
const DESCRIPTOR_KEY_BYTES: usize = super::media_clock::ID_ROOM_BYTES;
/// The lease children of a stream that a part is remembered as sent to
/// (twice `DEFAULT_MAX_LEASE_CHILDREN`: a child is one entry per role). A
/// child beyond them gets no descriptor and reads the next whole part.
const DESCRIPTOR_CHILDREN: usize = 32;

impl LastWhole {
    const FREE: Self = Self {
        key: [0; DESCRIPTOR_KEY_BYTES],
        key_len: 0,
        datagram: [0; DESCRIPTOR_BYTES],
        len: 0,
        kept_us: 0,
        had: [SocketAddr::V4(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::UNSPECIFIED,
            0,
        )); DESCRIPTOR_CHILDREN],
        had_len: 0,
    };

    fn had(&self) -> &[SocketAddr] {
        &self.had[..usize::from(self.had_len)]
    }

    /// Remember `peer` as sent the kept part; `false` when there is no room
    /// (it is then treated as having had it).
    fn mark(&mut self, peer: SocketAddr) -> bool {
        let at = usize::from(self.had_len);
        if at == DESCRIPTOR_CHILDREN {
            return false;
        }
        self.had[at] = peer;
        self.had_len += 1;
        true
    }
}

impl LeaseDescriptors {
    fn new() -> Self {
        let mut streams = vec![LastWhole::FREE; MAX_DESCRIPTOR_STREAMS].into_boxed_slice();
        Self {
            _pin: crate::memory::pin_slice("lease descriptors", &mut streams),
            streams,
        }
    }

    /// The entry of `stream`, taking a free one for a new stream when
    /// `take`; `None` when the table is full or the id too long.
    fn entry(&mut self, stream: &str, take: bool) -> Option<&mut LastWhole> {
        let key = stream.as_bytes();
        if key.is_empty() || key.len() > DESCRIPTOR_KEY_BYTES {
            return None;
        }
        // FNV-1a: the table is this thread's, and its keys are its streams.
        let hash = key.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
        });
        let slots = self.streams.len();
        let first = (hash as usize) % slots;
        for probe in 0..slots {
            let index = (first + probe) % slots;
            let entry = &self.streams[index];
            if entry.key_len == 0 {
                if !take {
                    return None;
                }
                let entry = &mut self.streams[index];
                entry.key[..key.len()].copy_from_slice(key);
                entry.key_len = key.len() as u8;
                return Some(entry);
            }
            if &entry.key[..usize::from(entry.key_len)] == key {
                return Some(&mut self.streams[index]);
            }
        }
        None
    }

    /// Look at one admitted audio datagram that goes to the stream's lease
    /// children: keep a whole one-symbol source part, or send the kept one
    /// to each child that has not had it before this compact part goes.
    #[allow(clippy::too_many_arguments)]
    fn observe(
        &mut self,
        fd: std::os::fd::RawFd,
        snapshot: &LeaseSnapshot,
        class: TrafficClass,
        view: &RlsView<'_>,
        datagram: &[u8],
        now_us: u64,
        stats: &FastPlaneStats,
    ) {
        let children = snapshot
            .children_and_clock_of(view.stream)
            .map_or(&[][..], |(children, _)| children);
        let target = |child: &u16| snapshot.children[usize::from(*child)].targets[class.index()];
        if view.slim.is_none() {
            // Kept whether or not the stream has lease children yet: the
            // first child comes between two whole parts.
            if view.source_symbols != 1
                || !matches!(view.role, MediaDatagramRole::Source)
                || datagram.len() > DESCRIPTOR_BYTES
            {
                return;
            }
            let Some(last) = self.entry(view.stream, true) else {
                return;
            };
            last.datagram[..datagram.len()].copy_from_slice(datagram);
            last.len = datagram.len() as u16;
            last.kept_us = now_us;
            // Every child of the stream gets this one now.
            last.had_len = 0;
            for child in children {
                last.mark(target(child));
            }
            return;
        }
        if children.is_empty() {
            return;
        }
        let Some(last) = self.entry(view.stream, false) else {
            return;
        };
        if last.len == 0 || now_us.saturating_sub(last.kept_us) >= DESCRIPTOR_HOLD_US {
            return;
        }
        for child in children {
            let peer = target(child);
            if last.had().contains(&peer) || !last.mark(peer) {
                continue;
            }
            let kept = &last.datagram[..usize::from(last.len)];
            if sys::send_batch(fd, 1, |_| (kept, peer)).unwrap_or(0) == 1 {
                stats.lease_descriptors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// How often the audio thread looks at its loss meters.
const LOSS_POLL: Duration = Duration::from_millis(100);

/// Video parts under way that [`TeeParts`] remembers its verdict for.
const TEE_PARTS: usize = 64;

/// Tests: the video thread tees symbol by symbol, as before `TeeParts`
/// (a measurement's baseline).
#[cfg(test)]
pub(crate) static TEE_PARTS_OFF: AtomicBool = AtomicBool::new(false);

/// The video thread tees a part whole or not at all (fix4-video).
///
/// A full ring refused whichever symbol came, and took the next as soon as
/// the tee released an entry: while the tee was behind, each part lost
/// symbols here and there, more than its repair covers, so every part of
/// the episode was lost (and the tee still spent its time on all of them).
/// Now the verdict is made at a part's first symbol: the part is teed when
/// the ring has room for all its symbols and as many again (the part before
/// may still be arriving), else none of it is, and the parts after it find
/// the room the tee frees. A tee short of processor time by a tenth loses
/// about a tenth of the parts, not all of them.
///
/// A fixed table made before the thread runs: a verdict per (stream,
/// part), the oldest replaced by a new part.
struct TeeParts {
    parts: [TeePart; TEE_PARTS],
    next: usize,
}

#[derive(Clone, Copy, Default)]
struct TeePart {
    stream: u64,
    object: u64,
    used: bool,
    teed: bool,
}

/// What the tee needs of the ring for a part of `source_symbols` symbols
/// of `len` bytes: its sources, an eighth more for repair, and a few more.
fn tee_part_need(source_symbols: u64, len: u64) -> (u64, u64) {
    let entries = source_symbols + source_symbols / 8 + 8;
    (entries, entries.saturating_mul(len))
}

impl TeeParts {
    fn new() -> Self {
        Self {
            parts: [TeePart::default(); TEE_PARTS],
            next: 0,
        }
    }

    /// Whether this symbol of part `object` of `stream` goes to the tee,
    /// and whether the call shed a new part. `room` is the ring's
    /// ([`TeeSender::room`]), read only for a part's first symbol.
    fn admit(
        &mut self,
        stream: u64,
        object: u64,
        source_symbols: u64,
        len: u64,
        room: impl FnOnce() -> (u64, u64),
    ) -> (bool, bool) {
        if let Some(part) = self
            .parts
            .iter()
            .find(|part| part.used && part.object == object && part.stream == stream)
        {
            return (part.teed, false);
        }
        let (entries, bytes) = tee_part_need(source_symbols, len);
        let (free_entries, free_bytes) = room();
        let teed =
            free_entries >= entries.saturating_mul(2) && free_bytes >= bytes.saturating_mul(2);
        self.parts[self.next] = TeePart {
            stream,
            object,
            used: true,
            teed,
        };
        self.next = (self.next + 1) % TEE_PARTS;
        (teed, !teed)
    }
}

/// Parts under way whose forwarded repair `RepairTrim` counts.
const TRIM_PARTS: usize = 64;
/// Parts of fewer source symbols are forwarded whole (`RepairTrim`): by
/// size alone, so an audio part's one to three symbols and its copies are
/// as before on the audio thread.
const TRIM_MIN_SYMBOLS: u16 = 32;
/// Parts of fewer source symbols take no part repair (asks, answers, link
/// repair): a one-symbol part has its copies.
const PART_REPAIR_MIN_SYMBOLS: u16 = 2;

/// The upstream repair of a part that the children taking this
/// node's sources need (4k-repair): the sources this node lacks of the part
/// (they lack them too) and a margin. Without it every hop forwarded every
/// upstream hop's repair on top of its own link repair, so the repair grew
/// with each hop (in process at 1 % a link: 7, 13, 21 and 29 % of the
/// sources on four links). A repair symbol that comes before the part's
/// sources have (another lane's) is counted against the sources seen so
/// far, so it errs towards forwarding. A fixed table made before the
/// thread runs, the oldest part replaced by a new one.
struct RepairTrim {
    parts: [TrimPart; TRIM_PARTS],
    next: usize,
}

#[derive(Clone, Copy, Default)]
struct TrimPart {
    stream: u64,
    object: u64,
    used: bool,
    sources: u32,
    repair: u32,
}

impl RepairTrim {
    fn new() -> Self {
        Self {
            parts: [TrimPart::default(); TRIM_PARTS],
            next: 0,
        }
    }

    /// A first arrival of symbol `esi` of part `object` of `stream` (K
    /// `k`): whether it is upstream repair that the source-taking children
    /// do not need.
    fn beyond_need(
        &mut self,
        stream: u64,
        object: u64,
        k: u16,
        esi: u32,
        regenerated: bool,
    ) -> bool {
        let at = match self
            .parts
            .iter()
            .position(|part| part.used && part.object == object && part.stream == stream)
        {
            Some(at) => at,
            None => {
                let at = self.next;
                self.parts[at] = TrimPart {
                    stream,
                    object,
                    used: true,
                    sources: 0,
                    repair: 0,
                };
                self.next = (self.next + 1) % TRIM_PARTS;
                at
            }
        };
        let part = &mut self.parts[at];
        let k = u32::from(k);
        if esi < k {
            part.sources += 1;
            return false;
        }
        // With regenerated sources (4k-repair) the children get what this
        // node lacked as sources once it decodes: only the margin goes.
        let lacking = if regenerated {
            0
        } else {
            k.saturating_sub(part.sources)
        };
        if part.repair < lacking + 2 + lacking / 8 {
            part.repair += 1;
            return false;
        }
        true
    }
}

struct Pending {
    message: usize,
    peer: SocketAddr,
    path: RelayIngressParentPath,
    rx_us: u64,
    admitted: Option<bool>,
    /// A video part's symbol on the video thread: (stream key, part,
    /// source symbols), for [`TeeParts`].
    part: Option<(u64, u64, u64)>,
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
            // Wave 8: without real-time priority, the audio thread still
            // comes first: the process's other threads are made nicer.
            if self.class == TrafficClass::Audio && self.realtime && sys::fallback_nice() > 0 {
                let niced = sys::nice_other_threads();
                info!(
                    niced,
                    step = sys::fallback_nice(),
                    "the other threads of the process are nicer than the audio thread"
                );
            }
        }
    }

    fn run(mut self) {
        let mut tee = self.tee.take().expect("a class thread runs once");
        // The sends to the edge's clients: by this thread, or by fan-out
        // threads (wave 7 item 10), started here, before this thread is made
        // real-time and pinned, so that they inherit none of it and its
        // first client finds them running (wave 9 first-session stall).
        let mut client_sender = ClientSender::new(self.edge_clients.as_ref(), self.class);
        // Everything the loop uses is made (and touched) below, before the
        // thread is made real-time: once it runs, the loop allocates, frees
        // and first-touches nothing (stall follow-up; `alloc_free` tests).
        let fds: Vec<_> = self
            .sockets
            .iter()
            .map(|(_, socket)| socket.as_raw_fd())
            .collect();
        // UDP GRO on the video sockets (wave 9 D); a socket that refuses it
        // still reads one datagram per message through the same batch.
        let gro_sockets = if self.video_gro && self.class == TrafficClass::Video {
            self.sockets
                .iter()
                .filter(|(_, socket)| match sys::enable_gro(socket) {
                    Ok(()) => true,
                    Err(error) => {
                        info!(error = %error, "UDP GRO refused on a video socket");
                        false
                    }
                })
                .count()
        } else {
            0
        };
        self.stats
            .gro_sockets
            .fetch_add(gro_sockets as u64, Ordering::Relaxed);
        let mut batch = if gro_sockets > 0 {
            RecvBatch::with_gro()
        } else {
            RecvBatch::new()
        };
        let mut dedup = FirstArrival::new(DEDUP_CAPACITY);
        let mut clocks = MediaClocks::new(MEDIA_CLOCKS);
        // The validated epochs' version and the clocks made when the clocks
        // last followed them.
        let mut followed = None;
        let mut lag_second = 0;
        // Room for every lease child: two entries (roles) per child.
        let lease_children = self
            .lease
            .as_ref()
            .map_or(0, |lease| 2 * lease.max_children());
        let mut plan = self.forwarder.as_ref().map_or_else(
            || ForwardPlan::new(0, 0, 0, lease_children),
            |forwarder| {
                ForwardPlan::new(
                    forwarder.socket_groups.len(),
                    forwarder.paths.len(),
                    forwarder.warm_paths(),
                    lease_children,
                )
            },
        );
        plan.touch();
        let mut pending = Vec::with_capacity(RECV_BATCH);
        crate::memory::touch_spare(&mut pending);
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
        // The loss of each parent lane: on the audio thread (the copies of
        // one-symbol audio parts), and on the video thread (4k-repair: the
        // contributor sizes video repair to it, a forwarder its children's
        // link repair, a child its asks).
        let mut meters: Vec<LossMeter> = if self.loss_report {
            self.sockets.iter().map(|_| LossMeter::new()).collect()
        } else {
            Vec::new()
        };
        let mut descriptors = (self.lease_descriptors
            && self.class == TrafficClass::Audio
            && self.lease.is_some())
        .then(LeaseDescriptors::new);
        let mut feed_misses = FeedMisses::new();
        let mut tee_parts = TeeParts::new();
        let mut repair_trim = RepairTrim::new();
        let mut last_loss_poll = Instant::now();
        let mut last_demand_report: Option<Instant> = None;
        // Loop lag (wave 9): the last wait's return that brought datagrams.
        let mut last_ready_at: Option<Instant> = None;
        let mut faults_read_at = Instant::now();
        // The lock-free snapshots' first load registers this thread with
        // arc-swap (an allocation, once per thread): here, not in a pass.
        if let Some(lease) = &self.lease {
            drop(lease.load());
        }
        if let Some(lane) = &self.edge_clients {
            drop(lane.load());
        }
        self.feeds.warm_up();
        // The loop's deepest calls find their stack pages, and every path
        // its code, mapped.
        crate::memory::touch_stack();
        crate::memory::populate_mappings();
        self.schedule();
        self.stats.threads.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        crate::alloc_count::watch_this_thread(self.test_alloc_counter);
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
            let woke = Instant::now();
            let woke_us = now_unix_us();
            self.turn_clocks(&mut clocks, &mut followed, woke_us);
            if self.class == TrafficClass::Audio && woke_us / 1_000_000 != lag_second {
                lag_second = woke_us / 1_000_000;
                self.stats.roll_duplicate_lag();
            }
            // A wait that timed out is idle time, not lag: the next gap
            // starts after it.
            let gap_us = if ready == 0 {
                last_ready_at = None;
                None
            } else {
                last_ready_at
                    .replace(woke)
                    .map(|at| woke.saturating_duration_since(at).as_micros() as u64)
            };
            for (index, (path, socket)) in self.sockets.iter().enumerate() {
                if ready & (1 << index) == 0 {
                    continue;
                }
                // A few batches, then the next socket: what is left is
                // still readable at the next wait.
                for _ in 0..SOCKET_TURN_BATCHES {
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
                    // A GRO receive shows its datagrams RECV_BATCH at a time.
                    loop {
                        self.drain(
                            &batch,
                            *path,
                            &mut kernel_dropped[index],
                            &mut dedup,
                            &mut clocks,
                            &mut plan,
                            &mut pending,
                            &mut clients,
                            &mut client_sender,
                            &mut gso,
                            copies.as_mut(),
                            meters.get_mut(index),
                            descriptors.as_mut(),
                            &mut tee,
                            &mut tee_parts,
                            &mut repair_trim,
                            &mut feed_misses,
                        );
                        if !batch.next_window() {
                            break;
                        }
                    }
                    if received < RECV_BATCH {
                        break;
                    }
                }
            }
            // What the feeds held of new keys, once the tee made them.
            self.feeds.book_misses(&mut feed_misses, woke_us);
            if let Some(copies) = copies.as_mut() {
                self.send_copies(copies);
            }
            if !meters.is_empty() && last_loss_poll.elapsed() >= LOSS_POLL {
                last_loss_poll = Instant::now();
                self.report_loss(&fds, &mut meters, last_loss_poll);
            }
            if self.demand.is_some()
                && last_demand_report.is_none_or(|at| at.elapsed() >= DEMAND_REPORT_INTERVAL)
            {
                last_demand_report = Some(Instant::now());
                self.report_demand(&fds);
            }
            if batch.is_gro() {
                self.stats
                    .gro_messages
                    .store(batch.gro_messages, Ordering::Relaxed);
                self.stats
                    .gro_datagrams
                    .store(batch.gro_datagrams, Ordering::Relaxed);
            }
            if ready != 0 {
                self.stats.record_loop(
                    self.class,
                    woke.elapsed().as_micros() as u64,
                    gap_us,
                );
                stall_pass(&self.stats, self.class, woke.elapsed().as_micros() as u64);
                stall_gap(&self.stats, self.class, gap_us);
            }
            if faults_read_at.elapsed() >= Duration::from_secs(1) {
                faults_read_at = Instant::now();
                let (minor, major) = crate::memory::faults(true);
                let slot = &self.stats.thread_faults[self.class.index()];
                slot[0].store(minor, Ordering::Relaxed);
                slot[1].store(major, Ordering::Relaxed);
            }
            #[cfg(test)]
            crate::alloc_count::pass_done();
        }
    }

    /// The media clocks at the start of a turn: the local time, by which a
    /// clock goes idle, and the epochs the cache validated since they last
    /// looked (or for a stream whose clock is new).
    fn turn_clocks(
        &self,
        clocks: &mut MediaClocks,
        followed: &mut Option<(u64, u64)>,
        now_us: u64,
    ) {
        clocks.tick(now_us);
        // The version before the epochs: a write after this read is seen
        // at the next turn.
        let seen = (self.epochs.version(), clocks.created());
        if *followed != Some(seen) {
            clocks.follow(&self.epochs);
            *followed = Some(seen);
        }
        self.stats.media_clock_overflow[self.class.index()]
            .store(clocks.overflow(), Ordering::Relaxed);
        self.stats
            .media_clock_unrecorded
            .store(self.epochs.unrecorded(), Ordering::Relaxed);
    }

    /// Send every relay parent this node's demand, from each audio socket:
    /// a parent knows its child by the address its symbols go to, and
    /// ignores a report from any other (a promoted secondary parent sends
    /// to the primary socket, so the admission's path does not say which).
    fn report_demand(&self, fds: &[std::os::fd::RawFd]) {
        let Some(demand) = &self.demand else {
            return;
        };
        let report = demand.report();
        if report.is_empty() {
            return;
        }
        for peer in self.demand_peers.iter() {
            for fd in fds {
                if sys::send_batch(*fd, 1, |_| (report.as_slice(), *peer)).unwrap_or(0) == 1 {
                    self.stats.demand_reports.fetch_add(1, Ordering::Relaxed);
                }
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
            if self.class == TrafficClass::Video {
                self.stats.video_parent_loss_ppm[lane_index(self.sockets[index].0)]
                    .store(u64::from(meter.loss_ppm()), Ordering::Relaxed);
            }
            if let Some(lane) = self.stats.lane(self.class, self.sockets[index].0) {
                lane.loss_ppm
                    .store(u64::from(meter.loss_ppm()), Ordering::Relaxed);
            }
        }
        if self.class == TrafficClass::Audio {
            self.stats
                .parent_loss_ppm
                .store(u64::from(worst), Ordering::Relaxed);
        }
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
        clocks: &mut MediaClocks,
        plan: &mut ForwardPlan,
        pending: &mut Vec<Pending>,
        clients: &mut ClientPlan,
        client_sender: &mut ClientSender,
        gso: &mut bool,
        copies: Option<&mut BurstCopies>,
        mut meter: Option<&mut LossMeter>,
        mut descriptors: Option<&mut LeaseDescriptors>,
        tee: &mut TeeSender<TeeMeta>,
        tee_parts: &mut TeeParts,
        repair_trim: &mut RepairTrim,
        feed_misses: &mut FeedMisses,
    ) {
        let now_us = now_unix_us();
        let now = Instant::now();
        pending.clear();
        plan.clear();
        let class = self.class;
        // The lease children as they are now, without a lock.
        let lease_now = self.lease.as_deref().map(|lease| (lease, lease.load()));
        // Read through the guard, never cloned: a snapshot that a rebuild
        // replaces is freed by the rebuild's thread, not this one.
        let lease_snapshot: Option<(&LeaseChildren, &LeaseSnapshot)> =
            lease_now.as_ref().map(|(lease, guard)| (*lease, &***guard));
        // The whole parts are kept for children to come (wave 9), so the
        // descriptors look at every batch; the lease plan only when there
        // are children.
        let descriptor_lease = lease_snapshot.filter(|_| descriptors.is_some());
        let lease = lease_snapshot.filter(|(_, snapshot)| !snapshot.is_empty());
        // The edge's fast-lane clients as they are now, without a lock.
        let edge_clients = self
            .edge_clients
            .as_ref()
            .map(|lane| (lane, lane.load()))
            .filter(|(_, snapshot)| !snapshot.streams.is_empty());
        clients.clear();
        self.stats.received[class.index()].fetch_add(batch.len() as u64, Ordering::Relaxed);
        self.stats.batches[class.index()].fetch_add(1, Ordering::Relaxed);
        let parent_lane = self.stats.lane(class, path);
        if let Some(parent_lane) = parent_lane {
            parent_lane
                .received
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
        if class == TrafficClass::Video {
            // What `VideoSocketBuffers` sizes the socket's buffer to.
            let bytes: usize = (0..batch.len()).map(|message| batch.data(message).len()).sum();
            self.stats.video_rx_bytes[lane_index(path)].fetch_add(bytes as u64, Ordering::Relaxed);
        }
        for message in 0..batch.len() {
            let received = batch.get(message);
            if let Some(dropped) = received.dropped {
                record_kernel_drops(&self.stats, class, path, kernel_dropped, dropped);
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
            stall_rx(&self.stats, class, message, now_us.saturating_sub(rx_us));
            let unchecked = Pending {
                message,
                peer,
                path,
                rx_us,
                admitted: None,
                part: None,
            };
            if !is_relay_datagram(received.data) {
                // A parent's ledger (RLL1) for the loss meter of this lane.
                if let Some(meter) = meter.as_deref_mut() {
                    if let Some(sent) = decode_ledger(received.data) {
                        meter.ledger(peer, sent, *kernel_dropped, now);
                        if let Some(parent_lane) = parent_lane {
                            parent_lane.ledger(peer, now_us);
                        }
                        continue;
                    }
                }
                pending.push(unchecked);
                continue;
            }
            // Every relay datagram that arrived counts against the parent's
            // ledger, admitted or not: the meter measures the path.
            if let Some(meter) = meter.as_deref_mut() {
                meter.observe(peer);
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
                if let Some(parent_lane) = parent_lane {
                    parent_lane.not_admitted.fetch_add(1, Ordering::Relaxed);
                }
                continue;
            }
            // On its stream's own timeline (A 2.5): a datagram of an ended
            // session, or a budget behind its stream's head (a sender that
            // was frozen and resumes), is neither forwarded nor teed, and
            // makes no parent look live.
            match clocks.observe(
                MediaClocks::key(view.tenant, view.stream),
                view.epoch,
                view.deadline_us,
                self.media_deadline_us,
            ) {
                Verdict::Fresh => {}
                Verdict::StaleEpoch => {
                    self.stats.stale_epoch.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Verdict::StaleMedia => {
                    self.stats.stale_media.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
            self.failover.observe(path, view.role, now);
            self.feeds.observe(peer.ip(), &view, rx_us, feed_misses);
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
            // The audio class compares the bytes of every copy (A 2.7).
            let digest = if class == TrafficClass::Audio {
                dedup::digest(received.data)
            } else {
                0
            };
            match dedup.first(key, digest, rx_us, lane_index(path) as u8) {
                Arrival::First => {
                    if let Some(parent_lane) = parent_lane {
                        parent_lane.first_arrivals.fetch_add(1, Ordering::Relaxed);
                    }
                }
                duplicate @ Arrival::Duplicate { .. } => {
                    self.stats.record_duplicate(class, path, duplicate, &view);
                    continue;
                }
            }
            // Upstream repair beyond the part's need here goes only to
            // repair-only children (4k-repair).
            let trim = self.trim_repair
                && view.source_symbols >= TRIM_MIN_SYMBOLS
                && repair_trim.beyond_need(
                    MediaClocks::key(view.tenant, view.stream).hash(),
                    view.object,
                    view.source_symbols,
                    view.esi,
                    self.trim_regenerated,
                );
            if trim {
                self.stats.repair_trimmed.fetch_add(1, Ordering::Relaxed);
            }
            if let Some(forwarder) = &self.forwarder {
                forwarder.plan_fast(message, &view, plan, now, trim);
            }
            if let (Some(descriptors), Some((children, snapshot))) =
                (descriptors.as_deref_mut(), &descriptor_lease)
            {
                descriptors.observe(
                    children.fd(class),
                    snapshot,
                    class,
                    &view,
                    received.data,
                    now_us,
                    &self.stats,
                );
            }
            if let Some((_, snapshot)) = &lease {
                plan_lease(snapshot, class, message, &view, plan, now_us, trim);
            }
            if let Some((lane, snapshot)) = &edge_clients {
                clients.plan(lane, snapshot, message, &view);
            }
            let video_part = class == TrafficClass::Video && view.is_video_kind();
            pending.push(Pending {
                admitted: Some(view.is_video_kind()),
                part: video_part.then(|| {
                    (
                        MediaClocks::key(view.tenant, view.stream).hash(),
                        view.object,
                        u64::from(view.source_symbols),
                    )
                }),
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
        // client socket of the class (or, for many, from fan-out threads).
        if let Some((lane, _)) = &edge_clients {
            client_sender.send(clients, lane, class);
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
            let meta = TeeMeta {
                peer: entry.peer,
                path: entry.path,
                rx_us: entry.rx_us,
                woken_us: now_us,
                client_sent: clients.sent_to(entry.message),
                kind,
            };
            let data = batch.data(entry.message);
            // A video part goes whole or not at all (`TeeParts`).
            #[cfg(test)]
            let part = entry
                .part
                .filter(|_| !TEE_PARTS_OFF.load(Ordering::Relaxed));
            #[cfg(not(test))]
            let part = entry.part;
            let teed = match part {
                Some((stream, object, source_symbols)) => {
                    let (teed, shed) = tee_parts.admit(
                        stream,
                        object,
                        source_symbols,
                        data.len() as u64,
                        || tee.room(),
                    );
                    if shed {
                        self.stats.tee_parts_shed.fetch_add(1, Ordering::Relaxed);
                    }
                    teed
                }
                None => true,
            };
            // Into the pinned ring: no allocation, and nothing for the
            // class thread to free (stall follow-up). A full ring drops
            // the copy, as a full channel did.
            if !teed || !tee.push(meta, data) {
                self.stats.tee_dropped.fetch_add(1, Ordering::Relaxed);
                if tee.growing() {
                    self.stats
                        .tee_dropped_growing
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        // The tee also makes the feed entries asked for in the batch: a
        // key is asked for at a datagram that is teed with it.
        tee.wake();
        self.stats.tee_ring_bytes[class.index()].store(tee.capacity(), Ordering::Relaxed);
    }
}

/// Count what a socket's receive queue dropped since `last` (the socket's
/// SO_RXQ_OVFL counter as its last datagram carried it): in all, and for the
/// socket's lane.
fn record_kernel_drops(
    stats: &FastPlaneStats,
    class: TrafficClass,
    path: RelayIngressParentPath,
    last: &mut u32,
    dropped: u32,
) {
    if dropped == *last {
        return;
    }
    let more = u64::from(dropped.wrapping_sub(*last));
    stats.kernel_dropped.fetch_add(more, Ordering::Relaxed);
    stats.kernel_dropped_lanes[class.index()][lane_index(path)].fetch_add(more, Ordering::Relaxed);
    *last = dropped;
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
    a.compact_decoded_objects += v.compact_decoded_objects;
    a.compact_unknown_descriptor += v.compact_unknown_descriptor;
    a.compact_descriptors_learned += v.compact_descriptors_learned;
    a.wall_late_parts += v.wall_late_parts;
    a.wall_late_max_us = a.wall_late_max_us.max(v.wall_late_max_us);
    a.rejected_by_kind.add(&v.rejected_by_kind);
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
    /// The parent feeds, whose new entries the class thread asks the tee
    /// to make.
    feeds: Arc<ParentFeeds>,
    /// 4k-repair, for parts of several symbols of any stream: the node's
    /// part repair (it keeps each such part decoded here for its children's
    /// asks, and sends lossy children their link repair), the asks of this
    /// node's own parts, the lease children (for link repair), and the tick
    /// that polls the asks between datagrams.
    part_repair: Option<Arc<PartRepair>>,
    requester: Option<Requester>,
    lease: Option<Arc<LeaseChildren>>,
    repair_tick: Option<Arc<Notify>>,
    /// The newest arrival (kernel µs) of a symbol the tee has taken.
    last_rx_us: u64,
    /// The part whose `relay_forward` the short path journaled last.
    forward_journaled: (u64, u64),
    /// The node's decode pool, and this tee's channel back from it.
    decodes: Option<Arc<DecodePool>>,
    decode_tx: Option<tokio::sync::mpsc::UnboundedSender<DecodeDone>>,
    decode_rx: Option<tokio::sync::mpsc::UnboundedReceiver<DecodeDone>>,
    /// Tests: the tee panics on its first datagram.
    #[cfg(test)]
    panic_on_item: bool,
    /// Tests: processor time spent on each datagram besides its work.
    #[cfg(test)]
    spin_ns: u64,
}

/// The nice value of a lane's tee thread: the small-part lane's (audio)
/// first.
const fn tee_nice(class: TrafficClass) -> i32 {
    match class {
        TrafficClass::Audio => 0,
        TrafficClass::Video => 10,
    }
}

/// Entries the tee takes in one wake before it publishes its snapshot and
/// looks at its other work (fix4-video: was a receive batch, 65, so a tee
/// behind by a second published and went round its loop 400 times a
/// second more than it needed to).
const TEE_WAKE_ENTRIES: usize = 1_024;

impl Tee {
    /// Runs until the thread's channel closes. It waits on nothing but its
    /// datagrams and the housekeeping task's nudge to expire its state
    /// (`fast_control::run_tee_housekeeping`, which also rotates the cache):
    /// no timer of its own sits beside the datagrams.
    async fn run(mut self, items: TeeReceiver<TeeMeta>, expire: Arc<Notify>) -> Result<()> {
        let mut decoded = self.decode_rx.take();
        let tick = self.repair_tick.clone();
        loop {
            tokio::select! {
                Some(done) = async {
                    match decoded.as_mut() {
                        Some(decoded) => decoded.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.on_decode_done(done).await;
                    self.publish();
                }
                ready = items.ready() => {
                    if ready == Ready::Closed {
                        return Ok(());
                    }
                    // What is queued (a batch at most), each released once
                    // processed, then one snapshot. Its CPU time is a
                    // playlist's, not a listener's: the load's CPU term
                    // leaves it out (edge_admission::PLAYLIST_CPU_US).
                    let cpu_before = crate::edge_admission::thread_cpu_us();
                    let wall_before = Instant::now();
                    let mut lag_max_us = 0;
                    for _ in 0..TEE_WAKE_ENTRIES {
                        let Some((meta, bytes)) = items.peek() else {
                            break;
                        };
                        lag_max_us = lag_max_us.max(now_unix_us().saturating_sub(meta.woken_us));
                        self.process(meta, bytes).await;
                        items.release();
                    }
                    if let Some(requester) = self.requester.as_mut() {
                        // A tee behind its ring judges a part's silence by
                        // the arrivals it has taken, not by the clock.
                        let now = if items.peek().is_none() {
                            now_unix_us()
                        } else {
                            self.last_rx_us
                        };
                        requester.poll(now);
                    }
                    let class = self.class.index();
                    self.stats.tee_batches[class].fetch_add(1, Ordering::Relaxed);
                    self.stats.tee_lag_max_us[class].fetch_max(lag_max_us, Ordering::Relaxed);
                    self.feeds.make_requested();
                    self.publish();
                    // The batch ran on one worker thread unless `process`
                    // was parked on a lock and woke on another: then the
                    // difference of two threads' clocks is no measure, and
                    // what is booked never exceeds the batch's wall time.
                    if let (Some(before), Some(after)) =
                        (cpu_before, crate::edge_admission::thread_cpu_us())
                    {
                        let wall_us = wall_before.elapsed().as_micros() as u64;
                        let used = after.saturating_sub(before).min(wall_us);
                        crate::edge_admission::PLAYLIST_CPU_US.fetch_add(used, Ordering::Relaxed);
                        self.stats.tee_cpu_us[class].fetch_add(used, Ordering::Relaxed);
                    }
                }
                () = expire.notified() => {
                    self.expire().await;
                }
                () = async {
                    match &tick {
                        Some(tick) => tick.notified().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(requester) = self.requester.as_mut() {
                        let now = if items.peek().is_none() {
                            now_unix_us()
                        } else {
                            self.last_rx_us
                        };
                        requester.poll(now);
                    }
                }
            }
        }
    }

    fn publish(&self) {
        let snapshot = self.dispatch.receiver().snapshot();
        self.stats.deadline_drops[self.class.index()]
            .store(snapshot.counters.deadline_drops, Ordering::Relaxed);
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
        // Assemblies that their stream's head has passed, or that went
        // inactive. Datagrams the media clocks shed are counted by kind and
        // logged by `process_symbol`'s limiter.
        let relay_expired = self.dispatch.receiver_mut().expire(now_unix_us());
        if relay_expired.objects > 0 {
            debug!(
                class = self.class.as_str(),
                expired_objects = relay_expired.objects,
                "expired RelaySession receive state"
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

    /// A part decoded here, inline or on the decode pool: its asks end,
    /// it is kept for the children's asks (with the sources it lacked
    /// regenerated for them, and their link repair), and it is committed.
    async fn on_decoded(
        &mut self,
        ctx: DecodedCtx,
        object: Box<media_object::MediaObject>,
        envelope: bytes::Bytes,
        slot: Option<bytes::Bytes>,
        parent_count: usize,
        accepted_datagrams: usize,
    ) {
        let DecodedCtx {
            peer,
            rx_us,
            woken_us,
            forwarded,
            video_kind,
            meta,
            missing,
        } = ctx;
        let repair_meta = meta.as_ref().filter(|meta| meta.repairable());
        let journal_forward = |key: &ObjectKey| {
            if let (Some(forwarder), Some((child, sent_us))) = (&self.forwarder, forwarded) {
                forwarder.journal_forward(key, child, sent_us);
            }
        };
        let decoded_us = now_unix_us();
        // Every part decoded here ends its asks and follows its stream's
        // head, whatever its size (4k-repair review M2: a one-symbol part
        // was otherwise a gap, asked for whole).
        if let (Some(requester), Some(meta)) = (self.requester.as_mut(), meta.as_ref()) {
            requester.committed(meta.key, peer, decoded_us);
        }
        // Kept for the children's asks and link repair, when this node has
        // children (4k-repair).
        // A part of a session older than the stream's current one (the
        // cache's epoch floor) is neither kept nor sent to children: the
        // commit below refuses it too.
        let stale = object
            .key()
            .stream()
            .parse::<u64>()
            .ok()
            .and_then(|stream_id| self.cache.chunk_cache.epoch_floor(stream_id))
            .is_some_and(|floor| object.key().epoch() < floor);
        let kept = match (&self.part_repair, repair_meta) {
            (Some(repair), Some(_)) if stale => {
                repair.stats.refused_stale.fetch_add(1, Ordering::Relaxed);
                None
            }
            (Some(_), Some(meta)) if self.has_children() => {
                StoredPart::new(meta, object.key().clone(), envelope.clone())
            }
            _ => None,
        };
        journal_relay_symbol(&self.cache, object.key(), peer, rx_us);
        journal_woken(&self.cache, object.key(), peer, woken_us);
        journal_forward(object.key());
        self.mark_class(object.key().stream(), video_kind);
        let stream = object.key().stream().to_owned();
        let sequence = object.key().object();
        let publication_clock = relay_publication_clock(&object);
        let fast_lane = self.edge_clients.clone().zip(stream.parse::<u64>().ok());
        let canonical = !av_mesh::canonical_slot::is_soundkit_v2_bundle(&object)
            && object.kind() == media_object::ObjectKind::Media;
        let media = object.kind() == media_object::ObjectKind::Media;
        // The envelope for a stream with clients (shared, not copied).
        let top_up_envelope = fast_lane
            .as_ref()
            .filter(|(lane, stream_id)| lane.load().streams.contains_key(stream_id))
            .map(|_| envelope.clone());
        if let (Some(part), Some(repair)) = (kept, &self.part_repair) {
            repair.keep(part, &missing, |loss| self.part_children(&stream, loss));
        }
        match commit_relay_object(&self.cache, *object, envelope, slot).await {
            // Another copy came first (the cache mesh, or the other form of
            // the part): its clients and the journal have it. The parent is
            // live all the same, and the stream commits canonical slots all
            // the same: the fast lane serves it even while the cache mesh
            // wins every part.
            Ok(Committed::AlreadyPresent(_)) => {
                if let (Some((lane, stream_id)), true) = (fast_lane, media) {
                    lane.mark_committed(stream_id, canonical);
                }
                self.failover.stamp(&self.failover.decoded, Instant::now());
            }
            // Counted and logged by the cache; the first copy stays.
            Ok(Committed::Refused(_)) => {}
            Ok(Committed::Stored(_)) => {
                journal_relay_decoded(&self.cache, &stream, sequence, peer, decoded_us);
                if let Some((lane, stream_id)) = fast_lane {
                    if media {
                        lane.mark_committed(stream_id, canonical);
                    }
                    if let (Some(meta), Some(envelope), true) = (&meta, top_up_envelope, canonical)
                    {
                        lane.top_up(
                            stream_id,
                            sequence,
                            &meta.fec,
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

    /// Run the decodes that the receiver made ready: on the pool, or here
    /// when there is none or its queue is full.
    async fn start_decodes(&mut self, ctx: &DecodedCtx) {
        let jobs = self.dispatch.receiver_mut().take_decode_jobs();
        for job in jobs {
            let ctx = DecodedCtx {
                missing: job.missing_sources(),
                ..ctx.clone()
            };
            let back = match (&self.decodes, &self.decode_tx) {
                (Some(pool), Some(tx)) => pool.submit(job, ctx, tx).err(),
                _ => Some((job, ctx)),
            };
            if let Some((job, ctx)) = back {
                let envelope = match &self.decodes {
                    Some(pool) => super::decode_pool::decode(&job, &pool.stats),
                    None => job.run(),
                };
                self.on_decode_done(DecodeDone {
                    job,
                    ctx,
                    envelope,
                    bytes: 0,
                })
                .await;
            }
        }
    }

    /// A decode finished: commit the part, unless it completed meanwhile.
    async fn on_decode_done(&mut self, done: DecodeDone) {
        let DecodeDone {
            job,
            ctx,
            envelope,
            bytes,
        } = done;
        if let (Some(pool), true) = (&self.decodes, bytes > 0) {
            pool.taken(bytes);
        }
        match self
            .dispatch
            .receiver_mut()
            .complete_decode(job.key(), envelope, now_unix_us())
        {
            Ok(Some(RelayIngressOutcome::Decoded {
                object,
                envelope,
                slot,
                parent_count,
                accepted_datagrams,
                ..
            })) => {
                self.on_decoded(ctx, object, envelope, slot, parent_count, accepted_datagrams)
                    .await;
            }
            Ok(_) => {}
            Err(error) => {
                warn!(error = %error, "a decoded part did not read as its object");
            }
        }
    }

    /// Whether this node feeds children (compiled or lease) on the fast
    /// plane.
    fn has_children(&self) -> bool {
        self.forwarder
            .as_ref()
            .is_some_and(|forwarder| !forwarder.paths.is_empty())
            || self.lease.is_some()
    }

    /// The children of `stream` for part repair: those that lose datagrams
    /// from this node (with the socket to each and its loss, `loss` of the
    /// child's address), and those that take this node's sources.
    fn part_children(&self, stream: &str, loss: &dyn Fn(SocketAddr) -> u32) -> Children {
        let mut out = Children::default();
        let mut add = |via: Arc<dyn SendSocket>, to: SocketAddr, sources: bool| {
            let ppm = loss(to);
            if ppm > 0 && !out.lossy.iter().any(|(_, at, _)| *at == to) {
                out.lossy.push((Arc::clone(&via), to, ppm));
            }
            if sources && !out.sources.iter().any(|(_, at)| *at == to) {
                out.sources.push((via, to));
            }
        };
        if let Some(forwarder) = &self.forwarder {
            for (index, path) in forwarder.paths.iter().enumerate() {
                let target = forwarder.class_target(self.class, index);
                let via: Arc<dyn SendSocket> =
                    match (self.class, forwarder.video_sockets.get(path.group)) {
                        (TrafficClass::Video, Some(twin)) => Arc::clone(twin) as Arc<dyn SendSocket>,
                        _ => Arc::clone(&path.socket) as Arc<dyn SendSocket>,
                    };
                let sources = path.role.permits(MediaDatagramRole::Source)
                    || path.promoted.load(Ordering::Relaxed);
                add(via, target, sources);
            }
        }
        if let Some(lease) = &self.lease {
            let snapshot = lease.load();
            if let Some((children, _)) = snapshot.children_and_clock_of(stream) {
                for &child in children {
                    let entry = &snapshot.children[usize::from(child)];
                    let via: Arc<dyn SendSocket> =
                        Arc::new(super::part_repair::LeaseSocket(Arc::clone(lease), self.class));
                    add(
                        via,
                        entry.targets[self.class.index()],
                        entry.primary || entry.is_promoted(),
                    );
                }
            }
        }
        out
    }

    /// `relay_forward` of a part whose symbols take the receiver's short
    /// path: once a part (the journal keeps the first anyway), without its
    /// lock for each of the part's symbols.
    fn journal_forward_once(&mut self, view: &RlsView<'_>, child: SocketAddr, sent_us: u64) {
        let Some(stream) = view.stream_id() else {
            return;
        };
        if self.forward_journaled == (stream, view.object) {
            return;
        }
        self.forward_journaled = (stream, view.object);
        if let Some(forwarder) = &self.forwarder {
            forwarder.journal_forward_part(stream, view.object, child, sent_us);
        }
    }

    fn mark_class(&self, stream: &str, video_kind: bool) {
        if self.class == TrafficClass::Video || video_kind {
            if let Ok(stream_id) = stream.parse() {
                self.classes.mark_video(stream_id);
            }
        }
    }

    async fn process(&mut self, item: TeeMeta, bytes: &[u8]) {
        #[cfg(test)]
        assert!(!self.panic_on_item, "test: the tee panics");
        let started = Instant::now();
        #[cfg(test)]
        while (started.elapsed().as_nanos() as u64) < self.spin_ns {
            std::hint::spin_loop();
        }
        stall_tee(self.class, now_unix_us().saturating_sub(item.woken_us));
        match item.kind {
            TeeKind::Admitted {
                forwarded,
                video_kind,
            } => {
                self.process_symbol(&item, bytes, forwarded, video_kind)
                    .await;
            }
            TeeKind::Unchecked => {
                self.process_unchecked(&item, bytes).await;
            }
        }
        self.cache.record_relay_processing(started.elapsed());
    }

    async fn process_unchecked(&mut self, item: &TeeMeta, bytes: &[u8]) {
        // A parent's answer to an ask of this node's (4k-repair).
        if let Some(answer) = Answer::decode(bytes) {
            if let Some(requester) = self.requester.as_mut() {
                requester.on_answer(&answer, item.peer, now_unix_us());
            }
            return;
        }
        if item.path == RelayIngressParentPath::Primary {
            if let Some(socket) = self.reply_socket.as_deref() {
                if self
                    .native_audio_relay
                    .handle_control(socket, item.peer, bytes)
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
            bytes,
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
            bytes,
            &self.cache,
            &mut self.receiver,
            &mut self.dispatch,
            None,
        )
        .await;
    }

    async fn process_symbol(
        &mut self,
        item: &TeeMeta,
        bytes: &[u8],
        forwarded: Option<(SocketAddr, u64)>,
        video_kind: bool,
    ) {
        let peer = item.peer;
        // The symbol's header, read again in place, for the fast lane's
        // top-up (the upstream repair of the stream's objects) and the hole
        // trigger.
        let fast_view = (self.edge_clients.is_some()
            || self.holes.is_some()
            || self.requester.is_some()
            || self.part_repair.is_some()
            || self.dispatch.receiver().defers_decode())
        .then(|| RlsView::parse(bytes, self.limits).ok())
        .flatten();
        // Part repair takes any part of several symbols, whatever its
        // stream (4k-repair: by size, not by media kind).
        let repair_view = fast_view
            .as_ref()
            .filter(|view| view.slim.is_none() && view.source_symbols >= PART_REPAIR_MIN_SYMBOLS);
        let lane = lane_index(item.path);
        self.last_rx_us = self.last_rx_us.max(item.rx_us);
        if let (Some(view), Some(lane)) = (&fast_view, &self.edge_clients) {
            if let Some(stream_id) = view.stream_id() {
                self.upstream_repair
                    .observe(stream_id, view.object, view.esi, view.source_symbols);
                // The compact part that the class thread made (or would
                // have) is kept in its stream's ring here, off that thread.
                lane.keep_compact(stream_id, view);
            }
        }
        let journal_forward = |key: &ObjectKey| {
            if let (Some(forwarder), Some((child, sent_us))) = (&self.forwarder, forwarded) {
                forwarder.journal_forward(key, child, sent_us);
            }
        };
        // The receiver's clocks age on the arrival time: a symbol that
        // waited in the tee is not judged by the wait. A symbol of a part
        // under way here takes the receiver's short path (teeperf): no
        // owned decode, no allocation, and the part's journal and class
        // were done at its first symbol.
        let parsed = fast_view
            .as_ref()
            .and_then(|view| view.parsed_symbol(bytes));
        let outcome = match &parsed {
            Some(symbol) => self.dispatch.push_parsed(peer, bytes, symbol, item.rx_us),
            None => self
                .dispatch
                .push(peer, bytes, item.rx_us)
                .map(|outcome| match outcome {
                    RelayUdpDispatchOutcome::Relay(outcome) => ParsedOutcome::Decoded(outcome),
                    RelayUdpDispatchOutcome::Legacy => ParsedOutcome::Legacy,
                }),
        };
        match outcome {
            Ok(ParsedOutcome::Buffered) => {
                if let (Some(requester), Some(view)) = (self.requester.as_mut(), &fast_view) {
                    match repair_view {
                        Some(view) => requester.observe(view, peer, lane, item.rx_us),
                        None => requester.seen(view, peer, lane, item.rx_us),
                    }
                }
                if let (Some(view), Some((child, sent_us))) = (&fast_view, forwarded) {
                    self.journal_forward_once(view, child, sent_us);
                }
                if self.dispatch.receiver().has_decode_jobs() {
                    let ctx = DecodedCtx {
                        peer,
                        rx_us: item.rx_us,
                        woken_us: item.woken_us,
                        forwarded,
                        video_kind,
                        meta: fast_view.as_ref().map(PartMeta::of),
                        missing: Vec::new(),
                    };
                    self.start_decodes(&ctx).await;
                }
            }
            Ok(ParsedOutcome::Duplicate) => {
                if let (Some(requester), Some(view)) = (self.requester.as_mut(), &fast_view) {
                    requester.observe_done(view, peer, lane, item.rx_us);
                }
                if let (Some(view), Some((child, sent_us))) = (&fast_view, forwarded) {
                    self.journal_forward_once(view, child, sent_us);
                }
            }
            Ok(ParsedOutcome::Decoded(RelayIngressOutcome::Buffered { key, .. })) => {
                if let Some(requester) = self.requester.as_mut() {
                    match (repair_view, &fast_view) {
                        (Some(view), _) => requester.observe(view, peer, lane, item.rx_us),
                        // A part too small for part repair is still a part
                        // of its stream: never a gap (4k-repair review M2).
                        (None, Some(view)) => requester.seen(view, peer, lane, item.rx_us),
                        (None, None) => {}
                    }
                }
                journal_relay_symbol(&self.cache, &key, peer, item.rx_us);
                journal_woken(&self.cache, &key, peer, item.woken_us);
                journal_forward(&key);
                self.mark_class(key.stream(), video_kind);
                if self.dispatch.receiver().has_decode_jobs() {
                    let ctx = DecodedCtx {
                        peer,
                        rx_us: item.rx_us,
                        woken_us: item.woken_us,
                        forwarded,
                        video_kind,
                        meta: fast_view.as_ref().map(PartMeta::of),
                        missing: Vec::new(),
                    };
                    self.start_decodes(&ctx).await;
                }
            }
            Ok(ParsedOutcome::Decoded(RelayIngressOutcome::Decoded {
                object,
                envelope,
                slot,
                parent_count,
                accepted_datagrams,
                ..
            })) => {
                let ctx = DecodedCtx {
                    peer,
                    rx_us: item.rx_us,
                    woken_us: item.woken_us,
                    forwarded,
                    video_kind,
                    meta: fast_view.as_ref().map(PartMeta::of),
                    missing: Vec::new(),
                };
                self.on_decoded(ctx, object, envelope, slot, parent_count, accepted_datagrams)
                    .await;
            }
            Ok(ParsedOutcome::Decoded(RelayIngressOutcome::Duplicate { key, .. })) => {
                if let (Some(requester), Some(view)) = (self.requester.as_mut(), &fast_view) {
                    requester.observe_done(view, peer, lane, item.rx_us);
                }
                journal_forward(&key);
            }
            Ok(ParsedOutcome::Legacy) => {}
            Err(error) => {
                warn_relay_rejection(
                    &self.dispatch,
                    peer,
                    Some(self.class.as_str()),
                    bytes.len(),
                    &error,
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

/// The video sockets' receive buffers, sized to their stream (fix4-video,
/// task 4). A video socket was made with `--udp-socket-buffer-bytes` (8 MiB
/// asked; the kernel books 16 MiB of buffer memory against it, which holds
/// 10-12 MB of 1,390-byte datagrams at their skb truesize): 0.3 s at 247
/// Mbit/s of 4K (37 MB/s on the wire), 0.8-1.1 s at 80. A class thread
/// that does not run longer than that (a node stopped 1.6-8.5 s, Linode
/// wave 9) loses what overflows. Once a second this asks the kernel for
/// `ms` of the busiest second the socket has had, if that is more than it
/// has, up to `max` (the kernel books twice what is asked, so the buffer
/// holds about 1.3 times `ms` of that second at a truesize of 1.5 times the
/// datagram). It never shrinks: a buffer is a bound, not memory, until a
/// stall fills it. Past `net.core.rmem_max` it needs CAP_NET_ADMIN
/// (SO_RCVBUFFORCE), which the deploy gives. Off the class thread, on a
/// timer (`fast_control::run_video_socket_buffers`).
pub(crate) struct VideoSocketBuffers {
    sockets: Vec<(usize, std::net::UdpSocket)>,
    ms: u64,
    floor: usize,
    max: usize,
    asked: [usize; 2],
    last_bytes: [u64; 2],
}

/// What a video socket's receive buffer is asked for: `ms` of
/// `bytes_per_second`, at least `floor` and at most `max` (all as asked of
/// `SO_RCVBUF`, which the kernel doubles).
pub(crate) fn video_socket_buffer_target(
    bytes_per_second: u64,
    ms: u64,
    floor: usize,
    max: usize,
) -> usize {
    let wanted = bytes_per_second.saturating_mul(ms) / 1_000;
    (usize::try_from(wanted).unwrap_or(usize::MAX))
        .clamp(floor.min(max), max.max(floor))
}

impl VideoSocketBuffers {
    fn new(sockets: Vec<(usize, std::net::UdpSocket)>, config: &RelayClassConfig) -> Self {
        Self {
            sockets,
            ms: config.video_socket_buffer_ms,
            floor: config.socket_buffer_bytes,
            max: config.video_socket_buffer_max_bytes,
            asked: [config.socket_buffer_bytes; 2],
            last_bytes: [0; 2],
        }
    }

    /// One second's look: grow each socket whose stream asks for more.
    pub(crate) fn turn(&mut self, stats: &FastPlaneStats, elapsed: Duration) {
        for (lane, socket) in &self.sockets {
            let lane = *lane;
            let bytes = stats.video_rx_bytes[lane].load(Ordering::Relaxed);
            let delta = bytes.saturating_sub(self.last_bytes[lane]);
            self.last_bytes[lane] = bytes;
            let per_second = (delta as f64 / elapsed.as_secs_f64().max(1e-3)) as u64;
            let target = video_socket_buffer_target(per_second, self.ms, self.floor, self.max);
            // The node's sockets share one budget (memory round).
            let target = if self.ms > 0 && target > self.asked[lane] {
                av_mesh::mem_guard::SOCKETS.grow(self.asked[lane], target)
            } else {
                target
            };
            if self.ms > 0 && target > self.asked[lane] {
                let granted = sys::grow_receive_buffer(socket, target);
                info!(
                    lane,
                    bytes_per_second = per_second,
                    asked_bytes = target,
                    granted_bytes = granted,
                    "a video socket's receive buffer grows to its stream"
                );
                self.asked[lane] = target;
            }
            if let Some(granted) = sys::receive_buffer(socket) {
                stats.video_socket_buffer_bytes[lane].store(granted as u64, Ordering::Relaxed);
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
    // Faulted in here: the class threads write their counters.
    let stats = crate::memory::touched_arc(FastPlaneStats::default());
    if let Some(lease) = &config.lease {
        let _ = stats.lease.set(Arc::clone(lease));
    }
    cache.set_fast_plane_stats(Arc::clone(&stats));
    let feeds = crate::memory::touched_arc(ParentFeeds::default());
    cache.set_parent_feeds(Arc::clone(&feeds));
    let admissions = Arc::new(dispatch.peer_admissions());
    let mut demand_peers: Vec<SocketAddr> = admissions
        .keys()
        .copied()
        .filter(|peer| !config.ranked_parents.contains(peer))
        .collect();
    demand_peers.sort_unstable();
    let demand_peers: Arc<[SocketAddr]> = demand_peers.into();
    let limits = dispatch.receiver().config().relay_limits;
    let video_dispatch = dispatch
        .twin()
        .context("failed to copy the relay dispatch")?;
    let snapshots = Arc::new(StdMutex::new([
        dispatch.receiver().snapshot(),
        video_dispatch.receiver().snapshot(),
    ]));
    cache.update_relay_ingress(dispatch.receiver().snapshot());
    let failover = crate::memory::touched_arc(FailoverInputs::new());
    // The epochs that the cache takes up, for the threads' media clocks.
    let epochs = Arc::new(ValidatedEpochs::new(VALIDATED_EPOCH_STREAMS));
    {
        let epochs = Arc::clone(&epochs);
        cache
            .on_source_epoch(Arc::new(move |stream_id, epoch| {
                epochs.set(stream_id, epoch)
            }))
            .await;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let reply_socket = match sockets.iter().find(|socket| {
        socket.class == TrafficClass::Audio && socket.path == RelayIngressParentPath::Primary
    }) {
        Some(socket) => Some(Arc::new(UdpSocket::from_std(socket.socket.try_clone()?)?)),
        None => None,
    };
    let mut by_class: [Vec<(RelayIngressParentPath, std::net::UdpSocket)>; 2] = Default::default();
    // The video sockets again (the same sockets), for their buffers, and
    // for the asks of part repair (4k-repair), which leave from the socket
    // that the parent's symbols come to.
    let mut video_sockets = Vec::new();
    let mut ask_sockets: [[Option<Arc<dyn SendSocket>>; 2]; 2] = Default::default();
    for socket in sockets {
        ask_sockets[socket.class.index()][lane_index(socket.path)] =
            Some(Arc::new(socket.socket.try_clone()?) as Arc<dyn SendSocket>);
        if socket.class == TrafficClass::Video {
            video_sockets.push((lane_index(socket.path), socket.socket.try_clone()?));
        }
        by_class[socket.class.index()].push((socket.path, socket.socket));
    }
    let video_buffers = (!video_sockets.is_empty())
        .then(|| VideoSocketBuffers::new(video_sockets, &config));
    let part_repair = config.part_repair.clone();
    if let Some(pool) = &config.decode_pool {
        let _ = stats.decode_pool.set(Arc::clone(pool));
    }
    if let Some(repair) = &part_repair {
        let _ = stats.part_repair.set(Arc::clone(repair));
        if let Some(forwarder) = &forwarder {
            let _ = forwarder.part_repair.set(Arc::clone(repair));
        }
        if let Some(lease) = &config.lease {
            let _ = lease.part_repair.set(Arc::clone(repair));
        }
    }
    // One tick for each class's tee.
    let repair_ticks: Option<[Arc<Notify>; 2]> = part_repair
        .as_ref()
        .filter(|repair| repair.config.requests)
        .map(|_| [Arc::new(Notify::new()), Arc::new(Notify::new())]);
    // Parts of two symbols or more are assembled by the tees' receivers
    // (4k-repair): whole from their sources with no RaptorQ, a damaged one
    // decoded on the pool.
    let (mut dispatch, mut video_dispatch) = (dispatch, video_dispatch);
    if part_repair.is_some() || config.decode_pool.is_some() {
        dispatch.receiver_mut().set_defer_decode(2);
        video_dispatch.receiver_mut().set_defer_decode(2);
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
        // Made, faulted in and locked here, before the thread runs. The
        // video ring only in part: the first `tee_capacity` entries' worth
        // of bytes (16 MB), and the rest of its `--relay-video-tee-mb` once
        // a video datagram comes, so that a node that carries no video
        // never has it resident or locked (tee_ring's module docs).
        let (tx, rx) = match class {
            TrafficClass::Video => {
                let initial = config.tee_capacity.saturating_mul(tee_ring::BYTES_PER_ENTRY);
                tee_ring_growing(
                    config.tee_capacity,
                    initial,
                    config.video_tee_bytes.max(initial),
                )
            }
            TrafficClass::Audio => tee_ring(config.tee_capacity),
        };
        let thread = ClassThread {
            class,
            sockets: class_sockets,
            admissions: Arc::clone(&admissions),
            limits,
            forwarder: forwarder.clone(),
            link_telemetry: link_telemetry.clone(),
            tee: Some(tx),
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
            demand: (class == TrafficClass::Audio)
                .then(|| config.demand.clone())
                .flatten(),
            demand_peers: Arc::clone(&demand_peers),
            lease_descriptors: config.lease_descriptors,
            video_gro: config.video_gro,
            feeds: Arc::clone(&feeds),
            media_deadline_us: config.media_deadline_us,
            trim_repair: config.trim_forwarded_repair,
            trim_regenerated: config.trim_to_margin,
            epochs: Arc::clone(&epochs),
            #[cfg(test)]
            test_alloc_counter: match class {
                TrafficClass::Audio => config.test_alloc_counter,
                TrafficClass::Video => config.test_video_alloc_counter,
            },
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
        let (decode_tx, decode_rx) = tokio::sync::mpsc::unbounded_channel();
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
            feeds: Arc::clone(&feeds),
            part_repair: part_repair.clone(),
            requester: part_repair
                .as_ref()
                .filter(|repair| repair.config.requests)
                .map(|repair| {
                    Requester::new(
                        Arc::clone(repair),
                        ask_sockets[class.index()].clone(),
                        Arc::clone(&stats),
                        class,
                    )
                }),
            lease: config.lease.clone(),
            repair_tick: repair_ticks
                .as_ref()
                .map(|ticks| Arc::clone(&ticks[class.index()])),
            last_rx_us: 0,
            forward_journaled: (u64::MAX, u64::MAX),
            decodes: config.decode_pool.clone(),
            decode_tx: Some(decode_tx),
            decode_rx: Some(decode_rx),
            #[cfg(test)]
            panic_on_item: config.test_tee_panic == Some(class),
            #[cfg(test)]
            spin_ns: if class == TrafficClass::Video {
                config.test_video_tee_spin_ns
            } else {
                0
            },
        };
        audio_tee |= class == TrafficClass::Audio;
        let nudge = Arc::new(Notify::new());
        expire.push(Arc::clone(&nudge));
        // Each lane's tee on a thread of its own, with its own runtime
        // (hls-beside-4k): on one runtime (one thread with --media-cpu) a
        // large-part lane's backlog held the small-part lane's commits back
        // by seconds. The small-part lane's tee (audio) runs at nice 0, the
        // large-part lane's at +10: the CPU goes to the small parts first.
        #[cfg(test)]
        if config.test_tees_on_caller_runtime {
            tees.spawn(async move { (class, tee.run(rx, nudge).await) });
            continue;
        }
        let (ended_tx, ended_rx) = tokio::sync::oneshot::channel();
        let nice = tee_nice(class);
        let slice_us = if class == TrafficClass::Audio {
            config.small_lane_tee_slice_us
        } else {
            0
        };
        // The tee logs where its caller logs.
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let started = std::thread::Builder::new()
            .name(format!("nt-tee-{}", class.as_str()))
            .spawn(move || {
                let _log = tracing::dispatcher::set_default(&dispatch);
                let _ = sys::set_nice(nice);
                if slice_us > 0 {
                    crate::set_thread_slice(slice_us);
                }
                let result = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime.block_on(tee.run(rx, nudge)),
                    Err(error) => Err(anyhow::Error::new(error).context("no tee runtime")),
                };
                let _ = ended_tx.send(result);
            });
        if let Err(error) = started {
            stop.store(true, Ordering::Relaxed);
            join_threads(threads).await;
            tees.shutdown().await;
            return Err(error).context("failed to start a tee thread");
        }
        tees.spawn(async move {
            let result = ended_rx
                .await
                .unwrap_or_else(|_| Err(anyhow::anyhow!("the tee thread ended")));
            (class, result)
        });
    }
    let housekeeping = tokio::spawn(crate::fast_control::run_tee_housekeeping(
        Arc::clone(&cache),
        audio_tee,
        expire,
        shutdown_rx.clone(),
    ));
    let sampler = tokio::spawn(crate::fast_control::run_parent_feeds_sampler(
        feeds,
        Arc::clone(&stats),
        shutdown_rx.clone(),
    ));
    let repair_ticker = repair_ticks.map(|ticks| {
        tokio::spawn(crate::fast_control::run_part_repair_tick(
            ticks.to_vec(),
            shutdown_rx.clone(),
        ))
    });
    let child_ledgers = part_repair
        .as_ref()
        .filter(|repair| repair.config.link_repair)
        .map(|repair| {
            tokio::spawn(crate::fast_control::run_child_ledgers(
                forwarder.clone(),
                config.lease.clone(),
                Arc::clone(repair),
                shutdown_rx.clone(),
            ))
        });
    let video_buffers = video_buffers.map(|buffers| {
        tokio::spawn(crate::fast_control::run_video_socket_buffers(
            buffers,
            Arc::clone(&stats),
            shutdown_rx.clone(),
        ))
    });
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
        video_gro = config.video_gro,
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
    sampler.abort();
    for task in [&repair_ticker, &child_ledgers].into_iter().flatten() {
        task.abort();
    }
    if let Some(video_buffers) = &video_buffers {
        video_buffers.abort();
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
    let _ = housekeeping.await;
    let _ = sampler.await;
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

#[cfg(test)]
mod two_contributors;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_video_socket_buffer_is_asked_for_seconds_of_its_stream() {
        let (floor, max) = (8 << 20, 128 << 20);
        // 247 Mbit/s of 4K: 37 MB/s on the wire, two seconds of it.
        assert_eq!(video_socket_buffer_target(37_000_000, 2_000, floor, max), 74_000_000);
        // A quiet socket keeps what it was made with.
        assert_eq!(video_socket_buffer_target(1_000_000, 2_000, floor, max), floor);
        // 1 Gbit/s is capped.
        assert_eq!(video_socket_buffer_target(125_000_000, 2_000, floor, max), max);
        assert_eq!(video_socket_buffer_target(u64::MAX, 2_000, floor, max), max);
    }

    /// A turn grows a socket whose stream asks for more, never shrinks it,
    /// and reports what the kernel gave (here within net.core.rmem_max: no
    /// CAP_NET_ADMIN).
    #[test]
    fn a_video_socket_s_buffer_grows_to_its_stream_and_never_shrinks() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let small = 64 * 1024;
        socket2::SockRef::from(&socket)
            .set_recv_buffer_size(small)
            .unwrap();
        let made = sys::receive_buffer(&socket).unwrap();
        let stats = FastPlaneStats::default();
        let config = RelayClassConfig {
            socket_buffer_bytes: small,
            video_socket_buffer_ms: 2_000,
            video_socket_buffer_max_bytes: 1 << 20,
            ..Default::default()
        };
        let mut buffers = VideoSocketBuffers::new(vec![(1, socket.try_clone().unwrap())], &config);
        // 400 kB in a second: 800 kB asked.
        stats.video_rx_bytes[1].store(400_000, Ordering::Relaxed);
        buffers.turn(&stats, Duration::from_secs(1));
        let grown = sys::receive_buffer(&socket).unwrap();
        assert!(grown > made, "{made} -> {grown}");
        assert_eq!(buffers.asked[1], 800_000);
        assert_eq!(
            stats.video_socket_buffer_bytes[1].load(Ordering::Relaxed),
            grown as u64
        );
        // A quiet second asks for nothing less; a busier one for no more
        // than the cap.
        buffers.turn(&stats, Duration::from_secs(1));
        assert_eq!(sys::receive_buffer(&socket).unwrap(), grown);
        stats.video_rx_bytes[1].store(400_000 + 10_000_000, Ordering::Relaxed);
        buffers.turn(&stats, Duration::from_secs(1));
        assert_eq!(buffers.asked[1], 1 << 20);
    }

    #[test]
    fn a_video_part_is_teed_whole_or_not_at_all() {
        let mut parts = TeeParts::new();
        let len = 1_400;
        let (entries, bytes) = tee_part_need(1_000, len);
        let roomy = || (2 * entries, 2 * bytes);
        let tight = || (2 * entries - 1, 2 * bytes);
        // A part's verdict is made at its first symbol and kept.
        assert_eq!(parts.admit(7, 1, 1_000, len, roomy), (true, false));
        assert_eq!(parts.admit(7, 1, 1_000, len, tight), (true, false));
        // A part that finds too little room is not teed, nor are its later
        // symbols when the room comes back.
        assert_eq!(parts.admit(7, 2, 1_000, len, tight), (false, true));
        assert_eq!(parts.admit(7, 2, 1_000, len, roomy), (false, false));
        // Bytes count as entries do; other streams have verdicts of their own.
        assert_eq!(parts.admit(8, 2, 1_000, len, || (2 * entries, bytes)), (false, true));
        assert_eq!(parts.admit(9, 2, 1_000, len, roomy), (true, false));
        // The table holds the last TEE_PARTS parts: the oldest is judged
        // again (as a new part) once that many came after it.
        for object in 100..100 + TEE_PARTS as u64 {
            parts.admit(7, object, 10, len, roomy);
        }
        assert_eq!(parts.admit(7, 2, 1_000, len, roomy), (true, false));
    }

    #[test]
    fn a_lease_descriptor_takes_the_longest_admitted_stream_id() {
        // D3 review 2: the table's key has room for any id the wire admits.
        let mut descriptors = LeaseDescriptors::new();
        let longest = "s".repeat(relay_session::RelayLimits::HARD.max_identifier_bytes);
        assert!(descriptors.entry(&longest, false).is_none());
        let entry = descriptors.entry(&longest, true).expect("an entry");
        entry.len = 7;
        assert_eq!(descriptors.entry(&longest, false).map(|entry| entry.len), Some(7));
    }

    #[test]
    fn a_lane_reports_its_loss_only_while_its_ledgers_are_fresh() {
        let stats = FastPlaneStats::default();
        let lane = stats
            .lane(TrafficClass::Audio, RelayIngressParentPath::Secondary)
            .unwrap();
        assert!(stats
            .lane(TrafficClass::Video, RelayIngressParentPath::Secondary)
            .is_none());
        let peer: SocketAddr = "10.77.1.7:22301".parse().unwrap();
        let now_us = 1_763_924_000_000_000;
        // No ledger yet: nothing measured.
        let snapshot = stats.parent_lane_snapshot(1, now_us);
        assert_eq!(snapshot.lane, ParentLane::Secondary);
        assert_eq!((snapshot.ledger_age_ms, snapshot.loss_ppm), (None, None));
        lane.ledger(peer, now_us);
        lane.loss_ppm.store(5_000, Ordering::Relaxed);
        let snapshot = stats.parent_lane_snapshot(1, now_us + 400_000);
        assert_eq!(snapshot.peer, Some(peer));
        assert_eq!(
            (snapshot.ledger_age_ms, snapshot.loss_ppm),
            (Some(400), Some(5_000))
        );
        // The parent stopped sending ledgers: its last estimate is not loss.
        let snapshot = stats.parent_lane_snapshot(1, now_us + 3_000_000);
        assert_eq!(
            (snapshot.ledger_age_ms, snapshot.loss_ppm),
            (Some(3_000), None)
        );
        let json = serde_json::to_value(snapshot).unwrap();
        assert_eq!(json["lane"], "secondary");
        assert_eq!(json["peer"], "10.77.1.7:22301");
        assert!(json["loss_ppm"].is_null());
    }

    #[test]
    fn a_lanes_lag_is_the_largest_of_the_last_whole_second_or_the_one_under_way() {
        let stats = FastPlaneStats::default();
        let lane = &stats.parent_lanes[0];
        lane.lag_now_us.fetch_max(310, Ordering::Relaxed);
        lane.lag_now_us.fetch_max(40, Ordering::Relaxed);
        assert_eq!(stats.parent_lane_snapshot(0, 0).duplicate_lag_max_us, 310);
        stats.roll_duplicate_lag();
        lane.lag_now_us.fetch_max(20, Ordering::Relaxed);
        assert_eq!(stats.parent_lane_snapshot(0, 0).duplicate_lag_max_us, 310);
        stats.roll_duplicate_lag();
        assert_eq!(stats.parent_lane_snapshot(0, 0).duplicate_lag_max_us, 20);
        stats.roll_duplicate_lag();
        assert_eq!(stats.parent_lane_snapshot(0, 0).duplicate_lag_max_us, 0);
    }

    #[test]
    fn a_socket_drop_counts_in_all_and_for_its_lane() {
        let stats = FastPlaneStats::default();
        let mut video_secondary = 0_u32;
        let mut audio_primary = 0_u32;
        // The counter a socket's datagrams carry: 7 dropped, then 7 again
        // (nothing new), then 10.
        for dropped in [7, 7, 10] {
            record_kernel_drops(
                &stats,
                TrafficClass::Video,
                RelayIngressParentPath::Secondary,
                &mut video_secondary,
                dropped,
            );
        }
        record_kernel_drops(
            &stats,
            TrafficClass::Audio,
            RelayIngressParentPath::Primary,
            &mut audio_primary,
            2,
        );
        // The kernel's counter wraps.
        let mut wrapping = u32::MAX - 1;
        record_kernel_drops(
            &stats,
            TrafficClass::Video,
            RelayIngressParentPath::Primary,
            &mut wrapping,
            1,
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.video_secondary_kernel_dropped, 10);
        assert_eq!(snapshot.audio_primary_kernel_dropped, 2);
        assert_eq!(snapshot.video_primary_kernel_dropped, 3);
        assert_eq!(snapshot.audio_secondary_kernel_dropped, 0);
        assert_eq!(snapshot.kernel_dropped, 15);
        assert_eq!(wrapping, 1);
    }
}

#[cfg(test)]
mod loop_lag_tests {
    use super::*;

    #[test]
    fn loop_passes_and_gaps_are_booked_per_class() {
        let stats = FastPlaneStats::default();
        stats.record_loop(TrafficClass::Audio, 300, None);
        stats.record_loop(TrafficClass::Audio, 6_000, Some(9_000));
        stats.record_loop(TrafficClass::Video, 60_000, Some(70_000));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.audio_loop_busy_max_us, 6_000);
        assert_eq!(
            (
                snapshot.audio_loop_busy_over_1ms,
                snapshot.audio_loop_busy_over_5ms,
                snapshot.audio_loop_busy_over_50ms
            ),
            (1, 1, 0)
        );
        assert_eq!(snapshot.audio_loop_gap_max_us, 9_000);
        assert_eq!(snapshot.video_loop_busy_over_50ms, 1);
        assert_eq!(snapshot.video_loop_gap_max_us, 70_000);
    }
}

#[cfg(test)]
mod stall_tests {
    use super::*;
    use av_mesh::stall_journal::{StallEvent, StallJournal};

    /// The journal of the test process. Other tests' fast-plane threads
    /// record into it too when the machine stalls under them.
    fn journal() -> &'static Arc<StallJournal> {
        stall_journal::install(Arc::new(StallJournal::new("fast-plane-tests", 4_096)))
    }

    #[test]
    fn the_fast_plane_s_lines_reach_the_stall_journal_at_their_thresholds() {
        let journal = journal();
        let stats = FastPlaneStats::default();
        // Durations that no real event has: an hour and more.
        const MARK: u64 = 3_600_000_000;
        stall_pass(&stats, TrafficClass::Audio, MARK + 1);
        stall_gap(&stats, TrafficClass::Video, Some(MARK + 2));
        stall_rx(&stats, TrafficClass::Audio, 0, MARK + 3);
        // Not the oldest datagram of its batch.
        stall_rx(&stats, TrafficClass::Audio, 1, MARK + 4);
        stall_send(&stats, TrafficClass::Video, MARK + 5);
        stall_tee(TrafficClass::Video, MARK + 6);
        stall_tee(TrafficClass::Audio, MARK + 7);
        // Under their thresholds, and a pass with no gap before it.
        stall_pass(&stats, TrafficClass::Audio, stall_journal::CLASS_PASS_US - 1);
        stall_gap(&stats, TrafficClass::Audio, None);
        let events: Vec<StallEvent> = journal
            .query(None)
            .events
            .iter()
            .map(|line| line.event)
            .filter(|event| event.dur_us > MARK)
            .collect();
        let mut marked: Vec<(u64, StallKind, StallThread)> = events
            .iter()
            .map(|event| (event.dur_us - MARK, event.kind, event.thread))
            .collect();
        marked.sort_unstable_by_key(|(mark, _, _)| *mark);
        assert_eq!(
            marked,
            vec![
                (1, StallKind::ClassPass, StallThread::Audio),
                (2, StallKind::ClassGap, StallThread::Video),
                (3, StallKind::RxLate, StallThread::Audio),
                (5, StallKind::SendSlow, StallThread::Video),
                (6, StallKind::TeeLag, StallThread::TeeVideo),
                (7, StallKind::TeeLag, StallThread::TeeAudio),
            ]
        );
        assert!(journal.snapshot().rx_late_max_us >= MARK + 3);
    }

    /// What the journal's lines cost a class thread while nothing stalls:
    /// the lines of one pass that drained a batch of eight datagrams and
    /// sent it to compiled and lease children, as `run`, `drain`,
    /// `send_fast_plan` and `send_lease_plan` hold them.
    #[test]
    fn a_quiet_pass_pays_the_journal_three_clock_readings() {
        journal();
        let stats = FastPlaneStats::default();
        let class = TrafficClass::Audio;
        const PASSES: u32 = 100_000;
        let mut best_ns = f64::MAX;
        for _ in 0..30 {
            let began = Instant::now();
            for _ in 0..PASSES {
                let woke = std::hint::black_box(began);
                for message in 0..8 {
                    stall_rx(&stats, class, message, std::hint::black_box(150));
                }
                stall_send(&stats, class, std::hint::black_box(30));
                let started = Instant::now();
                stall_send(&stats, class, started.elapsed().as_micros() as u64);
                stall_pass(&stats, class, woke.elapsed().as_micros() as u64 % 1_000);
                stall_gap(&stats, class, std::hint::black_box(Some(1_000)));
            }
            best_ns = best_ns.min(began.elapsed().as_nanos() as f64 / f64::from(PASSES));
        }
        eprintln!("the stall journal's lines of a quiet pass of 8 datagrams: {best_ns:.1} ns");
        let bound_ns = if cfg!(debug_assertions) { 5_000.0 } else { 500.0 };
        assert!(best_ns < bound_ns, "{best_ns:.1} ns a pass");
    }
}
