//! The client UDP port of an edge: RaptorQ FEC UDP to clients.
//!
//! See `av_mesh::client_udp` for the protocol. An edge keeps one sender for
//! each stream that has subscribers. The sender follows the stream in the
//! edge's cache as an LL-HLS reader of the newest part does (the same wait
//! for a part, the same repair of a hole from the parents), encodes each part
//! once for each repair policy that its subscribers asked for, and sends it to
//! them. A subscription is reader demand: it subscribes the edge to the
//! playlist at its distributors and keeps the playlist held, as playlist
//! requests do.
//!
//! An edge sends media only to an address that echoed a token that the edge
//! sent to it, so a forged source address gets at most an answer smaller than
//! the request.

use crate::fast_plane::edge_clients::{EdgeFastLane, FastClient, FastGroup};
use crate::fast_plane::{StreamClasses, TrafficClass};
use crate::{AppRouter, ParentsLost};
use av_mesh::client_udp::{
    block_id, cut_through_block_id, cut_through_symbol_size, encode_block, encode_object, geometry,
    source_datagram, Control, ObjectKind, PartObject, RepairPolicy, KEEPALIVE,
    MIN_FIRST_REQUEST_BYTES, SUBSCRIPTION_TTL,
};
use bytes::Bytes;
use playlists::mesh::{chunk_frame_slot, CacheMeshReceiveObserver};
use raptorq_datagram_fec::DatagramFecHeader;
use std::collections::hash_map::RandomState;
use std::collections::BTreeSet;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// How long a new subscription waits for the edge to hold parts of its
/// stream before it is refused.
const ADMIT_WAIT: Duration = Duration::from_secs(4);
/// How often a sender renews the demand for its playlist and checks that
/// the edge still serves.
const HOUSEKEEPING: Duration = Duration::from_secs(1);
/// The longest a sender waits for the next part before it looks after its
/// subscribers (keepalive, newcomers, drain).
const LIVE_WAIT: Duration = Duration::from_millis(250);
/// The longest a sender tries to repair a part that the stream has passed
/// before it sends a gap.
const HOLE_WAIT: Duration = Duration::from_millis(400);
/// Default of --client-udp-send-queue-ms: how long a datagram that found
/// the client socket full waits for it.
pub(crate) const DEFAULT_SEND_QUEUE_MS: u64 = 250;
/// Default of --client-udp-playout-ms: how far behind the newest part a part
/// is still worth sending to a client.
pub(crate) const DEFAULT_PLAYOUT_MS: u64 = 500;
/// Default of --client-udp-move-hold-down-ms.
pub(crate) const DEFAULT_MOVE_HOLD_DOWN_MS: u64 = 3_000;
/// Default of --client-udp-inline-complete-bytes: an audio part, and not a
/// 4K frame, whose encode takes milliseconds.
pub(crate) const DEFAULT_INLINE_COMPLETE_BYTES: usize = 16_384;
/// Most datagrams that the send queues of one socket hold, whatever their
/// age: about 40 MB of full datagrams.
const SEND_QUEUE_MAX_DATAGRAMS: usize = 32_768;

#[derive(Debug, Clone, Copy)]
pub(crate) struct EdgeUdpConfig {
    pub symbol_size: u16,
    pub max_subscribers: usize,
    /// How long a datagram that found the socket full waits in its
    /// client's queue (zero: it is dropped, or the cache-fed sender waits
    /// for the socket, as before).
    pub send_queue: Duration,
    /// Playout deadline of a subscriber, behind the newest part (zero: the
    /// whole window, and HOLE_WAIT for each hole, as before).
    pub playout: Duration,
    /// A subscriber is not moved for lost parents within this of its
    /// admission, and a client moved away for that is taken back within
    /// this of its move (zero: no hold-down).
    pub move_hold_down: Duration,
    /// Hand the datagrams of a part for all its clients to the kernel in
    /// one `sendmmsg` (`false`: one `sendto` each).
    pub batch_send: bool,
    /// A cut-through part of at most this many bytes is completed (its
    /// repair encoded and sent) where it decoded; a larger one on the
    /// blocking pool, as every part was before (zero).
    pub inline_complete_bytes: usize,
    /// A client's datagrams of a completed part leave as one UDP GSO
    /// message (`UDP_SEGMENT`) in the batch, not one message each.
    pub gso: bool,
}

impl EdgeUdpConfig {
    /// The configuration with the defaults of the flags.
    #[cfg(test)]
    pub(crate) fn with_defaults(symbol_size: u16, max_subscribers: usize) -> Self {
        Self {
            symbol_size,
            max_subscribers,
            send_queue: Duration::from_millis(DEFAULT_SEND_QUEUE_MS),
            playout: Duration::from_millis(DEFAULT_PLAYOUT_MS),
            move_hold_down: Duration::from_millis(DEFAULT_MOVE_HOLD_DOWN_MS),
            batch_send: true,
            inline_complete_bytes: DEFAULT_INLINE_COMPLETE_BYTES,
            gso: true,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct EdgeUdpStats {
    pub subscriptions: AtomicU64,
    pub refusals: AtomicU64,
    pub moves: AtomicU64,
    pub parts: AtomicU64,
    pub gaps: AtomicU64,
    pub datagrams: AtomicU64,
    pub bytes: AtomicU64,
    pub send_errors: AtomicU64,
    pub tokens_sent: AtomicU64,
    pub bad_tokens: AtomicU64,
    /// Parts forwarded cut-through, and their datagrams.
    pub cut_through_parts: AtomicU64,
    pub cut_through_datagrams: AtomicU64,
    /// Cut-through parts completed from another parent's block.
    pub cut_through_fallbacks: AtomicU64,
    /// Datagrams not sent because the socket was full.
    pub would_block: AtomicU64,
    /// Cache-mesh datagrams that the cut-through thread could not take in
    /// time (its queue was full): their client symbols go with the part's
    /// completion instead.
    pub cut_through_queue_drops: AtomicU64,
    /// Datagrams that found the socket full and waited in their client's
    /// send queue, and those dropped from it (too old, or the queues full).
    pub send_queued: AtomicU64,
    pub send_queue_drops: AtomicU64,
    /// Parts that subscribers skipped because they were past their playout
    /// deadline (after a move or a backlog), and holes sent as gaps at once
    /// because they were.
    pub playout_skipped_parts: AtomicU64,
    pub holes_past_playout: AtomicU64,
    /// Clients kept (or taken back) within the move hold-down although the
    /// edge had lost its parents.
    pub moves_held_down: AtomicU64,
    /// Times that the edge held its readers because the silence of its
    /// parents was region-wide.
    pub region_silence_held: AtomicU64,
    /// The fast client lane, once armed, whose counters the API reports.
    pub fast_lane: OnceLock<Arc<EdgeFastLane>>,
}

/// The client socket of an edge, or a pair of them on one address: audio
/// streams' datagrams leave from the audio socket (SO_PRIORITY 6, band 0 of
/// the host's prio qdisc), video streams' from the video socket, so a 4K
/// burst never sits ahead of audio in a socket or the host queue. Both read
/// control: SO_REUSEPORT spreads a client's requests over them.
#[derive(Clone)]
pub(crate) struct EdgeSockets {
    audio: Arc<UdpSocket>,
    video: Option<Arc<UdpSocket>>,
    classes: StreamClasses,
    /// The send queues of the sockets, when --client-udp-send-queue-ms is
    /// not zero.
    audio_queue: Option<Arc<SendQueue>>,
    video_queue: Option<Arc<SendQueue>>,
    /// The fast client lane, when the edge's class threads feed one
    /// (`--client-udp-fast-lane`).
    fast_lane: Option<Arc<EdgeFastLane>>,
}

/// Where a stream's datagrams leave from: its socket, and the socket's send
/// queue if there is one.
#[derive(Clone, Copy)]
pub(crate) struct ClientOut<'a> {
    socket: &'a Arc<UdpSocket>,
    queue: Option<&'a Arc<SendQueue>>,
}

/// What became of one datagram for one client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    Sent,
    /// The socket was full: it waits in the client's queue.
    Queued,
    /// The socket was full and there is no queue (or the queue is full).
    Blocked,
    Failed,
}

impl ClientOut<'_> {
    /// Send now, or queue if the socket is full; never waits.
    pub(crate) fn send(
        &self,
        stats: &EdgeUdpStats,
        datagram: &[u8],
        peer: SocketAddr,
    ) -> SendOutcome {
        match self.queue {
            Some(queue) => queue.send(datagram, peer),
            None => try_send_counted(self.socket, stats, datagram, peer),
        }
    }
}

/// The host refused UDP GSO on a client socket: the edge sends without it
/// from then on.
static CLIENT_GSO_REFUSED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

impl ClientOut<'_> {
    /// Send each datagram to its client now, or queue it if the socket is
    /// full; never waits. With `batch`, as many as the socket takes leave
    /// in one `sendmmsg` and the rest as [`Self::send`] would send them;
    /// with `gso` as well, a run of datagrams of one size to one client is
    /// one GSO message of the batch. Returns how many were sent or queued,
    /// and how many found the socket full with nowhere to wait.
    pub(crate) fn send_many(
        &self,
        stats: &EdgeUdpStats,
        batch: bool,
        gso: bool,
        datagrams: &[(&[u8], SocketAddr)],
    ) -> (usize, usize) {
        let mut first = 0;
        // A client with datagrams waiting has the new ones queued behind
        // them: the batch is only for a socket with nothing queued.
        let batch = batch
            && datagrams.len() > 1
            && self
                .queue
                .is_none_or(|queue| queue.waiting.load(Ordering::Acquire) == 0);
        if batch && gso && !CLIENT_GSO_REFUSED.load(Ordering::Relaxed) {
            use std::os::fd::AsRawFd;
            match crate::fast_plane::sys::send_gso_batch(
                self.socket.as_raw_fd(),
                datagrams.len(),
                |index| datagrams[index],
            ) {
                Ok(sent) => {
                    let bytes: usize = datagrams[..sent.datagrams]
                        .iter()
                        .map(|(datagram, _)| datagram.len())
                        .sum();
                    stats
                        .datagrams
                        .fetch_add(sent.datagrams as u64, Ordering::Relaxed);
                    stats.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
                    first = sent.datagrams;
                }
                Err(error) if crate::fast_plane::sys::gso_unsupported(&error) => {
                    CLIENT_GSO_REFUSED.store(true, Ordering::Relaxed);
                    warn!(error = %error, "edge client UDP: no GSO on this host, sending without it");
                }
                Err(_) => {}
            }
        } else if batch {
            if let Ok(sent) =
                crate::fast_path::try_send_batch(self.socket, datagrams.len(), |index| {
                    datagrams[index]
                })
            {
                let bytes: usize = datagrams[..sent]
                    .iter()
                    .map(|(datagram, _)| datagram.len())
                    .sum();
                stats.datagrams.fetch_add(sent as u64, Ordering::Relaxed);
                stats.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
                first = sent;
            }
        }
        let mut delivered = first;
        let mut blocked = 0;
        for (datagram, peer) in &datagrams[first..] {
            match self.send(stats, datagram, *peer) {
                SendOutcome::Sent | SendOutcome::Queued => delivered += 1,
                SendOutcome::Blocked => blocked += 1,
                SendOutcome::Failed => {}
            }
        }
        (delivered, blocked)
    }
}

fn try_send_counted(
    socket: &UdpSocket,
    stats: &EdgeUdpStats,
    datagram: &[u8],
    peer: SocketAddr,
) -> SendOutcome {
    match socket.try_send_to(datagram, peer) {
        Ok(sent) => {
            stats.datagrams.fetch_add(1, Ordering::Relaxed);
            stats.bytes.fetch_add(sent as u64, Ordering::Relaxed);
            SendOutcome::Sent
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            stats.would_block.fetch_add(1, Ordering::Relaxed);
            SendOutcome::Blocked
        }
        Err(error) => {
            stats.send_errors.fetch_add(1, Ordering::Relaxed);
            debug!(%peer, error = %error, "edge client UDP send failed");
            SendOutcome::Failed
        }
    }
}

impl EdgeSockets {
    pub(crate) fn single(socket: UdpSocket) -> Self {
        Self {
            audio: Arc::new(socket),
            video: None,
            classes: StreamClasses::default(),
            audio_queue: None,
            video_queue: None,
            fast_lane: None,
        }
    }

    pub(crate) fn pair(audio: UdpSocket, video: UdpSocket, classes: StreamClasses) -> Self {
        Self {
            audio: Arc::new(audio),
            video: Some(Arc::new(video)),
            classes,
            audio_queue: None,
            video_queue: None,
            fast_lane: None,
        }
    }

    /// Serve `lane`'s clients from these sockets (armed by `run_edge_udp`).
    pub(crate) fn with_fast_lane(mut self, lane: Option<Arc<EdgeFastLane>>) -> Self {
        self.fast_lane = lane;
        self
    }

    pub(crate) fn fast_lane(&self) -> Option<&Arc<EdgeFastLane>> {
        self.fast_lane.as_ref()
    }

    /// The socket of a class: the video socket of a pair, or the one socket.
    pub(crate) fn class_socket(&self, class: TrafficClass) -> &Arc<UdpSocket> {
        match (class, &self.video) {
            (TrafficClass::Video, Some(video)) => video,
            _ => &self.audio,
        }
    }

    /// Give each socket a send queue that holds a datagram for `limit`
    /// (none when zero), with its drain task on the current runtime.
    pub(crate) fn with_send_queues(mut self, limit: Duration, stats: &Arc<EdgeUdpStats>) -> Self {
        if limit.is_zero() {
            return self;
        }
        self.audio_queue = Some(SendQueue::start(
            Arc::clone(&self.audio),
            limit,
            Arc::clone(stats),
        ));
        self.video_queue = self
            .video
            .as_ref()
            .map(|video| SendQueue::start(Arc::clone(video), limit, Arc::clone(stats)));
        self
    }

    /// The socket that a stream's datagrams leave from.
    pub(crate) fn for_stream(&self, stream_id: u64) -> &Arc<UdpSocket> {
        self.out_for_stream(stream_id).socket
    }

    /// The socket and send queue that a stream's datagrams leave from.
    pub(crate) fn out_for_stream(&self, stream_id: u64) -> ClientOut<'_> {
        match (&self.video, self.classes.class(stream_id)) {
            (Some(video), TrafficClass::Video) => ClientOut {
                socket: video,
                queue: self.video_queue.as_ref(),
            },
            _ => ClientOut {
                socket: &self.audio,
                queue: self.audio_queue.as_ref(),
            },
        }
    }
}

// ------------------------------------------------------------------ send queue

/// The datagrams of each client that found its socket full, in order.
///
/// A datagram that the socket cannot take now is never dropped at once and
/// never holds up the sender: it waits in its client's queue, and a drain
/// task sends it when the socket can take it. A client with datagrams
/// waiting has its next ones queued behind them, so its order holds. A
/// datagram older than the limit (a part past any client's playout) is
/// dropped and counted, as is one that finds the queues full.
pub(crate) struct SendQueue {
    socket: Arc<UdpSocket>,
    limit: Duration,
    /// Datagrams waiting, all clients: zero lets a send skip the lock.
    waiting: std::sync::atomic::AtomicUsize,
    clients: Mutex<HashMap<SocketAddr, std::collections::VecDeque<(Instant, Bytes)>>>,
    wake: tokio::sync::Notify,
    stats: Arc<EdgeUdpStats>,
}

impl SendQueue {
    fn start(socket: Arc<UdpSocket>, limit: Duration, stats: Arc<EdgeUdpStats>) -> Arc<Self> {
        let queue = Arc::new(Self {
            socket,
            limit,
            waiting: std::sync::atomic::AtomicUsize::new(0),
            clients: Mutex::new(HashMap::new()),
            wake: tokio::sync::Notify::new(),
            stats,
        });
        tokio::spawn(drain_send_queue(Arc::downgrade(&queue)));
        queue
    }

    pub(crate) fn send(&self, datagram: &[u8], peer: SocketAddr) -> SendOutcome {
        if self.waiting.load(Ordering::Acquire) == 0 {
            match try_send_counted(&self.socket, &self.stats, datagram, peer) {
                SendOutcome::Blocked => {}
                outcome => return outcome,
            }
            return self.enqueue(datagram, peer, true);
        }
        self.enqueue(datagram, peer, false)
    }

    /// Queue behind the client's waiting datagrams; a client with none
    /// tries the socket first (unless it just did).
    fn enqueue(&self, datagram: &[u8], peer: SocketAddr, tried: bool) -> SendOutcome {
        let now = Instant::now();
        let mut clients = self.clients.lock().unwrap();
        let waiting_here = clients.get(&peer).is_some_and(|queue| !queue.is_empty());
        if !waiting_here && !tried {
            match try_send_counted(&self.socket, &self.stats, datagram, peer) {
                SendOutcome::Blocked => {}
                outcome => return outcome,
            }
        }
        if self.waiting.load(Ordering::Acquire) >= SEND_QUEUE_MAX_DATAGRAMS {
            self.stats.send_queue_drops.fetch_add(1, Ordering::Relaxed);
            return SendOutcome::Blocked;
        }
        let queue = clients.entry(peer).or_default();
        // What the limit already condemns goes now, not at the next drain.
        while queue
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > self.limit)
        {
            queue.pop_front();
            self.waiting.fetch_sub(1, Ordering::AcqRel);
            self.stats.send_queue_drops.fetch_add(1, Ordering::Relaxed);
        }
        queue.push_back((now, Bytes::copy_from_slice(datagram)));
        self.waiting.fetch_add(1, Ordering::AcqRel);
        self.stats.send_queued.fetch_add(1, Ordering::Relaxed);
        drop(clients);
        self.wake.notify_one();
        SendOutcome::Queued
    }

    /// Send what the socket takes, a datagram of each client in turn, and
    /// drop what is too old. `true` when the socket filled up before the
    /// queues were empty.
    fn drain(&self) -> bool {
        let mut clients = self.clients.lock().unwrap();
        let now = Instant::now();
        let mut blocked = false;
        while !blocked && self.waiting.load(Ordering::Acquire) > 0 {
            let mut progressed = false;
            for (peer, queue) in clients.iter_mut() {
                let Some((at, datagram)) = queue.pop_front() else {
                    continue;
                };
                if now.duration_since(at) > self.limit {
                    self.waiting.fetch_sub(1, Ordering::AcqRel);
                    self.stats.send_queue_drops.fetch_add(1, Ordering::Relaxed);
                    progressed = true;
                    continue;
                }
                match try_send_counted(&self.socket, &self.stats, &datagram, *peer) {
                    SendOutcome::Blocked => {
                        // Not a would-block of a new datagram: undo the count.
                        self.stats.would_block.fetch_sub(1, Ordering::Relaxed);
                        queue.push_front((at, datagram));
                        blocked = true;
                        break;
                    }
                    _ => {
                        self.waiting.fetch_sub(1, Ordering::AcqRel);
                        progressed = true;
                    }
                }
            }
            if !progressed {
                break;
            }
        }
        clients.retain(|_, queue| !queue.is_empty());
        blocked
    }
}

/// Drain a send queue whenever it holds datagrams and its socket can take
/// one. Ends when the queue is dropped.
async fn drain_send_queue(queue: std::sync::Weak<SendQueue>) {
    loop {
        let Some(strong) = queue.upgrade() else {
            return;
        };
        if strong.waiting.load(Ordering::Acquire) == 0 {
            // Wake on a queued datagram; look again now and then so that a
            // dropped queue ends this task.
            let _ = tokio::time::timeout(Duration::from_secs(1), strong.wake.notified()).await;
            continue;
        }
        if strong.drain() {
            // The socket is full: wait until it can take a datagram.
            let socket = Arc::clone(&strong.socket);
            drop(strong);
            let _ = tokio::time::timeout(Duration::from_millis(50), socket.writable()).await;
        }
    }
}

async fn recv_optional(
    socket: Option<&Arc<UdpSocket>>,
    buffer: &mut [u8],
) -> std::io::Result<(usize, SocketAddr)> {
    match socket {
        Some(socket) => socket.recv_from(buffer).await,
        None => std::future::pending().await,
    }
}

#[derive(Debug)]
pub(crate) struct Subscriber {
    repair: RepairPolicy,
    expires_at: Instant,
    /// The next part that this subscriber needs.
    next_part: u64,
    /// The init segment last sent to it.
    init: Option<Bytes>,
    /// Parts that cut-through carries to it: the cache sender skips them.
    cut_slots: BTreeSet<u64>,
    /// When this edge took it.
    admitted_at: Instant,
    /// Its place on the fast lane, if it asked for it (`F1`) and the edge
    /// has one.
    fast: Option<Arc<FastClient>>,
}

#[derive(Debug, Default)]
pub(crate) struct StreamEntry {
    subscribers: HashMap<SocketAddr, Subscriber>,
    /// A sender task runs for the stream.
    running: bool,
}

pub(crate) type Table = Arc<Mutex<HashMap<u64, StreamEntry>>>;

/// The fast-lane clients of a stream, by repair policy (for the snapshot
/// that the class threads read).
pub(crate) fn fast_groups(entry: &StreamEntry) -> Vec<FastGroup> {
    let mut groups: Vec<FastGroup> = Vec::new();
    for (peer, subscriber) in &entry.subscribers {
        let Some(fast) = &subscriber.fast else {
            continue;
        };
        match groups
            .iter_mut()
            .find(|group| group.repair == subscriber.repair)
        {
            Some(group) => group.clients.push((*peer, Arc::clone(fast))),
            None => groups.push(FastGroup {
                repair: subscriber.repair,
                clients: vec![(*peer, Arc::clone(fast))],
            }),
        }
    }
    // A stable order, so that the rotation by part means something.
    for group in &mut groups {
        group.clients.sort_by_key(|(peer, _)| *peer);
    }
    groups
}

/// Rebuild the fast lane's snapshot after the table changed, under its lock.
fn fast_lane_changed(sockets: &EdgeSockets, table: &HashMap<u64, StreamEntry>) {
    if let Some(lane) = sockets.fast_lane() {
        lane.rebuild_locked(table);
    }
}
/// Clients that this edge sent away because it had lost its parents, and
/// when: one that comes back within the move hold-down is taken.
type MovedAway = Arc<Mutex<HashMap<(SocketAddr, u64), Instant>>>;
/// Subscriptions that are being admitted: a client repeats its request
/// until it is answered, and the repeats must not admit it twice.
type Admitting = Arc<Mutex<HashSet<(SocketAddr, u64)>>>;

/// Tokens that show that a client receives at its address: a keyed hash of
/// the address and the minute, valid for the minute and the next.
struct Tokens(RandomState);

impl Tokens {
    fn minute() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs() / 60)
    }

    fn token(&self, peer: SocketAddr, minute: u64) -> u64 {
        self.0.hash_one((peer, minute))
    }

    fn current(&self, peer: SocketAddr) -> u64 {
        self.token(peer, Self::minute())
    }

    fn valid(&self, peer: SocketAddr, token: u64) -> bool {
        let minute = Self::minute();
        token == self.token(peer, minute) || token == self.token(peer, minute.saturating_sub(1))
    }
}

fn subscriber_count(table: &Table) -> usize {
    table
        .lock()
        .unwrap()
        .values()
        .map(|entry| entry.subscribers.len())
        .sum()
}

pub(crate) async fn run_edge_udp(
    sockets: EdgeSockets,
    router: AppRouter,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    cut_through: Vec<Arc<CutThrough>>,
    mut shutdown_rx: watch::Receiver<()>,
) -> anyhow::Result<()> {
    let sockets = sockets.with_send_queues(config.send_queue, &stats);
    let table: Table = Arc::default();
    let moved: MovedAway = Arc::default();
    if let Some(lane) = sockets.fast_lane() {
        let _ = stats.fast_lane.set(Arc::clone(lane));
        // The lane's own copy of the sockets does not hold the lane.
        lane.arm(
            sockets.clone().with_fast_lane(None),
            Arc::clone(&table),
            Arc::clone(&stats),
        );
    }
    for cut in &cut_through {
        cut.arm(
            sockets.clone(),
            Arc::clone(&table),
            config,
            Arc::clone(&stats),
        );
    }
    let admitting: Admitting = Arc::default();
    let tokens = Tokens(RandomState::new());
    let mut audio_buf = vec![0_u8; 2_048];
    let mut video_buf = vec![0_u8; 2_048];
    info!(
        bind = %sockets.audio.local_addr()?,
        class_sockets = sockets.video.is_some(),
        "edge client UDP (RaptorQ FEC) listening"
    );
    loop {
        let (len, peer, from_video) = tokio::select! {
            _ = shutdown_rx.changed() => return Ok(()),
            received = sockets.audio.recv_from(&mut audio_buf) => match received {
                Ok((len, peer)) => (len, peer, false),
                Err(error) => {
                    // An ICMP port unreachable of a departed client surfaces
                    // here on some kernels; it is not fatal.
                    debug!(error = %error, "edge client UDP receive error");
                    continue;
                }
            },
            received = recv_optional(sockets.video.as_ref(), &mut video_buf) => match received {
                Ok((len, peer)) => (len, peer, true),
                Err(error) => {
                    debug!(error = %error, "edge client UDP receive error");
                    continue;
                }
            },
        };
        // Answer from the socket the request came to; both have the
        // client's address.
        let (socket, request) = if from_video {
            (
                sockets.video.as_ref().expect("received on it"),
                &video_buf[..len],
            )
        } else {
            (&sockets.audio, &audio_buf[..len])
        };
        let socket = Arc::clone(socket);
        let Some(control) = Control::decode(request) else {
            continue;
        };
        match control {
            Control::Subscribe {
                stream_id,
                from_part,
                repair,
                token,
                fast_lane,
            } => {
                let valid = token.is_some_and(|token| tokens.valid(peer, token));
                if !valid {
                    if token.is_some() {
                        stats.bad_tokens.fetch_add(1, Ordering::Relaxed);
                    }
                    // Answer only a request at least as large as the answer.
                    if len >= MIN_FIRST_REQUEST_BYTES || token.is_some() {
                        stats.tokens_sent.fetch_add(1, Ordering::Relaxed);
                        send_control(
                            &socket,
                            peer,
                            &Control::Token {
                                stream_id,
                                token: tokens.current(peer),
                            },
                        )
                        .await;
                    }
                    continue;
                }
                let repair = repair.bounded();
                let window = router.cache.window_parts_for(stream_id).await as u64;
                let head = head_part(&router, stream_id).await;
                let refreshed = {
                    let mut table = table.lock().unwrap();
                    let refreshed = match table
                        .get_mut(&stream_id)
                        .and_then(|entry| entry.subscribers.get_mut(&peer))
                    {
                        Some(subscriber) => {
                            subscriber.expires_at = Instant::now() + SUBSCRIPTION_TTL;
                            // A fast-lane client whose repair changed moves
                            // to the group of its new policy.
                            let regroup = subscriber.fast.is_some() && subscriber.repair != repair;
                            subscriber.repair = repair;
                            // A client that names a part has not yet heard
                            // the edge's answer: it resumes there.
                            if let (Some(part), Some(head)) = (from_part, head) {
                                if part < subscriber.next_part && head.saturating_sub(part) < window
                                {
                                    subscriber.next_part = part;
                                }
                            }
                            Some((subscriber.next_part, regroup))
                        }
                        None => None,
                    };
                    if refreshed.is_some_and(|(_, regroup)| regroup) {
                        fast_lane_changed(&sockets, &table);
                    }
                    refreshed.map(|(next_part, _)| next_part)
                };
                if let Some(next_part) = refreshed {
                    send_control(
                        &socket,
                        peer,
                        &Control::Subscribed {
                            stream_id,
                            next_part,
                        },
                    )
                    .await;
                    continue;
                }
                if !admitting.lock().unwrap().insert((peer, stream_id)) {
                    continue;
                }
                let pending = admitting.lock().unwrap().len();
                if subscriber_count(&table) + pending > config.max_subscribers {
                    admitting.lock().unwrap().remove(&(peer, stream_id));
                    stats.refusals.fetch_add(1, Ordering::Relaxed);
                    send_control(
                        &socket,
                        peer,
                        &Control::Refused {
                            stream_id,
                            reason: "edge-full".into(),
                        },
                    )
                    .await;
                    continue;
                }
                let admission = admit(
                    Arc::clone(&socket),
                    sockets.clone(),
                    router.clone(),
                    Arc::clone(&table),
                    Arc::clone(&moved),
                    config,
                    Arc::clone(&stats),
                    peer,
                    stream_id,
                    from_part,
                    repair,
                    fast_lane,
                );
                let admitting = Arc::clone(&admitting);
                tokio::spawn(async move {
                    admission.await;
                    admitting.lock().unwrap().remove(&(peer, stream_id));
                });
            }
            Control::Unsubscribe { stream_id, token } => {
                if !tokens.valid(peer, token) {
                    stats.bad_tokens.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let mut table = table.lock().unwrap();
                let removed = table
                    .get_mut(&stream_id)
                    .and_then(|entry| entry.subscribers.remove(&peer));
                if removed.is_some_and(|subscriber| subscriber.fast.is_some()) {
                    fast_lane_changed(&sockets, &table);
                }
            }
            _ => {}
        }
    }
}

async fn send_control(socket: &UdpSocket, peer: SocketAddr, control: &Control) {
    if let Err(error) = socket.send_to(&control.encode(), peer).await {
        debug!(%peer, error = %error, "edge client UDP control send failed");
    }
}

/// Why this edge sends its clients elsewhere, if it does.
async fn move_reason(router: &AppRouter) -> Option<&'static str> {
    if router.lifecycle.is_draining().await {
        return Some("draining");
    }
    if router.parents_are_lost().await {
        return Some("parents-lost");
    }
    None
}

async fn head_part(router: &AppRouter, stream_id: u64) -> Option<u64> {
    router
        .cache
        .stream_position_for_id(stream_id)
        .await
        .map(|(_, last)| last as u64)
}

/// Parts of a stream within the playout deadline of a subscriber, or
/// `None` when there is no deadline (--client-udp-playout-ms 0).
async fn playout_parts(router: &AppRouter, config: EdgeUdpConfig, stream_id: u64) -> Option<u64> {
    if config.playout.is_zero() {
        return None;
    }
    let part_us = router.cache.state.read().await.stream_part_us(stream_id);
    Some(playout_parts_of(config.playout, part_us))
}

fn playout_parts_of(playout: Duration, part_us: u64) -> u64 {
    (u64::try_from(playout.as_micros()).unwrap_or(u64::MAX) / part_us.max(1)).max(1)
}

/// The part that a new subscriber starts at. A client that moves from
/// another edge names the part that it needs next: it resumes there if the
/// part is still in the window, and otherwise at the newest part. A part
/// past the playout deadline is not worth sending: it resumes at the oldest
/// part within it.
fn first_part_for(
    from_part: Option<u64>,
    head: u64,
    window: u64,
    playout_parts: Option<u64>,
) -> u64 {
    match from_part {
        Some(part) if part <= head.saturating_add(1) && head.saturating_sub(part) < window => {
            playout_parts.map_or(part, |parts| part.max(head.saturating_sub(parts)))
        }
        _ => head,
    }
}

/// How long a hole at `part` may be repaired before its playout deadline
/// passes; `None` without a deadline (HOLE_WAIT then). Zero: send the gap.
fn hole_budget(head: Option<u64>, part: u64, part_us: u64, playout: Duration) -> Option<Duration> {
    if playout.is_zero() {
        return None;
    }
    let behind = head.map_or(0, |head| head.saturating_sub(part));
    let age = Duration::from_micros(behind.saturating_mul(part_us));
    Some(playout.saturating_sub(age).min(HOLE_WAIT))
}

/// Whether the edge takes a client back that it sent away for lost parents
/// within the hold-down (and forgets the moves that are older).
fn moved_back_within_hold_down(
    moved: &MovedAway,
    peer: SocketAddr,
    stream_id: u64,
    hold_down: Duration,
    now: Instant,
) -> bool {
    if hold_down.is_zero() {
        return false;
    }
    let mut moved = moved.lock().unwrap();
    moved.retain(|_, at| now.duration_since(*at) < hold_down);
    moved.contains_key(&(peer, stream_id))
}

#[allow(clippy::too_many_arguments)]
async fn admit(
    socket: Arc<UdpSocket>,
    sockets: EdgeSockets,
    router: AppRouter,
    table: Table,
    moved: MovedAway,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    peer: SocketAddr,
    stream_id: u64,
    from_part: Option<u64>,
    repair: RepairPolicy,
    fast_lane: bool,
) {
    let reason = match move_reason(&router).await {
        // A client sent away for lost parents that comes back at once has
        // found no better edge: it is taken, not sent away again.
        Some("parents-lost")
            if moved_back_within_hold_down(
                &moved,
                peer,
                stream_id,
                config.move_hold_down,
                Instant::now(),
            ) =>
        {
            stats.moves_held_down.fetch_add(1, Ordering::Relaxed);
            None
        }
        reason => reason,
    };
    if let Some(reason) = reason {
        stats.moves.fetch_add(1, Ordering::Relaxed);
        send_control(
            &socket,
            peer,
            &Control::Move {
                stream_id,
                reason: reason.into(),
            },
        )
        .await;
        return;
    }
    // The same demand as the first playlist request of a reader.
    if !router.ensure_playlist_copy(stream_id).await {
        stats.refusals.fetch_add(1, Ordering::Relaxed);
        send_control(
            &socket,
            peer,
            &Control::Refused {
                stream_id,
                reason: "no-playlist-slot".into(),
            },
        )
        .await;
        return;
    }
    let deadline = Instant::now() + ADMIT_WAIT;
    let head = loop {
        if let Some(head) = head_part(&router, stream_id).await {
            break Some(head);
        }
        if Instant::now() >= deadline {
            break None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let Some(head) = head else {
        stats.refusals.fetch_add(1, Ordering::Relaxed);
        send_control(
            &socket,
            peer,
            &Control::Refused {
                stream_id,
                reason: "no-such-stream".into(),
            },
        )
        .await;
        return;
    };
    let window = router.cache.window_parts_for(stream_id).await as u64;
    let playout = playout_parts(&router, config, stream_id).await;
    let first_part = first_part_for(from_part, head, window, playout);
    if let Some(part) = from_part.filter(|part| head.saturating_sub(*part) < window) {
        if first_part > part {
            stats
                .playout_skipped_parts
                .fetch_add(first_part - part, Ordering::Relaxed);
        }
    }
    // A client that asked for the fast lane takes it from its first part:
    // the threads send it each part from there on that they forward after
    // the snapshot below, and the cache-fed lane every part that they do not
    // (older parts, a part already on its way).
    let fast = (fast_lane && sockets.fast_lane().is_some_and(|lane| lane.is_armed()))
        .then(|| FastClient::new(first_part));
    let start_sender = {
        let mut table = table.lock().unwrap();
        let entry = table.entry(stream_id).or_default();
        entry.subscribers.insert(
            peer,
            Subscriber {
                repair,
                expires_at: Instant::now() + SUBSCRIPTION_TTL,
                next_part: first_part,
                init: None,
                cut_slots: BTreeSet::new(),
                admitted_at: Instant::now(),
                fast: fast.clone(),
            },
        );
        let start = !std::mem::replace(&mut entry.running, true);
        // In the threads' snapshot before the client hears OK.
        if fast.is_some() {
            fast_lane_changed(&sockets, &table);
        }
        start
    };
    stats.subscriptions.fetch_add(1, Ordering::Relaxed);
    send_control(
        &socket,
        peer,
        &Control::Subscribed {
            stream_id,
            next_part: first_part,
        },
    )
    .await;
    info!(
        %peer,
        stream_id,
        first_part,
        head,
        fast_lane = fast.is_some(),
        "edge client UDP subscription"
    );
    if start_sender {
        tokio::spawn(run_stream_sender(
            sockets, router, table, moved, config, stats, stream_id,
        ));
    }
}

/// Send one object to each target, encoded once for each repair policy that
/// the targets asked for: one subscriber on a lossy path does not raise the
/// repair that the others receive.
async fn send_object(
    out: ClientOut<'_>,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    object: &PartObject,
    targets: &[(SocketAddr, RepairPolicy)],
) {
    let mut groups: HashMap<RepairPolicy, Vec<SocketAddr>> = HashMap::new();
    for (peer, repair) in targets {
        groups.entry(*repair).or_default().push(*peer);
    }
    for (repair, peers) in groups {
        let datagrams = match encode_object(object, config.symbol_size, repair) {
            Ok(datagrams) => datagrams,
            Err(error) => {
                warn!(stream_id = object.stream_id, part = object.part, error = %error,
                      "edge client UDP could not encode a part");
                return;
            }
        };
        // Interleave the subscribers, so that one of them does not wait for
        // the whole of a large part to be sent to the others.
        if config.batch_send && out.queue.is_some() {
            let batch: Vec<(&[u8], SocketAddr)> = datagrams
                .iter()
                .flat_map(|datagram| peers.iter().map(move |peer| (datagram.as_slice(), *peer)))
                .collect();
            out.send_many(stats, true, false, &batch);
            continue;
        }
        for datagram in &datagrams {
            for peer in &peers {
                if out.queue.is_some() {
                    // A full socket queues the datagram for this client and
                    // does not hold up the others.
                    out.send(stats, datagram, *peer);
                    continue;
                }
                match out.socket.send_to(datagram, *peer).await {
                    Ok(sent) => {
                        stats.datagrams.fetch_add(1, Ordering::Relaxed);
                        stats.bytes.fetch_add(sent as u64, Ordering::Relaxed);
                    }
                    Err(error) => {
                        stats.send_errors.fetch_add(1, Ordering::Relaxed);
                        debug!(%peer, error = %error, "edge client UDP send failed");
                    }
                }
            }
        }
    }
}

async fn part_object(router: &AppRouter, stream_id: u64, part: u64, bytes: Bytes) -> PartObject {
    let times = router.cache.part_times_unix_us(stream_id, part).await;
    PartObject {
        kind: ObjectKind::Part,
        datagrams: 0,
        stream_id,
        part,
        available_unix_us: times.as_ref().map_or(0, |times| times.available_unix_us),
        published_unix_us: times.and_then(|times| times.published_unix_us).unwrap_or(0),
        payload: bytes,
    }
}

fn gap_object(stream_id: u64, part: u64) -> PartObject {
    PartObject {
        kind: ObjectKind::Gap,
        datagrams: 0,
        stream_id,
        part,
        available_unix_us: 0,
        published_unix_us: 0,
        payload: Bytes::new(),
    }
}

/// Send the init segment of an fMP4 stream to each target that does not
/// have this one yet.
#[allow(clippy::too_many_arguments)]
async fn send_init_if_new(
    out: ClientOut<'_>,
    router: &AppRouter,
    table: &Table,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    stream_id: u64,
    part: u64,
    targets: &[(SocketAddr, RepairPolicy)],
) {
    let Some(init) = router.cache.get_init_for_stream_id(stream_id).await else {
        return;
    };
    let needing: Vec<(SocketAddr, RepairPolicy)> = {
        let mut table = table.lock().unwrap();
        let Some(entry) = table.get_mut(&stream_id) else {
            return;
        };
        targets
            .iter()
            .filter(|(peer, _)| {
                entry.subscribers.get_mut(peer).is_some_and(|subscriber| {
                    if subscriber.init.as_ref() == Some(&init) {
                        false
                    } else {
                        subscriber.init = Some(init.clone());
                        true
                    }
                })
            })
            .copied()
            .collect()
    };
    if needing.is_empty() {
        return;
    }
    let object = PartObject {
        kind: ObjectKind::Init,
        datagrams: 0,
        stream_id,
        part,
        available_unix_us: 0,
        published_unix_us: 0,
        payload: init,
    };
    send_object(out, config, stats, &object, &needing).await;
}

/// Subscribers to send an object to, with their repair.
type Targets = Vec<(SocketAddr, RepairPolicy)>;

/// The subscribers due for `part`, after the part has been taken for them,
/// and those due that the fast lane carries it to (they need only its init
/// segment). A subscriber that cut-through carries the part to is passed
/// over, unless `gap`: a gap goes to every subscriber due that the fast lane
/// did not carry the part to.
fn take_due(table: &Table, stream_id: u64, part: u64, gap: bool) -> (Targets, Targets) {
    let mut table = table.lock().unwrap();
    let Some(entry) = table.get_mut(&stream_id) else {
        return (Vec::new(), Vec::new());
    };
    let mut due = Vec::new();
    let mut on_fast_lane = Vec::new();
    for (peer, subscriber) in entry
        .subscribers
        .iter_mut()
        .filter(|(_, subscriber)| subscriber.next_part == part)
    {
        subscriber.next_part = part + 1;
        let cut = subscriber.cut_slots.remove(&part);
        while subscriber
            .cut_slots
            .first()
            .is_some_and(|slot| *slot < subscriber.next_part)
        {
            subscriber.cut_slots.pop_first();
        }
        if subscriber
            .fast
            .as_ref()
            .is_some_and(|fast| !fast.claim_cache(part))
        {
            on_fast_lane.push((*peer, subscriber.repair));
            continue;
        }
        if !cut || gap {
            due.push((*peer, subscriber.repair));
        }
    }
    (due, on_fast_lane)
}

#[allow(clippy::too_many_arguments)]
async fn deliver(
    out: ClientOut<'_>,
    router: &AppRouter,
    table: &Table,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    stream_id: u64,
    part: u64,
    bytes: Option<Bytes>,
) {
    let (targets, on_fast_lane) = take_due(table, stream_id, part, bytes.is_none());
    if bytes.is_some() && !on_fast_lane.is_empty() {
        // The fast lane carries parts, never the init segment: it goes on
        // this lane, in space 2.
        send_init_if_new(
            out,
            router,
            table,
            config,
            stats,
            stream_id,
            part,
            &on_fast_lane,
        )
        .await;
    }
    if targets.is_empty() {
        return;
    }
    let object = match bytes {
        Some(bytes) => {
            send_init_if_new(out, router, table, config, stats, stream_id, part, &targets).await;
            stats.parts.fetch_add(1, Ordering::Relaxed);
            part_object(router, stream_id, part, bytes).await
        }
        None => {
            stats.gaps.fetch_add(1, Ordering::Relaxed);
            gap_object(stream_id, part)
        }
    };
    send_object(out, config, stats, &object, &targets).await;
}

async fn run_stream_sender(
    sockets: EdgeSockets,
    router: AppRouter,
    table: Table,
    moved: MovedAway,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    stream_id: u64,
) {
    let mut housekeeping_at = Instant::now();
    let mut last_sent = Instant::now();
    // The edge has lost its parents but keeps subscribers within the move
    // hold-down: it serves them from its cache alone, without repair.
    let mut held_while_lost = false;
    loop {
        let now = Instant::now();
        // The stream's class can become known after it started.
        let socket = Arc::clone(sockets.for_stream(stream_id));
        let out = sockets.out_for_stream(stream_id);
        let head = head_part(&router, stream_id).await;
        let window = router.cache.window_parts_for(stream_id).await as u64;
        let playout = playout_parts(&router, config, stream_id).await;
        // Subscribers that stopped refreshing end; with none left, so does
        // the sender. A subscriber whose next part has left the window
        // continues at the newest part, and one whose next part is past its
        // playout deadline (after a move, or a backlog) at the oldest part
        // within it.
        let (oldest_next, newest_next, peers) = {
            let mut guard = table.lock().unwrap();
            let Some(entry) = guard.get_mut(&stream_id) else {
                return;
            };
            let mut fast_expired = false;
            entry.subscribers.retain(|_, subscriber| {
                let live = subscriber.expires_at > now;
                fast_expired |= !live && subscriber.fast.is_some();
                live
            });
            let empty = entry.subscribers.is_empty();
            if empty {
                guard.remove(&stream_id);
            }
            if fast_expired {
                fast_lane_changed(&sockets, &guard);
            }
            if empty {
                debug!(stream_id, "edge client UDP sender stops: no subscriber");
                return;
            }
            let Some(entry) = guard.get_mut(&stream_id) else {
                return;
            };
            if let Some(head) = head {
                for subscriber in entry.subscribers.values_mut() {
                    if head.saturating_sub(subscriber.next_part) >= window {
                        subscriber.next_part = head;
                    } else if let Some(parts) = playout {
                        let floor = head.saturating_sub(parts);
                        if subscriber.next_part < floor {
                            stats
                                .playout_skipped_parts
                                .fetch_add(floor - subscriber.next_part, Ordering::Relaxed);
                            subscriber.next_part = floor;
                        }
                    }
                }
            }
            let nexts = entry
                .subscribers
                .values()
                .map(|subscriber| subscriber.next_part);
            (
                nexts.clone().min().unwrap_or(0),
                nexts.max().unwrap_or(0),
                entry.subscribers.keys().copied().collect::<Vec<_>>(),
            )
        };

        if now >= housekeeping_at {
            housekeeping_at = now + HOUSEKEEPING;
            held_while_lost = false;
            if let Some(reason) = move_reason(&router).await {
                // Lost parents do not move a subscriber taken within the
                // hold-down: it may have just come from an edge that lost
                // its parents too. Draining moves every subscriber.
                let hold_down = if reason == "parents-lost" {
                    config.move_hold_down
                } else {
                    Duration::ZERO
                };
                let (leaving, kept) = {
                    let mut guard = table.lock().unwrap();
                    match guard.get_mut(&stream_id) {
                        Some(entry) => {
                            let leaving = take_movable(entry, hold_down, now);
                            let kept = entry.subscribers.len();
                            if kept == 0 {
                                guard.remove(&stream_id);
                            }
                            if !leaving.is_empty() {
                                fast_lane_changed(&sockets, &guard);
                            }
                            (leaving, kept)
                        }
                        None => (Vec::new(), 0),
                    }
                };
                if kept > 0 {
                    stats
                        .moves_held_down
                        .fetch_add(kept as u64, Ordering::Relaxed);
                }
                if !leaving.is_empty() {
                    stats
                        .moves
                        .fetch_add(leaving.len() as u64, Ordering::Relaxed);
                    info!(
                        stream_id,
                        reason,
                        clients = leaving.len(),
                        kept,
                        "edge client UDP moves its clients"
                    );
                    if !hold_down.is_zero() {
                        let mut moved = moved.lock().unwrap();
                        for peer in &leaving {
                            moved.insert((*peer, stream_id), now);
                        }
                    }
                }
                for peer in leaving {
                    send_control(
                        &socket,
                        peer,
                        &Control::Move {
                            stream_id,
                            reason: reason.into(),
                        },
                    )
                    .await;
                }
                if kept == 0 {
                    return;
                }
                held_while_lost = true;
                continue;
            }
            // Keep the playlist held, as a reader's playlist reloads do.
            router.ensure_playlist_copy(stream_id).await;
            router
                .request_replica_for_stream(stream_id, "udp-client-demand", None)
                .await;
        }

        let part = oldest_next;
        let behind_head = head.is_some_and(|head| part <= head);
        if behind_head || part < newest_next {
            // A part that the stream has passed: send it from the cache, or
            // repair it from the parents while it is within the playout
            // deadline (HOLE_WAIT without one), or send a gap.
            let bytes = match router.cache.get_part_for_stream_id(stream_id, part).await {
                Some((bytes, _)) => Some(bytes),
                None if held_while_lost => None,
                None => {
                    let part_us = router.cache.state.read().await.stream_part_us(stream_id);
                    let wait =
                        hole_budget(head, part, part_us, config.playout).unwrap_or(HOLE_WAIT);
                    if wait.is_zero() {
                        stats.holes_past_playout.fetch_add(1, Ordering::Relaxed);
                        None
                    } else {
                        match tokio::time::timeout(
                            wait,
                            router.get_part_repairing_holes(stream_id, part),
                        )
                        .await
                        {
                            Ok(Ok(Some((bytes, _)))) => Some(bytes),
                            Ok(Err(ParentsLost)) => {
                                housekeeping_at = Instant::now();
                                continue;
                            }
                            _ => None,
                        }
                    }
                }
            };
            deliver(out, &router, &table, config, &stats, stream_id, part, bytes).await;
            last_sent = Instant::now();
            continue;
        }

        // The newest part: wait for it as an LL-HLS part request does, for a
        // short while at a time.
        let waited = if held_while_lost {
            Ok(Ok(router
                .cache
                .get_part_until_for_stream_id(stream_id, part, Instant::now() + LIVE_WAIT)
                .await))
        } else {
            tokio::time::timeout(LIVE_WAIT, router.get_part_repairing_holes(stream_id, part)).await
        };
        match waited {
            Ok(Ok(Some((bytes, _)))) => {
                deliver(
                    out,
                    &router,
                    &table,
                    config,
                    &stats,
                    stream_id,
                    part,
                    Some(bytes),
                )
                .await;
                last_sent = Instant::now();
            }
            Ok(Err(ParentsLost)) => {
                // Housekeeping sends the clients away.
                housekeeping_at = Instant::now();
            }
            Ok(Ok(None)) | Err(_) => {
                if last_sent.elapsed() >= KEEPALIVE {
                    // A quiet stream is not a dead edge.
                    for peer in &peers {
                        send_control(
                            &socket,
                            *peer,
                            &Control::Waiting {
                                stream_id,
                                next_part: part,
                            },
                        )
                        .await;
                    }
                    last_sent = Instant::now();
                }
            }
        }
    }
}

/// Take the subscribers that may be moved: all of them, or with a
/// hold-down those that the edge took at least that long ago.
fn take_movable(entry: &mut StreamEntry, hold_down: Duration, now: Instant) -> Vec<SocketAddr> {
    let leaving: Vec<SocketAddr> = entry
        .subscribers
        .iter()
        .filter(|(_, subscriber)| now.duration_since(subscriber.admitted_at) >= hold_down)
        .map(|(peer, _)| *peer)
        .collect();
    for peer in &leaving {
        entry.subscribers.remove(peer);
    }
    leaving
}

// ------------------------------------------------------------------ cut-through

/// Forwards the parts that reach this edge over the cache mesh to its UDP
/// clients as their datagrams arrive.
///
/// The cache mesh sends a part as one systematic RaptorQ block of a chunk
/// frame, so each source symbol is a slice of the frame. When a source symbol
/// arrives it is sent on at once, cut into client symbols (a divisor of the
/// mesh symbol size that fits a client datagram), with no wait for the rest
/// of the block and no timer. When the edge has decoded the block, it sends
/// each client the source symbols that it never received and the repair of
/// the client's policy, from the same frame in the same symbols. A part is
/// cut through from the first parent block that names it; if another
/// parent's block of the part decodes first, that block completes it.
///
/// It is registered with the cache mesh at start, before the client socket
/// exists, and armed by [`run_edge_udp`].
#[derive(Default)]
pub(crate) struct CutThrough {
    inner: OnceLock<CutInner>,
}

struct CutInner {
    sockets: EdgeSockets,
    table: Table,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    state: Mutex<CutState>,
}

#[derive(Default)]
struct CutState {
    /// Each block of a parent seen: where its symbols go.
    routes: HashMap<(SocketAddr, u32), Route>,
    /// Datagrams of blocks whose first symbol (which names the part) has not
    /// arrived yet.
    pending: HashMap<(SocketAddr, u32), (Instant, Vec<Vec<u8>>)>,
    /// Parts that the edge decoded before any block of theirs was claimed:
    /// a later block of the part must not claim it (the clients would wait
    /// for a block that may never decode, and the cache sender has sent it).
    decoded_unclaimed: HashMap<(u64, u64), Instant>,
    /// Each part claimed for cut-through.
    slots: HashMap<(u64, u64), SlotState>,
    pruned_at: Option<Instant>,
}

struct Route {
    stream_id: u64,
    slot: u64,
    /// This block carries the part to the clients.
    chosen: bool,
    client_symbol: u16,
    mesh_symbol: u16,
    transfer_length: u32,
    forwarded: Vec<bool>,
    sequence: u32,
    seen_at: Instant,
}

struct SlotState {
    chosen: (SocketAddr, u32),
    done: bool,
    clients: Vec<(SocketAddr, RepairPolicy)>,
    seen_at: Instant,
}

const CUT_STATE_TTL: Duration = Duration::from_secs(5);
const MAX_PENDING_PER_BLOCK: usize = 512;
const MAX_PENDING_BLOCKS: usize = 1_024;

impl CutThrough {
    pub(crate) fn arm(
        &self,
        sockets: EdgeSockets,
        table: Table,
        config: EdgeUdpConfig,
        stats: Arc<EdgeUdpStats>,
    ) {
        let _ = self.inner.set(CutInner {
            sockets,
            table,
            config,
            stats,
            state: Mutex::new(CutState::default()),
        });
    }
}

/// Send to every client now. `false` if a send found the socket full: the
/// symbol is then sent again with the part's repair.
fn send_now(
    inner: &CutInner,
    stream_id: u64,
    datagram: &[u8],
    clients: &[(SocketAddr, RepairPolicy)],
) -> bool {
    let out = inner.sockets.out_for_stream(stream_id);
    if inner.config.batch_send {
        let batch: Vec<(&[u8], SocketAddr)> =
            clients.iter().map(|(peer, _)| (datagram, *peer)).collect();
        let (delivered, blocked) = out.send_many(&inner.stats, true, false, &batch);
        inner
            .stats
            .cut_through_datagrams
            .fetch_add(delivered as u64, Ordering::Relaxed);
        return blocked == 0;
    }
    let mut all = true;
    for (peer, _) in clients {
        match out.send(&inner.stats, datagram, *peer) {
            SendOutcome::Sent | SendOutcome::Queued => {
                inner
                    .stats
                    .cut_through_datagrams
                    .fetch_add(1, Ordering::Relaxed);
            }
            SendOutcome::Blocked => all = false,
            SendOutcome::Failed => {}
        }
    }
    all
}

impl CutState {
    fn prune(&mut self, now: Instant) {
        if self
            .pruned_at
            .is_some_and(|at| now.duration_since(at) < Duration::from_secs(1))
        {
            return;
        }
        self.pruned_at = Some(now);
        self.routes
            .retain(|_, route| now.duration_since(route.seen_at) < CUT_STATE_TTL);
        self.slots
            .retain(|_, slot| now.duration_since(slot.seen_at) < CUT_STATE_TTL);
        let routes = &self.routes;
        self.pending.retain(|key, (since, _)| {
            !routes.contains_key(key) && now.duration_since(*since) < Duration::from_secs(1)
        });
        self.decoded_unclaimed
            .retain(|_, at| now.duration_since(*at) < CUT_STATE_TTL);
        if self.pending.len() > MAX_PENDING_BLOCKS {
            self.pending.clear();
        }
    }
}

/// Forward one mesh source symbol of a chosen route, cut into client symbols.
fn forward_symbol(
    inner: &CutInner,
    route: &mut Route,
    clients: &[(SocketAddr, RepairPolicy)],
    esi: u32,
    data: &[u8],
) {
    let per_mesh = u32::from(route.mesh_symbol / route.client_symbol).max(1);
    let client_size = usize::from(route.client_symbol);
    for (index, piece) in data.chunks(client_size).enumerate() {
        let client_esi = esi * per_mesh + index as u32;
        let Some(done) = route.forwarded.get_mut(client_esi as usize) else {
            break;
        };
        if *done {
            continue;
        }
        let Some(datagram) = source_datagram(
            cut_through_block_id(route.slot),
            route.transfer_length,
            route.sequence,
            route.client_symbol,
            client_esi,
            piece,
        ) else {
            continue;
        };
        route.sequence = route.sequence.wrapping_add(1);
        *done = send_now(inner, route.stream_id, &datagram, clients);
    }
}

impl CacheMeshReceiveObserver for CutThrough {
    fn datagram(&self, peer: SocketAddr, datagram: &[u8]) {
        let Some(inner) = self.inner.get() else {
            return;
        };
        let Ok(header) = DatagramFecHeader::decode(datagram) else {
            return;
        };
        let Ok(payload) = header.payload(datagram) else {
            return;
        };
        if payload.len() < 4 {
            return;
        }
        let esi = u32::from_be_bytes([0, payload[1], payload[2], payload[3]]);
        if esi >= u32::from(header.source_symbols) {
            return; // a mesh repair symbol: the edge sends its own repair
        }
        let key = (peer, header.block_id);
        let now = Instant::now();
        let mut state = inner.state.lock().unwrap();
        state.prune(now);
        if !state.routes.contains_key(&key) {
            if esi != 0 {
                // The first symbol names the part; until it arrives, hold.
                let (_, held) = state
                    .pending
                    .entry(key)
                    .or_insert_with(|| (now, Vec::new()));
                if held.len() < MAX_PENDING_PER_BLOCK {
                    held.push(datagram.to_vec());
                }
                return;
            }
            let route = new_route(inner, &mut state, key, &header, &payload[4..], now);
            state.routes.insert(key, route);
            let held = state
                .pending
                .remove(&key)
                .map(|(_, held)| held)
                .unwrap_or_default();
            let state = &mut *state;
            let route = state.routes.get_mut(&key).unwrap();
            if !route.chosen {
                return;
            }
            let clients = state
                .slots
                .get(&(route.stream_id, route.slot))
                .map(|slot| slot.clients.clone())
                .unwrap_or_default();
            forward_symbol(inner, route, &clients, 0, &payload[4..]);
            for earlier in held {
                if let Ok(header) = DatagramFecHeader::decode(&earlier) {
                    if let Ok(payload) = header.payload(&earlier) {
                        if payload.len() >= 4 {
                            let esi = u32::from_be_bytes([0, payload[1], payload[2], payload[3]]);
                            forward_symbol(inner, route, &clients, esi, &payload[4..]);
                        }
                    }
                }
            }
            return;
        }
        let state = &mut *state;
        let route = state.routes.get_mut(&key).unwrap();
        route.seen_at = now;
        if !route.chosen {
            return;
        }
        let Some(slot) = state.slots.get(&(route.stream_id, route.slot)) else {
            return;
        };
        if slot.done {
            return;
        }
        let clients = slot.clients.clone();
        forward_symbol(inner, route, &clients, esi, &payload[4..]);
    }

    fn decoded(&self, peer: SocketAddr, datagram: &[u8], frame: &[u8]) {
        let Some(inner) = self.inner.get() else {
            return;
        };
        let Ok(header) = DatagramFecHeader::decode(datagram) else {
            return;
        };
        let key = (peer, header.block_id);
        let mut guard = inner.state.lock().unwrap();
        let (stream_id, slot) = match guard.routes.get(&key) {
            Some(route) => (route.stream_id, route.slot),
            None => match chunk_frame_slot(frame) {
                Some(found) => found,
                None => return,
            },
        };
        let state = &mut *guard;
        let Some(claim) = state.slots.get_mut(&(stream_id, slot)) else {
            state
                .decoded_unclaimed
                .insert((stream_id, slot), Instant::now());
            return;
        };
        if claim.done {
            return;
        }
        claim.done = true;
        let clients = claim.clients.clone();
        let chosen = claim.chosen == key;
        let route = state
            .routes
            .get(&key)
            .filter(|route| route.chosen && chosen);
        let job = match route {
            // The chosen block: the client symbols it never sent, and repair.
            Some(route) => CompleteJob::Chosen {
                client_symbol: route.client_symbol,
                missing: route
                    .forwarded
                    .iter()
                    .enumerate()
                    .filter(|(_, sent)| !**sent)
                    .map(|(esi, _)| esi)
                    .collect(),
            },
            // Another parent's block decoded first: it carries the whole part.
            None => {
                inner
                    .stats
                    .cut_through_fallbacks
                    .fetch_add(1, Ordering::Relaxed);
                CompleteJob::Whole
            }
        };
        inner
            .stats
            .cut_through_parts
            .fetch_add(1, Ordering::Relaxed);
        // Each client's repair as it asks for it now, not when the part began.
        let clients: Vec<(SocketAddr, RepairPolicy)> = {
            let table = inner.table.lock().unwrap();
            let current = table.get(&stream_id);
            clients
                .into_iter()
                .map(|(peer, repair)| {
                    let repair = current
                        .and_then(|entry| entry.subscribers.get(&peer))
                        .map_or(repair, |subscriber| subscriber.repair);
                    (peer, repair)
                })
                .collect()
        };
        let max_symbol = inner.config.symbol_size;
        let batch = Batch {
            send: inner.config.batch_send,
            gso: inner.config.gso,
        };
        if frame.len() <= inner.config.inline_complete_bytes {
            // A small part encodes in microseconds: complete it here rather
            // than wake a thread of the blocking pool for it, and without
            // the cut-through state locked.
            drop(guard);
            complete(
                inner.sockets.out_for_stream(stream_id),
                &inner.stats,
                max_symbol,
                batch,
                slot,
                frame,
                job,
                &clients,
            );
            return;
        }
        let frame = frame.to_vec();
        let sockets = inner.sockets.clone();
        let stats = Arc::clone(&inner.stats);
        // Encoding a 4K part takes milliseconds: not on the receive task.
        tokio::task::spawn_blocking(move || {
            complete(
                sockets.out_for_stream(stream_id),
                &stats,
                max_symbol,
                batch,
                slot,
                &frame,
                job,
                &clients,
            )
        });
    }
}

/// Datagrams a cut-through thread holds before it drops one.
const CUT_LANE_CAPACITY: usize = 65_536;

enum CutEvent {
    Datagram(SocketAddr, Vec<u8>),
    Decoded(SocketAddr, Vec<u8>, Vec<u8>),
}

/// Runs a [`CutThrough`] on a thread of its own: the cache-mesh receive
/// task only copies each datagram onto a queue, so the per-client sends,
/// the cut-through state's lock and the completion's spawn never hold back
/// the decode of the next part. One lane per class: with a class-split
/// mesh, the audio and the video receive task each feed their own.
pub(crate) struct CutThroughLane {
    cut: Arc<CutThrough>,
    queue: std::sync::mpsc::SyncSender<CutEvent>,
}

impl CutThroughLane {
    /// Start the lane's thread. `runtime` is where the completions of
    /// decoded parts run (they encode on its blocking pool).
    pub(crate) fn start(
        cut: Arc<CutThrough>,
        name: &str,
        runtime: tokio::runtime::Handle,
    ) -> std::io::Result<Arc<Self>> {
        let (queue, events) = std::sync::mpsc::sync_channel::<CutEvent>(CUT_LANE_CAPACITY);
        let worker = Arc::clone(&cut);
        std::thread::Builder::new()
            .name(format!("needletail-cut-{name}"))
            .spawn(move || {
                let _runtime = runtime.enter();
                // Ends when the cache mesh drops its observer.
                while let Ok(event) = events.recv() {
                    match event {
                        CutEvent::Datagram(peer, datagram) => worker.datagram(peer, &datagram),
                        CutEvent::Decoded(peer, datagram, frame) => {
                            worker.decoded(peer, &datagram, &frame);
                        }
                    }
                }
            })?;
        Ok(Arc::new(Self { cut, queue }))
    }
}

impl CacheMeshReceiveObserver for CutThroughLane {
    fn datagram(&self, peer: SocketAddr, datagram: &[u8]) {
        let Some(inner) = self.cut.inner.get() else {
            return;
        };
        if self
            .queue
            .try_send(CutEvent::Datagram(peer, datagram.to_vec()))
            .is_err()
        {
            inner
                .stats
                .cut_through_queue_drops
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn decoded(&self, peer: SocketAddr, datagram: &[u8], frame: &[u8]) {
        if self.cut.inner.get().is_none() {
            return;
        }
        // A completion is never dropped: the clients of the part wait for
        // it. The queue is full only far behind, and then this waits.
        let _ = self
            .queue
            .send(CutEvent::Decoded(peer, datagram.to_vec(), frame.to_vec()));
    }
}

enum CompleteJob {
    Chosen {
        client_symbol: u16,
        missing: Vec<usize>,
    },
    Whole,
}

/// How [`complete`] hands its datagrams to the kernel.
#[derive(Clone, Copy)]
struct Batch {
    /// One `sendmmsg` for the part's datagrams to all its clients.
    send: bool,
    /// Each client's datagrams as one GSO message of it.
    gso: bool,
}

#[allow(clippy::too_many_arguments)]
fn complete(
    out: ClientOut<'_>,
    stats: &EdgeUdpStats,
    max_symbol: u16,
    batch: Batch,
    slot: u64,
    frame: &[u8],
    job: CompleteJob,
    clients: &[(SocketAddr, RepairPolicy)],
) {
    let mut groups: HashMap<RepairPolicy, Vec<SocketAddr>> = HashMap::new();
    for (peer, repair) in clients {
        groups.entry(*repair).or_default().push(*peer);
    }
    for (repair, peers) in groups {
        let (datagrams, send): (Vec<Vec<u8>>, Vec<usize>) = match &job {
            CompleteJob::Chosen {
                client_symbol,
                missing,
                ..
            } => {
                let sources = frame.len().div_ceil(usize::from(*client_symbol));
                let repair_symbols = repair.repair_symbols(sources as u32);
                let Ok(datagrams) = encode_block(
                    frame,
                    cut_through_block_id(slot),
                    *client_symbol,
                    repair_symbols,
                ) else {
                    continue;
                };
                let send = missing
                    .iter()
                    .copied()
                    .chain(sources..datagrams.len())
                    .collect();
                (datagrams, send)
            }
            CompleteJob::Whole => {
                let (sources, symbol) = geometry(frame.len(), max_symbol);
                let Ok(datagrams) = encode_block(
                    frame,
                    block_id(ObjectKind::Part, slot),
                    symbol,
                    repair.repair_symbols(sources),
                ) else {
                    continue;
                };
                let send = (0..datagrams.len()).collect();
                (datagrams, send)
            }
        };
        if batch.send && out.queue.is_some() {
            // Sent, or queued until the socket takes it: no wait. With GSO
            // a client's datagrams are together, one message of the batch;
            // without it the clients take turns, datagram by datagram.
            let list: Vec<(&[u8], SocketAddr)> = if batch.gso {
                peers
                    .iter()
                    .flat_map(|peer| {
                        send.iter()
                            .filter_map(|index| datagrams.get(*index))
                            .map(move |datagram| (datagram.as_slice(), *peer))
                    })
                    .collect()
            } else {
                send.iter()
                    .filter_map(|index| datagrams.get(*index))
                    .flat_map(|datagram| peers.iter().map(move |peer| (datagram.as_slice(), *peer)))
                    .collect()
            };
            let (delivered, _) = out.send_many(stats, true, batch.gso, &list);
            stats
                .cut_through_datagrams
                .fetch_add(delivered as u64, Ordering::Relaxed);
            continue;
        }
        for index in send {
            let Some(datagram) = datagrams.get(index) else {
                continue;
            };
            for peer in &peers {
                if out.queue.is_some() {
                    // Sent, or queued until the socket takes it: no wait.
                    if matches!(
                        out.send(stats, datagram, *peer),
                        SendOutcome::Sent | SendOutcome::Queued
                    ) {
                        stats.cut_through_datagrams.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
                let socket = out.socket;
                let mut attempt = 0;
                let result = loop {
                    match socket.try_send_to(datagram, *peer) {
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock && attempt < 200 =>
                        {
                            // A blocking thread: wait for the socket to drain.
                            attempt += 1;
                            std::thread::sleep(Duration::from_micros(50));
                        }
                        other => break other,
                    }
                };
                match result {
                    Ok(sent) => {
                        stats.datagrams.fetch_add(1, Ordering::Relaxed);
                        stats.cut_through_datagrams.fetch_add(1, Ordering::Relaxed);
                        stats.bytes.fetch_add(sent as u64, Ordering::Relaxed);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        stats.would_block.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        stats.send_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

/// The route of a new block, from its first source symbol, which names the
/// part. The first block of a part that has subscribers due for it is
/// chosen and claims the part for them.
fn new_route(
    inner: &CutInner,
    state: &mut CutState,
    key: (SocketAddr, u32),
    header: &DatagramFecHeader,
    first_symbol: &[u8],
    now: Instant,
) -> Route {
    let (stream_id, slot) = chunk_frame_slot(first_symbol).unwrap_or((u64::MAX, u64::MAX));
    // A part in one mesh symbol is cut through in one client symbol of its
    // own size (the mesh pads it to the mesh symbol size); a larger part in
    // client symbols that divide the mesh symbol.
    let client_symbol = if header.source_symbols == 1
        && header.transfer_length <= u32::from(inner.config.symbol_size)
    {
        Some((header.transfer_length as u16).max(16))
    } else {
        cut_through_symbol_size(header.symbol_size, inner.config.symbol_size)
    };
    let mut route = Route {
        stream_id,
        slot,
        chosen: false,
        client_symbol: client_symbol.unwrap_or(1),
        mesh_symbol: header.symbol_size,
        transfer_length: header.transfer_length,
        forwarded: Vec::new(),
        sequence: 0,
        seen_at: now,
    };
    if stream_id == u64::MAX
        || client_symbol.is_none()
        || state.slots.contains_key(&(stream_id, slot))
        || state.decoded_unclaimed.contains_key(&(stream_id, slot))
    {
        return route;
    }
    let clients: Vec<(SocketAddr, RepairPolicy)> = {
        let mut table = inner.table.lock().unwrap();
        let Some(entry) = table.get_mut(&stream_id) else {
            return route;
        };
        // A fast-lane client takes the part from the fast lane.
        entry
            .subscribers
            .iter_mut()
            .filter(|(_, subscriber)| subscriber.next_part <= slot && subscriber.fast.is_none())
            .map(|(peer, subscriber)| {
                subscriber.cut_slots.insert(slot);
                (*peer, subscriber.repair)
            })
            .collect()
    };
    if clients.is_empty() {
        return route;
    }
    route.chosen = true;
    route.forwarded = vec![
        false;
        header
            .transfer_length
            .div_ceil(u32::from(route.client_symbol)) as usize
    ];
    state.slots.insert(
        (stream_id, slot),
        SlotState {
            chosen: key,
            done: false,
            clients,
            seen_at: now,
        },
    );
    route
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_subscriber_resumes_within_its_playout_deadline() {
        // 500 ms of 5 ms parts: 100 parts.
        let playout = Some(playout_parts_of(Duration::from_millis(500), 5_000));
        assert_eq!(playout, Some(100));
        // A client that moved names part 1000; the head is 1500 and the
        // window 2400 parts: it resumes at 1400, not 500 parts back.
        assert_eq!(first_part_for(Some(1_000), 1_500, 2_400, playout), 1_400);
        // The behaviour from before: anywhere in the window.
        assert_eq!(first_part_for(Some(1_000), 1_500, 2_400, None), 1_000);
        // A part within the deadline, the next part, and none named.
        assert_eq!(first_part_for(Some(1_450), 1_500, 2_400, playout), 1_450);
        assert_eq!(first_part_for(Some(1_501), 1_500, 2_400, playout), 1_501);
        assert_eq!(first_part_for(None, 1_500, 2_400, playout), 1_500);
        // Out of the window: the newest part, as before.
        assert_eq!(first_part_for(Some(10), 5_000, 2_400, playout), 5_000);
        // Parts longer than the deadline: one part.
        assert_eq!(playout_parts_of(Duration::from_millis(500), 2_000_000), 1);
    }

    #[test]
    fn a_hole_is_repaired_only_while_it_is_within_the_playout_deadline() {
        let playout = Duration::from_millis(500);
        // No deadline: HOLE_WAIT, as before.
        assert_eq!(hole_budget(Some(10), 5, 5_000, Duration::ZERO), None);
        // 20 parts of 5 ms behind: 400 ms left, capped at HOLE_WAIT.
        assert_eq!(
            hole_budget(Some(120), 100, 5_000, playout),
            Some(Duration::from_millis(400))
        );
        // 80 parts behind: 100 ms left.
        assert_eq!(
            hole_budget(Some(180), 100, 5_000, playout),
            Some(Duration::from_millis(100))
        );
        // Past the deadline: the gap goes at once.
        assert_eq!(
            hole_budget(Some(300), 100, 5_000, playout),
            Some(Duration::ZERO)
        );
        // A part at or ahead of the head has its whole budget.
        assert_eq!(
            hole_budget(Some(100), 101, 5_000, Duration::from_millis(50)),
            Some(Duration::from_millis(50))
        );
    }

    fn subscriber(admitted_at: Instant) -> Subscriber {
        Subscriber {
            repair: RepairPolicy {
                permille: 0,
                min_symbols: 0,
                loss_ppm: 0,
            },
            expires_at: admitted_at + Duration::from_secs(10),
            next_part: 0,
            init: None,
            cut_slots: BTreeSet::new(),
            admitted_at,
            fast: None,
        }
    }

    #[test]
    fn lost_parents_do_not_move_a_subscriber_within_the_hold_down() {
        let now = Instant::now();
        let old: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let new: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let mut entry = StreamEntry::default();
        entry
            .subscribers
            .insert(old, subscriber(now - Duration::from_secs(5)));
        entry
            .subscribers
            .insert(new, subscriber(now - Duration::from_millis(500)));
        assert_eq!(
            take_movable(&mut entry, Duration::from_secs(3), now),
            vec![old]
        );
        assert_eq!(entry.subscribers.len(), 1);
        // Without a hold-down (or draining) every subscriber goes.
        assert_eq!(take_movable(&mut entry, Duration::ZERO, now), vec![new]);
        assert!(entry.subscribers.is_empty());
    }

    #[test]
    fn a_client_moved_away_for_lost_parents_is_taken_back_within_the_hold_down() {
        let moved: MovedAway = Arc::default();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let other: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let now = Instant::now();
        let hold = Duration::from_secs(3);
        moved.lock().unwrap().insert((peer, 7), now);
        assert!(moved_back_within_hold_down(
            &moved,
            peer,
            7,
            hold,
            now + Duration::from_secs(1)
        ));
        assert!(!moved_back_within_hold_down(&moved, peer, 8, hold, now));
        assert!(!moved_back_within_hold_down(&moved, other, 7, hold, now));
        assert!(!moved_back_within_hold_down(
            &moved,
            peer,
            7,
            Duration::ZERO,
            now
        ));
        // After the hold-down the move is forgotten.
        assert!(!moved_back_within_hold_down(
            &moved,
            peer,
            7,
            hold,
            now + Duration::from_secs(4)
        ));
        assert!(moved.lock().unwrap().is_empty());
    }

    async fn receive(socket: &UdpSocket, count: usize) -> Vec<Vec<u8>> {
        let mut buf = vec![0_u8; 2_048];
        let mut got = Vec::new();
        while got.len() < count {
            let (len, _) = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut buf))
                .await
                .expect("the queued datagrams arrive")
                .unwrap();
            got.push(buf[..len].to_vec());
        }
        got
    }

    /// A part's datagrams to several clients leave in one batch, with or
    /// without GSO, with or without a send queue: each client receives its
    /// own, whole and in order, and each counts once.
    #[tokio::test]
    async fn a_batch_reaches_each_client_whole_and_in_order() {
        for (queue, gso) in [(true, true), (true, false), (false, true), (false, false)] {
            let sender = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            sender.writable().await.unwrap();
            let stats = Arc::new(EdgeUdpStats::default());
            let send_queue = queue.then(|| {
                SendQueue::start(
                    Arc::clone(&sender),
                    Duration::from_millis(250),
                    Arc::clone(&stats),
                )
            });
            let out = ClientOut {
                socket: &sender,
                queue: send_queue.as_ref(),
            };
            let clients = [
                UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            ];
            // Three repair datagrams of one size for each client, and a
            // shorter last one, as GSO allows; client by client.
            let datagrams: Vec<Vec<u8>> = (0..4_u8)
                .map(|index| vec![index; if index == 3 { 40 } else { 100 }])
                .collect();
            let list: Vec<(&[u8], SocketAddr)> = clients
                .iter()
                .flat_map(|client| {
                    let to = client.local_addr().unwrap();
                    datagrams
                        .iter()
                        .map(move |datagram| (datagram.as_slice(), to))
                })
                .collect();
            let (delivered, blocked) = out.send_many(&stats, true, gso, &list);
            assert_eq!((delivered, blocked), (12, 0), "queue {queue}, gso {gso}");
            for client in &clients {
                assert_eq!(
                    receive(client, 4).await,
                    datagrams,
                    "queue {queue}, gso {gso}"
                );
            }
            assert_eq!(stats.datagrams.load(Ordering::Relaxed), 12);
            assert_eq!(stats.bytes.load(Ordering::Relaxed), 3 * 340);
        }
    }

    /// A datagram that found the socket full waits in its client's queue and
    /// leaves in order when the socket can take it; the client's next
    /// datagrams queue behind it, and another client is not held up.
    #[tokio::test]
    async fn a_datagram_that_finds_the_socket_full_is_sent_later_in_order() {
        let sender = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        // Until the reactor has seen the socket writable, a try_send says
        // WouldBlock without trying.
        sender.writable().await.unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let other = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stats = Arc::new(EdgeUdpStats::default());
        let queue = SendQueue::start(
            Arc::clone(&sender),
            Duration::from_millis(250),
            Arc::clone(&stats),
        );
        let to = client.local_addr().unwrap();
        // As if the socket had refused the first two.
        assert_eq!(queue.enqueue(b"one", to, true), SendOutcome::Queued);
        assert_eq!(queue.enqueue(b"two", to, true), SendOutcome::Queued);
        // The client's next datagram goes behind them (or, if the drain has
        // already emptied the queue, straight out): never ahead.
        let third = queue.send(b"three", to);
        assert!(
            matches!(third, SendOutcome::Queued | SendOutcome::Sent),
            "{third:?}"
        );
        // Another client goes straight out.
        assert_eq!(
            queue.send(b"other", other.local_addr().unwrap()),
            SendOutcome::Sent
        );
        assert_eq!(receive(&other, 1).await, vec![b"other".to_vec()]);
        assert_eq!(
            receive(&client, 3).await,
            vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
        );
        assert!(stats.send_queued.load(Ordering::Relaxed) >= 2);
        assert_eq!(stats.send_queue_drops.load(Ordering::Relaxed), 0);
        assert_eq!(queue.waiting.load(Ordering::Relaxed), 0);
    }

    /// A datagram older than the limit is dropped, not sent, and counted.
    #[tokio::test]
    async fn a_queued_datagram_older_than_the_limit_is_dropped_and_counted() {
        let sender = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stats = Arc::new(EdgeUdpStats::default());
        // No drain task: the test drains by hand.
        let queue = SendQueue {
            socket: sender,
            limit: Duration::from_millis(20),
            waiting: std::sync::atomic::AtomicUsize::new(0),
            clients: Mutex::new(HashMap::new()),
            wake: tokio::sync::Notify::new(),
            stats: Arc::clone(&stats),
        };
        let to = client.local_addr().unwrap();
        queue.enqueue(b"stale", to, true);
        tokio::time::sleep(Duration::from_millis(40)).await;
        queue.enqueue(b"fresh", to, true);
        // The stale one went when the fresh one was queued behind it.
        assert_eq!(stats.send_queue_drops.load(Ordering::Relaxed), 1);
        assert!(!queue.drain());
        assert_eq!(receive(&client, 1).await, vec![b"fresh".to_vec()]);
        queue.enqueue(b"late", to, true);
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(!queue.drain());
        assert_eq!(stats.send_queue_drops.load(Ordering::Relaxed), 2);
        assert_eq!(queue.waiting.load(Ordering::Relaxed), 0);
        let mut buf = [0_u8; 64];
        assert!(
            tokio::time::timeout(Duration::from_millis(100), client.recv_from(&mut buf))
                .await
                .is_err(),
            "a dropped datagram is not sent"
        );
    }
}
