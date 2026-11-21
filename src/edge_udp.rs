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

use crate::{AppRouter, ParentsLost};
use av_mesh::client_udp::{
    block_id, cut_through_block_id, cut_through_symbol_size, encode_block, encode_object,
    geometry, source_datagram, Control, ObjectKind, PartObject, RepairPolicy, KEEPALIVE,
    MIN_FIRST_REQUEST_BYTES, SUBSCRIPTION_TTL,
};
use playlists::mesh::{chunk_frame_slot, CacheMeshReceiveObserver};
use raptorq_datagram_fec::DatagramFecHeader;
use std::collections::BTreeSet;
use std::sync::OnceLock;
use bytes::Bytes;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct EdgeUdpConfig {
    pub symbol_size: u16,
    pub max_subscribers: usize,
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
}

#[derive(Debug, Default)]
pub(crate) struct StreamEntry {
    subscribers: HashMap<SocketAddr, Subscriber>,
    /// A sender task runs for the stream.
    running: bool,
}

pub(crate) type Table = Arc<Mutex<HashMap<u64, StreamEntry>>>;
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
    socket: UdpSocket,
    router: AppRouter,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    cut_through: Option<Arc<CutThrough>>,
    mut shutdown_rx: watch::Receiver<()>,
) -> anyhow::Result<()> {
    let socket = Arc::new(socket);
    let table: Table = Arc::default();
    if let Some(cut) = &cut_through {
        cut.arm(Arc::clone(&socket), Arc::clone(&table), config, Arc::clone(&stats));
    }
    let admitting: Admitting = Arc::default();
    let tokens = Tokens(RandomState::new());
    let mut buf = vec![0_u8; 2_048];
    info!(bind = %socket.local_addr()?, "edge client UDP (RaptorQ FEC) listening");
    loop {
        let (len, peer) = tokio::select! {
            _ = shutdown_rx.changed() => return Ok(()),
            received = socket.recv_from(&mut buf) => match received {
                Ok(received) => received,
                Err(error) => {
                    // An ICMP port unreachable of a departed client surfaces
                    // here on some kernels; it is not fatal.
                    debug!(error = %error, "edge client UDP receive error");
                    continue;
                }
            },
        };
        let Some(control) = Control::decode(&buf[..len]) else {
            continue;
        };
        match control {
            Control::Subscribe {
                stream_id,
                from_part,
                repair,
                token,
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
                    match table
                        .get_mut(&stream_id)
                        .and_then(|entry| entry.subscribers.get_mut(&peer))
                    {
                        Some(subscriber) => {
                            subscriber.expires_at = Instant::now() + SUBSCRIPTION_TTL;
                            subscriber.repair = repair;
                            // A client that names a part has not yet heard
                            // the edge's answer: it resumes there.
                            if let (Some(part), Some(head)) = (from_part, head) {
                                if part < subscriber.next_part
                                    && head.saturating_sub(part) < window
                                {
                                    subscriber.next_part = part;
                                }
                            }
                            Some(subscriber.next_part)
                        }
                        None => None,
                    }
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
                    router.clone(),
                    Arc::clone(&table),
                    config,
                    Arc::clone(&stats),
                    peer,
                    stream_id,
                    from_part,
                    repair,
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
                if let Some(entry) = table.lock().unwrap().get_mut(&stream_id) {
                    entry.subscribers.remove(&peer);
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

#[allow(clippy::too_many_arguments)]
async fn admit(
    socket: Arc<UdpSocket>,
    router: AppRouter,
    table: Table,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    peer: SocketAddr,
    stream_id: u64,
    from_part: Option<u64>,
    repair: RepairPolicy,
) {
    if let Some(reason) = move_reason(&router).await {
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
    // A client that moves from another edge names the part that it needs
    // next. It resumes there if the part is still in the window, and
    // otherwise at the newest part.
    let first_part = match from_part {
        Some(part) if part <= head.saturating_add(1) && head.saturating_sub(part) < window => part,
        _ => head,
    };
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
            },
        );
        !std::mem::replace(&mut entry.running, true)
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
    info!(%peer, stream_id, first_part, head, "edge client UDP subscription");
    if start_sender {
        tokio::spawn(run_stream_sender(
            socket, router, table, config, stats, stream_id,
        ));
    }
}

/// Send one object to each target, encoded once for each repair policy that
/// the targets asked for: one subscriber on a lossy path does not raise the
/// repair that the others receive.
async fn send_object(
    socket: &UdpSocket,
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
        for datagram in &datagrams {
            for peer in &peers {
                match socket.send_to(datagram, *peer).await {
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
        available_unix_us: times.map_or(0, |(available, _)| available),
        published_unix_us: times.and_then(|(_, published)| published).unwrap_or(0),
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
    socket: &UdpSocket,
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
    send_object(socket, config, stats, &object, &needing).await;
}

/// The subscribers due for `part`, after the part has been taken for them.
/// A subscriber that cut-through carries the part to is passed over, unless
/// `gap`: a gap goes to every subscriber due.
fn take_due(table: &Table, stream_id: u64, part: u64, gap: bool) -> Vec<(SocketAddr, RepairPolicy)> {
    let mut table = table.lock().unwrap();
    let Some(entry) = table.get_mut(&stream_id) else {
        return Vec::new();
    };
    entry
        .subscribers
        .iter_mut()
        .filter(|(_, subscriber)| subscriber.next_part == part)
        .filter_map(|(peer, subscriber)| {
            subscriber.next_part = part + 1;
            let cut = subscriber.cut_slots.remove(&part);
            while subscriber
                .cut_slots
                .first()
                .is_some_and(|slot| *slot < subscriber.next_part)
            {
                subscriber.cut_slots.pop_first();
            }
            (!cut || gap).then_some((*peer, subscriber.repair))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn deliver(
    socket: &UdpSocket,
    router: &AppRouter,
    table: &Table,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    stream_id: u64,
    part: u64,
    bytes: Option<Bytes>,
) {
    let targets = take_due(table, stream_id, part, bytes.is_none());
    if targets.is_empty() {
        return;
    }
    let object = match bytes {
        Some(bytes) => {
            send_init_if_new(socket, router, table, config, stats, stream_id, part, &targets).await;
            stats.parts.fetch_add(1, Ordering::Relaxed);
            part_object(router, stream_id, part, bytes).await
        }
        None => {
            stats.gaps.fetch_add(1, Ordering::Relaxed);
            gap_object(stream_id, part)
        }
    };
    send_object(socket, config, stats, &object, &targets).await;
}

async fn run_stream_sender(
    socket: Arc<UdpSocket>,
    router: AppRouter,
    table: Table,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    stream_id: u64,
) {
    let mut housekeeping_at = Instant::now();
    let mut last_sent = Instant::now();
    loop {
        let now = Instant::now();
        let head = head_part(&router, stream_id).await;
        let window = router.cache.window_parts_for(stream_id).await as u64;
        // Subscribers that stopped refreshing end; with none left, so does
        // the sender. A subscriber whose next part has left the window
        // continues at the newest part.
        let (oldest_next, newest_next, peers) = {
            let mut guard = table.lock().unwrap();
            let Some(entry) = guard.get_mut(&stream_id) else {
                return;
            };
            entry.subscribers.retain(|_, subscriber| subscriber.expires_at > now);
            if entry.subscribers.is_empty() {
                guard.remove(&stream_id);
                debug!(stream_id, "edge client UDP sender stops: no subscriber");
                return;
            }
            if let Some(head) = head {
                for subscriber in entry.subscribers.values_mut() {
                    if head.saturating_sub(subscriber.next_part) >= window {
                        subscriber.next_part = head;
                    }
                }
            }
            let nexts = entry.subscribers.values().map(|subscriber| subscriber.next_part);
            (
                nexts.clone().min().unwrap_or(0),
                nexts.max().unwrap_or(0),
                entry.subscribers.keys().copied().collect::<Vec<_>>(),
            )
        };

        if now >= housekeeping_at {
            housekeeping_at = now + HOUSEKEEPING;
            if let Some(reason) = move_reason(&router).await {
                let peers: Vec<SocketAddr> = {
                    let mut guard = table.lock().unwrap();
                    guard
                        .remove(&stream_id)
                        .map(|entry| entry.subscribers.into_keys().collect())
                        .unwrap_or_default()
                };
                stats.moves.fetch_add(peers.len() as u64, Ordering::Relaxed);
                info!(stream_id, reason, clients = peers.len(), "edge client UDP moves its clients");
                for peer in peers {
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
                return;
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
            // repair it from the parents for a short while, or send a gap.
            let bytes = match router.cache.get_part_for_stream_id(stream_id, part).await {
                Some((bytes, _)) => Some(bytes),
                None => match tokio::time::timeout(
                    HOLE_WAIT,
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
                },
            };
            deliver(&socket, &router, &table, config, &stats, stream_id, part, bytes).await;
            last_sent = Instant::now();
            continue;
        }

        // The newest part: wait for it as an LL-HLS part request does, for a
        // short while at a time.
        match tokio::time::timeout(LIVE_WAIT, router.get_part_repairing_holes(stream_id, part))
            .await
        {
            Ok(Ok(Some((bytes, _)))) => {
                deliver(&socket, &router, &table, config, &stats, stream_id, part, Some(bytes))
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
    socket: Arc<UdpSocket>,
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
    pending: HashMap<(SocketAddr, u32), Vec<Vec<u8>>>,
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
        socket: Arc<UdpSocket>,
        table: Table,
        config: EdgeUdpConfig,
        stats: Arc<EdgeUdpStats>,
    ) {
        let _ = self.inner.set(CutInner {
            socket,
            table,
            config,
            stats,
            state: Mutex::new(CutState::default()),
        });
    }
}

fn send_now(inner: &CutInner, datagram: &[u8], clients: &[(SocketAddr, RepairPolicy)]) {
    for (peer, _) in clients {
        match inner.socket.try_send_to(datagram, *peer) {
            Ok(sent) => {
                inner.stats.datagrams.fetch_add(1, Ordering::Relaxed);
                inner.stats.cut_through_datagrams.fetch_add(1, Ordering::Relaxed);
                inner.stats.bytes.fetch_add(sent as u64, Ordering::Relaxed);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                inner.stats.would_block.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                inner.stats.send_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
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
        self.pending.retain(|key, _| !self.routes.contains_key(key));
        if self.pending.len() > MAX_PENDING_BLOCKS {
            self.pending.clear();
        }
    }
}

/// Forward one mesh source symbol of a chosen route, cut into client symbols.
fn forward_symbol(inner: &CutInner, route: &mut Route, clients: &[(SocketAddr, RepairPolicy)], esi: u32, data: &[u8]) {
    let per_mesh = u32::from(route.mesh_symbol / route.client_symbol);
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
        *done = true;
        send_now(inner, &datagram, clients);
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
                let held = state.pending.entry(key).or_default();
                if held.len() < MAX_PENDING_PER_BLOCK {
                    held.push(datagram.to_vec());
                }
                return;
            }
            let route = new_route(inner, &mut state, key, &header, &payload[4..], now);
            state.routes.insert(key, route);
            let held = state.pending.remove(&key).unwrap_or_default();
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
        let mut state = inner.state.lock().unwrap();
        let (stream_id, slot) = match state.routes.get(&key) {
            Some(route) => (route.stream_id, route.slot),
            None => match chunk_frame_slot(frame) {
                Some(found) => found,
                None => return,
            },
        };
        let state = &mut *state;
        let Some(claim) = state.slots.get_mut(&(stream_id, slot)) else {
            return;
        };
        if claim.done {
            return;
        }
        claim.done = true;
        let clients = claim.clients.clone();
        let chosen = claim.chosen == key;
        let route = state.routes.get(&key).filter(|route| route.chosen && chosen);
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
                inner.stats.cut_through_fallbacks.fetch_add(1, Ordering::Relaxed);
                CompleteJob::Whole
            }
        };
        inner.stats.cut_through_parts.fetch_add(1, Ordering::Relaxed);
        let frame = frame.to_vec();
        let socket = Arc::clone(&inner.socket);
        let stats = Arc::clone(&inner.stats);
        let max_symbol = inner.config.symbol_size;
        // Encoding a 4K part takes milliseconds: not on the receive task.
        tokio::task::spawn_blocking(move || {
            complete(&socket, &stats, max_symbol, slot, &frame, job, &clients)
        });
    }
}

enum CompleteJob {
    Chosen {
        client_symbol: u16,
        missing: Vec<usize>,
    },
    Whole,
}

fn complete(
    socket: &UdpSocket,
    stats: &EdgeUdpStats,
    max_symbol: u16,
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
                let Ok(datagrams) =
                    encode_block(frame, cut_through_block_id(slot), *client_symbol, repair_symbols)
                else {
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
        for index in send {
            let Some(datagram) = datagrams.get(index) else {
                continue;
            };
            for peer in &peers {
                match socket.try_send_to(datagram, *peer) {
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
    let client_symbol = cut_through_symbol_size(header.symbol_size, inner.config.symbol_size);
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
    {
        return route;
    }
    let clients: Vec<(SocketAddr, RepairPolicy)> = {
        let mut table = inner.table.lock().unwrap();
        let Some(entry) = table.get_mut(&stream_id) else {
            return route;
        };
        entry
            .subscribers
            .iter_mut()
            .filter(|(_, subscriber)| subscriber.next_part <= slot)
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
