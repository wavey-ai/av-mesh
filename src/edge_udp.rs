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
    encode_object, Control, ObjectKind, PartObject, RepairPolicy, KEEPALIVE,
    MIN_FIRST_REQUEST_BYTES, SUBSCRIPTION_TTL,
};
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
}

#[derive(Debug)]
struct Subscriber {
    repair: RepairPolicy,
    expires_at: Instant,
    /// The next part that this subscriber needs.
    next_part: u64,
    /// The init segment last sent to it.
    init: Option<Bytes>,
}

#[derive(Debug, Default)]
struct StreamEntry {
    subscribers: HashMap<SocketAddr, Subscriber>,
    /// A sender task runs for the stream.
    running: bool,
}

type Table = Arc<Mutex<HashMap<u64, StreamEntry>>>;
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
    mut shutdown_rx: watch::Receiver<()>,
) -> anyhow::Result<()> {
    let socket = Arc::new(socket);
    let table: Table = Arc::default();
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
fn take_due(table: &Table, stream_id: u64, part: u64) -> Vec<(SocketAddr, RepairPolicy)> {
    let mut table = table.lock().unwrap();
    let Some(entry) = table.get_mut(&stream_id) else {
        return Vec::new();
    };
    entry
        .subscribers
        .iter_mut()
        .filter(|(_, subscriber)| subscriber.next_part == part)
        .map(|(peer, subscriber)| {
            subscriber.next_part = part + 1;
            (*peer, subscriber.repair)
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
    let targets = take_due(table, stream_id, part);
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
