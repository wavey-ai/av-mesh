//! The client UDP port of an edge: RaptorQ FEC UDP to clients.
//!
//! See `av_mesh::client_udp` for the protocol. An edge keeps one sender for
//! each stream that has subscribers. The sender follows the stream in the
//! edge's cache exactly as an LL-HLS reader of the newest part does (the
//! same wait for a part, the same repair of a hole from the parents), encodes
//! each part once and sends it to every subscriber of the stream. A
//! subscription is reader demand: it subscribes the edge to the playlist at
//! its distributors and keeps the playlist held, as playlist requests do.

use crate::{AppRouter, ParentsLost};
use av_mesh::client_udp::{
    encode_object, Control, ObjectKind, PartObject, RepairPolicy, SUBSCRIPTION_TTL,
};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// How long a new subscription waits for the edge to hold parts of its
/// stream before it is refused.
const ADMIT_WAIT: Duration = Duration::from_secs(4);
/// How often a sender renews the demand for its playlist and checks that
/// the edge still serves.
const HOUSEKEEPING: Duration = Duration::from_secs(1);

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
            } => {
                let repair = repair.bounded();
                let refreshed = {
                    let mut table = table.lock().unwrap();
                    match table
                        .get_mut(&stream_id)
                        .and_then(|entry| entry.subscribers.get_mut(&peer))
                    {
                        Some(subscriber) => {
                            subscriber.expires_at = Instant::now() + SUBSCRIPTION_TTL;
                            subscriber.repair = repair;
                            true
                        }
                        None => false,
                    }
                };
                if refreshed || !admitting.lock().unwrap().insert((peer, stream_id)) {
                    continue;
                }
                if subscriber_count(&table) >= config.max_subscribers {
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
            Control::Unsubscribe { stream_id } => {
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
            first_part,
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

/// One object for each subscriber that is due for it, encoded once for
/// the strongest repair that those subscribers asked for.
async fn send_object(
    socket: &UdpSocket,
    config: EdgeUdpConfig,
    stats: &EdgeUdpStats,
    object: &PartObject,
    targets: &[(SocketAddr, RepairPolicy)],
) {
    if targets.is_empty() {
        return;
    }
    let repair = targets
        .iter()
        .fold(RepairPolicy { permille: 0, min_symbols: 0 }, |policy, (_, repair)| {
            policy.max(*repair)
        });
    let datagrams = match encode_object(object, config.symbol_size, repair) {
        Ok(datagrams) => datagrams,
        Err(error) => {
            warn!(stream_id = object.stream_id, part = object.part, error = %error,
                  "edge client UDP could not encode a part");
            return;
        }
    };
    // Interleave the subscribers, so that one of them does not wait for the
    // whole of a large part to be sent to the others.
    for datagram in &datagrams {
        for (peer, _) in targets {
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

async fn run_stream_sender(
    socket: Arc<UdpSocket>,
    router: AppRouter,
    table: Table,
    config: EdgeUdpConfig,
    stats: Arc<EdgeUdpStats>,
    stream_id: u64,
) {
    let mut housekeeping_at = Instant::now();
    loop {
        let now = Instant::now();
        // Subscribers that stopped refreshing end; with none left, so does
        // the sender.
        let (oldest_next, newest_next) = {
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
            let nexts = entry.subscribers.values().map(|subscriber| subscriber.next_part);
            (nexts.clone().min().unwrap_or(0), nexts.max().unwrap_or(0))
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

        // A subscriber that resumes behind the others is sent the parts that
        // the cache holds already, without waiting.
        let part = oldest_next;
        if part < newest_next {
            let targets = take_due(&table, stream_id, part);
            send_init_if_new(&socket, &router, &table, config, &stats, stream_id, part, &targets)
                .await;
            let object = match router.cache.get_part_for_stream_id(stream_id, part).await {
                Some((bytes, _)) => part_object(&router, stream_id, part, bytes).await,
                None => {
                    stats.gaps.fetch_add(1, Ordering::Relaxed);
                    gap_object(stream_id, part)
                }
            };
            stats.parts.fetch_add(1, Ordering::Relaxed);
            send_object(&socket, config, &stats, &object, &targets).await;
            continue;
        }

        // The newest part: wait for it as an LL-HLS part request does.
        match router.get_part_repairing_holes(stream_id, part).await {
            Ok(Some((bytes, _))) => {
                let targets = take_due(&table, stream_id, part);
                send_init_if_new(&socket, &router, &table, config, &stats, stream_id, part, &targets)
                    .await;
                let object = part_object(&router, stream_id, part, bytes).await;
                stats.parts.fetch_add(1, Ordering::Relaxed);
                send_object(&socket, config, &stats, &object, &targets).await;
            }
            Ok(None) => {
                // Not here in time. If the stream has moved past it, the edge
                // will not get it: tell the clients, and go on.
                if head_part(&router, stream_id).await.is_some_and(|head| head > part) {
                    let targets = take_due(&table, stream_id, part);
                    stats.gaps.fetch_add(1, Ordering::Relaxed);
                    send_object(&socket, config, &stats, &gap_object(stream_id, part), &targets)
                        .await;
                }
            }
            Err(ParentsLost) => {
                // Housekeeping sends the clients away.
                housekeeping_at = Instant::now();
            }
        }
    }
}
