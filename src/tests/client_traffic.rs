//! What a steady one-leg UDP listener costs on the wire: datagrams and bytes
//! per part from the edge, and the control datagrams it sends, for 1 ms Opus
//! Custom over 30 s (integration 2, the local smoke's +4 %). The listener is
//! `UdpSession` with `needletail-udp-client`'s defaults, driven as the
//! binary drives it, against an in-process edge; its path drops a share of
//! the edge's datagrams by a fixed seed (after they are counted).
//!
//! An ignored measurement: `cargo test --release client_wire_per_part --
//! --ignored --nocapture --test-threads 1`, with CLIENT_TRAFFIC_SECONDS (30)
//! and CLIENT_TRAFFIC_LOSS_PERMILLE (0, 10).

use super::compact_lane::{publish_paced, AnyEdge, Feed, Payload};
use super::*;
use av_mesh::client_session::{Received, SessionConfig, UdpSession};
use socket2::SockRef;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use tokio::time::timeout;

#[derive(Debug, Default, Clone)]
pub(super) struct Wire {
    /// Edge to listener: media datagrams and their bytes with IP and UDP
    /// headers, and control datagrams.
    pub(super) media_datagrams: u64,
    pub(super) media_bytes: u64,
    pub(super) control_in: BTreeMap<String, u64>,
    /// Listener to edge, by the control's first word.
    pub(super) control_out: BTreeMap<String, u64>,
    pub(super) control_out_bytes: u64,
    /// Parts delivered, first and later copies.
    pub(super) parts: u64,
    pub(super) duplicates: u64,
    pub(super) lost_on_path: u64,
    pub(super) plan_changes: u64,
}

/// A control's verb: the word after the `NTU/1` magic.
fn word(datagram: &[u8]) -> String {
    let text = String::from_utf8_lossy(&datagram[..datagram.len().min(64)]).into_owned();
    let mut words = text.split([' ', '\n']);
    let first = words.next().unwrap_or_default();
    let verb = if first.starts_with("NTU") {
        words.next().unwrap_or_default()
    } else {
        first
    };
    verb.chars().take(12).collect()
}

/// One listener of `stream_id` at `edge` until `stop`, counting from when
/// `measuring` is set.
pub(super) async fn steady_listener(
    edge: SocketAddr,
    stream_id: u64,
    loss_permille: u64,
    measuring: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) -> Wire {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let _ = SockRef::from(&socket).set_recv_buffer_size(16 << 20);
    let mut session = UdpSession::new(SessionConfig::new(stream_id), Instant::now());
    let mut wire = Wire::default();
    let mut outbox: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut buf = vec![0_u8; 65_536];
    session.begin(edge, None, Instant::now(), &mut |to, datagram| {
        outbox.push((to, datagram.to_vec()))
    });
    loop {
        for (to, datagram) in outbox.drain(..) {
            if measuring.load(Ordering::Relaxed) {
                *wire.control_out.entry(word(&datagram)).or_default() += 1;
                wire.control_out_bytes += datagram.len() as u64 + 28;
            }
            let _ = socket.send_to(&datagram, to).await;
        }
        if stop.load(Ordering::Relaxed) {
            wire.plan_changes = session.plan_changes;
            break;
        }
        let now = Instant::now();
        let _ = session.poll(now, &mut |to, datagram| outbox.push((to, datagram.to_vec())));
        if !outbox.is_empty() {
            continue;
        }
        let wait = session
            .next_wake()
            .min(now + Duration::from_millis(50))
            .saturating_duration_since(now);
        let Ok(Ok((len, peer))) = timeout(wait, socket.recv_from(&mut buf)).await else {
            continue;
        };
        let datagram = &buf[..len];
        let control = av_mesh::client_udp::Control::decode(datagram).is_some();
        if measuring.load(Ordering::Relaxed) {
            if control {
                *wire.control_in.entry(word(datagram)).or_default() += 1;
            } else {
                wire.media_datagrams += 1;
                wire.media_bytes += len as u64 + 28;
            }
        }
        if !control && loss_permille > 0 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            if seed % 1_000 < loss_permille {
                if measuring.load(Ordering::Relaxed) {
                    wire.lost_on_path += 1;
                }
                continue;
            }
        }
        let outcome = session.on_datagram(peer, datagram, Instant::now(), &mut |to, datagram| {
            outbox.push((to, datagram.to_vec()))
        });
        if matches!(outcome, Received::Move(_)) {
            panic!("the listener was moved: {outcome:?}");
        }
        while let Some(object) = session.next_object() {
            if object.kind != av_mesh::client_udp::ObjectKind::Part {
                continue;
            }
            if measuring.load(Ordering::Relaxed) {
                if seen.insert(object.part) {
                    wire.parts += 1;
                } else {
                    wire.duplicates += 1;
                }
            }
        }
    }
    wire
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Datagrams and bytes per part to a steady one-leg listener of 1 ms Opus
/// Custom, and what it sends back, over `seconds` after a 3 s settle.
pub(super) async fn measure_wire(feed: Feed, loss_permille: u64, seconds: u64) -> String {
    let payload = Payload::Opus1;
    let stream_id = 7_301;
    let (edge, next) = AnyEdge::start(feed, payload, stream_id, 91).await;
    let measuring = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let listener = tokio::spawn(steady_listener(
        edge.client_port(),
        stream_id,
        loss_permille,
        Arc::clone(&measuring),
        Arc::clone(&stop),
    ));
    let settle = 3_000_u64;
    let total = settle + seconds * 1_000;
    let flag = Arc::clone(&measuring);
    let marker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(settle)).await;
        flag.store(true, Ordering::Relaxed);
    });
    let stats = Arc::clone(edge.stats());
    let read = |stats: &edge_udp::EdgeUdpStats| {
        [
            stats.compact_parts.load(Ordering::Relaxed),
            stats.compact_copies.load(Ordering::Relaxed),
            stats.compact_window_repair.load(Ordering::Relaxed),
            stats.nack_parts.load(Ordering::Relaxed),
            stats.nack_resent.load(Ordering::Relaxed),
            stats.nack_refused.load(Ordering::Relaxed),
        ]
    };
    let mut at_start = [0; 6];
    let publish = async {
        let half = next + settle;
        publish_paced(&edge, payload, stream_id, next, half).await;
        at_start = read(&stats);
        publish_paced(&edge, payload, stream_id, half, next + total).await;
    };
    publish.await;
    marker.await.unwrap();
    measuring.store(false, Ordering::Relaxed);
    let at_end = read(&stats);
    stop.store(true, Ordering::Relaxed);
    let wire = listener.await.unwrap();
    edge.stop().await;
    let parts = wire.parts.max(1) as f64;
    let edge_delta: Vec<u64> = at_end
        .iter()
        .zip(at_start.iter())
        .map(|(end, start)| end - start)
        .collect();
    format!(
        "feed {feed:?} loss {loss_permille}/1000 {seconds} s: parts {} dup {} lost_on_path {} plan_changes {} | \
         media {} datagrams ({:.4}/part) {} B ({:.2} B/part) | control in {:?} | \
         control out {:?} ({} B, {:.2} B/part) | edge compact_parts {} copies {} \
         window_repair {} nack_parts {} nack_resent {} nack_refused {}",
        wire.parts,
        wire.duplicates,
        wire.lost_on_path,
        wire.plan_changes,
        wire.media_datagrams,
        wire.media_datagrams as f64 / parts,
        wire.media_bytes,
        wire.media_bytes as f64 / parts,
        wire.control_in,
        wire.control_out,
        wire.control_out_bytes,
        wire.control_out_bytes as f64 / parts,
        edge_delta[0],
        edge_delta[1],
        edge_delta[2],
        edge_delta[3],
        edge_delta[4],
        edge_delta[5],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: datagrams and bytes per part to a steady one-leg listener"]
async fn client_wire_per_part() {
    let seconds = env_u64("CLIENT_TRAFFIC_SECONDS", 30);
    let losses = std::env::var("CLIENT_TRAFFIC_LOSS_PERMILLE")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|loss| loss.trim().parse().ok())
                .collect::<Vec<u64>>()
        })
        .unwrap_or_else(|| vec![0, 10]);
    let feeds = match std::env::var("CLIENT_TRAFFIC_FEEDS").as_deref() {
        Ok("fast") => vec![Feed::Fast],
        Ok("compact") => vec![Feed::FastCompact],
        _ => vec![Feed::Fast, Feed::FastCompact],
    };
    for feed in feeds {
        for loss in &losses {
            println!("{}", measure_wire(feed, *loss, seconds).await);
        }
    }
}
