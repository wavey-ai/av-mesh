//! What a steady one-leg UDP listener costs on the wire: datagrams and bytes
//! per part from the edge, and the control datagrams it sends, for 1 ms Opus
//! Custom over 30 s (integration 2, the local smoke's +4 %). The listener is
//! `UdpSession` with `needletail-udp-client`'s defaults, driven as the
//! binary drives it, against an in-process edge; its path drops a share of
//! the edge's datagrams by a fixed seed (after they are counted).
//!
//! An ignored measurement: `cargo test --release client_wire_per_part --
//! --ignored --nocapture --test-threads 1`, with CLIENT_TRAFFIC_SECONDS (30)
//! and CLIENT_TRAFFIC_LOSS_PERMILLE (0, 10). The same file built on the base
//! cdfeb5c (target/base-cdfeb5c) gives the base's figures, which
//! [`BASE_DATAGRAMS_A_PART_AT_3_PER_MILLE`] records.

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
    let mut config = SessionConfig::new(stream_id);
    // A form of the plan hold to measure (CLIENT_TRAFFIC_FORM
    // `<hold ms>:<smoothing>`), the defaults otherwise.
    if let Some((hold, smoothing)) = std::env::var("CLIENT_TRAFFIC_FORM")
        .ok()
        .and_then(|form| form.split_once(':').map(|(a, b)| (a.to_owned(), b.to_owned())))
    {
        config.plan_hold = Duration::from_millis(hold.parse().unwrap());
        config.plan_smoothing = smoothing.parse().unwrap();
    }
    let mut session = UdpSession::new(config, Instant::now());
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

/// What [`measure_wire`] measured: the listener's side and the edge's
/// counters over the same span.
pub(super) struct Measured {
    pub(super) wire: Wire,
    /// compact_parts, copies, window_repair, nack_parts, nack_resent,
    /// nack_refused.
    pub(super) edge: Vec<u64>,
}

impl Measured {
    pub(super) fn datagrams_a_part(&self) -> f64 {
        self.wire.media_datagrams as f64 / self.wire.parts.max(1) as f64
    }

    pub(super) fn line(&self, feed: Feed, loss_permille: u64, seconds: u64) -> String {
        let wire = &self.wire;
        let parts = wire.parts.max(1) as f64;
        let edge = &self.edge;
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
            edge[0],
            edge[1],
            edge[2],
            edge[3],
            edge[4],
            edge[5],
        )
    }
}

/// Datagrams and bytes per part to a steady one-leg listener of 1 ms Opus
/// Custom, and what it sends back, over `seconds` after a 3 s settle.
pub(super) async fn measure_wire(feed: Feed, loss_permille: u64, seconds: u64) -> Measured {
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
    let edge = at_end
        .iter()
        .zip(at_start.iter())
        .map(|(end, start)| end - start)
        .collect();
    Measured { wire, edge }
}

/// Datagrams a part to the steady one-leg listener at 0.3 % loss on the
/// base cdfeb5c (compact feed, 30 s): 1.231 to 1.245 in integration 2,
/// 1.2376 in av-mesh-fix-3 (the same file built on target/base-cdfeb5c).
pub(super) const BASE_DATAGRAMS_A_PART_AT_3_PER_MILLE: f64 = 1.238;

/// The plan hold as the lead set it after integration 2 (B 2.7: less a
/// second after the last change, not after a second of wanting less): at
/// 0.3 % random loss a steady listener costs at most 12 % more datagrams a
/// part than the base, which had no hold, and changes its plan at most 50
/// times in 30 s (the hold of wanting cost 21 to 23 % for 20 changes; no
/// hold 70 changes).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_plan_hold_costs_at_most_twelve_percent_over_the_base_at_three_per_mille() {
    let seconds = 30;
    let measured = measure_wire(Feed::FastCompact, 3, seconds).await;
    println!("{}", measured.line(Feed::FastCompact, 3, seconds));
    let ratio = measured.datagrams_a_part() / BASE_DATAGRAMS_A_PART_AT_3_PER_MILLE;
    assert!(
        measured.wire.parts >= seconds * 900,
        "the listener got {} parts in {seconds} s",
        measured.wire.parts
    );
    assert!(
        ratio <= 1.12,
        "{:.4} datagrams a part at 0.3 % loss, {:.1} % over the base's {}",
        measured.datagrams_a_part(),
        (ratio - 1.0) * 100.0,
        BASE_DATAGRAMS_A_PART_AT_3_PER_MILLE
    );
    assert!(
        measured.wire.plan_changes <= 50,
        "{} plan changes in {seconds} s",
        measured.wire.plan_changes
    );
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
            let measured = measure_wire(feed, *loss, seconds).await;
            println!("{}", measured.line(feed, *loss, seconds));
        }
    }
}
