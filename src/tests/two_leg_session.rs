//! The client session against edges in this process (first av-mesh
//! integration of the resilient mesh, step 5): what no single unit could
//! test, because it needs the two-path session (B X5), the edge's leaving
//! subscribers (B Z1) and the edge's subscriber table with `o=` (B Z2) at
//! once. The director (B Y1) is not in the loop: the binaries are not on it
//! yet (B Y2-Y5), and its own tests use a scripted session.
//!
//! - One leg, through the session's one-edge API (`begin`, `poll`), as
//!   `needletail-udp-client` uses it today (`--paths 1 --spare false`
//!   semantics): a drain's `MOVE` is answered as the wave 9 client answered
//!   it, by moving to the next edge, whose `begin` leaves the old one; the
//!   old edge, which now keeps a moved subscriber until its `UNSUB` or the
//!   grace, lets it go at that `UNSUB`, and no part is lost or doubled
//!   (B-listener.md section 6, step 0, in process).
//! - Two legs on two edges fed the same stream, one leg on a lossy path:
//!   every part comes out of the session once, and both legs carry it.

use super::fast_lane::{packaged_envelope, LaneEdge, Packaging};
use super::fast_lease::loopback;
use super::*;
use av_mesh::client_director::{LegEvent, LegKey, LegStart, SessionLegs, StartFrom};
use av_mesh::client_session::{Received, SessionConfig, UdpSession};
use av_mesh::client_udp::ObjectKind as UdpObjectKind;
use std::sync::atomic::{AtomicBool, AtomicU64};

/// The stream, its parts, and when the client stops: this long after it has
/// the last part with nothing more (under the session's 200 ms silence, so
/// that the end of the stream is no verdict).
const STREAM: u64 = 4_201;
const PARTS: u64 = 400;
const QUIET: Duration = Duration::from_millis(150);
/// The two-leg run is long enough for the legs' refreshes (2 s) to carry
/// `o=` while the stream runs.
const TWO_LEG_PARTS: u64 = 2_000;

/// What a session delivered and what it said.
#[derive(Debug, Default)]
struct Delivered {
    /// Each part's deliveries (a part delivered twice is a fault).
    parts: BTreeMap<u64, u32>,
    gaps: u64,
    events: Vec<LegEvent>,
    /// The `MOVE`s and `REFUSED`s the one-edge API reported, and the
    /// reasons its `poll` gave to move.
    moves: Vec<String>,
    failures: Vec<String>,
}

/// Part `sequence` of the stream, as the contributor's symbols.
fn symbols(sequence: u64, deadline_us: u64) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let (envelope, _) = packaged_envelope(Packaging::Pcm, STREAM, 0, sequence, 288);
    fast_test_symbols(
        &envelope,
        relay_session::MediaPriority::Audio,
        1_088,
        400,
        0,
        deadline_us,
    )
}

/// Feed `edges` the stream's parts `range`, each committed at every edge
/// before the next.
async fn feed(edges: &[&LaneEdge], range: std::ops::Range<u64>, deadline_us: u64) {
    for sequence in range {
        let part = symbols(sequence, deadline_us);
        for edge in edges {
            edge.send(false, &part, false).await;
        }
        for edge in edges {
            wait_for_part(&edge.cache, STREAM, sequence).await;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Datagram loss of a client path, deterministic (as `fast_lane`'s).
struct Loss(u64, u64);

impl Loss {
    fn lost(&mut self) -> bool {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 & u64::from(u32::MAX)) < self.1
    }
}

/// How the client holds its edges.
enum Mode {
    /// The one-edge API: `begin` at `edges[0]`, and at a `MOVE` or a failed
    /// leg `begin` at the next edge, as the wave 9 client did.
    OneEdge { edges: Vec<SocketAddr> },
    /// Legs added at the start through [`SessionLegs`], driven by
    /// `poll_legs`; media datagrams from `lossy` are lost at `loss`.
    Legs {
        legs: Vec<(LegKey, &'static str, SocketAddr)>,
        lossy: SocketAddr,
        loss: f64,
    },
}

/// Run a session until it has part `last` (once `last` is set) and nothing
/// more came for [`QUIET`], or 30 s. `live` is set once every leg is live.
async fn run_session(
    mode: Mode,
    last: Arc<AtomicU64>,
    live: Arc<AtomicBool>,
) -> (UdpSession, Delivered) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let _ = SockRef::from(&socket).set_recv_buffer_size(8 << 20);
    let mut session = UdpSession::new(SessionConfig::new(STREAM), Instant::now());
    let mut outbox: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
    let mut run = Delivered::default();
    let (one_edge, mut lose, lossy, wanted_live) = match &mode {
        Mode::OneEdge { edges } => {
            session.begin(edges[0], None, Instant::now(), &mut |to, datagram| {
                outbox.push((to, datagram.to_vec()))
            });
            (Some((edges.clone(), 0_usize)), Loss(1, 0), None, 1)
        }
        Mode::Legs { legs, lossy, loss } => {
            for (key, node, addr) in legs {
                session.add_leg(
                    *key,
                    node,
                    *addr,
                    LegStart {
                        from: StartFrom::AfterNewest,
                        token: None,
                        token_wait: Duration::from_millis(500),
                    },
                    Instant::now(),
                    &mut |to, datagram| outbox.push((to, datagram.to_vec())),
                );
            }
            (
                None,
                Loss(0x9e37_79b9_7f4a_7c15, (loss * f64::from(u32::MAX)) as u64),
                Some(*lossy),
                legs.len(),
            )
        }
    };
    let mut one_edge = one_edge;
    let mut lives = 0;
    let mut buf = vec![0_u8; 65_536];
    let started = Instant::now();
    let mut last_part_at: Option<Instant> = None;
    loop {
        for (to, datagram) in outbox.drain(..) {
            let _ = socket.send_to(&datagram, to).await;
        }
        let now = Instant::now();
        let done = run
            .parts
            .last_key_value()
            .is_some_and(|(part, _)| *part >= last.load(Ordering::Relaxed));
        if done && last_part_at.is_some_and(|at| now.duration_since(at) >= QUIET) {
            break;
        }
        assert!(
            now.duration_since(started) < Duration::from_secs(30),
            "the session ran 30 s: {run:?}"
        );
        // The timers, and what they found.
        if let Some((edges, at)) = one_edge.as_mut() {
            if let Some(reason) = session.poll(now, &mut |to, datagram| {
                outbox.push((to, datagram.to_vec()))
            }) {
                run.failures.push(reason);
                *at = (*at + 1) % edges.len();
                let from = run.parts.last_key_value().map(|(part, _)| part + 1);
                session.begin(edges[*at], from, now, &mut |to, datagram| {
                    outbox.push((to, datagram.to_vec()))
                });
                continue;
            }
        } else {
            session.poll_legs(now, &mut |to, datagram| {
                outbox.push((to, datagram.to_vec()))
            });
            while let Some(event) = session.next_event() {
                if matches!(event, LegEvent::Live { .. }) {
                    lives += 1;
                    if lives == wanted_live {
                        live.store(true, Ordering::Relaxed);
                    }
                }
                run.events.push(event);
            }
        }
        if !outbox.is_empty() {
            continue;
        }
        let wait = session
            .next_wake()
            .saturating_duration_since(now)
            .min(Duration::from_millis(20));
        let Ok(Ok((len, peer))) = tokio::time::timeout(wait, socket.recv_from(&mut buf)).await
        else {
            continue;
        };
        let datagram = &buf[..len];
        if lossy == Some(peer)
            && av_mesh::client_udp::Control::decode(datagram).is_none()
            && lose.lost()
        {
            continue;
        }
        let outcome = session.on_datagram(peer, datagram, Instant::now(), &mut |to, datagram| {
            outbox.push((to, datagram.to_vec()))
        });
        match outcome {
            Received::Move(reason) => {
                run.moves.push(reason);
                if let Some((edges, at)) = one_edge.as_mut() {
                    // The wave 9 client: the next edge, from the part after
                    // the newest (or the oldest hole, `--move-from-hole`).
                    *at = (*at + 1) % edges.len();
                    let from = run.parts.last_key_value().map(|(part, _)| part + 1);
                    session.begin(edges[*at], from, Instant::now(), &mut |to, datagram| {
                        outbox.push((to, datagram.to_vec()))
                    });
                }
                continue;
            }
            Received::Subscribed { first: true, .. } if one_edge.is_some() => {
                live.store(true, Ordering::Relaxed);
            }
            _ => {}
        }
        while let Some(object) = session.next_object() {
            match object.kind {
                UdpObjectKind::Part => {
                    *run.parts.entry(object.part).or_default() += 1;
                    last_part_at = Some(Instant::now());
                }
                UdpObjectKind::Gap => run.gaps += 1,
                UdpObjectKind::Init => {}
            }
        }
    }
    session.leave(&mut |to, datagram| outbox.push((to, datagram.to_vec())));
    for (to, datagram) in outbox.drain(..) {
        let _ = socket.send_to(&datagram, to).await;
    }
    (session, run)
}

/// Every part from the first delivered to `last` came out exactly once,
/// and no gap.
fn assert_each_part_once(name: &str, run: &Delivered, last: u64) {
    let first = *run.parts.keys().next().unwrap_or_else(|| panic!("{name}: no part"));
    assert!(first < last / 2, "{name}: the session began late, at {first}");
    for part in first..=last {
        match run.parts.get(&part) {
            Some(1) => {}
            Some(count) => panic!("{name}: part {part} came out {count} times"),
            None => panic!("{name}: part {part} never came out (first {first}, last {last})"),
        }
    }
    assert_eq!(run.gaps, 0, "{name}: no gap");
}

/// Wait until `done`, at most 3 s.
async fn until(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

/// B-listener.md section 6, step 0, in process: a one-leg client on the
/// session's one-edge API moves off a draining edge as the wave 9 client
/// did. The edge (with B Z1's rules) keeps serving it after its `MOVE` and
/// lets it go at the `UNSUB` that the client's `begin` at the next edge
/// sends: nothing expires at the grace, nothing is lost or doubled, and
/// the client moves once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_leg_moves_off_a_draining_edge_as_the_wave_9_client_did() {
    let edge_a = LaneEdge::start(loopback(201)).await;
    let edge_b = LaneEdge::start(loopback(202)).await;
    let deadline_us = now_unix_us() + 60_000_000;
    let edges = [&edge_a, &edge_b];
    feed(&edges, 0..20, deadline_us).await;
    let last = Arc::new(AtomicU64::new(u64::MAX));
    let live = Arc::new(AtomicBool::new(false));
    let client = tokio::spawn(run_session(
        Mode::OneEdge {
            edges: vec![edge_a.client_port, edge_b.client_port],
        },
        Arc::clone(&last),
        Arc::clone(&live),
    ));
    until("the client is subscribed at edge A", || {
        live.load(Ordering::Relaxed)
    })
    .await;
    feed(&edges, 20..150, deadline_us).await;
    let moves_before = edge_a.stats.moves.load(Ordering::Relaxed);
    edge_a.router.lifecycle.drain(node_status::DrainReason::Operator);
    feed(&edges, 150..PARTS, deadline_us).await;
    last.store(PARTS - 1, Ordering::Relaxed);
    let (session, run) = client.await.unwrap();
    assert_each_part_once("one leg", &run, PARTS - 1);
    assert_eq!(run.moves, vec!["draining".to_owned()], "{run:?}");
    assert!(run.failures.is_empty(), "{run:?}");
    drop(session);
    // Edge A sent its MOVE, served the client until its UNSUB, and then
    // let it go: no subscriber left, none leaving, none cut at the grace.
    let stats = &edge_a.stats;
    assert_eq!(stats.moves.load(Ordering::Relaxed), moves_before + 1);
    until("edge A lets the client go at its UNSUB", || {
        stats.legs().is_some_and(|legs| legs.subscribers == 0 && legs.leaving == 0)
    })
    .await;
    assert_eq!(stats.moves_expired.load(Ordering::Relaxed), 0);
    edge_a.stop().await;
    edge_b.stop().await;
}

/// Two legs of one session on two edges fed the same stream, the first on a
/// path that loses 5 % of its media datagrams: every part comes out of the
/// session once, no leg fails, both legs bring parts, and each edge learns
/// the listener's other edge (`o=`, B Z2: no single leg at either).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_leg_session_on_two_edges_delivers_every_part_once() {
    let edge_a = LaneEdge::start(loopback(203)).await;
    let edge_b = LaneEdge::start(loopback(204)).await;
    let deadline_us = now_unix_us() + 60_000_000;
    let edges = [&edge_a, &edge_b];
    feed(&edges, 0..20, deadline_us).await;
    let last = Arc::new(AtomicU64::new(u64::MAX));
    let live = Arc::new(AtomicBool::new(false));
    let (a, b) = (LegKey(1), LegKey(2));
    let client = tokio::spawn(run_session(
        Mode::Legs {
            legs: vec![
                (a, "edge-a", edge_a.client_port),
                (b, "edge-b", edge_b.client_port),
            ],
            lossy: edge_a.client_port,
            loss: 0.05,
        },
        Arc::clone(&last),
        Arc::clone(&live),
    ));
    // A leg is live at its `OK` and its first media: the stream runs while
    // they join. Then each edge learns the listener's other edge from the
    // leg's refreshes (`o=`, B Z2): neither holds it as a single leg.
    let others_known = async {
        until("both legs are live", || live.load(Ordering::Relaxed)).await;
        let deadline = Instant::now() + Duration::from_secs(6);
        loop {
            let known = edges.iter().all(|edge| {
                edge.stats
                    .legs()
                    .is_some_and(|legs| legs.subscribers == 1 && legs.single == 0)
            });
            if known || Instant::now() >= deadline {
                break known;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let ((), others_known) = tokio::join!(feed(&edges, 20..TWO_LEG_PARTS, deadline_us), others_known);
    let legs: Vec<_> = edges.iter().map(|edge| edge.stats.legs()).collect();
    assert!(others_known, "{legs:?}");
    last.store(TWO_LEG_PARTS - 1, Ordering::Relaxed);
    let (session, run) = client.await.unwrap();
    assert_each_part_once("two legs", &run, TWO_LEG_PARTS - 1);
    assert!(
        !run.events.iter().any(|event| matches!(
            event,
            LegEvent::Failed { .. } | LegEvent::Refused { .. } | LegEvent::Moved { .. }
        )),
        "{:?}",
        run.events
    );
    for key in [a, b] {
        let report = session.leg_report(key).expect("the leg is held");
        assert!(report.datagrams > 0, "{key:?} brought nothing: {report:?}");
    }
    edge_a.stop().await;
    edge_b.stop().await;
}
