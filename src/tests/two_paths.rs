//! B Y5: two-path listeners in this process, as the referee's changes meet
//! them (B-listener sections 3.11 and 6). A region of four edges fed one
//! stream (B Z1-Z3's edge: leaving subscribers, `o=`, the redundant gate),
//! two entry points that answer `ASSIGN` with the real assignment of the
//! region's advertisements (`assign_reader`, `why=`), and listeners that
//! each run the director (`client_director`) over the session
//! (`client_session`) on a socket of their own, as `needletail-udp-client`
//! does. The edges' client UDP sockets share one port on their own
//! loopback addresses, as a deploy's edges do.
//!
//! Each test makes one change as the referee scores it (a mark at its start
//! and its end; the window is widened 2 s before and 5 s after) and asserts
//! for every listener: no part missing from its first part to the last one
//! fed; no silence between first arrivals (kernel receive times, on the
//! source's timeline) at its own leg events longer than the region's steady
//! silence plus one part, and no interruption anywhere in the window (see
//! [`assert_the_rule`]); and the hand-over, exposure and single-path times
//! of the change. The tests take turns ([`SERIAL`]); each runs 25 to 45 s,
//! the token test up to two and a half minutes.
//!
//! The gap clause here is an in-process stand-in, not the referee's: the
//! full clause (no gap in the window over the steady gap plus one part,
//! anywhere, scored against the probe's own null windows, notes-stage2) is
//! the lab's, with probes of 360 s or more (B section 6).

use super::compact_lane::Payload;
use super::fast_lane::LaneEdge;
use super::fast_lease::loopback;
use super::*;
use av_mesh::client_director::{
    Action, Director, DirectorConfig, DirectorStats, LegKey, Replacement, ReplacementKind,
    SinglePath,
};
use av_mesh::client_session::{Received, SessionConfig, UdpSession};
use av_mesh::client_udp::ObjectKind as UdpObjectKind;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8};

const STREAM: u64 = 4_301;
const REGION: &str = "test-region";
/// The entries' client UDP port is the edges' (one port, own addresses).
const ENTRY_OFFSET: u8 = 5;
/// The edges' advertisements: this often.
const ADVERTISE_EVERY: Duration = Duration::from_millis(200);
/// The referee's window around a change (B 3.11; CW).
const WINDOW_BEFORE: Duration = Duration::from_secs(2);
const WINDOW_AFTER: Duration = Duration::from_secs(5);
/// After the last part fed, a listener stops once nothing came this long.
const QUIET: Duration = Duration::from_millis(400);
/// Steady state before a change: the window's outside is at least as long
/// as its inside, so that the longest gap outside is a fair measure.
const STEADY_BEFORE: u64 = 8;

/// One region at a time in this process: four edges, two entries and their
/// listeners are a load of their own, and the rule compares gaps.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn now_us() -> u64 {
    now_unix_us()
}

/// One edge of the region: its node, its client address, the edge itself
/// while it runs, and what the region's advertisements say of it.
struct Slot {
    node: String,
    client: SocketAddr,
    edge: Option<Arc<LaneEdge>>,
    /// It has run (a later start is a restart, at the same address).
    started: bool,
    draining: bool,
    /// Load besides its legs (permille), which a test may change, and what
    /// a leg costs it: its load as it measures it (its CPU term) and as it
    /// advertises it.
    base_permille: Arc<AtomicU32>,
    leg_permille: Arc<AtomicU32>,
    rebalance: bool,
}

impl Slot {
    fn load_permille(&self, legs: u64) -> u32 {
        self.base_permille.load(Ordering::Relaxed)
            + self.leg_permille.load(Ordering::Relaxed) * legs as u32
    }
}

/// How a region is made: its stream, its edges' base loads, how many of
/// them run from the start, what a leg costs, and whether the edges
/// rebalance (B Z2-Z3: target 800, margin 150, every 3 s, to the mean).
#[derive(Debug, Clone, Copy)]
struct Shape {
    payload: Payload,
    bases: [u32; 4],
    serving: usize,
    leg_permille: u32,
    rebalance: bool,
}

impl Shape {
    fn four(payload: Payload) -> Self {
        Self {
            payload,
            bases: [100; 4],
            serving: 4,
            leg_permille: 5,
            rebalance: false,
        }
    }
}

/// An edge of `slot` at `client` (relay ports at its address): its node
/// and region, its load as `slot` says (a CPU term, measured over 1 s as a
/// node's is), and the region's rebalance if it has one.
async fn start_edge(slot: &Slot, client: SocketAddr) -> LaneEdge {
    let SocketAddr::V4(at) = client else {
        unreachable!("loopback")
    };
    let (node, base, leg, rebalance) = (
        slot.node.clone(),
        Arc::clone(&slot.base_permille),
        Arc::clone(&slot.leg_permille),
        slot.rebalance,
    );
    LaneEdge::start_at(*at.ip(), client, 1_024, move |router| {
        let mut router = router;
        router.node = mesh_node_for_tests(&node, REGION, "test-continent", 51.6, -0.2);
        let stats = Arc::clone(&router.edge_udp_stats);
        let used = Arc::new(std::sync::Mutex::new((Instant::now(), 0.0_f64)));
        router.edge_load.egress.set_cpu_counter(
            Arc::new(move || {
                let legs = stats.legs().map_or(0, |legs| legs.subscribers);
                let permille = f64::from(base.load(Ordering::Relaxed))
                    + f64::from(leg.load(Ordering::Relaxed)) * legs as f64;
                let mut used = used.lock().unwrap();
                let now = Instant::now();
                used.1 += now.duration_since(used.0).as_micros() as f64 * permille / 1_000.0;
                used.0 = now;
                Some(used.1 as u64)
            }),
            1,
            100,
            1_000,
        );
        if rebalance {
            router = router.with_rebalance(Some(RebalanceConfig {
                target_permille: 800,
                margin_permille: 150,
                interval: Duration::from_secs(3),
                spread: Duration::from_millis(500),
                to_mean: true,
            }));
        }
        router
    })
    .await
}

/// An entry point: a node of the region that does not serve readers, whose
/// client UDP port answers `ASSIGN` from its view of the region.
struct Entry {
    router: AppRouter,
    mesh: Arc<CacheMeshHandle>,
    url: String,
    shutdown: watch::Sender<()>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// Four edges, two entries, the publisher and the advertisements.
struct Region {
    port: u16,
    slots: Arc<std::sync::Mutex<Vec<Slot>>>,
    entries: Vec<Entry>,
    /// The newest part fed, when each part was sent (Unix µs, by part
    /// number), and whether the publisher runs.
    published: Arc<AtomicU64>,
    sent: Arc<std::sync::Mutex<Vec<u64>>>,
    publishing: Arc<AtomicBool>,
    publisher: Option<std::thread::JoinHandle<()>>,
    advertiser: tokio::task::JoinHandle<()>,
}

/// A port free at every address of the region.
fn region_port(base: u8) -> u16 {
    'search: for _ in 0..64 {
        let probe = std::net::UdpSocket::bind((loopback(base), 0)).unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        for offset in 0..=ENTRY_OFFSET + 1 {
            if std::net::UdpSocket::bind((loopback(base + offset), port)).is_err() {
                continue 'search;
            }
        }
        return port;
    }
    panic!("no port free at 127.0.0.{base}+");
}

fn edge_snapshot(slot: &Slot, head: u64, subscribers: u64) -> MeshSnapshot {
    let mut snapshot = telemetry_snapshot_for_tests(
        &slot.node,
        REGION,
        "test-continent",
        51.6,
        -0.2,
        Vec::new(),
        STREAM,
    );
    snapshot.node.draining = slot.draining;
    let mut edge = EdgeServiceSnapshot::from_node(
        &snapshot.node,
        Some(format!("https://{}:19444/live", slot.client.ip())),
        "edge",
        EdgeLoadSnapshot::default(),
    );
    edge.load_permille = slot.load_permille(subscribers);
    edge.draining = slot.draining;
    edge.udp_subscribers = Some(subscribers);
    edge.playlists = vec![PlaylistHolding {
        request_rate: None,
        stream_id: STREAM,
        head_part: head,
        bitrate_bps: Some(2_600_000),
        sessions: subscribers,
        video: false,
    }];
    snapshot.edge_service = Some(edge);
    snapshot
}

impl Region {
    /// Four edges at 127.0.0.`base`..+3 and two entries at +5 and +6, the
    /// edges' base loads `bases`.
    async fn start(base: u8, payload: Payload, bases: [u32; 4]) -> Self {
        Self::start_shaped(
            base,
            Shape {
                bases,
                ..Shape::four(payload)
            },
        )
        .await
    }

    async fn start_shaped(base: u8, shape: Shape) -> Self {
        let payload = shape.payload;
        let port = region_port(base);
        let mut slots = Vec::new();
        for (index, base_permille) in shape.bases.into_iter().enumerate() {
            let ip = loopback(base + index as u8);
            let mut slot = Slot {
                node: format!("edge-{}", index + 1),
                client: SocketAddr::from((ip, port)),
                edge: None,
                started: false,
                draining: false,
                base_permille: Arc::new(AtomicU32::new(base_permille)),
                leg_permille: Arc::new(AtomicU32::new(shape.leg_permille)),
                rebalance: shape.rebalance,
            };
            if index < shape.serving {
                slot.edge = Some(Arc::new(start_edge(&slot, slot.client).await));
                slot.started = true;
            }
            slots.push(slot);
        }
        let slots = Arc::new(std::sync::Mutex::new(slots));
        let mut entries = Vec::new();
        for offset in [ENTRY_OFFSET, ENTRY_OFFSET + 1] {
            let ip = loopback(base + offset);
            let cache = LiveTsCache::new(STREAM, Duration::from_millis(50), 2, 6, 64).await;
            let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
            let mut router = app_router_for_tests_with_telemetry(
                cache,
                Arc::clone(&mesh),
                TelemetryAggregator::default(),
            )
            .with_client_udp_port(Some(port));
            router.cache_mesh_role = CacheMeshRole::Distributor;
            let socket = UdpSocket::bind((ip, port)).await.unwrap();
            let mut config = edge_udp::EdgeUdpConfig::with_defaults(1_200, 16);
            config.assign = true;
            config.assign_confirms_per_source = 4_096;
            let (shutdown, shutdown_rx) = watch::channel(());
            let stats = Arc::clone(&router.edge_udp_stats);
            let task = tokio::spawn(edge_udp::run_edge_udp(
                edge_udp::EdgeSockets::single(socket),
                router.clone(),
                config,
                stats,
                Vec::new(),
                shutdown_rx,
            ));
            entries.push(Entry {
                router,
                mesh,
                url: format!("https://{ip}:19444"),
                shutdown,
                task,
            });
        }
        let published = Arc::new(AtomicU64::new(0));
        let sent = Arc::new(std::sync::Mutex::new(Vec::with_capacity(64_000)));
        let publishing = Arc::new(AtomicBool::new(true));
        let publisher = {
            let (slots, published, sent, publishing) = (
                Arc::clone(&slots),
                Arc::clone(&published),
                Arc::clone(&sent),
                Arc::clone(&publishing),
            );
            std::thread::Builder::new()
                .name("y5-publisher".into())
                .spawn(move || publish(payload, slots, published, sent, publishing))
                .unwrap()
        };
        let routers: Vec<AppRouter> = entries.iter().map(|entry| entry.router.clone()).collect();
        let advertiser = tokio::spawn(advertise(
            Arc::clone(&slots),
            routers,
            Arc::clone(&published),
        ));
        // The stream is in every edge, and advertised, before anyone joins.
        let region = Self {
            port,
            slots,
            entries,
            published,
            sent,
            publishing,
            publisher: Some(publisher),
            advertiser,
        };
        until(
            "the stream runs at every edge",
            Duration::from_secs(5),
            || region.published.load(Ordering::Relaxed) > 200,
        )
        .await;
        tokio::time::sleep(ADVERTISE_EVERY * 2).await;
        region
    }

    fn entry_urls(&self) -> Vec<String> {
        self.entries.iter().map(|entry| entry.url.clone()).collect()
    }

    fn edge(&self, index: usize) -> Arc<LaneEdge> {
        Arc::clone(
            self.slots.lock().unwrap()[index]
                .edge
                .as_ref()
                .expect("a running edge"),
        )
    }

    fn node(&self, index: usize) -> String {
        self.slots.lock().unwrap()[index].node.clone()
    }

    /// Legs at edge `index` now (leaving ones included).
    fn legs_at(&self, index: usize) -> u64 {
        self.slots.lock().unwrap()[index]
            .edge
            .as_ref()
            .and_then(|edge| edge.stats.legs())
            .map_or(0, |legs| legs.subscribers)
    }

    /// A drain as `ControlKind::DrainNode` makes one (or its end), and the
    /// region's advertisements say so.
    async fn set_draining(&self, index: usize, draining: bool) {
        let edge = {
            let mut slots = self.slots.lock().unwrap();
            slots[index].draining = draining;
            Arc::clone(slots[index].edge.as_ref().expect("a running edge"))
        };
        if draining {
            edge.router.lifecycle.drain(node_status::DrainReason::Operator);
        } else {
            edge.router.lifecycle.resume(node_status::DrainReason::Operator);
        }
    }

    /// Take edge `index` out of the slot (it is no longer fed or
    /// advertised): the edge itself, once the publisher lets go of it.
    async fn take(&self, index: usize) -> LaneEdge {
        let mut edge = self.slots.lock().unwrap()[index]
            .edge
            .take()
            .expect("a running edge");
        loop {
            match Arc::try_unwrap(edge) {
                Ok(edge) => return edge,
                Err(shared) => {
                    edge = shared;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    /// SIGKILL: the edge sends nothing more, answers nothing, says nothing.
    async fn kill(&self, index: usize) {
        self.take(index).await.kill().await;
    }

    /// A stop (B rule 6: `MOVE draining` to whoever is left).
    async fn stop(&self, index: usize) {
        self.take(index).await.stop().await;
    }

    /// The edge starts: an empty cache that the stream fills from now on,
    /// new token keys, advertised under its name, at its address. An edge
    /// that ran before comes back at the same address, as a restarted
    /// process does, once the old edge's client sockets are closed (a
    /// plain bind of the address succeeds): with SO_REUSEPORT a new socket
    /// would otherwise share the address with a dead one.
    async fn start_edge(&self, index: usize) {
        let (client, restart) = {
            let slots = self.slots.lock().unwrap();
            assert!(
                slots[index].edge.is_none(),
                "an edge that runs is not started"
            );
            (slots[index].client, slots[index].started)
        };
        if restart {
            until(
                "the stopped edge's client sockets are closed",
                Duration::from_secs(10),
                || std::net::UdpSocket::bind(client).is_ok(),
            )
            .await;
        }
        let slot = {
            let mut slots = self.slots.lock().unwrap();
            let slot = &mut slots[index];
            slot.started = true;
            slot.draining = false;
            Slot {
                node: slot.node.clone(),
                client,
                edge: None,
                started: true,
                draining: false,
                base_permille: Arc::clone(&slot.base_permille),
                leg_permille: Arc::clone(&slot.leg_permille),
                rebalance: slot.rebalance,
            }
        };
        let edge = Arc::new(start_edge(&slot, client).await);
        self.slots.lock().unwrap()[index].edge = Some(edge);
    }

    /// Set edge `index`'s load to `load_permille`, all of it its legs' (a
    /// cost a leg such that its legs now make that load): as listeners
    /// move, its load moves with them, as a rebalance reckons it.
    fn set_load(&self, index: usize, load_permille: u32) {
        let slots = self.slots.lock().unwrap();
        let slot = &slots[index];
        let legs = slot
            .edge
            .as_ref()
            .and_then(|edge| edge.stats.legs())
            .map_or(0, |legs| legs.subscribers)
            .max(1);
        slot.base_permille.store(0, Ordering::Relaxed);
        slot.leg_permille
            .store(load_permille / legs as u32, Ordering::Relaxed);
    }

    /// Set edge `index`'s load besides its legs (before a join: fewer legs
    /// land on a loaded edge).
    fn set_base(&self, index: usize, base_permille: u32) {
        self.slots.lock().unwrap()[index]
            .base_permille
            .store(base_permille, Ordering::Relaxed);
    }

    fn load(&self, index: usize) -> u32 {
        let slots = self.slots.lock().unwrap();
        let slot = &slots[index];
        let legs = slot
            .edge
            .as_ref()
            .and_then(|edge| edge.stats.legs())
            .map_or(0, |legs| legs.subscribers);
        slot.load_permille(legs)
    }

    fn client(&self, index: usize) -> SocketAddr {
        self.slots.lock().unwrap()[index].client
    }

    async fn shutdown(mut self) {
        self.publishing.store(false, Ordering::Relaxed);
        if let Some(publisher) = self.publisher.take() {
            let _ = tokio::task::spawn_blocking(move || publisher.join()).await;
        }
        self.advertiser.abort();
        let _ = (&mut self.advertiser).await;
        for index in 0..4 {
            let running = self.slots.lock().unwrap()[index].edge.is_some();
            if running {
                self.take(index).await.kill().await;
            }
        }
        for entry in std::mem::take(&mut self.entries) {
            let _ = entry.shutdown.send(());
            let _ = entry.task.await;
            entry.mesh.shutdown();
        }
    }
}

/// The publisher, on a thread of its own: each part made once (the same
/// bytes at every edge, as two forwarders feed them) and sent to every edge
/// that runs, back to back (each edge's two forwarder sockets, as
/// [`LaneEdge::send`] sends an audio part), at the payload's pace.
fn publish(
    payload: Payload,
    slots: Arc<std::sync::Mutex<Vec<Slot>>>,
    published: Arc<AtomicU64>,
    sent: Arc<std::sync::Mutex<Vec<u64>>>,
    publishing: Arc<AtomicBool>,
) {
    let start = Instant::now();
    let mut sequence = 0_u64;
    while publishing.load(Ordering::Relaxed) {
        let due = start + Duration::from_micros(payload.part_us() * sequence);
        if let Some(wait) = due.checked_duration_since(Instant::now()) {
            std::thread::sleep(wait);
        }
        let (envelope, _) = payload.envelope(STREAM, sequence);
        let (sources, repairs) = payload.symbols(&envelope);
        let edges: Vec<Arc<LaneEdge>> = slots
            .lock()
            .unwrap()
            .iter()
            .filter_map(|slot| slot.edge.clone())
            .collect();
        sent.lock().unwrap().push(now_us());
        for edge in &edges {
            for symbol in &sources {
                send_from(edge.forwarder_fd(0), symbol, edge.relay_port(0, false));
                send_from(edge.forwarder_fd(1), symbol, edge.relay_port(1, false));
            }
            for symbol in &repairs {
                send_from(edge.forwarder_fd(1), symbol, edge.relay_port(1, false));
            }
        }
        drop(edges);
        published.store(sequence, Ordering::Relaxed);
        sequence += 1;
    }
}

/// One datagram from a forwarder's socket (a blocking send: the publisher
/// thread is not a task).
fn send_from(fd: std::os::fd::RawFd, datagram: &[u8], to: SocketAddr) {
    let SocketAddr::V4(to) = to else {
        unreachable!("loopback")
    };
    let address = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: to.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from(*to.ip()).to_be(),
        },
        sin_zero: [0; 8],
    };
    // SAFETY: a valid fd, a buffer and an address that live for the call.
    unsafe {
        libc::sendto(
            fd,
            datagram.as_ptr().cast(),
            datagram.len(),
            0,
            std::ptr::from_ref(&address).cast(),
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        );
    }
}

/// The edges' advertisements, as the entries hear them: every edge that
/// runs, its legs, its load and whether it drains. A killed edge's last
/// advertisement stays fresh for its few seconds, as it does in a region.
async fn advertise(
    slots: Arc<std::sync::Mutex<Vec<Slot>>>,
    routers: Vec<AppRouter>,
    published: Arc<AtomicU64>,
) {
    loop {
        let head = published.load(Ordering::Relaxed);
        // The entries hear every edge, and so does each edge (its
        // rebalance plans over the region's).
        let mut heard = routers.clone();
        let snapshots: Vec<MeshSnapshot> = slots
            .lock()
            .unwrap()
            .iter()
            .filter_map(|slot| {
                let edge = slot.edge.as_ref()?;
                heard.push(edge.router.clone());
                let subscribers = edge.stats.legs().map_or(0, |legs| legs.subscribers);
                Some(edge_snapshot(slot, head, subscribers))
            })
            .collect();
        for router in &heard {
            for snapshot in &snapshots {
                router.telemetry.ingest_snapshot(snapshot.clone()).await;
            }
        }
        tokio::time::sleep(ADVERTISE_EVERY).await;
    }
}

async fn until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(limit, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}: not within {limit:?}"));
}

/// How a listener holds its edges.
#[derive(Debug, Clone)]
struct ListenerConfig {
    sid: String,
    paths: u8,
    spare: bool,
    /// Datagram loss of its media from every edge (a lossy last mile).
    loss: f64,
}

impl ListenerConfig {
    fn two_paths(sid: impl Into<String>) -> Self {
        Self {
            sid: sid.into(),
            paths: 2,
            spare: true,
            loss: 0.0,
        }
    }
}

/// What the test can see of a listener while it runs.
#[derive(Debug, Default)]
struct Live {
    /// Legs that delivered within the window, and the legs held.
    delivering: AtomicU8,
    held: AtomicU8,
    single_path: AtomicBool,
    /// It has delivered parts on two legs together.
    two_seen: AtomicBool,
    stop: AtomicBool,
    /// The director's legs and spare, for a failure's message, and the
    /// nodes of the legs it holds that are not leaving.
    state: std::sync::Mutex<String>,
    legs: std::sync::Mutex<Vec<String>>,
    /// Its socket's address.
    addr: std::sync::Mutex<Option<SocketAddr>>,
}

/// What a listener received and what its director did.
#[derive(Debug)]
struct ListenerRun {
    sid: String,
    /// Each part's first arrival: its kernel receive time (Unix µs) and
    /// the edge of the datagram that brought it.
    parts: BTreeMap<u64, (u64, String)>,
    delivered_twice: u64,
    gaps: u64,
    replacements: Vec<Replacement>,
    stats: DirectorStats,
    single_path: SinglePath,
    /// Its single-path spans, as Unix µs.
    single_spans_us: Vec<(u64, u64)>,
    nodes_held: BTreeSet<String>,
    /// Holes given up, and NACKs sent.
    abandoned: u64,
    nacked: u64,
    /// Its own leg events (Unix µs): each replacement, from the `MOVE` or
    /// the last media of the leg it replaced to its end, and each span on
    /// one path. What a change can do to this listener, it does then.
    events_us: Vec<(u64, u64)>,
}

/// Datagram loss, deterministic per listener.
struct Loss(u64, u64);

impl Loss {
    fn new(seed: u64, loss: f64) -> Self {
        Self(
            seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1,
            (loss * f64::from(u32::MAX)) as u64,
        )
    }

    fn lost(&mut self) -> bool {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 & u64::from(u32::MAX)) < self.1
    }
}

/// Receive one datagram with the kernel's receive time (Unix µs).
async fn recv_stamped(
    socket: &UdpSocket,
    buf: &mut [u8],
) -> std::io::Result<(usize, SocketAddr, u64)> {
    use std::os::fd::AsRawFd;
    socket
        .async_io(tokio::io::Interest::READABLE, || {
            let mut address: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            let mut control = [0_u64; 8];
            let mut iov = libc::iovec {
                iov_base: buf.as_mut_ptr().cast(),
                iov_len: buf.len(),
            };
            // SAFETY: every pointer is to a buffer that lives for the call.
            let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
            message.msg_name = std::ptr::from_mut(&mut address).cast();
            message.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            message.msg_iov = &mut iov;
            message.msg_iovlen = 1;
            message.msg_control = control.as_mut_ptr().cast();
            message.msg_controllen = std::mem::size_of_val(&control) as _;
            let len =
                unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, libc::MSG_DONTWAIT) };
            if len < 0 {
                return Err(std::io::Error::last_os_error());
            }
            let mut kernel_us = now_us();
            // SAFETY: the kernel filled msg_control with well-formed cmsg
            // headers up to msg_controllen.
            unsafe {
                let mut cmsg = libc::CMSG_FIRSTHDR(&message);
                while !cmsg.is_null() {
                    if (*cmsg).cmsg_level == libc::SOL_SOCKET
                        && (*cmsg).cmsg_type == libc::SCM_TIMESTAMPNS
                    {
                        let time = std::ptr::read_unaligned(
                            libc::CMSG_DATA(cmsg).cast::<libc::timespec>(),
                        );
                        kernel_us = (time.tv_sec as u64) * 1_000_000 + time.tv_nsec as u64 / 1_000;
                    }
                    cmsg = libc::CMSG_NXTHDR(&message, cmsg);
                }
            }
            // SAFETY: the kernel wrote an IPv4 address (the socket is IPv4).
            let address: libc::sockaddr_in =
                unsafe { std::ptr::read(std::ptr::from_ref(&address).cast()) };
            let peer = SocketAddr::from((
                Ipv4Addr::from(u32::from_be(address.sin_addr.s_addr)),
                u16::from_be(address.sin_port),
            ));
            Ok((len as usize, peer, kernel_us))
        })
        .await
}

/// One listener, as `needletail-udp-client` runs the director over the
/// session, until told to stop and nothing more comes for [`QUIET`].
async fn listen(
    region_port: u16,
    entries: Vec<String>,
    config: ListenerConfig,
    live: Arc<Live>,
) -> ListenerRun {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let _ = SockRef::from(&socket).set_recv_buffer_size(8 << 20);
    *live.addr.lock().unwrap() = socket.local_addr().ok();
    {
        use std::os::fd::AsRawFd;
        let on: libc::c_int = 1;
        // SAFETY: a valid fd and an int option of its size.
        unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_TIMESTAMPNS,
                (&on as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    let mut session = UdpSession::new(SessionConfig::new(STREAM), Instant::now());
    let mut director_config = DirectorConfig::new(STREAM, config.sid.clone(), entries);
    director_config.paths = config.paths;
    director_config.spare = config.spare;
    director_config.assignment_udp_port = region_port;
    director_config.udp_port = Some(region_port);
    director_config.nonce_seed = config
        .sid
        .bytes()
        .fold(0, |hash, byte| hash * 31 + u64::from(byte));
    let mut director = Director::new(director_config, Instant::now());
    let mut loss = Loss::new(director.config.nonce_seed, config.loss);
    let mut run = ListenerRun {
        sid: config.sid.clone(),
        parts: BTreeMap::new(),
        delivered_twice: 0,
        gaps: 0,
        replacements: Vec::new(),
        stats: DirectorStats::default(),
        single_path: SinglePath::new(director.config.token_wait),
        single_spans_us: Vec::new(),
        nodes_held: BTreeSet::new(),
        abandoned: 0,
        nacked: 0,
        events_us: Vec::new(),
    };
    let mut nodes: Vec<String> = Vec::new();
    let mut lasts: Vec<(LegKey, Instant)> = Vec::new();
    let mut outbox: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
    let mut actions: Vec<Action> = Vec::new();
    let mut buf = vec![0_u8; 65_536];
    let mut last_part_at = Instant::now();
    async fn send_all(
        socket: &UdpSocket,
        outbox: &mut Vec<(SocketAddr, Vec<u8>)>,
        actions: &mut Vec<Action>,
    ) {
        for (to, datagram) in outbox.drain(..) {
            let _ = socket.send_to(&datagram, to).await;
        }
        for action in actions.drain(..) {
            // HTTPS is the hedge of a UDP request that went unanswered:
            // here the UDP answer is the assignment, as for the probe.
            if let Action::Send { to, datagram } = action {
                let _ = socket.send_to(&datagram, to).await;
            }
        }
    }
    let close_spans = |run: &mut ListenerRun| {
        let spans = &run.single_path.spans;
        while run.single_spans_us.len() < spans.len() {
            let (from, to) = spans[run.single_spans_us.len()];
            let unix = |at: Instant| {
                now_us()
                    .saturating_sub(Instant::now().saturating_duration_since(at).as_micros() as u64)
            };
            run.single_spans_us.push((unix(from), unix(to)));
        }
    };
    let mut stopped_at: Option<Instant> = None;
    let mut next_state = Instant::now();
    loop {
        let now = Instant::now();
        if live.stop.load(Ordering::Relaxed) {
            // The stream ends now: its silence is no time on one path.
            if stopped_at.is_none() {
                run.single_path.close(now);
            }
            let since = *stopped_at.get_or_insert(now);
            // Nothing more comes once the stream has ended; a listener
            // that still receives is stopped after 3 s all the same.
            if now.duration_since(last_part_at) >= QUIET
                || now.duration_since(since) >= Duration::from_secs(3)
            {
                break;
            }
        }
        session.queued = {
            use std::os::fd::AsRawFd;
            let mut queued: libc::c_int = 0;
            // SAFETY: FIONREAD writes one int to the pointer given.
            let result = unsafe { libc::ioctl(socket.as_raw_fd(), libc::FIONREAD, &mut queued) };
            result == 0 && queued > 0
        };
        session.poll_legs(now, &mut |to, datagram| {
            outbox.push((to, datagram.to_vec()))
        });
        director.poll(&mut session, now, &mut actions);
        send_all(&socket, &mut outbox, &mut actions).await;
        let interval = session.receiver.compact.part_interval().unwrap_or_default();
        let window = Duration::from_millis(20).max(interval * 8);
        lasts.retain(|(key, _)| director.legs().iter().any(|leg| leg.key == *key));
        if stopped_at.is_none() {
            single_path_observe(&mut run.single_path, &director, &lasts, now, window);
        }
        close_spans(&mut run);
        let delivering = lasts
            .iter()
            .filter(|(_, at)| now.saturating_duration_since(*at) <= window)
            .count();
        live.delivering.store(delivering as u8, Ordering::Relaxed);
        live.held
            .store(director.legs().len() as u8, Ordering::Relaxed);
        live.single_path
            .store(run.single_path.is_open(), Ordering::Relaxed);
        if delivering >= 2 {
            live.two_seen.store(true, Ordering::Relaxed);
        }
        for leg in director.legs() {
            if !run.nodes_held.contains(&leg.node) {
                run.nodes_held.insert(leg.node.clone());
            }
        }
        if now >= next_state {
            next_state = now + Duration::from_millis(100);
            let legs: Vec<String> = director
                .legs()
                .iter()
                .map(|leg| {
                    format!(
                        "{}{}{}{}{}",
                        leg.node,
                        if leg.live { "" } else { " (not live)" },
                        leg.leaving
                            .as_ref()
                            .map_or(String::new(), |(why, _)| format!(" leaving {why}")),
                        leg.hedge_for
                            .map_or(String::new(), |key| format!(" hedge for {}", key.0)),
                        leg.stands_for
                            .map_or(String::new(), |key| format!(" for {}", key.0)),
                    )
                })
                .collect();
            *live.legs.lock().unwrap() = director
                .legs()
                .iter()
                .filter(|leg| leg.leaving.is_none() && leg.hedge_for.is_none())
                .map(|leg| leg.node.clone())
                .collect();
            *live.state.lock().unwrap() = format!(
                "{}: legs {legs:?}, spare {:?}, want {}, skipped {:?}, delivering {delivering}",
                config.sid,
                director.spare(),
                director.want(),
                director.skipped(now)
            );
        }
        let wake = session
            .next_wake()
            .min(director.next_wake())
            .min(now + Duration::from_millis(20));
        let Ok(Ok((len, peer, kernel_us))) = tokio::time::timeout(
            wake.saturating_duration_since(now),
            recv_stamped(&socket, &mut buf),
        )
        .await
        else {
            continue;
        };
        let datagram = &buf[..len];
        let now = Instant::now();
        if director.on_datagram(&mut session, peer, datagram, now, &mut actions) {
            send_all(&socket, &mut outbox, &mut actions).await;
            continue;
        }
        if config.loss > 0.0
            && av_mesh::client_udp::Control::decode(datagram).is_none()
            && loss.lost()
        {
            continue;
        }
        let outcome = session.on_datagram(peer, datagram, now, &mut |to, datagram| {
            outbox.push((to, datagram.to_vec()))
        });
        match outcome {
            Received::Media => {}
            Received::Subscribed { .. } if session.has_objects() => {}
            _ => continue,
        }
        let leg = director
            .legs()
            .iter()
            .find(|leg| leg.addr == peer)
            .map(|leg| (leg.key, leg.node.clone()));
        let edge = match &leg {
            Some((key, node)) => {
                if outcome == Received::Media {
                    match lasts.iter_mut().find(|(held, _)| held == key) {
                        Some(entry) => entry.1 = now,
                        None => lasts.push((*key, now)),
                    }
                }
                if !nodes.contains(node) {
                    nodes.push(node.clone());
                }
                node.clone()
            }
            None => String::new(),
        };
        while let Some(object) = session.next_object() {
            match object.kind {
                UdpObjectKind::Part => {
                    last_part_at = now;
                    if run
                        .parts
                        .insert(object.part, (kernel_us, edge.clone()))
                        .is_some()
                    {
                        run.delivered_twice += 1;
                    }
                }
                UdpObjectKind::Gap => run.gaps += 1,
                UdpObjectKind::Init => {}
            }
        }
    }
    director.leave(&mut session, Instant::now(), &mut actions);
    send_all(&socket, &mut outbox, &mut actions).await;
    run.single_path.close(Instant::now());
    close_spans(&mut run);
    run.replacements = director.replacements.clone();
    let unix = |at: Instant| {
        now_us().saturating_sub(Instant::now().saturating_duration_since(at).as_micros() as u64)
    };
    run.events_us = run
        .replacements
        .iter()
        .map(|replacement| {
            let end = unix(replacement.at);
            let lasted = replacement
                .handover
                .or(replacement.exposure)
                .unwrap_or_default();
            (end.saturating_sub(lasted.as_micros() as u64), end)
        })
        .chain(run.single_spans_us.iter().copied())
        .collect();
    run.stats = director.stats.clone();
    run.abandoned = session.receiver.compact.stats.abandoned;
    run.nacked = session.receiver.compact.stats.nacked;
    run
}

fn single_path_observe(
    path: &mut SinglePath,
    director: &Director,
    lasts: &[(LegKey, Instant)],
    now: Instant,
    window: Duration,
) {
    path.observe(
        now,
        window,
        director.legs().iter().filter_map(|leg| {
            lasts
                .iter()
                .find(|(key, _)| *key == leg.key)
                .map(|(_, at)| *at)
        }),
        director.want() >= 2,
    );
}

/// The listeners of a test.
struct Crowd {
    lives: Vec<Arc<Live>>,
    tasks: Vec<tokio::task::JoinHandle<ListenerRun>>,
}

impl Crowd {
    async fn join(region: &Region, configs: Vec<ListenerConfig>) -> Self {
        let mut lives = Vec::new();
        let mut tasks = Vec::new();
        for config in configs {
            let live = Arc::new(Live::default());
            tasks.push(tokio::spawn(listen(
                region.port,
                region.entry_urls(),
                config,
                Arc::clone(&live),
            )));
            lives.push(live);
        }
        let crowd = Self { lives, tasks };
        until(
            "every listener delivers on two legs",
            Duration::from_secs(10),
            || {
                crowd
                    .lives
                    .iter()
                    .all(|live| live.two_seen.load(Ordering::Relaxed))
            },
        )
        .await;
        crowd
    }

    fn describe(&self) -> String {
        self.lives
            .iter()
            .map(|live| live.state.lock().unwrap().clone())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Every listener has two delivering legs and is not on one path.
    fn all_on_two_paths(&self) -> bool {
        self.lives.iter().all(|live| {
            live.delivering.load(Ordering::Relaxed) >= 2
                && !live.single_path.load(Ordering::Relaxed)
        })
    }

    async fn finish(self) -> Vec<ListenerRun> {
        for live in &self.lives {
            live.stop.store(true, Ordering::Relaxed);
        }
        let mut runs = Vec::new();
        for task in self.tasks {
            runs.push(task.await.unwrap());
        }
        runs
    }
}

/// A change's marks (Unix µs), as the change command prints them.
#[derive(Debug, Clone, Copy)]
struct Marks {
    start: u64,
    end: u64,
}

/// The referee's rule on one listener (B 3.11, CW): what it found.
#[derive(Debug)]
struct Judged {
    sid: String,
    missing: Vec<u64>,
    first: u64,
    /// The longest silence inside the window at the listener's own leg
    /// events, and anywhere inside it; the longest outside it.
    gap_at_events_us: u64,
    gap_inside_us: u64,
    gap_outside_us: u64,
    /// When the longest silence inside ended (Unix µs).
    gap_inside_at: u64,
}

/// A silence this long is an interruption (B 2.9 `--interrupt-ms`).
const INTERRUPTION_US: u64 = 30_000;

/// A silence counts as a leg event's when it ends this long before the
/// event begins, or up to this long after it ends.
const EVENT_BEFORE_US: u64 = 20_000;
const EVENT_AFTER_US: u64 = 250_000;

/// Judge `run` against `marks`: parts from its first to `last` (the last
/// part every listener must have), and the longest silence between first
/// arrivals inside the window and outside it (from 1 s after its first
/// part: the join is no steady state). A silence is judged on the
/// source's timeline: the gap between two first arrivals less the gap
/// between those parts' sending, plus one part interval. The in-process
/// publisher is a thread among the listeners' and the edges', and its own
/// pacing stalls would otherwise be read as the path's.
fn judge(run: &ListenerRun, marks: Marks, last: u64, sent: &[u64], part_us: u64) -> Judged {
    let first = *run
        .parts
        .keys()
        .next()
        .unwrap_or_else(|| panic!("{}: no part", run.sid));
    let missing: Vec<u64> = (first..=last)
        .filter(|part| !run.parts.contains_key(part))
        .collect();
    let mut arrivals: Vec<(u64, u64)> = run
        .parts
        .range(first..=last)
        .filter_map(|(part, (at, _))| Some((*at, *sent.get(*part as usize)?)))
        .collect();
    arrivals.sort_unstable();
    let steady_from = arrivals.first().map_or(0, |(at, _)| *at) + 1_000_000;
    let from = marks.start.saturating_sub(WINDOW_BEFORE.as_micros() as u64);
    let to = marks.end + WINDOW_AFTER.as_micros() as u64;
    let (mut inside, mut outside, mut inside_at, mut at_events) = (0_u64, 0_u64, 0_u64, 0_u64);
    let at_event = |at: u64| {
        run.events_us
            .iter()
            .any(|(start, end)| at + EVENT_BEFORE_US >= *start && at <= end + EVENT_AFTER_US)
    };
    for pair in arrivals.windows(2) {
        let ((at_a, sent_a), (at_b, sent_b)) = (pair[0], pair[1]);
        let silence =
            (at_b as i64 - at_a as i64) - (sent_b as i64 - sent_a as i64) + part_us as i64;
        let silence = silence.max(0) as u64;
        if (from..=to).contains(&at_b) {
            if silence > inside {
                inside = silence;
                inside_at = at_b;
            }
            if at_event(at_b) {
                at_events = at_events.max(silence);
            }
        } else if at_a >= steady_from {
            outside = outside.max(silence);
        }
    }
    Judged {
        sid: run.sid.clone(),
        missing,
        first,
        gap_at_events_us: at_events,
        gap_inside_us: inside,
        gap_outside_us: outside,
        gap_inside_at: inside_at,
    }
}

/// The rule's first two clauses for every listener. Nothing missing. No
/// silence over the steady silence plus one part where the change can
/// cause one: at the listener's own leg events (its replacements and spans
/// on one path, from 20 ms before to 250 ms after each). And no silence of
/// 30 ms or more (an interruption, B 2.9) anywhere inside the window.
///
/// The steady silence is the longest outside the window over every
/// listener of the region. On this shared host a silence of 2 to 3 ms
/// comes every few listener-minutes at random, in the window and out of it
/// alike, and listeners that no change touched show it as often as the
/// others: one listener's seconds of steady state cannot price that, which
/// is why the referee scores a change only against 120 null windows or
/// more (300 s of probe). Judged at the listener's own events, the clause
/// asks what the change did; the interruption bound keeps every other
/// moment of the window honest.
fn assert_the_rule(
    name: &str,
    runs: &[ListenerRun],
    marks: Marks,
    last: u64,
    sent: &[u64],
    part_us: u64,
) {
    let judged: Vec<Judged> = runs
        .iter()
        .map(|run| judge(run, marks, last, sent, part_us))
        .collect();
    let steady = judged
        .iter()
        .map(|judged| judged.gap_outside_us)
        .max()
        .unwrap_or(0);
    for (run, judged) in runs.iter().zip(&judged) {
        assert!(
            judged.missing.is_empty(),
            "{name}: {} missed {} parts from {} to {last}, first {:?}: {:?}",
            judged.sid,
            judged.missing.len(),
            judged.first,
            &judged.missing[..judged.missing.len().min(20)],
            run.replacements
        );
        assert_eq!(
            run.delivered_twice, 0,
            "{name}: {} delivered a part twice",
            run.sid
        );
        assert_eq!(run.gaps, 0, "{name}: {} got gaps", run.sid);
        assert!(
            judged.gap_at_events_us <= steady + part_us,
            "{name}: {} had a silence of {} us at its leg events against {} us in steady state ({} us its own): {:?}, single path {:?}",
            judged.sid,
            judged.gap_at_events_us,
            steady,
            judged.gap_outside_us,
            run.replacements,
            run.single_spans_us
        );
        assert!(
            judged.gap_inside_us < INTERRUPTION_US,
            "{name}: {} was interrupted for {} us (ending at {}, the change {:?})",
            judged.sid,
            judged.gap_inside_us,
            judged.gap_inside_at,
            marks
        );
    }
}

/// The time each listener spent on one path inside the window, its
/// longest span (µs).
fn single_path_in(run: &ListenerRun, marks: Marks) -> (u64, u64) {
    let from = marks.start.saturating_sub(WINDOW_BEFORE.as_micros() as u64);
    let to = marks.end + WINDOW_AFTER.as_micros() as u64;
    run.single_spans_us
        .iter()
        .filter(|(start, end)| *end >= from && *start <= to)
        .fold((0, 0), |(total, longest), (start, end)| {
            let span = end.saturating_sub(*start);
            (total + span, longest.max(span))
        })
}

/// Drain edge `index` under `crowd` until it has no leg, saying where each
/// listener stands if it does not end within `limit`.
async fn drain_watched(region: &Region, crowd: &Crowd, index: usize, limit: Duration) {
    region.set_draining(index, true).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let drained = tokio::time::timeout(limit, async {
        while region.legs_at(index) > 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    if drained.is_err() {
        panic!(
            "{} still has {} legs {limit:?} into its drain:\n{}",
            region.node(index),
            region.legs_at(index),
            crowd.describe()
        );
    }
}

/// Hold the stream through `seconds` of steady state.
async fn steady(seconds: u64) {
    tokio::time::sleep(Duration::from_secs(seconds)).await;
}

/// Steady state after the window: the steady silence is measured on both
/// sides of a change, as this host's load drifts.
const STEADY_AFTER: Duration = Duration::from_secs(6);

/// Stop the listeners once the window and the steady state after it have
/// passed, then the publisher; the last part every listener must have.
async fn finish(region: &Region, crowd: Crowd, marks: Marks) -> (Vec<ListenerRun>, u64, Vec<u64>) {
    let close = marks.end + (WINDOW_AFTER + STEADY_AFTER).as_micros() as u64;
    let wait = close.saturating_sub(now_us());
    tokio::time::sleep(Duration::from_micros(wait)).await;
    // The listeners are told, then the stream ends: they stop once
    // nothing more comes.
    for live in &crowd.lives {
        live.stop.store(true, Ordering::Relaxed);
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    region.publishing.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let last = region.published.load(Ordering::Relaxed);
    let runs = crowd.finish().await;
    let sent = region.sent.lock().unwrap().clone();
    (runs, last, sent)
}

/// Each of `replacements` a hand-over within the level wait and this
/// host's margin (1.5 s; B 3.11: level at 32 parts after 3.5 r).
fn assert_handovers<'a>(
    run: &ListenerRun,
    replacements: impl IntoIterator<Item = &'a Replacement>,
) {
    for replacement in replacements {
        assert_eq!(
            replacement.kind,
            ReplacementKind::Handover,
            "{}: {replacement:?}",
            run.sid
        );
        let handover = replacement
            .handover
            .unwrap_or_else(|| panic!("{}: no hand-over time: {replacement:?}", run.sid));
        assert!(
            handover <= Duration::from_millis(1_500),
            "{}: {replacement:?}",
            run.sid
        );
    }
}

/// The listeners that held a leg at `node` during the run.
fn held_at<'a>(runs: &'a [ListenerRun], node: &str) -> Vec<&'a ListenerRun> {
    runs.iter()
        .filter(|run| run.nodes_held.contains(node))
        .collect()
}

fn listeners(prefix: &str, count: usize) -> Vec<ListenerConfig> {
    (0..count)
        .map(|index| ListenerConfig::two_paths(format!("{prefix}-{index}")))
        .collect()
}

// ------------------------------------------------------------------ tests

/// An edge killed under two-path listeners (B 3.11 "Edge kill"): the other
/// leg carries on, the spare is hedged at 50 to 70 ms, the dead leg is
/// failed at 200 ms. Nothing missing, no gap over the steady gap plus one
/// part, every listener that had a leg there on one path for at most
/// 70 ms + r and a few ms of this host, its exposure likewise
/// ([`assert_kill_replaced`]).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill() {
    let _serial = SERIAL.lock().await;
    let region = Region::start(120, Payload::Pcm, [100; 4]).await;
    let crowd = Crowd::join(&region, listeners("kill", 8)).await;
    steady(STEADY_BEFORE).await;
    let killed = region.node(0);
    let start = now_us();
    region.kill(0).await;
    let marks = Marks {
        start,
        end: now_us(),
    };
    let (runs, last, sent) = finish(&region, crowd, marks).await;
    region.shutdown().await;
    assert_the_rule("kill", &runs, marks, last, &sent, Payload::Pcm.part_us());
    assert_kill_replaced("kill", &runs, &killed, marks);
}

/// A kill's bound on one path and on each exposure: 70 ms + r (B 3.11; 80 ms
/// + r in section 6) at r of loopback, with this host's few ms.
const KILL_BOUND_US: u64 = 100_000;

/// An edge drained under two-path listeners (B 3.11 "Edge drain"): each
/// leg there moves make before break, released when its replacement is
/// level. No time on one path, nothing cut at the edge's grace, every
/// hand-over within the level wait and the drain's end within the drain's
/// spread and the hand-overs (3.2 s + 3.5 r, asserted at 4 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain() {
    let _serial = SERIAL.lock().await;
    let region = Region::start(128, Payload::Pcm, [100; 4]).await;
    let crowd = Crowd::join(&region, listeners("drain", 8)).await;
    steady(STEADY_BEFORE).await;
    let drained = region.node(0);
    let edge = region.edge(0);
    let start = now_us();
    drain_watched(&region, &crowd, 0, Duration::from_secs(4)).await;
    let marks = Marks {
        start,
        end: now_us(),
    };
    region.set_draining(0, false).await;
    let (runs, last, sent) = finish(&region, crowd, marks).await;
    assert_eq!(
        edge.stats.moves_expired.load(Ordering::Relaxed),
        0,
        "nothing cut at the grace"
    );
    drop(edge);
    region.shutdown().await;
    assert_the_rule("drain", &runs, marks, last, &sent, Payload::Pcm.part_us());
    let hit = held_at(&runs, &drained);
    assert!(!hit.is_empty(), "a listener had a leg at {drained}");
    for run in &runs {
        assert_eq!(
            single_path_in(run, marks).0,
            0,
            "{}: {:?}",
            run.sid,
            run.single_spans_us
        );
    }
    for run in hit {
        let handovers: Vec<&Replacement> = run
            .replacements
            .iter()
            .filter(|replacement| replacement.from == drained)
            .collect();
        assert!(!handovers.is_empty(), "{}: moved off {drained}", run.sid);
        assert_handovers(run, handovers);
    }
}

/// An edge killed and back at its address 2 s later, empty, with new token
/// keys (B 3.11 "Edge crash-restart"): as a kill. Every listener that had a
/// leg there has it replaced from its spare beside its other leg, with an
/// exposure and a time on one path within the kill's bound; the listeners
/// skip the edge 30 s and the region serves them throughout; the restarted
/// edge counts no request with a dead token.
///
/// A restart 2 s later does not reach the rules for an edge back within a
/// leg's 200 ms (its refresh answered `TOKEN` and admitted as new at the
/// head, B 4) or a hedge to a restarted spare (B 3.8): they are left to the
/// lab's `udp-edge-crash-restart` under a supervisor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_restart() {
    let _serial = SERIAL.lock().await;
    let region = Region::start(136, Payload::Pcm, [100; 4]).await;
    let crowd = Crowd::join(&region, listeners("crash", 8)).await;
    steady(STEADY_BEFORE).await;
    let crashed = region.node(1);
    let address = region.client(1);
    let start = now_us();
    region.kill(1).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    region.start_edge(1).await;
    assert_eq!(region.client(1), address, "back at its address");
    let marks = Marks {
        start,
        end: now_us(),
    };
    let restarted = region.edge(1);
    let (runs, last, sent) = finish(&region, crowd, marks).await;
    let bad_tokens = restarted.stats.bad_tokens.load(Ordering::Relaxed);
    drop(restarted);
    region.shutdown().await;
    assert_the_rule(
        "crash_restart",
        &runs,
        marks,
        last,
        &sent,
        Payload::Pcm.part_us(),
    );
    assert_eq!(
        bad_tokens, 0,
        "no request with a dead token at the restarted edge"
    );
    assert_kill_replaced("crash_restart", &runs, &crashed, marks);
}

/// The kill clauses (B 3.11, section 6): a listener had a leg at `killed`;
/// each such leg was replaced from the spare beside the other leg (a
/// hedge), its exposure present and, like the listener's longest span on
/// one path, within 70 ms + r and this host's few ms (asserted at 100 ms).
fn assert_kill_replaced(name: &str, runs: &[ListenerRun], killed: &str, marks: Marks) {
    let hit = held_at(runs, killed);
    assert!(!hit.is_empty(), "{name}: a listener had a leg at {killed}");
    for run in hit {
        let (_, longest) = single_path_in(run, marks);
        assert!(
            longest <= KILL_BOUND_US,
            "{name}: {}: {longest} us on one path: {:?}",
            run.sid,
            run.single_spans_us
        );
        let replaced = run
            .replacements
            .iter()
            .filter(|replacement| replacement.from == killed)
            .collect::<Vec<_>>();
        assert!(
            !replaced.is_empty(),
            "{name}: {}: its leg at {killed} was replaced",
            run.sid
        );
        for replacement in replaced {
            assert_eq!(
                replacement.kind,
                ReplacementKind::Hedge,
                "{name}: {}: {replacement:?}",
                run.sid
            );
            let exposure = replacement
                .exposure
                .unwrap_or_else(|| panic!("{name}: {}: no exposure: {replacement:?}", run.sid));
            assert!(
                exposure <= Duration::from_micros(KILL_BOUND_US),
                "{name}: {}: {replacement:?}",
                run.sid
            );
        }
    }
}

/// Two edges drained at once (B 3.11): the two edges of one listener's
/// legs, so that it replaces both, each make before break; nobody on one
/// path, every replacement a hand-over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_drains() {
    let _serial = SERIAL.lock().await;
    let region = Region::start(144, Payload::Pcm, [100; 4]).await;
    let crowd = Crowd::join(&region, listeners("two-drains", 8)).await;
    steady(STEADY_BEFORE).await;
    let pair: Vec<String> = crowd.lives[0].legs.lock().unwrap().clone();
    assert_eq!(pair.len(), 2, "{}", crowd.describe());
    let indices: Vec<usize> = pair
        .iter()
        .map(|node| node.trim_start_matches("edge-").parse::<usize>().unwrap() - 1)
        .collect();
    let start = now_us();
    for index in &indices {
        region.set_draining(*index, true).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::timeout(Duration::from_secs(8), async {
        while indices.iter().any(|index| region.legs_at(*index) > 0) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{pair:?} still have legs:\n{}", crowd.describe()));
    let marks = Marks {
        start,
        end: now_us(),
    };
    for index in &indices {
        region.set_draining(*index, false).await;
    }
    let (runs, last, sent) = finish(&region, crowd, marks).await;
    region.shutdown().await;
    assert_the_rule(
        "two_drains",
        &runs,
        marks,
        last,
        &sent,
        Payload::Pcm.part_us(),
    );
    for run in &runs {
        assert_eq!(
            single_path_in(run, marks).0,
            0,
            "{}: {:?}",
            run.sid,
            run.single_spans_us
        );
        assert_handovers(run, &run.replacements);
    }
    // The listener whose two legs were drained replaced both.
    let both = &runs[0];
    let moved: BTreeSet<&str> = both.replacements.iter().map(|r| r.from.as_str()).collect();
    assert!(
        pair.iter().all(|node| moved.contains(node.as_str())),
        "{}: {:?}",
        both.sid,
        both.replacements
    );
}

/// A roll of three of the four edges (B 3.11 "Edge roll"): each drained,
/// stopped, started again and back in the region before the next. Before
/// each drain no listener is without a leg or on one path; nothing missing
/// over the whole roll.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn roll_of_three_edges() {
    let _serial = SERIAL.lock().await;
    let region = Region::start(152, Payload::Pcm, [100; 4]).await;
    let crowd = Crowd::join(&region, listeners("roll", 8)).await;
    steady(STEADY_BEFORE).await;
    let start = now_us();
    for index in 0..3 {
        until(
            "every listener on two paths before the drain",
            Duration::from_secs(10),
            || crowd.all_on_two_paths(),
        )
        .await;
        assert!(crowd
            .lives
            .iter()
            .all(|live| live.held.load(Ordering::Relaxed) >= 1));
        drain_watched(&region, &crowd, index, Duration::from_secs(6)).await;
        region.stop(index).await;
        region.start_edge(index).await;
        // Advertised again, warm: the next step waits for two paths.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let marks = Marks {
        start,
        end: now_us(),
    };
    let (runs, last, sent) = finish(&region, crowd, marks).await;
    region.shutdown().await;
    assert_the_rule(
        "roll_of_three_edges",
        &runs,
        marks,
        last,
        &sent,
        Payload::Pcm.part_us(),
    );
    for run in &runs {
        assert_eq!(
            single_path_in(run, marks).0,
            0,
            "{}: {:?}",
            run.sid,
            run.single_spans_us
        );
        assert_handovers(run, &run.replacements);
    }
}

/// C 3.2 against B 3.12 (integration 2): SIGTERM on an edge that holds legs
/// of two-path listeners, and a moved LL-HLS reader that still fetches, is
/// a graceful stop. The edge says it is leaving (`stopping`, advertised as
/// draining), moves each leg make before break, waits until its UDP
/// subscribers have left (each at its `UNSUB` or the grace) and the reader
/// has stopped fetching, then releases its subscriptions and exits; here it
/// is then taken out as the process ends. Nothing is missing, no listener
/// is on one path or interrupted, every leg there is handed over, nothing
/// is cut at the grace, and the stop ends well within its bound (the
/// default `--stop-drain-ms`: drain spread + move grace + 2 s = 14 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sigterm_stop_hands_two_path_listeners_over() {
    let _serial = SERIAL.lock().await;
    let region = Region::start(216, Payload::Pcm, [100; 4]).await;
    let crowd = Crowd::join(&region, listeners("sigterm", 8)).await;
    steady(STEADY_BEFORE).await;
    let stopped_node = region.node(0);
    let edge = region.edge(0);
    assert!(region.legs_at(0) > 0, "{stopped_node} holds legs");
    // A reader redirected at its last reload that still fetches parts it
    // was told about for 1.2 s (`hls_moved_active`, then a read).
    let request = |draining| PlaybackRequest {
        session_id: Some("reader"),
        stream_id: Some(STREAM),
        draining,
    };
    assert_eq!(
        edge.router.edge_load.admit_playback(request(false)),
        PlaybackAdmission::Admitted
    );
    assert!(edge.router.edge_load.egress.release_session("reader"));
    let fetching = edge.router.clone();
    let reader = tokio::spawn(async move {
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(1_200) {
            let _ = fetching.edge_load.admit_playback(request(true));
            let read = fetching.edge_load.begin_read(false);
            tokio::time::sleep(Duration::from_millis(60)).await;
            drop(read);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Instant::now()
    });
    let bound = Duration::from_millis(
        edge_udp::DEFAULT_DRAIN_SPREAD_MS
            + edge_udp::DEFAULT_MOVE_GRACE_MS
            + lifecycle::STOP_DRAIN_SPARE_MS,
    );
    let start = now_us();
    let began = Instant::now();
    // `stopping` is advertised at once: the entries assign the edge nobody.
    region.slots.lock().unwrap()[0].draining = true;
    let stopped = lifecycle::stop_edge(&edge.router, bound, std::future::pending()).await;
    let reader_done = reader.await.unwrap();
    let marks = Marks {
        start,
        end: now_us(),
    };
    assert!(stopped.drained && !stopped.skipped, "{stopped:?}");
    assert!(
        began + stopped.waited >= reader_done + lifecycle::STOP_HOLD,
        "the stop waited for the moved reader: {stopped:?}"
    );
    // The drain spread (2 s), the hand-overs (B 3.11: level 32 parts after
    // 3.5 r) and a second empty: far inside the bound.
    assert!(
        stopped.waited <= Duration::from_secs(6),
        "{stopped:?} against a bound of {bound:?}"
    );
    assert_eq!(region.legs_at(0), 0, "{}", crowd.describe());
    assert_eq!(
        edge.stats.moves_expired.load(Ordering::Relaxed),
        0,
        "nothing cut at the grace"
    );
    drop(edge);
    // The process ends.
    region.stop(0).await;
    let (runs, last, sent) = finish(&region, crowd, marks).await;
    region.shutdown().await;
    assert_the_rule("sigterm", &runs, marks, last, &sent, Payload::Pcm.part_us());
    let hit = held_at(&runs, &stopped_node);
    assert!(!hit.is_empty(), "a listener had a leg at {stopped_node}");
    for run in &runs {
        assert_eq!(
            single_path_in(run, marks).0,
            0,
            "{}: {:?}",
            run.sid,
            run.single_spans_us
        );
    }
    for run in hit {
        let handovers: Vec<&Replacement> = run
            .replacements
            .iter()
            .filter(|replacement| replacement.from == stopped_node)
            .collect();
        assert!(!handovers.is_empty(), "{}: moved off {stopped_node}", run.sid);
        assert_handovers(run, handovers);
    }
}

/// 72 two-path listeners of 2.5 ms Opus on four edges; the edge with the
/// most legs (about 36) is killed. Its legs are replaced through the
/// entries, which apportion movers by headroom to the rebalance target
/// (C4): each survivor takes within 1.3 of its headroom share. Nothing is
/// missing, and no listener of the killed edge is on one path for longer
/// than the dead leg's verdict (200 ms) and its replacement's start.
///
/// The listeners hold no spare here (`--spare false`): with a spare, a
/// dead leg's place goes to the spare that the listener's join was given,
/// not to the entry's apportionment
/// ([`the_36_legs_of_a_killed_edge_with_a_spare`]).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_36_legs_of_a_killed_edge() {
    let killed = kill_the_busiest_of_72(160, false).await;
    for ((index, _), (got, share)) in killed.survivors.iter().zip(killed.shares()) {
        assert!(
            got <= share * 1.3 && got >= share / 1.3,
            "edge-{}: {got} legs against a share of {share:.1}: {:?}",
            index + 1,
            killed.landed
        );
    }
    for replacement in &killed.replacements {
        assert_eq!(
            replacement.kind,
            ReplacementKind::Replace,
            "{replacement:?}"
        );
    }
}

/// The same kill with the client's defaults (a spare): each dead leg's
/// place goes first to the listener's spare, a weighted rendezvous among
/// the edges that are not its other leg, weighted by its headroom under the
/// redundant leg's gate.
/// That is not the entry's apportionment by headroom to the target, so the
/// share clause is not claimed here (raised with the lead, review of B Y5,
/// finding 4); what the design claims is: every leg replaced, as a hedge or,
/// where the spare refused it, through the entry; no survivor over the
/// rebalance target (800) after the kill; nothing missing; the time on one
/// path within the dead leg's verdict and the new leg's start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_36_legs_of_a_killed_edge_with_a_spare() {
    let killed = kill_the_busiest_of_72(196, true).await;
    for replacement in &killed.replacements {
        assert!(
            matches!(
                replacement.kind,
                ReplacementKind::Hedge | ReplacementKind::Replace
            ),
            "{replacement:?}"
        );
    }
    // The spare is chosen by headroom under the redundant leg's gate (the
    // lead's decision of 24 Nov). That does not bring each survivor within
    // 1.3 of its headroom share (integration 2: 13/15/16 against 19.1/14.5/
    // 10.3, from 12/15/17 by headroom to full load): a spare is drawn per
    // listener from the edges that are not its legs, so an edge seldom a
    // leg is overrepresented among a dead edge's spares, and a draw of ~44
    // among three spreads by about 30 % at one sigma. The share clause needs
    // the hedge's place apportioned (integration report 2); it is printed,
    // not asserted.
    eprintln!("the_36_legs (spare): taken and shares {:?}", killed.shares());
    for ((index, _), peak) in killed.survivors.iter().zip(&killed.peaks) {
        assert!(
            *peak <= 800,
            "edge-{} reached {peak} permille after the kill: {:?}",
            index + 1,
            killed.landed
        );
    }
}

/// What [`kill_the_busiest_of_72`] found: the killed edge's legs, each
/// survivor's index and legs before the kill, its peak load after it, where
/// the killed edge's legs went, and those replacements.
struct BusiestKilled {
    legs: u64,
    survivors: Vec<(usize, u64)>,
    bases: [u32; 4],
    peaks: Vec<u32>,
    landed: BTreeMap<String, u64>,
    replacements: Vec<Replacement>,
}

impl BusiestKilled {
    /// Each survivor's legs taken and its share of them by headroom to the
    /// target (800) as the entries saw it.
    fn shares(&self) -> Vec<(f64, f64)> {
        let headroom: Vec<f64> = self
            .survivors
            .iter()
            .map(|(index, before)| 800.0 - f64::from(self.bases[*index] + 5 * *before as u32))
            .collect();
        let total: f64 = headroom.iter().sum();
        self.survivors
            .iter()
            .zip(&headroom)
            .map(|((index, _), room)| {
                let got = self
                    .landed
                    .get(&region_node_name(*index))
                    .copied()
                    .unwrap_or(0);
                (got as f64, self.legs as f64 * room / total)
            })
            .collect()
    }
}

/// 72 two-path listeners (`spare` or not) of 2.5 ms Opus on four edges at
/// base loads 100/100/250/400 and 5 permille a leg; the edge with the most
/// legs is killed. Asserts the rule, every leg of the killed edge replaced,
/// and every such listener's time on one path and exposure within the dead
/// leg's verdict (200 ms), the request beside a delivering leg spread over
/// 250 ms (B 3.5 step 3) and the new leg's start (500 ms).
async fn kill_the_busiest_of_72(base: u8, spare: bool) -> BusiestKilled {
    let _serial = SERIAL.lock().await;
    let bases = [100_u32, 100, 250, 400];
    let region = Region::start(base, Payload::Opus, bases).await;
    let configs = (0..72)
        .map(|index| ListenerConfig {
            spare,
            ..ListenerConfig::two_paths(format!("legs36-{index}"))
        })
        .collect();
    let crowd = Crowd::join(&region, configs).await;
    steady(STEADY_BEFORE).await;
    let killed = (0..4).max_by_key(|index| region.legs_at(*index)).unwrap();
    let legs = region.legs_at(killed);
    let node = region.node(killed);
    let survivors: Vec<(usize, u64)> = (0..4)
        .filter(|index| *index != killed)
        .map(|index| (index, region.legs_at(index)))
        .collect();
    let start = now_us();
    region.kill(killed).await;
    let marks = Marks {
        start,
        end: now_us(),
    };
    // Each survivor's peak load over the window after the kill.
    let mut peaks = vec![0_u32; survivors.len()];
    let watched = Instant::now();
    while watched.elapsed() < WINDOW_AFTER {
        tokio::time::sleep(Duration::from_millis(100)).await;
        for ((index, _), peak) in survivors.iter().zip(peaks.iter_mut()) {
            *peak = (*peak).max(region.load(*index));
        }
    }
    let (runs, last, sent) = finish(&region, crowd, marks).await;
    region.shutdown().await;
    assert_the_rule(
        "the_36_legs_of_a_killed_edge",
        &runs,
        marks,
        last,
        &sent,
        Payload::Opus.part_us(),
    );
    let mut landed: BTreeMap<String, u64> = BTreeMap::new();
    let mut replacements = Vec::new();
    for run in &runs {
        for replacement in run.replacements.iter().filter(|r| r.from == node) {
            *landed.entry(replacement.to.clone()).or_default() += 1;
            replacements.push(replacement.clone());
        }
    }
    let moved: u64 = landed.values().sum();
    eprintln!(
        "the_36_legs (spare {spare}): {legs} legs at {node}, {moved} replaced: {landed:?}, survivors {survivors:?}, peaks {peaks:?}"
    );
    assert!(legs >= 30, "{legs} legs at the busiest edge");
    assert_eq!(
        moved, legs,
        "every leg of the killed edge was replaced: {landed:?}"
    );
    for run in held_at(&runs, &node) {
        let (_, longest) = single_path_in(run, marks);
        assert!(longest <= 500_000, "{}: {longest} us on one path", run.sid);
        for replacement in run.replacements.iter().filter(|r| r.from == node) {
            let exposure = replacement
                .exposure
                .unwrap_or_else(|| panic!("{}: no exposure: {replacement:?}", run.sid));
            assert!(
                exposure <= Duration::from_millis(500),
                "{}: {replacement:?}",
                run.sid
            );
        }
    }
    let killed = BusiestKilled {
        legs,
        survivors,
        bases,
        peaks,
        landed,
        replacements,
    };
    eprintln!(
        "the_36_legs (spare {spare}): taken and shares {:?}",
        killed.shares()
    );
    killed
}

/// B 2.6: a token is good in the edge's minute of issue and the next. Four
/// listeners on paths that lose 5 % of their media hold their legs past the
/// minute in which their first tokens die (the change: that minute's
/// start). Each leg renews its token 45 s after issue, so every NACK,
/// refresh and UNSUB carries a live one: the edges count no bad token, no
/// hole is given up, nothing is missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_token_expiry_across_a_stepped_minute_under_five_percent_loss() {
    let _serial = SERIAL.lock().await;
    let region = Region::start(168, Payload::Opus, [100; 4]).await;
    let configs = (0..4)
        .map(|index| ListenerConfig {
            loss: 0.05,
            ..ListenerConfig::two_paths(format!("tokens-{index}"))
        })
        .collect();
    let crowd = Crowd::join(&region, configs).await;
    let joined_minute = now_us() / 60_000_000;
    // The first tokens die as the edges' minute becomes this one.
    let death = (joined_minute + 2) * 60_000_000;
    tokio::time::sleep(Duration::from_micros(death.saturating_sub(now_us()))).await;
    let marks = Marks {
        start: death,
        end: death,
    };
    let (runs, last, sent) = finish(&region, crowd, marks).await;
    let bad_tokens: Vec<u64> = (0..4)
        .map(|index| region.edge(index).stats.bad_tokens.load(Ordering::Relaxed))
        .collect();
    region.shutdown().await;
    assert_the_rule(
        "a_token_expiry_across_a_stepped_minute",
        &runs,
        marks,
        last,
        &sent,
        Payload::Opus.part_us(),
    );
    assert_eq!(bad_tokens, [0; 4], "no request with a dead token");
    for run in &runs {
        assert!(
            run.nacked > 0,
            "{}: the loss was repaired by NACK too",
            run.sid
        );
        assert_eq!(run.abandoned, 0, "{}: no hole given up", run.sid);
    }
}

/// The rebalance replacements of every listener (reason `rebalance`).
fn rebalanced(runs: &[ListenerRun]) -> Vec<(&str, &Replacement)> {
    runs.iter()
        .flat_map(|run| {
            run.replacements
                .iter()
                .filter(|replacement| replacement.reason == "rebalance")
                .map(move |replacement| (run.sid.as_str(), replacement))
        })
        .collect()
}

/// The shape of the rebalance tests: three or two edges serving, a leg
/// costing 40 permille, the edges rebalancing to the region's mean.
fn rebalancing(serving: usize) -> Shape {
    Shape {
        payload: Payload::Pcm,
        bases: [0; 4],
        serving,
        leg_permille: 40,
        rebalance: true,
    }
}

/// Rule R3 (B 3.11 "Rebalance", autoscale up): a fourth edge joins a
/// region of three under 15 two-path listeners. The edges over the mean
/// shed legs older than 10 s, each a hand-over make before break (released
/// only when the new leg is level, else taken back at 3 s); the joined
/// edge takes legs and stays under 900 permille; nothing missing, no
/// silence over the steady state, nobody on one path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebalance_onto_a_joining_edge() {
    let _serial = SERIAL.lock().await;
    let region = Region::start_shaped(176, rebalancing(3)).await;
    let crowd = Crowd::join(&region, listeners("joiner", 15)).await;
    steady(STEADY_BEFORE).await;
    let start = now_us();
    region.start_edge(3).await;
    // Until the joined edge's legs have not changed for three rounds.
    let (mut legs, mut changed, mut peak) = (0, Instant::now(), 0);
    let began = Instant::now();
    while began.elapsed() < Duration::from_secs(30)
        && (legs == 0 || changed.elapsed() < Duration::from_secs(9))
    {
        tokio::time::sleep(Duration::from_millis(100)).await;
        peak = peak.max(region.load(3));
        let now = region.legs_at(3);
        if now != legs {
            legs = now;
            changed = Instant::now();
        }
    }
    let marks = Marks {
        start,
        end: now_us(),
    };
    let (runs, last, sent) = finish(&region, crowd, marks).await;
    region.shutdown().await;
    assert_the_rule(
        "rebalance_onto_a_joining_edge",
        &runs,
        marks,
        last,
        &sent,
        Payload::Pcm.part_us(),
    );
    let moves = rebalanced(&runs);
    eprintln!("rebalance_onto_a_joining_edge: {legs} legs at the joined edge, peak {peak} permille, {moves:?}");
    assert!(legs > 0, "the joined edge took legs");
    assert!(peak < 900, "the joined edge reached {peak} permille");
    assert!(moves
        .iter()
        .any(|(_, r)| r.to == "edge-4" && r.kind == ReplacementKind::Handover));
    for (sid, replacement) in &moves {
        assert!(
            matches!(
                replacement.kind,
                ReplacementKind::Handover | ReplacementKind::Takeback
            ),
            "{sid}: {replacement:?}"
        );
    }
    for run in &runs {
        assert_eq!(
            single_path_in(run, marks).0,
            0,
            "{}: {:?}",
            run.sid,
            run.single_spans_us
        );
    }
}

/// B 3.6 and rules R1-R3 at 850 / 500 / 780 permille (three edges, a leg
/// 40 permille): the edge at 850 sheds to the mean, only to the edge under
/// the receivers' level (725), passing over listeners whose other leg is
/// that receiver; no leg lands on an edge over 725; the moves stop within
/// three rounds; at most one take-back in five moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebalance_at_850_500_780() {
    let _serial = SERIAL.lock().await;
    let region = Region::start_shaped(184, rebalancing(3)).await;
    // Loaded at the join, the second edge takes fewer legs (the weighted
    // rendezvous weighs 1,000 - load): its legs then make 500 at a cost a
    // leg no higher than the others', which is what leaves it room as the
    // rebalance reckons a listener's cost (the median of load over legs).
    region.set_base(1, 600);
    tokio::time::sleep(ADVERTISE_EVERY * 2).await;
    let crowd = Crowd::join(&region, listeners("levels", 12)).await;
    let start = now_us();
    region.set_load(0, 850);
    region.set_load(1, 500);
    region.set_load(2, 780);
    // The legs are 10 s old, then three rounds of 3 s, and one more; the
    // receiver's load every 20 ms, for each landing.
    let mut peak = [0_u32; 3];
    let mut receiver: Vec<(Instant, u32)> = Vec::new();
    let began = Instant::now();
    while began.elapsed() < Duration::from_secs(23) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        for (index, peak) in peak.iter_mut().enumerate() {
            *peak = (*peak).max(region.load(index));
        }
        receiver.push((Instant::now(), region.load(1)));
    }
    let marks = Marks {
        start,
        end: now_us(),
    };
    let (runs, last, sent) = finish(&region, crowd, marks).await;
    region.shutdown().await;
    assert_the_rule(
        "rebalance_at_850_500_780",
        &runs,
        marks,
        last,
        &sent,
        Payload::Pcm.part_us(),
    );
    let moves = rebalanced(&runs);
    eprintln!("rebalance_at_850_500_780: peaks {peak:?}, {moves:?}");
    let handovers: Vec<&Replacement> = moves
        .iter()
        .filter(|(_, r)| r.kind == ReplacementKind::Handover)
        .map(|(_, r)| *r)
        .collect();
    let takebacks = moves
        .iter()
        .filter(|(_, r)| r.kind == ReplacementKind::Takeback)
        .count();
    assert!(!handovers.is_empty(), "the edge at 850 shed");
    for handover in &handovers {
        assert_eq!(
            (handover.from.as_str(), handover.to.as_str()),
            ("edge-1", "edge-2"),
            "{handover:?}"
        );
    }
    // Each leg it took was granted while the receiver was under the
    // receivers' level: its load at the last sample before the move began
    // (the hand-over ends at `at`).
    for handover in &handovers {
        let began = handover.at - handover.handover.expect("a hand-over time");
        let load = receiver
            .iter()
            .take_while(|(at, _)| *at <= began)
            .last()
            .map(|(_, load)| *load)
            .expect("a sample before the move");
        assert!(
            load < 725,
            "a leg landed on the receiver at {load} permille: {handover:?}"
        );
    }
    let first = handovers.iter().map(|r| r.at).min().unwrap();
    let last_move = handovers.iter().map(|r| r.at).max().unwrap();
    assert!(
        last_move.duration_since(first) <= Duration::from_millis(9_500),
        "{handovers:?}"
    );
    assert!(
        takebacks * 5 <= handovers.len() + takebacks,
        "{takebacks} take-backs in {} moves",
        handovers.len() + takebacks
    );
}

/// Rule R1 (B 4, "a rebalance when every receiver is the listener's other
/// leg"): two edges, every listener on both; the one at 850 could send
/// listeners only to the other, which each of them holds. Nobody is marked.
/// A `MOVE rebalance` forced on a listener (sent from the edge's address)
/// is back at once: the entry names no cache (the listener holds the only
/// other edge), and the director takes the leg back within a round trip and
/// 250 ms. The edge keeps serving it: a second later every listener still
/// holds both edges and delivers on both, and none is on one path from the
/// forced mark to the end.
///
/// `rebalance_ineffective` is not asserted (departure from the commit's
/// list): a rebalance reckons a listener's cost as the median of load over
/// legs among the edges, and with every listener on both edges the other
/// edge then reads nearly as full as this one, at the margin of being a
/// receiver at all. Runs have seen no round and one; a round that begins
/// marks nobody (asserted), and whether it is judged ineffective depends on
/// that margin. R1 itself is the edge's unit test
/// (`a_rebalance_passes_over_a_subscriber_whose_other_leg_is_the_only_receiver`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebalance_with_room_only_at_the_held_edge() {
    let _serial = SERIAL.lock().await;
    let region = Region::start_shaped(192, rebalancing(2)).await;
    let crowd = Crowd::join(&region, listeners("held", 6)).await;
    let start = now_us();
    region.set_load(0, 850);
    region.set_load(1, 300);
    let edge = region.edge(0);
    let moves_before = edge.stats.moves.load(Ordering::Relaxed);
    // The legs are 10 s old, then two rounds.
    tokio::time::sleep(Duration::from_secs(17)).await;
    let marked = edge.stats.moves.load(Ordering::Relaxed) - moves_before;

    // A forced mark, from the edge's address: a socket that joins the
    // edge's SO_REUSEPORT group for one send. Dropped at once, or the
    // kernel would hash a share of the listeners' datagrams to it.
    let listener = crowd.lives[0].addr.lock().unwrap().expect("its address");
    let forced = Instant::now();
    let forced_us = now_us();
    {
        let forger = fast_plane::sys::bind_reuseport(region.client(0)).unwrap();
        let datagram = av_mesh::client_udp::Control::Move {
            stream_id: STREAM,
            reason: "rebalance".into(),
        }
        .encode();
        forger.send_to(&datagram, listener).unwrap();
    }
    drop(edge);
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Taken back at the edge: it still serves every listener's leg, and
    // each listener delivers on both edges.
    assert_eq!(region.legs_at(0), 6, "{}", crowd.describe());
    assert!(crowd.all_on_two_paths(), "{}", crowd.describe());
    assert!(
        crowd.lives[0]
            .legs
            .lock()
            .unwrap()
            .contains(&region.node(0)),
        "{}",
        crowd.describe()
    );
    let marks = Marks {
        start,
        end: now_us(),
    };
    let (runs, last, sent) = finish(&region, crowd, marks).await;
    region.shutdown().await;
    assert_the_rule(
        "rebalance_with_room_only_at_the_held_edge",
        &runs,
        marks,
        last,
        &sent,
        Payload::Pcm.part_us(),
    );
    assert_eq!(marked, 0, "nobody is marked");

    let back = runs[0]
        .replacements
        .iter()
        .find(|r| r.reason == "rebalance" && r.kind == ReplacementKind::Takeback)
        .unwrap_or_else(|| panic!("{}: {:?}", runs[0].sid, runs[0].replacements));
    assert!(
        back.at.duration_since(forced) <= Duration::from_millis(250),
        "{back:?}"
    );
    for run in &runs {
        assert!(
            run.replacements
                .iter()
                .all(|r| r.kind == ReplacementKind::Takeback),
            "{}: {:?}",
            run.sid,
            run.replacements
        );
        assert!(
            run.single_spans_us.iter().all(|(_, end)| *end < forced_us),
            "{}: on one path after the forced mark: {:?}",
            run.sid,
            run.single_spans_us
        );
    }
}

/// Review of B Y5, finding 3: an edge killed or stopped lets go of its
/// client address, so that it can come back there as a restarted process
/// does. The armed fast lane held the edge's counters, and the counters
/// held the lane: the cycle kept the lane, its copy of the client sockets
/// and its repair thread alive after the edge had gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_edge_lets_go_of_its_client_address() {
    let _serial = SERIAL.lock().await;
    let port = region_port(210);
    for (round, kill) in [(0_u8, true), (1, false)] {
        let client = SocketAddr::from((loopback(210 + round), port));
        let edge = LaneEdge::start_at(loopback(210 + round), client, 16, |router| router).await;
        assert!(
            std::net::UdpSocket::bind(client).is_err(),
            "the edge holds it"
        );
        if kill {
            edge.kill().await;
        } else {
            edge.stop().await;
        }
        until(
            "the edge's client sockets are closed",
            Duration::from_secs(2),
            || std::net::UdpSocket::bind(client).is_ok(),
        )
        .await;
    }
}

fn region_node_name(index: usize) -> String {
    format!("edge-{}", index + 1)
}
