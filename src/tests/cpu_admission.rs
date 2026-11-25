//! rm/cpuadmit in process: edges whose threads are pinned to two CPUs of
//! their own (as a g6-dedicated-2 has, the receivers' kernel path of each
//! send landing on them as a lab veth's does), a publisher of 1 ms Opus
//! Custom parts on a CPU of its own, and listeners on worker threads of
//! their own CPUs: each its own socket and compact-lane subscription on a
//! clean path (the CLEAN plan, no NACK: a part neither lane sends is
//! missing), trying its edges in order and moving on at `REFUSED`.
//!
//! - `cpu_admission_diag_ramp` (ignored, a measurement): one edge, the term
//!   measured but not enforced, listeners ramped in steps; one line a
//!   sample with every term, the fan-out's sheds and lag, and the parts
//!   missing so far: where the edge fills, and what reads it first.
//! - (a) `a_ramp_is_refused_before_any_listener_misses_a_part` (ignored):
//!   1 ms Opus listeners ramp onto edge 1 with edge 2 behind it: every
//!   admitted listener gets every part through the whole ramp, edge 1
//!   refuses before its CPUs or any send thread is full, edge 2 takes the
//!   spill; with no second edge the rest are refused, counted.
//! - (c) `an_overloaded_edge_sheds_two_leg_listeners_without_loss`
//!   (ignored): two-leg listeners (edge 1 and one of three more) admitted
//!   past edge 1's full point with the check off; with it on, the edge asks two-leg listeners to give up their
//!   leg there (`MOVE shed`) until it is under full, and none misses a part.
//!
//! Each proof prints its "after" line and, with `CPUADMIT_OFF=1`, runs the
//! same load with the term measured but not enforced (the "before"), whose
//! assertions then fail. Run alone, in release, on an idle box:
//! `cargo test --release cpu_admission -- --ignored --nocapture
//! --test-threads 1` (`CPUADMIT_FIRST_CPU`, default 8: edges from there,
//! two CPUs each, then the workers and the publisher).

use super::compact_lane::Payload;
use super::fast_lane::LaneEdge;
use super::fast_lease::loopback;
use super::*;
use crate::cpu_admit::{CpuAdmit, CpuAdmitConfig};
use av_mesh::client_compact as compact;
use av_mesh::client_udp::{CompactPlan, Control, RepairPolicy, UNMEASURED_LOSS};
use fast_plane::sys::RecvBatch;
use serde_json::Value;
use std::os::fd::AsRawFd;
use std::sync::atomic::AtomicU64;

const STREAM: u64 = 4_001;

pub(super) fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

pub(super) fn enforced() -> bool {
    std::env::var("CPUADMIT_OFF").as_deref() != Ok("1")
}

// ------------------------------------------------------------------ CPUs

/// Pin thread `tid` (0: the caller) to `cpus`.
pub(super) fn pin(tid: libc::pid_t, cpus: &[usize]) {
    if cpus.is_empty() {
        return;
    }
    // SAFETY: a zeroed cpu_set_t is empty; CPU_SET writes inside it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for cpu in cpus {
            libc::CPU_SET(*cpu, &mut set);
        }
        libc::sched_setaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

/// This process's threads whose names start with `prefix`.
fn threads_named(prefix: &str) -> Vec<libc::pid_t> {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return Vec::new();
    };
    tasks
        .flatten()
        .filter_map(|task| {
            let tid = task.file_name().to_str()?.parse().ok()?;
            let comm = std::fs::read_to_string(task.path().join("comm")).ok()?;
            comm.starts_with(prefix).then_some(tid)
        })
        .collect()
}

fn first_cpu() -> usize {
    env_u64("CPUADMIT_FIRST_CPU", 8) as usize
}

// ------------------------------------------------------------------ an edge

/// An edge of the proofs: its own runtime and two CPUs, and its CPU
/// admission sampled every 500 ms.
pub(super) struct AdmitEdge {
    pub(super) edge: Arc<LaneEdge>,
    pub(super) admit: Arc<CpuAdmit>,
    pub(super) cpus: Vec<usize>,
    runtime: tokio::runtime::Runtime,
    rt_name: String,
}

impl AdmitEdge {
    /// Edge `index` (its loopback address 127.0.0.`ip`), its threads on
    /// `cpus`, its stream's first part in its cache.
    pub(super) fn start(index: usize, ip: u8, cpus: Vec<usize>, capacity: usize) -> Self {
        let rt_name = format!("adm{index}-rt");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name(rt_name.clone())
            .enable_all()
            .build()
            .unwrap();
        let payload = Payload::Opus1;
        let edge = runtime.block_on(async move {
            tokio::spawn(async move {
                let edge =
                    LaneEdge::start_with_capacity(loopback(ip), Duration::ZERO, capacity, true).await;
                let (envelope, _) = payload.envelope(STREAM, 0);
                edge.send(false, &payload.symbols(&envelope), false).await;
                wait_for_part(&edge.cache, STREAM, 0).await;
                edge
            })
            .await
            .unwrap()
        });
        let lane = Arc::clone(&edge.lane);
        let admit = Arc::new(CpuAdmit::new(
            CpuAdmitConfig {
                cpus: cpus.clone(),
                // The box is shared: this process's threads on the edge's
                // CPUs and those CPUs' softirq (see `own_threads_only`).
                own_threads_only: std::env::var("CPUADMIT_PROC_STAT").as_deref() != Ok("1"),
                ..CpuAdmitConfig::default()
            },
            edge.lane.threads.clone(),
            Arc::new(move || lane.stats.fanout_sends.load(Ordering::Relaxed)),
        ));
        admit.set_enforce(enforced());
        edge.router.edge_load.egress.set_cpu_admit(Arc::clone(&admit));
        {
            let admit = Arc::clone(&admit);
            let (_keep, shutdown) = tokio::sync::watch::channel(());
            runtime.spawn(async move {
                let _keep = _keep;
                crate::cpu_admit::spawn_sampler(admit, shutdown).await.unwrap();
            });
        }
        let edge = Self {
            edge: Arc::new(edge),
            admit,
            cpus,
            runtime,
            rt_name,
        };
        edge.pin();
        edge
    }

    /// Its threads (those registered, its runtime's) on its CPUs; again
    /// after threads that start later (a stream's sender).
    pub(super) fn pin(&self) {
        for (tid, _) in self.admit.threads() {
            pin(tid, &self.cpus);
        }
        for tid in threads_named(&self.rt_name) {
            pin(tid, &self.cpus);
        }
    }

    pub(super) fn port(&self) -> SocketAddr {
        self.edge.client_port
    }

    pub(super) fn counters(&self) -> Value {
        let lane = self.edge.lane.snapshot_stats();
        let stats = &self.edge.stats;
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let legs = stats.legs().unwrap_or_default();
        serde_json::json!({
            "legs": legs.subscribers,
            "legs_two": legs.subscribers - legs.single,
            "subscriptions": get(&stats.subscriptions),
            "refusals": get(&stats.refusals),
            "cpu_refusals": get(&stats.cpu_refusals),
            "cpu_shed_moves": get(&stats.cpu_shed_moves),
            "playout_skipped_parts": get(&stats.playout_skipped_parts),
            "fanout_shed": lane.fanout_shed,
            "fanout_lag_over_1ms": lane.fanout_lag_over_1ms,
            "fanout_lag_max_us": lane.fanout_lag_max_us,
            "given_back": lane.given_back,
            "load_permille": self.edge.router.edge_load.admission_snapshot().load_permille,
            "cpu": self.admit.status(),
        })
    }

    pub(super) fn stop(self) {
        let Self { edge, runtime, .. } = self;
        let edge = Arc::try_unwrap(edge).ok().expect("the edge is no longer shared");
        runtime.block_on(edge.stop());
        runtime.shutdown_background();
    }
}

/// CPU admission on an edge that runs on a shared runtime (place_heavy's):
/// its registered threads on `cpus`, sampled every 500 ms on the caller's
/// runtime.
pub(super) fn attach(edge: &LaneEdge, cpus: Vec<usize>) -> Arc<CpuAdmit> {
    let lane = Arc::clone(&edge.lane);
    let admit = Arc::new(CpuAdmit::new(
        CpuAdmitConfig {
            cpus,
            own_threads_only: std::env::var("CPUADMIT_PROC_STAT").as_deref() != Ok("1"),
            ..CpuAdmitConfig::default()
        },
        edge.lane.threads.clone(),
        Arc::new(move || lane.stats.fanout_sends.load(Ordering::Relaxed)),
    ));
    admit.set_enforce(enforced());
    edge.router.edge_load.egress.set_cpu_admit(Arc::clone(&admit));
    let (keep, shutdown) = tokio::sync::watch::channel(());
    let sampled = Arc::clone(&admit);
    tokio::spawn(async move {
        let _keep = keep;
        let _ = crate::cpu_admit::spawn_sampler(sampled, shutdown).await;
    });
    admit
}

/// Each admission's threads on its CPUs, and every other thread of this
/// process that may run on them (but those of `keep`'s prefixes) on
/// `rest`.
pub(super) fn pin_apart(admits: &[Arc<CpuAdmit>], rest: &[usize], keep: &[&str]) {
    let mut edge_tids = Vec::new();
    for admit in admits {
        for (tid, _) in admit.threads() {
            pin(tid, admit.cpus());
            edge_tids.push(tid);
        }
    }
    let reserved: Vec<usize> = admits.iter().flat_map(|admit| admit.cpus().to_vec()).collect();
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return;
    };
    for task in tasks.flatten() {
        let Some(tid) = task.file_name().to_str().and_then(|tid| tid.parse::<i32>().ok()) else {
            continue;
        };
        let comm = std::fs::read_to_string(task.path().join("comm")).unwrap_or_default();
        if edge_tids.contains(&tid) || keep.iter().any(|prefix| comm.starts_with(prefix)) {
            continue;
        }
        // SAFETY: a zeroed cpu_set_t is valid; sched_getaffinity writes it.
        let on_reserved = unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::sched_getaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0
                && reserved.iter().any(|cpu| libc::CPU_ISSET(*cpu, &set))
        };
        if on_reserved {
            pin(tid, rest);
        }
    }
}

// ------------------------------------------------------------------ the publisher

/// Each part's source symbol from both forwarders of every edge at the
/// part's pace, on a thread of its own on `cpu`.
pub(super) fn publisher(
    edges: &[&LaneEdge],
    payload: Payload,
    first: u64,
    stop: Arc<AtomicBool>,
    published: Arc<AtomicU64>,
    cpu: usize,
) -> std::thread::JoinHandle<()> {
    let targets: Vec<(std::os::fd::RawFd, SocketAddr)> = edges
        .iter()
        .flat_map(|edge| {
            [
                (edge.forwarder_fd(0), edge.relay_port(0, false)),
                (edge.forwarder_fd(1), edge.relay_port(1, false)),
            ]
        })
        .collect();
    std::thread::Builder::new()
        .name("adm-publisher".into())
        .spawn(move || {
            pin(0, &[cpu]);
            let started = Instant::now();
            let part_us = payload.part_us();
            let mut sequence = first;
            while !stop.load(Ordering::Relaxed) {
                let due = started + Duration::from_micros(part_us * (sequence - first));
                let now = Instant::now();
                if due > now + Duration::from_micros(200) {
                    std::thread::sleep(due - now - Duration::from_micros(150));
                }
                while Instant::now() < due {
                    std::hint::spin_loop();
                }
                let (envelope, _) = payload.envelope(STREAM, sequence);
                let (sources, _) = payload.symbols(&envelope);
                for symbol in &sources {
                    for (fd, port) in &targets {
                        let _ = fast_plane::sys::send_batch(*fd, 1, |_| (symbol.as_slice(), *port));
                    }
                }
                published.store(sequence, Ordering::Relaxed);
                sequence += 1;
            }
        })
        .unwrap()
}

// ------------------------------------------------------------------ listeners

const SEEN_WORDS: usize = 4_096;
/// A listener's join: its first 200 parts (200 ms at 1 ms) after its
/// first arrival. A part missed there is counted apart (`join_missing`):
/// these listeners take the CLEAN plan and never NACK, and a few join
/// gaps show at an idle edge too; what the load costs is after it.
pub(super) const JOIN_WINDOW_PARTS: u64 = 200;

/// One leg: its edge (index), token, first part.
#[derive(Debug, Clone)]
struct Leg {
    edge: usize,
    token: Option<u64>,
    first: Option<u64>,
    asked_at: Option<Instant>,
    next_refresh: Instant,
    gone: bool,
}

/// A listener: one socket, one or two legs, every part it got.
pub(super) struct Listener {
    socket: std::net::UdpSocket,
    stream_id: u64,
    /// Its edges, tried in order.
    order: Vec<usize>,
    /// Legs it wants.
    want: usize,
    legs: Vec<Leg>,
    /// The next of `order` to try.
    next: usize,
    start_at: Duration,
    started: bool,
    first: Option<u64>,
    /// The first part that arrived: what was missed before it is the
    /// join's (counted apart), not the load's.
    first_arrival: Option<u64>,
    seen: Vec<u64>,
    newest: u64,
    pub(super) refused: Vec<String>,
    pub(super) shed: u64,
    /// Refused by every edge: it asks again from the first then (as a
    /// client asks again after `edge-busy`).
    retry_at: Option<Instant>,
}

impl Listener {
    fn new(stream_id: u64, order: Vec<usize>, want: usize, start_at: Duration) -> Self {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(1 << 20);
        fast_plane::sys::enable_receive_metadata(&socket).unwrap();
        Self {
            socket,
            stream_id,
            order,
            want,
            legs: Vec::new(),
            next: 0,
            start_at,
            started: false,
            first: None,
            first_arrival: None,
            seen: vec![0; SEEN_WORDS],
            newest: 0,
            refused: Vec::new(),
            shed: 0,
            retry_at: None,
        }
    }

    fn subscribe(&self, ports: &[SocketAddr], leg: &Leg) {
        let others: Vec<String> = self
            .legs
            .iter()
            .filter(|other| other.edge != leg.edge && !other.gone)
            .map(|other| format!("edge-{}", other.edge + 1))
            .collect();
        let request = Control::Subscribe {
            stream_id: self.stream_id,
            from_part: None,
            repair: RepairPolicy::for_loss(
                UNMEASURED_LOSS,
                RepairPolicy::base_for_lane(true, None, None),
            ),
            token: leg.token,
            fast_lane: true,
            compact: Some(CompactPlan::CLEAN),
            others,
            resume: false,
        }
        .encode();
        let _ = self.socket.send_to(&request, ports[leg.edge]);
    }

    fn unsubscribe(&self, ports: &[SocketAddr], leg: &Leg) {
        let Some(token) = leg.token else {
            return;
        };
        let request = Control::Unsubscribe {
            stream_id: self.stream_id,
            token,
        }
        .encode();
        let _ = self.socket.send_to(&request, ports[leg.edge]);
    }

    fn live_legs(&self) -> usize {
        self.legs.iter().filter(|leg| !leg.gone).count()
    }

    /// Ask the next edge of its order for a leg while it wants one; with
    /// none left to ask and none pending, again from the first in a second.
    fn ask_more(&mut self, ports: &[SocketAddr], now: Instant) {
        if self.next >= self.order.len()
            && self.live_legs() < self.want
            && !self.legs.iter().any(|leg| !leg.gone && leg.first.is_none())
        {
            match self.retry_at {
                None => {
                    self.retry_at = Some(now + Duration::from_secs(1));
                    return;
                }
                Some(at) if now < at => return,
                Some(_) => {
                    self.retry_at = None;
                    self.next = 0;
                    self.legs.retain(|leg| !leg.gone);
                }
            }
        }
        while self.live_legs() < self.want && self.next < self.order.len() {
            let edge = self.order[self.next];
            self.next += 1;
            if self.legs.iter().any(|leg| leg.edge == edge) {
                continue;
            }
            let leg = Leg {
                edge,
                token: None,
                first: None,
                asked_at: Some(now),
                next_refresh: now + Duration::from_secs(2),
                gone: false,
            };
            self.subscribe(ports, &leg);
            self.legs.push(leg);
            break;
        }
    }

    fn mark(&mut self, part: u64) {
        let Some(first) = self.first else {
            return;
        };
        let Some(index) = part.checked_sub(first).map(|i| i as usize) else {
            return;
        };
        if index >= SEEN_WORDS * 64 {
            return;
        }
        self.newest = self.newest.max(part);
        self.first_arrival.get_or_insert(part);
        self.seen[index / 64] |= 1 << (index % 64);
    }

    fn has(&self, part: u64) -> bool {
        let Some(first) = self.first else {
            return false;
        };
        let Some(index) = part.checked_sub(first).map(|i| i as usize) else {
            return false;
        };
        index < SEEN_WORDS * 64 && self.seen[index / 64] & (1 << (index % 64)) != 0
    }

    /// Parts it missed from its join window's end (its first arrival and
    /// [`JOIN_WINDOW_PARTS`]) to `to`, and that first part.
    pub(super) fn missing(&self, to: u64) -> (u64, Option<u64>) {
        let Some(first) = self.first_arrival.map(|first| first + JOIN_WINDOW_PARTS) else {
            return (0, None);
        };
        (
            (first..=to).filter(|part| !self.has(*part)).count() as u64,
            Some(first),
        )
    }

    /// Parts from the one its edge named to its join window's end that it
    /// missed (the join's: the fast lane takes over from the cache-fed lane
    /// there, and these listeners never NACK).
    pub(super) fn join_missing(&self) -> u64 {
        match (self.first, self.first_arrival) {
            (Some(named), Some(arrived)) => (named..arrived + JOIN_WINDOW_PARTS)
                .filter(|part| !self.has(*part))
                .count() as u64,
            _ => 0,
        }
    }

    /// The edges it holds a leg at.
    pub(super) fn edges(&self) -> Vec<usize> {
        self.legs
            .iter()
            .filter(|leg| !leg.gone && leg.first.is_some())
            .map(|leg| leg.edge)
            .collect()
    }

    pub(super) fn admitted(&self) -> bool {
        self.first.is_some()
    }
}

pub(super) struct Crowd {
    stop: Arc<AtomicBool>,
    workers: Vec<std::thread::JoinHandle<Vec<Listener>>>,
    pub(super) subscribed: Arc<AtomicU64>,
}

/// `count` listeners, listener `i` starting at `i / per_step * every`,
/// each wanting `want` legs from `order(i)`, on `workers` threads pinned to
/// `cpus` (one each, in turn).
pub(super) fn crowd(
    stream_id: u64,
    ports: Vec<SocketAddr>,
    count: usize,
    per_step: usize,
    every: Duration,
    want: usize,
    order: impl Fn(usize) -> Vec<usize>,
    workers: usize,
    cpus: &[usize],
) -> Crowd {
    let stop = Arc::new(AtomicBool::new(false));
    let subscribed = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut handles = Vec::new();
    let mut lists: Vec<Vec<Listener>> = (0..workers).map(|_| Vec::new()).collect();
    for index in 0..count {
        let start_at = every * (index / per_step.max(1)) as u32;
        lists[index % workers].push(Listener::new(stream_id, order(index), want, start_at));
    }
    for (worker, listeners) in lists.into_iter().enumerate() {
        let (stop, subscribed, ports) = (Arc::clone(&stop), Arc::clone(&subscribed), ports.clone());
        let cpu = cpus.get(worker % cpus.len().max(1)).copied();
        handles.push(
            std::thread::Builder::new()
                .name(format!("adm-load{worker}"))
                .spawn(move || {
                    if let Some(cpu) = cpu {
                        pin(0, &[cpu]);
                    }
                    run_worker(listeners, &ports, &stop, &subscribed, t0)
                })
                .unwrap(),
        );
    }
    Crowd {
        stop,
        workers: handles,
        subscribed,
    }
}

impl Crowd {
    pub(super) fn finish(self) -> Vec<Listener> {
        self.stop.store(true, Ordering::Relaxed);
        self.workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect()
    }
}

fn run_worker(
    mut listeners: Vec<Listener>,
    ports: &[SocketAddr],
    stop: &AtomicBool,
    subscribed: &AtomicU64,
    t0: Instant,
) -> Vec<Listener> {
    // SAFETY: plain epoll calls on descriptors this thread owns.
    let epoll = unsafe { libc::epoll_create1(0) };
    assert!(epoll >= 0);
    for (index, listener) in listeners.iter().enumerate() {
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: index as u64,
        };
        unsafe {
            libc::epoll_ctl(epoll, libc::EPOLL_CTL_ADD, listener.socket.as_raw_fd(), &mut event);
        }
    }
    let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; 256];
    let mut batch = RecvBatch::new();
    while !stop.load(Ordering::Relaxed) {
        let now = Instant::now();
        let elapsed = now.duration_since(t0);
        for listener in listeners.iter_mut() {
            if !listener.started && elapsed >= listener.start_at {
                listener.started = true;
                listener.ask_more(ports, now);
            }
            if !listener.started {
                continue;
            }
            if listener.retry_at.is_some_and(|at| now >= at) {
                listener.ask_more(ports, now);
            }
            for index in 0..listener.legs.len() {
                let leg = listener.legs[index].clone();
                if leg.gone {
                    continue;
                }
                let due = match (leg.first, leg.asked_at) {
                    (None, Some(asked)) => now.duration_since(asked) > Duration::from_millis(500),
                    (Some(_), _) => now >= leg.next_refresh,
                    (None, None) => false,
                };
                if due {
                    listener.subscribe(ports, &leg);
                    listener.legs[index].asked_at = Some(now);
                    listener.legs[index].next_refresh = now + Duration::from_secs(2);
                }
            }
        }
        std::thread::sleep(Duration::from_micros(1_000));
        // SAFETY: the events buffer outlives the call.
        let ready =
            unsafe { libc::epoll_wait(epoll, events.as_mut_ptr(), events.len() as i32, 0) };
        for event in &events[..ready.max(0) as usize] {
            let index = event.u64 as usize;
            loop {
                let received = batch.recv(&listeners[index].socket).unwrap_or(0);
                for message in 0..received {
                    let got = batch.get(message);
                    let from = got.peer;
                    let listener = &mut listeners[index];
                    if let Some(view) = compact::parse(got.data) {
                        let reference = listener.newest.max(listener.first.unwrap_or(0));
                        let part = compact::extend_u32(reference, view.part_low);
                        if view.kind == compact::Kind::Part {
                            listener.mark(part);
                        }
                        continue;
                    }
                    let Some(edge) = ports.iter().position(|port| Some(*port) == from) else {
                        continue;
                    };
                    let Some(leg) = listener.legs.iter().position(|leg| leg.edge == edge && !leg.gone)
                    else {
                        continue;
                    };
                    match Control::decode(got.data) {
                        Some(Control::Token { token, .. }) => {
                            if listener.legs[leg].token != Some(token) {
                                listener.legs[leg].token = Some(token);
                                let copy = listener.legs[leg].clone();
                                listener.subscribe(ports, &copy);
                            }
                        }
                        Some(Control::Subscribed { next_part, .. }) => {
                            if listener.legs[leg].first.is_none() {
                                listener.legs[leg].first = Some(next_part);
                                if listener.first.is_none() {
                                    listener.first = Some(next_part);
                                    subscribed.fetch_add(1, Ordering::Relaxed);
                                }
                                listener.ask_more(ports, Instant::now());
                            }
                        }
                        Some(Control::Refused { reason, .. }) => {
                            listener.refused.push(reason);
                            listener.legs[leg].gone = true;
                            listener.ask_more(ports, Instant::now());
                        }
                        Some(Control::Move { reason, .. }) => {
                            // As the director does with nowhere else to go:
                            // a shed leg goes while another delivers; any
                            // other move is kept here.
                            if reason == "shed" && listener.live_legs() > 1 {
                                listener.shed += 1;
                                let copy = listener.legs[leg].clone();
                                listener.unsubscribe(ports, &copy);
                                listener.legs[leg].gone = true;
                            }
                        }
                        _ => {}
                    }
                }
                if received < fast_plane::sys::RECV_BATCH {
                    break;
                }
            }
        }
    }
    unsafe { libc::close(epoll) };
    listeners
}

/// What the listeners got, to part `to`.
pub(super) fn tally(listeners: &[Listener], edges: usize, to: u64) -> Value {
    let mut by_edge = vec![0_u64; edges];
    let (mut admitted, mut refused, mut missing, mut with_missing, mut shed) = (0, 0, 0, 0, 0);
    let mut reasons: BTreeMap<String, u64> = BTreeMap::new();
    let mut missing_by_second: BTreeMap<u64, u64> = BTreeMap::new();
    let mut join_missing = 0_u64;
    let mut missing_after_first: BTreeMap<u64, u64> = BTreeMap::new();
    for listener in listeners {
        for reason in &listener.refused {
            *reasons.entry(reason.clone()).or_default() += 1;
        }
        shed += listener.shed;
        if !listener.admitted() {
            refused += u64::from(!listener.refused.is_empty());
            continue;
        }
        admitted += 1;
        for edge in listener.edges() {
            by_edge[edge] += 1;
        }
        join_missing += listener.join_missing();
        let (lost, first) = listener.missing(to);
        missing += lost;
        with_missing += u64::from(lost > 0);
        if let Some(first) = first.filter(|_| lost > 0) {
            for part in (first..=to).filter(|part| !listener.has(*part)) {
                // Parts are 1 ms apart from part 1 at the run's start.
                *missing_by_second.entry(part / 1_000).or_default() += 1;
                *missing_after_first.entry(((part - first) / 100).min(50) * 100).or_default() += 1;
            }
        }
    }
    serde_json::json!({
        "listeners": listeners.len(),
        "admitted": admitted,
        "refused_everywhere": refused,
        "refusal_reasons": reasons,
        "legs_by_edge_at_end": by_edge,
        "missing_parts": missing,
        "listeners_missing": with_missing,
        "legs_shed": shed,
        "missing_by_second": missing_by_second,
        "join_missing_parts": join_missing,
        "missing_ms_after_first": missing_after_first,
    })
}

/// Samples of every edge each `every` until `stop`: (seconds, lines).
fn sampler(
    edges: Vec<(Arc<CpuAdmit>, Arc<LaneEdge>)>,
    stop: Arc<AtomicBool>,
    every: Duration,
) -> std::thread::JoinHandle<Vec<Value>> {
    std::thread::spawn(move || {
        let t0 = Instant::now();
        let mut lines = Vec::new();
        while !stop.load(Ordering::Relaxed) {
            std::thread::sleep(every);
            let mut line = serde_json::json!({"t": t0.elapsed().as_secs_f64()});
            for (index, (admit, edge)) in edges.iter().enumerate() {
                let lane = edge.lane.snapshot_stats();
                let legs = edge.stats.legs().unwrap_or_default();
                let verdict = admit.verdict();
                line[format!("edge{}", index + 1)] = serde_json::json!({
                    "legs": legs.subscribers,
                    "load": admit.load_permille(),
                    "term": verdict.term,
                    "measured": verdict.permille,
                    "cpus_busy": verdict.cpus_busy_permille,
                    "threads": verdict.threads.iter().map(|t| (t.name.clone(), t.busy_permille)).collect::<BTreeMap<_, _>>(),
                    "datagram_ns": verdict.datagram_ns,
                    "fanout_shed": lane.fanout_shed,
                    "lag_over_1ms": lane.fanout_lag_over_1ms,
                    "skipped": edge.stats.playout_skipped_parts.load(Ordering::Relaxed),
                    "cpu_refusals": edge.stats.cpu_refusals.load(Ordering::Relaxed),
                    "refused_full": admit.stats.refused_cpus.load(Ordering::Relaxed)
                        + admit.stats.refused_thread.load(Ordering::Relaxed),
                    "refused_pending": admit.stats.refused_pending.load(Ordering::Relaxed),
                    "shed_moves": edge.stats.cpu_shed_moves.load(Ordering::Relaxed),
                });
            }
            lines.push(line);
        }
        lines
    })
}

fn out(name: &str, lines: &[Value], result: &Value) {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target").join("cpuadmit");
    let _ = std::fs::create_dir_all(&dir);
    let text: String = lines
        .iter()
        .chain(std::iter::once(result))
        .map(|line| serde_json::to_string(line).unwrap() + "\n")
        .collect();
    std::fs::write(dir.join(format!("{name}.ndjson")), text).unwrap();
}

// ------------------------------------------------------------------ the runs

struct Ramp {
    name: &'static str,
    edges: usize,
    listeners: usize,
    per_step: usize,
    every: Duration,
    want: usize,
    /// Seconds after the last step.
    hold: u64,
    /// Refusals and sheds on from the start (else off until the callback
    /// turns them on).
    start_enforced: bool,
}

/// A ramp onto `edges` edges (listener `i` tries edge 1, then 2, ...; with
/// two legs, legs on edges `0` and `1 + i % (edges - 1)`), sampled.
/// Returns the samples and the tally; `before` runs a callback on the
/// edges once every listener has started.
fn ramp(ramp: &Ramp, mid: impl FnOnce(&[AdmitEdge])) -> (Vec<Value>, Value) {
    let base = first_cpu();
    let edges: Vec<AdmitEdge> = (0..ramp.edges)
        .map(|index| {
            AdmitEdge::start(
                index,
                60 + index as u8,
                vec![base + 2 * index, base + 2 * index + 1],
                ramp.listeners + 64,
            )
        })
        .collect();
    for edge in &edges {
        edge.admit.set_enforce(ramp.start_enforced && enforced());
    }
    let next = base + 2 * ramp.edges;
    let workers: Vec<usize> = (next..next + 4).collect();
    let stop = Arc::new(AtomicBool::new(false));
    let published = Arc::new(AtomicU64::new(0));
    let lanes: Vec<&LaneEdge> = edges.iter().map(|edge| edge.edge.as_ref()).collect();
    let publisher = publisher(&lanes, Payload::Opus1, 1, Arc::clone(&stop), Arc::clone(&published), next + 4);
    std::thread::sleep(Duration::from_millis(500));
    for edge in &edges {
        edge.pin();
    }
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let samples = sampler(
        edges
            .iter()
            .map(|edge| (Arc::clone(&edge.admit), Arc::clone(&edge.edge)))
            .collect(),
        Arc::clone(&sampler_stop),
        Duration::from_millis(500),
    );
    let ports: Vec<SocketAddr> = edges.iter().map(AdmitEdge::port).collect();
    let count = ramp.edges;
    let want = ramp.want;
    let crowd = crowd(
        STREAM,
        ports,
        ramp.listeners,
        ramp.per_step,
        ramp.every,
        want,
        move |index| {
            if want == 2 && count > 1 {
                let second = 1 + index % (count - 1);
                let mut order = vec![0, second];
                order.extend((1..count).filter(|edge| *edge != second));
                order
            } else {
                (0..count).collect()
            }
        },
        workers.len(),
        &workers,
    );
    let steps = ramp.listeners.div_ceil(ramp.per_step) as u32;
    let ramp_time = ramp.every * steps;
    // Threads the edges start as clients come (a stream's sender) go to
    // their CPUs too.
    let ends = Instant::now() + ramp_time;
    while Instant::now() < ends {
        std::thread::sleep(Duration::from_millis(500));
        for edge in &edges {
            edge.pin();
        }
    }
    mid(&edges);
    for _ in 0..ramp.hold * 2 {
        std::thread::sleep(Duration::from_millis(500));
        for edge in &edges {
            edge.pin();
        }
    }
    // Parts up to 1 s before the end are due; the rest drains.
    let to = published.load(Ordering::Relaxed).saturating_sub(1_000);
    std::thread::sleep(Duration::from_millis(1_000));
    let listeners = crowd.finish();
    sampler_stop.store(true, Ordering::Relaxed);
    let samples = samples.join().unwrap();
    stop.store(true, Ordering::Relaxed);
    publisher.join().unwrap();
    let mut result = tally(&listeners, ramp.edges, to);
    result["name"] = ramp.name.into();
    result["enforced"] = enforced().into();
    result["edges"] = Value::Array(edges.iter().map(AdmitEdge::counters).collect());
    out(ramp.name, &samples, &result);
    for edge in edges {
        edge.stop();
    }
    (samples, result)
}

/// The busiest send thread (permille of a core) and the CPUs' busy
/// permille of edge `edge` in the samples up to and including the first
/// in which it refused a leg (`None`: it never did).
fn at_first_refusal(samples: &[Value], edge: usize) -> Option<(f64, u64, u64)> {
    let key = format!("edge{edge}");
    let mut busiest_thread = 0;
    let mut busiest_cpus = 0;
    for sample in samples {
        let block = &sample[&key];
        let threads = block["threads"].as_object()?;
        for (name, busy) in threads {
            if name.starts_with("class") || name.starts_with("fanout") {
                busiest_thread = busiest_thread.max(busy.as_u64().unwrap_or(0));
            }
        }
        busiest_cpus = busiest_cpus.max(block["cpus_busy"].as_u64().unwrap_or(0));
        if block["refused_full"].as_u64().unwrap_or(0) > 0 {
            return Some((sample["t"].as_f64().unwrap_or(0.0), busiest_thread, busiest_cpus));
        }
    }
    None
}

#[test]
#[ignore = "a measurement: run it alone, in release"]
fn cpu_admission_diag_ramp() {
    // The term measured, never enforced (start_enforced false, no callback).
    let (samples, result) = ramp(
        &Ramp {
            name: "diag-ramp",
            edges: 1,
            listeners: env_u64("CPUADMIT_DIAG_LISTENERS", 500) as usize,
            per_step: env_u64("CPUADMIT_DIAG_STEP", 25) as usize,
            every: Duration::from_millis(env_u64("CPUADMIT_DIAG_EVERY_MS", 3_000)),
            want: 1,
            hold: 3,
            start_enforced: false,
        },
        |_| {},
    );
    for sample in &samples {
        println!("{}", serde_json::to_string(sample).unwrap());
    }
    println!("{}", serde_json::to_string(&result).unwrap());
}

/// (a) A ramp of 1 ms Opus listeners onto edge 1, edge 2 behind it.
#[test]
#[ignore = "a proof at 1 ms rates: run it alone, in release"]
fn a_ramp_is_refused_before_any_listener_misses_a_part() {
    let listeners = env_u64("CPUADMIT_A_LISTENERS", 700) as usize;
    let (samples, result) = ramp(
        &Ramp {
            name: "a-two-edges",
            edges: 2,
            listeners,
            per_step: 50,
            every: Duration::from_millis(1_500),
            want: 1,
            hold: 5,
            start_enforced: true,
        },
        |_| {},
    );
    println!("{}", serde_json::to_string(&result).unwrap());
    let refusal = at_first_refusal(&samples, 1);
    println!("edge 1 at its first refusal (t, busiest send thread, CPUs busy): {refusal:?}");
    assert_eq!(result["missing_parts"], 0, "{result:#}");
    let refusal = refusal.expect("edge 1 refused a leg");
    assert!(refusal.1 < 1_000, "a send thread was full before edge 1 refused: {refusal:?}");
    assert!(refusal.2 < 950, "edge 1's CPUs were full before it refused: {refusal:?}");
    let by_edge = &result["legs_by_edge_at_end"];
    assert!(by_edge[1].as_u64().unwrap() > 0, "edge 2 took the spill: {result:#}");
    assert!(
        result["edges"][0]["cpu_refusals"].as_u64().unwrap() > 0,
        "{result:#}"
    );

    // With no second edge the rest are refused, counted.
    let (_, alone) = ramp(
        &Ramp {
            name: "a-one-edge",
            edges: 1,
            listeners: listeners / 2,
            per_step: 50,
            every: Duration::from_millis(1_500),
            want: 1,
            hold: 5,
            start_enforced: true,
        },
        |_| {},
    );
    println!("{}", serde_json::to_string(&alone).unwrap());
    assert_eq!(alone["missing_parts"], 0, "{alone:#}");
    assert!(alone["refused_everywhere"].as_u64().unwrap() > 0, "{alone:#}");
    // Every refusal a listener heard is the edge's CPU refusal, counted by
    // its reason.
    let heard: u64 = alone["refusal_reasons"]["edge-busy"].as_u64().unwrap();
    let edge = &alone["edges"][0];
    assert_eq!(edge["cpu_refusals"].as_u64().unwrap(), heard, "{alone:#}");
    let cpu = &edge["cpu"]["refused"];
    assert_eq!(
        cpu["cpus"].as_u64().unwrap() + cpu["thread"].as_u64().unwrap() + cpu["pending"].as_u64().unwrap(),
        heard,
        "{alone:#}"
    );
    assert!(cpu["cpus"].as_u64().unwrap() + cpu["thread"].as_u64().unwrap() > 0, "{alone:#}");
}

/// (c) Two-leg listeners past the full point with the check off; then on.
#[test]
#[ignore = "a proof at 1 ms rates: run it alone, in release"]
fn an_overloaded_edge_sheds_two_leg_listeners_without_loss() {
    let listeners = env_u64("CPUADMIT_C_LISTENERS", 360) as usize;
    let on = enforced();
    let (samples, result) = ramp(
        &Ramp {
            name: "c-shed",
            // Edge 1 takes every listener's first leg (360); the second
            // legs spread over three more edges (120 each), well under
            // their full point, so that only edge 1 is over it.
            edges: 4,
            listeners,
            per_step: 60,
            every: Duration::from_millis(500),
            want: 2,
            hold: 12,
            start_enforced: false,
        },
        |edges| {
            // Admitted past full with the check off (set below); on now.
            std::thread::sleep(Duration::from_secs(3));
            for edge in edges {
                edge.admit.set_enforce(on);
            }
        },
    );
    println!("{}", serde_json::to_string(&result).unwrap());
    let first = &samples[samples.len().min(10) - 1]["edge1"];
    let last = &samples[samples.len() - 1]["edge1"];
    println!("edge 1 before the shed: {first}\nedge 1 at the end: {last}");
    assert_eq!(result["missing_parts"], 0, "{result:#}");
    assert!(result["legs_shed"].as_u64().unwrap() > 0, "{result:#}");
    // Back under its full point, and it did not empty itself.
    assert!(last["measured"].as_u64().unwrap() < 1_000, "{last}");
    assert!(
        last["legs"].as_u64().unwrap() * 2 > first["legs"].as_u64().unwrap(),
        "{first} {last}"
    );
}

#[test]
fn the_cpu_admission_flags_default_on_and_core_busy_distress_off() {
    let args = Args::try_parse_from(["av-mesh"]).unwrap();
    let config = args.cpu_admit.config().expect("on by default");
    assert_eq!(config.cpus_full_permille, 800);
    assert_eq!(config.thread_full_permille, 700);
    assert_eq!(config.window, Duration::from_secs(2));
    assert_eq!(config.sample_every, Duration::from_millis(500));
    assert_eq!(config.datagram_ns, 3_000);
    assert_eq!(config.pending_max_permille, 100);
    assert_eq!(config.shed_after, Duration::from_secs(2));
    assert!(config.cpus.is_empty() && !config.own_threads_only);
    assert_eq!(args.heavy.distress_core_permille, 0);
    let args = Args::try_parse_from([
        "av-mesh",
        "--cpu-admit",
        "false",
        "--cpu-admit-cpus",
        "0-1",
        "--heavy-distress-core-permille",
        "970",
    ])
    .unwrap();
    assert!(args.cpu_admit.config().is_none());
    assert_eq!(args.cpu_admit.cpus.0, vec![0, 1]);
    assert_eq!(args.heavy.distress_core_permille, 970);
}

/// rm/cpuadmit follow-up: what one refused `SUB` costs the edge's runtime
/// (an ignored measurement). One edge, its CPU term forced: at full (the
/// refusal before any other work, d1d5b89) and just under full with a leg
/// that does not fit (the refusal at the priced check, after the move
/// reason, the heavy cap and the leg's rate). One client asks again and
/// again with a valid token; the edge's runtime threads' CPU time over the
/// refusals. `CPUADMIT_REFUSALS` (20,000) a case.
#[test]
#[ignore = "a measurement: run it alone, in release"]
fn cpu_admission_refusal_cost() {
    use crate::cpu_admit::CpuVerdict;
    let refusals = env_u64("CPUADMIT_REFUSALS", 20_000);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("rf-rt")
        .enable_all()
        .build()
        .unwrap();
    let payload = Payload::Opus1;
    let edge = runtime.block_on(async move {
        tokio::spawn(async move {
            let edge = LaneEdge::start_with_capacity(loopback(91), Duration::ZERO, 64, true).await;
            let (envelope, _) = payload.envelope(STREAM, 0);
            edge.send(false, &payload.symbols(&envelope), false).await;
            wait_for_part(&edge.cache, STREAM, 0).await;
            edge
        })
        .await
        .unwrap()
    });
    let admit = Arc::new(CpuAdmit::new(
        CpuAdmitConfig::default(),
        edge.lane.threads.clone(),
        Arc::new(|| 0),
    ));
    edge.router.edge_load.egress.set_cpu_admit(Arc::clone(&admit));
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.set_read_timeout(Some(Duration::from_millis(200))).unwrap();
    let sub = |token: Option<u64>| {
        Control::Subscribe {
            stream_id: STREAM,
            from_part: None,
            repair: RepairPolicy::for_loss(UNMEASURED_LOSS, RepairPolicy::base_for_lane(true, None, None)),
            token,
            fast_lane: true,
            compact: Some(CompactPlan::CLEAN),
            others: Vec::new(),
            resume: false,
        }
        .encode()
    };
    let mut buf = [0_u8; 2_048];
    socket.send_to(&sub(None), edge.client_port).unwrap();
    let token = loop {
        let (len, _) = socket.recv_from(&mut buf).expect("a token");
        if let Some(Control::Token { token, .. }) = Control::decode(&buf[..len]) {
            break token;
        }
    };
    let rt_ns = || -> u64 {
        threads_named("rf-rt")
            .iter()
            .filter_map(|tid| {
                std::fs::read_to_string(format!("/proc/self/task/{tid}/schedstat"))
                    .ok()?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
            .sum()
    };
    let verdict = |permille| CpuVerdict {
        permille,
        term: "cpus".into(),
        cpus: 2,
        datagram_ns: 20_000,
        capacity_ns: 1_600_000_000,
        ..CpuVerdict::default()
    };
    let mut lines = Vec::new();
    // Just under full, a leg priced at 200 µs a datagram (125 permille at
    // 1,000 a second) does not fit: refused at the priced check.
    for (case, permille, datagram_ns) in [("at-full-early", 1_000, 20_000), ("priced-late", 990, 200_000)] {
        admit.force_verdict(CpuVerdict { datagram_ns, ..verdict(permille) }, None);
        let refused_before = edge.stats.cpu_refusals.load(Ordering::Relaxed);
        let before = rt_ns();
        let started = Instant::now();
        let request = sub(Some(token));
        let mut answered = 0_u64;
        let mut kinds: BTreeMap<String, u64> = BTreeMap::new();
        // One at a time: each answered before the next.
        while answered < refusals {
            socket.send_to(&request, edge.client_port).unwrap();
            loop {
                let Ok((len, _)) = socket.recv_from(&mut buf) else {
                    *kinds.entry("timeout".into()).or_default() += 1;
                    break;
                };
                let kind = match Control::decode(&buf[..len]) {
                    Some(Control::Refused { reason, .. }) => format!("refused-{reason}"),
                    Some(Control::Subscribed { .. }) => "subscribed".into(),
                    Some(other) => format!("{other:?}").chars().take(12).collect(),
                    None => "media".into(),
                };
                let done = kind.starts_with("refused") || kind == "subscribed";
                *kinds.entry(kind).or_default() += 1;
                if done {
                    answered += 1;
                    break;
                }
            }
        }
        let wall = started.elapsed().as_secs_f64();
        let used = rt_ns() - before;
        let refused = edge.stats.cpu_refusals.load(Ordering::Relaxed) - refused_before;
        let line = serde_json::json!({
            "case": case,
            "refusals": refused,
            "answers": answered,
            "answers_by_kind": kinds,
            "wall_s": wall,
            "edge_runtime_cpu_s": used as f64 / 1e9,
            "us_per_refusal": used as f64 / 1e3 / refused.max(1) as f64,
            "refusals_per_s": refused as f64 / wall,
            "load": std::fs::read_to_string("/proc/loadavg").unwrap_or_default().trim().to_owned(),
        });
        println!("{}", serde_json::to_string(&line).unwrap());
        lines.push(line);
    }
    out("refusal-cost", &lines, &serde_json::json!({"refusals_per_case": refusals}));
    runtime.block_on(Arc::try_unwrap(Arc::new(edge)).ok().unwrap().stop());
}
