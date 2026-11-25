//! Pacing of the heavy lane's sends (send240, B), as a measured option
//! (`--send-pacing <k>`, off by default until a lab A/B).
//!
//! cmpd8: a forwarder sent each 1.3 to 6 MB part to its children at once (up
//! to 30 MB in one instant to five), and an edge to its clients; the stalls
//! came with the guest's NIC transmit interrupts falling 3 to 5 times.
//! With `k` above 0, each heavy (video) send socket of a forwarder and an
//! edge is paced by the kernel at `k` times its own measured egress
//! (`SO_MAX_PACING_RATE`, refreshed once a second off the class threads;
//! the sending thread adds each send call's bytes to the socket's counter),
//! and never below `--send-pacing-floor-mbit`: a part to every
//! destination of the socket leaves over about `1/k` of the time its bytes
//! take at the stream's rate (a 4K part at 24 frames a second over about 10
//! ms with k = 4) instead of at line rate. GSO messages stay whole (the
//! qdisc paces them as one packet of their length). The class thread does
//! nothing more per datagram: no timer, no queue, no sleep; other sockets
//! (audio, the cache mesh) are other flows of the qdisc and are not held.
//!
//! The kernel keeps the rate only under the `fq` qdisc: the deploy must
//! render `tc qdisc replace dev <public NIC> root fq` (the edges already need
//! it for the per-client SO_TXTIME pacing). Under another qdisc the option
//! sets the rate and nothing is paced. Chosen over a schedule on the class
//! thread (a per-part send plan driven by the thread's loop) because that
//! needs the datagrams kept (a copy each), more and smaller sendmmsg calls
//! and a wake-up every millisecond: per-datagram work on the thread that the
//! kernel's qdisc does without.

use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Default of `--send-pacing-floor-mbit`: a socket is never paced below
/// this.
pub const DEFAULT_FLOOR_MBIT: u64 = 100;

/// The node's pacing, for `/api/node` (set once at start).
pub static NODE: std::sync::OnceLock<Arc<SendPacing>> = std::sync::OnceLock::new();

type ByteSource = Box<dyn Fn() -> u64 + Send + Sync>;

/// One paced socket: what it sent, as last looked at, and its rate.
struct Paced {
    fd: RawFd,
    bytes: ByteSource,
    last: u64,
    rate_bps: u64,
}

/// The pacing of the node's heavy send sockets, each at `k` times its own
/// measured egress.
pub struct SendPacing {
    factor: u64,
    floor_bps: u64,
    sockets: Mutex<(Option<Instant>, Vec<Paced>)>,
    /// The heavy egress measured (bits a second, all sockets), the highest
    /// rate set on a socket (bits a second, 0: none yet), the sockets paced
    /// and the refusals.
    pub egress_bps: AtomicU64,
    pub rate_bps: AtomicU64,
    pub paced: AtomicU64,
    pub refused: AtomicU64,
}

impl std::fmt::Debug for SendPacing {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SendPacing")
            .field("factor", &self.factor)
            .field("rate_bps", &self.rate_bps.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl SendPacing {
    /// `factor` times the measured egress (0: off, `None`).
    pub fn new(factor: u64, floor_mbit: u64) -> Option<Arc<Self>> {
        (factor > 0).then(|| {
            Arc::new(Self {
                factor,
                floor_bps: floor_mbit.saturating_mul(1_000_000),
                sockets: Mutex::new((None, Vec::new())),
                egress_bps: AtomicU64::new(0),
                rate_bps: AtomicU64::new(0),
                paced: AtomicU64::new(0),
                refused: AtomicU64::new(0),
            })
        })
    }

    /// A heavy send socket whose bytes `bytes` reads (a counter of the
    /// thread that sends from it). Registered once, off the hot path.
    pub fn register(&self, fd: RawFd, bytes: impl Fn() -> u64 + Send + Sync + 'static) {
        let mut sockets = self
            .sockets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if sockets.1.iter().any(|paced| paced.fd == fd) {
            return;
        }
        let last = bytes();
        sockets.1.push(Paced {
            fd,
            bytes: Box::new(bytes),
            last,
            rate_bps: 0,
        });
        self.paced.store(sockets.1.len() as u64, Ordering::Relaxed);
    }

    /// [`Self::register`] with a counter of its own, which the sending
    /// thread adds each send call's bytes to (one relaxed add a call).
    pub fn register_counted(&self, fd: RawFd) -> Arc<AtomicU64> {
        let counter = Arc::new(AtomicU64::new(0));
        let read = Arc::clone(&counter);
        self.register(fd, move || read.load(Ordering::Relaxed));
        counter
    }

    /// The rate a socket is paced at for its egress of `egress_bps`: `k`
    /// times it, never below the floor; 0 (none) until egress is measured.
    pub fn rate_for(&self, egress_bps: u64) -> u64 {
        if egress_bps == 0 {
            return 0;
        }
        egress_bps.saturating_mul(self.factor).max(self.floor_bps)
    }

    /// Once a second: each socket's egress since the last look, and its
    /// rate. A rate rises at once and falls only below half of what is
    /// set (a stream that pauses is not throttled when it comes back).
    pub fn turn(&self) {
        let now = Instant::now();
        let mut sockets = self
            .sockets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (at, paced) = &mut *sockets;
        let seconds = at
            .replace(now)
            .map(|at| now.duration_since(at).as_secs_f64().max(1e-3));
        let mut egress_total = 0;
        let mut highest = 0;
        for socket in paced.iter_mut() {
            let bytes = (socket.bytes)();
            let sent = bytes.saturating_sub(socket.last);
            socket.last = bytes;
            let Some(seconds) = seconds else {
                continue;
            };
            let egress_bps = (sent as f64 * 8.0 / seconds) as u64;
            egress_total += egress_bps;
            let rate = self.rate_for(egress_bps);
            let rate = if rate >= socket.rate_bps || rate * 2 < socket.rate_bps {
                rate
            } else {
                socket.rate_bps
            };
            if rate > 0 && rate != socket.rate_bps {
                if super::sys::set_max_pacing_rate(socket.fd, rate / 8).is_ok() {
                    socket.rate_bps = rate;
                } else {
                    self.refused.fetch_add(1, Ordering::Relaxed);
                }
            }
            highest = highest.max(socket.rate_bps);
        }
        self.egress_bps.store(egress_total, Ordering::Relaxed);
        self.rate_bps.store(highest, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> SendPacingSnapshot {
        SendPacingSnapshot {
            factor: self.factor,
            floor_bps: self.floor_bps,
            egress_bps: self.egress_bps.load(Ordering::Relaxed),
            rate_bps: self.rate_bps.load(Ordering::Relaxed),
            sockets: self.paced.load(Ordering::Relaxed),
            refused: self.refused.load(Ordering::Relaxed),
        }
    }
}

/// `/api/node`'s `send_pacing`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SendPacingSnapshot {
    pub factor: u64,
    pub floor_bps: u64,
    pub egress_bps: u64,
    pub rate_bps: u64,
    pub sockets: u64,
    pub refused: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_socket_is_paced_at_k_times_the_egress_and_never_below_the_floor() {
        assert!(SendPacing::new(0, 100).is_none(), "0 is off");
        let pacing = SendPacing::new(4, 100).unwrap();
        assert_eq!(pacing.rate_for(0), 0, "no rate before egress is measured");
        assert_eq!(pacing.rate_for(10_000_000), 100_000_000, "the floor");
        assert_eq!(pacing.rate_for(500_000_000), 2_000_000_000);
        // The rate is set on the registered sockets once egress is measured.
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bytes = pacing.register_counted(std::os::fd::AsRawFd::as_raw_fd(&socket));
        pacing.turn();
        assert_eq!(pacing.snapshot().rate_bps, 0);
        bytes.store(31_000_000, Ordering::Relaxed);
        std::thread::sleep(std::time::Duration::from_millis(100));
        pacing.turn();
        let snapshot = pacing.snapshot();
        assert!(snapshot.egress_bps > 1_000_000_000, "{snapshot:?}");
        assert_eq!(snapshot.rate_bps, snapshot.egress_bps * 4);
        assert_eq!((snapshot.sockets, snapshot.refused), (1, 0));
        // A quieter second keeps the rate within a factor of two.
        std::thread::sleep(std::time::Duration::from_millis(100));
        pacing.turn();
        assert_eq!(pacing.snapshot().rate_bps, snapshot.rate_bps);
    }
}
