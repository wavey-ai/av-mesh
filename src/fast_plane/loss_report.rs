//! Loss reports to a parent lane (wave 7 item 2, `--relay-loss-report`).
//!
//! The contributor sizes the copies of a one-symbol audio part (its repair
//! symbols, each a full copy) to the loss of its lanes. It cannot see that
//! loss itself: the forwarder at the other end of a lane does.
//!
//! The contributor tells each lane's forwarder how many datagrams it has
//! handed to the kernel on that lane's socket: an `RLL1` ledger, a running
//! count, sent on the same socket after those datagrams, about four times a
//! second. The forwarder's audio thread counts the RLS1/RLC1 datagrams that
//! arrive from that parent on that socket. Between two ledgers, the loss
//! is what was sent less what arrived, less what the forwarder's own
//! receive queue dropped (SO_RXQ_OVFL: an overloaded receiver is not a
//! lossy path, and more copies would only load it more). What the lane
//! carries does not matter (sources only, repair only, both), and parts
//! that the contributor drops before sending (a full socket, a passed
//! deadline) are never counted as sent: the measure is the path's (wave 7
//! review: a meter on packet-sequence gaps read every sequence a lane does
//! not carry as lost, and held the copies at their maximum).
//!
//! The forwarder sends the parent an `RLR1` (version 2) datagram with the
//! loss (an exponentially weighted mean of the windows, in parts per
//! million) from the socket the datagrams came to: when it changes, and
//! once a second while ledgers arrive, zero included, so the contributor
//! can tell a clean lane from one that reports nothing (an older forwarder,
//! `--relay-loss-report false`) and fall back to its configured loss there.
//! A socket that gets no ledger (every node below the first forwarders)
//! measures and sends nothing.
//!
//! Wire formats, 16 bytes, big-endian:
//! - `RLL1`, version 1, 3 reserved bytes (zero), datagrams sent (u64).
//! - `RLR1`, version 2, 3 reserved bytes (zero), loss in parts per million
//!   (u32), datagrams of the last window (u32).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

pub const LOSS_REPORT_MAGIC: [u8; 4] = *b"RLR1";
/// Version 1 measured packet-sequence gaps; contributors ignore it.
pub const LOSS_REPORT_VERSION: u8 = 2;
pub const LOSS_REPORT_LEN: usize = 16;
pub const LEDGER_MAGIC: [u8; 4] = *b"RLL1";
pub const LEDGER_VERSION: u8 = 1;
pub const LEDGER_LEN: usize = 16;
/// A report is repeated this often while ledgers arrive.
pub const LOSS_REPORT_REPEAT: Duration = Duration::from_secs(1);
/// No ledger for this long: the parent is gone, and nothing is reported.
const LEDGER_TIMEOUT: Duration = Duration::from_secs(3);
/// A window of fewer datagrams than this waits for the next ledger.
const MIN_WINDOW_DATAGRAMS: u64 = 32;
/// More datagrams arrived than were counted as sent (a datagram counted
/// just after a ledger left, or reordered past one): the surplus offsets
/// the next window's loss, up to this many datagrams.
const MAX_CARRY: i64 = 64;
/// Weight of the newest window in the estimate, in 1/256.
const EWMA_WEIGHT: u64 = 96;
/// Changes smaller than this (ppm), or than an eighth of the last report,
/// are not reported at once, apart from a return to zero.
const REPORT_HYSTERESIS_PPM: u32 = 500;

/// An `RLR1` datagram.
#[must_use]
pub fn encode_loss_report(loss_ppm: u32, window_datagrams: u32) -> [u8; LOSS_REPORT_LEN] {
    let mut out = [0_u8; LOSS_REPORT_LEN];
    out[..4].copy_from_slice(&LOSS_REPORT_MAGIC);
    out[4] = LOSS_REPORT_VERSION;
    out[8..12].copy_from_slice(&loss_ppm.min(1_000_000).to_be_bytes());
    out[12..16].copy_from_slice(&window_datagrams.to_be_bytes());
    out
}

/// The loss (ppm) and window size of an `RLR1` datagram.
#[must_use]
#[cfg_attr(not(test), allow(dead_code))]
pub fn decode_loss_report(datagram: &[u8]) -> Option<(u32, u32)> {
    if datagram.len() != LOSS_REPORT_LEN
        || datagram[..4] != LOSS_REPORT_MAGIC
        || datagram[4] != LOSS_REPORT_VERSION
    {
        return None;
    }
    let loss = u32::from_be_bytes(datagram[8..12].try_into().ok()?);
    let window = u32::from_be_bytes(datagram[12..16].try_into().ok()?);
    (loss <= 1_000_000).then_some((loss, window))
}

/// An `RLL1` ledger: `sent` datagrams handed to the kernel on the lane.
#[must_use]
#[cfg_attr(not(test), allow(dead_code))]
pub fn encode_ledger(sent: u64) -> [u8; LEDGER_LEN] {
    let mut out = [0_u8; LEDGER_LEN];
    out[..4].copy_from_slice(&LEDGER_MAGIC);
    out[4] = LEDGER_VERSION;
    out[8..16].copy_from_slice(&sent.to_be_bytes());
    out
}

/// The count of an `RLL1` ledger.
#[must_use]
pub fn decode_ledger(datagram: &[u8]) -> Option<u64> {
    if datagram.len() != LEDGER_LEN
        || datagram[..4] != LEDGER_MAGIC
        || datagram[4] != LEDGER_VERSION
    {
        return None;
    }
    Some(u64::from_be_bytes(datagram[8..16].try_into().ok()?))
}

/// The counts at the last ledger that closed a window.
#[derive(Debug, Clone, Copy)]
struct Base {
    sent: u64,
    received: u64,
    dropped: u32,
}

/// Loss of one parent lane, measured against the parent's ledgers.
#[derive(Debug, Default)]
pub struct LossMeter {
    /// The parent whose ledgers arrive here; only its datagrams count.
    peer: Option<SocketAddr>,
    received: u64,
    base: Option<Base>,
    carry: i64,
    /// The estimate, in ppm.
    estimate_ppm: u32,
    measured: bool,
    last_window: u32,
    last_ledger: Option<Instant>,
    reported: Option<(Instant, u32)>,
}

impl LossMeter {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The estimate, in ppm.
    #[must_use]
    pub const fn loss_ppm(&self) -> u32 {
        self.estimate_ppm
    }

    /// A datagram of the lane arrived from `peer`. Until a ledger names the
    /// parent this does nothing.
    pub fn observe(&mut self, peer: SocketAddr) {
        if self.peer == Some(peer) {
            self.received += 1;
        }
    }

    /// The parent `peer` says it has sent `sent` datagrams on the lane;
    /// `dropped` is the socket's receive-queue drop count (SO_RXQ_OVFL) now.
    /// A new parent, or a count that went back (the parent restarted),
    /// starts the measurement again.
    pub fn ledger(&mut self, peer: SocketAddr, sent: u64, dropped: u32, now: Instant) {
        if self.peer != Some(peer) {
            *self = Self {
                peer: Some(peer),
                ..Self::default()
            };
        }
        self.last_ledger = Some(now);
        let current = Base {
            sent,
            received: self.received,
            dropped,
        };
        let Some(base) = self.base else {
            self.base = Some(current);
            return;
        };
        if sent < base.sent {
            self.base = Some(current);
            self.carry = 0;
            return;
        }
        let sent_delta = sent - base.sent;
        if sent_delta < MIN_WINDOW_DATAGRAMS {
            return;
        }
        let received_delta = self.received - base.received;
        let dropped_delta = u64::from(dropped.wrapping_sub(base.dropped));
        let lost = i64::try_from(sent_delta).unwrap_or(i64::MAX)
            - i64::try_from(received_delta + dropped_delta).unwrap_or(i64::MAX)
            + self.carry;
        let lost = if lost < 0 {
            self.carry = lost.max(-MAX_CARRY);
            0
        } else {
            self.carry = 0;
            lost.unsigned_abs().min(sent_delta)
        };
        let mut window_ppm = lost * 1_000_000 / sent_delta;
        if dropped_delta > 0 {
            // The receiver itself was overloaded: the window does not
            // raise the estimate.
            window_ppm = window_ppm.min(u64::from(self.estimate_ppm));
        }
        let weighted =
            (u64::from(self.estimate_ppm) * (256 - EWMA_WEIGHT) + window_ppm * EWMA_WEIGHT) / 256;
        // A clean window after loss decays the estimate; one under 50 ppm
        // is zero.
        self.estimate_ppm = if weighted < 50 { 0 } else { weighted as u32 };
        self.last_window = u32::try_from(sent_delta).unwrap_or(u32::MAX);
        self.measured = true;
        self.base = Some(current);
    }

    /// The report to send to the parent, if one is due: on a change of more
    /// than the hysteresis, on a return to zero, and once a second while
    /// the parent's ledgers arrive.
    pub fn poll(&mut self, now: Instant) -> Option<(SocketAddr, [u8; LOSS_REPORT_LEN])> {
        let peer = self.peer?;
        if !self.measured
            || self
                .last_ledger
                .is_none_or(|at| now.duration_since(at) >= LEDGER_TIMEOUT)
        {
            return None;
        }
        let loss = self.estimate_ppm;
        let due = match self.reported {
            None => true,
            Some((at, last)) => {
                (loss == 0 && last != 0)
                    || loss.abs_diff(last) > REPORT_HYSTERESIS_PPM.max(last / 8)
                    || now.duration_since(at) >= LOSS_REPORT_REPEAT
            }
        };
        if !due {
            return None;
        }
        self.reported = Some((now, loss));
        Some((peer, encode_loss_report(loss, self.last_window)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer() -> SocketAddr {
        "10.77.1.5:22301".parse().unwrap()
    }

    /// A lane of `seconds` at 1,000 datagrams a second with a ledger every
    /// 250 ms: `lost(n)` says whether datagram n is lost on the way, and
    /// `dropped(n)` whether the receiver's queue dropped it. The reports,
    /// in ppm.
    fn run(
        meter: &mut LossMeter,
        seconds: u64,
        lost: impl Fn(u64) -> bool,
        dropped: impl Fn(u64) -> bool,
    ) -> Vec<u32> {
        let start = Instant::now();
        let mut reports = Vec::new();
        let mut queue_drops = 0_u32;
        for index in 0..seconds * 1_000 {
            let at = start + Duration::from_millis(index);
            if dropped(index) {
                queue_drops += 1;
            } else if !lost(index) {
                meter.observe(peer());
            }
            if index % 250 == 249 {
                meter.ledger(peer(), index + 1, queue_drops, at);
            }
            if index % 100 == 0 {
                if let Some((to, report)) = meter.poll(at) {
                    assert_eq!(to, peer());
                    reports.push(decode_loss_report(&report).unwrap().0);
                }
            }
        }
        reports
    }

    #[test]
    fn reports_and_ledgers_round_trip_and_garbage_is_refused() {
        let bytes = encode_loss_report(12_345, 900);
        assert_eq!(&bytes[..4], b"RLR1");
        assert_eq!(bytes[4], 2);
        assert_eq!(decode_loss_report(&bytes), Some((12_345, 900)));
        assert_eq!(decode_loss_report(&bytes[..15]), None);
        let mut old = bytes;
        old[4] = 1;
        assert_eq!(decode_loss_report(&old), None, "version 1 is not read");
        assert_eq!(decode_loss_report(b"RLS1............"), None);
        let ledger = encode_ledger(u64::from(u32::MAX) + 7);
        assert_eq!(&ledger[..4], b"RLL1");
        assert_eq!(decode_ledger(&ledger), Some(u64::from(u32::MAX) + 7));
        assert_eq!(decode_ledger(&ledger[..15]), None);
        assert_eq!(decode_ledger(&bytes), None);
    }

    #[test]
    fn a_clean_lane_reports_zero_about_once_a_second() {
        let mut meter = LossMeter::new();
        let reports = run(&mut meter, 5, |_| false, |_| false);
        assert!(reports.iter().all(|&loss| loss == 0), "{reports:?}");
        assert!((4..=6).contains(&reports.len()), "{reports:?}");
        assert_eq!(meter.loss_ppm(), 0);
    }

    #[test]
    fn without_a_ledger_nothing_is_measured_or_reported() {
        let mut meter = LossMeter::new();
        let start = Instant::now();
        for index in 0..5_000_u64 {
            meter.observe(peer());
            assert!(meter.poll(start + Duration::from_millis(index)).is_none());
        }
        assert_eq!(meter.received, 0, "no parent named: nothing counted");
    }

    #[test]
    fn five_percent_loss_is_measured_and_reported() {
        let mut meter = LossMeter::new();
        let reports = run(&mut meter, 10, |index| index % 20 == 7, |_| false);
        let last = *reports.last().unwrap();
        assert!((45_000..=55_000).contains(&last), "{reports:?}");
        assert!((9..=30).contains(&reports.len()), "{reports:?}");
    }

    #[test]
    fn a_lane_that_carries_part_of_the_sequences_is_not_lossy() {
        // The ledger counts what the lane was sent, whatever its share of
        // the encoder's packet sequences: a source-only lane beside a
        // repair lane (the wave 7 review's latch) reports no loss.
        let mut meter = LossMeter::new();
        let reports = run(&mut meter, 5, |_| false, |_| false);
        assert_eq!(reports.iter().max(), Some(&0));
    }

    #[test]
    fn receive_queue_drops_are_not_path_loss() {
        let mut meter = LossMeter::new();
        // 10 % dropped by an overloaded receiver, nothing on the path.
        let reports = run(&mut meter, 5, |_| false, |index| index % 10 == 3);
        assert!(reports.iter().all(|&loss| loss == 0), "{reports:?}");
        // Path loss during an overload is not raised by it either.
        let mut meter = LossMeter::new();
        let reports = run(&mut meter, 5, |index| index % 20 == 7, |index| index % 10 == 3);
        assert!(reports.iter().all(|&loss| loss <= 50_000), "{reports:?}");
    }

    #[test]
    fn loss_that_ends_returns_to_zero() {
        let mut meter = LossMeter::new();
        let reports = run(&mut meter, 30, |index| index < 2_000 && index % 10 == 0, |_| false);
        assert!(reports.iter().any(|&loss| loss > 10_000), "{reports:?}");
        assert_eq!(*reports.last().unwrap(), 0);
        assert_eq!(meter.loss_ppm(), 0);
    }

    #[test]
    fn a_datagram_counted_after_its_ledger_left_is_no_loss() {
        // The parent queued a datagram but counted it only after the
        // ledger was read: one window sees one more arrival than was sent,
        // the next one less.
        let mut meter = LossMeter::new();
        let start = Instant::now();
        let mut sent = 0_u64;
        for window in 0..40_u64 {
            for _ in 0..250 {
                meter.observe(peer());
            }
            sent += 250;
            let counted = if window % 2 == 0 { sent - 1 } else { sent };
            meter.ledger(peer(), counted, 0, start + Duration::from_millis(window * 250));
        }
        assert_eq!(meter.loss_ppm(), 0);
    }

    #[test]
    fn another_peer_or_a_restart_starts_again() {
        let mut meter = LossMeter::new();
        let start = Instant::now();
        meter.ledger(peer(), 1_000, 0, start);
        for _ in 0..100 {
            meter.observe(peer());
            meter.observe("10.77.1.6:22301".parse().unwrap());
        }
        // The parent restarted: its count went back.
        meter.ledger(peer(), 100, 0, start + Duration::from_millis(250));
        for _ in 0..250 {
            meter.observe(peer());
        }
        meter.ledger(peer(), 350, 0, start + Duration::from_millis(500));
        assert_eq!(meter.loss_ppm(), 0);
        // Ledgers stop: no more reports.
        assert!(meter.poll(start + Duration::from_millis(600)).is_some());
        assert!(meter.poll(start + Duration::from_secs(10)).is_none());
    }
}
