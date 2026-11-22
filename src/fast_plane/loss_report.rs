//! Loss reports to a parent lane (wave 7 item 2, `--relay-loss-report`).
//!
//! The contributor sizes the copies of a one-symbol audio part (its repair
//! symbols, each a full copy) to the loss of its lanes. It cannot see that
//! loss itself: the forwarder at the other end of a lane does. The audio
//! fast-plane thread of every node counts, for the parent of each of its
//! receive sockets, which RLS1 packet sequences arrived in a window of half
//! a second, and sends the parent an `RLR1` datagram with the loss it
//! measured (an exponentially weighted mean of the windows, in parts per
//! million) from the socket the symbols came to: when it changes, and once
//! a second while it is above zero. A clean path sends nothing.
//!
//! The estimate assumes that the lane carries every packet sequence of the
//! parent's audio encoder, as each contributor lane does (sources and, with
//! repair on both lanes, repair). A forwarder's lanes to its own children
//! carry a subset; their reports are only informational (a forwarder
//! encodes nothing) and nothing reads them.
//!
//! Wire format, 16 bytes, big-endian: `RLR1`, version 1, 3 reserved bytes
//! (zero), loss in parts per million (u32), packet sequences of the last
//! window (u32).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

pub const LOSS_REPORT_MAGIC: [u8; 4] = *b"RLR1";
pub const LOSS_REPORT_VERSION: u8 = 1;
pub const LOSS_REPORT_LEN: usize = 16;
/// One measurement window.
pub const LOSS_WINDOW: Duration = Duration::from_millis(500);
/// A report is repeated this often while the loss is above zero.
pub const LOSS_REPORT_REPEAT: Duration = Duration::from_secs(1);
/// Packet sequences a window tracks; a sequence further ahead closes it.
const WINDOW_SEQUENCES: u32 = 16_384;
/// A window with fewer sequences than this leaves the estimate as it was.
const MIN_WINDOW_SEQUENCES: u32 = 32;
/// Weight of the newest window in the estimate, in 1/256.
const EWMA_WEIGHT: u64 = 96;
/// Changes smaller than this (ppm) are not reported, apart from a return
/// to zero.
const REPORT_HYSTERESIS_PPM: u32 = 500;

/// An `RLR1` datagram.
#[must_use]
pub fn encode_loss_report(loss_ppm: u32, window_sequences: u32) -> [u8; LOSS_REPORT_LEN] {
    let mut out = [0_u8; LOSS_REPORT_LEN];
    out[..4].copy_from_slice(&LOSS_REPORT_MAGIC);
    out[4] = LOSS_REPORT_VERSION;
    out[8..12].copy_from_slice(&loss_ppm.min(1_000_000).to_be_bytes());
    out[12..16].copy_from_slice(&window_sequences.to_be_bytes());
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

/// Loss of one parent lane, measured on the packet sequences that arrive.
#[derive(Debug)]
pub struct LossMeter {
    peer: Option<SocketAddr>,
    base: Option<u32>,
    seen: Box<[u64]>,
    span: u32,
    received: u32,
    started: Option<Instant>,
    /// The estimate, in ppm.
    estimate_ppm: u32,
    last_window: u32,
    reported: Option<(Instant, u32)>,
}

impl Default for LossMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl LossMeter {
    #[must_use]
    pub fn new() -> Self {
        Self {
            peer: None,
            base: None,
            seen: vec![0_u64; (WINDOW_SEQUENCES / 64) as usize].into_boxed_slice(),
            span: 0,
            received: 0,
            started: None,
            estimate_ppm: 0,
            last_window: 0,
            reported: None,
        }
    }

    /// The estimate, in ppm.
    #[must_use]
    pub const fn loss_ppm(&self) -> u32 {
        self.estimate_ppm
    }

    /// A symbol of packet sequence `sequence` arrived from `peer`. A new
    /// parent starts the measurement again.
    pub fn observe(&mut self, peer: SocketAddr, sequence: u32, now: Instant) {
        if self.peer != Some(peer) {
            *self = Self::new();
            self.peer = Some(peer);
        }
        if self.started.is_none() {
            self.started = Some(now);
        }
        let base = *self.base.get_or_insert(sequence);
        let mut offset = sequence.wrapping_sub(base);
        if offset >= WINDOW_SEQUENCES {
            if offset > u32::MAX / 2 {
                // Older than the window (a late copy): already counted or
                // counted lost.
                return;
            }
            // Far ahead: the window closes here.
            self.close();
            self.base = Some(sequence);
            self.started = Some(now);
            offset = 0;
        }
        let (word, bit) = ((offset / 64) as usize, offset % 64);
        if self.seen[word] & (1 << bit) == 0 {
            self.seen[word] |= 1 << bit;
            self.received += 1;
        }
        self.span = self.span.max(offset + 1);
    }

    /// Fold the window into the estimate and start the next one.
    fn close(&mut self) {
        if self.span >= MIN_WINDOW_SEQUENCES {
            let lost = u64::from(self.span - self.received.min(self.span));
            let window_ppm = lost * 1_000_000 / u64::from(self.span);
            let weighted = (u64::from(self.estimate_ppm) * (256 - EWMA_WEIGHT)
                + window_ppm * EWMA_WEIGHT)
                / 256;
            // A clean window after loss decays the estimate; one under
            // 50 ppm is zero.
            self.estimate_ppm = if weighted < 50 { 0 } else { weighted as u32 };
            self.last_window = self.span;
        }
        let words = (self.span.div_ceil(64) as usize).min(self.seen.len());
        self.seen[..words].fill(0);
        self.base = None;
        self.span = 0;
        self.received = 0;
        self.started = None;
    }

    /// Close the window when it is due, and return the report to send to
    /// the parent, if one is due: on a change of more than the hysteresis,
    /// on a return to zero, and once a second while there is loss.
    pub fn poll(&mut self, now: Instant) -> Option<(SocketAddr, [u8; LOSS_REPORT_LEN])> {
        let peer = self.peer?;
        if self
            .started
            .is_some_and(|started| now.duration_since(started) >= LOSS_WINDOW)
        {
            self.close();
        }
        let loss = self.estimate_ppm;
        let due = match self.reported {
            None => loss > 0,
            Some((at, last)) => {
                (loss == 0 && last != 0)
                    || loss.abs_diff(last) > REPORT_HYSTERESIS_PPM
                    || (loss > 0 && now.duration_since(at) >= LOSS_REPORT_REPEAT)
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

    #[test]
    fn a_report_round_trips_and_garbage_is_refused() {
        let bytes = encode_loss_report(12_345, 900);
        assert_eq!(&bytes[..4], b"RLR1");
        assert_eq!(decode_loss_report(&bytes), Some((12_345, 900)));
        assert_eq!(decode_loss_report(&bytes[..15]), None);
        let mut wrong = bytes;
        wrong[4] = 2;
        assert_eq!(decode_loss_report(&wrong), None);
        assert_eq!(decode_loss_report(b"RLS1............"), None);
    }

    #[test]
    fn a_clean_lane_never_reports() {
        let mut meter = LossMeter::new();
        let start = Instant::now();
        for second in 0..5_u32 {
            for index in 0..1_000_u32 {
                let at = start + Duration::from_micros(u64::from(second * 1_000_000 + index * 1_000));
                meter.observe(peer(), second * 1_000 + index, at);
                assert!(meter.poll(at).is_none());
            }
        }
        assert_eq!(meter.loss_ppm(), 0);
    }

    #[test]
    fn five_percent_loss_is_measured_reported_and_repeated() {
        let mut meter = LossMeter::new();
        let start = Instant::now();
        let mut reports = Vec::new();
        for sequence in 0..10_000_u32 {
            let at = start + Duration::from_millis(u64::from(sequence));
            // Every 20th sequence is lost.
            if sequence % 20 != 7 {
                meter.observe(peer(), sequence, at);
            }
            if let Some((to, report)) = meter.poll(at) {
                assert_eq!(to, peer());
                reports.push((sequence, decode_loss_report(&report).unwrap().0));
            }
        }
        let (_, last) = *reports.last().unwrap();
        assert!((45_000..=55_000).contains(&last), "{reports:?}");
        // Repeated about once a second while there is loss, not more.
        assert!((8..=20).contains(&reports.len()), "{reports:?}");
    }

    #[test]
    fn loss_that_ends_is_reported_as_zero_once() {
        let mut meter = LossMeter::new();
        let start = Instant::now();
        let mut zero_reports = 0;
        for sequence in 0..30_000_u32 {
            let at = start + Duration::from_millis(u64::from(sequence));
            if sequence >= 2_000 || sequence % 10 != 0 {
                meter.observe(peer(), sequence, at);
            }
            if let Some((_, report)) = meter.poll(at) {
                if decode_loss_report(&report).unwrap().0 == 0 {
                    zero_reports += 1;
                }
            }
        }
        assert_eq!(meter.loss_ppm(), 0);
        assert_eq!(zero_reports, 1);
    }

    #[test]
    fn duplicates_and_late_copies_are_not_counted_twice() {
        let mut meter = LossMeter::new();
        let start = Instant::now();
        for sequence in 0..5_000_u32 {
            let at = start + Duration::from_millis(u64::from(sequence));
            meter.observe(peer(), sequence, at);
            // A burst copy of the symbol of 20 ms ago.
            meter.observe(peer(), sequence.saturating_sub(20), at);
            meter.poll(at);
        }
        assert_eq!(meter.loss_ppm(), 0);
    }

    #[test]
    fn packet_sequences_wrap() {
        let mut meter = LossMeter::new();
        let start = Instant::now();
        for index in 0..3_000_u32 {
            let at = start + Duration::from_millis(u64::from(index));
            meter.observe(peer(), u32::MAX - 1_500 + index, at);
            assert!(meter.poll(at).is_none());
        }
        assert_eq!(meter.loss_ppm(), 0);
    }
}
