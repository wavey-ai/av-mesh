//! The parts a one-leg listener receives, judged (place240 rule 3).
//!
//! cmpd8: a 4K viewer holds one leg. When its edge's send side stalled, the
//! leg kept delivering datagrams (late parts, repair, `WAIT`s), so none of
//! the silence, hedge, `refused-1s` or `stalled-1s` rules fired; parts went
//! missing (11) and came 3 to 5.7 s late, and nothing judged lateness or
//! missing parts. This judges the parts, not the datagrams:
//!
//! - **late**: a part's lateness is its arrival less its publication (the
//!   contributor's stamp), less the least of that over the last 10 s (the
//!   path's own delay and the clocks' offset). Parts later than
//!   `late_part` (1 s), or a p95 over the window above `late_p95` (500 ms),
//!   are `late-parts`.
//! - **missing**: a part not delivered `playout` (500 ms) after a later
//!   part arrived was skipped at its playout deadline. At least
//!   `missing_parts` (2) of them in the window, and at least
//!   `missing_permille` (20) of the parts due in it, are `missing-parts`.
//!
//! A stream whose source pauses publishes nothing, so nothing is late or
//! missing: the rules hold while the stream's head advances. The window is
//! `window` (1 s). Per part, not per datagram: it runs when an object comes
//! out of the receiver, and costs a few compares and a bounded queue push.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

pub const DEFAULT_LATE_PART_MS: u64 = 1_000;
pub const DEFAULT_LATE_P95_MS: u64 = 500;
pub const DEFAULT_QUALITY_WINDOW_MS: u64 = 1_000;
pub const DEFAULT_PLAYOUT_MS: u64 = 500;
pub const DEFAULT_MISSING_PARTS: u32 = 2;
pub const DEFAULT_MISSING_PERMILLE: u32 = 20;
/// The least delay of the last this long is the path's own.
pub const BASELINE: Duration = Duration::from_secs(10);
/// Holes kept at most (the oldest go as missing).
const MAX_HOLES: usize = 512;
/// Parts of the window kept at most.
const MAX_WINDOW_PARTS: usize = 4_096;
/// A part further ahead than this opens no holes (a new numbering).
const MAX_JUMP: u64 = 4_096;

/// The rules' settings (`--late-part-ms`, `--late-p95-ms`,
/// `--quality-window-ms`, `--playout-ms`, `--missing-parts`,
/// `--missing-permille`; `--quality-moves false` turns them off).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QualityConfig {
    pub enabled: bool,
    pub late_part: Duration,
    pub late_p95: Duration,
    pub window: Duration,
    pub playout: Duration,
    pub missing_parts: u32,
    pub missing_permille: u32,
}

impl Default for QualityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            late_part: Duration::from_millis(DEFAULT_LATE_PART_MS),
            late_p95: Duration::from_millis(DEFAULT_LATE_P95_MS),
            window: Duration::from_millis(DEFAULT_QUALITY_WINDOW_MS),
            playout: Duration::from_millis(DEFAULT_PLAYOUT_MS),
            missing_parts: DEFAULT_MISSING_PARTS,
            missing_permille: DEFAULT_MISSING_PERMILLE,
        }
    }
}

/// Why a lone leg should be left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    LateParts,
    MissingParts,
}

impl Verdict {
    pub fn reason(self) -> &'static str {
        match self {
            Self::LateParts => "late-parts",
            Self::MissingParts => "missing-parts",
        }
    }
}

/// A histogram of lateness in milliseconds: buckets of powers of two (0,
/// 1, 2-3, 4-7, ... 16,384 and more).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LatenessHistogram {
    buckets: [u64; 16],
    pub count: u64,
    pub max_us: u64,
}

impl LatenessHistogram {
    pub fn record(&mut self, lateness_us: u64) {
        let ms = lateness_us / 1_000;
        let bucket = (64 - ms.leading_zeros()).min(15) as usize;
        self.buckets[bucket] += 1;
        self.count += 1;
        self.max_us = self.max_us.max(lateness_us);
    }

    /// The upper edge of the bucket that holds the quantile `q`, ms.
    pub fn quantile_ms(&self, q: f64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let rank = ((self.count as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (bucket, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return Some(if bucket == 0 { 0 } else { (1_u64 << bucket) - 1 });
            }
        }
        Some(self.max_us / 1_000)
    }

    pub fn merge(&mut self, other: &Self) {
        for (mine, theirs) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *mine += theirs;
        }
        self.count += other.count;
        self.max_us = self.max_us.max(other.max_us);
    }
}

/// The parts of a session, judged for a lone leg.
#[derive(Debug, Clone)]
pub struct QualityJudge {
    pub config: QualityConfig,
    /// The least delay (arrival less publication) of each second of the
    /// last [`BASELINE`], µs (signed: the clocks may differ).
    baseline: VecDeque<(Instant, i64)>,
    /// The window's parts: when each came and how late, µs.
    arrivals: VecDeque<(Instant, u64)>,
    /// Parts skipped at their deadline in the window.
    missing: VecDeque<Instant>,
    /// Parts not delivered, each from when a later part arrived.
    holes: VecDeque<(u64, Instant)>,
    highest: Option<u64>,
    /// Parts counted missing in all, and late (over `late_part`).
    pub missing_total: u64,
    pub late_total: u64,
}

impl QualityJudge {
    pub fn new(config: QualityConfig) -> Self {
        Self {
            config,
            baseline: VecDeque::new(),
            arrivals: VecDeque::new(),
            missing: VecDeque::new(),
            holes: VecDeque::new(),
            highest: None,
            missing_total: 0,
            late_total: 0,
        }
    }

    /// The window starts again (a new lone leg): what the last leg did is
    /// not this one's. The path's baseline stays.
    pub fn reset(&mut self) {
        self.arrivals.clear();
        self.missing.clear();
    }

    /// A new numbering of the stream (a new source session): no hole of
    /// the old one is missing.
    pub fn new_flow(&mut self) {
        self.holes.clear();
        self.highest = None;
    }

    /// The path's least delay over the last [`BASELINE`], µs.
    fn baseline_us(&self) -> Option<i64> {
        self.baseline.iter().map(|(_, delay)| *delay).min()
    }

    /// Part `part` was delivered at `now` (`arrival_unix_us`), published at
    /// `published_unix_us` (0: unknown). Returns its lateness, µs.
    pub fn on_part(
        &mut self,
        part: u64,
        published_unix_us: u64,
        arrival_unix_us: u64,
        now: Instant,
    ) -> Option<u64> {
        self.fill_hole(part, now);
        if published_unix_us == 0 || arrival_unix_us == 0 {
            return None;
        }
        let delay = arrival_unix_us as i64 - published_unix_us as i64;
        while self
            .baseline
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) > BASELINE)
        {
            self.baseline.pop_front();
        }
        match self.baseline.back_mut() {
            Some((at, least)) if now.saturating_duration_since(*at) < Duration::from_secs(1) => {
                *least = (*least).min(delay);
            }
            _ => self.baseline.push_back((now, delay)),
        }
        let lateness = (delay - self.baseline_us().unwrap_or(delay)).max(0) as u64;
        if self.arrivals.len() >= MAX_WINDOW_PARTS {
            self.arrivals.pop_front();
        }
        self.arrivals.push_back((now, lateness));
        if lateness > self.config.late_part.as_micros() as u64 {
            self.late_total += 1;
        }
        Some(lateness)
    }

    /// A gap (the source's own): not missing.
    pub fn on_gap(&mut self, part: u64, now: Instant) {
        self.fill_hole(part, now);
    }

    fn fill_hole(&mut self, part: u64, now: Instant) {
        match self.highest {
            Some(highest) if part > highest => {
                if part - highest <= MAX_JUMP {
                    for hole in highest + 1..part {
                        if self.holes.len() >= MAX_HOLES {
                            self.holes.pop_front();
                            self.count_missing(now);
                        }
                        self.holes.push_back((hole, now));
                    }
                } else {
                    self.holes.clear();
                }
                self.highest = Some(part);
            }
            Some(_) => {
                if let Some(at) = self.holes.iter().position(|(hole, _)| *hole == part) {
                    self.holes.remove(at);
                }
            }
            None => self.highest = Some(part),
        }
    }

    fn count_missing(&mut self, now: Instant) {
        self.missing_total += 1;
        self.missing.push_back(now);
    }

    /// The verdict now: holes past their deadline become missing, the
    /// window moves on, and the rules are judged.
    pub fn judge(&mut self, now: Instant) -> Option<Verdict> {
        while self
            .holes
            .front()
            .is_some_and(|(_, at)| now.saturating_duration_since(*at) >= self.config.playout)
        {
            self.holes.pop_front();
            self.count_missing(now);
        }
        let window = self.config.window;
        while self
            .arrivals
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) > window)
        {
            self.arrivals.pop_front();
        }
        while self
            .missing
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) > window)
        {
            self.missing.pop_front();
        }
        if !self.config.enabled {
            return None;
        }
        let missing = self.missing.len() as u64;
        let due = self.arrivals.len() as u64 + missing;
        if missing >= u64::from(self.config.missing_parts)
            && missing * 1_000 >= u64::from(self.config.missing_permille) * due
        {
            return Some(Verdict::MissingParts);
        }
        let late_us = self.config.late_part.as_micros() as u64;
        let over = self.arrivals.iter().filter(|(_, late)| *late > late_us).count();
        if over >= 2 {
            return Some(Verdict::LateParts);
        }
        // The p95 over the window, of at least five parts, two of them
        // over it (one slow part is not a slow edge).
        let p95_us = self.config.late_p95.as_micros() as u64;
        if self.arrivals.len() >= 5
            && self.arrivals.iter().filter(|(_, late)| *late > p95_us).count() >= 2
        {
            let mut lateness: Vec<u64> = self.arrivals.iter().map(|(_, late)| *late).collect();
            lateness.sort_unstable();
            let p95 = lateness[((lateness.len() as f64 * 0.95).ceil() as usize).saturating_sub(1)];
            if p95 > p95_us {
                return Some(Verdict::LateParts);
            }
        }
        None
    }

    /// When a hole next passes its deadline (for the caller's wake).
    pub fn next_deadline(&self) -> Option<Instant> {
        self.holes.front().map(|(_, at)| *at + self.config.playout)
    }
}

/// The flags of place240's rules 3 and 4, the same on
/// `needletail-udp-client` and `needletail-load`.
#[derive(clap::Args, Debug, Clone)]
pub struct QualityArgs {
    /// A lone leg whose parts come late or go missing is left
    /// make-before-break (`false`: the rules are off).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub quality_moves: bool,
    /// A part later than this (arrival less publication, less the path's
    /// least over 10 s) is late; two in the window move the leg.
    #[arg(long, default_value_t = DEFAULT_LATE_PART_MS)]
    pub late_part_ms: u64,
    /// The p95 lateness of the window's parts above this moves the leg.
    #[arg(long, default_value_t = DEFAULT_LATE_P95_MS)]
    pub late_p95_ms: u64,
    /// The window the rules judge, ms.
    #[arg(long, default_value_t = DEFAULT_QUALITY_WINDOW_MS)]
    pub quality_window_ms: u64,
    /// A part not delivered this long after a later part came was skipped
    /// at its playout deadline (missing).
    #[arg(long, default_value_t = DEFAULT_PLAYOUT_MS)]
    pub playout_ms: u64,
    /// Missing parts in the window that move the leg (at least), and their
    /// share of the parts due (thousandths, at least).
    #[arg(long, default_value_t = DEFAULT_MISSING_PARTS)]
    pub missing_parts: u32,
    #[arg(long, default_value_t = DEFAULT_MISSING_PERMILLE)]
    pub missing_permille: u32,
    /// A node left for late or missing parts (or its distress) is not
    /// taken again for this long.
    #[arg(long, default_value_t = crate::client_director::DEFAULT_QUALITY_HOLD_OFF_MS)]
    pub quality_hold_off_ms: u64,
    /// Such moves at most this many a minute.
    #[arg(long, default_value_t = crate::client_director::DEFAULT_QUALITY_MOVES_PER_MINUTE)]
    pub quality_moves_per_minute: u32,
    /// A degradation within this long of such a move is not moved for (the
    /// new edge is no better: upstream), and holds them off for the
    /// hold-off.
    #[arg(long, default_value_t = crate::client_director::DEFAULT_QUALITY_NO_BETTER_MS)]
    pub quality_no_better_ms: u64,
    /// A replacement that brought nothing by then is released and the old
    /// leg kept.
    #[arg(long, default_value_t = crate::client_director::DEFAULT_QUALITY_GIVEUP_MS)]
    pub quality_giveup_ms: u64,
}

impl QualityArgs {
    /// The session's rules.
    pub fn config(&self) -> QualityConfig {
        QualityConfig {
            enabled: self.quality_moves,
            late_part: Duration::from_millis(self.late_part_ms),
            late_p95: Duration::from_millis(self.late_p95_ms),
            window: Duration::from_millis(self.quality_window_ms.max(1)),
            playout: Duration::from_millis(self.playout_ms),
            missing_parts: self.missing_parts.max(1),
            missing_permille: self.missing_permille,
        }
    }

    /// The director's guards.
    pub fn apply(&self, director: &mut crate::client_director::DirectorConfig) {
        director.quality_hold_off = Duration::from_millis(self.quality_hold_off_ms);
        director.quality_moves_per_minute = self.quality_moves_per_minute;
        director.quality_no_better = Duration::from_millis(self.quality_no_better_ms);
        director.quality_giveup = Duration::from_millis(self.quality_giveup_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PART_US: u64 = 41_667;

    /// A stream of parts at 24 a second: part n published at n * PART_US,
    /// arriving `delay` later, judged as it goes.
    struct Feed {
        judge: QualityJudge,
        start: Instant,
    }

    impl Feed {
        fn new() -> Self {
            Self {
                judge: QualityJudge::new(QualityConfig::default()),
                start: Instant::now(),
            }
        }

        fn part(&mut self, part: u64, delay_us: u64) -> Option<Verdict> {
            let published = 1_000_000 + part * PART_US;
            let arrival = published + delay_us;
            let now = self.start + Duration::from_micros(arrival);
            self.judge.on_part(part, published, arrival, now);
            self.judge.judge(now)
        }
    }

    #[test]
    fn steady_parts_are_neither_late_nor_missing() {
        let mut feed = Feed::new();
        for part in 0..2_000 {
            // A 20-300 ms spread of a 4K stream's sizes.
            let delay = 20_000 + (part * 7_919 % 280) * 1_000;
            assert_eq!(feed.part(part, delay), None, "part {part}");
        }
        assert_eq!((feed.judge.late_total, feed.judge.missing_total), (0, 0));
    }

    #[test]
    fn parts_that_grow_late_are_judged_within_the_window() {
        let mut feed = Feed::new();
        for part in 0..240 {
            assert_eq!(feed.part(part, 30_000), None);
        }
        // The send side stalls: each part leaves 50 ms later than the last.
        let mut verdict = None;
        let mut at = 0;
        for (index, part) in (240..400).enumerate() {
            verdict = feed.part(part, 30_000 + index as u64 * 50_000);
            if verdict.is_some() {
                at = index;
                break;
            }
        }
        assert_eq!(verdict, Some(Verdict::LateParts));
        // p95 over 500 ms in the window: about 0.6 s into the stall.
        assert!(at <= 16, "judged after {at} parts");
    }

    #[test]
    fn parts_two_seconds_late_are_late_and_a_paused_source_is_not() {
        let mut feed = Feed::new();
        for part in 0..240 {
            feed.part(part, 30_000);
        }
        // A pause of the source: nothing published, nothing late.
        let paused = feed.start + Duration::from_secs(30);
        assert_eq!(feed.judge.judge(paused), None);
        assert_eq!(feed.part(240, 30_000), None);
        assert_eq!(feed.part(241, 2_030_000), None, "one late part is not enough");
        assert_eq!(feed.part(242, 2_030_000), Some(Verdict::LateParts));
    }

    #[test]
    fn parts_skipped_at_their_deadline_are_missing() {
        let mut feed = Feed::new();
        for part in 0..240 {
            feed.part(part, 30_000);
        }
        // Parts 240 and 241 never come; the later ones do.
        let mut verdict = None;
        for part in 242..300 {
            verdict = verdict.or(feed.part(part, 30_000));
        }
        assert_eq!(verdict, Some(Verdict::MissingParts));
        assert_eq!(feed.judge.missing_total, 2);
        // A hole filled within the deadline is not missing.
        let mut feed = Feed::new();
        for part in 0..240 {
            feed.part(part, 30_000);
        }
        feed.part(241, 30_000);
        feed.part(242, 30_000);
        feed.part(240, 300_000);
        for part in 243..300 {
            assert_eq!(feed.part(part, 30_000), None);
        }
        assert_eq!(feed.judge.missing_total, 0);
    }

    #[test]
    fn a_new_numbering_opens_no_holes() {
        let mut feed = Feed::new();
        for part in 0..100 {
            feed.part(part, 30_000);
        }
        feed.judge.new_flow();
        for part in 0..100 {
            assert_eq!(feed.part(1_000_000 + part, 30_000), None);
        }
        assert_eq!(feed.judge.missing_total, 0);
    }

    #[test]
    fn the_histogram_gives_the_bucket_of_a_quantile() {
        let mut histogram = LatenessHistogram::default();
        for ms in 0..100 {
            histogram.record(ms * 1_000);
        }
        histogram.record(2_500_000);
        assert_eq!(histogram.quantile_ms(0.5), Some(63));
        assert_eq!(histogram.quantile_ms(0.999), Some(4_095));
        assert_eq!(histogram.max_us, 2_500_000);
    }
}
