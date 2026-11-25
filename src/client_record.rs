//! What a UDP client keeps of a run for its report and its output files, in
//! memory that does not grow with the run (udp-client memory round, 25 Nov
//! 2026).
//!
//! The referee's 4K probe (`needletail-udp-client --arrivals --write-parts`)
//! kept every part's bytes until the end of the run to write them in part
//! order: at 240 Mbit/s for 300 s it reached 7.6 GB and the kernel killed it
//! on every viewer host. Here:
//!
//! - [`PartsWriter`]: each part goes, as it is taken, to a writer thread
//!   through a queue bounded in bytes; the thread puts the parts back in
//!   part order ([`OrderedOutput`], a window bounded in time, parts and
//!   bytes) and writes the arrivals line and the bytes of each, so that the
//!   files are those of before, byte for byte. A queue that is full (the
//!   disk cannot keep up) drops the part from both files and counts it: the
//!   receive loop never waits for the disk and never holds more.
//! - [`PartLedger`]: the parts received, gaps, missing parts and jumps of
//!   the report, in a fixed window of [`LEDGER_WINDOW`] parts behind the
//!   newest; what leaves the window is settled into counters.
//! - [`BoundedSamples`]: the timing samples of the report's percentiles,
//!   exact (the values of before) up to [`EXACT_SAMPLES`], then a fixed
//!   log histogram (0.27 % wide buckets).

use bytes::Bytes;
use serde::Serialize;
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Samples.

/// Samples kept exactly, at most (8 bytes each): a run with fewer reports
/// the very values of before.
pub const EXACT_SAMPLES: usize = 65_536;
/// The smallest magnitude the histogram tells from zero.
const HIST_MIN: f64 = 1e-3;
/// Buckets an octave: each 0.54 % wide, a value read within 0.27 %.
const HIST_STEPS: f64 = 128.0;
/// 34 octaves above `HIST_MIN` (to 1.7e7); larger values share the last
/// bucket (the maximum is kept exactly).
const HIST_BUCKETS: usize = 34 * 128;

#[derive(Debug, Clone)]
struct LogHistogram {
    zero: u64,
    positive: Vec<u64>,
    negative: Vec<u64>,
}

impl LogHistogram {
    fn new() -> Self {
        Self {
            zero: 0,
            positive: Vec::new(),
            negative: Vec::new(),
        }
    }

    fn bucket(magnitude: f64) -> usize {
        (((magnitude / HIST_MIN).log2() * HIST_STEPS).floor() as usize).min(HIST_BUCKETS - 1)
    }

    fn value(bucket: usize) -> f64 {
        HIST_MIN * ((bucket as f64 + 0.5) / HIST_STEPS).exp2()
    }

    fn record(&mut self, value: f64) {
        let magnitude = value.abs();
        if magnitude.is_nan() || magnitude < HIST_MIN {
            self.zero += 1;
            return;
        }
        let side = if value < 0.0 {
            &mut self.negative
        } else {
            &mut self.positive
        };
        if side.is_empty() {
            side.resize(HIST_BUCKETS, 0);
        }
        side[Self::bucket(magnitude)] += 1;
    }

    /// The value of the sample of `rank` (0-based) in ascending order.
    fn at_rank(&self, rank: u64) -> f64 {
        let mut seen = 0_u64;
        for (bucket, count) in self.negative.iter().enumerate().rev() {
            seen += count;
            if seen > rank {
                return -Self::value(bucket);
            }
        }
        seen += self.zero;
        if seen > rank {
            return 0.0;
        }
        for (bucket, count) in self.positive.iter().enumerate() {
            seen += count;
            if seen > rank {
                return Self::value(bucket);
            }
        }
        0.0
    }
}

/// Samples of one figure of the report, for its percentiles: the values
/// themselves up to a cap, then a fixed histogram (bounded however long
/// the run). The quantile of `p` is the sample of rank
/// `round((count - 1) * p)` in ascending order, as the clients' sorted
/// vectors gave it.
#[derive(Debug, Clone)]
pub struct BoundedSamples {
    exact: Vec<f64>,
    cap: usize,
    histogram: Option<Box<LogHistogram>>,
    count: u64,
    min: f64,
    max: f64,
    sorted: bool,
}

impl Default for BoundedSamples {
    fn default() -> Self {
        Self::new()
    }
}

impl BoundedSamples {
    pub fn new() -> Self {
        Self::with_exact(EXACT_SAMPLES)
    }

    /// Keep at most `cap` samples exactly.
    pub fn with_exact(cap: usize) -> Self {
        Self {
            exact: Vec::new(),
            cap,
            histogram: None,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sorted: true,
        }
    }

    pub fn push(&mut self, value: f64) {
        self.count += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        if let Some(histogram) = &mut self.histogram {
            histogram.record(value);
            return;
        }
        if self.exact.len() < self.cap {
            self.exact.push(value);
            self.sorted = false;
            return;
        }
        let mut histogram = Box::new(LogHistogram::new());
        for value in self.exact.drain(..) {
            histogram.record(value);
        }
        self.exact = Vec::new();
        histogram.record(value);
        self.histogram = Some(histogram);
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Whether the values are still kept exactly.
    pub fn is_exact(&self) -> bool {
        self.histogram.is_none()
    }

    pub fn max(&self) -> Option<f64> {
        (self.count > 0).then_some(self.max)
    }

    /// The `p` quantile (0..=1), as the sorted values gave it.
    pub fn quantile(&mut self, p: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        self.at_rank(((self.count - 1) as f64 * p).round() as u64)
    }

    /// The sample of `rank` (0-based) in ascending order.
    pub fn at_rank(&mut self, rank: u64) -> Option<f64> {
        if rank >= self.count {
            return None;
        }
        if let Some(histogram) = &self.histogram {
            if rank + 1 >= self.count {
                return Some(self.max);
            }
            if rank == 0 {
                return Some(self.min);
            }
            return Some(histogram.at_rank(rank).clamp(self.min, self.max));
        }
        if !self.sorted {
            self.exact.sort_by(f64::total_cmp);
            self.sorted = true;
        }
        Some(self.exact[rank as usize])
    }

    /// Bytes this holds (for the memory tests).
    pub fn held_bytes(&self) -> usize {
        self.exact.capacity() * 8
            + self.histogram.as_ref().map_or(0, |histogram| {
                (histogram.positive.capacity() + histogram.negative.capacity()) * 8
            })
    }
}

// ---------------------------------------------------------------------------
// Parts received.

/// Parts behind the newest whose state is kept one by one (two bits each):
/// far past a hole's give-up (3,072 parts) and the receiver's window
/// (4,096). A part that comes further behind than this is counted in
/// [`PartLedger::beyond_window`] and not taken.
pub const LEDGER_WINDOW: u64 = 65_536;
const LEDGER_WORDS: usize = (LEDGER_WINDOW / 64) as usize;
/// The first missing parts named in the report.
pub const MISSING_EXAMPLES: usize = 20;
/// Jumps recorded with their ends, at most (their parts are all counted).
pub const JUMPS_KEPT: usize = 1_024;

/// What [`PartLedger::receive`] made of a part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    New,
    Duplicate,
    /// Further behind the newest than the ledger's window.
    BeyondWindow,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct Settled {
    first: Option<u64>,
    last: Option<u64>,
    missing: u64,
    examples: Vec<u64>,
    jumps: Vec<[u64; 2]>,
    jump_parts: u64,
}

impl Settled {
    /// A part present (received or a gap) after `last`: the run between is
    /// a jump if longer than `jump`, else missing parts, each passed to
    /// `missing_part` with `want_each`.
    fn present(&mut self, part: u64, jump: u64, mut missing_part: Option<&mut dyn FnMut(u64)>) {
        self.first.get_or_insert(part);
        if let Some(previous) = self.last {
            let run = part - previous - 1;
            if run > jump {
                if self.jumps.len() < JUMPS_KEPT {
                    self.jumps.push([previous, part]);
                }
                self.jump_parts += run;
            } else if run > 0 {
                self.missing += run;
                let want = MISSING_EXAMPLES - self.examples.len();
                self.examples.extend((previous + 1..part).take(want));
                if let Some(each) = missing_part.as_mut() {
                    (previous + 1..part).for_each(|missing| each(missing));
                }
            }
        }
        self.last = Some(part);
    }
}

/// The report's account of the parts (as `needletail-udp-client`'s
/// `account_parts` made it from every part number of the run).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PartAccount {
    /// The first and last parts present (received, or a gap).
    pub span: Option<(u64, u64)>,
    /// The parts due in the span, less the jumps.
    pub expected: u64,
    pub missing: u64,
    /// The first [`MISSING_EXAMPLES`] missing parts.
    pub missing_examples: Vec<u64>,
    pub jumps: Vec<[u64; 2]>,
    pub jump_parts: u64,
    /// The missing parts still in the ledger's window (they may still
    /// come), in order.
    pub window_missing: Vec<u64>,
    /// Missing parts behind the window (they will not come).
    pub older_missing: bool,
}

/// The parts of a run: received, gaps, and from them the missing parts and
/// jumps, in a fixed window of [`LEDGER_WINDOW`] parts behind the newest
/// (32 KiB whatever the run's length).
#[derive(Debug, Clone)]
pub struct PartLedger {
    /// A run of missing parts longer than this is a jump.
    jump: u64,
    /// The lowest part not settled (the window is `base..base + WINDOW`).
    base: Option<u64>,
    got: Box<[u64]>,
    gap: Box<[u64]>,
    newest: Option<u64>,
    settled: Settled,
    received: u64,
    gaps: u64,
    beyond_window: u64,
}

fn slot(part: u64) -> (usize, u64) {
    let index = part % LEDGER_WINDOW;
    ((index / 64) as usize, 1 << (index % 64))
}

impl PartLedger {
    /// `jump`: a run of more missing parts than this is a jump.
    pub fn new(jump: u64) -> Self {
        Self {
            jump,
            base: None,
            got: vec![0; LEDGER_WORDS].into_boxed_slice(),
            gap: vec![0; LEDGER_WORDS].into_boxed_slice(),
            newest: None,
            settled: Settled::default(),
            received: 0,
            gaps: 0,
            beyond_window: 0,
        }
    }

    /// Parts received (each once).
    pub fn received(&self) -> u64 {
        self.received
    }

    /// Parts delivered as gaps and not received after.
    pub fn gaps(&self) -> u64 {
        self.gaps
    }

    /// Parts or gaps that came further behind the newest than the window.
    pub fn beyond_window(&self) -> u64 {
        self.beyond_window
    }

    /// Make `part` a part of the window, settling what falls out of it:
    /// false if it is behind the window.
    fn place(&mut self, part: u64) -> bool {
        let base = *self
            .base
            .get_or_insert(part.saturating_sub(LEDGER_WINDOW / 4));
        if part < base {
            self.beyond_window += 1;
            return false;
        }
        if part >= base + LEDGER_WINDOW {
            self.settle_to(part + 1 - LEDGER_WINDOW);
        }
        self.newest = Some(self.newest.map_or(part, |newest| newest.max(part)));
        true
    }

    fn settle_to(&mut self, upto: u64) {
        let Some(base) = self.base else {
            return;
        };
        let end = upto.min(base + LEDGER_WINDOW);
        let mut part = base;
        while part < end {
            let (word, bit) = slot(part);
            if bit == 1 && part + 64 <= end && self.got[word] == 0 && self.gap[word] == 0 {
                part += 64;
                continue;
            }
            if (self.got[word] | self.gap[word]) & bit != 0 {
                self.settled.present(part, self.jump, None);
            }
            self.got[word] &= !bit;
            self.gap[word] &= !bit;
            part += 1;
        }
        self.base = Some(upto.max(base));
    }

    /// Whether `part` was received (None: behind the window).
    pub fn is_received(&self, part: u64) -> Option<bool> {
        match self.base {
            Some(base) if part < base => None,
            Some(_) => {
                let (word, bit) = slot(part);
                Some(self.got[word] & bit != 0)
            }
            None => Some(false),
        }
    }

    /// A part came: new (a gap of it is undone), a duplicate, or behind the
    /// window (not taken).
    pub fn receive(&mut self, part: u64) -> Mark {
        if !self.place(part) {
            return Mark::BeyondWindow;
        }
        let (word, bit) = slot(part);
        if self.got[word] & bit != 0 {
            return Mark::Duplicate;
        }
        self.got[word] |= bit;
        if self.gap[word] & bit != 0 {
            self.gap[word] &= !bit;
            self.gaps -= 1;
        }
        self.received += 1;
        Mark::New
    }

    /// A gap of `part` came (the edge never had it).
    pub fn gap(&mut self, part: u64) {
        if !self.place(part) {
            return;
        }
        let (word, bit) = slot(part);
        if self.gap[word] & bit == 0 {
            self.gap[word] |= bit;
            self.gaps += 1;
        }
    }

    /// The account of the parts so far (the window is read, not settled).
    pub fn account(&self) -> PartAccount {
        let mut settled = self.settled.clone();
        let mut window_missing = Vec::new();
        let mut older_missing = settled.missing > 0;
        if let (Some(base), Some(newest)) = (self.base, self.newest) {
            let mut each = |missing: u64| {
                if missing >= base {
                    window_missing.push(missing);
                } else {
                    older_missing = true;
                }
            };
            for part in base..=newest.min(base + LEDGER_WINDOW - 1) {
                let (word, bit) = slot(part);
                if (self.got[word] | self.gap[word]) & bit != 0 {
                    settled.present(part, self.jump, Some(&mut each));
                }
            }
        }
        let span = settled.first.zip(settled.last);
        PartAccount {
            span,
            expected: span.map_or(0, |(first, last)| last - first + 1 - settled.jump_parts),
            missing: settled.missing,
            missing_examples: settled.examples,
            jumps: settled.jumps,
            jump_parts: settled.jump_parts,
            window_missing,
            older_missing,
        }
    }

    /// Bytes this holds (for the memory tests).
    pub fn held_bytes(&self) -> usize {
        (self.got.len() + self.gap.len()) * 8
            + (self.settled.examples.capacity() + self.settled.jumps.capacity() * 2) * 8
    }
}

// ---------------------------------------------------------------------------
// Output files.

/// How long and how much the writer holds parts that came out of order,
/// waiting for the parts before them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderLimits {
    /// A part is written at the latest this long after it came (a hole's
    /// give-up and a margin: a hole later filled lands in its place).
    pub hold: Duration,
    /// Bytes of parts held, at most: past this the oldest are written.
    pub max_bytes: usize,
    /// A part is written at the latest once this many parts behind the
    /// newest.
    pub max_parts: u64,
}

/// The reorder window of the output files: a hole's give-up and half a
/// second; 128 MiB (3.2 s of 4K at 320 Mbit/s); the receiver's window.
pub const ORDER_LIMITS: OrderLimits = OrderLimits {
    hold: Duration::from_millis(3_500),
    max_bytes: 128 << 20,
    max_parts: crate::client_compact::STALE_PARTS,
};

/// Bytes queued for the writer thread, at most: 1.6 s of 4K at 320 Mbit/s
/// that the disk may fall behind before parts are dropped from the files.
pub const QUEUE_BYTES: usize = 64 << 20;
/// Entries queued for the writer thread, at most (1 ms audio: 16 s).
pub const QUEUE_ENTRIES: usize = 16_384;

/// What the output wrote and could not write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct OutputStats {
    /// Parts written (a line, its bytes, or both).
    pub written: u64,
    /// Parts that came after a later part had been written: not in the
    /// files (the order of the files is the parts').
    pub late: u64,
    /// The most bytes and parts held for order at once.
    pub held_peak_bytes: u64,
    pub held_peak_parts: u64,
}

struct Held {
    line: Option<String>,
    payload: Option<Bytes>,
    at: Instant,
}

/// The arrivals file and the parts file, written in part order from parts
/// that come in any order: a part is written once every part before it has
/// been written or given up ([`OrderLimits`]); the files are those of the
/// whole run sorted by part, which the client wrote before at the end of
/// the run from every part kept in memory.
pub struct OrderedOutput<A: Write, P: Write> {
    arrivals: Option<A>,
    parts: Option<P>,
    limits: OrderLimits,
    held: BTreeMap<u64, Held>,
    held_bytes: usize,
    /// Parts below this have been written or passed over.
    next: Option<u64>,
    newest: Option<u64>,
    pub stats: OutputStats,
}

impl<A: Write, P: Write> OrderedOutput<A, P> {
    pub fn new(arrivals: Option<A>, parts: Option<P>, limits: OrderLimits) -> Self {
        Self {
            arrivals,
            parts,
            limits,
            held: BTreeMap::new(),
            held_bytes: 0,
            next: None,
            newest: None,
            stats: OutputStats::default(),
        }
    }

    pub fn held_bytes(&self) -> usize {
        self.held_bytes
    }

    pub fn held_parts(&self) -> usize {
        self.held.len()
    }

    /// Part `part` came at `at`: its arrivals line and its bytes.
    pub fn push(
        &mut self,
        part: u64,
        line: Option<String>,
        payload: Option<Bytes>,
        at: Instant,
    ) -> io::Result<()> {
        if self.next.is_some_and(|next| part < next) || self.held.contains_key(&part) {
            self.stats.late += 1;
            return Ok(());
        }
        self.held_bytes += payload.as_ref().map_or(0, Bytes::len);
        self.held.insert(part, Held { line, payload, at });
        self.newest = Some(self.newest.map_or(part, |newest| newest.max(part)));
        self.stats.held_peak_bytes = self.stats.held_peak_bytes.max(self.held_bytes as u64);
        self.stats.held_peak_parts = self.stats.held_peak_parts.max(self.held.len() as u64);
        self.release(at)
    }

    /// Write the parts that are due at `now`.
    pub fn release(&mut self, now: Instant) -> io::Result<()> {
        while let Some(entry) = self.held.first_entry() {
            let part = *entry.key();
            let held = entry.get();
            let due = self.next == Some(part)
                || self.held_bytes > self.limits.max_bytes
                || now.saturating_duration_since(held.at) >= self.limits.hold
                || self
                    .newest
                    .is_some_and(|newest| newest - part >= self.limits.max_parts);
            if !due {
                break;
            }
            let held = entry.remove();
            self.write(part, held)?;
        }
        Ok(())
    }

    fn write(&mut self, part: u64, held: Held) -> io::Result<()> {
        self.next = Some(part + 1);
        self.stats.written += 1;
        if let Some(payload) = &held.payload {
            self.held_bytes -= payload.len();
        }
        if let (Some(file), Some(line)) = (self.arrivals.as_mut(), held.line.as_deref()) {
            file.write_all(line.as_bytes())?;
        }
        if let (Some(file), Some(payload)) = (self.parts.as_mut(), held.payload.as_deref()) {
            file.write_all(payload)?;
        }
        Ok(())
    }

    /// Write what is held, in order, and flush: the end of the run.
    pub fn finish(&mut self) -> io::Result<OutputStats> {
        while let Some((part, held)) = self.held.pop_first() {
            self.write(part, held)?;
        }
        if let Some(file) = self.arrivals.as_mut() {
            file.flush()?;
        }
        if let Some(file) = self.parts.as_mut() {
            file.flush()?;
        }
        Ok(self.stats)
    }
}

/// What [`PartsWriter`] reports at the end of a run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct WriterReport {
    #[serde(flatten)]
    pub output: OutputStats,
    /// Parts dropped from the files because the writer's queue was full
    /// (the disk did not keep up).
    pub queue_full: u64,
    /// The most bytes queued for the writer at once.
    pub queued_peak_bytes: u64,
}

struct Entry {
    part: u64,
    line: Option<String>,
    payload: Option<Bytes>,
    at: Instant,
}

impl Entry {
    fn bytes(&self) -> usize {
        self.line.as_ref().map_or(0, String::len) + self.payload.as_ref().map_or(0, Bytes::len)
    }
}

/// The output files written on a thread of their own: [`Self::send`] never
/// blocks, and holds at most `queue_bytes` (or one part, when the queue is
/// empty) besides the writer's [`OrderLimits::max_bytes`].
pub struct PartsWriter {
    tx: Option<SyncSender<Entry>>,
    queued: Arc<AtomicUsize>,
    queue_bytes: usize,
    queue_full: u64,
    queued_peak: usize,
    thread: Option<JoinHandle<io::Result<OutputStats>>>,
}

impl PartsWriter {
    pub fn spawn<A, P>(
        arrivals: Option<A>,
        parts: Option<P>,
        limits: OrderLimits,
        queue_bytes: usize,
    ) -> io::Result<Self>
    where
        A: Write + Send + 'static,
        P: Write + Send + 'static,
    {
        let (tx, rx) = mpsc::sync_channel::<Entry>(QUEUE_ENTRIES);
        let queued = Arc::new(AtomicUsize::new(0));
        let taken = Arc::clone(&queued);
        let thread = std::thread::Builder::new()
            .name("parts-writer".into())
            .spawn(move || {
                let mut output = OrderedOutput::new(arrivals, parts, limits);
                let mut failed: Option<io::Error> = None;
                loop {
                    let result = match rx.recv_timeout(Duration::from_millis(50)) {
                        Ok(entry) => {
                            taken.fetch_sub(entry.bytes(), Ordering::Relaxed);
                            if failed.is_some() {
                                continue;
                            }
                            output.push(entry.part, entry.line, entry.payload, entry.at)
                        }
                        Err(RecvTimeoutError::Timeout) if failed.is_none() => {
                            output.release(Instant::now())
                        }
                        Err(RecvTimeoutError::Timeout) => Ok(()),
                        Err(RecvTimeoutError::Disconnected) => break,
                    };
                    if let Err(error) = result {
                        failed.get_or_insert(error);
                    }
                }
                match failed {
                    Some(error) => Err(error),
                    None => output.finish(),
                }
            })?;
        Ok(Self {
            tx: Some(tx),
            queued,
            queue_bytes,
            queue_full: 0,
            queued_peak: 0,
            thread: Some(thread),
        })
    }

    /// Hand part `part` to the writer: false if the queue was full (it is
    /// counted, and the part is in neither file).
    pub fn send(&mut self, part: u64, line: Option<String>, payload: Option<Bytes>) -> bool {
        let entry = Entry {
            part,
            line,
            payload,
            at: Instant::now(),
        };
        let bytes = entry.bytes();
        let queued = self.queued.load(Ordering::Relaxed);
        if queued > 0 && queued + bytes > self.queue_bytes {
            self.queue_full += 1;
            return false;
        }
        let Some(tx) = &self.tx else {
            return false;
        };
        self.queued.fetch_add(bytes, Ordering::Relaxed);
        match tx.try_send(entry) {
            Ok(()) => {
                self.queued_peak = self.queued_peak.max(queued + bytes);
                true
            }
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.queued.fetch_sub(bytes, Ordering::Relaxed);
                self.queue_full += 1;
                false
            }
        }
    }

    /// Bytes queued for the writer now.
    pub fn queued_bytes(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }

    /// Parts dropped so far for a full queue.
    pub fn queue_full(&self) -> u64 {
        self.queue_full
    }

    /// The end of the run: what is held is written, the files flushed.
    pub fn finish(mut self) -> io::Result<WriterReport> {
        drop(self.tx.take());
        let output = match self.thread.take().map(JoinHandle::join) {
            Some(Ok(result)) => result?,
            Some(Err(_)) => return Err(io::Error::other("the parts writer panicked")),
            None => OutputStats::default(),
        };
        Ok(WriterReport {
            output,
            queue_full: self.queue_full,
            queued_peak_bytes: self.queued_peak as u64,
        })
    }
}

impl Drop for PartsWriter {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The old code: every value kept, sorted, indexed.
    fn sorted_quantile(values: &[f64], p: f64) -> f64 {
        let mut values = values.to_vec();
        values.sort_by(f64::total_cmp);
        values[((values.len() - 1) as f64 * p).round() as usize]
    }

    #[test]
    fn samples_are_exact_up_to_the_cap_and_close_after() {
        let values: Vec<f64> = (0..5_000_u64)
            .map(|index| ((index * 7_919) % 10_007) as f64 / 37.0 - 20.0)
            .collect();
        let mut exact = BoundedSamples::new();
        let mut folded = BoundedSamples::with_exact(1_000);
        for value in &values {
            exact.push(*value);
            folded.push(*value);
        }
        assert!(exact.is_exact());
        assert!(!folded.is_exact());
        for p in [0.0, 0.5, 0.95, 0.99, 1.0] {
            let want = sorted_quantile(&values, p);
            assert_eq!(exact.quantile(p), Some(want), "p {p}");
            let got = folded.quantile(p).unwrap();
            assert!(
                (got - want).abs() <= want.abs() * 0.006 + 2e-3,
                "p {p}: {got} against {want}"
            );
        }
        assert_eq!(folded.count(), 5_000);
        assert_eq!(folded.max(), Some(sorted_quantile(&values, 1.0)));
        // The histogram replaced the values: a fixed size from then on.
        let held = folded.held_bytes();
        for value in &values {
            folded.push(*value);
        }
        assert_eq!(folded.held_bytes(), held);
        assert!(held <= 2 * HIST_BUCKETS * 8);
    }

    /// The old account: every part number of the run in two sets.
    fn old_account(
        received: &std::collections::BTreeSet<u64>,
        gaps: &std::collections::BTreeSet<u64>,
        jump: u64,
    ) -> (Option<(u64, u64)>, u64, Vec<u64>, Vec<[u64; 2]>, u64) {
        let all: std::collections::BTreeSet<u64> = received.union(gaps).copied().collect();
        let (mut missing, mut jumps, mut jump_parts) = (Vec::new(), Vec::new(), 0);
        let mut previous: Option<u64> = None;
        for &part in &all {
            if let Some(previous) = previous {
                let run = part - previous - 1;
                if run > jump {
                    jumps.push([previous, part]);
                    jump_parts += run;
                } else {
                    missing.extend(previous + 1..part);
                }
            }
            previous = Some(part);
        }
        let span = all.first().copied().zip(all.last().copied());
        let expected = span.map_or(0, |(first, last)| last - first + 1 - jump_parts);
        (span, expected, missing, jumps, jump_parts)
    }

    #[test]
    fn the_ledger_accounts_as_every_part_kept() {
        let jump = 4_096;
        let mut ledger = PartLedger::new(jump);
        let (mut received, mut gaps) = (
            std::collections::BTreeSet::new(),
            std::collections::BTreeSet::new(),
        );
        let mut duplicates = 0;
        // 200,000 parts (three windows), some lost, some late, some gaps
        // later filled, a jump of 2^31, and parts behind a hole.
        let mut events: Vec<(u64, bool)> = Vec::new();
        for part in 1_000..201_000_u64 {
            let at = if part >= 150_000 {
                part + (1 << 31)
            } else {
                part
            };
            match part % 997 {
                0 | 1 => {}
                5 => events.push((at, true)),
                6 => {
                    events.push((at, true));
                    events.push((at, false));
                }
                _ => events.push((at, false)),
            }
            if part % 1_000 == 0 {
                // A late part, and a duplicate.
                events.push((at - 30, false));
                events.push((at - 2, false));
            }
        }
        for (part, gap) in events {
            if gap {
                ledger.gap(part);
                gaps.insert(part);
                continue;
            }
            let old = if received.contains(&part) {
                duplicates += 1;
                Mark::Duplicate
            } else {
                gaps.remove(&part);
                received.insert(part);
                Mark::New
            };
            assert_eq!(ledger.receive(part), old, "part {part}");
        }
        let (span, expected, missing, jumps, jump_parts) = old_account(&received, &gaps, jump);
        let account = ledger.account();
        assert_eq!(account.span, span);
        assert_eq!(account.expected, expected);
        assert_eq!(account.missing, missing.len() as u64);
        assert_eq!(account.missing_examples, missing[..MISSING_EXAMPLES]);
        assert_eq!(account.jumps, jumps);
        assert_eq!(account.jump_parts, jump_parts);
        assert_eq!(ledger.received(), received.len() as u64);
        assert_eq!(ledger.gaps(), gaps.len() as u64);
        assert!(duplicates > 0);
        assert_eq!(ledger.beyond_window(), 0);
        // The window's missing parts are the newest of the old list.
        let window: Vec<u64> = missing
            .iter()
            .copied()
            .filter(|part| *part + LEDGER_WINDOW > span.unwrap().1)
            .collect();
        assert_eq!(account.window_missing, window);
        assert!(account.older_missing);
        assert!(ledger.held_bytes() < 40 * 1024);
    }

    #[test]
    fn the_output_is_the_parts_in_order() {
        let start = Instant::now();
        let mut output = OrderedOutput::new(Some(Vec::new()), Some(Vec::new()), ORDER_LIMITS);
        let order = [10_u64, 12, 11, 14, 15, 13, 16, 20, 18, 17, 19];
        for (index, part) in order.iter().enumerate() {
            let at = start + Duration::from_millis(index as u64);
            output
                .push(
                    *part,
                    Some(format!("{part}\n")),
                    Some(Bytes::from(vec![*part as u8; *part as usize])),
                    at,
                )
                .unwrap();
        }
        // Before the first part is known to be the first, and behind a hole
        // (21), parts are held until their give-up.
        output
            .push(
                22,
                Some("22\n".into()),
                Some(Bytes::from(vec![22; 22])),
                start,
            )
            .unwrap();
        assert_eq!(output.held_parts(), 12);
        output.release(start + ORDER_LIMITS.hold).unwrap();
        assert_eq!(output.held_parts(), 0);
        // The next part in order is written at once.
        output
            .push(
                23,
                Some("23\n".into()),
                Some(Bytes::from(vec![23; 23])),
                start,
            )
            .unwrap();
        assert_eq!(output.held_parts(), 0);
        // 21 after its give-up: late, in neither file.
        output
            .push(
                21,
                Some("21\n".into()),
                Some(Bytes::from(vec![21; 21])),
                start,
            )
            .unwrap();
        let stats = output.finish().unwrap();
        assert_eq!((stats.written, stats.late), (13, 1));
        let lines = String::from_utf8(output.arrivals.take().unwrap()).unwrap();
        assert_eq!(
            lines,
            "10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20\n22\n23\n"
        );
        let bytes = output.parts.take().unwrap();
        let want: Vec<u8> = (10..=20_u8)
            .chain([22, 23])
            .flat_map(|part| vec![part; part as usize])
            .collect();
        assert_eq!(bytes, want);
    }

    #[test]
    fn the_output_holds_at_most_its_bytes() {
        let start = Instant::now();
        let limits = OrderLimits {
            hold: Duration::from_secs(3_600),
            max_bytes: 10_000,
            max_parts: 1 << 40,
        };
        let mut output = OrderedOutput::new(None::<Vec<u8>>, Some(io::sink()), limits);
        // Every 50th part never comes: the parts after it wait for it, up
        // to the byte bound.
        for part in (1..10_000_u64).filter(|part| part % 50 != 0) {
            output
                .push(part, None, Some(Bytes::from(vec![0; 1_000])), start)
                .unwrap();
            assert!(output.held_bytes() <= 11_000);
        }
        assert_eq!(output.stats.held_peak_bytes, 11_000);
    }
}
