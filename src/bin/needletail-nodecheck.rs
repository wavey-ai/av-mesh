//! `needletail-nodecheck`: what a node itself can say about whether it is
//! run, apart from any service on it.
//!
//! - `watch`: the stall journal's tick threads in a process of their own,
//!   written as NDJSON: every late tick with its cause, and every second a
//!   `sec` line for every CPU (what the CPU did, how late its tick thread
//!   woke, how long a fixed arithmetic loop took, and on the first CPU's line
//!   the growth of the memory manager's counters). Beside a service's own
//!   `/api/stalls` it tells a stopped process from a stopped node. As on
//!   `/api/stalls`, an event that grows by a merge is written again with the
//!   same kind, thread and start (`t_us - span_us`): the later line holds.
//! - `warm`: takes the memory the node has to spare, writes a byte in every
//!   page from one thread per CPU, gives it back, and says how long the
//!   slowest page took.
//! - `touch`: touches fresh memory at a fixed rate with or without huge
//!   pages and times every page; with `--hold` it keeps the memory until
//!   it is sent SIGTERM or SIGINT, so that a second `touch` started
//!   meanwhile takes memory the guest has not handed out before.
//! - `echo-serve`, `echo-send`: numbered datagrams echoed with the server's
//!   kernel receive stamp. A run of datagrams the kernel stamped late,
//!   together at their release, beside a run of lost ones is the signature
//!   of a node that was not run while its traffic queued; the record says
//!   how long the run was held and which end of it was lost.
//! - `boot`: the check a node runs once at boot, before its service starts:
//!   an optional warm pass, then a watch of `--check-seconds`; it writes and
//!   prints a verdict that the service's readiness reads. Its exit status is
//!   0 whatever the verdict: the service starts, and readiness keeps a bad
//!   node out.

use anyhow::{bail, Context, Result};
use av_mesh::stall_journal::{
    self, Cause, StallJournal, StallKind, StallReport, TickConfig, TickThreads,
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::io::{BufWriter, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, UdpSocket};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A page of memory.
const PAGE: usize = 4_096;
const MB: u64 = 1 << 20;
const GIB: f64 = (1_u64 << 30) as f64;
/// Events the journal of `watch` and `boot` keeps between two writes.
const WATCH_CAPACITY: usize = 16_384;
/// A touch of one page this slow is written as a line of its own.
const SLOW_TOUCH_US: u64 = 200;
/// A datagram whose one-way delay is this far above the median was held.
const HELD_US: i64 = 2_000;
/// The first bytes of an echo datagram: magic, sequence, send time and the
/// server's kernel receive stamp (big-endian).
const ECHO_MAGIC: &[u8; 4] = b"NTE1";
const ECHO_HEADER: usize = 28;
/// The schema of the boot check's record.
const BOOT_SCHEMA: &str = "needletail.node-warm.v2";

#[derive(Debug, Parser)]
#[command(
    name = "needletail-nodecheck",
    version = av_mesh::build::VERSION,
    about = "Whether a node is run: a tick journal, a warm pass, touch and echo probes, a boot check"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Tick threads on every CPU, written as NDJSON until SIGTERM or SIGINT.
    Watch(WatchArgs),
    /// Write a byte in every page of the memory the node can spare, then
    /// give it back; prints the `warm` object.
    Warm(WarmArgs),
    /// Touch fresh memory page by page at a fixed rate and time every page;
    /// SIGTERM or SIGINT ends it early, with what it has written.
    Touch(TouchArgs),
    /// Echo every numbered datagram with its kernel receive stamp, until
    /// SIGTERM or SIGINT.
    EchoServe(EchoServeArgs),
    /// Send numbered datagrams to an echo server and record what came back.
    EchoSend(EchoSendArgs),
    /// The boot check: optional warm pass, then a watch; writes the verdict.
    Boot(BootArgs),
}

#[derive(Debug, Args)]
struct WatchArgs {
    #[arg(long)]
    out: PathBuf,
    /// The tick threads' period.
    #[arg(long, default_value_t = stall_journal::DEFAULT_TICK_US)]
    tick_us: u64,
    /// A tick this late is written as an event.
    #[arg(long, default_value_t = stall_journal::TICK_LATE_US)]
    threshold_us: u64,
}

#[derive(Debug, Clone, Copy, Args)]
struct WarmArgs {
    /// Memory left untouched, out of what the kernel says is available
    /// (the referee's warm-guest.py leaves 300 MB: so does this, so that
    /// one method covers every host of a measured run).
    #[arg(long, default_value_t = DEFAULT_LEAVE_MB)]
    leave_mb: u64,
    /// Memory is taken in anonymous private mappings of this size
    /// (eligible for transparent huge pages as the system allows).
    #[arg(long, default_value_t = 64)]
    chunk_mb: u64,
    /// The pass stops here, finished or not. Default: `max_seconds_per_4gb`
    /// for every 4 GiB of the node's memory (MemTotal), at least that.
    #[arg(long)]
    max_seconds: Option<u64>,
    /// The default limit per 4 GiB of memory: the hunt lab took 43 to 79 s
    /// on 4 GB guests, another lab of 24 Nov 83 to 95 s, and one Tokyo edge
    /// over 240 s.
    #[arg(long, default_value_t = 300)]
    max_seconds_per_4gb: u64,
    /// A write that takes this long is listed in `stalls`.
    #[arg(long, default_value_t = 100)]
    stall_ms: u64,
    /// The host is `slow_host` when one page write takes this long.
    #[arg(long, default_value_t = 1_000)]
    slow_write_ms: u64,
    /// ... or when the pass takes more than this many seconds per 4 GiB of
    /// the node's memory (MemTotal).
    #[arg(long, default_value_t = 180)]
    slow_seconds_per_4gb: u64,
}

const DEFAULT_LEAVE_MB: u64 = 300;

impl Default for WarmArgs {
    fn default() -> Self {
        Self {
            leave_mb: DEFAULT_LEAVE_MB,
            chunk_mb: 64,
            max_seconds: None,
            max_seconds_per_4gb: 300,
            stall_ms: 100,
            slow_write_ms: 1_000,
            slow_seconds_per_4gb: 180,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Thp {
    /// `madvise(MADV_HUGEPAGE)`.
    Always,
    /// `madvise(MADV_NOHUGEPAGE)`.
    Never,
}

impl Thp {
    fn as_str(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::Never => "never",
        }
    }
}

#[derive(Debug, Args)]
struct TouchArgs {
    #[arg(long)]
    mb: u64,
    #[arg(long)]
    rate_mb_s: f64,
    /// Huge pages asked for or refused on the region; the system's setting
    /// when not given.
    #[arg(long, value_enum)]
    thp: Option<Thp>,
    #[arg(long)]
    out: PathBuf,
    /// After the last page, keep the memory mapped until SIGTERM or SIGINT,
    /// then release it (a `release` line says how long it was held).
    #[arg(long)]
    hold: bool,
}

#[derive(Debug, Args)]
struct EchoServeArgs {
    #[arg(long)]
    bind: SocketAddr,
}

#[derive(Debug, Args)]
struct EchoSendArgs {
    #[arg(long)]
    target: SocketAddr,
    #[arg(long, default_value_t = 2_000)]
    pps: u64,
    #[arg(long, default_value_t = 150)]
    bytes: usize,
    #[arg(long)]
    seconds: u64,
    #[arg(long)]
    out: PathBuf,
}

#[derive(Debug, Clone, Args)]
struct BootArgs {
    #[arg(long)]
    out: PathBuf,
    /// Run the warm pass (`warm` at its defaults) before the check.
    #[arg(long)]
    warm: bool,
    /// Seconds of watch after the warm pass; 0 watches nothing.
    #[arg(long, default_value_t = 0)]
    check_seconds: u64,
    /// A stopped tick longer than this fails the check (`stops`).
    #[arg(long, default_value_t = 20)]
    max_stop_ms: u64,
    /// Steal above this share of the CPUs' time fails the check (`steal`).
    #[arg(long, default_value_t = 10)]
    max_steal_permille: u64,
    /// The warm pass's `--leave-mb`.
    #[arg(long, default_value_t = DEFAULT_LEAVE_MB)]
    warm_leave_mb: u64,
    /// The warm pass's `--max-seconds` (default: 300 s per 4 GiB of memory).
    #[arg(long)]
    warm_max_seconds: Option<u64>,
    /// The warm pass's `--max-seconds-per-4gb`.
    #[arg(long, default_value_t = 300)]
    warm_max_seconds_per_4gb: u64,
    /// The warm pass's `--slow-write-ms`.
    #[arg(long, default_value_t = 1_000)]
    slow_write_ms: u64,
    /// The warm pass's `--slow-seconds-per-4gb`.
    #[arg(long, default_value_t = 180)]
    slow_seconds_per_4gb: u64,
}

impl BootArgs {
    fn warm_args(&self) -> WarmArgs {
        WarmArgs {
            leave_mb: self.warm_leave_mb,
            max_seconds: self.warm_max_seconds,
            max_seconds_per_4gb: self.warm_max_seconds_per_4gb,
            slow_write_ms: self.slow_write_ms,
            slow_seconds_per_4gb: self.slow_seconds_per_4gb,
            ..WarmArgs::default()
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli.command) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("needletail-nodecheck: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(command: Command) -> Result<ExitCode> {
    match command {
        Command::Watch(args) => {
            let stop = stop_on_signals();
            let mut out = create(&args.out)?;
            watch(
                TickConfig {
                    tick_us: args.tick_us,
                    threshold_us: args.threshold_us,
                    detail: true,
                },
                &mut out,
                stop,
                |_| {},
            )?;
        }
        Command::Warm(args) => {
            let report = warm_node(args);
            println!("{}", serde_json::to_string(&WarmLine::of(report, &args))?);
        }
        Command::Touch(args) => {
            let stop = stop_on_signals();
            let mut out = create(&args.out)?;
            let done = touch(
                TouchPlan {
                    mb: args.mb,
                    rate_mb_s: args.rate_mb_s,
                    thp: args.thp,
                    hold: args.hold,
                },
                &mut out,
                stop,
                |_| {},
            )?;
            println!("{}", serde_json::to_string(&done)?);
        }
        Command::EchoServe(args) => {
            let socket =
                UdpSocket::bind(args.bind).with_context(|| format!("bind {}", args.bind))?;
            echo_serve(&socket, stop_on_signals())?;
        }
        Command::EchoSend(args) => {
            let mut out = create(&args.out)?;
            let summary = echo_send(&args, &mut out)?;
            println!("{}", serde_json::to_string(&summary)?);
        }
        Command::Boot(args) => return Ok(boot_command(&args, |_| {})),
    }
    Ok(ExitCode::SUCCESS)
}

fn create(path: &Path) -> Result<BufWriter<std::fs::File>> {
    let file = std::fs::File::create(path).with_context(|| format!("create {}", path.display()))?;
    Ok(BufWriter::new(file))
}

/// Set on SIGTERM or SIGINT.
static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_stop_signal(_signal: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

/// A flag that SIGTERM and SIGINT set, so that a long run writes what it
/// has before it ends.
fn stop_on_signals() -> &'static AtomicBool {
    // SAFETY: the handler only stores to an atomic.
    unsafe {
        libc::signal(
            libc::SIGTERM,
            on_stop_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGINT,
            on_stop_signal as *const () as libc::sighandler_t,
        );
    }
    &STOP
}

/// Sleep for `duration`, or less once `stop` is set.
fn sleep_unless(stop: &AtomicBool, duration: Duration) {
    let until = Instant::now() + duration;
    while !stop.load(Ordering::Relaxed) {
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        std::thread::sleep(left.min(Duration::from_millis(50)));
    }
}

/// The node's name, for the journal's lines.
fn host_name() -> String {
    let mut buffer = [0_u8; 256];
    // SAFETY: gethostname writes at most the buffer's length.
    let ok = unsafe { libc::gethostname(buffer.as_mut_ptr().cast(), buffer.len()) } == 0;
    let end = buffer.iter().position(|&byte| byte == 0).unwrap_or(0);
    if ok && end > 0 {
        String::from_utf8_lossy(&buffer[..end]).into_owned()
    } else {
        "unknown".to_owned()
    }
}

// ---------------------------------------------------------------- watch

/// Run tick threads with `config` until `stop`, writing what the journal
/// gains every second to `out`. `started` sees the threads once they run.
fn watch(
    config: TickConfig,
    out: &mut dyn Write,
    stop: &AtomicBool,
    started: impl FnOnce(&TickThreads),
) -> Result<()> {
    let journal = Arc::new(StallJournal::new(host_name(), WATCH_CAPACITY));
    let ticks = stall_journal::spawn_ticks(&journal, config);
    if ticks.threads().is_empty() {
        bail!("no tick thread started (a zero period, or not Linux)");
    }
    started(&ticks);
    let mut since_us = 0;
    let mut write_new = |out: &mut dyn Write| -> Result<()> {
        let report = journal.query(Some(since_us));
        since_us = report.upto_us + 1;
        out.write_all(journal.render_report(&report).as_bytes())?;
        out.flush()?;
        Ok(())
    };
    while !stop.load(Ordering::Relaxed) {
        sleep_unless(stop, Duration::from_secs(1));
        write_new(out)?;
    }
    ticks.stop();
    write_new(out)
}

// ---------------------------------------------------------------- warm

/// What a warm pass did.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
struct WarmReport {
    ran: bool,
    /// Every page it meant to touch was touched within its time.
    finished: bool,
    /// Memory touched.
    gib: f64,
    seconds: f64,
    worst_page_us: u64,
    pages_over_1ms: u64,
    pages_over_100ms: u64,
    /// `MemAvailable` before the pass, what it touched and left, and its
    /// time limit.
    mem_available_before_mb: u64,
    touched_mb: u64,
    leave_mb: u64,
    max_seconds: u64,
    /// MemTotal, and the time past which the pass marks the host slow.
    mem_total_mb: u64,
    slow_bound_seconds: u64,
    /// Every write of `stall_ms` or more (at most MAX_STALLS; the rest
    /// counted in `stalls_dropped`).
    stall_ms: u64,
    stalls: Vec<WarmStall>,
    stalls_dropped: u64,
    /// `/sys/kernel/mm/transparent_hugepage/enabled` as the pass saw it.
    thp: String,
}

/// One slow write of a warm pass.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
struct WarmStall {
    /// When it ended, Unix seconds (ms precision).
    at_unix: f64,
    ms: f64,
    /// Where in the pass it was: MB from the start of the memory taken.
    after_mb: u64,
}

/// Stalls listed at most in a warm record.
const MAX_STALLS: usize = 512;

/// The `slow_host` judgement of a warm pass (integration 4, after the hunt
/// lab): a page write of `slow_write_ms` or more, a pass longer than its
/// `slow_bound_seconds` (180 s per 4 GiB of memory by default), or one
/// that ran out of time.
fn slow_reasons(warm: &WarmReport, slow_write_ms: u64) -> Vec<&'static str> {
    let mut reasons = Vec::new();
    if !warm.ran {
        return reasons;
    }
    if warm.worst_page_us >= slow_write_ms * 1_000 {
        reasons.push("page-stall");
    }
    if warm.slow_bound_seconds > 0 && warm.seconds > warm.slow_bound_seconds as f64 {
        reasons.push("slow-pass");
    }
    if !warm.finished {
        reasons.push("warm-timeout");
    }
    reasons
}

/// The line `warm` prints: the pass and its judgement.
#[derive(Debug, Serialize)]
struct WarmLine {
    #[serde(flatten)]
    warm: WarmReport,
    slow_host: bool,
    slow_reasons: Vec<&'static str>,
}

impl WarmLine {
    fn of(warm: WarmReport, args: &WarmArgs) -> Self {
        let slow_reasons = slow_reasons(&warm, args.slow_write_ms);
        Self {
            warm,
            slow_host: !slow_reasons.is_empty(),
            slow_reasons,
        }
    }
}

/// `MemAvailable` of `/proc/meminfo`, in bytes.
fn mem_available_bytes(meminfo: &str) -> Option<u64> {
    meminfo.lines().find_map(|line| {
        let rest = line.strip_prefix("MemAvailable:")?;
        let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
        Some(kb * 1_024)
    })
}

/// What a warm pass takes: what is available less the reserve.
fn warm_bytes(available: u64, leave_mb: u64) -> u64 {
    available.saturating_sub(leave_mb * MB)
}

/// `per_4gb` seconds for every 4 GiB of `total` memory, at least `per_4gb`.
fn per_4gb_seconds(total: u64, per_4gb: u64) -> u64 {
    ((total as f64 / (4.0 * GIB)) * per_4gb as f64)
        .ceil()
        .max(per_4gb as f64) as u64
}

/// `MemTotal` of `/proc/meminfo`, in bytes.
fn mem_total_bytes(meminfo: &str) -> Option<u64> {
    meminfo.lines().find_map(|line| {
        let rest = line.strip_prefix("MemTotal:")?;
        let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
        Some(kb * 1_024)
    })
}

/// The warm pass of `warm` and `boot`, sized from `/proc/meminfo`.
fn warm_node(args: WarmArgs) -> WarmReport {
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let available = mem_available_bytes(&meminfo).unwrap_or(0);
    let total = mem_total_bytes(&meminfo).unwrap_or(available);
    let cpus = stall_journal::allowed_cpus();
    let bytes = warm_bytes(available, args.leave_mb);
    let max_seconds = args
        .max_seconds
        .unwrap_or_else(|| per_4gb_seconds(total, args.max_seconds_per_4gb));
    let mut report = warm(
        bytes,
        args.chunk_mb.max(1) * MB,
        &cpus,
        Duration::from_secs(max_seconds),
        args.stall_ms,
    );
    report.mem_available_before_mb = available / MB;
    report.leave_mb = args.leave_mb;
    report.max_seconds = max_seconds;
    report.mem_total_mb = total / MB;
    report.slow_bound_seconds = per_4gb_seconds(total, args.slow_seconds_per_4gb);
    report.thp = std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled")
        .map(|text| text.trim().to_owned())
        .unwrap_or_default();
    report
}

/// An anonymous private mapping, unmapped when dropped.
struct Mapping {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: the mapping is plain memory owned by this value.
unsafe impl Send for Mapping {}

impl Mapping {
    fn new(len: usize) -> Option<Self> {
        // SAFETY: an anonymous mapping of `len` bytes; checked below.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        (ptr != libc::MAP_FAILED).then(|| Self {
            ptr: ptr.cast(),
            len,
        })
    }

    /// Write one byte in page `page` and say how long it took.
    #[inline]
    fn touch_page(&self, page: usize) -> u64 {
        let began = stall_journal::monotonic_ns();
        // SAFETY: `page * PAGE` is inside the mapping (callers stay below
        // `len / PAGE`); the write is volatile so that it happens.
        unsafe { std::ptr::write_volatile(self.ptr.add(page * PAGE), 1) };
        stall_journal::monotonic_ns().saturating_sub(began) / 1_000
    }

    fn pages(&self) -> usize {
        self.len / PAGE
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: the mapping is this value's own.
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

/// Slow pages of one thread of a warm pass.
#[derive(Debug, Default)]
struct PageTimes {
    pages: u64,
    worst_us: u64,
    over_1ms: u64,
    over_100ms: u64,
    stalls: Vec<WarmStall>,
    stalls_dropped: u64,
}

impl PageTimes {
    /// A write of `us` at `offset` bytes into the pass; listed when it
    /// took `stall_us` or more.
    fn add(&mut self, us: u64, offset: u64, stall_us: u64) {
        self.pages += 1;
        self.worst_us = self.worst_us.max(us);
        self.over_1ms += u64::from(us >= 1_000);
        self.over_100ms += u64::from(us >= 100_000);
        if us >= stall_us {
            if self.stalls.len() < MAX_STALLS {
                self.stalls.push(WarmStall {
                    at_unix: round(stall_journal::realtime_us() as f64 / 1e6, 3),
                    ms: round(us as f64 / 1e3, 1),
                    after_mb: offset / MB,
                });
            } else {
                self.stalls_dropped += 1;
            }
        }
    }

    fn merge(&mut self, other: &mut Self) {
        self.pages += other.pages;
        self.worst_us = self.worst_us.max(other.worst_us);
        self.over_1ms += other.over_1ms;
        self.over_100ms += other.over_100ms;
        self.stalls_dropped += other.stalls_dropped;
        for stall in other.stalls.drain(..) {
            if self.stalls.len() < MAX_STALLS {
                self.stalls.push(stall);
            } else {
                self.stalls_dropped += 1;
            }
        }
    }
}

/// Take `bytes` of fresh memory in chunks of `chunk`, write one byte in
/// every page from one thread pinned to each of `cpus`, then give it all
/// back. It stops at `max`, finished or not, and when the kernel refuses a
/// chunk. Every write is timed; those of `stall_ms` or more are listed.
fn warm(bytes: u64, chunk: u64, cpus: &[u16], max: Duration, stall_ms: u64) -> WarmReport {
    let stall_us = stall_ms * 1_000;
    let began = Instant::now();
    let deadline = began + max;
    let chunk = (chunk as usize).max(PAGE) / PAGE * PAGE;
    let chunks = (bytes as usize).div_ceil(chunk);
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let held: Mutex<Vec<Mapping>> = Mutex::new(Vec::with_capacity(chunks));
    let total = Mutex::new(PageTimes::default());
    let one_cpu = [0_u16];
    let cpus = if cpus.is_empty() { &one_cpu[..] } else { cpus };
    std::thread::scope(|scope| {
        for &cpu in cpus {
            let (next, failed, held, total) = (&next, &failed, &held, &total);
            scope.spawn(move || {
                stall_journal::pin_current_thread(cpu);
                let mut times = PageTimes::default();
                'chunks: loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= chunks || failed.load(Ordering::Relaxed) {
                        break;
                    }
                    let len = chunk.min(bytes as usize - index * chunk).div_ceil(PAGE) * PAGE;
                    let Some(mapping) = Mapping::new(len) else {
                        failed.store(true, Ordering::Relaxed);
                        break;
                    };
                    for page in 0..mapping.pages() {
                        if page % 256 == 0 && Instant::now() >= deadline {
                            failed.store(true, Ordering::Relaxed);
                            held.lock().unwrap().push(mapping);
                            break 'chunks;
                        }
                        let offset = (index * chunk + page * PAGE) as u64;
                        times.add(mapping.touch_page(page), offset, stall_us);
                    }
                    held.lock().unwrap().push(mapping);
                }
                total.lock().unwrap().merge(&mut times);
            });
        }
    });
    let seconds = began.elapsed().as_secs_f64();
    // Every page given back at once.
    drop(held.into_inner().unwrap());
    let mut total = total.into_inner().unwrap();
    let touched = total.pages * PAGE as u64;
    total
        .stalls
        .sort_by(|a, b| a.at_unix.total_cmp(&b.at_unix));
    WarmReport {
        ran: true,
        finished: !failed.load(Ordering::Relaxed) && touched >= bytes / PAGE as u64 * PAGE as u64,
        gib: round(touched as f64 / GIB, 3),
        seconds: round(seconds, 3),
        worst_page_us: total.worst_us,
        pages_over_1ms: total.over_1ms,
        pages_over_100ms: total.over_100ms,
        touched_mb: touched / MB,
        max_seconds: max.as_secs(),
        stall_ms,
        stalls: total.stalls,
        stalls_dropped: total.stalls_dropped,
        ..WarmReport::default()
    }
}

fn round(value: f64, places: i32) -> f64 {
    let scale = 10_f64.powi(places);
    (value * scale).round() / scale
}

// ---------------------------------------------------------------- touch

#[derive(Debug, Clone, Copy)]
struct TouchPlan {
    mb: u64,
    rate_mb_s: f64,
    thp: Option<Thp>,
    /// Keep the memory after the last page until stopped.
    hold: bool,
}

/// The line of `touch` after its last page.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
struct TouchDone {
    kind: &'static str,
    mb: u64,
    rate_mb_s: f64,
    thp: &'static str,
    /// Pages touched: all of `mb`, unless a signal ended the touch first.
    pages: u64,
    /// A signal ended the touch before its last page.
    stopped: bool,
    seconds: f64,
    slow: u64,
    max_us: u64,
    /// The process's memory in huge pages at the end (`AnonHugePages`).
    anon_huge_kb: Option<u64>,
    /// The memory is held until a signal (`--hold`).
    hold: bool,
}

/// The last line of `touch --hold`: the memory is given back.
#[derive(Debug, Serialize)]
struct TouchRelease {
    kind: &'static str,
    t_us: u64,
    /// From the last page to the release.
    held_s: f64,
}

/// One second of `touch`.
#[derive(Debug, Serialize)]
struct TouchSecond {
    kind: &'static str,
    t_us: u64,
    pages: u64,
    slow: u64,
    max_us: u64,
    sum_us: u64,
}

/// A slow page of `touch`.
#[derive(Debug, Serialize)]
struct SlowTouch {
    kind: &'static str,
    t_us: u64,
    page: usize,
    dur_us: u64,
}

/// Map `plan.mb` of fresh memory (aligned to 2 MB, so that a huge page can
/// back it), ask for or refuse huge pages, and touch it page by page at
/// the plan's rate, timing every touch: a line for each touch of 200 us or
/// more and one a second, then the `touch_done` line. With `plan.hold` the
/// memory stays mapped until `stop`, and a `release` line follows. `stop`
/// also ends the touch itself early. `before_touch` runs inside each timed
/// touch (a test's delay).
fn touch(
    plan: TouchPlan,
    out: &mut dyn Write,
    stop: &AtomicBool,
    mut before_touch: impl FnMut(usize),
) -> Result<TouchDone> {
    const HUGE: usize = 2 << 20;
    let len = (plan.mb * MB) as usize;
    let Some(mapping) = Mapping::new(len + HUGE) else {
        bail!("{} MB could not be mapped", plan.mb);
    };
    let skip = mapping.ptr.align_offset(HUGE) / PAGE;
    if let Some(thp) = plan.thp {
        let advice = match thp {
            Thp::Always => libc::MADV_HUGEPAGE,
            Thp::Never => libc::MADV_NOHUGEPAGE,
        };
        // SAFETY: the range is inside the mapping.
        if unsafe { libc::madvise(mapping.ptr.add(skip * PAGE).cast(), len, advice) } != 0 {
            bail!("madvise: {}", std::io::Error::last_os_error());
        }
    }
    let pages = len / PAGE;
    let rate_pages = (plan.rate_mb_s.max(0.001) * (MB as f64) / PAGE as f64).max(1.0);
    let began_ns = stall_journal::monotonic_ns();
    let mut second = TouchSecond {
        kind: "sec",
        t_us: 0,
        pages: 0,
        slow: 0,
        max_us: 0,
        sum_us: 0,
    };
    let mut second_due_ns = began_ns + 1_000_000_000;
    let (mut slow, mut max_us) = (0_u64, 0_u64);
    let mut touched = 0_usize;
    for page in 0..pages {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let due_ns = began_ns + (page as f64 / rate_pages * 1e9) as u64;
        if due_ns > stall_journal::monotonic_ns() {
            sleep_until_unless(stop, due_ns);
            if stop.load(Ordering::Relaxed) {
                break;
            }
        }
        let began = stall_journal::monotonic_ns();
        before_touch(page);
        // SAFETY: `skip + page` is inside the mapping, which holds `len`
        // bytes past the aligned start.
        unsafe { std::ptr::write_volatile(mapping.ptr.add((skip + page) * PAGE), 1) };
        let now = stall_journal::monotonic_ns();
        let us = now.saturating_sub(began) / 1_000;
        touched = page + 1;
        second.pages += 1;
        second.sum_us += us;
        second.max_us = second.max_us.max(us);
        max_us = max_us.max(us);
        if us >= SLOW_TOUCH_US {
            slow += 1;
            second.slow += 1;
            serde_json::to_writer(
                &mut *out,
                &SlowTouch {
                    kind: "touch",
                    t_us: stall_journal::realtime_us(),
                    page,
                    dur_us: us,
                },
            )?;
            out.write_all(b"\n")?;
        }
        if now >= second_due_ns || page + 1 == pages {
            second.t_us = stall_journal::realtime_us();
            serde_json::to_writer(&mut *out, &second)?;
            out.write_all(b"\n")?;
            out.flush()?;
            second = TouchSecond {
                pages: 0,
                slow: 0,
                max_us: 0,
                sum_us: 0,
                ..second
            };
            second_due_ns = now + 1_000_000_000;
        }
    }
    if second.pages > 0 {
        // A touch that a signal ended keeps its last second's line.
        second.t_us = stall_journal::realtime_us();
        serde_json::to_writer(&mut *out, &second)?;
        out.write_all(b"\n")?;
    }
    let stopped = touched < pages;
    let done = TouchDone {
        kind: "touch_done",
        mb: plan.mb,
        rate_mb_s: plan.rate_mb_s,
        thp: plan.thp.map_or("system", Thp::as_str),
        pages: touched as u64,
        stopped,
        seconds: round(
            stall_journal::monotonic_ns().saturating_sub(began_ns) as f64 / 1e9,
            3,
        ),
        slow,
        max_us,
        anon_huge_kb: anon_huge_kb(),
        hold: plan.hold,
    };
    serde_json::to_writer(&mut *out, &done)?;
    out.write_all(b"\n")?;
    out.flush()?;
    if plan.hold && !stopped {
        let held_from = Instant::now();
        while !stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(50));
        }
        serde_json::to_writer(
            &mut *out,
            &TouchRelease {
                kind: "release",
                t_us: stall_journal::realtime_us(),
                held_s: round(held_from.elapsed().as_secs_f64(), 3),
            },
        )?;
        out.write_all(b"\n")?;
        out.flush()?;
    }
    drop(mapping);
    Ok(done)
}

/// Sleep until CLOCK_MONOTONIC `due_ns`, or less once `stop` is set.
fn sleep_until_unless(stop: &AtomicBool, due_ns: u64) {
    const STEP_NS: u64 = 50_000_000;
    loop {
        let now = stall_journal::monotonic_ns();
        if now >= due_ns || stop.load(Ordering::Relaxed) {
            return;
        }
        stall_journal::sleep_until(due_ns.min(now + STEP_NS));
    }
}

/// `AnonHugePages` of `/proc/self/smaps_rollup`, in kB.
fn anon_huge_kb() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/smaps_rollup").ok()?;
    text.lines().find_map(|line| {
        let rest = line.strip_prefix("AnonHugePages:")?;
        rest.trim().trim_end_matches("kB").trim().parse().ok()
    })
}

// ---------------------------------------------------------------- echo

/// Ask the kernel to stamp every datagram the socket receives.
fn stamp_receives(socket: &UdpSocket) -> Result<()> {
    let on: libc::c_int = 1;
    // SAFETY: SO_TIMESTAMPNS takes an int.
    let set = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            (&on as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if set != 0 {
        bail!("SO_TIMESTAMPNS: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// One datagram, with its sender and the kernel's receive stamp (Unix us).
fn recv_stamped(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> std::io::Result<(usize, Option<SocketAddr>, Option<u64>)> {
    // SAFETY: zeroed sockaddr_storage, iovec, msghdr and a control buffer
    // are valid; recvmsg writes within the lengths given.
    unsafe {
        let mut name: libc::sockaddr_storage = std::mem::zeroed();
        let mut control = [0_u64; 16];
        let mut iov = libc::iovec {
            iov_base: buffer.as_mut_ptr().cast(),
            iov_len: buffer.len(),
        };
        let mut message: libc::msghdr = std::mem::zeroed();
        message.msg_name = (&mut name as *mut libc::sockaddr_storage).cast();
        message.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        message.msg_iov = &mut iov;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = std::mem::size_of_val(&control) as _;
        let read = libc::recvmsg(socket.as_raw_fd(), &mut message, 0);
        if read < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut stamp = None;
        let mut header = libc::CMSG_FIRSTHDR(&message);
        while !header.is_null() {
            if (*header).cmsg_level == libc::SOL_SOCKET
                && (*header).cmsg_type == libc::SCM_TIMESTAMPNS
            {
                let time: libc::timespec = std::ptr::read_unaligned(libc::CMSG_DATA(header).cast());
                stamp = Some(time.tv_sec as u64 * 1_000_000 + time.tv_nsec as u64 / 1_000);
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
        Ok((read as usize, socket_addr(&name), stamp))
    }
}

fn socket_addr(name: &libc::sockaddr_storage) -> Option<SocketAddr> {
    match libc::c_int::from(name.ss_family) {
        libc::AF_INET => {
            // SAFETY: the family says the storage holds a sockaddr_in.
            let v4 =
                unsafe { &*(name as *const libc::sockaddr_storage).cast::<libc::sockaddr_in>() };
            Some(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(u32::from_be(v4.sin_addr.s_addr)),
                u16::from_be(v4.sin_port),
            )))
        }
        libc::AF_INET6 => {
            // SAFETY: the family says the storage holds a sockaddr_in6.
            let v6 =
                unsafe { &*(name as *const libc::sockaddr_storage).cast::<libc::sockaddr_in6>() };
            Some(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(v6.sin6_addr.s6_addr),
                u16::from_be(v6.sin6_port),
                v6.sin6_flowinfo,
                v6.sin6_scope_id,
            )))
        }
        _ => None,
    }
}

/// Echo every datagram that starts with the magic, its server field set to
/// the kernel's receive stamp, until `stop`.
fn echo_serve(socket: &UdpSocket, stop: &AtomicBool) -> Result<()> {
    stamp_receives(socket)?;
    socket.set_read_timeout(Some(Duration::from_millis(100)))?;
    let mut buffer = vec![0_u8; 65_536];
    while !stop.load(Ordering::Relaxed) {
        let (read, peer, stamp) = match recv_stamped(socket, &mut buffer) {
            Ok(received) => received,
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                ) =>
            {
                continue;
            }
            Err(err) => return Err(err.into()),
        };
        let (Some(peer), true) = (peer, read >= ECHO_HEADER && buffer.starts_with(ECHO_MAGIC))
        else {
            continue;
        };
        let stamp = stamp.unwrap_or_else(stall_journal::realtime_us);
        buffer[20..28].copy_from_slice(&stamp.to_be_bytes());
        let _ = socket.send_to(&buffer[..read], peer);
    }
    Ok(())
}

/// One datagram that came back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Echo {
    /// The sender's clock just before the send.
    send_us: u64,
    /// The server's kernel receive stamp (the server's clock).
    server_us: u64,
    /// The sender's kernel receive stamp of the echo.
    back_us: u64,
}

/// A run of datagrams the server's kernel received late, with the losses
/// at its two ends.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
struct HeldRun {
    kind: &'static str,
    first_seq: u64,
    last_seq: u64,
    /// The first held datagram's send time.
    start_unix_us: u64,
    /// Datagrams of the run that came back.
    held: u64,
    lost_within: u64,
    /// Lost just before the first held datagram, and just after the last.
    lost_before: u64,
    lost_after: u64,
    /// The longest wait above the median one-way delay.
    held_ms: f64,
    /// The server's stamps of the run: near 0 when it was released at once.
    release_spread_us: u64,
}

/// Lost datagrams with no held run beside them.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
struct LossRun {
    kind: &'static str,
    first_seq: u64,
    last_seq: u64,
    n: u64,
}

/// The last line of `echo-send`.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct EchoSummary {
    kind: &'static str,
    target: String,
    pps: u64,
    bytes: usize,
    seconds: u64,
    sent: u64,
    received: u64,
    lost: u64,
    held_runs: u64,
    /// One-way delay above its median (one clock offset cancels).
    excess_p99_us: i64,
    excess_max_us: i64,
    /// Round trip on the sender's kernel clock.
    rtt_p50_us: u64,
    rtt_max_us: u64,
}

#[derive(Debug, Clone, PartialEq, Default)]
struct EchoAnalysis {
    held: Vec<HeldRun>,
    losses: Vec<LossRun>,
    received: u64,
    excess_p99_us: i64,
    excess_max_us: i64,
    rtt_p50_us: u64,
    rtt_max_us: u64,
}

fn percentile<T: Copy + Ord>(sorted: &[T], permille: usize) -> Option<T> {
    (!sorted.is_empty()).then(|| sorted[(sorted.len() - 1) * permille / 1_000])
}

/// Find the held runs and the losses in what came back, indexed by
/// sequence (`None`: lost), of datagrams sent `spacing_us` apart.
///
/// A held run starts at a datagram whose one-way delay is 2 ms or more
/// above the median. It takes each next datagram that came back while that
/// one is held too, or was stamped within half a spacing of the one before
/// it (released with it: the last datagrams before a release waited less
/// than 2 ms), with the lost ones among them. The losses directly before
/// and after it are its ends: which of them the node dropped says whether
/// it kept the first or the last datagrams of the hold. Other losses are
/// runs of their own.
fn analyze_echoes(echoes: &[Option<Echo>], spacing_us: u64) -> EchoAnalysis {
    let one_way = |echo: &Echo| echo.server_us as i64 - echo.send_us as i64;
    let mut delays: Vec<i64> = echoes.iter().flatten().map(one_way).collect();
    delays.sort_unstable();
    let mut rtts: Vec<u64> = echoes
        .iter()
        .flatten()
        .map(|echo| echo.back_us.saturating_sub(echo.send_us))
        .collect();
    rtts.sort_unstable();
    let median = percentile(&delays, 500).unwrap_or(0);
    let excess: Vec<i64> = delays.iter().map(|delay| delay - median).collect();
    let mut analysis = EchoAnalysis {
        received: delays.len() as u64,
        excess_p99_us: percentile(&excess, 990).unwrap_or(0),
        excess_max_us: excess.last().copied().unwrap_or(0),
        rtt_p50_us: percentile(&rtts, 500).unwrap_or(0),
        rtt_max_us: rtts.last().copied().unwrap_or(0),
        ..EchoAnalysis::default()
    };
    let held = |echo: &Echo| one_way(echo) - median >= HELD_US;
    let together_us = spacing_us / 2;
    // The sequences that came back, in order.
    let received: Vec<(usize, Echo)> = echoes
        .iter()
        .enumerate()
        .filter_map(|(seq, echo)| echo.map(|echo| (seq, echo)))
        .collect();
    // The sequences each held run accounts for, its ends' losses included.
    let mut booked: Vec<std::ops::Range<usize>> = Vec::new();
    let mut index = 0;
    while index < received.len() {
        if !held(&received[index].1) {
            index += 1;
            continue;
        }
        let mut end = index;
        while let Some(&(_, next)) = received.get(end + 1) {
            let previous = received[end].1;
            if held(&next) || next.server_us.saturating_sub(previous.server_us) < together_us {
                end += 1;
            } else {
                break;
            }
        }
        let members = &received[index..=end];
        let (first, last) = (members[0].0, members[members.len() - 1].0);
        let from = index
            .checked_sub(1)
            .map_or(0, |before| received[before].0 + 1);
        let to = received.get(end + 1).map_or(echoes.len(), |&(seq, _)| seq);
        let stamps = members.iter().map(|(_, echo)| echo.server_us);
        let spread = stamps.clone().max().unwrap_or(0) - stamps.min().unwrap_or(0);
        let longest = members
            .iter()
            .map(|(_, echo)| one_way(echo) - median)
            .max()
            .unwrap_or(0);
        analysis.held.push(HeldRun {
            kind: "held",
            first_seq: first as u64,
            last_seq: last as u64,
            start_unix_us: members[0].1.send_us,
            held: members.len() as u64,
            lost_within: (last + 1 - first - members.len()) as u64,
            lost_before: (first - from) as u64,
            lost_after: (to - last - 1) as u64,
            held_ms: round(longest as f64 / 1_000.0, 3),
            release_spread_us: spread,
        });
        booked.push(from..to);
        index = end + 1;
    }
    let mut seq = 0;
    while seq < echoes.len() {
        if echoes[seq].is_some() {
            seq += 1;
            continue;
        }
        let start = seq;
        while seq < echoes.len() && echoes[seq].is_none() {
            seq += 1;
        }
        if !booked.iter().any(|range| range.contains(&start)) {
            analysis.losses.push(LossRun {
                kind: "loss",
                first_seq: start as u64,
                last_seq: seq as u64 - 1,
                n: (seq - start) as u64,
            });
        }
    }
    analysis
}

/// Send `seconds` of numbered datagrams at `pps` to the server, receive
/// the echoes for a second more, and write the held runs, the losses and
/// a summary.
fn echo_send(args: &EchoSendArgs, out: &mut dyn Write) -> Result<EchoSummary> {
    let bind: SocketAddr = match args.target.ip() {
        IpAddr::V4(_) => (Ipv4Addr::UNSPECIFIED, 0).into(),
        IpAddr::V6(_) => (Ipv6Addr::UNSPECIFIED, 0).into(),
    };
    let socket = UdpSocket::bind(bind)?;
    socket.connect(args.target)?;
    stamp_receives(&socket)?;
    socket.set_read_timeout(Some(Duration::from_millis(100)))?;
    let total = (args.pps.max(1) * args.seconds) as usize;
    let bytes = args.bytes.max(ECHO_HEADER);
    let interval_ns = 1_000_000_000 / args.pps.max(1);
    let done = AtomicBool::new(false);
    let echoes = std::thread::scope(|scope| -> Result<Vec<Option<Echo>>> {
        let receiver = scope.spawn(|| {
            let mut echoes: Vec<Option<Echo>> = vec![None; total];
            let mut buffer = vec![0_u8; 65_536];
            let mut quiet_since: Option<Instant> = None;
            loop {
                if done.load(Ordering::Relaxed) {
                    // A second for the last echoes.
                    let since = *quiet_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= Duration::from_secs(1) {
                        break;
                    }
                }
                let Ok((read, _, stamp)) = recv_stamped(&socket, &mut buffer) else {
                    continue;
                };
                if read < ECHO_HEADER || !buffer.starts_with(ECHO_MAGIC) {
                    continue;
                }
                let field = |at: usize| u64::from_be_bytes(buffer[at..at + 8].try_into().unwrap());
                let seq = field(4) as usize;
                if let Some(slot) = echoes.get_mut(seq) {
                    slot.get_or_insert(Echo {
                        send_us: field(12),
                        server_us: field(20),
                        back_us: stamp.unwrap_or_else(stall_journal::realtime_us),
                    });
                }
            }
            echoes
        });
        let mut datagram = vec![0_u8; bytes];
        datagram[..4].copy_from_slice(ECHO_MAGIC);
        let began_ns = stall_journal::monotonic_ns();
        for seq in 0..total as u64 {
            stall_journal::sleep_until(began_ns + seq * interval_ns);
            datagram[4..12].copy_from_slice(&seq.to_be_bytes());
            datagram[12..20].copy_from_slice(&stall_journal::realtime_us().to_be_bytes());
            // A refused send is a lost datagram.
            let _ = socket.send(&datagram);
        }
        done.store(true, Ordering::Relaxed);
        Ok(receiver.join().expect("the receiver does not panic"))
    })?;
    let analysis = analyze_echoes(&echoes, interval_ns / 1_000);
    for run in &analysis.held {
        serde_json::to_writer(&mut *out, run)?;
        out.write_all(b"\n")?;
    }
    for run in &analysis.losses {
        serde_json::to_writer(&mut *out, run)?;
        out.write_all(b"\n")?;
    }
    let summary = EchoSummary {
        kind: "echo",
        target: args.target.to_string(),
        pps: args.pps,
        bytes,
        seconds: args.seconds,
        sent: total as u64,
        received: analysis.received,
        lost: total as u64 - analysis.received,
        held_runs: analysis.held.len() as u64,
        excess_p99_us: analysis.excess_p99_us,
        excess_max_us: analysis.excess_max_us,
        rtt_p50_us: analysis.rtt_p50_us,
        rtt_max_us: analysis.rtt_max_us,
    };
    serde_json::to_writer(&mut *out, &summary)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(summary)
}

// ---------------------------------------------------------------- boot

/// What the check after the warm pass saw.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
struct CheckReport {
    seconds: u64,
    cpus: u64,
    /// Stopped ticks of 20 ms or more.
    stops_over_20ms: u64,
    stop_max_ms: u64,
    /// Summed lateness of stopped ticks of 20 ms or more, averaged over
    /// the CPUs (the journal's `stopped_ms`).
    stopped_ms: u64,
    /// Steal as a share of the CPUs' time over the check.
    steal_permille: u64,
    late_max_us: u64,
    /// The longest stopped tick, for the verdict (not written).
    #[serde(skip)]
    stop_max_us: u64,
}

/// The boot check's record (`/run/needletail/node-warm.json`).
#[derive(Debug, Clone, PartialEq, Serialize)]
struct BootRecord {
    schema: &'static str,
    complete: bool,
    verdict: &'static str,
    unix_us: u64,
    /// The warm pass judged the host slow (`slow_reasons`): not a verdict
    /// (a slow host that finished its pass is ready), a matter for the
    /// deploy, which replaces such an edge once (integration 4).
    slow_host: bool,
    slow_reasons: Vec<&'static str>,
    warm: WarmReport,
    check: CheckReport,
}

/// Steal and all time of the CPUs of `cpus` from `/proc/stat`, in ticks.
fn steal_and_total(stat: &str, cpus: &[u16]) -> (u64, u64) {
    let (mut steal, mut total) = (0, 0);
    for line in stat.lines() {
        let Some(rest) = line.strip_prefix("cpu") else {
            break;
        };
        let mut fields = rest.split_ascii_whitespace();
        let Some(Ok(cpu)) = fields.next().map(str::parse::<u16>) else {
            continue;
        };
        if !cpus.contains(&cpu) {
            continue;
        }
        // user nice system idle iowait irq softirq steal (guest time is
        // inside user already)
        let values: Vec<u64> = fields
            .take(8)
            .filter_map(|field| field.parse().ok())
            .collect();
        total += values.iter().sum::<u64>();
        steal += values.get(7).copied().unwrap_or(0);
    }
    (steal, total)
}

/// Watch for `seconds` with the fixed work loop running and say what was
/// seen. `started` sees the tick threads once they run.
fn check(seconds: u64, started: impl FnOnce(&TickThreads)) -> CheckReport {
    if seconds == 0 {
        return CheckReport::default();
    }
    let journal = Arc::new(StallJournal::new(host_name(), WATCH_CAPACITY));
    let ticks = stall_journal::spawn_ticks(
        &journal,
        TickConfig {
            detail: true,
            ..TickConfig::every(stall_journal::DEFAULT_TICK_US)
        },
    );
    let cpus: Vec<u16> = ticks.threads().iter().map(|thread| thread.cpu).collect();
    let stat = || std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let (steal_before, total_before) = steal_and_total(&stat(), &cpus);
    started(&ticks);
    std::thread::sleep(Duration::from_secs(seconds));
    ticks.stop();
    let (steal_after, total_after) = steal_and_total(&stat(), &cpus);
    check_report(
        seconds,
        &journal,
        &journal.query(None),
        steal_after.saturating_sub(steal_before),
        total_after.saturating_sub(total_before),
    )
}

fn check_report(
    seconds: u64,
    journal: &StallJournal,
    report: &StallReport,
    steal: u64,
    total: u64,
) -> CheckReport {
    let stops: Vec<u64> = report
        .events
        .iter()
        .filter(|line| line.event.kind == StallKind::Tick && line.cause == Cause::Stopped)
        .map(|line| line.event.dur_us)
        .collect();
    let snapshot = journal.snapshot();
    let stop_max_us = stops.iter().copied().max().unwrap_or(0);
    CheckReport {
        seconds,
        cpus: snapshot.tick_threads,
        stops_over_20ms: stops.iter().filter(|&&us| us >= 20_000).count() as u64,
        stop_max_ms: stop_max_us / 1_000,
        stopped_ms: snapshot.stopped_ms,
        steal_permille: (steal * 1_000).checked_div(total).unwrap_or(0),
        late_max_us: snapshot.tick_late_max_us,
        stop_max_us,
    }
}

/// `ok`, or the first reason the node is not to be offered listeners.
fn verdict(warm: &WarmReport, check: &CheckReport, args: &BootArgs) -> &'static str {
    if warm.ran && !warm.finished {
        "warm-timeout"
    } else if check.stop_max_us > args.max_stop_ms * 1_000 {
        "stops"
    } else if check.steal_permille > args.max_steal_permille {
        "steal"
    } else {
        "ok"
    }
}

fn boot(args: &BootArgs, started: impl FnOnce(&TickThreads)) -> BootRecord {
    let warm = if args.warm {
        warm_node(args.warm_args())
    } else {
        WarmReport::default()
    };
    let check = check(args.check_seconds, started);
    let verdict = verdict(&warm, &check, args);
    let slow_reasons = slow_reasons(&warm, args.slow_write_ms);
    BootRecord {
        schema: BOOT_SCHEMA,
        complete: verdict == "ok",
        verdict,
        unix_us: stall_journal::realtime_us(),
        slow_host: !slow_reasons.is_empty(),
        slow_reasons,
        warm,
        check,
    }
}

/// Write `body` to `path` whole: a reader never sees half of it.
fn write_whole(path: &Path, body: &[u8]) -> std::io::Result<()> {
    let mut partial = path.as_os_str().to_owned();
    partial.push(".partial");
    std::fs::write(&partial, body)?;
    std::fs::rename(&partial, path)
}

/// `boot`: run the check, write and print its record. The exit status is
/// 0 whatever happened: the service starts, and its readiness reads the
/// record (or its absence) and keeps the node out.
fn boot_command(args: &BootArgs, started: impl FnOnce(&TickThreads)) -> ExitCode {
    let record = boot(args, started);
    match serde_json::to_string(&record) {
        Ok(body) => {
            if let Err(err) = write_whole(&args.out, format!("{body}\n").as_bytes()) {
                eprintln!(
                    "needletail-nodecheck: the record could not be written to {}: {err}",
                    args.out.display()
                );
            }
            println!("{body}");
        }
        Err(err) => eprintln!("needletail-nodecheck: {err}"),
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    /// C CR3: every binary of the crate names the build it came from
    /// (`--version`).
    #[test]
    fn the_version_names_the_build() {
        use clap::CommandFactory;
        assert_eq!(
            Cli::command().get_version(),
            Some(av_mesh::build::VERSION)
        );
    }

    const HOLD_MS: u64 = 300;

    extern "C" fn hold(_signal: libc::c_int) {
        let held = libc::timespec {
            tv_sec: 0,
            tv_nsec: (HOLD_MS * 1_000_000) as libc::c_long,
        };
        // SAFETY: nanosleep is async-signal-safe.
        unsafe { libc::nanosleep(&held, std::ptr::null_mut()) };
    }

    /// Keep one thread from running for `HOLD_MS`, as a stopped CPU would:
    /// it sleeps inside a signal handler, on no run queue.
    fn hold_thread(tid: i32) {
        // SAFETY: the handler only sleeps; the signal goes to one thread.
        unsafe {
            libc::signal(libc::SIGUSR1, hold as *const () as libc::sighandler_t);
            libc::syscall(libc::SYS_tgkill, libc::getpid(), tid, libc::SIGUSR1);
        }
    }

    /// Allow the calling thread the first `count` of its CPUs, so that the
    /// tick threads it starts are that many.
    fn keep_cpus(count: usize) -> Vec<u16> {
        let cpus: Vec<u16> = stall_journal::allowed_cpus()
            .into_iter()
            .take(count)
            .collect();
        // SAFETY: a zeroed cpu_set_t is empty; CPU_SET writes inside it.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            for &cpu in &cpus {
                libc::CPU_SET(usize::from(cpu), &mut set);
            }
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        }
        cpus
    }

    fn json_lines(body: &[u8]) -> Vec<serde_json::Value> {
        std::str::from_utf8(body)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).expect("one JSON object a line"))
            .collect()
    }

    /// A field of `/proc/self/status`, in kB.
    fn status_kb(field: &str) -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix(field)?.strip_prefix(':'))
            .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
            .unwrap()
    }

    #[test]
    fn the_command_line_is_the_specification_s() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
        let parse = |line: &str| Cli::try_parse_from(line.split(' ')).unwrap().command;
        let Command::Watch(watch) = parse("nodecheck watch --out w.ndjson") else {
            panic!("watch");
        };
        assert_eq!((watch.tick_us, watch.threshold_us), (1_000, 2_000));
        let Command::Warm(warm) = parse("nodecheck warm") else {
            panic!("warm");
        };
        assert_eq!(
            (warm.leave_mb, warm.chunk_mb, warm.max_seconds),
            (300, 64, None)
        );
        let Command::Touch(touch) =
            parse("nodecheck touch --mb 400 --rate-mb-s 2 --thp always --out t.ndjson")
        else {
            panic!("touch");
        };
        assert_eq!(
            (touch.mb, touch.rate_mb_s, touch.thp),
            (400, 2.0, Some(Thp::Always))
        );
        assert!(matches!(
            parse("nodecheck echo-serve --bind 0.0.0.0:7400"),
            Command::EchoServe(_)
        ));
        let Command::EchoSend(send) =
            parse("nodecheck echo-send --target 10.0.0.2:7400 --seconds 60 --out e.ndjson")
        else {
            panic!("echo-send");
        };
        assert_eq!((send.pps, send.bytes, send.seconds), (2_000, 150, 60));
        let Command::Boot(boot) = parse("nodecheck boot --out /run/needletail/node-warm.json")
        else {
            panic!("boot");
        };
        assert!(!boot.warm);
        assert_eq!(
            (
                boot.check_seconds,
                boot.max_stop_ms,
                boot.max_steal_permille
            ),
            (0, 20, 10)
        );
        let Command::Boot(boot) = parse("nodecheck boot --out o --warm --check-seconds 60") else {
            panic!("boot");
        };
        assert!(boot.warm && boot.check_seconds == 60);
    }

    #[test]
    fn warm_touches_what_it_reports_and_stops_at_its_limit() {
        // Sized from what the kernel says, less the reserve.
        let meminfo =
            "MemTotal:        4005060 kB\nMemFree:  3700000 kB\nMemAvailable:    3866624 kB\n";
        let available = mem_available_bytes(meminfo).unwrap();
        assert_eq!(available, 3_866_624 * 1_024);
        assert_eq!(warm_bytes(available, 384), available - 384 * MB);
        assert_eq!(warm_bytes(200 * MB, 384), 0, "nothing to spare");
        assert_eq!(mem_available_bytes("MemTotal: 1 kB\n"), None);
        let cpus: Vec<u16> = stall_journal::allowed_cpus().into_iter().take(2).collect();

        // 40 MB in chunks of 8: every page touched, and all of it given
        // back. The process's peak resident size, reset first, shows what
        // was touched (faults do not: with transparent huge pages one fault
        // maps 2 MB).
        std::fs::write("/proc/self/clear_refs", "5").unwrap();
        let resident_before = status_kb("VmRSS");
        let report = warm(40 * MB, 8 * MB, &cpus, Duration::from_secs(60), 100);
        let peak_kb = status_kb("VmHWM");
        assert!(report.ran && report.finished, "{report:?}");
        assert_eq!(report.gib, round(40.0 * MB as f64 / GIB, 3));
        // The kernel's resident counts are batched per CPU, a few hundred
        // kB off: nine tenths of the 40 MB is the bar.
        assert!(
            peak_kb >= resident_before + 36 * 1_024,
            "peak {peak_kb} kB from {resident_before} kB"
        );
        // Given back (other tests of this process hold a few MB at most).
        assert!(status_kb("VmRSS") < resident_before + 16 * 1_024);
        assert!(report.worst_page_us > 0 || report.seconds > 0.0);
        assert!(report.pages_over_100ms <= report.pages_over_1ms);
        // A chunk that is not whole is touched to its end, and no further.
        let odd = warm(5 * MB + 12_288, 4 * MB, &cpus, Duration::from_secs(60), 100);
        assert!(odd.finished, "{odd:?}");
        assert_eq!(odd.gib, round((5 * MB + 12_288) as f64 / GIB, 3));

        // A pass with no time left stops at its limit and says so.
        let stopped = warm(4_096 * MB, 64 * MB, &cpus, Duration::from_millis(20), 100);
        assert!(stopped.ran && !stopped.finished, "{stopped:?}");
        assert!(stopped.gib < 4.0, "{stopped:?}");
        assert!(stopped.seconds < 5.0, "{stopped:?}");
        // As the boot check reads it.
        let args = BootArgs {
            out: PathBuf::new(),
            warm: true,
            check_seconds: 0,
            max_stop_ms: 20,
            max_steal_permille: 10,
            warm_leave_mb: DEFAULT_LEAVE_MB,
            warm_max_seconds: None,
            warm_max_seconds_per_4gb: 300,
            slow_write_ms: 1_000,
            slow_seconds_per_4gb: 180,
        };
        assert_eq!(
            verdict(&stopped, &CheckReport::default(), &args),
            "warm-timeout"
        );
        assert_eq!(verdict(&report, &CheckReport::default(), &args), "ok");
        // slow_host (integration 4): a pass out of time is one.
        assert_eq!(slow_reasons(&stopped, 1_000), ["warm-timeout"]);
    }

    /// Integration 4: the warm pass touches what the referee's
    /// warm-guest.py does or more (MemAvailable less 300 MB, anonymous
    /// 64 MiB mappings, every write timed, writes of 100 ms or more
    /// listed), and judges the host slow on a write of a second or a pass
    /// slower than 40 s a GiB; the time limit scales with the memory.
    #[test]
    fn a_warm_pass_lists_its_stalls_and_judges_the_host() {
        let defaults = WarmArgs::default();
        assert_eq!((defaults.leave_mb, defaults.chunk_mb, defaults.stall_ms), (300, 64, 100));
        let parse = |line: &str| Cli::try_parse_from(line.split(' ')).unwrap().command;
        let Command::Warm(parsed) = parse("nodecheck warm") else {
            panic!("warm");
        };
        assert_eq!((parsed.leave_mb, parsed.max_seconds), (300, None));
        let Command::Boot(boot) = parse(
            "nodecheck boot --out o --warm --warm-max-seconds 240 --slow-write-ms 500 --slow-seconds-per-4gb 200",
        ) else {
            panic!("boot");
        };
        let warm_args = boot.warm_args();
        assert_eq!(warm_args.max_seconds, Some(240));
        assert_eq!((warm_args.slow_write_ms, warm_args.leave_mb), (500, 300));
        assert_eq!((warm_args.slow_seconds_per_4gb, warm_args.max_seconds_per_4gb), (200, 300));
        // 300 s per 4 GiB of memory, at least 300: a 4 GB guest (3.8 GiB
        // MemTotal) 300 s, a 16 GB one 1,200 s; the slow bound 180 s per
        // 4 GiB.
        assert_eq!(per_4gb_seconds((3.8 * GIB) as u64, 300), 300);
        assert_eq!(per_4gb_seconds(16 * GIB as u64, 300), 1_200);
        assert_eq!(per_4gb_seconds(8 * GIB as u64, 180), 360);
        assert_eq!(
            mem_total_bytes("MemTotal:        4005060 kB\nMemAvailable: 1 kB\n"),
            Some(4_005_060 * 1_024)
        );

        // Every write of `stall_ms` or more is listed with where it was.
        let mut times = PageTimes::default();
        times.add(50, 0, 100_000);
        times.add(150_000, 70 * MB, 100_000);
        times.add(1_200_000, 130 * MB, 100_000);
        assert_eq!(times.stalls.len(), 2);
        assert_eq!((times.stalls[0].ms, times.stalls[0].after_mb), (150.0, 70));
        assert_eq!(times.stalls[1].ms, 1_200.0);
        let mut more = PageTimes::default();
        for _ in 0..MAX_STALLS {
            more.add(100_000, 0, 100_000);
        }
        times.merge(&mut more);
        assert_eq!((times.stalls.len(), times.stalls_dropped), (MAX_STALLS, 2));

        // A real pass with a stall bar of 0 ms lists every write, capped.
        let cpus: Vec<u16> = stall_journal::allowed_cpus().into_iter().take(1).collect();
        let pass = warm(4 * MB, 4 * MB, &cpus, Duration::from_secs(60), 0);
        assert!(pass.finished, "{pass:?}");
        assert_eq!(pass.touched_mb, 4);
        assert_eq!(pass.stalls.len(), MAX_STALLS);
        assert_eq!(pass.stalls_dropped, 1_024 - MAX_STALLS as u64);

        // The judgement.
        let fine = WarmReport {
            ran: true,
            finished: true,
            gib: 3.4,
            seconds: 95.0,
            worst_page_us: 900_000,
            slow_bound_seconds: 180,
            ..WarmReport::default()
        };
        assert!(slow_reasons(&fine, 1_000).is_empty(), "95 s on a 4 GB guest is not slow");
        let stalled = WarmReport {
            worst_page_us: 1_930_000,
            ..fine.clone()
        };
        assert_eq!(slow_reasons(&stalled, 1_000), ["page-stall"]);
        let slow = WarmReport {
            seconds: 241.0,
            ..fine.clone()
        };
        assert_eq!(slow_reasons(&slow, 1_000), ["slow-pass"]);
        assert!(slow_reasons(&WarmReport::default(), 1_000).is_empty(), "no pass");
        let line = serde_json::to_value(WarmLine::of(stalled, &WarmArgs::default())).unwrap();
        assert_eq!(line["slow_host"], true);
        assert_eq!(line["slow_reasons"][0], "page-stall");
        assert_eq!(line["gib"], 3.4, "the pass's fields at the top level");
    }

    #[test]
    fn watch_books_an_injected_delay_on_its_cpu_only() {
        let stop = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel();
        let watcher = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let cpus = keep_cpus(2);
                let mut out = Vec::new();
                watch(
                    TickConfig {
                        detail: true,
                        ..TickConfig::every(1_000)
                    },
                    &mut out,
                    &stop,
                    |ticks| tx.send(ticks.threads().to_vec()).unwrap(),
                )
                .unwrap();
                (cpus, out)
            })
        };
        let threads = rx.recv().unwrap();
        if threads.len() < 2 {
            stop.store(true, Ordering::Relaxed);
            watcher.join().unwrap();
            return;
        }
        let held = threads[1];
        // On a busy machine a held thread may also wait for its CPU once it
        // wakes, which is rightly `starved`: the hold is given three times.
        for _ in 0..3 {
            std::thread::sleep(Duration::from_millis(200));
            hold_thread(held.tid);
            std::thread::sleep(Duration::from_millis(HOLD_MS + 100));
        }
        std::thread::sleep(Duration::from_millis(1_100));
        stop.store(true, Ordering::Relaxed);
        let (cpus, out) = watcher.join().unwrap();
        let lines = json_lines(&out);
        let long: Vec<&serde_json::Value> = lines
            .iter()
            .filter(|line| line["kind"] == "tick")
            .filter(|line| line["dur_us"].as_u64().unwrap() >= HOLD_MS * 1_000 - 5_000)
            .collect();
        // Every long tick is the held thread's, and on its CPU's journal.
        assert!(!long.is_empty(), "no long tick in {lines:?}");
        for line in &long {
            assert_eq!(line["thread"], format!("tick-{}", held.cpu), "{line}");
        }
        assert!(
            long.iter()
                .any(|line| line["cause"] == "stopped" && line["scope"] == "one"),
            "{long:?}"
        );
        // Every CPU has its seconds, with the work loop timed; the first
        // CPU's lines carry the memory manager's counters.
        for &cpu in &cpus {
            let secs: Vec<&serde_json::Value> = lines
                .iter()
                .filter(|line| line["kind"] == "sec" && line["cpu"] == cpu)
                .collect();
            assert!(secs.len() >= 2, "{} seconds of CPU {cpu}", secs.len());
            for sec in secs {
                assert!(sec["work_us_max"].is_u64(), "{sec}");
                assert!(sec["steal"].is_u64() && sec["system"].is_u64(), "{sec}");
                assert_eq!(sec["pgfault"].is_u64(), cpu == cpus[0], "{sec}");
            }
        }
        // Oldest first, and a node name on every line.
        let times: Vec<u64> = lines
            .iter()
            .map(|line| line["t_us"].as_u64().unwrap())
            .collect();
        assert!(times.windows(2).all(|pair| pair[0] <= pair[1]));
        assert!(lines.iter().all(|line| line["node"].is_string()));
    }

    #[test]
    fn touch_counts_a_slow_page() {
        let mut out = Vec::new();
        // 4 MB at 64 MB/s: about 62 ms, with page 100 held for 2 ms.
        let done = touch(
            TouchPlan {
                mb: 4,
                rate_mb_s: 64.0,
                thp: Some(Thp::Never),
                hold: false,
            },
            &mut out,
            &AtomicBool::new(false),
            |page| {
                if page == 100 {
                    std::thread::sleep(Duration::from_millis(2));
                }
            },
        )
        .unwrap();
        assert_eq!(done.pages, 1_024);
        assert!(done.slow >= 1 && done.max_us >= 2_000, "{done:?}");
        assert!(done.seconds >= 0.05, "the rate was kept: {done:?}");
        assert_eq!(done.thp, "never");
        let lines = json_lines(&out);
        let slow: Vec<&serde_json::Value> = lines
            .iter()
            .filter(|line| line["kind"] == "touch")
            .collect();
        assert!(
            slow.iter()
                .any(|line| line["page"] == 100 && line["dur_us"].as_u64().unwrap() >= 2_000),
            "{slow:?}"
        );
        assert_eq!(slow.len() as u64, done.slow);
        let secs: Vec<&serde_json::Value> =
            lines.iter().filter(|line| line["kind"] == "sec").collect();
        let counted: u64 = secs
            .iter()
            .map(|line| line["pages"].as_u64().unwrap())
            .sum();
        assert_eq!(counted, 1_024, "every page in a second's line");
        assert_eq!(lines.last().unwrap()["kind"], "touch_done");
        assert!(!done.stopped && !done.hold);
    }

    #[test]
    fn touch_holds_its_memory_until_stopped_then_releases_it() {
        // The hunt driver's call (linode-stall-hunt.py `touch_body`):
        // `touch --mb 400 --rate-mb-s 2 --hold --thp always --out F`, ended
        // by `kill` (SIGTERM). Here 2 MB at 256 MB/s, held, then stopped.
        let cli = Cli::try_parse_from([
            "needletail-nodecheck",
            "touch",
            "--mb",
            "400",
            "--rate-mb-s",
            "2",
            "--hold",
            "--thp",
            "always",
            "--out",
            "touch-fresh.ndjson",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Touch(TouchArgs {
                hold: true,
                mb: 400,
                ..
            })
        ));
        let stop = AtomicBool::new(false);
        let mut out = Vec::new();
        let done = std::thread::scope(|scope| {
            let toucher = scope.spawn(|| {
                touch(
                    TouchPlan {
                        mb: 2,
                        rate_mb_s: 256.0,
                        thp: Some(Thp::Never),
                        hold: true,
                    },
                    &mut out,
                    &stop,
                    |_| {},
                )
                .unwrap()
            });
            // Touching takes about 8 ms; the memory is then held.
            std::thread::sleep(Duration::from_millis(300));
            assert!(!toucher.is_finished(), "a held touch waits for its signal");
            stop.store(true, Ordering::Relaxed);
            toucher.join().unwrap()
        });
        assert_eq!(done.pages, 512);
        assert!(done.hold && !done.stopped, "{done:?}");
        let lines = json_lines(&out);
        let kinds: Vec<&str> = lines
            .iter()
            .map(|line| line["kind"].as_str().unwrap())
            .collect();
        assert_eq!(
            &kinds[kinds.len() - 2..],
            ["touch_done", "release"],
            "{kinds:?}"
        );
        let held = lines.last().unwrap()["held_s"].as_f64().unwrap();
        assert!((0.2..5.0).contains(&held), "held {held} s");

        // A signal during the touch ends it at once, with what it has.
        let stop = AtomicBool::new(false);
        let mut out = Vec::new();
        let done = std::thread::scope(|scope| {
            let toucher = scope.spawn(|| {
                touch(
                    TouchPlan {
                        mb: 64,
                        rate_mb_s: 16.0,
                        thp: Some(Thp::Never),
                        hold: true,
                    },
                    &mut out,
                    &stop,
                    |_| {},
                )
                .unwrap()
            });
            std::thread::sleep(Duration::from_millis(200));
            stop.store(true, Ordering::Relaxed);
            toucher.join().unwrap()
        });
        assert!(
            done.stopped && done.pages > 0 && done.pages < 16_384,
            "{done:?}"
        );
        let lines = json_lines(&out);
        assert_eq!(
            lines.last().unwrap()["kind"],
            "touch_done",
            "no hold after a stop"
        );
        let counted: u64 = lines
            .iter()
            .filter(|line| line["kind"] == "sec")
            .map(|line| line["pages"].as_u64().unwrap())
            .sum();
        assert_eq!(counted, done.pages);
    }

    #[test]
    fn echo_finds_a_held_run_and_which_end_was_lost() {
        // 1,000 datagrams 500 us apart, 100 us one way.
        let send = |seq: usize| 1_000_000 + seq as u64 * 500;
        let mut echoes: Vec<Option<Echo>> = (0..1_000)
            .map(|seq| {
                Some(Echo {
                    send_us: send(seq),
                    server_us: send(seq) + 100,
                    back_us: send(seq) + 210,
                })
            })
            .collect();
        // The node stopped: 395 to 399 lost, 400 to 459 queued and stamped
        // together at the release.
        let release = send(459) + 200;
        for seq in 395..400 {
            echoes[seq] = None;
        }
        for (seq, echo) in echoes.iter_mut().enumerate().take(460).skip(400) {
            *echo = Some(Echo {
                send_us: send(seq),
                server_us: release + (seq as u64 % 3),
                back_us: release + 150,
            });
        }
        // Another stop, whose later end was lost (the queue was full), with
        // one loss inside: released after the last lost one was sent.
        let release = send(739) + 300;
        for (seq, echo) in echoes.iter_mut().enumerate().take(730).skip(700) {
            *echo = Some(Echo {
                send_us: send(seq),
                server_us: release,
                back_us: release + 150,
            });
        }
        echoes[710] = None;
        for seq in 730..740 {
            echoes[seq] = None;
        }
        // A loss with no hold beside it.
        echoes[900] = None;
        echoes[901] = None;
        let analysis = analyze_echoes(&echoes, 500);
        assert_eq!(analysis.held.len(), 2, "{analysis:?}");
        let first = analysis.held[0];
        assert_eq!((first.first_seq, first.last_seq), (400, 459));
        assert_eq!((first.held, first.lost_within), (60, 0));
        assert_eq!((first.lost_before, first.lost_after), (5, 0));
        // 400 was sent 29.5 ms before the release.
        assert!((29.5..29.8).contains(&first.held_ms), "{first:?}");
        assert!(first.release_spread_us <= 2);
        assert_eq!(first.start_unix_us, send(400));
        let second = analysis.held[1];
        assert_eq!((second.first_seq, second.last_seq), (700, 729));
        assert_eq!((second.held, second.lost_within), (29, 1));
        assert_eq!((second.lost_before, second.lost_after), (0, 10));
        assert_eq!(
            analysis.losses,
            vec![LossRun {
                kind: "loss",
                first_seq: 900,
                last_seq: 901,
                n: 2
            }]
        );
        assert_eq!(analysis.received, 1_000 - 5 - 1 - 10 - 2);
        assert_eq!(analysis.rtt_p50_us, 210);
        // Everything lost: one loss run.
        let lost = analyze_echoes(&[None, None, None], 500);
        assert_eq!(lost.losses.len(), 1);
        assert_eq!(lost.losses[0].n, 3);

        // And the probe itself, on loopback: every datagram comes back with
        // the server's kernel stamp.
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let target = server.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let serving = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || echo_serve(&server, &stop).unwrap())
        };
        let mut out = Vec::new();
        let summary = echo_send(
            &EchoSendArgs {
                target,
                pps: 1_000,
                bytes: 150,
                seconds: 1,
                out: PathBuf::new(),
            },
            &mut out,
        )
        .unwrap();
        stop.store(true, Ordering::Relaxed);
        serving.join().unwrap();
        assert_eq!(
            (summary.sent, summary.received, summary.lost),
            (1_000, 1_000, 0)
        );
        assert!(summary.rtt_max_us < 1_000_000, "{summary:?}");
        assert_eq!(json_lines(&out).last().unwrap()["kind"], "echo");
    }

    #[test]
    fn boot_says_stops_and_exits_zero() {
        let out = std::env::temp_dir().join(format!(
            "needletail-nodecheck-boot-{}.json",
            std::process::id()
        ));
        let args = BootArgs {
            out: out.clone(),
            warm: false,
            check_seconds: 1,
            max_stop_ms: 20,
            max_steal_permille: 1_000,
            warm_leave_mb: DEFAULT_LEAVE_MB,
            warm_max_seconds: None,
            warm_max_seconds_per_4gb: 300,
            slow_write_ms: 1_000,
            slow_seconds_per_4gb: 180,
        };
        // On a busy machine a held thread may also wait for its CPU once it
        // wakes (`starved`, not a stop): the check is given three tries.
        let mut said = None;
        for _ in 0..3 {
            let record = std::thread::spawn({
                let args = args.clone();
                move || {
                    keep_cpus(2);
                    let code = boot_command(&args, |ticks| {
                        let tid = ticks.threads().last().unwrap().tid;
                        std::thread::spawn(move || {
                            std::thread::sleep(Duration::from_millis(200));
                            hold_thread(tid);
                        });
                    });
                    (code, std::fs::read(&args.out).unwrap())
                }
            })
            .join()
            .unwrap();
            assert_eq!(record.0, ExitCode::SUCCESS);
            let record: serde_json::Value = serde_json::from_slice(&record.1).unwrap();
            if record["verdict"] == "stops" {
                said = Some(record);
                break;
            }
        }
        let record = said.expect("a stopped tick of 300 ms fails the check");
        assert_eq!(record["schema"], BOOT_SCHEMA);
        assert_eq!(record["complete"], false);
        assert_eq!(record["warm"]["ran"], false);
        assert_eq!(record["slow_host"], false, "no pass, nothing judged");
        let check = &record["check"];
        assert_eq!(check["seconds"], 1);
        assert!(check["stops_over_20ms"].as_u64().unwrap() >= 1, "{check}");
        assert!(
            check["stop_max_ms"].as_u64().unwrap() >= HOLD_MS - 5,
            "{check}"
        );
        assert!(check["late_max_us"].as_u64().unwrap() >= (HOLD_MS - 5) * 1_000);
        for field in ["cpus", "stopped_ms", "steal_permille"] {
            assert!(check[field].is_u64(), "{field} in {check}");
        }
        assert!(check.get("stop_max_us").is_none());

        // With nothing to check the node is ready; a record that cannot be
        // written still exits 0.
        let quiet = BootArgs {
            out: PathBuf::from("/proc/needletail-nodecheck/none.json"),
            check_seconds: 0,
            ..args.clone()
        };
        assert_eq!(boot_command(&quiet, |_| {}), ExitCode::SUCCESS);
        let record = boot(&quiet, |_| {});
        assert_eq!((record.verdict, record.complete), ("ok", true));
        // Steal over its limit.
        let steal = CheckReport {
            steal_permille: 11,
            ..CheckReport::default()
        };
        assert_eq!(
            verdict(
                &WarmReport::default(),
                &steal,
                &BootArgs {
                    max_steal_permille: 10,
                    ..args.clone()
                }
            ),
            "steal"
        );
        assert_eq!(
            steal_and_total("cpu  9 9 9 9 9 9 9 9 0 0\ncpu0 1 2 3 4 5 6 7 8 0 0\ncpu1 1 1 1 1 1 1 1 1 0 0\nintr 1\n", &[0]),
            (8, 36)
        );
        let _ = std::fs::remove_file(&out);
    }
}
