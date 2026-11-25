//! Thousands of simulated UDP listeners of a Needletail stream on one load
//! host: each a real client (its own socket, token and subscription, the
//! `needletail-udp-client` protocol and decode, NACK and loss accounting),
//! a few worker threads for all of them. See `av_mesh::load`.
//!
//! ```text
//! needletail-load --entry https://lon-dist-a:19444,https://lon-dist-b:19444 --tls-ca ca.crt \
//!     --stream 4001 --ramp 100@0,1000@30,5000@60 --duration 120 --sample 20 --out run1
//! ```
//!
//! Each listener runs the session and the director of `needletail-udp-client`
//! with its defaults and flags of the same names (B 2.4): two legs on two
//! edges and a spare (`--paths 2 --spare true`), its legs asked of an entry
//! point with `ASSIGN` from its own socket and HTTPS beside it.
//!
//! Writes `<out>/intervals.jsonl` (one line an interval: per edge and in
//! total, listeners active and subscribed, parts, gaps, missing, recovered,
//! duplicates, NACKs, bytes in, publication-to-arrival, time-to-first-part
//! and receive-lag percentiles, and the load host's CPU, worker busy time,
//! timer lateness, UDP receive buffer errors and socket drops, with
//! `load_host_saturated` and its reasons; B Y4: the gap between parts
//! delivered first at each listener, interruptions and the listeners
//! interrupted, listeners with missing parts, time on one path, leg
//! replacements by reason and kind, datagrams and bytes per listener and
//! per leg), `<out>/summary.json` (the same
//! over the run, each histogram with its buckets so that the summaries of
//! several load hosts merge, and the worst listeners), and for `--sample K` listeners
//! `<out>/arrivals/listener-<i>.csv` (the `needletail-udp-client --arrivals`
//! columns, in part order). The summary also goes to standard output.
//! SIGINT or SIGTERM ends the run early, with its drain and summary.

use anyhow::{bail, Context, Result};
use av_mesh::client_compact::NackTiming;
use av_mesh::client_session::SessionConfig;
use av_mesh::load::{parse_range_ms, parse_ramp, run, Churn, IntervalBuckets, LoadConfig, Waves};
use av_mesh::load_assign::AssignConfig;
use clap::Parser;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "needletail-load",
    version = av_mesh::build::VERSION,
    about = "Simulated UDP listeners of a Needletail stream, thousands per host"
)]
struct Args {
    /// The edges' client UDP ports, `host:port,...`, without --entry (the
    /// in-process tests' mode): each listener's director is answered from
    /// this list in the session's rendezvous order (legs on different edges,
    /// no spare).
    #[arg(long, value_delimiter = ',')]
    edges: Vec<String>,
    /// Entry points of the home region, `https://host:port` (comma list),
    /// as needletail-udp-client takes them: each listener asks one
    /// (`entries[hash of its session]`, the next too after --first-retry-ms)
    /// for its legs and its spare.
    #[arg(long, value_delimiter = ',')]
    entry: Vec<String>,
    /// PEM file of the nodes' certificate authority (with --entry).
    #[arg(long)]
    tls_ca: Option<PathBuf>,
    /// TLS server name of the nodes, when it is not their address.
    #[arg(long)]
    server_name: Option<String>,
    /// The caches' client UDP port, over what an answer names.
    #[arg(long)]
    udp_port: Option<u16>,
    /// HTTPS requests to the entry points in flight at most.
    #[arg(long, default_value_t = 256)]
    assign_in_flight: usize,
    /// Legs held on a stream the entry grants two for (B 2.4).
    #[arg(long, default_value_t = av_mesh::client_director::DEFAULT_PATHS)]
    paths: u8,
    /// Hold a spare and hedge to it; false: a dead leg is replaced through
    /// the entry.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    spare: bool,
    /// A leg with no TOKEN after this (or four round trips to its entry) is
    /// passed over.
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_TOKEN_MS)]
    token_ms: u64,
    /// A leg with no media this long while another delivers is hedged at
    /// the spare.
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_HEDGE_AFTER_MS)]
    hedge_after_ms: u64,
    /// The only leg with no datagram this long is hedged at the spare.
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_LONE_HEDGE_AFTER_MS)]
    lone_hedge_after_ms: u64,
    /// After MOVE rebalance: a replacement not level by then is released and
    /// the old edge asked to keep the listener.
    #[arg(long, default_value_t = av_mesh::client_director::DEFAULT_REBALANCE_GIVEUP_MS)]
    rebalance_giveup_ms: u64,
    /// After any other MOVE: a replacement steady this long takes over
    /// though it is not level.
    #[arg(long, default_value_t = av_mesh::client_director::DEFAULT_LEVEL_WAIT_MS)]
    level_wait_ms: u64,
    /// Place240 rules 3 and 4: a lone leg's late or missing parts, and
    /// its edge's distress.
    #[command(flatten)]
    quality: av_mesh::client_quality::QualityArgs,
    /// A leg's first SUBs are each sent again this soon (0: off).
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_FIRST_DOUBLE_MS)]
    first_double_ms: u64,
    /// The compact plan's residual loss target, parts per million.
    #[arg(long, default_value_t = (av_mesh::client_compact::RESIDUAL_TARGET * 1e6).round() as u64)]
    compact_residual_ppm: u64,
    /// A hole is given up after this long.
    #[arg(long, default_value_t = av_mesh::client_compact::HOLE_GIVE_UP.as_millis() as u64)]
    hole_give_up_ms: u64,
    /// The entry points' client UDP port for ASSIGN (0: HTTPS alone).
    #[arg(long, default_value_t = av_mesh::client_udp::DEFAULT_CLIENT_UDP_PORT)]
    assignment_udp_port: u16,
    /// The HTTPS hedge of an assignment request goes after this long.
    #[arg(long, default_value_t = av_mesh::client_director::DEFAULT_ASSIGNMENT_HTTPS_AFTER_MS)]
    assignment_https_after_ms: u64,
    /// The whole budget of one assignment request.
    #[arg(long, default_value_t = av_mesh::client_director::DEFAULT_ASSIGNMENT_TIMEOUT_MS)]
    assignment_timeout_ms: u64,
    /// A listener with no part for this long counts as stranded.
    #[arg(long, default_value_t = 5_000)]
    stranded_ms: u64,
    /// Session ids are `<prefix>-<listener>` (by default the label, or
    /// `load-<pid>`).
    #[arg(long)]
    sid_prefix: Option<String>,
    #[arg(long)]
    stream: u64,
    /// Listeners for the whole run (when --ramp is not given).
    #[arg(long, default_value_t = 100)]
    listeners: usize,
    /// Listener counts over time, `COUNT@SECOND,...` (for example
    /// `100@0,1000@30,5000@60`): from each second on, that many listeners.
    #[arg(long)]
    ramp: Option<String>,
    /// The listening party: this percentage of the listeners present leave
    /// in a minute (at random times, in random order) and each comes back
    /// as a new session after --churn-absence-ms (0: none).
    #[arg(long, default_value_t = 0.0)]
    churn_percent_per_min: f64,
    /// The absence of a listener that left, drawn evenly from `MIN-MAX`
    /// milliseconds.
    #[arg(long, default_value = "10000-60000")]
    churn_absence_ms: String,
    /// Joins in waves: the target is reached --wave-size listeners at a
    /// time, every --wave-every-ms (0: as --ramp and --start-rate say).
    #[arg(long, default_value_t = 0)]
    wave_size: usize,
    #[arg(long, default_value_t = 10_000)]
    wave_every_ms: u64,
    /// Seed of the churn's draws (by default from the session-id prefix,
    /// the process and the time: two hosts do not churn together).
    #[arg(long)]
    seed: Option<u64>,
    /// One listener in this many writes every part it receives first to
    /// <out>/raw/worker-<w>.csv (session id, part, arrival, bytes,
    /// publication, availability, edge); the summary states the share (0:
    /// none).
    #[arg(long, default_value_t = 0)]
    raw_sample: usize,
    /// Seconds of the run (a drain follows).
    #[arg(long, default_value_t = 30)]
    duration: u64,
    /// Worker threads (by default one per CPU).
    #[arg(long)]
    threads: Option<usize>,
    /// Listeners started a second at most.
    #[arg(long, default_value_t = 2_000.0)]
    start_rate: f64,
    /// Listeners 0..K write every part's arrival to <out>/arrivals.
    #[arg(long, default_value_t = 0)]
    sample: usize,
    /// Output directory.
    #[arg(long, alias = "out-dir")]
    out: Option<PathBuf>,
    /// A label carried into every line and the summary.
    #[arg(long)]
    label: Option<String>,
    /// Seconds of an interval line.
    #[arg(long, default_value_t = 1.0)]
    interval_seconds: f64,
    /// UDP receive buffer of each listener's socket.
    #[arg(long, default_value_t = 512 * 1024)]
    receive_buffer_bytes: usize,
    /// Local addresses for the sockets, in turn (about 28,000 listeners an
    /// address).
    #[arg(long, value_delimiter = ',', default_value = "0.0.0.0")]
    bind: Vec<IpAddr>,
    /// Take each datagram's arrival from the kernel (SO_TIMESTAMPNS): a
    /// busy load host does not add its own queueing to the latency.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    kernel_timestamps: bool,
    /// After the run, keep receiving this long (NACKs answered) before the
    /// last parts are counted.
    #[arg(long, default_value_t = 600)]
    drain_ms: u64,
    /// A part is counted missing once it is this long behind its listener's
    /// newest part (in parts of the stream's interval; just past the 3 s
    /// give-up of a hole). A listener that leaves has its parts not yet due
    /// left unjudged.
    #[arg(long, default_value_t = av_mesh::load::DEFAULT_SETTLE_MS)]
    settle_ms: u64,
    /// The same as a count of parts (1 to 4,095), over --settle-ms.
    #[arg(long)]
    settle_parts: Option<u64>,
    /// A silence between two parts delivered first this long or longer
    /// interrupts a listener (`interruptions`, `listeners_interrupted`).
    #[arg(long, default_value_t = av_mesh::load::DEFAULT_INTERRUPT_MS)]
    interrupt_ms: u64,
    /// The latency histograms of an interval line that carry their buckets
    /// (so a window's p99 merges across hosts): total, edges or off.
    #[arg(long, default_value = "total")]
    interval_buckets: IntervalBuckets,
    /// The load host counts as saturated in an interval whose timers or
    /// datagrams waited longer than this (p99) for the workers.
    #[arg(long, default_value_t = 5)]
    lag_limit_ms: u64,
    /// Read the sockets at most this often (µs), so that one `recvmmsg`
    /// takes several datagrams of a socket: less processor time a listener.
    /// The latency measured is the same with kernel timestamps; NACKs and
    /// refreshes go up to this late. 0: read at once.
    #[arg(long, default_value_t = 0)]
    coalesce_us: u64,
    /// No status line on standard error each interval.
    #[arg(long)]
    quiet: bool,
    // The client's own flags, as needletail-udp-client has them.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    fast_lane: bool,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compact: bool,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compact_burst_rows: bool,
    /// Ask for parts in the short form (wave 7 item 9, 7-byte header); an
    /// edge that refuses it is asked again with the long form.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compact_short_header: bool,
    /// The NACK timing of needletail-udp-client (wave 6): wait before the
    /// first NACK of a hole, µs; floor of the wait between NACKs, µs; NACK
    /// the parts due during a silence.
    #[arg(long, default_value_t = av_mesh::client_compact::FAST_NACK_DELAY.as_micros() as u64)]
    nack_delay_us: u64,
    #[arg(long, default_value_t = av_mesh::client_compact::FAST_NACK_RETRY.as_micros() as u64)]
    nack_retry_floor_us: u64,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    silence_nack: bool,
    /// Wave 8 (R6-3): the parts that one silence asks for, at most (0: no
    /// bound, before wave 8).
    #[arg(long, default_value_t = av_mesh::client_compact::DEFAULT_SILENCE_NACK_BUDGET)]
    silence_nack_budget: u16,
    /// Wave 8 (R6-4): a silence's NACKs one and a half round trips apart.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    silence_nack_rtt_floor: bool,
    /// Wave 8 (R6-5): gaps longer than this many intervals are no sample
    /// of the interval (0: every gap is).
    #[arg(long, default_value_t = av_mesh::client_compact::DEFAULT_INTERVAL_CAP)]
    interval_cap: u32,
    /// Wave 8 (S4): the silence margin in part intervals (1 before).
    #[arg(long, default_value_t = av_mesh::client_compact::DEFAULT_SILENCE_MARGIN_INTERVALS)]
    silence_margin_intervals: u32,
    /// Wave 8: the parts a second that the client asks for at most (0: no
    /// cap): an overloaded edge is not asked for thousands a second.
    #[arg(long, default_value_t = av_mesh::client_compact::DEFAULT_NACK_RATE_CAP)]
    nack_rate_cap: u32,
    /// Wave 8 (S2): the silence that moves again after a move for silence.
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_SILENCE_REPEAT_MS)]
    silence_repeat_ms: u64,
    /// Wave 8: ask for the compact lane again after a fall-back.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compact_retry: bool,
    /// Wave 8: drop the short-form parts that come before the edge's OK,
    /// as before (`false`: kept, read once the OK comes).
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    drop_early_parts: bool,
    /// Wave 8 local proofs: a move asks the new edge for the oldest part
    /// still missing (a hole the old edge never repaired), not only the
    /// parts after the newest (`false`: as before, those holes are lost).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    move_from_hole: bool,
    #[arg(long)]
    repair_permille: Option<u32>,
    #[arg(long)]
    min_repair: Option<u32>,
    #[arg(long)]
    fixed_repair: bool,
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_SILENCE_MS)]
    silence_ms: u64,
    #[arg(long, default_value_t = 5_000)]
    answer_ms: u64,
    /// Parts whose bytes each listener keeps for the compact lane's window
    /// repair (needletail-udp-client keeps 4,096; a window spans at most
    /// 64, and 512 keeps a listener near 200 KB).
    #[arg(long, default_value_t = 512)]
    known_parts: usize,
    /// Wave 9 B: an unanswered SUB is repeated after this (ms), or three
    /// round trips once one is measured, then twice as long each time up
    /// to 300 ms, as needletail-udp-client does (0: every 300 ms, before).
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_FIRST_RETRY_MS)]
    first_retry_ms: u64,
}

fn resolve(edge: &str) -> Result<SocketAddr> {
    edge.to_socket_addrs()
        .with_context(|| format!("edge {edge}"))?
        .next()
        .with_context(|| format!("edge {edge}: no address"))
}

fn config(args: &Args) -> Result<LoadConfig> {
    let edges = args
        .edges
        .iter()
        .filter(|edge| !edge.trim().is_empty())
        .map(|edge| resolve(edge.trim()))
        .collect::<Result<Vec<_>>>()?;
    let entries: Vec<String> = args
        .entry
        .iter()
        .map(|entry| entry.trim().trim_end_matches('/').to_owned())
        .filter(|entry| !entry.is_empty())
        .collect();
    match (edges.is_empty(), entries.is_empty()) {
        (true, true) => bail!("--edges or --entry must name the edges or an entry point"),
        (false, false) => bail!("--edges or --entry, not both"),
        _ => {}
    }
    if !(1..=2).contains(&args.paths) {
        bail!("--paths must be 1 or 2");
    }
    if args.settle_parts.is_some_and(|parts| parts == 0 || parts >= 4_096) {
        bail!("--settle-parts must be 1 to 4095");
    }
    if args.settle_ms == 0 || args.interrupt_ms == 0 {
        bail!("--settle-ms and --interrupt-ms must be above 0");
    }
    if !(args.interval_seconds > 0.0) {
        bail!("--interval-seconds must be above 0");
    }
    let session = SessionConfig {
        stream_id: args.stream,
        repair_permille: args.repair_permille,
        min_repair: args.min_repair,
        silence: Duration::from_millis(args.silence_ms),
        silence_repeat: Duration::from_millis(args.silence_repeat_ms),
        nack_after_drain: true,
        compact_retry: args.compact_retry,
        drop_early_parts: args.drop_early_parts,
        move_from_hole: args.move_from_hole,
        answer: Duration::from_millis(args.answer_ms),
        fixed_repair: args.fixed_repair,
        fast_lane: args.fast_lane,
        compact: args.compact,
        compact_burst_rows: args.compact_burst_rows,
        compact_short_header: args.compact_short_header,
        nack_timing: NackTiming {
            delay: Duration::from_micros(args.nack_delay_us),
            retry_floor: Duration::from_micros(args.nack_retry_floor_us),
            silence: args.silence_nack,
            silence_budget: args.silence_nack_budget,
            rtt_floor: args.silence_nack_rtt_floor,
            interval_cap: args.interval_cap,
            margin_intervals: args.silence_margin_intervals,
            rate_cap: args.nack_rate_cap,
        },
        known_parts: args.known_parts,
        first_retry: Duration::from_millis(args.first_retry_ms),
        residual_target: args.compact_residual_ppm as f64 / 1e6,
        hole_give_up: Duration::from_millis(args.hole_give_up_ms),
        token_wait: Duration::from_millis(args.token_ms),
        first_double: Duration::from_millis(args.first_double_ms),
        hedge_after: Duration::from_millis(args.hedge_after_ms),
        lone_hedge_after: Duration::from_millis(args.lone_hedge_after_ms),
        plan_hold: av_mesh::client_session::PLAN_HOLD,
        plan_smoothing: av_mesh::client_session::PLAN_SMOOTHING,
        quality: args.quality.config(),
    };
    let mut config = LoadConfig::new(session, edges, args.listeners);
    let director = &mut config.director;
    director.entries = entries;
    director.paths = args.paths;
    director.spare = args.spare;
    director.token_wait = Duration::from_millis(args.token_ms);
    director.rebalance_giveup = Duration::from_millis(args.rebalance_giveup_ms);
    director.level_wait = Duration::from_millis(args.level_wait_ms);
    args.quality.apply(director);
    director.first_retry = Duration::from_millis(args.first_retry_ms);
    director.assignment_udp_port = args.assignment_udp_port;
    director.assignment_https_after = Duration::from_millis(args.assignment_https_after_ms);
    director.assignment_timeout = Duration::from_millis(args.assignment_timeout_ms.max(1));
    director.udp_port = args.udp_port;
    if let Some(text) = &args.ramp {
        config.ramp = parse_ramp(text).map_err(anyhow::Error::msg)?;
    }
    config.duration = Duration::from_secs(args.duration);
    if let Some(threads) = args.threads {
        config.threads = threads.max(1);
    }
    config.start_rate = args.start_rate;
    config.sample = args.sample;
    config.out_dir = args.out.clone();
    config.interval = Duration::from_secs_f64(args.interval_seconds);
    config.receive_buffer = args.receive_buffer_bytes;
    config.bind_ips = args.bind.clone();
    config.kernel_timestamps = args.kernel_timestamps;
    config.drain = Duration::from_millis(args.drain_ms);
    config.settle = Duration::from_millis(args.settle_ms);
    config.settle_parts = args.settle_parts;
    config.interrupt = Duration::from_millis(args.interrupt_ms);
    config.interval_buckets = args.interval_buckets;
    config.lag_limit = Duration::from_millis(args.lag_limit_ms);
    config.progress = !args.quiet;
    config.label = args.label.clone();
    config.coalesce = Duration::from_micros(args.coalesce_us);
    config.stranded_after = Duration::from_millis(args.stranded_ms);
    config.sid_prefix = args.sid_prefix.clone().unwrap_or_else(|| {
        args.label
            .clone()
            .unwrap_or_else(|| format!("load-{}", std::process::id()))
    });
    if !(0.0..=6_000.0).contains(&args.churn_percent_per_min) {
        bail!("--churn-percent-per-min must be 0 to 6000");
    }
    if args.churn_percent_per_min > 0.0 {
        let (absent_min, absent_max) =
            parse_range_ms(&args.churn_absence_ms).map_err(anyhow::Error::msg)?;
        config.churn = Some(Churn {
            per_minute: args.churn_percent_per_min / 100.0,
            absent_min,
            absent_max,
        });
    }
    if args.wave_size > 0 {
        if args.wave_every_ms == 0 {
            bail!("--wave-every-ms must be above 0");
        }
        config.waves = Some(Waves {
            size: args.wave_size,
            every: Duration::from_millis(args.wave_every_ms),
        });
    }
    config.seed = args.seed.unwrap_or_else(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos() as u64);
        av_mesh::load_assign::rendezvous_score(&config.sid_prefix, &(std::process::id(), nanos))
    });
    config.raw_sample = args.raw_sample;
    if !config.director.entries.is_empty() {
        let Some(tls_ca) = args.tls_ca.clone() else {
            bail!("--entry needs --tls-ca");
        };
        config.assign = Some(AssignConfig {
            tls_ca,
            server_name: args.server_name.clone(),
            in_flight: args.assign_in_flight.max(1),
            timeout: config.director.assignment_timeout,
        });
    }
    Ok(config)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut config = config(&args)?;
    let stop = Arc::new(AtomicBool::new(false));
    config.stop = Some(Arc::clone(&stop));
    {
        // SIGINT or SIGTERM ends the run early, with its drain and summary.
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("signal runtime");
            runtime.block_on(async {
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            });
            stop.store(true, Ordering::Relaxed);
        });
    }
    let summary = run(config, &mut |_| {})?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
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
            Args::command().get_version(),
            Some(av_mesh::build::VERSION)
        );
    }

    #[test]
    fn the_flags_make_the_configuration() {
        let args = Args::try_parse_from([
            "needletail-load",
            "--edges",
            "127.0.0.1:19446,127.0.0.2:19446",
            "--stream",
            "4001",
            "--ramp",
            "100@0,1000@30",
            "--threads",
            "3",
            "--compact",
            "false",
            "--out-dir",
            "x",
            "--bind",
            "10.0.0.5,10.0.0.6",
        ])
        .unwrap();
        let config = config(&args).unwrap();
        assert_eq!(config.edges.len(), 2);
        assert_eq!(
            config.ramp,
            vec![(Duration::ZERO, 100), (Duration::from_secs(30), 1_000)]
        );
        assert_eq!(config.threads, 3);
        assert!(!config.session.compact);
        assert!(config.session.fast_lane);
        // The wave 6/7 client: short form and the fast NACK timing.
        assert!(config.session.compact_short_header);
        assert_eq!(config.session.nack_timing, NackTiming::default());
        // Wave 8: the 200 ms move, the repeat silence, the compact retry,
        // and the start-up parts kept.
        assert_eq!(config.session.silence, Duration::from_millis(200));
        assert_eq!(config.session.silence_repeat, Duration::from_millis(1_000));
        assert!(config.session.compact_retry);
        assert!(!config.session.drop_early_parts);
        assert!(config.kernel_timestamps);
        assert_eq!(config.session.stream_id, 4001);
        assert_eq!(config.out_dir, Some(PathBuf::from("x")));
        assert_eq!(config.bind_ips.len(), 2);
        // Without a ramp, --listeners from the start.
        let args = Args::try_parse_from([
            "needletail-load",
            "--edges",
            "127.0.0.1:19446",
            "--stream",
            "1",
            "--listeners",
            "7",
        ])
        .unwrap();
        assert_eq!(config_ramp(&args), vec![(Duration::ZERO, 7)]);
    }

    #[test]
    fn the_wave8_client_flags_turn_back_to_wave_6() {
        let args = Args::try_parse_from([
            "needletail-load",
            "--edges",
            "127.0.0.1:19446",
            "--stream",
            "1",
            "--silence-nack-budget",
            "0",
            "--silence-nack-rtt-floor",
            "false",
            "--interval-cap",
            "0",
            "--silence-margin-intervals",
            "1",
            "--nack-rate-cap",
            "0",
            "--silence-ms",
            "1000",
            "--compact-retry",
            "false",
            "--drop-early-parts",
            "true",
        ])
        .unwrap();
        let config = config(&args).unwrap();
        assert_eq!(config.session.nack_timing, NackTiming::WAVE6);
        assert_eq!(config.session.silence, Duration::from_millis(1_000));
        assert!(!config.session.compact_retry);
        assert!(config.session.drop_early_parts);
    }

    #[test]
    fn the_entry_points_and_the_director_take_the_client_s_flags_and_defaults() {
        let args = Args::try_parse_from([
            "needletail-load",
            "--stream",
            "4001",
            "--entry",
            "https://10.80.1.1:19444/,https://10.80.1.2:19444",
            "--tls-ca",
            "ca.crt",
            "--server-name",
            "local.wavey.ai",
            "--label",
            "lon-1",
        ])
        .unwrap();
        let config = config(&args).unwrap();
        let director = &config.director;
        assert_eq!(
            director.entries,
            vec!["https://10.80.1.1:19444", "https://10.80.1.2:19444"]
        );
        let assign = config.assign.as_ref().expect("the HTTPS hedge");
        assert_eq!(assign.server_name.as_deref(), Some("local.wavey.ai"));
        assert!(config.edges.is_empty());
        assert_eq!(config.stranded_after, Duration::from_secs(5));
        assert_eq!(config.sid_prefix, "lon-1");
        // The client's defaults (B 2.4): exactly the director and session
        // that needletail-udp-client makes for the same stream (the
        // referee's rule, integration 2): the library's defaults, compared
        // whole, so a field added later is compared too. Only the run's own
        // values (entries, sids, nonces) and the parts a listener keeps
        // (`--known-parts`, memory) are the load's.
        let client = av_mesh::client_director::DirectorConfig::new(4001, "", Vec::new());
        assert_eq!((director.paths, director.spare), (2, true));
        let expected = av_mesh::client_director::DirectorConfig {
            entries: director.entries.clone(),
            session: director.session.clone(),
            nonce_seed: director.nonce_seed,
            ..client.clone()
        };
        assert_eq!(format!("{director:#?}"), format!("{expected:#?}"));
        let session = SessionConfig {
            known_parts: config.session.known_parts,
            ..SessionConfig::new(4001)
        };
        assert_eq!(format!("{:#?}", config.session), format!("{session:#?}"));
        assert_eq!(assign.timeout, client.assignment_timeout);
        // --entry needs the CA; neither edges nor an entry, or both, is an
        // error; so is a third path.
        assert!(config_of(&["needletail-load", "--stream", "1", "--entry", "https://a:1"]).is_err());
        assert!(config_of(&["needletail-load", "--stream", "1"]).is_err());
        assert!(config_of(&[
            "needletail-load", "--stream", "1", "--entry", "https://a:1", "--tls-ca", "ca",
            "--edges", "127.0.0.1:1",
        ])
        .is_err());
        assert!(config_of(&["needletail-load", "--stream", "1", "--edges", "127.0.0.1:1", "--paths", "3"]).is_err());
        // One path and no spare, the control of B 2.4.
        let config = config_of(&[
            "needletail-load",
            "--stream",
            "1",
            "--edges",
            "127.0.0.1:1",
            "--paths",
            "1",
            "--spare",
            "false",
        ])
        .unwrap();
        assert_eq!((config.director.paths, config.director.spare), (1, false));
        assert!(config.director.entries.is_empty() && config.assign.is_none());
    }

    #[test]
    fn the_party_flags_make_churn_waves_and_the_raw_sample() {
        let config = config_of(&["needletail-load", "--stream", "1", "--edges", "127.0.0.1:1"]).unwrap();
        assert!(config.churn.is_none() && config.waves.is_none());
        assert_eq!(config.raw_sample, 0);
        let config = config_of(&[
            "needletail-load", "--stream", "1", "--edges", "127.0.0.1:1",
            "--churn-percent-per-min", "5", "--churn-absence-ms", "2000-30000",
            "--wave-size", "500", "--wave-every-ms", "15000", "--seed", "42", "--raw-sample", "50",
        ])
        .unwrap();
        assert_eq!(
            config.churn,
            Some(Churn {
                per_minute: 0.05,
                absent_min: Duration::from_secs(2),
                absent_max: Duration::from_secs(30),
            })
        );
        assert_eq!(
            config.waves,
            Some(Waves {
                size: 500,
                every: Duration::from_secs(15),
            })
        );
        assert_eq!((config.seed, config.raw_sample), (42, 50));
        // Without --seed two hosts draw differently.
        let a = config_of(&["needletail-load", "--stream", "1", "--edges", "127.0.0.1:1", "--label", "lon-1"]).unwrap();
        let b = config_of(&["needletail-load", "--stream", "1", "--edges", "127.0.0.1:1", "--label", "lon-2"]).unwrap();
        assert_ne!(a.seed, b.seed);
        assert!(config_of(&["needletail-load", "--stream", "1", "--edges", "127.0.0.1:1", "--churn-percent-per-min", "5", "--churn-absence-ms", "9-3"]).is_err());
    }

    #[test]
    fn the_accounting_flags_have_the_defaults_of_b_2_4() {
        let config = config_of(&["needletail-load", "--stream", "1", "--edges", "127.0.0.1:1"]).unwrap();
        assert_eq!(config.settle, Duration::from_millis(3_500));
        assert_eq!(config.settle_parts, None);
        assert_eq!(config.interrupt, Duration::from_millis(30));
        assert_eq!(config.interval_buckets, IntervalBuckets::Total);
        let config = config_of(&[
            "needletail-load", "--stream", "1", "--edges", "127.0.0.1:1", "--settle-parts", "500",
            "--interrupt-ms", "50", "--interval-buckets", "edges",
        ])
        .unwrap();
        assert_eq!(config.settle_parts, Some(500));
        assert_eq!(config.interrupt, Duration::from_millis(50));
        assert_eq!(config.interval_buckets, IntervalBuckets::Edges);
        assert!(config_of(&["needletail-load", "--stream", "1", "--edges", "127.0.0.1:1", "--settle-parts", "4096"]).is_err());
        assert!(Args::try_parse_from(["needletail-load", "--stream", "1", "--interval-buckets", "x"]).is_err());
    }

    fn config_of(argv: &[&str]) -> Result<LoadConfig> {
        config(&Args::try_parse_from(argv).unwrap())
    }

    fn config_ramp(args: &Args) -> Vec<(Duration, usize)> {
        config(args).unwrap().ramp
    }
}
