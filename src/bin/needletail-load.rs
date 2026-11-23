//! Thousands of simulated UDP listeners of a Needletail stream on one load
//! host: each a real client (its own socket, token and subscription, the
//! `needletail-udp-client` protocol and decode, NACK and loss accounting),
//! a few worker threads for all of them. See `av_mesh::load`.
//!
//! ```text
//! needletail-load --edges 10.0.0.21:19446,10.0.0.22:19446 --stream 4001 \
//!     --ramp 100@0,1000@30,5000@60 --duration 120 --sample 20 --out target/load/run1
//! ```
//!
//! Writes `<out>/intervals.jsonl` (one line an interval: per edge and in
//! total, listeners active and subscribed, parts, gaps, missing, recovered,
//! duplicates, NACKs, bytes in, publication-to-arrival, time-to-first-part
//! and receive-lag percentiles, and the load host's CPU, worker busy time,
//! timer lateness, UDP receive buffer errors and socket drops, with
//! `load_host_saturated` and its reasons), `<out>/summary.json` (the same
//! over the run, each histogram with its buckets so that the summaries of
//! several load hosts merge), and for `--sample K` listeners
//! `<out>/arrivals/listener-<i>.csv` (the `needletail-udp-client --arrivals`
//! columns, in part order). The summary also goes to standard output.
//! SIGINT or SIGTERM ends the run early, with its drain and summary.

use anyhow::{bail, Context, Result};
use av_mesh::client_compact::NackTiming;
use av_mesh::client_session::SessionConfig;
use av_mesh::load::{parse_ramp, run, LoadConfig};
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
    about = "Simulated UDP listeners of a Needletail stream, thousands per host"
)]
struct Args {
    /// The edges' client UDP ports, `host:port,...`. Without --entry,
    /// listener i starts at edge i mod n; a listener that must move (its
    /// edge says `MOVE` or `REFUSED`, does not answer, or goes silent) goes
    /// to the edge that rendezvous of its session picks among those it has
    /// not left in the last --skip-ms (--move-rule next: the next edge of
    /// the list, as before). With --entry, the fallback when no entry point
    /// answers.
    #[arg(long, value_delimiter = ',')]
    edges: Vec<String>,
    /// Phase AM item 10: entry points of the region, `https://host:port`,
    /// any node of it (comma list): each listener asks one
    /// (`/api/assignment`, its own session id) where to subscribe when it
    /// starts and each time it moves, as needletail-udp-client does, and
    /// takes the first cache of the answer that it has not left in the
    /// last --skip-ms (a failover region's entry point, Phase AM item 11,
    /// once its region is lost).
    #[arg(long, value_delimiter = ',')]
    entry: Vec<String>,
    /// PEM file of the nodes' certificate authority (with --entry).
    #[arg(long)]
    tls_ca: Option<PathBuf>,
    /// TLS server name of the nodes, when it is not their address.
    #[arg(long)]
    server_name: Option<String>,
    /// Client UDP port of the caches when the entry point names none.
    #[arg(long)]
    udp_port: Option<u16>,
    /// Assignment requests in flight at most, and an entry point's time to
    /// answer (ms) before the next is asked.
    #[arg(long, default_value_t = 256)]
    assign_in_flight: usize,
    #[arg(long, default_value_t = 500)]
    assign_timeout_ms: u64,
    /// A moving listener does not go back to an edge it left within this.
    #[arg(long, default_value_t = 30_000)]
    skip_ms: u64,
    /// A listener with no part for this long counts as stranded.
    #[arg(long, default_value_t = 5_000)]
    stranded_ms: u64,
    /// Where a listener moves without an assignment: `rendezvous` (of its
    /// session over the edges not left) or `next` (the next edge of
    /// --edges, as before).
    #[arg(long, default_value = "rendezvous")]
    move_rule: String,
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
    /// A part is counted missing once it is this many parts behind its
    /// listener's newest part (at most 4,095; past the NACKs' give-up of a
    /// second).
    #[arg(long, default_value_t = 1_500)]
    settle_parts: u64,
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
    if edges.is_empty() && entries.is_empty() {
        bail!("--edges or --entry must name the edges or an entry point");
    }
    let rendezvous_moves = match args.move_rule.as_str() {
        "rendezvous" => true,
        "next" => false,
        other => bail!("--move-rule {other:?}: rendezvous or next"),
    };
    if args.settle_parts == 0 || args.settle_parts >= 4_096 {
        bail!("--settle-parts must be 1 to 4095");
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
    };
    let mut config = LoadConfig::new(session, edges, args.listeners);
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
    config.settle_parts = args.settle_parts;
    config.lag_limit = Duration::from_millis(args.lag_limit_ms);
    config.progress = !args.quiet;
    config.label = args.label.clone();
    config.coalesce = Duration::from_micros(args.coalesce_us);
    config.skip_for = Duration::from_millis(args.skip_ms);
    config.stranded_after = Duration::from_millis(args.stranded_ms);
    config.rendezvous_moves = rendezvous_moves;
    config.sid_prefix = args.sid_prefix.clone().unwrap_or_else(|| {
        args.label
            .clone()
            .unwrap_or_else(|| format!("load-{}", std::process::id()))
    });
    if !entries.is_empty() {
        let Some(tls_ca) = args.tls_ca.clone() else {
            bail!("--entry needs --tls-ca");
        };
        config.assign = Some(AssignConfig {
            entries,
            stream_id: args.stream,
            tls_ca,
            server_name: args.server_name.clone(),
            udp_port: args.udp_port,
            in_flight: args.assign_in_flight.max(1),
            timeout: Duration::from_millis(args.assign_timeout_ms.max(1)),
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
    fn the_phase_am_flags_ask_the_entry_points_and_move_by_rendezvous() {
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
        let assign = config.assign.as_ref().expect("assignment");
        assert_eq!(
            assign.entries,
            vec!["https://10.80.1.1:19444", "https://10.80.1.2:19444"]
        );
        assert_eq!(assign.server_name.as_deref(), Some("local.wavey.ai"));
        assert!(config.edges.is_empty());
        assert!(config.rendezvous_moves);
        assert_eq!(config.skip_for, Duration::from_secs(30));
        assert_eq!(config.stranded_after, Duration::from_secs(5));
        assert_eq!(config.sid_prefix, "lon-1");
        // --entry needs the CA; neither edges nor an entry is an error.
        assert!(config_of(&["needletail-load", "--stream", "1", "--entry", "https://a:1"]).is_err());
        assert!(config_of(&["needletail-load", "--stream", "1"]).is_err());
        // The rule of before: the next edge of the list.
        let config = config_of(&[
            "needletail-load",
            "--stream",
            "1",
            "--edges",
            "127.0.0.1:1",
            "--move-rule",
            "next",
        ])
        .unwrap();
        assert!(!config.rendezvous_moves && config.assign.is_none());
    }

    fn config_of(argv: &[&str]) -> Result<LoadConfig> {
        config(&Args::try_parse_from(argv).unwrap())
    }

    fn config_ramp(args: &Args) -> Vec<(Duration, usize)> {
        config(args).unwrap().ramp
    }
}
