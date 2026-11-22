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
    /// The edges' client UDP ports, `host:port,...`: listener i starts at
    /// edge i mod n, and moves to the next edge when its edge says `MOVE`
    /// or `REFUSED`, does not answer, or goes silent.
    #[arg(long, value_delimiter = ',', required = true)]
    edges: Vec<String>,
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
    #[arg(long)]
    repair_permille: Option<u32>,
    #[arg(long)]
    min_repair: Option<u32>,
    #[arg(long)]
    fixed_repair: bool,
    #[arg(long, default_value_t = 1_000)]
    silence_ms: u64,
    #[arg(long, default_value_t = 5_000)]
    answer_ms: u64,
    /// Parts whose bytes each listener keeps for the compact lane's window
    /// repair (needletail-udp-client keeps 4,096; a window spans at most
    /// 64, and 512 keeps a listener near 200 KB).
    #[arg(long, default_value_t = 512)]
    known_parts: usize,
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
    if edges.is_empty() {
        bail!("--edges names no edge");
    }
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
        },
        known_parts: args.known_parts,
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

    fn config_ramp(args: &Args) -> Vec<(Duration, usize)> {
        config(args).unwrap().ramp
    }
}
