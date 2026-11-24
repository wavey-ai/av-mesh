//! A client of a Needletail stream by UDP from the edges of its region.
//!
//! The client holds its stream on two edges at once by default (B-listener
//! section 1): two legs, each a subscription at one edge that sends every
//! part, and a spare (a token at a third edge, nothing else there). The
//! first arrival of each part number is delivered. Which edges is the
//! director's (`av_mesh::client_director`): it asks one of the `--entry`
//! points for an assignment (`ASSIGN` over UDP from the listener's own
//! socket, HTTPS as a hedge), adds and releases legs, hedges a silent leg
//! at the spare, answers each `MOVE` make-before-break by its reason, and
//! fails over to another region. The legs themselves (requests, tokens,
//! parts, loss, NACK, the verdicts on a silent leg) are the session's
//! (`av_mesh::client_session`). This binary performs their I/O: the media
//! socket, the HTTPS requests and the name lookups. `--paths 1` holds one
//! leg; `--spare false` holds no spare.
//!
//! Each part comes as compact datagrams (`av_mesh::client_compact`, the
//! default) or as a RaptorQ object, on the fast lane when the edge serves
//! it. With `--verify-http` every part received is also fetched over HTTP
//! from the edge whose datagram brought it, as an LL-HLS player would, and
//! the bytes compared.
//!
//! The report (`needletail.udp-client.v2`, standard output) keeps every
//! field of v1 and adds the legs, their replacements, the time on one path,
//! the wire bytes a part and the short grants by reason. `--arrivals`
//! writes one line a part in part order, its seventh column the edge whose
//! datagram was the first arrival; `--write-parts` the parts' bytes in the
//! same order.

use anyhow::{anyhow, bail, Context, Result};
use av_mesh::client_compact::NackTiming;
use av_mesh::client_director::{
    Action, Director, DirectorConfig, LegEvent, LegKey, LegStart, LegView, Replacement,
    ReplacementKind, SessionLegs, ShortGrants, SinglePath,
};
use av_mesh::client_session::{LegReport, Received, SessionConfig, UdpSession};
use av_mesh::client_udp::{ObjectKind, PartObject};
use bytes::{Bytes, BytesMut};
use clap::Parser;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{mpsc, Semaphore};
use tokio_rustls::TlsConnector;

/// A gap between first arrivals this long or longer is an interruption
/// (B 2.8).
const INTERRUPTION: Duration = Duration::from_millis(30);
/// A leg delivers while its last media is at most this old, or 8 part
/// intervals if longer (B 2.8, as `LegView::delivering`).
const DELIVERING_FLOOR: Duration = Duration::from_millis(20);
const DELIVERING_INTERVALS: u32 = 8;
/// A part's lag on each leg is read this many parts after it, when the
/// slower leg has brought it (the session keeps the last 64).
const LAG_SETTLE_PARTS: u64 = 32;
const LAG_FORGET_PARTS: u64 = 60;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "needletail-udp-client",
    version = av_mesh::build::VERSION,
    about = "Receive a Needletail stream by UDP from two edges of its region"
)]
struct Args {
    /// Entry points, `https://host:port`: repeat the flag or separate them
    /// with commas. One is asked first (by the session id), the next after
    /// --first-retry-ms.
    #[arg(long, required = true, value_delimiter = ',')]
    entry: Vec<String>,
    #[arg(long)]
    stream: u64,
    /// PEM file of the certificate authority of the nodes.
    #[arg(long)]
    tls_ca: PathBuf,
    /// TLS server name of the nodes, when it is not their address.
    #[arg(long)]
    server_name: Option<String>,
    #[arg(long, default_value_t = 10)]
    duration_seconds: u64,
    /// Client UDP port of the caches, when the assignment does not name it.
    #[arg(long)]
    udp_port: Option<u16>,
    /// Legs held on a stream the entry grants two for (B 2.4). 1: one edge
    /// at a time, with the moves of the two-path work.
    #[arg(long, default_value_t = av_mesh::client_director::DEFAULT_PATHS)]
    paths: u8,
    /// Hold a spare (a token at a third edge) and hedge a silent leg to it.
    /// `false`: a dead leg is replaced through the entry.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    spare: bool,
    /// A leg with no TOKEN after this long (or four round trips to its
    /// entry, if longer) is passed over: another edge is asked while it
    /// goes on trying.
    #[arg(long, default_value_t = av_mesh::client_director::DEFAULT_TOKEN_MS)]
    token_ms: u64,
    /// Repair of each RaptorQ block, per thousand source symbols, at least.
    /// By default 0 when the edge serves the compact lane (NACK backs it
    /// up), and 250 on the RaptorQ lane alone, as before wave 5.
    #[arg(long)]
    repair_permille: Option<u32>,
    /// Repair symbols of each RaptorQ block, at least: by default 1 with
    /// the compact lane, 3 on the RaptorQ lane alone.
    #[arg(long)]
    min_repair: Option<u32>,
    /// A leg with no datagram for this long while another leg sent one has
    /// failed; with no media for this long while another leg delivers,
    /// likewise. No leg at all for this long: every leg is asked again.
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_SILENCE_MS)]
    silence_ms: u64,
    /// A leg with no OK this long after its first request has failed.
    #[arg(long, default_value_t = 5_000)]
    answer_ms: u64,
    /// A leg with no media for this long while another leg delivers is
    /// hedged at the spare.
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_HEDGE_AFTER_MS)]
    hedge_after_ms: u64,
    /// The only leg with no datagram at all for this long is hedged at the
    /// spare (two keepalives and a margin: a lost WAIT moves nobody).
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_LONE_HEDGE_AFTER_MS)]
    lone_hedge_after_ms: u64,
    /// After MOVE rebalance: a replacement not level by then is released
    /// and the old edge asked to take the listener back.
    #[arg(long, default_value_t = av_mesh::client_director::DEFAULT_REBALANCE_GIVEUP_MS)]
    rebalance_giveup_ms: u64,
    /// After any other MOVE: a replacement that has delivered steadily for
    /// this long takes over though it is not level.
    #[arg(long, default_value_t = av_mesh::client_director::DEFAULT_LEVEL_WAIT_MS)]
    level_wait_ms: u64,
    /// A leg's first tokenless SUB and its first SUB with the token are
    /// each sent again this soon. 0: off.
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_FIRST_DOUBLE_MS)]
    first_double_ms: u64,
    /// The residual part loss the compact plans aim for, parts per million.
    #[arg(long, default_value_t = (av_mesh::client_compact::RESIDUAL_TARGET * 1e6).round() as u32)]
    compact_residual_ppm: u32,
    /// A hole is asked for until this long after it showed (or until it is
    /// 3,072 parts behind the newest).
    #[arg(long, default_value_t = av_mesh::client_compact::HOLE_GIVE_UP.as_millis() as u64)]
    hole_give_up_ms: u64,
    /// Keep the repair that was asked for, whatever the measured loss.
    #[arg(long)]
    fixed_repair: bool,
    /// Fetch every received part over HTTP from the edge whose datagram
    /// brought it, and compare.
    #[arg(long)]
    verify_http: bool,
    /// The whole budget of one assignment request (ms).
    #[arg(long, default_value_t = av_mesh::client_director::DEFAULT_ASSIGNMENT_TIMEOUT_MS)]
    assignment_timeout_ms: u64,
    /// The entry points' client UDP port: an assignment is asked there
    /// (`ASSIGN`, one datagram each way, from the media socket). 0: HTTPS
    /// alone.
    #[arg(long, default_value_t = av_mesh::client_udp::DEFAULT_CLIENT_UDP_PORT)]
    assignment_udp_port: u16,
    /// With the assignment over UDP, the HTTPS request starts this long
    /// after it (ms) unless UDP has answered.
    #[arg(long, default_value_t = av_mesh::client_director::DEFAULT_ASSIGNMENT_HTTPS_AFTER_MS)]
    assignment_https_after_ms: u64,
    /// An unanswered UDP request (the assignment, a first SUB, a SUB with
    /// the token) is repeated after this (ms; three round trips once one is
    /// measured), then twice as long each time up to 300 ms; the next entry
    /// is asked too after it. 0: every 300 ms.
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_FIRST_RETRY_MS)]
    first_retry_ms: u64,
    /// Session id for assignment (CMCD `sid`).
    #[arg(long, default_value = "udp-client")]
    session: String,
    /// UDP receive buffer in bytes.
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    receive_buffer_bytes: usize,
    /// Wave 9 D: UDP GRO on the client's socket: an edge's GSO run of a 4K
    /// part's symbols (or what the NIC's GRO joined) is one receive of up to
    /// 64 KiB, cut into its datagrams here, instead of one receive and one
    /// queue entry per symbol. `false`: one datagram per receive.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    gro: bool,
    /// Local address for the UDP socket.
    #[arg(long)]
    bind: Option<SocketAddr>,
    /// Write the parts' bytes, concatenated in part order, to this file.
    #[arg(long)]
    write_parts: Option<PathBuf>,
    /// Ask the edges for the fast lane (`F1`): parts as their symbols
    /// arrive at the edge, before it decodes them. `false` takes the
    /// cache-fed lane alone.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    fast_lane: bool,
    /// Ask the edges for the compact lane: a part that fits one datagram as
    /// one datagram, with repair across parts and NACK. `false` takes the
    /// RaptorQ lane alone, with its repair of before wave 5.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compact: bool,
    /// On a path whose losses come in bursts, plan no copies and window
    /// repair rows for the burst. `false`: one copy and at most two more
    /// rows, as before.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compact_burst_rows: bool,
    /// Ask for parts in the short form (a 7-byte header instead of 11).
    /// `false` asks for the long form.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compact_short_header: bool,
    /// Wait before the first NACK of a hole that a later part showed, in
    /// µs (longer while a slower leg may still bring it).
    #[arg(long, default_value_t = av_mesh::client_compact::FAST_NACK_DELAY.as_micros() as u64)]
    nack_delay_us: u64,
    /// The floor of the wait between NACKs of a hole, in µs (one and a half
    /// round trips when longer).
    #[arg(long, default_value_t = av_mesh::client_compact::FAST_NACK_RETRY.as_micros() as u64)]
    nack_retry_floor_us: u64,
    /// NACK the parts that are due when no part has come for an interval
    /// and a margin past the next one's due time, and again each interval
    /// while the silence lasts, up to 50 ms. `false`: a hole is asked for
    /// only once a later part shows it.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    silence_nack: bool,
    /// The parts that one silence asks for, at most. 0: no bound.
    #[arg(long, default_value_t = av_mesh::client_compact::DEFAULT_SILENCE_NACK_BUDGET)]
    silence_nack_budget: u16,
    /// A silence's NACKs of a hole are one and a half round trips apart at
    /// least. `false`: every interval.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    silence_nack_rtt_floor: bool,
    /// A gap between arrivals longer than this many part intervals is no
    /// sample of the interval or jitter (0: every gap is).
    #[arg(long, default_value_t = av_mesh::client_compact::DEFAULT_INTERVAL_CAP)]
    interval_cap: u32,
    /// A silence is NACKed this many part intervals past the next part's
    /// due time (and at least 0.5 ms and four times the jitter).
    #[arg(long, default_value_t = av_mesh::client_compact::DEFAULT_SILENCE_MARGIN_INTERVALS)]
    silence_margin_intervals: u32,
    /// The parts a second that the client asks for at most (0: no cap).
    #[arg(long, default_value_t = av_mesh::client_compact::DEFAULT_NACK_RATE_CAP)]
    nack_rate_cap: u32,
    /// While every leg stays silent, the legs are asked again this often
    /// (ms; a stall of the stream itself is not asked about every
    /// --silence-ms).
    #[arg(long, default_value_t = av_mesh::client_session::DEFAULT_SILENCE_REPEAT_MS)]
    silence_repeat_ms: u64,
    /// An edge that answers after the client gave up the compact lane for
    /// want of an answer is asked for it again (twice at most).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compact_retry: bool,
    /// Drop the short-form parts that come before the edge's OK (`false`:
    /// kept, read once the OK comes).
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    drop_early_parts: bool,
    /// A leg started where no leg delivers asks its edge for the oldest
    /// part still missing, not only the parts after the newest.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    move_from_hole: bool,
    /// No NACK, and no verdict on a silent leg, while datagrams are queued
    /// on the socket. `false`: NACKs between the datagrams of a backlog.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    nack_after_drain: bool,
    /// At the end of the run, keep receiving for up to this long (ms) to
    /// fill the holes among the parts received so far, by NACK; parts past
    /// the last one are not taken. 0: stop at the end.
    #[arg(long, default_value_t = 300)]
    drain_ms: u64,
    /// Write one line for each part received, in part order: part number,
    /// arrival time (Unix µs), bytes, publication and availability times as
    /// the object carries them, the kernel's receive time of the datagram
    /// (Unix µs, SO_TIMESTAMPNS; 0 where the host gives none), and the edge
    /// whose datagram was the first arrival. With --write-parts, a
    /// measurement tool can find each part's bytes (the lengths give the
    /// offsets).
    #[arg(long)]
    arrivals: Option<PathBuf>,
    /// Read the media playlist of each edge a leg is added at, beside the
    /// subscription (its time is `startup.playlist_ms`). The client never
    /// waits for it: the legs are judged by the UDP lane. With --verify-http
    /// it is read either way (it names the parts' extension).
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    playlist_first: bool,
}

impl Args {
    fn session_config(&self) -> SessionConfig {
        SessionConfig {
            stream_id: self.stream,
            repair_permille: self.repair_permille,
            min_repair: self.min_repair,
            silence: Duration::from_millis(self.silence_ms),
            silence_repeat: Duration::from_millis(self.silence_repeat_ms),
            nack_after_drain: self.nack_after_drain,
            compact_retry: self.compact_retry,
            drop_early_parts: self.drop_early_parts,
            move_from_hole: self.move_from_hole,
            answer: Duration::from_millis(self.answer_ms),
            fixed_repair: self.fixed_repair,
            fast_lane: self.fast_lane,
            compact: self.compact,
            compact_burst_rows: self.compact_burst_rows,
            compact_short_header: self.compact_short_header,
            nack_timing: NackTiming {
                delay: Duration::from_micros(self.nack_delay_us),
                retry_floor: Duration::from_micros(self.nack_retry_floor_us),
                silence: self.silence_nack,
                silence_budget: self.silence_nack_budget,
                rtt_floor: self.silence_nack_rtt_floor,
                interval_cap: self.interval_cap,
                margin_intervals: self.silence_margin_intervals,
                rate_cap: self.nack_rate_cap,
            },
            known_parts: 0,
            first_retry: Duration::from_millis(self.first_retry_ms),
            residual_target: f64::from(self.compact_residual_ppm) / 1e6,
            hole_give_up: Duration::from_millis(self.hole_give_up_ms),
            token_wait: Duration::from_millis(self.token_ms),
            first_double: Duration::from_millis(self.first_double_ms),
            hedge_after: Duration::from_millis(self.hedge_after_ms),
            lone_hedge_after: Duration::from_millis(self.lone_hedge_after_ms),
        }
    }

    fn director_config(&self, nonce_seed: u64) -> DirectorConfig {
        let mut config = DirectorConfig::new(self.stream, self.session.clone(), self.entry.clone());
        config.paths = self.paths;
        config.spare = self.spare;
        config.token_wait = Duration::from_millis(self.token_ms);
        config.rebalance_giveup = Duration::from_millis(self.rebalance_giveup_ms);
        config.level_wait = Duration::from_millis(self.level_wait_ms);
        config.first_retry = Duration::from_millis(self.first_retry_ms);
        config.assignment_udp_port = self.assignment_udp_port;
        config.assignment_https_after = Duration::from_millis(self.assignment_https_after_ms);
        config.assignment_timeout = Duration::from_millis(self.assignment_timeout_ms);
        config.udp_port = self.udp_port;
        config.nonce_seed = nonce_seed;
        config
    }
}

fn now_unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_micros() as u64)
}

#[derive(Debug, Clone)]
struct Url {
    authority: String,
    path: String,
}

impl Url {
    fn parse(value: &str) -> Result<Self> {
        let rest = value
            .strip_prefix("https://")
            .ok_or_else(|| anyhow!("only https URLs are supported: {value}"))?;
        let (authority, path) = match rest.find('/') {
            Some(index) => (&rest[..index], &rest[index..]),
            None => (rest, "/"),
        };
        Ok(Self {
            authority: authority.to_owned(),
            path: path.trim_end_matches('/').to_owned(),
        })
    }

    fn host_port(&self) -> (String, u16) {
        match self.authority.rsplit_once(':') {
            Some((host, port)) => (
                host.trim_matches(['[', ']']).to_owned(),
                port.parse().unwrap_or(443),
            ),
            None => (self.authority.clone(), 443),
        }
    }
}

#[derive(Clone)]
struct Http {
    connector: TlsConnector,
    server_name: Option<String>,
    session: String,
    connections: Arc<tokio::sync::Mutex<HashMap<String, SendRequest<Empty<Bytes>>>>>,
}

impl Http {
    async fn connection(&self, authority: &str) -> Result<SendRequest<Empty<Bytes>>> {
        let mut connections = self.connections.lock().await;
        if let Some(connection) = connections.get(authority) {
            if !connection.is_closed() {
                return Ok(connection.clone());
            }
        }
        let url = Url {
            authority: authority.to_owned(),
            path: String::new(),
        };
        let (host, port) = url.host_port();
        let tcp = tokio::time::timeout(
            Duration::from_secs(3),
            TcpStream::connect((host.as_str(), port)),
        )
        .await
        .map_err(|_| anyhow!("connect {authority}: timeout"))??;
        tcp.set_nodelay(true)?;
        let name = self.server_name.clone().unwrap_or(host);
        let server_name = rustls::pki_types::ServerName::try_from(name)
            .map_err(|error| anyhow!("invalid TLS server name: {error}"))?;
        let tls = self.connector.connect(server_name, tcp).await?;
        let (sender, connection) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tls)).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        connections.insert(authority.to_owned(), sender.clone());
        Ok(sender)
    }

    async fn get(&self, authority: &str, path: &str) -> Result<(StatusCode, Bytes)> {
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            let mut sender = self.connection(authority).await?;
            let request = Request::builder()
                .method("GET")
                .uri(format!("https://{authority}{path}"))
                .header("cmcd-session", format!("sid=\"{}\"", self.session))
                .body(Empty::<Bytes>::new())?;
            let response = sender.send_request(request).await?;
            let (parts, mut body) = response.into_parts();
            let mut bytes = BytesMut::new();
            while let Some(frame) = body.frame().await {
                if let Ok(data) = frame?.into_data() {
                    bytes.extend_from_slice(&data);
                }
            }
            Ok::<_, anyhow::Error>((parts.status, bytes.freeze()))
        })
        .await;
        match result {
            Ok(result) => {
                if result.is_err() {
                    self.connections.lock().await.remove(authority);
                }
                result
            }
            Err(_) => {
                self.connections.lock().await.remove(authority);
                Err(anyhow!("GET https://{authority}{path}: timeout"))
            }
        }
    }
}

fn tls_connector(args: &Args) -> Result<TlsConnector> {
    let pem =
        std::fs::read(&args.tls_ca).with_context(|| format!("read {}", args.tls_ca.display()))?;
    let mut roots = rustls::RootCertStore::empty();
    for certificate in rustls_pemfile::certs(&mut pem.as_slice()) {
        roots.add(certificate?)?;
    }
    if roots.is_empty() {
        bail!("{} holds no certificate", args.tls_ca.display());
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(TlsConnector::from(Arc::new(config)))
}

/// The part numbers and extension that a media playlist names.
fn playlist_parts(body: &str) -> (Option<u64>, String) {
    let mut last = None;
    let mut extension = "bin".to_owned();
    for line in body.lines() {
        let Some(start) = line.find("URI=\"part") else {
            continue;
        };
        let uri = &line[start + 5..];
        let uri = &uri[..uri.find('"').unwrap_or(uri.len())];
        if let Some((number, ext)) = uri
            .strip_prefix("part")
            .and_then(|rest| rest.split_once('.'))
        {
            if let Ok(number) = number.parse::<u64>() {
                last = Some(last.map_or(number, |last: u64| last.max(number)));
                extension = ext.to_owned();
            }
        }
    }
    (last, extension)
}

/// A leg's lag behind the first arrival of each part, in 10 µs buckets up
/// to 100 ms (made when the leg is added: nothing is allocated per part).
#[derive(Debug, Clone)]
struct LagHistogram {
    counts: Box<[u32]>,
    over: u32,
    max: Duration,
}

const LAG_BUCKET_US: u64 = 10;
const LAG_BUCKETS: usize = 10_000;

impl LagHistogram {
    fn new() -> Self {
        Self {
            counts: vec![0; LAG_BUCKETS].into_boxed_slice(),
            over: 0,
            max: Duration::ZERO,
        }
    }

    fn record(&mut self, lag: Duration) {
        let bucket = (lag.as_micros() as u64 / LAG_BUCKET_US) as usize;
        match self.counts.get_mut(bucket) {
            Some(count) => *count += 1,
            None => self.over += 1,
        }
        self.max = self.max.max(lag);
    }

    fn merge(&mut self, other: &Self) {
        for (count, add) in self.counts.iter_mut().zip(other.counts.iter()) {
            *count += add;
        }
        self.over += other.over;
        self.max = self.max.max(other.max);
    }

    fn total(&self) -> u64 {
        self.counts
            .iter()
            .map(|count| u64::from(*count))
            .sum::<u64>()
            + u64::from(self.over)
    }

    /// The `q` quantile in ms (a bucket's upper edge; the maximum past the
    /// last bucket).
    fn quantile_ms(&self, q: f64) -> Option<f64> {
        let total = self.total();
        if total == 0 {
            return None;
        }
        let rank = ((total as f64 * q).ceil() as u64).clamp(1, total);
        let mut seen = 0_u64;
        for (bucket, count) in self.counts.iter().enumerate() {
            seen += u64::from(*count);
            if seen >= rank {
                return Some(((bucket as u64 + 1) * LAG_BUCKET_US) as f64 / 1_000.0);
            }
        }
        Some(self.max.as_secs_f64() * 1_000.0)
    }
}

/// One leg as the report keeps it: where and when it was added, what the
/// client saw of it, and the session's figures read as the director
/// releases it (a released leg's figures are gone from the session).
#[derive(Debug, Clone)]
struct LegLog {
    key: LegKey,
    node: String,
    /// Its node, as an index into the client's node names.
    edge: usize,
    added: Instant,
    /// Its last media datagram (the client's view: for the time on one
    /// path).
    last_media: Option<Instant>,
    lags: LagHistogram,
    report: Option<LegReport>,
}

/// The session as the director drives it in this binary: every leg added
/// is logged (its node named once in `nodes`), and its report is read
/// before the session lets it go.
struct Recorded<'a> {
    session: &'a mut UdpSession,
    logs: &'a mut Vec<LegLog>,
    nodes: &'a mut Vec<String>,
}

impl SessionLegs for Recorded<'_> {
    fn add_leg(
        &mut self,
        leg: LegKey,
        node: &str,
        addr: SocketAddr,
        start: LegStart,
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) {
        let edge = match self.nodes.iter().position(|known| known == node) {
            Some(edge) => edge,
            None => {
                self.nodes.push(node.to_owned());
                self.nodes.len() - 1
            }
        };
        self.logs.push(LegLog {
            key: leg,
            node: node.to_owned(),
            edge,
            added: now,
            last_media: None,
            lags: LagHistogram::new(),
            report: None,
        });
        self.session.add_leg(leg, node, addr, start, now, send);
    }

    fn release_leg(&mut self, leg: LegKey, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8])) {
        if let Some(log) = self.logs.iter_mut().rev().find(|log| log.key == leg) {
            log.report = self.session.leg_report(leg).or(log.report.take());
        }
        self.session.release_leg(leg, now, send);
    }

    fn takeback_leg(&mut self, leg: LegKey, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8])) {
        self.session.takeback_leg(leg, now, send);
    }

    fn reask_leg(&mut self, leg: LegKey, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8])) {
        self.session.reask_leg(leg, now, send);
    }

    fn leg(&self, leg: LegKey, now: Instant) -> Option<LegView> {
        self.session.leg(leg, now)
    }

    fn level(&self, new: LegKey, old: LegKey) -> bool {
        self.session.level(new, old)
    }

    fn oldest_hole(&self) -> Option<u64> {
        self.session.oldest_hole()
    }

    fn next_event(&mut self) -> Option<LegEvent> {
        self.session.next_event()
    }
}

/// What a spawned task brings back to the loop.
enum Done {
    Https {
        id: u64,
        body: Option<Bytes>,
    },
    Resolved {
        host: String,
        ip: Option<IpAddr>,
    },
    /// A media playlist read: the parts' extension, or why it failed.
    Playlist {
        node: String,
        answer: Result<String, String>,
    },
}

#[derive(Debug, Serialize)]
struct MoveRecord {
    at_ms: u64,
    from: String,
    to: String,
    reason: String,
    next_part: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
struct Percentiles {
    count: usize,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

fn percentiles(mut values: Vec<f64>) -> Percentiles {
    if values.is_empty() {
        return Percentiles::default();
    }
    values.sort_by(f64::total_cmp);
    let at = |p: f64| values[((values.len() - 1) as f64 * p).round() as usize];
    Percentiles {
        count: values.len(),
        p50: at(0.5),
        p95: at(0.95),
        p99: at(0.99),
        max: *values.last().unwrap(),
    }
}

/// When each step of the start took place, in ms from the client's start
/// (the first time it happened): the time to first audio, step by step.
#[derive(Debug, Default, Serialize)]
struct Startup {
    assignment_ms: Option<f64>,
    /// How the first assignment came: "udp" or "https".
    assignment_via: Option<&'static str>,
    /// The first playlist read (with --playlist-first or --verify-http).
    playlist_ms: Option<f64>,
    first_request_ms: Option<f64>,
    token_ms: Option<f64>,
    subscribed_ms: Option<f64>,
    first_part_ms: Option<f64>,
    /// The first part received, after the part that the first OK named.
    first_part_after_named: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
struct LagFigures {
    median: Option<f64>,
    p99: Option<f64>,
}

/// One edge's legs in the report (B 2.8 `legs`; the legs a listener held at
/// one node over the run, together).
#[derive(Debug, Default, Serialize)]
struct LegFigures {
    legs: u64,
    first_part: Option<u64>,
    parts_first: u64,
    duplicates: u64,
    datagrams: u64,
    wire_bytes: u64,
    loss: Option<f64>,
    rtt_ms: Option<f64>,
    lag_ms: LagFigures,
    longest_gap_ms: f64,
    plan: Option<String>,
    before_start: u64,
}

#[derive(Debug, Serialize)]
struct ReplacementRecord {
    at_ms: u64,
    from: String,
    to: String,
    reason: String,
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    handover_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exposure_ms: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
struct ShortGrantCounts {
    #[serde(rename = "one-edge")]
    one_edge: u64,
    bitrate: u64,
    gate: u64,
    unexplained: u64,
}

impl From<ShortGrants> for ShortGrantCounts {
    fn from(grants: ShortGrants) -> Self {
        Self {
            one_edge: grants.one_edge,
            bitrate: grants.bitrate,
            gate: grants.gate,
            unexplained: grants.unexplained,
        }
    }
}

fn ms_of(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

/// The report's `legs`: each node's legs, from their logs.
fn leg_figures(logs: &[LegLog]) -> BTreeMap<String, LegFigures> {
    let mut figures: BTreeMap<String, LegFigures> = BTreeMap::new();
    let mut lags: BTreeMap<&str, LagHistogram> = BTreeMap::new();
    for log in logs {
        let entry = figures.entry(log.node.clone()).or_default();
        entry.legs += 1;
        lags.entry(log.node.as_str())
            .and_modify(|merged| merged.merge(&log.lags))
            .or_insert_with(|| log.lags.clone());
        let Some(report) = &log.report else {
            continue;
        };
        entry.first_part = match (entry.first_part, report.first_part) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        entry.parts_first += report.stats.parts_first;
        entry.duplicates += report.stats.duplicates;
        entry.before_start += report.stats.before_start;
        entry.datagrams += report.datagrams;
        entry.wire_bytes += report.wire_bytes;
        entry.loss = report.loss.or(entry.loss);
        entry.rtt_ms = report.rtt.map(ms_of).or(entry.rtt_ms);
        entry.longest_gap_ms = entry.longest_gap_ms.max(ms_of(report.longest_gap));
        entry.plan = Some(report.plan.word());
    }
    for (node, histogram) in lags {
        if let Some(entry) = figures.get_mut(node) {
            entry.lag_ms = LagFigures {
                median: histogram.quantile_ms(0.5),
                p99: histogram.quantile_ms(0.99),
            };
        }
    }
    figures
}

/// The report's `replacements`, and `moves` (v1: one record per leg
/// replaced, take-backs left out) with the new leg's first part.
fn replacement_records(
    replacements: &[Replacement],
    logs: &[LegLog],
    started: Instant,
) -> (Vec<ReplacementRecord>, Vec<MoveRecord>) {
    let at_ms = |at: Instant| at.saturating_duration_since(started).as_millis() as u64;
    let records = replacements
        .iter()
        .map(|replacement| ReplacementRecord {
            at_ms: at_ms(replacement.at),
            from: replacement.from.clone(),
            to: replacement.to.clone(),
            reason: replacement.reason.clone(),
            kind: replacement.kind.name(),
            handover_ms: replacement.handover.map(ms_of),
            exposure_ms: replacement.exposure.map(ms_of),
        })
        .collect();
    let moves = replacements
        .iter()
        .filter(|replacement| replacement.kind != ReplacementKind::Takeback)
        .map(|replacement| MoveRecord {
            at_ms: at_ms(replacement.at),
            from: replacement.from.clone(),
            to: replacement.to.clone(),
            reason: replacement.reason.clone(),
            next_part: logs
                .iter()
                .rev()
                .find(|log| log.node == replacement.to && log.added <= replacement.at)
                .and_then(|log| log.report.as_ref())
                .and_then(|report| report.first_part),
        })
        .collect();
    (records, moves)
}

/// One part as the arrivals file writes it.
#[derive(Debug, Clone, Copy)]
struct Arrival {
    arrival_us: u64,
    bytes: usize,
    published_us: u64,
    available_us: u64,
    kernel_us: u64,
    /// The edge (an index into the client's node names).
    edge: Option<usize>,
}

/// The `--arrivals` file: the six columns of v1, then `edge` (B 2.8). The
/// referee's `ntcmp udp-ingest` reads the first three by position; the
/// lab's scripts read columns by name.
fn arrivals_csv(arrivals: &BTreeMap<u64, Arrival>, nodes: &[String]) -> String {
    let mut text = String::from(
        "part,arrival_unix_us,bytes,published_unix_us,available_unix_us,kernel_unix_us,edge\n",
    );
    for (part, arrival) in arrivals {
        let edge = arrival
            .edge
            .and_then(|index| nodes.get(index))
            .map_or("", String::as_str);
        text.push_str(&format!(
            "{part},{},{},{},{},{},{edge}\n",
            arrival.arrival_us,
            arrival.bytes,
            arrival.published_us,
            arrival.available_us,
            arrival.kernel_us
        ));
    }
    text
}

#[derive(Debug, Serialize)]
struct Report {
    schema: &'static str,
    stream_id: u64,
    duration_seconds: u64,
    caches: Vec<String>,
    moves: Vec<MoveRecord>,
    /// Moves back to the home region after a failover.
    home_returns: u64,
    first_part: Option<u64>,
    last_part: Option<u64>,
    expected_parts: u64,
    received_parts: u64,
    gap_parts: u64,
    missing_parts: u64,
    missing_examples: Vec<u64>,
    duplicate_parts: u64,
    init_objects: u64,
    datagrams: u64,
    source_datagrams: u64,
    repair_datagrams: u64,
    recovered_parts: u64,
    decode_errors: u64,
    lost_objects: u64,
    sent_datagrams_settled: u64,
    lost_datagrams_settled: u64,
    datagram_loss: Option<f64>,
    loss_estimate_max: Option<f64>,
    final_repair_permille: u32,
    final_min_repair: u32,
    repair_changes: u64,
    final_loss_ppm: u32,
    flows: u64,
    payload_bytes: u64,
    /// Bytes of the media datagrams received from every leg, with 28 bytes
    /// of IPv4 and UDP header each, and their rate over the parts' time.
    wire_bytes: u64,
    wire_bits_per_second: Option<f64>,
    /// Wire bytes over the parts' own bytes.
    wire_per_payload: Option<f64>,
    compact_asked: bool,
    compact_fallbacks: u64,
    compact_short_header_dropped: u64,
    /// The shortest round trip of the legs held at the end.
    nack_rtt_ms: Option<f64>,
    nack_timing: String,
    part_interval_ms: Option<f64>,
    compact_confirmed: bool,
    final_compact_plan: Option<String>,
    compact_plan_changes: u64,
    burst_estimate_max: Option<f64>,
    compact: CompactReport,
    fast_lane_asked: bool,
    fast_lane_parts: u64,
    compact_fast_lane_parts: u64,
    cache_lane_parts: u64,
    fast_envelope_bytes: Percentiles,
    fast_envelope_overhead_bytes: Percentiles,
    fast_epoch_changes: u64,
    verify: Option<VerifyReport>,
    publish_to_arrival_ms: Percentiles,
    /// Each datagram's kernel receive time -> the client read it: the
    /// listener host's own delay (scheduling, a busy CPU).
    kernel_to_arrival_ms: Percentiles,
    available_to_arrival_ms: Percentiles,
    time_to_first_part_ms: Option<f64>,
    startup: Startup,
    // v2 (B 2.8).
    paths: u8,
    spare: Option<String>,
    cpu_seconds: f64,
    legs: BTreeMap<String, LegFigures>,
    replacements: Vec<ReplacementRecord>,
    single_path_ms: f64,
    single_path_spans: Vec<[u64; 2]>,
    hedges: u64,
    hedges_released: u64,
    longest_silence_ms: f64,
    interruptions: u64,
    wire_bytes_per_part: Option<f64>,
    before_start: u64,
    stale_start: u64,
    cross_leg_mismatch: u64,
    short_grants: ShortGrantCounts,
}

#[derive(Debug, Default, Serialize)]
struct CompactReport {
    datagrams: u64,
    parts: u64,
    gaps: u64,
    duplicates: u64,
    retransmits: u64,
    repair_datagrams: u64,
    recovered_by_window: u64,
    nack_parts_sent: u64,
    nack_datagrams_sent: u64,
    holes_abandoned: u64,
    errors: u64,
}

#[derive(Debug, Default, Serialize)]
struct VerifyReport {
    checked: u64,
    identical: u64,
    different: u64,
    http_errors: u64,
    different_examples: Vec<u64>,
    error_examples: Vec<String>,
}

struct VerifyJob {
    authority: String,
    path: String,
    part: u64,
    payload: Bytes,
}

/// A part waiting for the parts' extension before it can be verified.
struct VerifyWaiting {
    node: String,
    part: u64,
    payload: Bytes,
}

/// The process's user and system time, in seconds.
fn cpu_seconds() -> f64 {
    // SAFETY: getrusage writes one rusage to the pointer given.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return 0.0;
    }
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1e6;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

fn set_receive_buffer(socket: &UdpSocket, bytes: usize) {
    use std::os::fd::AsRawFd;
    let value = bytes as libc::c_int;
    // SO_RCVBUFFORCE needs CAP_NET_ADMIN; SO_RCVBUF is capped by rmem_max.
    for option in [libc::SO_RCVBUFFORCE, libc::SO_RCVBUF] {
        let result = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                option,
                (&value as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if result == 0 {
            return;
        }
    }
}

/// Ask for the kernel's receive time of every datagram (SO_TIMESTAMPNS).
fn enable_kernel_timestamps(socket: &UdpSocket) {
    use std::os::fd::AsRawFd;
    let on: libc::c_int = 1;
    // SAFETY: a valid fd and an int option of its size.
    let _ = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            (&on as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
}

/// Ask the kernel to keep a sender's run of equal datagrams together
/// (UDP_GRO); a receive then says the size of each (see `recv_timestamped`).
fn enable_gro(socket: &UdpSocket) {
    use std::os::fd::AsRawFd;
    let on: libc::c_int = 1;
    // SAFETY: a valid fd and an int option of its size.
    let _ = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_UDP,
            libc::UDP_GRO,
            (&on as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
}

/// Receive one message, with its sender, the kernel's receive time (Unix
/// µs) when the socket gives one, and with UDP GRO the size of each of the
/// datagrams it carries (all but the last; None: one datagram).
/// Whether a datagram waits on the socket (FIONREAD).
fn datagrams_queued(socket: &UdpSocket) -> bool {
    use std::os::fd::AsRawFd;
    let mut queued: libc::c_int = 0;
    // SAFETY: FIONREAD writes one int to the pointer given.
    let result = unsafe { libc::ioctl(socket.as_raw_fd(), libc::FIONREAD, &mut queued) };
    result == 0 && queued > 0
}

async fn recv_timestamped(
    socket: &UdpSocket,
    buf: &mut [u8],
) -> std::io::Result<(usize, SocketAddr, Option<u64>, Option<usize>)> {
    use std::os::fd::AsRawFd;
    socket
        .async_io(tokio::io::Interest::READABLE, || {
            let mut address: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            let mut control = [0_u64; 16];
            let mut iov = libc::iovec {
                iov_base: buf.as_mut_ptr().cast(),
                iov_len: buf.len(),
            };
            // SAFETY: every pointer is to a buffer that lives for the call.
            let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
            message.msg_name = std::ptr::from_mut(&mut address).cast();
            message.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            message.msg_iov = &mut iov;
            message.msg_iovlen = 1;
            message.msg_control = control.as_mut_ptr().cast();
            message.msg_controllen = std::mem::size_of_val(&control) as _;
            let len =
                unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, libc::MSG_DONTWAIT) };
            if len < 0 {
                return Err(std::io::Error::last_os_error());
            }
            let mut kernel_us = None;
            let mut segment = None;
            // SAFETY: the kernel filled msg_control with well-formed cmsg
            // headers up to msg_controllen; CMSG_* walk only inside it.
            unsafe {
                let mut cmsg = libc::CMSG_FIRSTHDR(&message);
                while !cmsg.is_null() {
                    if (*cmsg).cmsg_level == libc::SOL_SOCKET
                        && (*cmsg).cmsg_type == libc::SCM_TIMESTAMPNS
                    {
                        let time = std::ptr::read_unaligned(
                            libc::CMSG_DATA(cmsg).cast::<libc::timespec>(),
                        );
                        kernel_us = Some(
                            (time.tv_sec as u64)
                                .saturating_mul(1_000_000)
                                .saturating_add(time.tv_nsec as u64 / 1_000),
                        );
                    } else if (*cmsg).cmsg_level == libc::SOL_UDP
                        && (*cmsg).cmsg_type == libc::UDP_GRO
                    {
                        let size =
                            std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<libc::c_int>());
                        segment = usize::try_from(size).ok();
                    }
                    cmsg = libc::CMSG_NXTHDR(&message, cmsg);
                }
            }
            // SAFETY: the kernel wrote an address of msg_namelen bytes.
            let peer = unsafe {
                socket2::SockAddr::try_init(|target, target_len| {
                    std::ptr::copy_nonoverlapping(
                        std::ptr::from_ref(&address).cast::<u8>(),
                        target.cast::<u8>(),
                        message.msg_namelen as usize,
                    );
                    *target_len = message.msg_namelen;
                    Ok(())
                })
            }?
            .1
            .as_socket()
            .ok_or_else(|| std::io::Error::other("a datagram from no IP address"))?;
            Ok((len as usize, peer, kernel_us, segment))
        })
        .await
}

/// Perform the director's actions: datagrams from the media socket, HTTPS
/// requests and name lookups on tasks of their own, answered on `done`.
async fn perform(
    socket: &UdpSocket,
    actions: &mut Vec<Action>,
    http: &Http,
    done: &mpsc::UnboundedSender<Done>,
) {
    for action in actions.drain(..) {
        match action {
            // A failed send is a lost request: the director repeats it.
            Action::Send { to, datagram } => {
                let _ = socket.send_to(&datagram, to).await;
            }
            Action::Https { id, entry, path } => {
                let (http, done) = (http.clone(), done.clone());
                tokio::spawn(async move {
                    let body = match Url::parse(&entry) {
                        Ok(url) => http
                            .get(&url.authority, &path)
                            .await
                            .ok()
                            .map(|(_, body)| body),
                        Err(_) => None,
                    };
                    let _ = done.send(Done::Https { id, body });
                });
            }
            Action::Resolve { host } => {
                let done = done.clone();
                tokio::spawn(async move {
                    let ip = tokio::net::lookup_host((host.as_str(), 0))
                        .await
                        .ok()
                        .and_then(|mut addresses| addresses.next())
                        .map(|address| address.ip());
                    let _ = done.send(Done::Resolved { host, ip });
                });
            }
        }
    }
}

async fn flush(socket: &UdpSocket, outbox: &mut Vec<(SocketAddr, Vec<u8>)>) {
    for (to, datagram) in outbox.drain(..) {
        // A failed send is a lost request: the next refresh repeats it.
        let _ = socket.send_to(&datagram, to).await;
    }
}

/// The leg of the director's whose edge sent from `peer`: the leg at
/// exactly that address, else the only leg at that IP (B 3.1, as the
/// session attributes it).
fn leg_at(director: &Director, peer: SocketAddr) -> Option<LegKey> {
    let legs = director.legs();
    if let Some(leg) = legs.iter().find(|leg| leg.addr == peer) {
        return Some(leg.key);
    }
    let mut same_ip = legs.iter().filter(|leg| leg.addr.ip() == peer.ip());
    match (same_ip.next(), same_ip.next()) {
        (Some(leg), None) => Some(leg.key),
        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let http = Http {
        connector: tls_connector(&args)?,
        server_name: args.server_name.clone(),
        session: args.session.clone(),
        connections: Arc::default(),
    };
    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration_seconds);
    let socket = UdpSocket::bind(args.bind.unwrap_or_else(|| "0.0.0.0:0".parse().unwrap())).await?;
    set_receive_buffer(&socket, args.receive_buffer_bytes);
    enable_kernel_timestamps(&socket);
    if args.gro {
        enable_gro(&socket);
    }

    // Verification runs beside reception, with bounded concurrency.
    let (verify_tx, mut verify_rx) = mpsc::unbounded_channel::<VerifyJob>();
    let verify_http = http.clone();
    let verifier = tokio::spawn(async move {
        let limit = Arc::new(Semaphore::new(32));
        let mut handles = Vec::new();
        while let Some(job) = verify_rx.recv().await {
            let permit = Arc::clone(&limit).acquire_owned().await.unwrap();
            let http = verify_http.clone();
            handles.push(tokio::spawn(async move {
                let _permit = permit;
                let result = http.get(&job.authority, &job.path).await;
                (job.part, job.payload, result)
            }));
        }
        let mut report = VerifyReport::default();
        for handle in handles {
            let Ok((part, payload, result)) = handle.await else {
                continue;
            };
            report.checked += 1;
            match result {
                Ok((StatusCode::OK, body)) if body == payload => report.identical += 1,
                Ok((StatusCode::OK, _)) => {
                    report.different += 1;
                    if report.different_examples.len() < 10 {
                        report.different_examples.push(part);
                    }
                }
                Ok((status, _)) => {
                    report.http_errors += 1;
                    if report.error_examples.len() < 10 {
                        report
                            .error_examples
                            .push(format!("part {part}: HTTP {status}"));
                    }
                }
                Err(error) => {
                    report.http_errors += 1;
                    if report.error_examples.len() < 10 {
                        report.error_examples.push(format!("part {part}: {error}"));
                    }
                }
            }
        }
        report
    });

    // The legs (av_mesh::client_session) and the edge-choosing code
    // (av_mesh::client_director), as needletail-load runs them.
    let mut session = UdpSession::new(args.session_config(), Instant::now());
    let mut nonce_seed = [0_u8; 8];
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut nonce_seed)
        .map_err(|_| anyhow!("no random nonce seed"))?;
    let mut director = Director::new(
        args.director_config(u64::from_be_bytes(nonce_seed)),
        Instant::now(),
    );
    let mut logs: Vec<LegLog> = Vec::new();
    let mut actions: Vec<Action> = Vec::new();
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<Done>();
    // The control datagrams that the session hands out, sent in order.
    let mut outbox: Vec<(SocketAddr, Vec<u8>)> = Vec::new();

    let mut received: BTreeMap<u64, usize> = BTreeMap::new();
    let mut gaps: BTreeSet<u64> = BTreeSet::new();
    let mut duplicates = 0_u64;
    let mut init_objects = 0_u64;
    let mut payload_bytes = 0_u64;
    let mut fast_lane_parts = 0_u64;
    let mut compact_fast_lane_parts = 0_u64;
    let mut cache_lane_parts = 0_u64;
    let mut fast_envelope_bytes = Vec::new();
    let mut fast_envelope_overhead = Vec::new();
    let mut publish_ms = Vec::new();
    // Datagram at the kernel -> at the client (the listener host's delay).
    let mut host_ms = Vec::new();
    let mut available_ms = Vec::new();
    let mut first_part_ms = None;
    let mut startup = Startup::default();
    let since_start = |started: Instant| started.elapsed().as_secs_f64() * 1_000.0;
    let mut parts_out = match &args.write_parts {
        Some(path) => Some(std::fs::File::create(path)?),
        None => None,
    };
    let mut written_parts: BTreeMap<u64, Bytes> = BTreeMap::new();
    let mut arrivals: BTreeMap<u64, Arrival> = BTreeMap::new();
    // The node names the arrivals and the time on one path refer to.
    let mut nodes: Vec<String> = Vec::new();
    let mut single_path = SinglePath::default();
    // The last first arrival (kernel time where the host gives it), the
    // longest gap between two, and the gaps of INTERRUPTION or more.
    let mut last_first_us: Option<u64> = None;
    let mut longest_silence_us = 0_u64;
    let mut interruptions = 0_u64;
    // Parts delivered whose lag on each leg is read once settled.
    let mut lag_due: VecDeque<u64> = VecDeque::with_capacity(256);
    // The parts' extension and the playlists asked for (--verify-http,
    // --playlist-first), and the parts that wait for the extension.
    let read_playlists = args.verify_http || args.playlist_first;
    let mut playlists_asked: BTreeSet<String> = BTreeSet::new();
    let mut extension: Option<String> = None;
    let mut verify_waiting: Vec<VerifyWaiting> = Vec::new();
    let mut logs_seen = 0_usize;
    let mut buf = vec![0_u8; 65_536];
    let mut first_published_us: Option<u64> = None;
    let mut last_published_us: Option<u64> = None;
    // The end of the run: the last part taken, and the holes before it
    // that the drain waits for.
    let drain = Duration::from_millis(args.drain_ms);
    let mut drain_last: Option<u64> = None;
    let mut drain_missing: BTreeSet<u64> = BTreeSet::new();
    // The datagrams of a GRO receive still to read: (next, end, size,
    // sender, kernel time) in `buf`.
    let mut pending: Option<(usize, usize, usize, SocketAddr, Option<u64>)> = None;

    director.poll(
        &mut Recorded {
            session: &mut session,
            logs: &mut logs,
            nodes: &mut nodes,
        },
        Instant::now(),
        &mut actions,
    );
    perform(&socket, &mut actions, &http, &done_tx).await;

    // At the end: the spare, and what the main leg asked for, read before
    // the legs are let go.
    let (spare_at_end, (repair, plan, compact_asked, compact_confirmed)) = loop {
        let now = Instant::now();
        if now >= deadline && drain_last.is_none() && session.is_subscribed() && !drain.is_zero() {
            let last = received.keys().chain(gaps.iter()).max().copied();
            let first = received.keys().chain(gaps.iter()).min().copied();
            if let (Some(first), Some(last)) = (first, last) {
                drain_last = Some(last);
                drain_missing = (first..=last)
                    .filter(|part| !received.contains_key(part) && !gaps.contains(part))
                    .collect();
            }
        }
        let draining = drain_last.is_some() && !drain_missing.is_empty() && now < deadline + drain;
        // The drain waits for NACK answers: no verdict on a silence then.
        session.hold = draining;
        if now >= deadline && !draining {
            let spare = director.spare().map(str::to_owned);
            let main_leg = (
                session.repair(),
                session.plan(),
                session.compact_asked(),
                session.compact_confirmed(),
            );
            director.leave(
                &mut Recorded {
                    session: &mut session,
                    logs: &mut logs,
                    nodes: &mut nodes,
                },
                now,
                &mut actions,
            );
            perform(&socket, &mut actions, &http, &done_tx).await;
            break (spare, main_leg);
        }

        // The timers: the session's on an empty socket only for its
        // verdicts (B 3.1), then the director's.
        session.queued = args.nack_after_drain && (pending.is_some() || datagrams_queued(&socket));
        session.poll_legs(now, &mut |to, datagram| {
            outbox.push((to, datagram.to_vec()))
        });
        flush(&socket, &mut outbox).await;
        director.poll(
            &mut Recorded {
                session: &mut session,
                logs: &mut logs,
                nodes: &mut nodes,
            },
            now,
            &mut actions,
        );
        perform(&socket, &mut actions, &http, &done_tx).await;
        // The time on one path, judged from each held leg's last media.
        let interval = session.receiver.compact.part_interval().unwrap_or_default();
        let window = DELIVERING_FLOOR.max(interval * DELIVERING_INTERVALS);
        single_path.observe(
            now,
            window,
            director.legs().iter().filter_map(|leg| {
                logs.iter()
                    .rev()
                    .find(|log| log.key == leg.key)
                    .and_then(|log| log.last_media)
            }),
            director.want() >= 2,
        );
        // Legs added since: their playlists, and their names.
        for log in &logs[logs_seen..] {
            startup
                .first_request_ms
                .get_or_insert(ms_of(log.added.saturating_duration_since(started)));
            if read_playlists && playlists_asked.insert(log.node.clone()) {
                if let Some(url) = director
                    .cache_url(&log.node)
                    .and_then(|url| Url::parse(url).ok())
                {
                    let (http, done, node) = (http.clone(), done_tx.clone(), log.node.clone());
                    let path = format!("{}/{}/stream.m3u8", url.path, args.stream);
                    tokio::spawn(async move {
                        let answer = match http.get(&url.authority, &path).await {
                            Ok((StatusCode::OK, body)) => {
                                Ok(playlist_parts(&String::from_utf8_lossy(&body)).1)
                            }
                            Ok((status, _)) => Err(format!("playlist answered {status}")),
                            Err(error) => Err(format!("playlist failed: {error:#}")),
                        };
                        let _ = done.send(Done::Playlist { node, answer });
                    });
                }
            }
        }
        logs_seen = logs.len();

        let end = if draining { deadline + drain } else { deadline };
        let wake = end.min(session.next_wake()).min(director.next_wake());
        let (start, len, peer, kernel_us) =
            if let Some((next, end, size, peer, kernel)) = pending.take() {
                let len = size.min(end - next);
                if next + len < end {
                    pending = Some((next + len, end, size, peer, kernel));
                }
                (next, len, peer, kernel)
            } else {
                tokio::select! {
                    biased;
                    received = recv_timestamped(&socket, &mut buf) => match received {
                        Ok((len, peer, kernel_us, Some(size))) if size > 0 && size < len => {
                            pending = Some((size, len, size, peer, kernel_us));
                            (0, size, peer, kernel_us)
                        }
                        Ok((len, peer, kernel_us, _)) => (0, len, peer, kernel_us),
                        Err(_) => continue,
                    },
                    Some(done) = done_rx.recv() => {
                        let now = Instant::now();
                        let mut legs = Recorded {
                            session: &mut session,
                            logs: &mut logs,
                            nodes: &mut nodes,
                        };
                        match done {
                            Done::Https { id, body } => {
                                director.on_https(&mut legs, id, body.as_deref(), now, &mut actions)
                            }
                            Done::Resolved { host, ip } => {
                                director.on_resolved(&mut legs, &host, ip, now, &mut actions)
                            }
                            Done::Playlist { node, answer } => match answer {
                                Ok(found) => {
                                    startup.playlist_ms.get_or_insert(since_start(started));
                                    extension.get_or_insert(found);
                                }
                                Err(failure) => eprintln!("{node}: {failure}"),
                            },
                        }
                        perform(&socket, &mut actions, &http, &done_tx).await;
                        continue;
                    }
                    () = tokio::time::sleep_until(wake.into()) => continue,
                }
            };
        let datagram = &buf[start..start + len];
        let now = Instant::now();
        // An assignment, or a token of an entry or of the spare: the
        // director's.
        if director.on_datagram(
            &mut Recorded {
                session: &mut session,
                logs: &mut logs,
                nodes: &mut nodes,
            },
            peer,
            datagram,
            now,
            &mut actions,
        ) {
            perform(&socket, &mut actions, &http, &done_tx).await;
            continue;
        }
        let outcome = session.on_datagram(peer, datagram, now, &mut |to, datagram| {
            outbox.push((to, datagram.to_vec()))
        });
        flush(&socket, &mut outbox).await;
        match outcome {
            Received::Media => {}
            // The OK that the parts before it waited for: taken now.
            Received::Subscribed { .. } if session.has_objects() => {}
            _ => continue,
        }
        // The leg whose edge sent it: its last media, and the edge of the
        // parts it brings first.
        let leg = leg_at(&director, peer);
        let mut edge = None;
        if let Some(log) = leg.and_then(|key| logs.iter_mut().rev().find(|log| log.key == key)) {
            if outcome == Received::Media {
                log.last_media = Some(now);
            }
            edge = Some(log.edge);
        }
        let fast_envelope = session.receiver.last_fast_envelope_bytes;
        let compact_fast = std::mem::take(&mut session.receiver.last_compact_fast_lane);
        let arrival_us = now_unix_us();
        let kernel_us = kernel_us.filter(|kernel| *kernel <= arrival_us);
        if let Some(kernel) = kernel_us {
            host_ms.push(arrival_us.saturating_sub(kernel) as f64 / 1_000.0);
        }
        while let Some(object) = session.next_object() {
            let PartObject {
                kind,
                part,
                payload,
                published_unix_us,
                available_unix_us,
                ..
            } = object;
            match kind {
                ObjectKind::Init => init_objects += 1,
                // Past the end of the run: not taken.
                ObjectKind::Gap | ObjectKind::Part
                    if drain_last.is_some_and(|last| part > last) => {}
                ObjectKind::Gap => {
                    drain_missing.remove(&part);
                    gaps.insert(part);
                }
                ObjectKind::Part => {
                    drain_missing.remove(&part);
                    if received.contains_key(&part) {
                        duplicates += 1;
                        continue;
                    }
                    // A part that came after its gap (the edge got it
                    // late) is received, not a gap.
                    gaps.remove(&part);
                    if first_part_ms.is_none() {
                        first_part_ms = Some(since_start(started));
                        startup.first_part_ms = first_part_ms;
                        startup.first_part_after_named = session
                            .first_subscribed_part
                            .map(|named| part.saturating_sub(named));
                    }
                    let at_us = kernel_us.unwrap_or(arrival_us);
                    if let Some(last) = last_first_us {
                        let gap = at_us.saturating_sub(last);
                        longest_silence_us = longest_silence_us.max(gap);
                        if gap >= INTERRUPTION.as_micros() as u64 {
                            interruptions += 1;
                        }
                    }
                    last_first_us = Some(last_first_us.map_or(at_us, |last| last.max(at_us)));
                    if lag_due.len() == lag_due.capacity() {
                        lag_due.pop_front();
                    }
                    lag_due.push_back(part);
                    match fast_envelope {
                        Some(envelope) => {
                            fast_lane_parts += 1;
                            fast_envelope_bytes.push(envelope as f64);
                            fast_envelope_overhead
                                .push(envelope.saturating_sub(payload.len()) as f64);
                        }
                        // A compact datagram of the edge's fast lane.
                        None if compact_fast.contains(&part) => {
                            fast_lane_parts += 1;
                            compact_fast_lane_parts += 1;
                        }
                        None => cache_lane_parts += 1,
                    }
                    received.insert(part, payload.len());
                    payload_bytes += payload.len() as u64;
                    if published_unix_us > 0 {
                        first_published_us = Some(
                            first_published_us
                                .map_or(published_unix_us, |first| first.min(published_unix_us)),
                        );
                        last_published_us = Some(
                            last_published_us
                                .map_or(published_unix_us, |last| last.max(published_unix_us)),
                        );
                        publish_ms
                            .push(arrival_us.saturating_sub(published_unix_us) as f64 / 1_000.0);
                    }
                    if available_unix_us > 0 {
                        available_ms
                            .push(arrival_us.saturating_sub(available_unix_us) as f64 / 1_000.0);
                    }
                    if args.arrivals.is_some() {
                        arrivals.insert(
                            part,
                            Arrival {
                                arrival_us,
                                bytes: payload.len(),
                                published_us: published_unix_us,
                                available_us: available_unix_us,
                                kernel_us: kernel_us.unwrap_or(0),
                                edge,
                            },
                        );
                    }
                    if parts_out.is_some() {
                        written_parts.insert(part, payload.clone());
                    }
                    if args.verify_http {
                        if let Some(node) = edge.map(|index| nodes[index].clone()) {
                            verify_waiting.push(VerifyWaiting {
                                node,
                                part,
                                payload,
                            });
                        }
                    }
                }
            }
        }
        // Each leg's lag on the parts that every leg has had time to bring.
        if let Some(newest) = session.receiver.compact.newest() {
            while let Some(&part) = lag_due.front() {
                if newest < part + LAG_SETTLE_PARTS {
                    break;
                }
                lag_due.pop_front();
                if newest >= part + LAG_FORGET_PARTS {
                    continue;
                }
                for leg in director.legs() {
                    if let Some(lag) = session.leg_lag_of(leg.key, part) {
                        if let Some(log) = logs.iter_mut().rev().find(|log| log.key == leg.key) {
                            log.lags.record(lag);
                        }
                    }
                }
            }
        }
        // Parts to verify, once the parts' extension is known.
        if let Some(extension) = &extension {
            for waiting in verify_waiting.drain(..) {
                if let Some(url) = director
                    .cache_url(&waiting.node)
                    .and_then(|url| Url::parse(url).ok())
                {
                    let _ = verify_tx.send(VerifyJob {
                        path: format!(
                            "{}/{}/part{}.{extension}",
                            url.path, args.stream, waiting.part
                        ),
                        authority: url.authority,
                        part: waiting.part,
                        payload: waiting.payload,
                    });
                }
            }
        }
    };
    // The second UNSUB of each leg let go goes 20 ms after the first.
    tokio::time::sleep(Duration::from_millis(25)).await;
    session.poll_legs(Instant::now(), &mut |to, datagram| {
        outbox.push((to, datagram.to_vec()))
    });
    flush(&socket, &mut outbox).await;
    single_path.close(Instant::now());
    let since = |at: Option<Instant>| at.map(|at| ms_of(at.saturating_duration_since(started)));
    startup.token_ms = since(session.first_token_at);
    startup.subscribed_ms = since(session.first_subscribed_at);
    if let Some((at, via)) = director.first_answer {
        startup.assignment_ms = since(Some(at));
        startup.assignment_via = Some(via.name());
    }
    let receiver = &session.receiver;
    drop(verify_tx);
    let verify = verifier.await.ok();

    if let Some(path) = &args.arrivals {
        std::fs::write(path, arrivals_csv(&arrivals, &nodes))
            .with_context(|| format!("write {}", path.display()))?;
    }
    if let Some(file) = parts_out.as_mut() {
        use std::io::Write;
        for payload in written_parts.values() {
            file.write_all(payload)?;
        }
    }

    // Missing parts are counted from the session's floor: its first part
    // delivered (B 3.1).
    let first = received.keys().chain(gaps.iter()).min().copied();
    let last = received.keys().chain(gaps.iter()).max().copied();
    let expected = match (first, last) {
        (Some(first), Some(last)) => last - first + 1,
        _ => 0,
    };
    let missing: Vec<u64> = match (first, last) {
        (Some(first), Some(last)) => (first..=last)
            .filter(|part| !received.contains_key(part) && !gaps.contains(part))
            .collect(),
        _ => Vec::new(),
    };
    let span_s = match (first_published_us, last_published_us) {
        (Some(first), Some(last)) if last > first => Some((last - first) as f64 / 1e6),
        _ => None,
    };
    let stats = receiver.compact.stats;
    let legs = leg_figures(&logs);
    let nack_rtt_ms = legs
        .values()
        .filter_map(|figures| figures.rtt_ms)
        .min_by(f64::total_cmp);
    let (replacements, moves) = replacement_records(&director.replacements, &logs, started);
    let at_ms = |at: Instant| at.saturating_duration_since(started).as_millis() as u64;
    let report = Report {
        schema: "needletail.udp-client.v2",
        stream_id: args.stream,
        duration_seconds: args.duration_seconds,
        caches: logs.iter().map(|log| log.node.clone()).collect(),
        moves,
        home_returns: director.stats.home_returns,
        first_part: first,
        last_part: last,
        expected_parts: expected,
        received_parts: received.len() as u64,
        gap_parts: gaps.len() as u64,
        missing_parts: missing.len() as u64,
        missing_examples: missing.iter().take(20).copied().collect(),
        duplicate_parts: duplicates,
        init_objects,
        datagrams: receiver.datagrams,
        source_datagrams: receiver.source_datagrams,
        repair_datagrams: receiver.repair_datagrams,
        recovered_parts: receiver.recovered_objects,
        decode_errors: receiver.errors,
        lost_objects: receiver.lost_objects,
        sent_datagrams_settled: receiver.sent_datagrams,
        lost_datagrams_settled: receiver.lost_datagrams,
        datagram_loss: (receiver.sent_datagrams > 0)
            .then(|| receiver.lost_datagrams as f64 / receiver.sent_datagrams as f64),
        loss_estimate_max: session.loss_max,
        final_repair_permille: repair.permille,
        final_min_repair: repair.min_symbols,
        repair_changes: session.repair_changes,
        final_loss_ppm: repair.loss_ppm,
        flows: receiver.flows,
        payload_bytes,
        wire_bytes: receiver.wire_bytes,
        wire_bits_per_second: span_s.map(|span| receiver.wire_bytes as f64 * 8.0 / span),
        wire_per_payload: (payload_bytes > 0)
            .then(|| receiver.wire_bytes as f64 / payload_bytes as f64),
        compact_asked,
        compact_fallbacks: session.compact_fallbacks,
        compact_short_header_dropped: session.short_header_dropped,
        nack_rtt_ms,
        nack_timing: format!(
            "delay {} us, retry floor {} us, silence {}",
            args.nack_delay_us, args.nack_retry_floor_us, args.silence_nack
        ),
        part_interval_ms: receiver.compact.part_interval().map(ms_of),
        compact_confirmed,
        final_compact_plan: compact_asked.then(|| plan.word()),
        compact_plan_changes: session.plan_changes,
        burst_estimate_max: session.burst_max,
        compact: CompactReport {
            datagrams: stats.datagrams,
            parts: stats.parts,
            gaps: stats.gaps,
            duplicates: stats.duplicates,
            retransmits: stats.retransmits,
            repair_datagrams: stats.repair_datagrams,
            recovered_by_window: stats.recovered,
            nack_parts_sent: stats.nacked,
            nack_datagrams_sent: session.nack_datagrams,
            holes_abandoned: stats.abandoned,
            errors: stats.errors,
        },
        fast_lane_asked: args.fast_lane,
        fast_lane_parts,
        compact_fast_lane_parts,
        cache_lane_parts,
        fast_envelope_bytes: percentiles(fast_envelope_bytes),
        fast_envelope_overhead_bytes: percentiles(fast_envelope_overhead),
        fast_epoch_changes: receiver.fast_epoch_changes,
        verify: if args.verify_http { verify } else { None },
        publish_to_arrival_ms: percentiles(publish_ms),
        kernel_to_arrival_ms: percentiles(host_ms),
        available_to_arrival_ms: percentiles(available_ms),
        time_to_first_part_ms: first_part_ms,
        startup,
        paths: args.paths,
        spare: spare_at_end,
        cpu_seconds: cpu_seconds(),
        legs,
        replacements,
        single_path_ms: ms_of(single_path.total),
        single_path_spans: single_path
            .spans
            .iter()
            .map(|(from, to)| [at_ms(*from), at_ms(*to)])
            .collect(),
        hedges: director.stats.hedges,
        hedges_released: director.stats.hedges_released,
        longest_silence_ms: longest_silence_us as f64 / 1_000.0,
        interruptions,
        wire_bytes_per_part: (!received.is_empty())
            .then(|| receiver.wire_bytes as f64 / received.len() as f64),
        before_start: stats.before_start,
        stale_start: session.stale_starts,
        cross_leg_mismatch: stats.cross_leg_mismatch,
        short_grants: director.stats.short_grants.into(),
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use av_mesh::client_compact::{CompactPlan, LegStats};

    /// The referee's probe (needletail-compare's change steps): this
    /// command line must keep working unchanged.
    const PROBE: [&str; 17] = [
        "needletail-udp-client",
        "--entry",
        "https://lon-dist-a:19444",
        "--stream",
        "3001",
        "--tls-ca",
        "/ca.pem",
        "--server-name",
        "mesh.needletail.test",
        "--duration-seconds",
        "360",
        "--session",
        "probe-lon-1",
        "--arrivals",
        "/out/arrivals.csv",
        "--write-parts",
        "/out/parts.bin",
    ];

    /// C CR3: every binary of the crate names the build it came from
    /// (`--version`).
    #[test]
    fn the_version_names_the_build() {
        use clap::CommandFactory;
        assert_eq!(Args::command().get_version(), Some(av_mesh::build::VERSION));
    }

    /// B 2.4: entry points by a repeated flag or a list, and the probe's
    /// command line of before, which takes the two-path defaults.
    #[test]
    fn two_entries_parse() {
        let args = Args::try_parse_from([
            "needletail-udp-client",
            "--entry",
            "https://lon-dist-a:19444,https://lon-dist-b:19444",
            "--entry",
            "https://lon-dist-c:19444",
            "--stream",
            "3001",
            "--tls-ca",
            "/ca.pem",
        ])
        .unwrap();
        assert_eq!(
            args.entry,
            [
                "https://lon-dist-a:19444",
                "https://lon-dist-b:19444",
                "https://lon-dist-c:19444"
            ]
        );
        assert!(
            Args::try_parse_from(["needletail-udp-client", "--stream", "1", "--tls-ca", "/c"])
                .is_err()
        );
        let args = Args::try_parse_from(PROBE).unwrap();
        assert_eq!(args.entry, ["https://lon-dist-a:19444"]);
        assert_eq!(args.session, "probe-lon-1");
        assert_eq!(args.duration_seconds, 360);
        assert_eq!(args.server_name.as_deref(), Some("mesh.needletail.test"));
        assert_eq!(args.arrivals, Some(PathBuf::from("/out/arrivals.csv")));
        assert_eq!(args.write_parts, Some(PathBuf::from("/out/parts.bin")));
        // The defaults of B 2.4, on the director and the session.
        let director = args.director_config(7);
        assert_eq!((director.paths, director.spare), (2, true));
        assert_eq!(director.token_wait, Duration::from_millis(400));
        assert_eq!(director.rebalance_giveup, Duration::from_millis(3_000));
        assert_eq!(director.level_wait, Duration::from_millis(1_000));
        assert_eq!(director.entries, ["https://lon-dist-a:19444"]);
        let session = args.session_config();
        assert_eq!(session.silence, Duration::from_millis(200));
        assert_eq!(session.answer, Duration::from_millis(5_000));
        assert_eq!(session.hedge_after, Duration::from_millis(50));
        assert_eq!(session.lone_hedge_after, Duration::from_millis(170));
        assert_eq!(session.first_double, Duration::from_millis(3));
        assert_eq!(session.hole_give_up, Duration::from_millis(3_000));
        assert!((session.residual_target - 0.008).abs() < 1e-12);
        // The controls the referee names: one path, and the residual.
        let control = Args::try_parse_from(PROBE.into_iter().chain([
            "--paths",
            "1",
            "--spare",
            "false",
            "--compact-residual-ppm",
            "2000",
        ]))
        .unwrap();
        let director = control.director_config(7);
        assert_eq!((director.paths, director.spare), (1, false));
        assert!((control.session_config().residual_target - 0.002).abs() < 1e-12);
    }

    fn report(node: &str, datagrams: u64, first: u64) -> LegReport {
        LegReport {
            node: node.into(),
            target: "10.0.1.1:19446".parse().unwrap(),
            first_part: Some(first),
            compact: true,
            rtt: Some(Duration::from_micros(1_500)),
            lag: Some(Duration::from_micros(100)),
            loss: Some(0.01),
            plan: CompactPlan::CLEAN,
            datagrams,
            wire_bytes: datagrams * 91,
            longest_gap: Duration::from_millis(12),
            stats: LegStats {
                datagrams,
                parts_first: datagrams / 2,
                duplicates: datagrams / 2,
                before_start: 1,
            },
        }
    }

    /// B 2.8: the report names each leg (by edge, with its lag median and
    /// p99 and its longest gap), each replacement with its kind and times,
    /// the time on one path, the wire bytes a part and the short grants.
    #[test]
    fn the_report_names_each_leg_and_the_time_on_one_path() {
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        let log = |key: u32, node: &str, added: u64, lags_us: &[u64], report: LegReport| {
            let mut lags = LagHistogram::new();
            for lag in lags_us {
                lags.record(Duration::from_micros(*lag));
            }
            LegLog {
                key: LegKey(key),
                node: node.into(),
                edge: key as usize,
                added: ms(added),
                last_media: None,
                lags,
                report: Some(report),
            }
        };
        // e1 first on 99 parts and 1 ms behind on one; e2 always behind.
        let mut e1_lags = vec![0; 99];
        e1_lags.push(1_000);
        let logs = vec![
            log(0, "e1", 0, &e1_lags, report("e1", 1_000, 100)),
            log(1, "e2", 0, &[300; 100], report("e2", 1_000, 101)),
            log(2, "e3", 4_000, &[200; 10], report("e3", 400, 4_100)),
        ];
        let figures = leg_figures(&logs);
        assert_eq!(figures.keys().collect::<Vec<_>>(), ["e1", "e2", "e3"]);
        let e1 = &figures["e1"];
        assert_eq!(
            (e1.legs, e1.first_part, e1.datagrams),
            (1, Some(100), 1_000)
        );
        assert_eq!(e1.lag_ms.median, Some(0.01));
        assert_eq!(e1.lag_ms.p99, Some(0.01));
        assert_eq!(figures["e2"].lag_ms.p99, Some(0.31));
        assert_eq!(figures["e1"].longest_gap_ms, 12.0);
        let replacements = vec![
            Replacement {
                at: ms(3_950),
                from: "e2".into(),
                to: "e3".into(),
                reason: "silence".into(),
                kind: ReplacementKind::Hedge,
                handover: None,
                exposure: Some(Duration::from_millis(62)),
            },
            Replacement {
                at: ms(9_000),
                from: "e1".into(),
                to: "e1".into(),
                reason: "rebalance".into(),
                kind: ReplacementKind::Takeback,
                handover: None,
                exposure: None,
            },
        ];
        let (records, moves) = replacement_records(&replacements, &logs, t0);
        let json = serde_json::to_value(&records).unwrap();
        assert_eq!(json[0]["kind"], "hedge");
        assert_eq!(json[0]["exposure_ms"], 62.0);
        assert!(json[0].get("handover_ms").is_none());
        assert_eq!(json[1]["kind"], "takeback");
        // v1's moves: one per leg replaced, take-backs left out.
        assert_eq!(moves.len(), 1);
        assert_eq!((moves[0].at_ms, moves[0].next_part), (3_950, None));
        let mut path = SinglePath::default();
        let window = Duration::from_millis(20);
        path.observe(ms(100), window, [ms(100), ms(100)], true);
        path.observe(ms(3_960), window, [ms(3_960), ms(3_900)], true);
        path.observe(ms(3_982), window, [ms(3_982), ms(3_900), ms(3_982)], true);
        assert_eq!(path.spans, [(ms(3_920), ms(3_982))]);
        let counts = ShortGrantCounts::from(ShortGrants {
            one_edge: 1,
            bitrate: 0,
            gate: 2,
            unexplained: 0,
        });
        let json = serde_json::to_value(&counts).unwrap();
        assert_eq!(json["one-edge"], 1);
        assert_eq!(json["gate"], 2);
        let figures = serde_json::to_value(&figures).unwrap();
        for key in [
            "first_part",
            "parts_first",
            "duplicates",
            "datagrams",
            "wire_bytes",
            "loss",
            "rtt_ms",
            "lag_ms",
            "longest_gap_ms",
            "plan",
        ] {
            assert!(figures["e1"].get(key).is_some(), "{key}");
        }
    }

    /// B 2.8: the arrivals file keeps v1's six columns first and adds the
    /// edge of each first arrival. The referee's `ntcmp udp-ingest`
    /// (needletail-compare/tools/ntcmp/udp.go) skips the first line, splits
    /// each other line at commas and reads fields 0 to 2 by position (part,
    /// arrival, bytes) for lines with three or more; the lab's scripts read
    /// columns by name. A seventh column breaks neither.
    #[test]
    fn the_arrivals_file_keeps_the_referees_columns_first() {
        let mut arrivals = BTreeMap::new();
        for (part, edge) in [(100_u64, Some(1)), (101, Some(0)), (102, None)] {
            arrivals.insert(
                part,
                Arrival {
                    arrival_us: 1_000 + part,
                    bytes: 56,
                    published_us: 900 + part,
                    available_us: 950 + part,
                    kernel_us: 999 + part,
                    edge,
                },
            );
        }
        let nodes = vec!["lon-edge-1".to_owned(), "lon-edge-2".to_owned()];
        let text = arrivals_csv(&arrivals, &nodes);
        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(
            lines[0],
            "part,arrival_unix_us,bytes,published_unix_us,available_unix_us,kernel_unix_us,edge"
        );
        assert_eq!(lines[1], "100,1100,56,1000,1050,1099,lon-edge-2");
        assert_eq!(lines[3], "102,1102,56,1002,1052,1101,");
        // As ntcmp reads it.
        for (line, part) in lines[1..].iter().zip([100_i64, 101, 102]) {
            let fields: Vec<&str> = line.split(',').collect();
            assert!(fields.len() >= 3);
            assert_eq!(fields[0].parse::<i64>().unwrap(), part);
            assert_eq!(fields[1].parse::<i64>().unwrap(), 1_000 + part);
            assert_eq!(fields[2].parse::<i64>().unwrap(), 56);
        }
    }

    /// The lag histogram's quantiles are bucket edges; past the last
    /// bucket, the maximum.
    #[test]
    fn a_legs_lag_percentiles_come_from_every_part_it_brought() {
        let mut lags = LagHistogram::new();
        assert_eq!(lags.quantile_ms(0.99), None);
        for _ in 0..98 {
            lags.record(Duration::from_micros(5));
        }
        lags.record(Duration::from_micros(2_345));
        lags.record(Duration::from_millis(250));
        assert_eq!(lags.quantile_ms(0.5), Some(0.01));
        assert_eq!(lags.quantile_ms(0.99), Some(2.35));
        assert_eq!(lags.quantile_ms(1.0), Some(250.0));
    }

    /// Wave 9 D: with UDP GRO a sender's GSO run comes as one receive that
    /// names its datagrams' size; without it, one datagram per receive. GRO
    /// is on by default.
    #[tokio::test]
    async fn a_gro_receive_names_the_size_of_its_datagrams() {
        use std::os::fd::AsRawFd;
        let args = Args::parse_from([
            "needletail-udp-client",
            "--entry",
            "https://127.0.0.1:19444",
            "--stream",
            "1",
            "--tls-ca",
            "/nonexistent",
        ]);
        assert!(args.gro);
        for gro in [true, false] {
            let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            enable_kernel_timestamps(&receiver);
            if gro {
                enable_gro(&receiver);
            }
            let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let size: libc::c_int = 1_316;
            // SAFETY: a valid fd and an int option of its size.
            let set = unsafe {
                libc::setsockopt(
                    sender.as_raw_fd(),
                    libc::SOL_UDP,
                    libc::UDP_SEGMENT,
                    (&size as *const libc::c_int).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            if set != 0 {
                eprintln!("UDP GSO is not available here");
                return;
            }
            // Ten datagrams of 1,316 bytes and a last one of 500.
            let payload: Vec<u8> = (0..13_660_u32).map(|byte| byte as u8).collect();
            sender
                .send_to(&payload, receiver.local_addr().unwrap())
                .unwrap();
            let mut buf = vec![0_u8; 65_536];
            let mut got = Vec::new();
            while got.len() < payload.len() {
                let (len, peer, kernel_us, segment) = tokio::time::timeout(
                    Duration::from_secs(2),
                    recv_timestamped(&receiver, &mut buf),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(peer, sender.local_addr().unwrap());
                assert!(kernel_us.is_some());
                if gro {
                    assert_eq!((len, segment), (13_660, Some(1_316)));
                } else {
                    assert!(len == 1_316 || len == 500, "{len}");
                    assert_eq!(segment, None);
                }
                got.extend_from_slice(&buf[..len]);
            }
            assert_eq!(got, payload);
        }
    }

    /// The playlist is not read unless asked: the legs are judged by the
    /// UDP lane alone (B 2.4).
    #[test]
    fn the_playlist_is_read_only_when_asked() {
        let args = Args::try_parse_from(PROBE).unwrap();
        assert!(!args.playlist_first && !args.verify_http);
        let args =
            Args::try_parse_from(PROBE.into_iter().chain(["--playlist-first", "true"])).unwrap();
        assert!(args.playlist_first);
    }

    #[test]
    fn the_report_names_each_step_of_the_start() {
        let startup = Startup {
            assignment_ms: Some(3.0),
            assignment_via: Some("udp"),
            playlist_ms: None,
            first_request_ms: Some(3.1),
            token_ms: Some(3.6),
            subscribed_ms: Some(4.2),
            first_part_ms: Some(4.4),
            first_part_after_named: Some(0),
        };
        let json = serde_json::to_value(&startup).unwrap();
        for key in [
            "assignment_ms",
            "playlist_ms",
            "first_request_ms",
            "token_ms",
            "subscribed_ms",
            "first_part_ms",
            "first_part_after_named",
        ] {
            assert!(json.get(key).is_some(), "{key}");
        }
    }
}
