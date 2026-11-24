//! An LL-HLS reader that follows the assignment of a Needletail entry point.
//!
//! The reader asks an entry point for the Multivariant Playlist of a stream.
//! The playlist names the cache that the reader is assigned to and the caches
//! that follow it. The reader then reads the parts of the media playlist from
//! the assigned cache, one request for each part, as a player does.
//!
//! A part request waits at the cache until the part exists. A reader on a
//! path with delay therefore asks for several parts ahead (`--pipeline`):
//! with one request at a time it receives one part for each round trip, and
//! a round trip is longer than a 5 ms part on any real network. By default
//! the reader measures the round trip and asks far enough ahead to cover it.
//!
//! It moves to another cache in three cases, and records each move:
//!
//! - the cache fails: a request times out, the connection breaks, or the part
//!   is not there. The reader uses the next cache of its list. A cache that
//!   has sent the reader parts and then sends nothing for half the stall
//!   time (`--stall-ms`) is asked for its playlist on the connection the
//!   reader has; with no answer by the stall time the reader leaves it.
//! - the cache drains: a playlist reload is answered with a redirect.
//! - the entry point steers it: the Content Steering manifest puts another
//!   cache first.
//!
//! The report says, for each reader, which cache served each part, every
//! move, and what a player with the given buffer would have shown: each
//! stall and its length, the time to the first part, and the time from the
//! moment the contributor published a part to its arrival. A run with
//! `--readers N` is N sessions.
//!
//! `--read-kbps` and `--freeze-after-seconds` make a reader that reads
//! slowly, or that stops reading and keeps its connections open.

use anyhow::{anyhow, bail, Context, Result};
use bytes::{Bytes, BytesMut};
use clap::Parser;
use http::{header::HeaderValue, Request, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use playlists::m3u8_manifest::parse_seconds_us;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Least time of parts that a reader asks for at one time by default.
const DEFAULT_MIN_INFLIGHT_MS: u64 = 250;
/// Most part requests on their way at one time by default: 250 ms of
/// 0.25 ms parts, and a little more.
const DEFAULT_PIPELINE_LIMIT: usize = 1_024;
/// Requests that one HTTP/2 connection carries at one time by default: the
/// concurrent streams that an av-mesh cache allows on a connection
/// (web-service H2_MAX_CONCURRENT_STREAMS).
const DEFAULT_STREAMS_PER_CONNECTION: usize = 256;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "needletail-reader",
    version = av_mesh::build::VERSION,
    about = "Read an LL-HLS stream through a Needletail entry point and report continuity"
)]
struct Args {
    /// Entry point, for example https://10.77.1.21:19444. Repeat to name
    /// more entry points of the region. They are tried in order.
    #[arg(long = "entry", required = true)]
    entries: Vec<String>,

    #[arg(long, default_value_t = 1)]
    stream: u64,

    /// Session identifier. With --readers above one, each reader appends its
    /// index.
    #[arg(long, default_value = "reader")]
    session: String,

    /// Number of independent readers in this process.
    #[arg(long, default_value_t = 1)]
    readers: usize,

    /// Spread the start of the readers over this many milliseconds.
    #[arg(long, default_value_t = 0)]
    arrival_spread_ms: u64,

    /// Pin every reader to this cache by name.
    #[arg(long)]
    cache: Option<String>,

    #[arg(long, default_value_t = 30)]
    duration_seconds: u64,

    /// Playout buffer of the player that this reader stands for. A part that
    /// arrives later than this behind its schedule is an underrun.
    #[arg(long, default_value_t = 1_000)]
    buffer_ms: u64,

    /// Time allowed for one part request, including the wait at the cache for
    /// a part that is not published yet. The reader adds four round trips of
    /// its path to it, so that a far reader does not take delay for failure.
    #[arg(long, default_value_t = 1_500)]
    request_timeout_ms: u64,

    /// A cache that has sent this reader parts and then sends no byte for
    /// the stall time (this, or eight parts if longer, and four round trips)
    /// is left: at half of it the reader asks the cache for its playlist,
    /// and a cache that answers is kept. 400 ms is two minimum TCP
    /// retransmission timeouts, so a live connection on a lossy path does
    /// not look dead. --request-timeout-ms still bounds a first request.
    #[arg(long, default_value_t = 400)]
    stall_ms: u64,

    /// Time allowed for the first answer of an entry point or of a cache,
    /// which includes the TCP and TLS handshakes.
    #[arg(long, default_value_t = 8_000)]
    connect_timeout_ms: u64,

    /// Give up a part that no cache supplies within this time and read on.
    #[arg(long, default_value_t = 6_000)]
    abandon_ms: u64,

    /// Reload the media playlist this often. A draining cache answers the
    /// reload with a redirect.
    #[arg(long, default_value_t = 500)]
    playlist_interval_ms: u64,

    /// Follow the Content Steering manifest of the entry point.
    #[arg(long)]
    steering: bool,

    /// Start this many parts behind the newest part of the playlist.
    #[arg(long, default_value_t = 1)]
    start_back_parts: u64,

    /// Parts requested at one time. 0 measures the round trip to the cache
    /// and asks far enough ahead to cover it. 1 is one request at a time.
    #[arg(long, default_value_t = 0)]
    pipeline: usize,

    /// Largest number of parts requested at one time. The default holds
    /// 250 ms of 0.25 ms parts. A report says `pipeline_limited` when the
    /// limit cut the --min-inflight-ms window.
    #[arg(long, default_value_t = DEFAULT_PIPELINE_LIMIT)]
    pipeline_limit: usize,

    /// Most requests on one HTTP/2 connection at one time. A deeper
    /// pipeline opens more connections to the same cache and spreads the
    /// parts over them, since a cache allows 256 concurrent streams on a
    /// connection and queues the rest. One stream is kept for the playlist
    /// reload.
    #[arg(long, default_value_t = DEFAULT_STREAMS_PER_CONNECTION)]
    streams_per_connection: usize,

    /// With --pipeline 0, ask for at least this many milliseconds of parts
    /// at one time, within --pipeline-limit: at 2.5 ms parts a round trip
    /// alone asks for a handful, and one slow answer then stalls the reader.
    #[arg(long, default_value_t = DEFAULT_MIN_INFLIGHT_MS)]
    min_inflight_ms: u64,

    /// Read response bodies at this rate at most, in kbit/s. 0 reads at the
    /// speed of the network.
    #[arg(long, default_value_t = 0)]
    read_kbps: u64,

    /// Stop reading after this many seconds and keep the connections open
    /// until the run ends, as a player that hangs does. 0 never stops.
    #[arg(long, default_value_t = 0)]
    freeze_after_seconds: u64,

    /// Certificate authority that signs the node certificates.
    #[arg(long)]
    tls_ca: PathBuf,

    /// TLS server name of the nodes when the URLs name addresses.
    #[arg(long)]
    server_name: Option<String>,

    /// Write the JSON report here instead of standard output.
    #[arg(long)]
    report: Option<PathBuf>,

    /// Keep the record of every part in the report.
    #[arg(long)]
    record_parts: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Url {
    authority: String,
    path_and_query: String,
}

impl Url {
    fn parse(value: &str) -> Result<Self> {
        let rest = value
            .strip_prefix("https://")
            .ok_or_else(|| anyhow!("only https URLs are supported: {value}"))?;
        let (authority, path_and_query) = match rest.find('/') {
            Some(index) => (&rest[..index], &rest[index..]),
            None => (rest, "/"),
        };
        if authority.is_empty() {
            bail!("URL has no host: {value}");
        }
        Ok(Self {
            authority: authority.to_owned(),
            path_and_query: path_and_query.to_owned(),
        })
    }

    fn path(&self) -> &str {
        self.path_and_query
            .split_once('?')
            .map_or(self.path_and_query.as_str(), |(path, _)| path)
    }

    /// Resolve a URI reference from a playlist or a redirect.
    fn join(&self, reference: &str) -> Result<Self> {
        if reference.starts_with("https://") {
            return Self::parse(reference);
        }
        if reference.starts_with('/') {
            return Ok(Self {
                authority: self.authority.clone(),
                path_and_query: reference.to_owned(),
            });
        }
        let directory = self
            .path()
            .rfind('/')
            .map_or("/", |index| &self.path()[..=index]);
        Ok(Self {
            authority: self.authority.clone(),
            path_and_query: format!("{directory}{reference}"),
        })
    }

    fn with_query(&self, name: &str, value: &str) -> Self {
        let separator = if self.path_and_query.contains('?') {
            '&'
        } else {
            '?'
        };
        Self {
            authority: self.authority.clone(),
            path_and_query: format!("{}{separator}{name}={value}", self.path_and_query),
        }
    }

    fn host_port(&self) -> (String, u16) {
        match self.authority.rsplit_once(':') {
            Some((host, port)) => (host.to_owned(), port.parse().unwrap_or(443)),
            None => (self.authority.clone(), 443),
        }
    }
}

impl std::fmt::Display for Url {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "https://{}{}",
            self.authority, self.path_and_query
        )
    }
}

struct HttpResponse {
    status: StatusCode,
    headers: http::HeaderMap,
    body: Bytes,
    /// When the last byte of the body arrived.
    arrived: Instant,
    arrived_unix_us: u64,
}

/// How a reader takes bytes off its connections.
#[derive(Debug)]
struct Pace {
    /// Bits per second at most. 0 is no limit.
    read_bps: u64,
    /// The reader has stopped reading.
    frozen: AtomicBool,
    /// When the reader last took bytes off a connection, in microseconds
    /// since `epoch`. A cache that still sends is slow, not dead.
    last_bytes_us: AtomicU64,
    epoch: Instant,
}

impl Pace {
    fn last_bytes(&self) -> Instant {
        self.epoch + Duration::from_micros(self.last_bytes_us.load(Ordering::Relaxed))
    }

    async fn took(&self, bytes: usize) {
        self.last_bytes_us
            .store(self.epoch.elapsed().as_micros() as u64, Ordering::Relaxed);
        if self.frozen.load(Ordering::Relaxed) {
            std::future::pending::<()>().await;
        }
        if self.read_bps > 0 {
            let micros = (bytes as u64).saturating_mul(8_000_000) / self.read_bps;
            tokio::time::sleep(Duration::from_micros(micros)).await;
        }
    }
}

async fn fetch(
    mut sender: SendRequest<Empty<Bytes>>,
    request: Request<Empty<Bytes>>,
    pace: Arc<Pace>,
) -> Result<HttpResponse> {
    let response = sender.send_request(request).await?;
    let (parts, mut body) = response.into_parts();
    let mut bytes = BytesMut::new();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame?.into_data() {
            pace.took(data.len()).await;
            bytes.extend_from_slice(&data);
        }
    }
    Ok(HttpResponse {
        status: parts.status,
        headers: parts.headers,
        body: bytes.freeze(),
        arrived: Instant::now(),
        arrived_unix_us: now_unix_us(),
    })
}

/// A part request that is on its way.
struct Pending {
    part: u64,
    /// Index of the cache that the request went to.
    cache: usize,
    authority: String,
    started: Instant,
    handle: tokio::task::JoinHandle<Result<HttpResponse>>,
}

/// One HTTP/2 connection for each cache, as a player keeps.
struct HttpClient {
    connector: TlsConnector,
    server_name: Option<String>,
    session: String,
    /// The first connection to each cache, which carries the playlist.
    connections: HashMap<String, SendRequest<Empty<Bytes>>>,
    /// Further connections to a cache for a deep pipeline of parts, by
    /// cache and lane (1 and up).
    lanes: HashMap<(String, usize), SendRequest<Empty<Bytes>>>,
    pace: Arc<Pace>,
    /// Time allowed for the TCP, TLS and HTTP/2 handshakes of a new
    /// connection.
    connect_timeout: Duration,
    /// Shortest TCP handshake so far: one round trip.
    tcp_round_trip: Option<Duration>,
}

impl HttpClient {
    async fn connection(&mut self, url: &Url) -> Result<SendRequest<Empty<Bytes>>> {
        self.connection_on(url, 0).await
    }

    /// The connection of `lane` to the cache of `url`. Lane 0 is the first
    /// connection, which also carries the playlist.
    async fn connection_on(&mut self, url: &Url, lane: usize) -> Result<SendRequest<Empty<Bytes>>> {
        let existing = if lane == 0 {
            self.connections.get(&url.authority)
        } else {
            self.lanes.get(&(url.authority.clone(), lane))
        };
        if let Some(connection) = existing {
            if !connection.is_closed() {
                return Ok(connection.clone());
            }
            if lane == 0 {
                self.connections.remove(&url.authority);
            } else {
                self.lanes.remove(&(url.authority.clone(), lane));
            }
        }
        let (host, port) = url.host_port();
        let asked = Instant::now();
        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("connect {}", url.authority))?;
        let took = asked.elapsed();
        self.tcp_round_trip = Some(self.tcp_round_trip.map_or(took, |best| best.min(took)));
        tcp.set_nodelay(true)?;
        let name = self.server_name.clone().unwrap_or(host);
        let server_name = rustls::pki_types::ServerName::try_from(name)
            .map_err(|error| anyhow!("invalid TLS server name: {error}"))?;
        let tls = self
            .connector
            .connect(server_name, tcp)
            .await
            .with_context(|| format!("TLS handshake with {}", url.authority))?;
        let (sender, connection) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tls))
                .await
                .with_context(|| format!("HTTP/2 handshake with {}", url.authority))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        if lane == 0 {
            self.connections
                .insert(url.authority.clone(), sender.clone());
        } else {
            self.lanes
                .insert((url.authority.clone(), lane), sender.clone());
        }
        Ok(sender)
    }

    async fn get(&mut self, url: &Url, timeout: Duration) -> Result<HttpResponse> {
        let result = tokio::time::timeout(timeout, self.get_once(url)).await;
        match result {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => {
                self.forget(&url.authority);
                Err(error)
            }
            Err(_) => {
                // A request that timed out can leave a stalled connection.
                self.forget(&url.authority);
                Err(anyhow!("timeout after {} ms", timeout.as_millis()))
            }
        }
    }

    fn request(&self, url: &Url) -> Result<Request<Empty<Bytes>>> {
        Ok(Request::builder()
            .method("GET")
            .uri(format!("https://{}{}", url.authority, url.path_and_query))
            .header(
                "cmcd-session",
                HeaderValue::from_str(&format!("sid=\"{}\"", self.session))?,
            )
            .body(Empty::<Bytes>::new())?)
    }

    async fn get_once(&mut self, url: &Url) -> Result<HttpResponse> {
        let sender = self.connection(url).await?;
        fetch(sender, self.request(url)?, self.pace.clone()).await
    }

    /// Send a request and return at once. The response is awaited later, so
    /// that several requests are on their way on one connection.
    async fn start(&mut self, url: &Url) -> Result<tokio::task::JoinHandle<Result<HttpResponse>>> {
        self.start_on(url, 0).await
    }

    /// [`Self::start`] on the connection of `lane`.
    async fn start_on(
        &mut self,
        url: &Url,
        lane: usize,
    ) -> Result<tokio::task::JoinHandle<Result<HttpResponse>>> {
        let sender = tokio::time::timeout(self.connect_timeout, self.connection_on(url, lane))
            .await
            .map_err(|_| {
                anyhow!(
                    "no connection to {} after {} ms",
                    url.authority,
                    self.connect_timeout.as_millis()
                )
            })??;
        let request = self.request(url)?;
        Ok(tokio::spawn(fetch(sender, request, self.pace.clone())))
    }

    fn forget(&mut self, authority: &str) {
        self.connections.remove(authority);
        self.lanes
            .retain(|(lane_authority, _), _| lane_authority != authority);
    }
}

fn attribute<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let attributes = line.split_once(':')?.1;
    let mut rest = attributes;
    while !rest.is_empty() {
        let (key, after) = rest.split_once('=')?;
        let (value, remaining) = if let Some(quoted) = after.strip_prefix('"') {
            let end = quoted.find('"')?;
            (&quoted[..end], quoted[end + 1..].trim_start_matches(','))
        } else {
            match after.find(',') {
                Some(end) => (&after[..end], &after[end + 1..]),
                None => (after, ""),
            }
        };
        if key.trim() == name {
            return Some(value);
        }
        rest = remaining;
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheTarget {
    /// The Content Steering pathway of the cache, or its URL when the
    /// playlist names no pathway.
    name: String,
    playlist: Url,
}

#[derive(Debug, Clone, Default)]
struct MasterPlaylist {
    caches: Vec<CacheTarget>,
    steering: Option<Url>,
}

fn parse_master(body: &str, base: &Url) -> Result<MasterPlaylist> {
    let mut master = MasterPlaylist::default();
    let mut pending: Option<Option<String>> = None;
    for line in body.lines().map(str::trim) {
        if line.starts_with("#EXT-X-CONTENT-STEERING:") {
            if let Some(uri) = attribute(line, "SERVER-URI") {
                master.steering = Some(base.join(uri)?);
            }
        } else if line.starts_with("#EXT-X-STREAM-INF:") {
            pending = Some(attribute(line, "PATHWAY-ID").map(str::to_owned));
        } else if !line.is_empty() && !line.starts_with('#') {
            if let Some(pathway) = pending.take() {
                let playlist = base.join(line)?;
                let name = pathway.unwrap_or_else(|| playlist.authority.clone());
                // One reader uses one rendition of each cache.
                if !master.caches.iter().any(|cache| cache.name == name) {
                    master.caches.push(CacheTarget { name, playlist });
                }
            }
        }
    }
    if master.caches.is_empty() {
        bail!("the Multivariant Playlist names no cache");
    }
    Ok(master)
}

#[derive(Debug, Clone, Default)]
struct MediaPlaylist {
    /// Part sequence and duration in microseconds, oldest first.
    parts: Vec<(u64, u64)>,
    extension: String,
    /// PART-TARGET in microseconds.
    part_target_us: u64,
}

fn part_sequence(uri: &str) -> Option<(u64, &str)> {
    let name = uri.rsplit('/').next()?;
    let name = name.split('?').next()?;
    let rest = name.strip_prefix("part")?;
    let (sequence, extension) = rest.split_once('.')?;
    Some((sequence.parse().ok()?, extension))
}

fn parse_media(body: &str) -> MediaPlaylist {
    let mut playlist = MediaPlaylist::default();
    for line in body.lines().map(str::trim) {
        if line.starts_with("#EXT-X-PART-INF:") {
            playlist.part_target_us = attribute(line, "PART-TARGET")
                .and_then(parse_seconds_us)
                .unwrap_or(0);
        } else if line.starts_with("#EXT-X-PART:") {
            let Some((sequence, extension)) = attribute(line, "URI").and_then(part_sequence) else {
                continue;
            };
            let duration_us = attribute(line, "DURATION")
                .and_then(parse_seconds_us)
                .unwrap_or(playlist.part_target_us);
            playlist.extension = extension.to_owned();
            playlist.parts.push((sequence, duration_us));
        }
    }
    playlist
}

#[derive(Debug, Clone, Serialize)]
struct Move {
    /// Milliseconds since the reader started.
    at_ms: u64,
    part: u64,
    from: String,
    to: String,
    /// `failure`, `drain-redirect`, `steering`, `rejected` or `reassigned`.
    reason: String,
    detail: String,
}

/// One stop of the player.
#[derive(Debug, Clone, Serialize)]
struct Stall {
    /// Milliseconds since the reader started.
    at_ms: u64,
    part: u64,
    ms: u64,
    /// The stop in microseconds: a stop of a few 0.25 ms parts is less
    /// than a millisecond.
    us: u64,
}

/// How late a part may be and still count as on time: half of the part,
/// and at most a millisecond. A part of 2 ms or more keeps the millisecond
/// of the schedule that was kept in whole milliseconds; a 0.25 ms part that
/// is 0.75 ms late is three parts of silence, and a stall.
fn stall_threshold_us(part_us: u64) -> u64 {
    (part_us / 2).clamp(1, 1_000)
}

/// The stalls of a player that plays each part at its place in the
/// schedule, later by every stall so far.
#[derive(Debug, Default)]
struct StallTracker {
    /// How much later the player plays than its first schedule.
    shift_us: u64,
    stalled_us: u64,
    in_stall: bool,
    run_us: u64,
}

impl StallTracker {
    /// A part of `part_us` arrived `late_us` after its moment to play.
    fn observe(
        &mut self,
        report: &mut ReaderReport,
        late_us: i64,
        part_us: u64,
        at_ms: u64,
        part: u64,
    ) {
        if late_us < stall_threshold_us(part_us) as i64 {
            self.in_stall = false;
            return;
        }
        let late_us = late_us as u64;
        self.shift_us += late_us;
        self.stalled_us += late_us;
        report.stalled_ms = self.stalled_us / 1_000;
        report.stalled_us = self.stalled_us;
        if self.in_stall {
            self.run_us += late_us;
            let recorded = report.stalls as usize == report.stall_events.len();
            if let Some(event) = report.stall_events.last_mut().filter(|_| recorded) {
                event.ms = self.run_us / 1_000;
                event.us = self.run_us;
            }
        } else {
            report.stalls += 1;
            self.run_us = late_us;
            if report.stall_events.len() < 64 {
                report.stall_events.push(Stall {
                    at_ms,
                    part,
                    ms: late_us / 1_000,
                    us: late_us,
                });
            }
        }
        report.longest_stall_ms = report.longest_stall_ms.max(self.run_us / 1_000);
        report.longest_stall_us = report.longest_stall_us.max(self.run_us);
        self.in_stall = true;
    }
}

#[derive(Debug, Clone, Serialize)]
struct PartRecord {
    part: u64,
    cache: String,
    bytes: usize,
    at_ms: u64,
    /// Milliseconds behind the playout schedule that started with the first
    /// part. Negative when the part is early.
    behind_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_latency_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct ReaderReport {
    session: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    entry_point: String,
    assigned: Vec<String>,
    first_cache: String,
    final_cache: String,
    /// Time to the first part.
    startup_ms: u64,
    /// Time until the reader held `buffer_ms` of media and a player would
    /// have started to play.
    #[serde(skip_serializing_if = "Option::is_none")]
    playback_start_ms: Option<u64>,
    first_part: u64,
    last_part: u64,
    parts_received: u64,
    parts_missing: u64,
    bytes: u64,
    /// Parts that arrived later than the playout buffer allows, against a
    /// schedule that never moves.
    underruns: u64,
    /// Times that a player with this buffer would have stopped: a part that
    /// arrived after its moment to play. The player then plays that much
    /// later, so one stall is one event however many parts were late.
    stalls: u64,
    stalled_ms: u64,
    stalled_us: u64,
    longest_stall_ms: u64,
    longest_stall_us: u64,
    /// The first 64 stalls.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    stall_events: Vec<Stall>,
    max_behind_ms: i64,
    max_gap_ms: u64,
    /// Caches that the steering manifest defined as pathway clones.
    pathway_clones: u64,
    /// Times that no entry point gave the reader a cache.
    entry_refusals: u64,
    /// Largest number of part requests on their way at one time.
    pipeline_depth: usize,
    /// Most HTTP/2 connections to one cache that carried them.
    pipeline_connections: usize,
    /// Whether --pipeline-limit kept the requests on their way below the
    /// --min-inflight-ms window.
    pipeline_limited: bool,
    /// Shortest round trip of a playlist reload.
    #[serde(skip_serializing_if = "Option::is_none")]
    round_trip_ms: Option<f64>,
    /// From the moment the contributor published a part to its arrival.
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_p50_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_p99_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_max_ms: Option<f64>,
    /// The reader stopped reading at this time, on purpose.
    #[serde(skip_serializing_if = "Option::is_none")]
    frozen_at_ms: Option<u64>,
    request_errors: u64,
    rejections: u64,
    parts_by_cache: BTreeMap<String, u64>,
    moves: Vec<Move>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_latency_p50_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_latency_p99_ms: Option<f64>,
    /// Hash of the parts in order, to compare readers on different caches.
    #[serde(skip_serializing_if = "Option::is_none")]
    etag_digest: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    parts: Vec<PartRecord>,
}

fn now_unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as u64)
        .unwrap_or(0)
}

fn percentile(sorted: &[f64], percent: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let index = ((sorted.len() - 1) as f64 * percent / 100.0).round() as usize;
    Some((sorted[index] * 1_000.0).round() / 1_000.0)
}

struct Reader {
    args: Args,
    http: HttpClient,
    session: String,
    started: Instant,
    master_url: Url,
    master: MasterPlaylist,
    current: usize,
    report: ReaderReport,
    steering_due: Instant,
    playlist_due: Instant,
    rejected_in_a_row: usize,
    /// Shortest playlist round trip so far.
    round_trip: Option<Duration>,
    /// The pathway priority of the newest steering manifest.
    priority: Vec<String>,
    /// Caches that failed lately, and when. A player leaves a pathway that
    /// failed alone for a while.
    penalised: HashMap<String, Instant>,
    /// The current cache has sent this reader a part: a silence of the
    /// stall time leaves it (B 3.10).
    received_from_current: bool,
}

impl Reader {
    /// A reader that starts now.
    fn new(args: Args, connector: TlsConnector, session: String) -> Self {
        let now = Instant::now();
        Self {
            http: HttpClient {
                connector,
                server_name: args.server_name.clone(),
                session: session.clone(),
                connections: HashMap::new(),
                lanes: HashMap::new(),
                pace: Arc::new(Pace {
                    read_bps: args.read_kbps * 1_000,
                    frozen: AtomicBool::new(false),
                    last_bytes_us: AtomicU64::new(0),
                    epoch: now,
                }),
                connect_timeout: Duration::from_millis(args.connect_timeout_ms),
                tcp_round_trip: None,
            },
            master_url: Url {
                authority: String::new(),
                path_and_query: "/".into(),
            },
            master: MasterPlaylist::default(),
            current: 0,
            report: ReaderReport {
                session: session.clone(),
                ..ReaderReport::default()
            },
            session,
            started: now,
            steering_due: now,
            playlist_due: now,
            rejected_in_a_row: 0,
            round_trip: None,
            priority: Vec::new(),
            penalised: HashMap::new(),
            received_from_current: false,
            args,
        }
    }

    /// Parts to ask for at one time: enough to cover a round trip and a
    /// little more, so that a part is on its way before the reader needs it.
    fn pipeline_depth(&self, part_us: u64) -> usize {
        let limit = self.args.pipeline_limit.max(1);
        if self.args.pipeline > 0 {
            return self.args.pipeline.min(limit);
        }
        pipeline_depth_for(
            self.path_round_trip(),
            part_us,
            self.args.min_inflight_ms,
            limit,
        )
    }

    fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// The round trip to the caches: the shorter of the best playlist
    /// reload and the best TCP handshake.
    fn path_round_trip(&self) -> Duration {
        match (self.round_trip, self.http.tcp_round_trip) {
            (Some(a), Some(b)) => a.min(b),
            (a, b) => a.or(b).unwrap_or(Duration::ZERO),
        }
    }

    /// A part can take several round trips on a new connection, while TCP
    /// still opens its window.
    fn timeout(&self) -> Duration {
        Duration::from_millis(self.args.request_timeout_ms) + self.path_round_trip() * 4
    }

    /// How long a cache that has sent this reader parts may send nothing
    /// before the reader leaves it (B 3.10).
    fn stall_timeout(&self, part_us: u64) -> Duration {
        stall_timeout_for(self.args.stall_ms, part_us, self.path_round_trip())
    }

    /// Time allowed for a request to `url`: longer when it needs a new
    /// connection.
    fn timeout_for(&self, url: &Url) -> Duration {
        if self.http.connections.contains_key(&url.authority) {
            self.timeout()
        } else {
            self.timeout()
                .max(Duration::from_millis(self.args.connect_timeout_ms))
        }
    }

    fn current_name(&self) -> String {
        self.master.caches[self.current].name.clone()
    }

    /// Ask an entry point for the assignment. Entry points are tried in
    /// order, so the failure of one does not stop a reader.
    async fn load_master(&mut self) -> Result<()> {
        let mut last_error = anyhow!("no entry point");
        for entry in self.args.entries.clone() {
            let mut url = Url::parse(&entry)?
                .join(&format!("/live/{}/master.m3u8", self.args.stream))?
                .with_query("sid", &self.session);
            if let Some(cache) = &self.args.cache {
                url = url.with_query("cache", cache);
            }
            match self.http.get(&url, self.timeout_for(&url)).await {
                Ok(response) if response.status == StatusCode::OK => {
                    let body = String::from_utf8_lossy(&response.body).into_owned();
                    self.master = parse_master(&body, &url)?;
                    self.master_url = url;
                    self.report.entry_point = entry;
                    return Ok(());
                }
                Ok(response) => {
                    last_error = anyhow!("entry point {entry} answered {}", response.status);
                }
                Err(error) => last_error = error.context(format!("entry point {entry}")),
            }
        }
        Err(last_error)
    }

    fn record_move(&mut self, part: u64, to: usize, reason: &str, detail: String) {
        let from = self.current_name();
        self.current = to;
        self.received_from_current = false;
        // The next cache can be on another path, for example in another
        // region. Its round trip is measured anew.
        self.round_trip = None;
        self.http.tcp_round_trip = None;
        // A player loads the playlist of a cache before it asks the cache
        // for a part.
        self.playlist_due = Instant::now();
        self.report.moves.push(Move {
            at_ms: self.elapsed_ms(),
            part,
            from,
            to: self.current_name(),
            reason: reason.to_owned(),
            detail,
        });
    }

    /// Use a cache that the reader was sent to: a redirect or an alternate.
    fn adopt(&mut self, playlist: Url, part: u64, reason: &str, detail: String) {
        let index = match self
            .master
            .caches
            .iter()
            .position(|cache| cache.playlist.authority == playlist.authority)
        {
            Some(index) => index,
            None => {
                self.master.caches.push(CacheTarget {
                    name: playlist.authority.clone(),
                    playlist,
                });
                self.master.caches.len() - 1
            }
        };
        if index != self.current {
            self.record_move(part, index, reason, detail);
        }
    }

    /// The current cache failed. Use the next cache of the assignment. When
    /// the whole list has failed, ask an entry point again.
    async fn fail_over(&mut self, part: u64, failed_in_a_row: usize, detail: String) {
        self.report.request_errors += 1;
        if failed_in_a_row > 0 && failed_in_a_row.is_multiple_of(self.master.caches.len()) {
            let previous = self.current_name();
            if self.load_master().await.is_ok() {
                self.current = 0;
                self.received_from_current = false;
                self.report.moves.push(Move {
                    at_ms: self.elapsed_ms(),
                    part,
                    from: previous,
                    to: self.current_name(),
                    reason: "reassigned".into(),
                    detail,
                });
                return;
            }
        }
        if self.master.caches.len() > 1 {
            // The cache that failed is left alone for a while. The next cache
            // is the first one of the steering manifest that has not failed
            // lately, as a player chooses it, or else the next of the list.
            let failed = self.current_name();
            self.penalised.insert(failed, Instant::now());
            self.penalised
                .retain(|_, at| at.elapsed() < Duration::from_secs(30));
            let steered = self
                .priority
                .iter()
                .filter(|name| !self.penalised.contains_key(*name))
                .find_map(|name| {
                    self.master
                        .caches
                        .iter()
                        .position(|cache| &cache.name == name)
                });
            let next = steered
                .filter(|index| *index != self.current)
                .unwrap_or((self.current + 1) % self.master.caches.len());
            self.record_move(part, next, "failure", detail);
        }
    }

    async fn load_playlist(&mut self, part: u64) -> Result<Option<MediaPlaylist>> {
        let url = self.master.caches[self.current].playlist.clone();
        let within = self.timeout_for(&url);
        self.load_playlist_within(part, within).await
    }

    async fn load_playlist_within(
        &mut self,
        part: u64,
        within: Duration,
    ) -> Result<Option<MediaPlaylist>> {
        let url = self.master.caches[self.current].playlist.clone();
        let reused = self.http.connections.contains_key(&url.authority);
        let asked = Instant::now();
        let response = self.http.get(&url, within).await?;
        self.playlist_response(url, reused.then_some(asked), response, part)
            .await
    }

    /// Time allowed for a question that a live cache answers at once: its
    /// playlist, when a part is overdue.
    fn quick_answer(&self) -> Duration {
        self.path_round_trip() * 3 + Duration::from_millis(250)
    }

    /// Act on the answer to a playlist request: the parts of the playlist, a
    /// redirect of a draining cache, or a refusal.
    async fn playlist_response(
        &mut self,
        url: Url,
        asked_on_open_connection: Option<Instant>,
        response: HttpResponse,
        part: u64,
    ) -> Result<Option<MediaPlaylist>> {
        if let Some(asked) = asked_on_open_connection {
            let took = response.arrived.duration_since(asked);
            let round_trip = self.round_trip.map_or(took, |best| best.min(took));
            self.round_trip = Some(round_trip);
            // The handshakes of a new connection are three round trips.
            let _ = round_trip;
            self.http.connect_timeout = self.timeout();
        }
        match response.status {
            StatusCode::OK => {
                self.rejected_in_a_row = 0;
                Ok(Some(parse_media(&String::from_utf8_lossy(&response.body))))
            }
            StatusCode::TEMPORARY_REDIRECT | StatusCode::FOUND | StatusCode::SEE_OTHER => {
                let location = response
                    .headers
                    .get("location")
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| anyhow!("redirect without a location"))?;
                let target = url.join(location)?;
                let detail = format!("{} redirected the playlist reload", self.current_name());
                self.adopt(target, part, "drain-redirect", detail);
                Ok(None)
            }
            StatusCode::TOO_MANY_REQUESTS => {
                self.report.rejections += 1;
                let alternate = response
                    .headers
                    .get("x-needletail-alternate-edges")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.split(',').map(str::trim).next())
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned);
                self.rejected_in_a_row += 1;
                if self.rejected_in_a_row >= self.master.caches.len().max(2) {
                    // Every cache has refused. Wait as the response asks
                    // before trying again.
                    self.rejected_in_a_row = 0;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                match alternate {
                    Some(alternate) => {
                        let detail = format!("{} admits no new reader", self.current_name());
                        self.adopt(url.join(&alternate)?, part, "rejected", detail);
                        Ok(None)
                    }
                    None => bail!("rejected without an alternate cache"),
                }
            }
            status => bail!("playlist answered {status}"),
        }
    }

    async fn poll_steering(&mut self, part: u64) {
        let Some(steering) = self.master.steering.clone() else {
            return;
        };
        let url = steering.with_query("_HLS_pathway", &self.current_name());
        let Ok(response) = self.http.get(&url, self.timeout()).await else {
            self.steering_due = Instant::now() + Duration::from_secs(1);
            return;
        };
        if response.status != StatusCode::OK {
            self.steering_due = Instant::now() + Duration::from_secs(1);
            return;
        }
        let Ok(manifest) = serde_json::from_slice::<serde_json::Value>(&response.body) else {
            return;
        };
        let ttl = manifest["TTL"].as_u64().unwrap_or(5).max(1);
        self.steering_due = Instant::now() + Duration::from_secs(ttl);
        if let Some(reload) = manifest["RELOAD-URI"].as_str() {
            if let Ok(reload) = steering.join(reload) {
                self.master.steering = Some(reload);
            }
        }
        // A pathway clone defines a cache that the Multivariant Playlist did
        // not name: the URIs of its base pathway with another host.
        for clone in manifest["PATHWAY-CLONES"].as_array().into_iter().flatten() {
            let (Some(id), Some(base), Some(host)) = (
                clone["ID"].as_str(),
                clone["BASE-ID"].as_str(),
                clone["URI-REPLACEMENT"]["HOST"].as_str(),
            ) else {
                continue;
            };
            if self.master.caches.iter().any(|cache| cache.name == id) {
                continue;
            }
            let Some(base) = self.master.caches.iter().find(|cache| cache.name == base) else {
                continue;
            };
            let (_, port) = base.playlist.host_port();
            self.master.caches.push(CacheTarget {
                name: id.to_owned(),
                playlist: Url {
                    authority: format!("{host}:{port}"),
                    path_and_query: base.playlist.path_and_query.clone(),
                },
            });
            self.report.pathway_clones += 1;
        }
        self.priority = manifest["PATHWAY-PRIORITY"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|pathway| pathway.as_str().map(str::to_owned))
            .collect();
        let first = manifest["PATHWAY-PRIORITY"]
            .as_array()
            .and_then(|priority| priority.first())
            .and_then(|pathway| pathway.as_str());
        if let Some(first) = first {
            if first != self.current_name() {
                if let Some(index) = self
                    .master
                    .caches
                    .iter()
                    .position(|cache| cache.name == first)
                {
                    self.record_move(
                        part,
                        index,
                        "steering",
                        format!("the steering manifest puts {first} first"),
                    );
                }
            }
        }
    }

    async fn run(mut self) -> ReaderReport {
        match self.read().await {
            Ok(()) => self.report.ok = true,
            Err(error) => self.report.error = Some(format!("{error:#}")),
        }
        self.report
    }

    async fn read(&mut self) -> Result<()> {
        let deadline = self.started + Duration::from_secs(self.args.duration_seconds);
        // An entry point can answer 503 while the region has no cache with
        // room. A player retries.
        // The wait doubles from half a second to eight seconds, as the retry
        // policy of a player does, so that a crowd that a full region has
        // refused does not ask it again all at once.
        let mut wait = Duration::from_millis(500);
        loop {
            match self.load_master().await {
                Ok(()) => break,
                Err(error) if Instant::now() + wait + Duration::from_secs(1) < deadline => {
                    let _ = error;
                    self.report.entry_refusals += 1;
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(Duration::from_secs(8));
                }
                Err(error) => return Err(error.context("no assignment")),
            }
        }
        self.report.assigned = self
            .master
            .caches
            .iter()
            .map(|cache| cache.name.clone())
            .collect();
        self.report.first_cache = self.current_name();

        // Join at the newest parts of the assigned cache.
        let mut playlist = MediaPlaylist::default();
        let mut failed = 0_usize;
        while playlist.parts.is_empty() {
            if Instant::now() >= deadline {
                bail!("no cache supplied a playlist with parts");
            }
            match self.load_playlist(0).await {
                Ok(Some(loaded)) if !loaded.parts.is_empty() => playlist = loaded,
                Ok(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                Err(error) => {
                    failed += 1;
                    self.fail_over(0, failed, format!("{error:#}")).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        let extension = playlist.extension.clone();
        let mut durations: HashMap<u64, u64> = playlist.parts.iter().copied().collect();
        let mut default_duration_us = playlist
            .parts
            .last()
            .map_or(playlist.part_target_us, |(_, duration)| *duration)
            .max(1);
        let newest = playlist.parts.last().map_or(0, |(sequence, _)| *sequence);
        let oldest = playlist.parts.first().map_or(0, |(sequence, _)| *sequence);
        let mut next = newest
            .saturating_sub(self.args.start_back_parts)
            .max(oldest);
        self.report.first_part = next;
        self.report.first_cache = self.current_name();
        self.playlist_due = Instant::now() + Duration::from_millis(self.args.playlist_interval_ms);

        let mut schedule: Option<(Instant, u64)> = None;
        // The schedule is kept in microseconds, so that parts of 2.5 ms or
        // less add up exactly.
        let mut scheduled_us = 0_u64;
        // How much later the player plays than its first schedule (the sum
        // of its stalls), and the stall in progress.
        let mut stalls = StallTracker::default();
        // When the player started, in microseconds after the first part.
        let mut play_from_us: Option<u64> = None;
        let mut last_arrival: Option<Instant> = None;
        let mut latencies = Vec::new();
        let mut publish_latencies = Vec::new();
        let mut digest = 0xcbf2_9ce4_8422_2325_u64;
        let mut failed = 0_usize;
        let mut part_started = Instant::now();
        let mut inflight: VecDeque<Pending> = VecDeque::new();
        let mut request_next = next;
        let mut catching_up = false;
        #[allow(clippy::type_complexity)]
        let mut reload: Option<(
            Url,
            Option<Instant>,
            Instant,
            tokio::task::JoinHandle<Result<HttpResponse>>,
        )> = None;
        let freeze_at = (self.args.freeze_after_seconds > 0)
            .then(|| self.started + Duration::from_secs(self.args.freeze_after_seconds));

        while Instant::now() < deadline {
            if freeze_at.is_some_and(|at| Instant::now() >= at) {
                // Stop reading. The connections and the requests stay open.
                self.http.pace.frozen.store(true, Ordering::Relaxed);
                self.report.frozen_at_ms = Some(self.elapsed_ms());
                tokio::time::sleep_until(deadline.into()).await;
                break;
            }
            if self.args.steering && Instant::now() >= self.steering_due {
                self.poll_steering(next).await;
            }
            // The playlist is reloaded beside the parts, so that a reload
            // never holds a part back.
            if reload.is_none() && Instant::now() >= self.playlist_due {
                self.playlist_due =
                    Instant::now() + Duration::from_millis(self.args.playlist_interval_ms);
                let url = self.master.caches[self.current].playlist.clone();
                let reused = self.http.connections.contains_key(&url.authority);
                match self.http.start(&url).await {
                    Ok(handle) => {
                        reload = Some((url, reused.then(Instant::now), Instant::now(), handle))
                    }
                    Err(error) => {
                        failed += 1;
                        self.fail_over(next, failed, format!("playlist: {error:#}"))
                            .await;
                        continue;
                    }
                }
            }
            let reload_done = reload.as_ref().is_some_and(|(_, _, asked, handle)| {
                handle.is_finished() || asked.elapsed() > self.timeout()
            });
            if reload_done {
                let (url, asked_open, _, handle) = reload.take().expect("reload");
                // A reload that was sent to a cache that the reader has left
                // says nothing about the cache that it reads now. Its
                // timeout made a reader leave the warm cache that it had
                // just moved to.
                if url.authority != self.master.caches[self.current].playlist.authority {
                    handle.abort();
                    continue;
                }
                let result = if handle.is_finished() {
                    match handle.await {
                        Ok(Ok(response)) => {
                            self.playlist_response(url, asked_open, response, next)
                                .await
                        }
                        Ok(Err(error)) => {
                            self.http.forget(&url.authority);
                            Err(error)
                        }
                        Err(join) => Err(anyhow!("playlist task failed: {join}")),
                    }
                } else {
                    handle.abort();
                    self.http.forget(&url.authority);
                    Err(anyhow!("timeout after {} ms", self.timeout().as_millis()))
                };
                match result {
                    Ok(Some(loaded)) => {
                        for (sequence, duration) in &loaded.parts {
                            durations.insert(*sequence, *duration);
                        }
                        if let Some((_, duration)) = loaded.parts.last() {
                            default_duration_us = (*duration).max(1);
                        }
                        durations.retain(|sequence, _| *sequence + 4_096 > next);
                        // A player plays what the playlist lists. When the
                        // part that the reader waits for is overdue, and the
                        // playlist lists later parts and not this one, the
                        // part will not come: it was lost before the region,
                        // or this cache began its copy after it. The reader
                        // goes on from the next part that is listed. It
                        // waited for each lost part in turn, 1.5 s or more
                        // for each.
                        if let Some(skip_to) = part_after_a_hole(
                            &loaded.parts,
                            next,
                            part_started.elapsed() >= self.timeout(),
                        ) {
                            for pending in inflight.drain(..) {
                                pending.handle.abort();
                            }
                            for sequence in next..skip_to {
                                scheduled_us += durations
                                    .get(&sequence)
                                    .copied()
                                    .unwrap_or(default_duration_us);
                            }
                            self.report.parts_missing += skip_to - next;
                            next = skip_to;
                            request_next = next;
                            part_started = Instant::now();
                            failed = 0;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        failed += 1;
                        self.fail_over(next, failed, format!("playlist: {error:#}"))
                            .await;
                        continue;
                    }
                }
            }

            // Requests that went to a cache that the reader has left are of
            // no use.
            if inflight
                .front()
                .is_some_and(|pending| pending.cache != self.current)
            {
                for pending in inflight.drain(..) {
                    pending.handle.abort();
                }
                request_next = next;
            }
            let playlist_url = self.master.caches[self.current].playlist.clone();
            // A reader that is behind the cache asks further ahead: the parts
            // exist, so only the link limits how fast it catches up.
            let depth = if catching_up && self.args.pipeline == 0 {
                (self.pipeline_depth(default_duration_us) * 4).min(self.args.pipeline_limit.max(1))
            } else {
                self.pipeline_depth(default_duration_us)
            };
            // A cache holds a request for a part that is up to three seconds
            // ahead of its newest part, and answers 404 for one further
            // ahead. A reader that catches up asked 20 parts of 200 ms ahead,
            // took the 404 for a failure and left its cache. Two seconds of
            // parts is the most that it asks ahead.
            let depth = depth.min(parts_in_two_seconds(default_duration_us));
            if self.args.pipeline == 0 && !self.report.pipeline_limited {
                let wanted = pipeline_depth_for(
                    self.path_round_trip(),
                    default_duration_us,
                    self.args.min_inflight_ms,
                    usize::MAX,
                );
                if wanted > self.args.pipeline_limit.max(2) {
                    self.report.pipeline_limited = true;
                    eprintln!(
                        "needletail-reader: --pipeline-limit {} cuts the in-flight window to \
                         {} us of {} us parts ({wanted} parts wanted)",
                        self.args.pipeline_limit,
                        self.args.pipeline_limit.max(2) as u64 * default_duration_us,
                        default_duration_us
                    );
                }
            }
            // Spread the parts over enough connections that none carries
            // more requests than a cache allows on one.
            let lanes = connections_for(depth, self.args.streams_per_connection);
            self.report.pipeline_connections = self.report.pipeline_connections.max(lanes);
            let mut start_error = None;
            while inflight.len() < depth {
                let part_url = playlist_url.join(&format!("part{request_next}.{extension}"))?;
                let lane = (request_next % lanes as u64) as usize;
                match self.http.start_on(&part_url, lane).await {
                    Ok(handle) => {
                        inflight.push_back(Pending {
                            part: request_next,
                            cache: self.current,
                            authority: part_url.authority.clone(),
                            started: Instant::now(),
                            handle,
                        });
                        request_next += 1;
                    }
                    Err(error) => {
                        start_error = Some(error);
                        break;
                    }
                }
            }
            self.report.pipeline_depth = self.report.pipeline_depth.max(inflight.len());

            // The playlist that the cache gave when it was asked during the
            // wait, when it shows the part to be a hole.
            let mut answered_hole: Option<MediaPlaylist> = None;
            let outcome = match inflight.pop_front() {
                Some(mut pending) => {
                    debug_assert_eq!(pending.part, next);
                    // The time allowed runs from the moment the reader began
                    // to wait for this part, not from the moment it asked.
                    // A cache that still sends bytes is slow, not dead: the
                    // time runs again from the last bytes. A cache that has
                    // sent this reader parts is asked for its playlist at
                    // half the stall time, and left at the stall time
                    // unless it answers (B 3.10).
                    let end = freeze_at.unwrap_or(deadline).min(deadline);
                    let stall = self
                        .received_from_current
                        .then(|| self.stall_timeout(default_duration_us));
                    let mut wait = PartWait::new(stall, self.timeout());
                    let waited = loop {
                        // No verdict on the cache once the run or the
                        // reading has ended (review of B Z4).
                        let now = Instant::now();
                        if now >= end {
                            break Waited::End;
                        }
                        let since = pending
                            .started
                            .max(part_started)
                            .max(self.http.pace.last_bytes());
                        let until = match wait.step(since, now) {
                            WaitStep::Until(at) => at,
                            WaitStep::TimedOut => break Waited::TimedOut,
                            WaitStep::Stalled => break Waited::Stalled,
                            WaitStep::AskPlaylist { by } => {
                                // On the connection the reader has, and no
                                // other cache: a pause of the stream opens
                                // no connection anywhere.
                                let within = by.min(end).saturating_duration_since(now);
                                match self.load_playlist_within(next, within).await {
                                    Ok(Some(loaded)) => {
                                        match playlist_verdict(&loaded.parts, next) {
                                            PlaylistVerdict::SkipTo(_) => {
                                                break Waited::Hole(loaded)
                                            }
                                            PlaylistVerdict::Stopped | PlaylistVerdict::Listed => {
                                                wait.answered()
                                            }
                                        }
                                    }
                                    // Redirected or refused: the reader may
                                    // have another cache now.
                                    Ok(None) if self.current != pending.cache => {
                                        break Waited::Moved
                                    }
                                    Ok(None) => wait.answered(),
                                    // No answer, or a refusal: no sign of
                                    // life, though a refusal's body moved
                                    // the time of the last byte. The stall
                                    // time the question was asked for
                                    // decides.
                                    Err(_) => wait.unanswered(by),
                                }
                                continue;
                            }
                        };
                        if let Ok(result) =
                            tokio::time::timeout_at(until.min(end).into(), &mut pending.handle)
                                .await
                        {
                            break Waited::Done(result);
                        }
                    };
                    match waited {
                        Waited::Done(Ok(result)) => {
                            if result.is_err() {
                                self.http.forget(&pending.authority);
                            }
                            result
                        }
                        Waited::Done(Err(join)) => Err(anyhow!("request task failed: {join}")),
                        Waited::End => {
                            // The run or the reading ends here. Keep the
                            // request open.
                            inflight.push_front(pending);
                            continue;
                        }
                        Waited::TimedOut => {
                            pending.handle.abort();
                            // A request that timed out can leave a stalled
                            // connection.
                            self.http.forget(&pending.authority);
                            Err(anyhow!("timeout after {} ms", self.timeout().as_millis()))
                        }
                        Waited::Hole(loaded) => {
                            pending.handle.abort();
                            answered_hole = Some(loaded);
                            Err(anyhow!("part {next} is a hole in the cache's playlist"))
                        }
                        Waited::Moved => {
                            pending.handle.abort();
                            for pending in inflight.drain(..) {
                                pending.handle.abort();
                            }
                            request_next = next;
                            continue;
                        }
                        Waited::Stalled => {
                            pending.handle.abort();
                            for pending in inflight.drain(..) {
                                pending.handle.abort();
                            }
                            request_next = next;
                            self.http.forget(&pending.authority);
                            failed += 1;
                            let detail = format!(
                                "part {next}: no byte and no answer to the playlist in {} ms",
                                stall.unwrap_or_default().as_millis()
                            );
                            self.fail_over(next, failed, detail).await;
                            // The next cache is the same for every reader
                            // of the playlist on this cache: they open it
                            // spread over 100 ms.
                            tokio::time::sleep(next_cache_delay(&self.session)).await;
                            continue;
                        }
                    }
                }
                None => Err(start_error.unwrap_or_else(|| anyhow!("no request was sent"))),
            };
            let response = match outcome {
                Ok(response) if response.status == StatusCode::OK && !response.body.is_empty() => {
                    response
                }
                Ok(response) if response.status == StatusCode::TOO_MANY_REQUESTS => {
                    for pending in inflight.drain(..) {
                        pending.handle.abort();
                    }
                    request_next = next;
                    self.report.rejections += 1;
                    let alternate = response
                        .headers
                        .get("x-needletail-alternate-edges")
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.split(',').map(str::trim).next())
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned);
                    match alternate {
                        Some(alternate) => {
                            let target = Url::parse(&alternate)?
                                .join(playlist_url.path_and_query.as_str())?;
                            let detail = format!("{} admits no new reader", self.current_name());
                            self.adopt(target, next, "rejected", detail);
                        }
                        None => {
                            failed += 1;
                            self.fail_over(next, failed, "rejected without an alternate".into())
                                .await;
                        }
                    }
                    continue;
                }
                other => {
                    for pending in inflight.drain(..) {
                        pending.handle.abort();
                    }
                    request_next = next;
                    let not_found = matches!(
                        &other,
                        Ok(response) if response.status == StatusCode::NOT_FOUND
                    );
                    let detail = match other {
                        Ok(response) => format!("part {next} answered {}", response.status),
                        Err(error) => format!("part {next}: {error:#}"),
                    };
                    // The cache says that it does not have the part, or the
                    // part is overdue. The playlist of the cache says what
                    // happened. Later parts are listed and this one is not:
                    // the part was lost, and a player plays the next one
                    // that is listed, from the same cache. Nothing newer is
                    // listed: the cache is there and the stream has
                    // stopped, for every cache alike unless the entry point
                    // says otherwise. A player then waits where it is. The
                    // reader used to leave, and went round every cache of
                    // two regions while the source was silent.
                    let timed_out = detail.contains("timeout after");
                    let loaded = match answered_hole {
                        Some(loaded) => Some(loaded),
                        None if not_found || timed_out => {
                            let within = self.quick_answer();
                            self.load_playlist_within(next, within).await.ok().flatten()
                        }
                        None => None,
                    };
                    if let Some(loaded) = loaded {
                        match playlist_verdict(&loaded.parts, next) {
                            PlaylistVerdict::Stopped => {
                                if self.master.steering.is_some() {
                                    self.poll_steering(next).await;
                                }
                                continue;
                            }
                            PlaylistVerdict::SkipTo(skip_to) => {
                                for (sequence, duration) in &loaded.parts {
                                    durations.insert(*sequence, *duration);
                                }
                                for sequence in next..skip_to {
                                    scheduled_us += durations
                                        .get(&sequence)
                                        .copied()
                                        .unwrap_or(default_duration_us);
                                }
                                self.report.parts_missing += skip_to - next;
                                next = skip_to;
                                request_next = next;
                                part_started = Instant::now();
                                failed = 0;
                                continue;
                            }
                            PlaylistVerdict::Listed => {}
                        }
                    }
                    failed += 1;
                    if part_started.elapsed() >= Duration::from_millis(self.args.abandon_ms) {
                        // No cache has the part. Count it as missing and read on.
                        self.report.parts_missing += 1;
                        scheduled_us +=
                            durations.get(&next).copied().unwrap_or(default_duration_us);
                        next += 1;
                        request_next = next;
                        part_started = Instant::now();
                        failed = 0;
                        continue;
                    }
                    self.fail_over(next, failed, detail).await;
                    continue;
                }
            };
            failed = 0;

            let arrival = response.arrived;
            let (origin, _) = *schedule.get_or_insert((arrival, next));
            if self.report.parts_received == 0 {
                self.report.startup_ms = arrival.duration_since(self.started).as_millis() as u64;
            }
            let since_origin_us = arrival.duration_since(origin).as_micros() as i64;
            let behind_us = since_origin_us - scheduled_us as i64;
            let behind_ms = behind_us.div_euclid(1_000);
            self.report.max_behind_ms = self.report.max_behind_ms.max(behind_ms);
            if behind_us > self.args.buffer_ms.saturating_mul(1_000) as i64 {
                self.report.underruns += 1;
            }
            // A player starts when it holds `buffer_ms` of media. From then
            // on it shows a part at its place in the schedule, plus every
            // stall so far.
            let part_us = durations.get(&next).copied().unwrap_or(default_duration_us);
            if play_from_us.is_none()
                && scheduled_us + part_us >= self.args.buffer_ms.saturating_mul(1_000)
            {
                play_from_us = Some(since_origin_us.max(0) as u64);
                self.report.playback_start_ms =
                    Some(arrival.duration_since(self.started).as_millis() as u64);
            }
            let late_us = match play_from_us {
                Some(play_from_us) => {
                    since_origin_us - (play_from_us + scheduled_us + stalls.shift_us) as i64
                }
                None => 0,
            };
            stalls.observe(
                &mut self.report,
                late_us,
                part_us,
                arrival.duration_since(self.started).as_millis() as u64,
                next,
            );
            if let Some(last) = last_arrival {
                self.report.max_gap_ms = self
                    .report
                    .max_gap_ms
                    .max(arrival.saturating_duration_since(last).as_millis() as u64);
            }
            last_arrival = Some(arrival);
            let header_u64 = |name: &str| {
                response
                    .headers
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
            };
            let cache_latency_ms = header_u64("x-needletail-cache-available-unix-us")
                .map(|available| (response.arrived_unix_us as f64 - available as f64) / 1_000.0);
            if let Some(latency) = cache_latency_ms {
                latencies.push(latency);
                // The part waited at the cache for longer than a round trip
                // and four parts: the reader is behind the live edge.
                let live_ms = self.path_round_trip().as_millis() as f64
                    + 4.0 * default_duration_us as f64 / 1_000.0
                    + 50.0;
                catching_up = latency > live_ms;
            }
            if let Some(published) = header_u64("x-needletail-published-unix-us") {
                publish_latencies
                    .push((response.arrived_unix_us as f64 - published as f64) / 1_000.0);
            }
            let etag = response
                .headers
                .get("etag")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            for byte in etag.as_deref().unwrap_or("").bytes() {
                digest ^= u64::from(byte);
                digest = digest.wrapping_mul(0x0000_0100_0000_01b3);
            }
            let cache = self.current_name();
            *self.report.parts_by_cache.entry(cache.clone()).or_default() += 1;
            self.report.parts_received += 1;
            self.received_from_current = true;
            self.report.bytes += response.body.len() as u64;
            self.report.last_part = next;
            if self.args.record_parts {
                self.report.parts.push(PartRecord {
                    part: next,
                    cache,
                    bytes: response.body.len(),
                    at_ms: arrival.duration_since(self.started).as_millis() as u64,
                    behind_ms,
                    cache_latency_ms,
                    etag,
                });
            }
            scheduled_us += durations.get(&next).copied().unwrap_or(default_duration_us);
            next += 1;
            part_started = Instant::now();
        }
        // Requests still on their way stay open until the reader goes.
        drop(inflight);

        self.report.round_trip_ms = self
            .round_trip
            .map(|value| (value.as_micros() as f64 / 1_000.0 * 10.0).round() / 10.0);
        publish_latencies.sort_by(f64::total_cmp);
        self.report.latency_p50_ms = percentile(&publish_latencies, 50.0);
        self.report.latency_p99_ms = percentile(&publish_latencies, 99.0);
        self.report.latency_max_ms = percentile(&publish_latencies, 100.0);
        self.report.final_cache = self.current_name();
        latencies.sort_by(f64::total_cmp);
        self.report.cache_latency_p50_ms = percentile(&latencies, 50.0);
        self.report.cache_latency_p99_ms = percentile(&latencies, 99.0);
        self.report.etag_digest = Some(format!("{digest:016x}"));
        if self.report.parts_received == 0 {
            bail!("no part was received");
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct Summary {
    readers: usize,
    readers_ok: usize,
    stream: u64,
    duration_seconds: u64,
    buffer_ms: u64,
    parts_received: u64,
    parts_missing: u64,
    underruns: u64,
    /// Readers that had at least one stall.
    readers_stalled: usize,
    /// Readers whose --pipeline-limit cut the --min-inflight-ms window.
    readers_pipeline_limited: usize,
    stalls: u64,
    stalled_ms: u64,
    stalled_us: u64,
    longest_stall_ms: u64,
    longest_stall_us: u64,
    max_behind_ms: i64,
    startup_ms_p50: Option<f64>,
    startup_ms_p99: Option<f64>,
    startup_ms_max: Option<f64>,
    playback_start_ms_p50: Option<f64>,
    playback_start_ms_max: Option<f64>,
    /// Over the readers: the median, and the worst, of each reader's 99th
    /// percentile from publication to arrival.
    latency_p99_ms_median: Option<f64>,
    latency_p99_ms_worst: Option<f64>,
    latency_p50_ms_median: Option<f64>,
    moves: usize,
    rejections: u64,
    request_errors: u64,
    /// Readers on each cache when the run ended.
    readers_by_final_cache: BTreeMap<String, usize>,
    /// Readers that each cache was first assigned.
    readers_by_first_cache: BTreeMap<String, usize>,
    parts_by_cache: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
struct Report {
    summary: Summary,
    readers: Vec<ReaderReport>,
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let connector = tls_connector(&args)?;
    let readers = args.readers.max(1);
    let mut tasks = Vec::with_capacity(readers);
    for index in 0..readers {
        let session = if readers == 1 {
            args.session.clone()
        } else {
            format!("{}-{index}", args.session)
        };
        let delay = if readers > 1 {
            Duration::from_millis(args.arrival_spread_ms * index as u64 / readers as u64)
        } else {
            Duration::ZERO
        };
        let args = args.clone();
        let connector = connector.clone();
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            Reader::new(args, connector, session).run().await
        }));
    }
    let mut reports = Vec::with_capacity(readers);
    for task in tasks {
        reports.push(task.await?);
    }

    let mut summary = Summary {
        readers,
        readers_ok: reports.iter().filter(|report| report.ok).count(),
        stream: args.stream,
        duration_seconds: args.duration_seconds,
        buffer_ms: args.buffer_ms,
        parts_received: 0,
        parts_missing: 0,
        underruns: 0,
        readers_stalled: reports.iter().filter(|report| report.stalls > 0).count(),
        readers_pipeline_limited: reports
            .iter()
            .filter(|report| report.pipeline_limited)
            .count(),
        stalls: 0,
        stalled_ms: 0,
        stalled_us: 0,
        longest_stall_ms: 0,
        longest_stall_us: 0,
        max_behind_ms: 0,
        startup_ms_p50: None,
        startup_ms_p99: None,
        startup_ms_max: None,
        playback_start_ms_p50: None,
        playback_start_ms_max: None,
        latency_p99_ms_median: None,
        latency_p99_ms_worst: None,
        latency_p50_ms_median: None,
        moves: 0,
        rejections: 0,
        request_errors: 0,
        readers_by_final_cache: BTreeMap::new(),
        readers_by_first_cache: BTreeMap::new(),
        parts_by_cache: BTreeMap::new(),
    };
    for report in &reports {
        summary.parts_received += report.parts_received;
        summary.parts_missing += report.parts_missing;
        summary.underruns += report.underruns;
        summary.stalls += report.stalls;
        summary.stalled_ms += report.stalled_ms;
        summary.stalled_us += report.stalled_us;
        summary.longest_stall_ms = summary.longest_stall_ms.max(report.longest_stall_ms);
        summary.longest_stall_us = summary.longest_stall_us.max(report.longest_stall_us);
        summary.request_errors += report.request_errors;
        summary.max_behind_ms = summary.max_behind_ms.max(report.max_behind_ms);
        summary.moves += report.moves.len();
        summary.rejections += report.rejections;
        if !report.final_cache.is_empty() {
            *summary
                .readers_by_final_cache
                .entry(report.final_cache.clone())
                .or_default() += 1;
        }
        if !report.first_cache.is_empty() {
            *summary
                .readers_by_first_cache
                .entry(report.first_cache.clone())
                .or_default() += 1;
        }
        for (cache, parts) in &report.parts_by_cache {
            *summary.parts_by_cache.entry(cache.clone()).or_default() += parts;
        }
    }
    let sorted = |values: Vec<f64>| {
        let mut values = values;
        values.sort_by(f64::total_cmp);
        values
    };
    let startups = sorted(
        reports
            .iter()
            .filter(|report| report.parts_received > 0)
            .map(|report| report.startup_ms as f64)
            .collect(),
    );
    summary.startup_ms_p50 = percentile(&startups, 50.0);
    summary.startup_ms_p99 = percentile(&startups, 99.0);
    summary.startup_ms_max = percentile(&startups, 100.0);
    let play_starts = sorted(
        reports
            .iter()
            .filter_map(|report| report.playback_start_ms)
            .map(|value| value as f64)
            .collect(),
    );
    summary.playback_start_ms_p50 = percentile(&play_starts, 50.0);
    summary.playback_start_ms_max = percentile(&play_starts, 100.0);
    let p99s = sorted(
        reports
            .iter()
            .filter_map(|report| report.latency_p99_ms)
            .collect(),
    );
    summary.latency_p99_ms_median = percentile(&p99s, 50.0);
    summary.latency_p99_ms_worst = percentile(&p99s, 100.0);
    let p50s = sorted(
        reports
            .iter()
            .filter_map(|report| report.latency_p50_ms)
            .collect(),
    );
    summary.latency_p50_ms_median = percentile(&p50s, 50.0);
    let all_ok = summary.readers_ok == readers;
    let json = serde_json::to_vec_pretty(&Report {
        summary,
        readers: reports,
    })?;
    match &args.report {
        Some(path) => {
            std::fs::write(path, &json).with_context(|| format!("write {}", path.display()))?
        }
        None => println!("{}", String::from_utf8_lossy(&json)),
    }
    if !all_ok {
        std::process::exit(2);
    }
    Ok(())
}

/// Parts to ask for at one time with --pipeline 0: a round trip and a half
/// of parts of `part_us`, and two more, and no less than `min_inflight_ms`
/// of parts; between two and `limit`.
fn pipeline_depth_for(
    round_trip: Duration,
    part_us: u64,
    min_inflight_ms: u64,
    limit: usize,
) -> usize {
    let part_us = part_us.max(1);
    let round_trip_us = round_trip.as_micros().min(u128::from(u64::MAX)) as u64;
    let ahead = (round_trip_us * 3 / 2) / part_us + 2;
    let window = min_inflight_ms.saturating_mul(1_000).div_ceil(part_us);
    usize::try_from(ahead.max(window))
        .unwrap_or(usize::MAX)
        .clamp(2, limit.max(2))
}

/// Connections to one cache that carry `depth` part requests and one
/// playlist reload with at most `streams_per_connection` requests each.
fn connections_for(depth: usize, streams_per_connection: usize) -> usize {
    depth
        .saturating_add(1)
        .div_ceil(streams_per_connection.max(2))
        .max(1)
}

/// Parts of `part_us` in two seconds, and no fewer than two.
fn parts_in_two_seconds(part_us: u64) -> usize {
    usize::try_from(2_000_000 / part_us.max(1))
        .unwrap_or(usize::MAX)
        .max(2)
}

/// The stall time of a cache that has sent a reader parts (B 3.10): the
/// larger of `stall_ms` and eight parts, and four round trips.
fn stall_timeout_for(stall_ms: u64, part_us: u64, round_trip: Duration) -> Duration {
    Duration::from_millis(stall_ms).max(Duration::from_micros(part_us.saturating_mul(8)))
        + round_trip * 4
}

/// How long a reader that left a vanished cache waits before it opens the
/// next one: 0 to 100 ms by its session, because the next cache is the same
/// for every reader of a playlist on a cache.
fn next_cache_delay(session: &str) -> Duration {
    // FNV-1a, the same in every build.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in session.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= hash >> 29;
    Duration::from_micros(hash % 100_000)
}

/// What a reader does next while it waits for a part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitStep {
    /// Wait for the part until then.
    Until(Instant),
    /// Ask the cache for its playlist, to be answered by then.
    AskPlaylist { by: Instant },
    /// The cache sent no byte and gave no answer for the stall time.
    Stalled,
    /// The request timeout passed (a first request, or a cache that
    /// answered its playlist).
    TimedOut,
}

/// The wait for one part (B 3.10). Before the cache has sent the reader a
/// part the request timeout alone bounds it. After, a silence of half the
/// stall time sends one playlist question to the cache (again after each
/// sign of life), and a silence of the stall time leaves the cache; a
/// cache that answers lives, and the request timeout bounds the wait again.
/// A question that fails is no sign of life: the wait then ends at the
/// stall time it was asked for, unless the part comes.
#[derive(Debug, Clone, Copy)]
struct PartWait {
    stall: Option<Duration>,
    timeout: Duration,
    asked: Option<Instant>,
    alive: bool,
    /// A question failed; the cache is left at this time.
    unanswered_by: Option<Instant>,
}

impl PartWait {
    fn new(stall: Option<Duration>, timeout: Duration) -> Self {
        Self {
            stall,
            timeout,
            asked: None,
            alive: false,
            unanswered_by: None,
        }
    }

    /// The next step at `now`, `since` being the later of the start of the
    /// wait and the last byte from any connection.
    fn step(&mut self, since: Instant, now: Instant) -> WaitStep {
        let Some(stall) = self.stall.filter(|_| !self.alive) else {
            let limit = since + self.timeout;
            return if now >= limit {
                WaitStep::TimedOut
            } else {
                WaitStep::Until(limit)
            };
        };
        if let Some(give_up) = self.unanswered_by {
            return if now >= give_up {
                WaitStep::Stalled
            } else {
                WaitStep::Until(give_up)
            };
        }
        let give_up = since + stall;
        if now >= give_up {
            return WaitStep::Stalled;
        }
        if self.asked.is_some_and(|asked| asked >= since) {
            return WaitStep::Until(give_up);
        }
        let ask_at = since + stall / 2;
        if now >= ask_at {
            self.asked = Some(now);
            WaitStep::AskPlaylist { by: give_up }
        } else {
            WaitStep::Until(ask_at)
        }
    }

    /// The cache answered its playlist: it lives.
    fn answered(&mut self) {
        self.alive = true;
    }

    /// The question asked to be answered `by` failed: no sign of life,
    /// whatever bytes its answer carried.
    fn unanswered(&mut self, by: Instant) {
        self.unanswered_by = Some(by);
    }
}

/// How the wait for a part ended.
enum Waited {
    /// The request finished.
    Done(std::result::Result<Result<HttpResponse>, tokio::task::JoinError>),
    /// The run or the reading ends.
    End,
    TimedOut,
    Stalled,
    /// The cache's playlist shows the part to be a hole.
    Hole(MediaPlaylist),
    /// The playlist question moved the reader to another cache.
    Moved,
}

/// What the playlist of the cache says of the part a reader waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaylistVerdict {
    /// Nothing at or after it is listed: the stream has stopped, for every
    /// cache alike. A player waits where it is.
    Stopped,
    /// Later parts are listed and it is not: it was lost, and a player
    /// plays this one next, from the same cache.
    SkipTo(u64),
    /// It is listed.
    Listed,
}

fn playlist_verdict(listed: &[(u64, u64)], wanted: u64) -> PlaylistVerdict {
    if let Some(skip_to) = part_after_a_hole(listed, wanted, true) {
        return PlaylistVerdict::SkipTo(skip_to);
    }
    let newest = listed.iter().map(|(sequence, _)| *sequence).max();
    if newest.is_none_or(|newest| newest < wanted) {
        PlaylistVerdict::Stopped
    } else {
        PlaylistVerdict::Listed
    }
}

/// The part that a reader goes on from when the part that it waits for will
/// not come: the first listed part after `wanted`, when `wanted` is overdue
/// and the playlist does not list it.
fn part_after_a_hole(listed: &[(u64, u64)], wanted: u64, overdue: bool) -> Option<u64> {
    if !overdue || listed.iter().any(|(sequence, _)| *sequence == wanted) {
        return None;
    }
    listed
        .iter()
        .map(|(sequence, _)| *sequence)
        .filter(|sequence| *sequence > wanted)
        .min()
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

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    /// B 3.10: the wait for a part. A cache that has sent the reader parts
    /// and then sends nothing is asked for its playlist once at half the
    /// stall time and left at the stall time (about 400 ms + 8 r, where the
    /// request timeout was 1,500 ms + 4 r); a cache that answers lives and
    /// the request timeout bounds the wait again; a question that fails is
    /// no sign of life, though its answer's bytes moved the last byte.
    #[test]
    fn the_part_wait_asks_once_at_half_the_stall_time() {
        let stall = stall_timeout_for(400, 1_000, ms(2));
        assert_eq!(stall, ms(408));
        // Eight long parts are more than 400 ms.
        assert_eq!(stall_timeout_for(400, 100_000, ms(2)), ms(808));
        let t0 = Instant::now();
        let mut wait = PartWait::new(Some(stall), ms(1_508));
        assert_eq!(wait.step(t0, t0), WaitStep::Until(t0 + ms(204)));
        assert_eq!(
            wait.step(t0, t0 + ms(204)),
            WaitStep::AskPlaylist { by: t0 + ms(408) }
        );
        // Asked once: no answer, and the stall time decides.
        assert_eq!(wait.step(t0, t0 + ms(250)), WaitStep::Until(t0 + ms(408)));
        assert_eq!(wait.step(t0, t0 + ms(408)), WaitStep::Stalled);
        // Bytes came after the question: the silence starts again, and so
        // does the question.
        let t1 = t0 + ms(300);
        let mut wait = PartWait::new(Some(stall), ms(1_508));
        assert!(matches!(
            wait.step(t0, t0 + ms(204)),
            WaitStep::AskPlaylist { .. }
        ));
        assert_eq!(wait.step(t1, t1 + ms(100)), WaitStep::Until(t1 + ms(204)));
        assert_eq!(
            wait.step(t1, t1 + ms(204)),
            WaitStep::AskPlaylist { by: t1 + ms(408) }
        );
        // The question failed with a body (a 503): its bytes moved the last
        // byte, but the cache is left at the stall time it was asked for,
        // and not asked again.
        let mut wait = PartWait::new(Some(stall), ms(1_508));
        let by = match wait.step(t0, t0 + ms(204)) {
            WaitStep::AskPlaylist { by } => by,
            other => panic!("{other:?}"),
        };
        wait.unanswered(by);
        let answered_at = t0 + ms(210);
        assert_eq!(wait.step(answered_at, t0 + ms(300)), WaitStep::Until(by));
        assert_eq!(wait.step(answered_at, by), WaitStep::Stalled);
        // An answer: the request timeout alone from then on.
        let mut wait = PartWait::new(Some(stall), ms(1_508));
        assert!(matches!(
            wait.step(t0, t0 + ms(204)),
            WaitStep::AskPlaylist { .. }
        ));
        wait.answered();
        assert_eq!(wait.step(t0, t0 + ms(408)), WaitStep::Until(t0 + ms(1_508)));
        assert_eq!(wait.step(t0, t0 + ms(1_508)), WaitStep::TimedOut);
        // Before the cache has sent a part, the request timeout alone.
        let mut first = PartWait::new(None, ms(1_508));
        assert_eq!(
            first.step(t0, t0 + ms(1_000)),
            WaitStep::Until(t0 + ms(1_508))
        );
        assert_eq!(first.step(t0, t0 + ms(1_508)), WaitStep::TimedOut);
        // What an answer says: the part is listed (the cache is slow), later
        // parts are and it is not (it is skipped, on this cache), or nothing
        // at or after it is (the stream has stopped).
        let listed = [(40, 1_000), (41, 1_000), (42, 1_000)];
        assert_eq!(playlist_verdict(&listed, 41), PlaylistVerdict::Listed);
        let holed = [(40, 1_000), (42, 1_000), (43, 1_000)];
        assert_eq!(playlist_verdict(&holed, 41), PlaylistVerdict::SkipTo(42));
        let quiet = [(38, 1_000), (39, 1_000), (40, 1_000)];
        assert_eq!(playlist_verdict(&quiet, 41), PlaylistVerdict::Stopped);
        assert_eq!(playlist_verdict(&[], 41), PlaylistVerdict::Stopped);
    }

    /// What a test cache does from the moment the test changes it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CacheMode {
        /// A part every 10 ms; a request for a later part waits for it.
        Serving,
        /// The host vanished: nothing is answered.
        Silent,
        /// The stream stopped: the playlist lists nothing newer.
        Stopped,
        /// The cache lists the parts and sends none (a slow cache).
        Listing,
        /// The playlist is refused with a 503 and a body.
        Refusing,
    }

    const TEST_PART_MS: u64 = 10;

    /// An LL-HLS cache for the reader's tests: HTTP/2 over TLS on
    /// 127.0.0.1, serving the master playlist that names both test caches,
    /// a media playlist and 10 ms parts from its start, and counting the
    /// connections it accepts.
    struct TestCache {
        authority: String,
        /// The mode, and the newest part when it was set.
        mode: Arc<std::sync::Mutex<(CacheMode, u64)>>,
        accepted: Arc<std::sync::Mutex<Vec<Instant>>>,
        epoch: Instant,
    }

    impl TestCache {
        fn head(&self) -> u64 {
            self.epoch.elapsed().as_millis() as u64 / TEST_PART_MS
        }

        fn set(&self, mode: CacheMode) {
            *self.mode.lock().unwrap() = (mode, self.head());
        }

        fn accepted(&self) -> Vec<Instant> {
            self.accepted.lock().unwrap().clone()
        }
    }

    fn test_tls() -> (tokio_rustls::TlsAcceptor, TlsConnector) {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()),
        );
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![certified.cert.der().clone()], key)
            .unwrap();
        server.alpn_protocols = vec![b"h2".to_vec()];
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certified.cert.der().clone()).unwrap();
        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client.alpn_protocols = vec![b"h2".to_vec()];
        (
            tokio_rustls::TlsAcceptor::from(Arc::new(server)),
            TlsConnector::from(Arc::new(client)),
        )
    }

    /// The answer of a test cache to `path`, in `mode` with the newest
    /// part `head`. `None`: not answered yet.
    fn test_answer(
        path: &str,
        master: &str,
        mode: CacheMode,
        head: u64,
    ) -> Option<(StatusCode, String)> {
        let playlist = |newest: u64| {
            let mut body = String::from("#EXTM3U\n#EXT-X-PART-INF:PART-TARGET=0.010\n");
            for sequence in newest.saturating_sub(30)..=newest {
                body.push_str(&format!(
                    "#EXT-X-PART:DURATION=0.010,URI=\"part{sequence}.bin\"\n"
                ));
            }
            body
        };
        if path.ends_with("master.m3u8") {
            return Some((StatusCode::OK, master.to_owned()));
        }
        if path.ends_with("stream.m3u8") {
            return match mode {
                CacheMode::Serving | CacheMode::Stopped => Some((StatusCode::OK, playlist(head))),
                CacheMode::Listing => Some((StatusCode::OK, playlist(head + 20))),
                CacheMode::Refusing => Some((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "the cache admits no request now\n".repeat(8),
                )),
                CacheMode::Silent => None,
            };
        }
        let (part, _) = part_sequence(path)?;
        if mode == CacheMode::Silent || part > head {
            return None;
        }
        Some((StatusCode::OK, format!("part {part} ").repeat(20)))
    }

    /// Two test caches, A and B; the master playlist names A first.
    async fn test_caches() -> (TestCache, TestCache, TlsConnector) {
        let (acceptor, connector) = test_tls();
        let listeners = [
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        ];
        let authorities = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string())
            .collect::<Vec<_>>();
        let master = Arc::new(format!(
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100000\nhttps://{}/live/1/stream.m3u8\n\
             #EXT-X-STREAM-INF:BANDWIDTH=100000\nhttps://{}/live/1/stream.m3u8\n",
            authorities[0], authorities[1]
        ));
        let epoch = Instant::now();
        let mut caches = Vec::new();
        for (listener, authority) in listeners.into_iter().zip(authorities) {
            let cache = TestCache {
                authority,
                mode: Arc::new(std::sync::Mutex::new((CacheMode::Serving, 0))),
                accepted: Arc::new(std::sync::Mutex::new(Vec::new())),
                epoch,
            };
            let (mode, accepted, acceptor, master) = (
                Arc::clone(&cache.mode),
                Arc::clone(&cache.accepted),
                acceptor.clone(),
                Arc::clone(&master),
            );
            tokio::spawn(async move {
                while let Ok((tcp, _)) = listener.accept().await {
                    accepted.lock().unwrap().push(Instant::now());
                    let (mode, acceptor, master) =
                        (Arc::clone(&mode), acceptor.clone(), Arc::clone(&master));
                    tokio::spawn(async move {
                        let Ok(tls) = acceptor.accept(tcp).await else {
                            return;
                        };
                        let service = hyper::service::service_fn(
                            move |request: Request<hyper::body::Incoming>| {
                                let (mode, master) = (Arc::clone(&mode), Arc::clone(&master));
                                async move {
                                    let path = request.uri().path().to_owned();
                                    let (status, body) = loop {
                                        let (now_mode, frozen) = *mode.lock().unwrap();
                                        let head = match now_mode {
                                            CacheMode::Serving => {
                                                epoch.elapsed().as_millis() as u64 / TEST_PART_MS
                                            }
                                            _ => frozen,
                                        };
                                        if let Some(answer) =
                                            test_answer(&path, &master, now_mode, head)
                                        {
                                            break answer;
                                        }
                                        tokio::time::sleep(Duration::from_millis(2)).await;
                                    };
                                    let mut response = http::Response::new(
                                        http_body_util::Full::new(Bytes::from(body)),
                                    );
                                    *response.status_mut() = status;
                                    Ok::<_, std::convert::Infallible>(response)
                                }
                            },
                        );
                        let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                            .serve_connection(TokioIo::new(tls), service)
                            .await;
                    });
                }
            });
            caches.push(cache);
        }
        let b = caches.pop().unwrap();
        let a = caches.pop().unwrap();
        (a, b, connector)
    }

    /// A reader of the test caches with the default stall time; no
    /// playlist reload during the run, so that only the wait for a part
    /// judges the cache.
    fn test_reader(a: &TestCache, connector: TlsConnector, session: &str, seconds: u64) -> Reader {
        let args = Args::try_parse_from([
            "needletail-reader",
            "--entry",
            &format!("https://{}", a.authority),
            "--tls-ca",
            "unused.pem",
            "--server-name",
            "localhost",
            "--session",
            session,
            "--duration-seconds",
            &seconds.to_string(),
            "--playlist-interval-ms",
            "60000",
        ])
        .unwrap();
        Reader::new(args, connector, session.to_owned())
    }

    /// Milliseconds from the reader's start to `at`.
    fn reader_ms(reader_started: Instant, at: Instant) -> u64 {
        at.saturating_duration_since(reader_started).as_millis() as u64
    }

    /// B 3.10: the host of a cache that has sent the reader parts vanishes.
    /// The reader leaves it at the stall time (400 ms on a local path), not
    /// the request timeout (1,500 ms), aborts what it asked there, and
    /// opens the next cache after its spread delay; the parts come from
    /// there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cache_that_stops_sending_is_left_after_the_stall_time() {
        let (a, b, connector) = test_caches().await;
        let session = "vanish-3";
        let reader = test_reader(&a, connector, session, 3);
        let started = reader.started;
        let task = tokio::spawn(reader.run());
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        a.set(CacheMode::Silent);
        let stopped = reader_ms(started, Instant::now());
        let report = task.await.unwrap();
        assert!(report.ok, "{:?}", report.error);
        let moved = report.moves.first().expect("the reader left the cache");
        assert_eq!(
            (
                moved.reason.as_str(),
                moved.from.as_str(),
                moved.to.as_str()
            ),
            ("failure", a.authority.as_str(), b.authority.as_str()),
            "{:?}",
            report.moves
        );
        // Left at the stall time after the last byte, with slack for a
        // loaded machine; the request timeout would be 1,500 ms.
        let after = moved.at_ms.saturating_sub(stopped);
        assert!(
            (380..=900).contains(&after),
            "left {after} ms after the stop"
        );
        // The next cache is opened after the spread delay, and serves.
        let opened = b.accepted();
        assert_eq!(opened.len(), 1, "{opened:?}");
        let delay = next_cache_delay(session).as_millis() as u64;
        assert!(
            reader_ms(started, opened[0]) + 1 >= moved.at_ms + delay,
            "opened at {} ms, left at {} ms, delay {delay} ms",
            reader_ms(started, opened[0]),
            moved.at_ms
        );
        assert!(
            report
                .parts_by_cache
                .get(&b.authority)
                .copied()
                .unwrap_or(0)
                > 50
        );
        assert_eq!(report.final_cache, b.authority);
    }

    /// B 3.10: a cache that answers its playlist with the part listed is
    /// slow, not gone: the reader keeps it past the stall time, until the
    /// request timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cache_that_answers_its_playlist_is_kept() {
        let (a, _b, connector) = test_caches().await;
        let reader = test_reader(&a, connector, "slow-1", 3);
        let started = reader.started;
        let task = tokio::spawn(reader.run());
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        a.set(CacheMode::Listing);
        let stopped = reader_ms(started, Instant::now());
        let report = task.await.unwrap();
        assert!(report.ok, "{:?}", report.error);
        // No move before the request timeout (1,500 ms after the last
        // byte); the stall time (400 ms) passed long before.
        if let Some(moved) = report.moves.first() {
            assert!(
                moved.at_ms >= stopped + 1_400,
                "left {} ms after the stop: {:?}",
                moved.at_ms - stopped,
                report.moves
            );
        }
    }

    /// B 3.10: a pause of the stream is answered by the cache (nothing
    /// newer listed): the reader waits where it is, and no connection
    /// opens anywhere, on its cache or another.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_quiet_stream_opens_no_connection() {
        let (a, b, connector) = test_caches().await;
        let reader = test_reader(&a, connector, "quiet-1", 2);
        let task = tokio::spawn(reader.run());
        tokio::time::sleep(Duration::from_millis(800)).await;
        a.set(CacheMode::Stopped);
        let report = task.await.unwrap();
        assert!(report.ok, "{:?}", report.error);
        assert!(report.moves.is_empty(), "{:?}", report.moves);
        assert_eq!(report.final_cache, a.authority);
        // The master playlist, the media playlist, the parts and the
        // question all went on one connection.
        assert_eq!(a.accepted().len(), 1);
        assert!(b.accepted().is_empty());
    }

    /// Review of B Z4: a cache that refuses the playlist question with a
    /// body (a 503) gave no sign of life: the reader leaves it at the stall
    /// time, though the refusal's bytes arrived after the last part.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cache_that_refuses_its_playlist_is_left_at_the_stall_time() {
        let (a, b, connector) = test_caches().await;
        let reader = test_reader(&a, connector, "refused-1", 3);
        let started = reader.started;
        let task = tokio::spawn(reader.run());
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        a.set(CacheMode::Refusing);
        let stopped = reader_ms(started, Instant::now());
        let report = task.await.unwrap();
        assert!(report.ok, "{:?}", report.error);
        let moved = report.moves.first().expect("the reader left the cache");
        assert_eq!(
            (moved.reason.as_str(), moved.to.as_str()),
            ("failure", b.authority.as_str()),
            "{:?}",
            report.moves
        );
        let after = moved.at_ms.saturating_sub(stopped);
        assert!(
            (380..=900).contains(&after),
            "left {after} ms after the stop"
        );
    }

    /// B 3.10: the readers that leave a vanished cache all go to the same
    /// next cache; each opens it after 0 to 100 ms by its session.
    #[test]
    fn readers_open_the_next_cache_spread_over_100_ms() {
        let mut buckets = [0_usize; 10];
        for index in 0..2_000 {
            let session = format!("reader-{index}");
            let delay = next_cache_delay(&session);
            assert!(delay < ms(100), "{session}: {delay:?}");
            assert_eq!(delay, next_cache_delay(&session));
            buckets[(delay.as_millis() / 10) as usize] += 1;
        }
        // 200 a bucket of 10 ms if even.
        assert!(
            buckets.iter().all(|count| (140..=260).contains(count)),
            "{buckets:?}"
        );
    }

    #[test]
    fn url_references_resolve_like_a_player() {
        let base = Url::parse("https://10.0.0.1:19444/live/7/master.m3u8?sid=a").unwrap();
        assert_eq!(base.path(), "/live/7/master.m3u8");
        assert_eq!(
            base.join("stream.m3u8").unwrap().to_string(),
            "https://10.0.0.1:19444/live/7/stream.m3u8"
        );
        assert_eq!(
            base.join("steering.json?sid=a").unwrap().to_string(),
            "https://10.0.0.1:19444/live/7/steering.json?sid=a"
        );
        assert_eq!(
            base.join("/live/8/stream.m3u8").unwrap().to_string(),
            "https://10.0.0.1:19444/live/8/stream.m3u8"
        );
        assert_eq!(
            base.join("https://10.0.0.2:19444/live/7/stream.m3u8")
                .unwrap()
                .authority,
            "10.0.0.2:19444"
        );
        assert_eq!(
            Url::parse("https://edge")
                .unwrap()
                .with_query("a", "1")
                .with_query("b", "2")
                .to_string(),
            "https://edge/?a=1&b=2"
        );
        assert_eq!(base.host_port(), ("10.0.0.1".to_owned(), 19444));
    }

    #[test]
    fn master_playlist_lists_the_assigned_cache_first() {
        let base = Url::parse("https://10.0.0.1:19444/live/7/master.m3u8?sid=a").unwrap();
        let master = parse_master(
            "#EXTM3U\n#EXT-X-VERSION:10\n\
             #EXT-X-CONTENT-STEERING:SERVER-URI=\"steering.json?sid=a\",PATHWAY-ID=\"edge-2\"\n\
             #EXT-X-STREAM-INF:BANDWIDTH=16000000,CODECS=\"avc1.640033,mp4a.40.2\",PATHWAY-ID=\"edge-2\"\n\
             https://10.0.0.2:19444/live/7/stream.m3u8\n\
             #EXT-X-STREAM-INF:BANDWIDTH=16000000,PATHWAY-ID=\"edge-1\"\n\
             stream.m3u8\n",
            &base,
        )
        .unwrap();
        assert_eq!(master.caches.len(), 2);
        assert_eq!(master.caches[0].name, "edge-2");
        assert_eq!(master.caches[0].playlist.authority, "10.0.0.2:19444");
        assert_eq!(
            master.caches[1].playlist.to_string(),
            "https://10.0.0.1:19444/live/7/stream.m3u8"
        );
        assert_eq!(
            master.steering.unwrap().to_string(),
            "https://10.0.0.1:19444/live/7/steering.json?sid=a"
        );

        // A playlist without pathways names each cache by its address.
        let plain = parse_master(
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=4000000\nstream.m3u8\n",
            &base,
        )
        .unwrap();
        assert_eq!(plain.caches[0].name, "10.0.0.1:19444");
        assert!(parse_master("#EXTM3U\n", &base).is_err());
    }

    #[test]
    fn a_reader_asks_no_more_than_two_seconds_ahead() {
        assert_eq!(parts_in_two_seconds(200_000), 10);
        assert_eq!(parts_in_two_seconds(5_000), 400);
        assert_eq!(parts_in_two_seconds(2_500), 800);
        assert_eq!(parts_in_two_seconds(250), 8_000);
        assert_eq!(parts_in_two_seconds(2_000_000), 2);
        assert_eq!(parts_in_two_seconds(0), 2_000_000);
    }

    #[test]
    fn a_reader_goes_on_from_the_next_listed_part_after_a_hole() {
        let listed = [(3_790, 208), (3_791, 208), (3_792, 125)];
        // Parts 3741 to 3789 were lost while the region had no source.
        assert_eq!(part_after_a_hole(&listed, 3_741, true), Some(3_790));
        // A part that is not overdue is waited for.
        assert_eq!(part_after_a_hole(&listed, 3_741, false), None);
        // A listed part will come, and so will the part after the newest.
        assert_eq!(part_after_a_hole(&listed, 3_791, true), None);
        assert_eq!(part_after_a_hole(&listed, 3_793, true), None);
        assert_eq!(part_after_a_hole(&[], 3_741, true), None);
    }

    #[test]
    fn media_playlist_durations_are_exact_microseconds() {
        // 2.5 ms Opus, 1 ms, 0.5 ms and 0.25 ms PCM, and a 23.976 fps frame:
        // no rounding to whole milliseconds, which made 2.5 ms parts 3 ms
        // and 0.25 ms parts 0.
        let playlist = parse_media(
            "#EXTM3U\n#EXT-X-PART-INF:PART-TARGET=0.0025\n\
             #EXT-X-PART:DURATION=0.0025,URI=\"part1.bin\"\n\
             #EXT-X-PART:DURATION=0.001,URI=\"part2.bin\"\n\
             #EXT-X-PART:DURATION=0.0005,URI=\"part3.bin\"\n\
             #EXT-X-PART:DURATION=0.00025,URI=\"part4.bin\"\n\
             #EXT-X-PART:DURATION=0.041708,URI=\"part5.bin\"\n\
             #EXT-X-PART:URI=\"part6.bin\"\n",
        );
        assert_eq!(playlist.part_target_us, 2_500);
        assert_eq!(
            playlist.parts,
            vec![
                (1, 2_500),
                (2, 1_000),
                (3, 500),
                (4, 250),
                (5, 41_708),
                // A part without a duration has the part target.
                (6, 2_500),
            ]
        );
        // Four hundred 2.5 ms parts make one second exactly.
        let second: u64 = std::iter::repeat_n(2_500_u64, 400).sum();
        assert_eq!(second, 1_000_000);
    }

    #[test]
    fn a_reader_keeps_at_least_a_quarter_second_of_parts_in_flight() {
        let near = Duration::from_millis(1);
        // With the default limit, every part size down to 0.25 ms has
        // 250 ms of parts on their way, on connections of at most 256
        // requests each (one kept for the playlist reload).
        for part_us in [41_708, 5_000, 2_500, 1_000, 500, 250] {
            let depth = pipeline_depth_for(near, part_us, 250, DEFAULT_PIPELINE_LIMIT);
            assert!(depth as u64 * part_us >= 250_000, "{part_us} µs: {depth}");
            let lanes = connections_for(depth, DEFAULT_STREAMS_PER_CONNECTION);
            assert!(
                (depth + 1).div_ceil(lanes) <= DEFAULT_STREAMS_PER_CONNECTION,
                "{part_us} µs: {depth} parts on {lanes} connections"
            );
        }
        // 2.5 ms parts: 100 parts are 250 ms; a round trip alone asks for 2.
        assert_eq!(pipeline_depth_for(near, 2_500, 250, 256), 100);
        // 1 ms parts: 250 parts, on one connection.
        assert_eq!(pipeline_depth_for(near, 1_000, 250, 256), 250);
        assert_eq!(connections_for(250, 256), 1);
        // 0.5 ms parts: 500 parts on two connections; 0.25 ms parts: 1,000
        // on four.
        assert_eq!(
            pipeline_depth_for(near, 500, 250, DEFAULT_PIPELINE_LIMIT),
            500
        );
        assert_eq!(connections_for(500, 256), 2);
        assert_eq!(
            pipeline_depth_for(near, 250, 250, DEFAULT_PIPELINE_LIMIT),
            1_000
        );
        assert_eq!(connections_for(1_000, 256), 4);
        // An explicit lower limit still caps it (and the report says so).
        assert_eq!(pipeline_depth_for(near, 250, 250, 256), 256);
        assert_eq!(pipeline_depth_for(near, 250, 250, 2_000), 1_000);
        // Exactly 256 requests and a reload need two connections.
        assert_eq!(connections_for(255, 256), 1);
        assert_eq!(connections_for(256, 256), 2);
        // A far reader covers its round trip and a half when that is more.
        let far = Duration::from_millis(400);
        assert_eq!(pipeline_depth_for(far, 2_500, 250, 1_000), 242);
        // 200 ms video parts: a round trip and two, at least two.
        assert_eq!(pipeline_depth_for(near, 200_000, 250, 256), 2);
        // The old behaviour: no time window.
        assert_eq!(pipeline_depth_for(near, 2_500, 0, 256), 2);
    }

    #[test]
    fn a_part_late_by_less_than_a_millisecond_is_a_stall_when_it_is_short() {
        // 0.25 ms parts 0.75 ms late: three parts of silence, a stall.
        let mut report = ReaderReport::default();
        let mut stalls = StallTracker::default();
        stalls.observe(&mut report, 750, 250, 10, 40);
        assert_eq!(report.stalls, 1);
        assert_eq!(report.stalled_us, 750);
        assert_eq!(report.stalled_ms, 0);
        assert_eq!(report.stall_events[0].us, 750);
        assert_eq!(stalls.shift_us, 750);
        // The next part is late by the same run: the same stall, longer.
        stalls.observe(&mut report, 300, 250, 11, 41);
        assert_eq!(report.stalls, 1);
        assert_eq!(report.longest_stall_us, 1_050);
        assert_eq!(report.stall_events[0].ms, 1);
        // On time ends it; within half a part is on time.
        stalls.observe(&mut report, 124, 250, 12, 42);
        stalls.observe(&mut report, 125, 250, 13, 43);
        assert_eq!(report.stalls, 2);
        assert_eq!(report.stalled_us, 1_175);

        // A part of 2 ms or more keeps the millisecond of slack it had.
        let mut report = ReaderReport::default();
        let mut stalls = StallTracker::default();
        stalls.observe(&mut report, 999, 2_500, 0, 1);
        stalls.observe(&mut report, 999, 41_708, 0, 2);
        assert_eq!(report.stalls, 0);
        stalls.observe(&mut report, 1_000, 2_500, 0, 3);
        assert_eq!(report.stalls, 1);
        assert_eq!(stall_threshold_us(1_000), 500);
        assert_eq!(stall_threshold_us(500), 250);
        assert_eq!(stall_threshold_us(1), 1);
    }

    #[test]
    fn media_playlist_parts_carry_sequence_and_duration() {
        let playlist = parse_media(
            "#EXTM3U\n#EXT-X-PART-INF:PART-TARGET=0.250\n\
             #EXT-X-PART:DURATION=0.250,URI=\"part40.mp4\",INDEPENDENT=YES\n\
             #EXT-X-PART:DURATION=0.200,URI=\"part41.mp4\"\n\
             #EXTINF:1.000,\nseg10.mp4\n\
             #EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part42.mp4\"\n",
        );
        assert_eq!(playlist.parts, vec![(40, 250_000), (41, 200_000)]);
        assert_eq!(playlist.extension, "mp4");
        assert_eq!(playlist.part_target_us, 250_000);
        assert_eq!(part_sequence("part7.bin"), Some((7, "bin")));
        assert_eq!(part_sequence("seg7.mp4"), None);
        assert_eq!(
            attribute("#EXT-X-PART:DURATION=0.005,URI=\"a,b.mp4\",X=1", "URI"),
            Some("a,b.mp4")
        );
    }
}
