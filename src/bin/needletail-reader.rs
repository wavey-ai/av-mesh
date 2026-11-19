//! An LL-HLS reader that follows the assignment of a Needletail entry point.
//!
//! The reader asks an entry point for the Multivariant Playlist of a stream.
//! The playlist names the cache that the reader is assigned to and the caches
//! that follow it. The reader then reads the parts of the media playlist from
//! the assigned cache, one request for each part, as a player does.
//!
//! It moves to another cache in three cases, and records each move:
//!
//! - the cache fails: a request times out, the connection breaks, or the part
//!   is not there. The reader uses the next cache of its list.
//! - the cache drains: a playlist reload is answered with a redirect.
//! - the entry point steers it: the Content Steering manifest puts another
//!   cache first.
//!
//! The report says, for each reader, which cache served each part, every
//! move, and whether a part arrived after the moment a player with the given
//! buffer would have needed it. A run with `--readers N` is N sessions.

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use clap::Parser;
use http::{header::HeaderValue, Request, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "needletail-reader",
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
    /// a part that is not published yet.
    #[arg(long, default_value_t = 1_500)]
    request_timeout_ms: u64,

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
}

/// One HTTP/2 connection for each cache, as a player keeps.
struct HttpClient {
    connector: TlsConnector,
    server_name: Option<String>,
    session: String,
    connections: HashMap<String, SendRequest<Empty<Bytes>>>,
}

impl HttpClient {
    async fn connection(&mut self, url: &Url) -> Result<SendRequest<Empty<Bytes>>> {
        if let Some(connection) = self.connections.get(&url.authority) {
            if !connection.is_closed() {
                return Ok(connection.clone());
            }
            self.connections.remove(&url.authority);
        }
        let (host, port) = url.host_port();
        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("connect {}", url.authority))?;
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
        self.connections
            .insert(url.authority.clone(), sender.clone());
        Ok(sender)
    }

    async fn get(&mut self, url: &Url, timeout: Duration) -> Result<HttpResponse> {
        let result = tokio::time::timeout(timeout, self.get_once(url)).await;
        match result {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => {
                self.connections.remove(&url.authority);
                Err(error)
            }
            Err(_) => {
                // A request that timed out can leave a stalled connection.
                self.connections.remove(&url.authority);
                Err(anyhow!("timeout after {} ms", timeout.as_millis()))
            }
        }
    }

    async fn get_once(&mut self, url: &Url) -> Result<HttpResponse> {
        let mut sender = self.connection(url).await?;
        let request = Request::builder()
            .method("GET")
            .uri(format!("https://{}{}", url.authority, url.path_and_query))
            .header(
                "cmcd-session",
                HeaderValue::from_str(&format!("sid=\"{}\"", self.session))?,
            )
            .body(Empty::<Bytes>::new())?;
        let response = sender.send_request(request).await?;
        let (parts, body) = response.into_parts();
        let body = body.collect().await?.to_bytes();
        Ok(HttpResponse {
            status: parts.status,
            headers: parts.headers,
            body,
        })
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
    /// Part sequence and duration in milliseconds, oldest first.
    parts: Vec<(u64, u64)>,
    extension: String,
    part_target_ms: u64,
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
            playlist.part_target_ms = attribute(line, "PART-TARGET")
                .and_then(|value| value.parse::<f64>().ok())
                .map_or(0, |seconds| (seconds * 1_000.0).round() as u64);
        } else if line.starts_with("#EXT-X-PART:") {
            let Some((sequence, extension)) = attribute(line, "URI").and_then(part_sequence) else {
                continue;
            };
            let duration_ms = attribute(line, "DURATION")
                .and_then(|value| value.parse::<f64>().ok())
                .map_or(playlist.part_target_ms, |seconds| {
                    (seconds * 1_000.0).round() as u64
                });
            playlist.extension = extension.to_owned();
            playlist.parts.push((sequence, duration_ms));
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
    startup_ms: u64,
    first_part: u64,
    last_part: u64,
    parts_received: u64,
    parts_missing: u64,
    bytes: u64,
    /// Parts that arrived later than the playout buffer allows.
    underruns: u64,
    max_behind_ms: i64,
    max_gap_ms: u64,
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
}

impl Reader {
    fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(self.args.request_timeout_ms)
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
            match self.http.get(&url, self.timeout()).await {
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
            let next = (self.current + 1) % self.master.caches.len();
            self.record_move(part, next, "failure", detail);
        }
    }

    async fn load_playlist(&mut self, part: u64) -> Result<Option<MediaPlaylist>> {
        let url = self.master.caches[self.current].playlist.clone();
        let response = self.http.get(&url, self.timeout()).await?;
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
        loop {
            match self.load_master().await {
                Ok(()) => break,
                Err(error) if Instant::now() + Duration::from_secs(1) < deadline => {
                    let _ = error;
                    tokio::time::sleep(Duration::from_millis(500)).await;
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
        let mut default_duration_ms = playlist
            .parts
            .last()
            .map_or(playlist.part_target_ms, |(_, duration)| *duration)
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
        let mut scheduled_ms = 0_u64;
        let mut last_arrival: Option<Instant> = None;
        let mut latencies = Vec::new();
        let mut digest = 0xcbf2_9ce4_8422_2325_u64;
        let mut failed = 0_usize;
        let mut part_started = Instant::now();

        while Instant::now() < deadline {
            if self.args.steering && Instant::now() >= self.steering_due {
                self.poll_steering(next).await;
            }
            if Instant::now() >= self.playlist_due {
                self.playlist_due =
                    Instant::now() + Duration::from_millis(self.args.playlist_interval_ms);
                match self.load_playlist(next).await {
                    Ok(Some(loaded)) => {
                        for (sequence, duration) in &loaded.parts {
                            durations.insert(*sequence, *duration);
                        }
                        if let Some((_, duration)) = loaded.parts.last() {
                            default_duration_ms = (*duration).max(1);
                        }
                        durations.retain(|sequence, _| *sequence + 4_096 > next);
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

            let playlist_url = self.master.caches[self.current].playlist.clone();
            let part_url = playlist_url.join(&format!("part{next}.{extension}"))?;
            let outcome = self.http.get(&part_url, self.timeout()).await;
            let response = match outcome {
                Ok(response) if response.status == StatusCode::OK && !response.body.is_empty() => {
                    response
                }
                Ok(response) if response.status == StatusCode::TOO_MANY_REQUESTS => {
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
                    let detail = match other {
                        Ok(response) => format!("part {next} answered {}", response.status),
                        Err(error) => format!("part {next}: {error:#}"),
                    };
                    failed += 1;
                    if part_started.elapsed() >= Duration::from_millis(self.args.abandon_ms) {
                        // No cache has the part. Count it as missing and read on.
                        self.report.parts_missing += 1;
                        scheduled_ms +=
                            durations.get(&next).copied().unwrap_or(default_duration_ms);
                        next += 1;
                        part_started = Instant::now();
                        failed = 0;
                        continue;
                    }
                    self.fail_over(next, failed, detail).await;
                    continue;
                }
            };
            failed = 0;

            let arrival = Instant::now();
            let (origin, _) = *schedule.get_or_insert((arrival, next));
            if self.report.parts_received == 0 {
                self.report.startup_ms = self.elapsed_ms();
            }
            let behind_ms = arrival.duration_since(origin).as_millis() as i64 - scheduled_ms as i64;
            self.report.max_behind_ms = self.report.max_behind_ms.max(behind_ms);
            if behind_ms > self.args.buffer_ms as i64 {
                self.report.underruns += 1;
            }
            if let Some(last) = last_arrival {
                self.report.max_gap_ms = self
                    .report
                    .max_gap_ms
                    .max(arrival.duration_since(last).as_millis() as u64);
            }
            last_arrival = Some(arrival);
            let cache_latency_ms = response
                .headers
                .get("x-needletail-cache-available-unix-us")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(|available| (now_unix_us() as f64 - available as f64) / 1_000.0);
            if let Some(latency) = cache_latency_ms {
                latencies.push(latency);
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
            self.report.bytes += response.body.len() as u64;
            self.report.last_part = next;
            if self.args.record_parts {
                self.report.parts.push(PartRecord {
                    part: next,
                    cache,
                    bytes: response.body.len(),
                    at_ms: self.elapsed_ms(),
                    behind_ms,
                    cache_latency_ms,
                    etag,
                });
            }
            scheduled_ms += durations.get(&next).copied().unwrap_or(default_duration_ms);
            next += 1;
            part_started = Instant::now();
        }

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
    max_behind_ms: i64,
    moves: usize,
    rejections: u64,
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
            let now = Instant::now();
            let reader = Reader {
                http: HttpClient {
                    connector,
                    server_name: args.server_name.clone(),
                    session: session.clone(),
                    connections: HashMap::new(),
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
                args,
            };
            reader.run().await
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
        max_behind_ms: 0,
        moves: 0,
        rejections: 0,
        readers_by_final_cache: BTreeMap::new(),
        readers_by_first_cache: BTreeMap::new(),
        parts_by_cache: BTreeMap::new(),
    };
    for report in &reports {
        summary.parts_received += report.parts_received;
        summary.parts_missing += report.parts_missing;
        summary.underruns += report.underruns;
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn media_playlist_parts_carry_sequence_and_duration() {
        let playlist = parse_media(
            "#EXTM3U\n#EXT-X-PART-INF:PART-TARGET=0.250\n\
             #EXT-X-PART:DURATION=0.250,URI=\"part40.mp4\",INDEPENDENT=YES\n\
             #EXT-X-PART:DURATION=0.200,URI=\"part41.mp4\"\n\
             #EXTINF:1.000,\nseg10.mp4\n\
             #EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part42.mp4\"\n",
        );
        assert_eq!(playlist.parts, vec![(40, 250), (41, 200)]);
        assert_eq!(playlist.extension, "mp4");
        assert_eq!(playlist.part_target_ms, 250);
        assert_eq!(part_sequence("part7.bin"), Some((7, "bin")));
        assert_eq!(part_sequence("seg7.mp4"), None);
        assert_eq!(
            attribute("#EXT-X-PART:DURATION=0.005,URI=\"a,b.mp4\",X=1", "URI"),
            Some("a,b.mp4")
        );
    }
}
