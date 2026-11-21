//! A client of a Needletail stream by RaptorQ FEC UDP from an edge.
//!
//! The client is assigned an edge exactly as an LL-HLS reader is: it asks an
//! entry point for an assignment (`/api/assignment`), which names the
//! assigned cache and the caches that follow it. It reads the media playlist
//! of its stream from the assigned cache, which names the stream and its
//! parts, and subscribes to the stream at the cache's client UDP port
//! (`av_mesh::client_udp`). It receives each part as one RaptorQ object.
//!
//! It moves to the next cache of its assignment when the cache tells it to
//! (`MOVE`: the cache drains or has lost its parents), refuses it, or sends
//! nothing for `--silence-ms`. It asks the next cache for the part that it
//! needs next, so that a move loses no part that the region still holds.
//!
//! With `--verify-http` it also fetches every part that it received by UDP
//! from the same cache over HTTP, as an LL-HLS player would, and compares the
//! bytes. The report says how many parts arrived, how many the FEC
//! recovered, how many the cache skipped (gaps) and how many never arrived,
//! every move, and the time from publication to arrival.

use anyhow::{anyhow, bail, Context, Result};
use av_mesh::client_udp::{
    Control, ObjectKind, ObjectReceiver, PartObject, RepairPolicy, REFRESH,
};
use bytes::{Bytes, BytesMut};
use clap::Parser;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{mpsc, Semaphore};
use tokio_rustls::TlsConnector;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "needletail-udp-client",
    about = "Receive a Needletail stream by RaptorQ FEC UDP from an assigned edge"
)]
struct Args {
    /// Entry point: `https://host:port`.
    #[arg(long)]
    entry: String,
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
    #[arg(long, default_value_t = av_mesh::client_udp::DEFAULT_REPAIR_PERMILLE)]
    repair_permille: u32,
    #[arg(long, default_value_t = av_mesh::client_udp::DEFAULT_MIN_REPAIR_SYMBOLS)]
    min_repair: u32,
    /// Move to the next cache after this long without a datagram.
    #[arg(long, default_value_t = 1_000)]
    silence_ms: u64,
    /// Move to the next cache when a subscription is not answered in this
    /// time (a new playlist copy at an edge takes up to a second).
    #[arg(long, default_value_t = 5_000)]
    answer_ms: u64,
    /// Keep the repair that was asked for, whatever the measured loss.
    #[arg(long)]
    fixed_repair: bool,
    /// Fetch every received part over HTTP from the same cache and compare.
    #[arg(long)]
    verify_http: bool,
    /// Session id for assignment (CMCD `sid`).
    #[arg(long, default_value = "udp-client")]
    session: String,
    /// UDP receive buffer in bytes.
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    receive_buffer_bytes: usize,
    /// Local address for the UDP socket.
    #[arg(long)]
    bind: Option<SocketAddr>,
    /// Write the parts' bytes, concatenated in part order, to this file.
    #[arg(long)]
    write_parts: Option<PathBuf>,
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
            Some((host, port)) => (host.trim_matches(['[', ']']).to_owned(), port.parse().unwrap_or(443)),
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

#[derive(Debug, Clone)]
struct Cache {
    node_id: String,
    base: Url,
}

async fn assignment(http: &Http, args: &Args, current: Option<&str>) -> Result<(Vec<Cache>, Option<u16>)> {
    let entry = Url::parse(&args.entry)?;
    let mut path = format!("/api/assignment?stream={}&sid={}", args.stream, args.session);
    if let Some(current) = current {
        path.push_str(&format!("&current={current}"));
    }
    let (status, body) = http.get(&entry.authority, &path).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)
        .with_context(|| format!("assignment answer is not JSON ({status})"))?;
    let port = json["client_udp_port"].as_u64().map(|port| port as u16);
    let caches = json["assigned"]
        .as_array()
        .ok_or_else(|| anyhow!("no assignment ({status}): {}", json["error"]))?
        .iter()
        .filter_map(|cache| {
            Some(Cache {
                node_id: cache["node_id"].as_str()?.to_owned(),
                base: Url::parse(cache["playback_base_url"].as_str()?).ok()?,
            })
        })
        .collect::<Vec<_>>();
    if caches.is_empty() {
        bail!("the assignment names no cache");
    }
    Ok((caches, port))
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
        if let Some((number, ext)) = uri.strip_prefix("part").and_then(|rest| rest.split_once('.')) {
            if let Ok(number) = number.parse::<u64>() {
                last = Some(last.map_or(number, |last: u64| last.max(number)));
                extension = ext.to_owned();
            }
        }
    }
    (last, extension)
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

#[derive(Debug, Serialize)]
struct Report {
    schema: &'static str,
    stream_id: u64,
    duration_seconds: u64,
    caches: Vec<String>,
    moves: Vec<MoveRecord>,
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
    verify: Option<VerifyReport>,
    publish_to_arrival_ms: Percentiles,
    available_to_arrival_ms: Percentiles,
    time_to_first_part_ms: Option<f64>,
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

async fn subscribe(
    socket: &UdpSocket,
    target: SocketAddr,
    args: &Args,
    from_part: Option<u64>,
    repair: RepairPolicy,
    token: Option<u64>,
) {
    let control = Control::Subscribe {
        stream_id: args.stream,
        from_part,
        repair,
        token,
    };
    // A failed send is a lost request: the next refresh repeats it.
    let _ = socket.send_to(&control.encode(), target).await;
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
                        report.error_examples.push(format!("part {part}: HTTP {status}"));
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

    let mut receiver = ObjectReceiver::new();
    let mut received: BTreeMap<u64, usize> = BTreeMap::new();
    let mut gaps: BTreeSet<u64> = BTreeSet::new();
    let mut duplicates = 0_u64;
    let mut init_objects = 0_u64;
    let mut payload_bytes = 0_u64;
    let mut publish_ms = Vec::new();
    let mut available_ms = Vec::new();
    let mut first_part_ms = None;
    let mut moves = Vec::new();
    let mut caches_used = Vec::new();
    let mut first_subscribed_part: Option<u64> = None;
    let mut parts_out = match &args.write_parts {
        Some(path) => Some(std::fs::File::create(path)?),
        None => None,
    };
    let mut written_parts: BTreeMap<u64, Bytes> = BTreeMap::new();

    let mut current: Option<String> = None;
    let mut skip: BTreeSet<String> = BTreeSet::new();
    let mut move_reason = String::from("start");
    let mut buf = vec![0_u8; 65_536];
    let base_repair = RepairPolicy {
        permille: args.repair_permille,
        min_symbols: args.min_repair,
        loss_ppm: 0,
    };
    // The edge left last, with the token that it gave, for an UNSUB.
    let mut leaving: Option<(SocketAddr, u64)> = None;
    let mut repair = base_repair;
    let mut loss_estimate: Option<f64> = None;
    let mut loss_samples: Vec<f64> = Vec::new();
    let mut repair_changes = 0_u64;

    'caches: while Instant::now() < deadline {
        let (caches, port) = match assignment(&http, &args, current.as_deref()).await {
            Ok(answer) => answer,
            Err(error) => {
                eprintln!("assignment failed: {error:#}");
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let port = args
            .udp_port
            .or(port)
            .ok_or_else(|| anyhow!("no client UDP port: the entry point names none; pass --udp-port"))?;
        // The first cache of the assignment that this client has not just
        // left, or the first one when it has left them all.
        let cache = caches
            .iter()
            .find(|cache| !skip.contains(&cache.node_id))
            .unwrap_or(&caches[0])
            .clone();
        if skip.len() >= caches.len() {
            skip.clear();
        }
        let next_part = received
            .keys()
            .chain(gaps.iter())
            .max()
            .map(|part| part + 1);
        let playlist_path = format!("{}/{}/stream.m3u8", cache.base.path, args.stream);
        let extension = match http.get(&cache.base.authority, &playlist_path).await {
            Ok((StatusCode::OK, body)) => playlist_parts(&String::from_utf8_lossy(&body)).1,
            Ok((status, _)) => {
                eprintln!("{}: playlist answered {status}", cache.node_id);
                skip.insert(cache.node_id.clone());
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(error) => {
                eprintln!("{}: playlist failed: {error:#}", cache.node_id);
                skip.insert(cache.node_id.clone());
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let (host, _) = cache.base.host_port();
        let target: SocketAddr = match tokio::net::lookup_host((host.as_str(), port))
            .await
            .ok()
            .and_then(|mut addresses| addresses.next())
        {
            Some(target) => target,
            None => {
                eprintln!("{}: no address for {host}", cache.node_id);
                skip.insert(cache.node_id.clone());
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        // Leave the last edge, so that it stops sending at once, and decode
        // the new edge's symbols apart from the old edge's.
        if let Some((old, token)) = leaving.take() {
            let _ = socket
                .send_to(&Control::Unsubscribe { stream_id: args.stream, token }.encode(), old)
                .await;
        }
        if current.is_some() {
            receiver.new_flow();
        }
        if let Some(previous) = &current {
            if previous != &cache.node_id {
                moves.push(MoveRecord {
                    at_ms: started.elapsed().as_millis() as u64,
                    from: previous.clone(),
                    to: cache.node_id.clone(),
                    reason: move_reason.clone(),
                    next_part,
                });
            }
        }
        current = Some(cache.node_id.clone());
        caches_used.push(cache.node_id.clone());
        let mut token: Option<u64> = None;
        subscribe(&socket, target, &args, next_part, repair, token).await;
        let asked_at = Instant::now();
        let mut last_datagram = Instant::now();
        // Until the cache answers, the request is repeated often.
        let mut next_refresh = Instant::now() + Duration::from_millis(300);
        let mut next_loss_check = Instant::now() + Duration::from_millis(500);
        let mut subscribed = false;

        loop {
            let now = Instant::now();
            if now >= deadline {
                if let Some(token) = token {
                    let _ = socket
                        .send_to(&Control::Unsubscribe { stream_id: args.stream, token }.encode(), target)
                        .await;
                }
                break 'caches;
            }
            leaving = token.map(|token| (target, token));
            if !subscribed && now.duration_since(asked_at) > Duration::from_millis(args.answer_ms) {
                move_reason = format!("no-answer-{}ms", args.answer_ms);
                skip.insert(cache.node_id.clone());
                continue 'caches;
            }
            if subscribed && now.duration_since(last_datagram) > Duration::from_millis(args.silence_ms) {
                move_reason = format!("silence-{}ms", args.silence_ms);
                skip.insert(cache.node_id.clone());
                continue 'caches;
            }
            if now >= next_loss_check {
                next_loss_check = now + Duration::from_millis(500);
                if let Some(loss) = receiver.take_loss(repair) {
                    loss_samples.push(loss);
                    // Rise at once, fall slowly.
                    let estimate = match loss_estimate {
                        Some(previous) if loss < previous => previous * 0.8 + loss * 0.2,
                        _ => loss,
                    };
                    loss_estimate = Some(estimate);
                    if !args.fixed_repair {
                        let wanted = RepairPolicy::for_loss(estimate, base_repair);
                        if wanted != repair {
                            repair = wanted;
                            repair_changes += 1;
                            next_refresh = now;
                        }
                    }
                }
            }
            if now >= next_refresh {
                // Before the edge answers, keep asking from the same part.
                subscribe(&socket, target, &args, if subscribed { None } else { next_part }, repair, token)
                    .await;
                next_refresh = now
                    + if subscribed {
                        REFRESH
                    } else {
                        Duration::from_millis(300)
                    };
            }
            let silence_at = if subscribed {
                last_datagram + Duration::from_millis(args.silence_ms)
            } else {
                asked_at + Duration::from_millis(args.answer_ms)
            };
            let wait = deadline
                .min(next_refresh)
                .min(next_loss_check)
                .min(silence_at + Duration::from_millis(1))
                .saturating_duration_since(now);
            let (len, peer) = match tokio::time::timeout(wait, socket.recv_from(&mut buf)).await {
                Ok(Ok(received)) => received,
                Ok(Err(_)) => continue,
                Err(_) => continue,
            };
            if peer.ip() != target.ip() {
                continue;
            }
            last_datagram = Instant::now();
            if let Some(control) = Control::decode(&buf[..len]) {
                match control {
                    Control::Token { token: given, .. } => {
                        if token != Some(given) {
                            token = Some(given);
                            subscribe(&socket, target, &args, next_part, repair, token).await;
                        }
                    }
                    Control::Subscribed { next_part: first, .. } => {
                        if !subscribed {
                            first_subscribed_part.get_or_insert(first);
                        }
                        subscribed = true;
                        next_refresh = Instant::now() + REFRESH;
                    }
                    Control::Move { reason, .. } | Control::Refused { reason, .. } => {
                        move_reason = reason;
                        skip.insert(cache.node_id.clone());
                        continue 'caches;
                    }
                    _ => {}
                }
                continue;
            }
            let Some(object) = receiver.push(&buf[..len]) else {
                continue;
            };
            let arrival_us = now_unix_us();
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
                ObjectKind::Gap => {
                    gaps.insert(part);
                }
                ObjectKind::Part => {
                    if received.contains_key(&part) {
                        duplicates += 1;
                        continue;
                    }
                    first_part_ms.get_or_insert(started.elapsed().as_secs_f64() * 1_000.0);
                    received.insert(part, payload.len());
                    payload_bytes += payload.len() as u64;
                    if published_unix_us > 0 {
                        publish_ms.push(arrival_us.saturating_sub(published_unix_us) as f64 / 1_000.0);
                    }
                    if available_unix_us > 0 {
                        available_ms.push(arrival_us.saturating_sub(available_unix_us) as f64 / 1_000.0);
                    }
                    if parts_out.is_some() {
                        written_parts.insert(part, payload.clone());
                    }
                    if args.verify_http {
                        let _ = verify_tx.send(VerifyJob {
                            authority: cache.base.authority.clone(),
                            path: format!("{}/{}/part{part}.{extension}", cache.base.path, args.stream),
                            part,
                            payload,
                        });
                    }
                }
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(600)).await;
    let _ = receiver.take_loss(repair);
    drop(verify_tx);
    let verify = verifier.await.ok();

    if let Some(file) = parts_out.as_mut() {
        use std::io::Write;
        for payload in written_parts.values() {
            file.write_all(payload)?;
        }
    }

    // Missing parts are counted from the part that the first edge said it
    // would send first.
    let first = received
        .keys()
        .chain(gaps.iter())
        .chain(first_subscribed_part.iter())
        .min()
        .copied();
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
    let report = Report {
        schema: "needletail.udp-client.v1",
        stream_id: args.stream,
        duration_seconds: args.duration_seconds,
        caches: caches_used,
        moves,
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
        loss_estimate_max: loss_samples.iter().copied().reduce(f64::max),
        final_repair_permille: repair.permille,
        final_min_repair: repair.min_symbols,
        repair_changes,
        final_loss_ppm: repair.loss_ppm,
        flows: receiver.flows,
        payload_bytes,
        verify: if args.verify_http { verify } else { None },
        publish_to_arrival_ms: percentiles(publish_ms),
        available_to_arrival_ms: percentiles(available_ms),
        time_to_first_part_ms: first_part_ms,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
