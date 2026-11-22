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
//!
//! With `--fast-lane` (the default) it asks the edge for the fast lane
//! (`SUB ... F1`): an edge fed by the fast plane sends it each part's
//! symbols as they arrive from the forwarders, the contributor's own, and the
//! client builds the part from the canonical envelope with the edge's own
//! slot functions. The report counts the parts of each lane and the envelope
//! bytes of the fast-lane parts.
//!
//! With `--compact` (the default) it asks for the compact lane
//! (`av_mesh::client_compact`): each part that fits one datagram comes as
//! one, with the copies and the repair across parts that the client sizes
//! from the loss and the bursts it measures, and it asks for a part it
//! misses again (`NACK`). The report gives the bytes received on the wire
//! (with IP and UDP headers) and their rate, beside the parts' own bytes.

use anyhow::{anyhow, bail, Context, Result};
use av_mesh::client_compact::NackTiming;
use av_mesh::client_session::{Received, SessionConfig, UdpSession};
use av_mesh::client_udp::{ObjectKind, PartObject};
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
    /// Repair of each RaptorQ block, per thousand source symbols, at least.
    /// By default 0 when the edge serves the compact lane (NACK backs it
    /// up), and 250 on the RaptorQ lane alone, as before wave 5.
    #[arg(long)]
    repair_permille: Option<u32>,
    /// Repair symbols of each RaptorQ block, at least: by default 1 with
    /// the compact lane, 3 on the RaptorQ lane alone.
    #[arg(long)]
    min_repair: Option<u32>,
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
    /// Ask the edge for the fast lane (`F1`): parts as their symbols arrive
    /// at the edge, before it decodes them. `false` takes the cache-fed lane
    /// alone, as before.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    fast_lane: bool,
    /// Ask the edge for the compact lane: a part that fits one datagram as
    /// one datagram, with repair across parts and NACK. `false` takes the
    /// RaptorQ lane alone, with its repair of before wave 5. An edge that
    /// does not answer a request for it (an edge older than the lane) is
    /// asked again without it.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compact: bool,
    /// On a path whose losses come in bursts (runs of two or more compact
    /// datagrams, copies included), plan no copies (a part's copies go out
    /// right behind it and a burst takes them too) and window repair rows
    /// for the burst. `false`: one copy and at most two more rows, as before.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compact_burst_rows: bool,
    /// Ask for parts in the short form (wave 7 item 9): a 7-byte header
    /// instead of 11 (`C...7`). The client reads both forms, so an edge
    /// that does not send it is read as before. `false` asks for the long
    /// form.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compact_short_header: bool,
    /// Wait before the first NACK of a hole that a later part showed, in
    /// µs (wave 6: 250; before, 1,000, when the cache-fed lane could trail
    /// its fast-lane neighbours).
    #[arg(long, default_value_t = av_mesh::client_compact::FAST_NACK_DELAY.as_micros() as u64)]
    nack_delay_us: u64,
    /// The floor of the wait between NACKs of a hole, in µs (one and a half
    /// round trips when longer; wave 6: 2,000; before, 5,000).
    #[arg(long, default_value_t = av_mesh::client_compact::FAST_NACK_RETRY.as_micros() as u64)]
    nack_retry_floor_us: u64,
    /// NACK the parts that are due when no part has come for an interval
    /// and a margin past the next one's due time, and again each interval
    /// while the silence lasts (a burst on the path), up to 50 ms. `false`:
    /// a hole is asked for only once a later part shows it, as before.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    silence_nack: bool,
    /// At the end of the run, keep receiving for up to this long (ms) to
    /// fill the holes among the parts received so far, by NACK, so that a
    /// part lost just before the end is not counted missing; parts past the
    /// last one are not taken. 0: stop at the end, as before.
    #[arg(long, default_value_t = 300)]
    drain_ms: u64,
    /// Write one line for each part received, in part order: part number,
    /// arrival time (Unix µs), bytes, publication and availability times as
    /// the object carries them, and the kernel's receive time of the
    /// datagram (Unix µs, SO_TIMESTAMPNS; 0 where the host gives none):
    /// arrival minus kernel time is the delay on the listener's host. With
    /// --write-parts, a measurement tool can find each part's bytes (the
    /// lengths give the offsets).
    #[arg(long)]
    arrivals: Option<PathBuf>,
    /// Read the assigned cache's media playlist before subscribing, as
    /// before (wave 7 item 8). By default the client subscribes right after
    /// the assignment and reads the playlist beside the subscription: a
    /// cache whose playlist answers with an error before the subscription
    /// is answered is still left. With --verify-http the playlist is read
    /// first either way (it names the parts' extension).
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    playlist_first: bool,
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

#[derive(Debug, Clone)]
struct Cache {
    node_id: String,
    base: Url,
}

async fn assignment(
    http: &Http,
    args: &Args,
    current: Option<&str>,
) -> Result<(Vec<Cache>, Option<u16>)> {
    let entry = Url::parse(&args.entry)?;
    let mut path = format!(
        "/api/assignment?stream={}&sid={}",
        args.stream, args.session
    );
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
    /// The playlist read (beside the subscription unless --playlist-first).
    playlist_ms: Option<f64>,
    first_request_ms: Option<f64>,
    token_ms: Option<f64>,
    subscribed_ms: Option<f64>,
    first_part_ms: Option<f64>,
    /// The first part received, after the part that the OK named.
    first_part_after_named: Option<u64>,
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
    /// Bytes of the media datagrams received, with 28 bytes of IPv4 and UDP
    /// header each, and their rate over the parts' time.
    wire_bytes: u64,
    wire_bits_per_second: Option<f64>,
    /// Wire bytes over the parts' own bytes.
    wire_per_payload: Option<f64>,
    /// The last edge was asked for the compact lane.
    compact_asked: bool,
    /// Edges that did not answer a request for the compact lane and were
    /// asked without it.
    compact_fallbacks: u64,
    /// Edges that refused the compact lane asked with the short form, and
    /// were asked again with the long form.
    compact_short_header_dropped: u64,
    /// The round trip to the edge that the NACKs wait for.
    nack_rtt_ms: Option<f64>,
    /// The NACK timing asked for, and the interval between parts measured.
    nack_timing: String,
    part_interval_ms: Option<f64>,
    compact_confirmed: bool,
    final_compact_plan: Option<String>,
    compact_plan_changes: u64,
    burst_estimate_max: Option<f64>,
    compact: CompactReport,
    fast_lane_asked: bool,
    /// Parts that came on the fast lane, and on the cache-fed lane.
    fast_lane_parts: u64,
    /// The fast-lane parts that came as compact datagrams (marked by the
    /// edge; an edge before the mark sends them unmarked, counted cache-fed).
    compact_fast_lane_parts: u64,
    cache_lane_parts: u64,
    /// Bytes of the canonical envelope of each fast-lane part, and the bytes
    /// of it that are not the part's media (the envelope's own header,
    /// metadata and timestamps).
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

/// Receive one datagram, with its sender and the kernel's receive time
/// (Unix µs) when the socket gives one.
async fn recv_timestamped(
    socket: &UdpSocket,
    buf: &mut [u8],
) -> std::io::Result<(usize, SocketAddr, Option<u64>)> {
    use std::os::fd::AsRawFd;
    socket
        .async_io(tokio::io::Interest::READABLE, || {
            let mut address: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            let mut control = [0_u64; 8];
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
            Ok((len as usize, peer, kernel_us))
        })
        .await
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

    // The protocol of one subscription: av_mesh::client_session, as the
    // load generator runs it.
    let mut session = UdpSession::new(
        SessionConfig {
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
            known_parts: 0,
        },
        Instant::now(),
    );
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
    let mut moves = Vec::new();
    let mut caches_used = Vec::new();
    let mut parts_out = match &args.write_parts {
        Some(path) => Some(std::fs::File::create(path)?),
        None => None,
    };
    let mut written_parts: BTreeMap<u64, Bytes> = BTreeMap::new();
    let mut arrivals: BTreeMap<u64, (u64, usize, u64, u64, u64)> = BTreeMap::new();

    let mut current: Option<String> = None;
    let mut skip: BTreeSet<String> = BTreeSet::new();
    let mut move_reason = String::from("start");
    let mut buf = vec![0_u8; 65_536];
    let mut first_published_us: Option<u64> = None;
    let mut last_published_us: Option<u64> = None;
    // The end of the run: the last part taken, and the holes before it
    // that the drain waits for.
    let drain = Duration::from_millis(args.drain_ms);
    let mut drain_last: Option<u64> = None;
    let mut drain_missing: BTreeSet<u64> = BTreeSet::new();
    // The control datagrams that the session hands out, sent in order.
    let mut outbox: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
    async fn flush(socket: &UdpSocket, outbox: &mut Vec<(SocketAddr, Vec<u8>)>) {
        for (to, datagram) in outbox.drain(..) {
            // A failed send is a lost request: the next refresh repeats it.
            let _ = socket.send_to(&datagram, to).await;
        }
    }

    'caches: while Instant::now() < deadline {
        let (caches, port) = match assignment(&http, &args, current.as_deref()).await {
            Ok(answer) => answer,
            Err(error) => {
                eprintln!("assignment failed: {error:#}");
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        startup.assignment_ms.get_or_insert(since_start(started));
        let port = args.udp_port.or(port).ok_or_else(|| {
            anyhow!("no client UDP port: the entry point names none; pass --udp-port")
        })?;
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
        // Unless it is read first, the playlist is read beside the
        // subscription; its answer can still send the client on.
        let mut playlist_check = None;
        if !args.playlist_first && !args.verify_http {
            let (http, authority, path) = (
                http.clone(),
                cache.base.authority.clone(),
                playlist_path.clone(),
            );
            playlist_check = Some(tokio::spawn(
                async move { http.get(&authority, &path).await },
            ));
        }
        let read_first = playlist_check.is_none();
        let extension = match async {
            if read_first {
                http.get(&cache.base.authority, &playlist_path).await
            } else {
                Ok((StatusCode::OK, Bytes::new()))
            }
        }
        .await
        {
            Ok((StatusCode::OK, body)) => {
                if read_first {
                    startup.playlist_ms.get_or_insert(since_start(started));
                }
                playlist_parts(&String::from_utf8_lossy(&body)).1
            }
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
        // Leave the last edge (UNSUB), and subscribe at this one.
        session.begin(target, next_part, Instant::now(), &mut |to, datagram| {
            outbox.push((to, datagram.to_vec()))
        });
        flush(&socket, &mut outbox).await;
        startup.first_request_ms.get_or_insert(since_start(started));

        loop {
            let now = Instant::now();
            if now >= deadline
                && drain_last.is_none()
                && session.is_subscribed()
                && !drain.is_zero()
            {
                let last = received.keys().chain(gaps.iter()).max().copied();
                let first = received
                    .keys()
                    .chain(gaps.iter())
                    .chain(session.first_subscribed_part.iter())
                    .min()
                    .copied();
                if let (Some(first), Some(last)) = (first, last) {
                    drain_last = Some(last);
                    drain_missing = (first..=last)
                        .filter(|part| !received.contains_key(part) && !gaps.contains(part))
                        .collect();
                }
            }
            let draining =
                drain_last.is_some() && !drain_missing.is_empty() && now < deadline + drain;
            // The drain waits for NACK answers: no move for a silence then.
            session.hold = draining;
            if now >= deadline && !draining {
                session.leave(&mut |to, datagram| outbox.push((to, datagram.to_vec())));
                flush(&socket, &mut outbox).await;
                break 'caches;
            }
            // The playlist read beside the subscription: an error before
            // the subscription is answered sends the client on, as a read
            // before it did.
            if playlist_check
                .as_ref()
                .is_some_and(|check| check.is_finished())
            {
                let answer = playlist_check.take().expect("finished").await;
                startup.playlist_ms.get_or_insert(since_start(started));
                let failure = match answer {
                    Ok(Ok((StatusCode::OK, _))) => None,
                    Ok(Ok((status, _))) => Some(format!("playlist answered {status}")),
                    Ok(Err(error)) => Some(format!("playlist failed: {error:#}")),
                    Err(error) => Some(format!("playlist read failed: {error}")),
                };
                if let (Some(failure), false) = (failure, session.is_subscribed()) {
                    eprintln!("{}: {failure}", cache.node_id);
                    skip.insert(cache.node_id.clone());
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue 'caches;
                }
            }
            let fallbacks = session.compact_fallbacks;
            let failed = session.poll(now, &mut |to, datagram| {
                outbox.push((to, datagram.to_vec()))
            });
            flush(&socket, &mut outbox).await;
            if session.compact_fallbacks > fallbacks {
                eprintln!(
                    "{}: no answer to the compact lane: asking without it",
                    cache.node_id
                );
            }
            if let Some(reason) = failed {
                move_reason = reason;
                skip.insert(cache.node_id.clone());
                continue 'caches;
            }
            let end = if draining { deadline + drain } else { deadline };
            let wake = end.min(session.next_wake());
            let wait = wake.saturating_duration_since(now);
            let (len, peer, kernel_us) =
                match tokio::time::timeout(wait, recv_timestamped(&socket, &mut buf)).await {
                    Ok(Ok(received)) => received,
                    Ok(Err(_)) => continue,
                    Err(_) => continue,
                };
            let dropped = session.short_header_dropped;
            let outcome =
                session.on_datagram(peer, &buf[..len], Instant::now(), &mut |to, datagram| {
                    outbox.push((to, datagram.to_vec()))
                });
            flush(&socket, &mut outbox).await;
            if session.short_header_dropped > dropped {
                eprintln!(
                    "{}: the compact lane was refused with the short form: asking with the long form",
                    cache.node_id
                );
            }
            match outcome {
                Received::Media => {}
                Received::Move(reason) => {
                    move_reason = reason;
                    skip.insert(cache.node_id.clone());
                    continue 'caches;
                }
                _ => continue,
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
                            first_published_us =
                                Some(first_published_us.map_or(published_unix_us, |first| {
                                    first.min(published_unix_us)
                                }));
                            last_published_us = Some(
                                last_published_us
                                    .map_or(published_unix_us, |last| last.max(published_unix_us)),
                            );
                            publish_ms.push(
                                arrival_us.saturating_sub(published_unix_us) as f64 / 1_000.0,
                            );
                        }
                        if available_unix_us > 0 {
                            available_ms.push(
                                arrival_us.saturating_sub(available_unix_us) as f64 / 1_000.0,
                            );
                        }
                        if args.arrivals.is_some() {
                            arrivals.insert(
                                part,
                                (
                                    arrival_us,
                                    payload.len(),
                                    published_unix_us,
                                    available_unix_us,
                                    kernel_us.unwrap_or(0),
                                ),
                            );
                        }
                        if parts_out.is_some() {
                            written_parts.insert(part, payload.clone());
                        }
                        if args.verify_http {
                            let _ = verify_tx.send(VerifyJob {
                                authority: cache.base.authority.clone(),
                                path: format!(
                                    "{}/{}/part{part}.{extension}",
                                    cache.base.path, args.stream
                                ),
                                part,
                                payload,
                            });
                        }
                    }
                }
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(600)).await;
    let _ = session.receiver.take_loss(session.repair);
    let since = |at: Option<Instant>| {
        at.map(|at| at.saturating_duration_since(started).as_secs_f64() * 1_000.0)
    };
    startup.token_ms = since(session.first_token_at);
    startup.subscribed_ms = since(session.first_subscribed_at);
    let receiver = &session.receiver;
    let repair = session.repair;
    let plan = session.plan;
    let compact_asked = session.compact_asked;
    drop(verify_tx);
    let verify = verifier.await.ok();

    if let Some(path) = &args.arrivals {
        let mut text = String::from(
            "part,arrival_unix_us,bytes,published_unix_us,available_unix_us,kernel_unix_us\n",
        );
        for (part, (arrival, bytes, published, available, kernel)) in &arrivals {
            text.push_str(&format!(
                "{part},{arrival},{bytes},{published},{available},{kernel}\n"
            ));
        }
        std::fs::write(path, text).with_context(|| format!("write {}", path.display()))?;
    }
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
        .chain(session.first_subscribed_part.iter())
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
    let span_s = match (first_published_us, last_published_us) {
        (Some(first), Some(last)) if last > first => Some((last - first) as f64 / 1e6),
        _ => None,
    };
    let stats = receiver.compact.stats;
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
        nack_timing: format!(
            "delay {} us, retry floor {} us, silence {}",
            args.nack_delay_us, args.nack_retry_floor_us, args.silence_nack
        ),
        part_interval_ms: receiver
            .compact
            .part_interval()
            .map(|interval| interval.as_secs_f64() * 1_000.0),
        nack_rtt_ms: receiver
            .compact
            .rtt()
            .map(|rtt| rtt.as_secs_f64() * 1_000.0),
        compact_confirmed: session.compact_confirmed,
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
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wave 7 item 8: the playlist is read beside the subscription by
    /// default; --playlist-first true reads it first, as before.
    #[test]
    fn the_playlist_is_read_beside_the_subscription_unless_asked_first() {
        let base = [
            "needletail-udp-client",
            "--entry",
            "https://edge:19444",
            "--stream",
            "3001",
            "--tls-ca",
            "/ca.pem",
        ];
        let args = Args::try_parse_from(base).unwrap();
        assert!(!args.playlist_first);
        let args =
            Args::try_parse_from(base.into_iter().chain(["--playlist-first", "true"])).unwrap();
        assert!(args.playlist_first);
    }

    #[test]
    fn the_report_names_each_step_of_the_start() {
        let startup = Startup {
            assignment_ms: Some(3.0),
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
