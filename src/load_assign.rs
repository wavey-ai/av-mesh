//! The load tool's transport for its listeners' directors (B Y3): the
//! HTTPS hedge of an assignment request (`/api/assignment`, asked of an
//! entry point beside the `ASSIGN` datagram that each listener sends from its
//! own socket, as `needletail-udp-client` does) and the name lookups that a
//! director asks for ([`crate::client_director::Action::Https`] and
//! [`crate::client_director::Action::Resolve`]).
//!
//! The listeners live on synchronous worker threads; the requests run on one
//! thread of their own (a current-thread runtime), one HTTP/2 connection per
//! entry point, many requests in flight on it. A worker sends a
//! [`ResolveRequest`] and reads the [`ResolveAnswer`] from its own channel
//! when it next looks (every loop of the worker, at most a few ms).

use anyhow::{anyhow, bail, Context, Result};
use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{mpsc, Arc};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// How to reach the entry points over HTTPS.
#[derive(Debug, Clone)]
pub struct AssignConfig {
    /// PEM file of the nodes' certificate authority.
    pub tls_ca: PathBuf,
    /// TLS name of the nodes, when it is not their address.
    pub server_name: Option<String>,
    /// Requests in flight at most.
    pub in_flight: usize,
    /// How long one request may take (the director's whole budget of an
    /// assignment, `--assignment-timeout-ms`).
    pub timeout: Duration,
}

/// What a director asked for.
#[derive(Debug, Clone)]
pub enum Job {
    /// `GET <entry><path>` with the CMCD session header.
    Https {
        id: u64,
        entry: String,
        path: String,
        sid: String,
    },
    /// The address of a host name.
    Resolve { host: String },
}

#[derive(Debug, Clone)]
pub struct ResolveRequest {
    pub worker: usize,
    pub slot: usize,
    /// The listener's session number in its worker: an answer for a session
    /// that has gone is dropped.
    pub listener: u64,
    pub job: Job,
}

/// The answer to a [`Job`].
#[derive(Debug)]
pub enum Reply {
    /// The body, or `None` on any failure.
    Https { id: u64, body: Option<Bytes> },
    Resolved { host: String, ip: Option<IpAddr> },
}

#[derive(Debug)]
pub struct ResolveAnswer {
    pub slot: usize,
    pub listener: u64,
    pub reply: Reply,
}

/// The resolver thread: send it requests, read each worker's answers from
/// its receiver.
pub struct Resolver {
    tx: tokio::sync::mpsc::UnboundedSender<ResolveRequest>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Resolver {
    /// Start the resolver for `workers` workers: one answer channel each.
    pub fn start(
        config: AssignConfig,
        workers: usize,
    ) -> Result<(Self, Vec<mpsc::Receiver<ResolveAnswer>>)> {
        let connector = tls_connector(&config.tls_ca)?;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ResolveRequest>();
        let (answer_txs, answer_rxs): (Vec<_>, Vec<_>) =
            (0..workers).map(|_| mpsc::channel::<ResolveAnswer>()).unzip();
        let thread = std::thread::Builder::new()
            .name("load-assign".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("the assignment runtime");
                runtime.block_on(async move {
                    let http = Arc::new(Http {
                        connector,
                        server_name: config.server_name.clone(),
                        connections: tokio::sync::Mutex::new(HashMap::new()),
                    });
                    let config = Arc::new(config);
                    let limit = Arc::new(tokio::sync::Semaphore::new(config.in_flight.max(1)));
                    let answer_txs = Arc::new(answer_txs);
                    while let Some(request) = rx.recv().await {
                        let (http, config, limit, answer_txs) = (
                            Arc::clone(&http),
                            Arc::clone(&config),
                            Arc::clone(&limit),
                            Arc::clone(&answer_txs),
                        );
                        tokio::spawn(async move {
                            let _permit = limit.acquire().await;
                            let reply = match request.job {
                                Job::Https {
                                    id,
                                    entry,
                                    path,
                                    sid,
                                } => {
                                    let body = tokio::time::timeout(
                                        config.timeout,
                                        ask(&http, &entry, &path, &sid),
                                    )
                                    .await
                                    .ok()
                                    .and_then(Result::ok);
                                    Reply::Https { id, body }
                                }
                                Job::Resolve { host } => {
                                    let ip = tokio::time::timeout(
                                        config.timeout,
                                        tokio::net::lookup_host((host.as_str(), 0)),
                                    )
                                    .await
                                    .ok()
                                    .and_then(Result::ok)
                                    .and_then(|mut addrs| addrs.next())
                                    .map(|addr| addr.ip());
                                    Reply::Resolved { host, ip }
                                }
                            };
                            if let Some(tx) = answer_txs.get(request.worker) {
                                let _ = tx.send(ResolveAnswer {
                                    slot: request.slot,
                                    listener: request.listener,
                                    reply,
                                });
                            }
                        });
                    }
                });
            })
            .context("start the assignment thread")?;
        Ok((
            Self {
                tx,
                thread: Some(thread),
            },
            answer_rxs,
        ))
    }

    pub fn request(&self, request: ResolveRequest) {
        let _ = self.tx.send(request);
    }
}

impl Drop for Resolver {
    fn drop(&mut self) {
        // Closing the channel ends the thread once its requests answer.
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        drop(std::mem::replace(&mut self.tx, tx));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The entry point that a session asks first (sessions spread over them).
pub fn entry_for(entries: &[String], sid: &str) -> usize {
    if entries.is_empty() {
        return 0;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sid.hash(&mut hasher);
    (hasher.finish() % entries.len() as u64) as usize
}

/// The HTTPS hedge: `GET <entry><path>` (the director's assignment path),
/// the body as it came.
async fn ask(http: &Http, entry: &str, path: &str, sid: &str) -> Result<Bytes> {
    let authority = authority_of(entry)?;
    http.get(&authority, path, sid).await
}

/// `host:port` of an `https://host:port/...` URL.
pub fn authority_of(url: &str) -> Result<String> {
    let rest = url
        .strip_prefix("https://")
        .ok_or_else(|| anyhow!("only https URLs are supported: {url}"))?;
    Ok(rest.split('/').next().unwrap_or_default().to_owned())
}

fn host_of(authority: &str) -> String {
    match authority.rsplit_once(':') {
        Some((host, port)) if port.parse::<u16>().is_ok() => host.trim_matches(['[', ']']).to_owned(),
        _ => authority.trim_matches(['[', ']']).to_owned(),
    }
}

fn tls_connector(ca: &PathBuf) -> Result<TlsConnector> {
    let pem = std::fs::read(ca).with_context(|| format!("read {}", ca.display()))?;
    let mut roots = rustls::RootCertStore::empty();
    for certificate in rustls_pemfile::certs(&mut pem.as_slice()) {
        roots.add(certificate?)?;
    }
    if roots.is_empty() {
        bail!("{} holds no certificate", ca.display());
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

struct Http {
    connector: TlsConnector,
    server_name: Option<String>,
    connections: tokio::sync::Mutex<HashMap<String, SendRequest<Empty<Bytes>>>>,
}

impl Http {
    async fn connection(&self, authority: &str) -> Result<SendRequest<Empty<Bytes>>> {
        let mut connections = self.connections.lock().await;
        if let Some(connection) = connections.get(authority) {
            if !connection.is_closed() {
                return Ok(connection.clone());
            }
        }
        let host = host_of(authority);
        let tcp = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(authority))
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

    async fn get(&self, authority: &str, path: &str, sid: &str) -> Result<Bytes> {
        let result = async {
            let mut sender = self.connection(authority).await?;
            let request = http::Request::builder()
                .method("GET")
                .uri(format!("https://{authority}{path}"))
                .header("cmcd-session", format!("sid=\"{sid}\""))
                .body(Empty::<Bytes>::new())?;
            let response = sender.send_request(request).await?;
            if !response.status().is_success() {
                bail!("{authority}{path}: {}", response.status());
            }
            let (_, mut body) = response.into_parts();
            let mut bytes = BytesMut::new();
            while let Some(frame) = body.frame().await {
                if let Ok(data) = frame?.into_data() {
                    bytes.extend_from_slice(&data);
                }
            }
            Ok::<_, anyhow::Error>(bytes.freeze())
        }
        .await;
        if result.is_err() {
            self.connections.lock().await.remove(authority);
        }
        result
    }
}

/// Rendezvous over `candidates` for a session: the one whose hash with the
/// session is highest (the order in which `--edges` alone, the in-process
/// tests' mode, names its edges to a session).
pub fn rendezvous<'a, T: Hash>(sid: &str, candidates: impl Iterator<Item = &'a T>) -> Option<&'a T>
where
    T: 'a,
{
    candidates.max_by_key(|candidate| rendezvous_score(sid, *candidate))
}

/// A session's rendezvous score of one candidate: candidates in falling
/// order of it are the session's ranking.
pub fn rendezvous_score<T: Hash + ?Sized>(sid: &str, candidate: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sid.hash(&mut hasher);
    candidate.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_split_into_authority_and_host() {
        assert_eq!(authority_of("https://10.0.0.1:19444/live").unwrap(), "10.0.0.1:19444");
        assert_eq!(authority_of("https://edge.example").unwrap(), "edge.example");
        assert!(authority_of("http://x").is_err());
        assert_eq!(host_of("10.0.0.1:19444"), "10.0.0.1");
        assert_eq!(host_of("[::1]:443"), "::1");
        assert_eq!(host_of("edge.example"), "edge.example");
    }

    #[test]
    fn rendezvous_spreads_sessions_and_keeps_each_one_s_choice() {
        let edges = ["a", "b", "c", "d"];
        let mut counts = HashMap::new();
        for index in 0..4_000 {
            let sid = format!("load-{index}");
            let first = rendezvous(&sid, edges.iter()).unwrap();
            assert_eq!(rendezvous(&sid, edges.iter()), Some(first), "stable");
            *counts.entry(*first).or_insert(0) += 1;
            // Without its first choice, a session goes to another edge, and
            // the others keep theirs.
            let rest: Vec<_> = edges.iter().filter(|edge| *edge != first).collect();
            assert_ne!(rendezvous(&sid, rest.iter().copied()), Some(first));
        }
        for edge in edges {
            let count = counts[edge];
            assert!((800..1_200).contains(&count), "{edge}: {count}");
        }
        let entries: Vec<String> = (0..3).map(|i| format!("https://e{i}")).collect();
        let mut seen = [0; 3];
        for index in 0..300 {
            seen[entry_for(&entries, &format!("s{index}"))] += 1;
        }
        assert!(seen.iter().all(|count| *count > 50), "{seen:?}");
    }
}
