use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::process::Command;
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, warn};

const PROBE_INTERVAL: Duration = Duration::from_secs(2);
const PROBE_WINDOW: usize = 30;
const FRESH_FOR_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LinkRole {
    Primary,
    Secondary,
}

impl LinkRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Secondary => "secondary",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct LinkObservation {
    pub from_node_id: String,
    pub to_node_id: String,
    pub role: String,
    pub state: String,
    pub method: String,
    pub rtt_ms: Option<f64>,
    pub jitter_ms: Option<f64>,
    pub loss_percent: f64,
    pub throughput_bps: u64,
    pub received_bytes_total: u64,
    pub sample_count: u64,
    pub observed_unix_ms: u64,
    pub fresh_for_ms: u64,
}

#[derive(Debug)]
struct ProbeWindow {
    samples: VecDeque<Option<f64>>,
    last_total_bytes: u64,
    last_rate_sample_unix_ms: u64,
    throughput_bps: u64,
    observed_unix_ms: u64,
}

impl Default for ProbeWindow {
    fn default() -> Self {
        Self {
            samples: VecDeque::with_capacity(PROBE_WINDOW),
            last_total_bytes: 0,
            last_rate_sample_unix_ms: 0,
            throughput_bps: 0,
            observed_unix_ms: 0,
        }
    }
}

#[derive(Debug)]
struct LinkState {
    from_node_id: String,
    to_node_id: String,
    role: LinkRole,
    peer_ip: IpAddr,
    received_bytes: AtomicU64,
    probe: Mutex<ProbeWindow>,
}

impl LinkState {
    fn new(
        from_node_id: String,
        to_node_id: String,
        role: LinkRole,
        peer: SocketAddr,
    ) -> Self {
        Self {
            from_node_id,
            to_node_id,
            role,
            peer_ip: peer.ip(),
            received_bytes: AtomicU64::new(0),
            probe: Mutex::new(ProbeWindow::default()),
        }
    }

    fn record_bytes(&self, peer: SocketAddr, bytes: usize) {
        if peer.ip() == self.peer_ip {
            self.received_bytes
                .fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    fn record_probe(&self, rtt_ms: Option<f64>, observed_unix_ms: u64) {
        let total_bytes = self.received_bytes.load(Ordering::Relaxed);
        let mut probe = self.probe.lock().unwrap_or_else(|error| error.into_inner());
        if probe.samples.len() == PROBE_WINDOW {
            probe.samples.pop_front();
        }
        probe.samples.push_back(rtt_ms);
        if probe.last_rate_sample_unix_ms > 0 {
            let elapsed_ms = observed_unix_ms.saturating_sub(probe.last_rate_sample_unix_ms);
            if elapsed_ms > 0 {
                let received = total_bytes.saturating_sub(probe.last_total_bytes);
                probe.throughput_bps = received
                    .saturating_mul(8)
                    .saturating_mul(1_000)
                    / elapsed_ms;
            }
        }
        probe.last_total_bytes = total_bytes;
        probe.last_rate_sample_unix_ms = observed_unix_ms;
        probe.observed_unix_ms = observed_unix_ms;
    }

    fn snapshot(&self) -> Option<LinkObservation> {
        let probe = self.probe.lock().unwrap_or_else(|error| error.into_inner());
        if probe.observed_unix_ms == 0 || probe.samples.is_empty() {
            return None;
        }
        let successful = probe.samples.iter().flatten().copied().collect::<Vec<_>>();
        let rtt_ms = if successful.is_empty() {
            None
        } else {
            Some(successful.iter().sum::<f64>() / successful.len() as f64)
        };
        let jitter_ms = if successful.len() < 2 {
            None
        } else {
            let total = successful
                .windows(2)
                .map(|pair| (pair[1] - pair[0]).abs())
                .sum::<f64>();
            Some(total / (successful.len() - 1) as f64)
        };
        let loss_percent = 100.0
            * (probe.samples.len().saturating_sub(successful.len())) as f64
            / probe.samples.len() as f64;
        Some(LinkObservation {
            from_node_id: self.from_node_id.clone(),
            to_node_id: self.to_node_id.clone(),
            role: self.role.as_str().to_owned(),
            state: if successful.is_empty() {
                "unreachable".to_owned()
            } else {
                "measured".to_owned()
            },
            method: "icmp_and_udp_ingress".to_owned(),
            rtt_ms,
            jitter_ms,
            loss_percent,
            throughput_bps: probe.throughput_bps,
            received_bytes_total: self.received_bytes.load(Ordering::Relaxed),
            sample_count: probe.samples.len() as u64,
            observed_unix_ms: probe.observed_unix_ms,
            fresh_for_ms: FRESH_FOR_MS,
        })
    }
}

#[derive(Debug)]
pub(crate) struct LinkTelemetry {
    primary: Option<Arc<LinkState>>,
    secondary: Option<Arc<LinkState>>,
}

impl LinkTelemetry {
    pub(crate) fn new(
        to_node_id: String,
        primary: Option<(String, SocketAddr)>,
        secondary: Option<(String, SocketAddr)>,
    ) -> Self {
        Self {
            primary: primary.map(|(from, peer)| {
                Arc::new(LinkState::new(
                    from,
                    to_node_id.clone(),
                    LinkRole::Primary,
                    peer,
                ))
            }),
            secondary: secondary.map(|(from, peer)| {
                Arc::new(LinkState::new(
                    from,
                    to_node_id,
                    LinkRole::Secondary,
                    peer,
                ))
            }),
        }
    }

    pub(crate) fn record_primary_bytes(&self, peer: SocketAddr, bytes: usize) {
        if let Some(link) = &self.primary {
            link.record_bytes(peer, bytes);
        }
    }

    pub(crate) fn record_secondary_bytes(&self, peer: SocketAddr, bytes: usize) {
        if let Some(link) = &self.secondary {
            link.record_bytes(peer, bytes);
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<LinkObservation> {
        [&self.primary, &self.secondary]
            .into_iter()
            .filter_map(|link| link.as_ref().and_then(|link| link.snapshot()))
            .collect()
    }

    pub(crate) fn start(self: &Arc<Self>, shutdown_rx: watch::Receiver<()>) {
        for link in [&self.primary, &self.secondary].into_iter().flatten() {
            tokio::spawn(run_probe(Arc::clone(link), shutdown_rx.clone()));
        }
    }
}

async fn run_probe(link: Arc<LinkState>, mut shutdown_rx: watch::Receiver<()>) {
    let mut tick = interval(PROBE_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = tick.tick() => {
                let result = Command::new("ping")
                    .args(["-n", "-c", "1", "-W", "1", &link.peer_ip.to_string()])
                    .output()
                    .await;
                let rtt_ms = match result {
                    Ok(output) => {
                        let stdout = String::from_utf8_lossy(&output.stdout);
                        let parsed = parse_ping_rtt_ms(&stdout);
                        if parsed.is_none() {
                            warn!(
                                peer = %link.peer_ip,
                                status = ?output.status,
                                stdout = %stdout.trim(),
                                stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                                "link probe did not return an RTT sample"
                            );
                        }
                        parsed
                    }
                    Err(error) => {
                        warn!(peer = %link.peer_ip, error = %error, "failed to execute link probe");
                        None
                    }
                };
                link.record_probe(rtt_ms, now_unix_ms());
            }
        }
    }
}

fn parse_ping_rtt_ms(output: &str) -> Option<f64> {
    output.split_whitespace().find_map(|token| {
        let value = token
            .strip_prefix("time=")
            .or_else(|| token.strip_prefix("time<"))?;
        value.parse::<f64>().ok()
    }).or_else(|| {
        output.lines().find_map(|line| {
            let values = line
                .split_once('=')?
                .1
                .trim()
                .strip_suffix(" ms")?;
            values.split('/').nth(1)?.parse::<f64>().ok()
        })
    })
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_linux_ping_rtt() {
        let output = "64 bytes from 20.0.0.1: icmp_seq=1 ttl=114 time=161.221 ms";
        assert_eq!(parse_ping_rtt_ms(output), Some(161.221));
        let summary = "rtt min/avg/max/mdev = 160.100/161.221/162.200/0.500 ms";
        assert_eq!(parse_ping_rtt_ms(summary), Some(161.221));
    }

    #[test]
    fn snapshot_reports_window_quality_and_wire_rate() {
        let peer: SocketAddr = "192.0.2.10:12001".parse().unwrap();
        let link = LinkState::new(
            "relay-parent".to_owned(),
            "edge-child".to_owned(),
            LinkRole::Primary,
            peer,
        );
        link.record_bytes(peer, 1_000);
        link.record_probe(Some(10.0), 1_000);
        link.record_bytes(peer, 2_000);
        link.record_probe(Some(14.0), 3_000);
        link.record_probe(None, 5_000);

        let snapshot = link.snapshot().unwrap();
        assert_eq!(snapshot.rtt_ms, Some(12.0));
        assert_eq!(snapshot.jitter_ms, Some(4.0));
        assert!((snapshot.loss_percent - (100.0 / 3.0)).abs() < 0.001);
        assert_eq!(snapshot.received_bytes_total, 3_000);
        assert_eq!(snapshot.throughput_bps, 0);
    }
}
