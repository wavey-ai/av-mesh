//! `GET /api/node`: what a node is, which build it runs, and whether it is
//! ready, with the figures a tool needs to judge it.
//!
//! The object is built from atomics and gauges only (no telemetry merge, no
//! replica plan; `AppRouter::node_status` gathers it). Readiness is decided
//! here by pure functions of [`ReadinessInputs`], so that each condition is
//! tested without a node.
//!
//! Two kinds of condition. A **level** condition is evaluated on every
//! request and can take `ready` away. A **warm-up** condition is latched:
//! once every warm-up condition has been met the node stays `warm` until
//! the process ends.
//!
//! `state` is the first that applies of: `stopping`; `starting` (the drain
//! reason `start` held, or the warm-up not yet met; an edge whose warm-up
//! was cut short by its start timeout, `ready_forced`, is past it);
//! `draining` (an operator's drain); `degraded` (a level condition fails);
//! `ready`. An edge past its start timeout serves cold: `ready` with `warm`
//! false, its unmet warm-up conditions still listed in `not_ready`.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

/// The schema of the object.
pub const SCHEMA: &str = "needletail.node.v1";

/// A cache-mesh parent counts as heard for this long (the mesh's peer
/// timeout).
pub const PARENT_HEARD_WITHIN_MS: u64 = 2_000;
/// An edge without a regional advertisement is past E4 this long after its
/// start.
pub const REGIONAL_VIEW_WAIT_MS: u64 = 2_000;
/// F3: a stream delivered in the last ten seconds needs a parent whose
/// sources of it are at most this old.
pub const DELIVERS_WITHIN_MS: u64 = 500;
/// F3: a forwarder that received no relay datagram for this long, after
/// this much uptime, is idle.
pub const IDLE_AFTER_MS: u64 = 1_000;
/// F4, F6: the streams seen this recently must be decoded and windowed.
pub const SEEN_WITHIN_MS: u64 = 1_000;
/// F5: no lease opened for this long.
pub const LEASES_QUIET_MS: u64 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeState {
    Starting,
    Ready,
    Degraded,
    Draining,
    Stopping,
}

/// Why a node drains. A set: the reasons are independent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DrainReason {
    /// The process started drained and is not yet warm.
    Start,
    /// `drain-node` or `close-node`, until `resume-node`.
    Operator,
    /// SIGTERM or SIGINT.
    Stopping,
}

impl DrainReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Operator => "operator",
            Self::Stopping => "stopping",
        }
    }
}

/// The build of the binary (`crate::build`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildStatus {
    pub git: String,
    pub git_full: String,
    pub dirty: bool,
    pub built_unix: u64,
    pub version: String,
}

impl BuildStatus {
    pub fn of_this_binary() -> Self {
        Self {
            git: av_mesh::build::GIT.to_owned(),
            git_full: av_mesh::build::GIT_FULL.to_owned(),
            dirty: av_mesh::build::DIRTY,
            built_unix: av_mesh::build::UNIX,
            version: av_mesh::build::PACKAGE.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessStatus {
    pub pid: u32,
    /// The node's start in Unix µs: changes on every restart.
    pub boot_id: u64,
    pub started_unix_ms: u64,
    pub uptime_ms: u64,
    /// sha256 of every argument (argv[0] included) joined by `\n`.
    pub args_sha256: String,
    pub cpus: u32,
    /// The process's CPU time over its available CPUs in the last second,
    /// in thousandths.
    pub cpu_permille: u32,
}

/// The sha256 of this process's arguments, argv[0] included, joined by
/// `\n`: the sha256 of the node's `.args` file without its last newline.
pub fn args_sha256() -> &'static str {
    static DIGEST: OnceLock<String> = OnceLock::new();
    DIGEST.get_or_init(|| {
        use std::os::unix::ffi::OsStrExt;
        let mut hasher = Sha256::new();
        for (index, argument) in std::env::args_os().enumerate() {
            if index > 0 {
                hasher.update(b"\n");
            }
            hasher.update(argument.as_bytes());
        }
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FastPlaneStatus {
    pub threads: u64,
    /// The class threads this node starts: one per class with a socket.
    pub threads_expected: u64,
    pub realtime_threads: u64,
    pub pinned_threads: u64,
    /// Relay datagrams received, admitted and not admitted in the parent
    /// feeds sampler's last second.
    pub received_1s: u64,
    pub admitted_1s: u64,
    pub not_admitted_1s: u64,
    /// Since the count of received datagrams last rose; null when none was
    /// ever received.
    pub received_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestStatus {
    /// Datagrams the tee of each class dropped as past their deadline.
    pub audio_deadline_drops: u64,
    pub video_deadline_drops: u64,
    pub tee_dropped: u64,
}

/// One parent of this node: an address that admitted datagrams came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentStatus {
    pub address: String,
    /// `--relay-primary-id`, `--relay-secondary-id`, else
    /// `lease-parent-<ip>`.
    pub id: String,
    /// `primary`, `secondary` or `lease`.
    pub path: String,
    /// The youngest source of any audio and any video stream of it.
    pub audio_age_ms: Option<u64>,
    pub video_age_ms: Option<u64>,
    pub streams: Vec<ParentStreamStatus>,
}

/// What one parent delivered of one stream and track (`ParentFeeds`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentStreamStatus {
    pub stream_id: Option<u64>,
    pub track: String,
    pub class: String,
    pub source_age_ms: Option<u64>,
    pub repair_age_ms: Option<u64>,
    pub head_object: Option<u64>,
    pub window_s: u64,
    pub objects_1s: u64,
    pub objects_10s: u64,
    pub skipped_10s: u64,
    pub resets_10s: u64,
    pub multi_symbol_10s: u64,
    pub slack_mean_us_1s: Option<i64>,
    pub slack_mean_us_10s: Option<i64>,
    pub slack_min_us_10s: Option<i64>,
}

/// One stream this node sees or holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamStatus {
    pub stream_id: u64,
    pub class: String,
    /// Since a datagram of it arrived from any parent, or a part of it was
    /// ingested, whichever is later.
    pub seen_age_ms: Option<u64>,
    /// The cache committed a part of it since start and within a second.
    pub decoded: bool,
    pub ingest_age_ms: Option<u64>,
    pub head_part: Option<u64>,
    pub first_part: Option<u64>,
    pub window_parts: u64,
    /// The window it must hold to be warm: `--cache-mesh-warm-parts`, at
    /// most the stream's window; 0 when that flag is 0.
    pub warm_parts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledChild {
    pub target: String,
    /// `all`, `source` or `repair`.
    pub lane: String,
    pub promoted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeasePair {
    pub child: String,
    pub stream_id: u64,
    /// This node is the child's primary for the stream (null: the fast
    /// plane does not feed the pair).
    pub primary: Option<bool>,
    pub rank: Option<u8>,
    pub warm_parts_pending: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseStream {
    pub stream_id: u64,
    pub children: u64,
    /// Since the class thread last fed the stream to its lease children.
    pub fed_age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseStatus {
    pub children: u64,
    pub pairs: u64,
    pub refused: u64,
    pub pairs_list: Vec<LeasePair>,
    pub streams: Vec<LeaseStream>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildrenStatus {
    pub compiled: Vec<CompiledChild>,
    /// Null on a node that feeds no lease children on its fast plane.
    pub lease: Option<LeaseStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheMeshStatus {
    /// Since a cache-mesh parent was last heard; null before the first.
    pub parent_heard_age_ms: Option<u64>,
    /// The peers this node may take playlists from that it heard within
    /// the peer timeout, draining or not.
    pub parents_live: u64,
    /// Parents that left a playlist's ranked set and are not released yet
    /// (make-before-break).
    pub pending_release: u64,
    /// Every peer as its newest hello described it.
    pub peers: Vec<CacheMeshPeerStatus>,
    pub children: u64,
    pub pairs: u64,
    /// The streams this node subscribes to on the cache mesh.
    pub subscribed_playlists: Vec<u64>,
    pub warm_parts_pending: u64,
    /// Frames of a kind, hellos of a role, and frames that this build
    /// cannot read (a later build's are read as far as they are known).
    pub rx_unknown_kinds: u64,
    pub rx_unknown_roles: u64,
    pub rx_invalid_frames: u64,
}

/// One cache-mesh peer, from its newest hello.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheMeshPeerStatus {
    pub peer: String,
    /// `peer`, `distributor` or `edge`; null before its first hello and for
    /// a role this build does not know.
    pub role: Option<String>,
    pub boot_id: Option<u64>,
    pub build: Option<String>,
    /// `starting`, `ready`, `degraded`, `draining` or `stopping`.
    pub state: Option<String>,
    pub heard_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientUdpStatus {
    /// Null before the edge serves clients.
    pub subscribers: Option<u64>,
    /// LL-HLS sessions not moved away.
    pub hls_sessions: u64,
    /// Sessions already redirected that made a request here in the last
    /// second.
    pub hls_moved_active: u64,
    /// LL-HLS reads in progress.
    pub hls_requests_in_flight: u64,
    pub refusals: u64,
    pub load_permille: u32,
    pub rebalance_moves: u64,
    pub parents_lost: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryStatus {
    /// The region's other edges that advertise freshly, not draining and
    /// draining.
    pub caches_fresh: u64,
    pub caches_draining: u64,
}

/// `GET /api/node`, and the `status` of `/api/mesh/local`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub schema: String,
    pub node_id: String,
    pub region: String,
    /// The physical host the node runs on (`--host-tag`), when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    pub role: String,
    pub build: BuildStatus,
    pub process: ProcessStatus,
    pub state: NodeState,
    pub ready: bool,
    pub warm: bool,
    /// When the node was first ready.
    pub ready_unix_ms: Option<u64>,
    pub ready_forced: bool,
    pub not_ready: Vec<String>,
    pub drain_reasons: Vec<DrainReason>,
    /// What peers rank this node by: its hello's draining bit.
    pub hello_draining: bool,
    pub fast_plane: FastPlaneStatus,
    pub ingest: IngestStatus,
    pub parents: Vec<ParentStatus>,
    pub streams: Vec<StreamStatus>,
    pub children: ChildrenStatus,
    pub cache_mesh: CacheMeshStatus,
    /// Edges that serve UDP clients.
    pub client_udp: Option<ClientUdpStatus>,
    pub entry: EntryStatus,
    /// D 2.4: the boot check's record (`--node-warm-file`); null when no
    /// check is configured.
    pub node_warm: Option<crate::node_warm::NodeWarmStatus>,
}

/// The reasons a node drains, as `ReadinessInputs` takes them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DrainReasons {
    pub start: bool,
    pub operator: bool,
    pub stopping: bool,
}

impl DrainReasons {
    pub fn list(self) -> Vec<DrainReason> {
        [
            (self.start, DrainReason::Start),
            (self.operator, DrainReason::Operator),
            (self.stopping, DrainReason::Stopping),
        ]
        .into_iter()
        .filter_map(|(held, reason)| held.then_some(reason))
        .collect()
    }
}

/// E5: a copy an edge opened to be warm, for an audio playlist whose head
/// moves at a fresh edge of its region.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WarmCopy {
    pub stream_id: u64,
    /// The edge opened demand for it.
    pub opened: bool,
    pub decoded: bool,
    pub head_part: Option<u64>,
    /// The highest head advertised in the region when the copy was opened.
    pub target_head: u64,
    pub window_parts: u64,
    /// `min(warm_parts, what the parent retains)`.
    pub needed_parts: u64,
}

/// Everything readiness is decided from. A field another unit produces has
/// a default that leaves its condition met, until that unit fills it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadinessInputs {
    pub uptime_ms: u64,
    /// E1, F1: the HTTP server answers.
    pub http_serving: bool,
    pub threads: u64,
    pub threads_expected: u64,
    /// E1: the client UDP task runs (true on an edge that serves none).
    pub client_udp_running: bool,
    /// E2: since a cache-mesh parent was heard.
    pub parent_heard_age_ms: Option<u64>,
    /// E3: `parents_are_lost`.
    pub parents_lost: bool,
    /// E4: a telemetry snapshot of the region was received.
    pub regional_view: bool,
    /// E5, F7: the region's moving heads are known (every fresh node of the
    /// region and every playlist of every fresh holder seen in two
    /// advertisements; latched), or this node opens nothing at its start
    /// (`WarmPass::judged`). An empty view is not an idle region: once the
    /// region has been heard, E4's two seconds do not stand in for it.
    pub region_heads_known: bool,
    /// E5: the warm copies (the start's warm pass, `warm_playlists`; empty
    /// in an idle region).
    pub warm_copies: Vec<WarmCopy>,
    /// F2, F3: the fast plane's figures.
    pub fast_plane: FastPlaneStatus,
    /// F3: what each parent delivers.
    pub parents: Vec<ParentStatus>,
    /// F4, F6: the streams.
    pub streams: Vec<StreamStatus>,
    /// F5: one renewal period of a lease (lease/3 + 500 ms) and the time
    /// since a lease was last opened here (null: none yet).
    pub lease_renewal_ms: u64,
    pub lease_opened_age_ms: Option<u64>,
    /// F7: the playlists a slot opened upstream demand for at its start
    /// (`run_upstream_demand`).
    pub slot_streams: Vec<u64>,
    /// The drain reasons (`NodeLifecycle`).
    pub drain: DrainReasons,
    /// An edge dropped `start` after `--start-warm-ms` without its warm-up.
    pub ready_forced: bool,
    /// D 2.4: whether the boot check came out complete (`None`: no check
    /// configured). Configured and not complete is not ready, for as long
    /// as the process runs: the record is read once, at start.
    pub node_warm_complete: Option<bool>,
}

/// What the readiness evaluator decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Readiness {
    pub state: NodeState,
    pub warm: bool,
    pub not_ready: Vec<String>,
}

/// The conditions evaluated now: the level ones that fail, and the warm-up
/// ones that are not met.
#[derive(Default)]
struct Conditions {
    level: Vec<String>,
    warm_up: Vec<String>,
}

impl Readiness {
    pub fn ready(&self) -> bool {
        self.state == NodeState::Ready
    }

    /// An edge's readiness. `was_warm`: the warm-up was met before.
    pub fn edge(inputs: &ReadinessInputs, was_warm: bool) -> Self {
        let mut conditions = Conditions::default();
        threads_condition(inputs, &mut conditions);
        node_warm_condition(inputs, &mut conditions);
        if !inputs.client_udp_running {
            conditions.level.push("client_udp not running".to_owned());
        }
        if !inputs
            .parent_heard_age_ms
            .is_some_and(|age| age <= PARENT_HEARD_WITHIN_MS)
        {
            conditions.level.push("no parent heard".to_owned());
        }
        if inputs.parents_lost {
            conditions.level.push("parents lost".to_owned());
        }
        if !inputs.regional_view && inputs.uptime_ms < REGIONAL_VIEW_WAIT_MS {
            conditions.warm_up.push("no regional view".to_owned());
        }
        region_heads_condition(inputs, &mut conditions);
        for copy in &inputs.warm_copies {
            let stream = copy.stream_id;
            if !copy.opened || !copy.decoded {
                conditions
                    .warm_up
                    .push(format!("stream {stream} not decoded"));
            } else if copy.head_part.unwrap_or(0) < copy.target_head {
                conditions.warm_up.push(format!(
                    "stream {stream} head {}/{}",
                    copy.head_part.unwrap_or(0),
                    copy.target_head
                ));
            } else if copy.window_parts < copy.needed_parts {
                conditions.warm_up.push(format!(
                    "window {stream} {}/{}",
                    copy.window_parts, copy.needed_parts
                ));
            }
        }
        Self::decide(inputs, was_warm, inputs.ready_forced, conditions)
    }

    /// A forwarder's (a distributor's) readiness. `was_warm`: the warm-up
    /// was met before. A forwarder has no forced start: while it is not
    /// warm it is ranked last and harms nothing.
    pub fn forwarder(inputs: &ReadinessInputs, was_warm: bool) -> Self {
        let mut conditions = Conditions::default();
        threads_condition(inputs, &mut conditions);
        node_warm_condition(inputs, &mut conditions);
        let plane = &inputs.fast_plane;
        // F2: datagrams arrive and none of them is admitted.
        if plane.received_1s > 0 && plane.admitted_1s == 0 {
            conditions.level.push("peers not admitted".to_owned());
        }
        // F3: idle passes; otherwise every stream a parent delivered in the
        // last ten seconds has a parent that delivers it now.
        let idle = inputs.uptime_ms >= IDLE_AFTER_MS
            && plane
                .received_age_ms
                .is_none_or(|age| age >= IDLE_AFTER_MS);
        if !idle {
            let mut delivered: Vec<(Option<u64>, &str, bool)> = Vec::new();
            for row in inputs.parents.iter().flat_map(|parent| &parent.streams) {
                if row.objects_10s == 0 {
                    continue;
                }
                let now = row
                    .source_age_ms
                    .is_some_and(|age| age <= DELIVERS_WITHIN_MS);
                match delivered
                    .iter_mut()
                    .find(|(stream, track, _)| *stream == row.stream_id && *track == row.track)
                {
                    Some(entry) => entry.2 |= now,
                    None => delivered.push((row.stream_id, &row.track, now)),
                }
            }
            if delivered.iter().any(|(_, _, now)| !now) {
                conditions.level.push("no parent delivers".to_owned());
            }
        }
        // F4, F6: every stream seen now is decoded and holds its window.
        for stream in &inputs.streams {
            if stream
                .seen_age_ms
                .is_some_and(|age| age <= SEEN_WITHIN_MS)
            {
                stream_warm_up(stream, &mut conditions);
            }
        }
        // F5: a whole renewal period since the first hello, and no lease
        // opened just now.
        if inputs.uptime_ms < inputs.lease_renewal_ms
            || inputs
                .lease_opened_age_ms
                .is_some_and(|age| age < LEASES_QUIET_MS)
        {
            conditions.warm_up.push("leases settling".to_owned());
        }
        // F7: a slot's start demand, decoded and windowed, once it is known.
        region_heads_condition(inputs, &mut conditions);
        for stream_id in &inputs.slot_streams {
            match inputs
                .streams
                .iter()
                .find(|stream| stream.stream_id == *stream_id)
            {
                Some(stream) => {
                    if !stream
                        .seen_age_ms
                        .is_some_and(|age| age <= SEEN_WITHIN_MS)
                    {
                        stream_warm_up(stream, &mut conditions);
                    }
                }
                None => conditions
                    .warm_up
                    .push(format!("stream {stream_id} seen, not decoded")),
            }
        }
        Self::decide(inputs, was_warm, false, conditions)
    }

    fn decide(
        inputs: &ReadinessInputs,
        was_warm: bool,
        forced: bool,
        conditions: Conditions,
    ) -> Self {
        let warm = was_warm || conditions.warm_up.is_empty();
        let degraded = !conditions.level.is_empty();
        let mut not_ready = conditions.level;
        // Unmet warm-up conditions are listed while the node is not warm,
        // also on an edge that serves cold after its start timeout.
        if !warm {
            not_ready.extend(conditions.warm_up);
        }
        let drain = inputs.drain;
        not_ready.extend(
            drain
                .list()
                .into_iter()
                .map(|reason| format!("draining: {}", reason.as_str())),
        );
        let state = if drain.stopping {
            NodeState::Stopping
        } else if drain.start || !(warm || forced) {
            NodeState::Starting
        } else if drain.operator {
            NodeState::Draining
        } else if degraded {
            NodeState::Degraded
        } else {
            NodeState::Ready
        };
        Self {
            state,
            warm,
            not_ready,
        }
    }
}

/// E1, F1: the HTTP server answers and every class thread runs.
fn threads_condition(inputs: &ReadinessInputs, conditions: &mut Conditions) {
    if !inputs.http_serving {
        conditions.level.push("http not serving".to_owned());
    }
    if inputs.threads < inputs.threads_expected {
        conditions.level.push(format!(
            "class_threads {}/{}",
            inputs.threads, inputs.threads_expected
        ));
    }
}

/// E5, F7: what must be warm is known only once the region's moving heads
/// are. A region never heard (an edge alone, or no telemetry) goes without
/// after E4's wait; a region heard waits for its heads, at most an edge's
/// start timeout.
fn region_heads_condition(inputs: &ReadinessInputs, conditions: &mut Conditions) {
    if !inputs.region_heads_known
        && (inputs.regional_view || inputs.uptime_ms < REGIONAL_VIEW_WAIT_MS)
    {
        conditions.warm_up.push("region heads not known".to_owned());
    }
}

/// D 2.4: a node whose boot check is configured and not complete is not
/// ready (a level condition: an edge's forced start does not pass it).
fn node_warm_condition(inputs: &ReadinessInputs, conditions: &mut Conditions) {
    if inputs.node_warm_complete == Some(false) {
        conditions
            .level
            .push("node warm check not complete".to_owned());
    }
}

/// F4, F6 of one stream.
fn stream_warm_up(stream: &StreamStatus, conditions: &mut Conditions) {
    let stream_id = stream.stream_id;
    if !stream.decoded {
        conditions
            .warm_up
            .push(format!("stream {stream_id} seen, not decoded"));
    } else if stream.window_parts < stream.warm_parts {
        conditions.warm_up.push(format!(
            "window {stream_id} {}/{}",
            stream.window_parts, stream.warm_parts
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A forwarder that has run for a while, is fed one audio stream by two
    /// parents and holds its window.
    fn fed_forwarder() -> ReadinessInputs {
        ReadinessInputs {
            uptime_ms: 60_000,
            http_serving: true,
            threads: 2,
            threads_expected: 2,
            client_udp_running: true,
            fast_plane: FastPlaneStatus {
                threads: 2,
                threads_expected: 2,
                received_1s: 2_000,
                admitted_1s: 2_000,
                received_age_ms: Some(0),
                ..FastPlaneStatus::default()
            },
            parents: vec![parent("10.80.3.11", 0), parent("10.80.3.12", 1)],
            streams: vec![stream(3001)],
            lease_renewal_ms: 2_200,
            lease_opened_age_ms: Some(30_000),
            regional_view: true,
            region_heads_known: true,
            ..ReadinessInputs::default()
        }
    }

    /// An edge that has run for a while and hears its parents.
    fn serving_edge() -> ReadinessInputs {
        ReadinessInputs {
            uptime_ms: 60_000,
            http_serving: true,
            threads: 2,
            threads_expected: 2,
            client_udp_running: true,
            parent_heard_age_ms: Some(40),
            regional_view: true,
            region_heads_known: true,
            ..ReadinessInputs::default()
        }
    }

    fn parent(address: &str, source_age_ms: u64) -> ParentStatus {
        ParentStatus {
            address: address.to_owned(),
            id: format!("lease-parent-{address}"),
            path: "lease".to_owned(),
            audio_age_ms: Some(source_age_ms),
            video_age_ms: None,
            streams: vec![ParentStreamStatus {
                stream_id: Some(3001),
                track: "opaque-audio".to_owned(),
                class: "audio".to_owned(),
                source_age_ms: Some(source_age_ms),
                repair_age_ms: None,
                head_object: Some(5_123_456),
                window_s: 10,
                objects_1s: 1_000,
                objects_10s: 10_000,
                skipped_10s: 0,
                resets_10s: 0,
                multi_symbol_10s: 0,
                slack_mean_us_1s: Some(993_100),
                slack_mean_us_10s: Some(993_000),
                slack_min_us_10s: Some(990_800),
            }],
        }
    }

    fn stream(stream_id: u64) -> StreamStatus {
        StreamStatus {
            stream_id,
            class: "audio".to_owned(),
            seen_age_ms: Some(0),
            decoded: true,
            ingest_age_ms: Some(1),
            head_part: Some(5_123_456),
            first_part: Some(5_111_456),
            window_parts: 12_001,
            warm_parts: 4_500,
        }
    }

    fn ready(readiness: &Readiness) {
        assert_eq!(readiness.state, NodeState::Ready, "{readiness:?}");
        assert!(readiness.ready() && readiness.warm && readiness.not_ready.is_empty());
    }

    /// A level condition: failing takes `ready` away (`degraded`, with the
    /// reason) from a warm node, and holding again gives it back.
    fn level(
        evaluate: fn(&ReadinessInputs, bool) -> Readiness,
        healthy: &ReadinessInputs,
        fail: impl Fn(&mut ReadinessInputs),
        reason: &str,
    ) {
        ready(&evaluate(healthy, false));
        let mut failing = healthy.clone();
        fail(&mut failing);
        let readiness = evaluate(&failing, true);
        assert_eq!(readiness.state, NodeState::Degraded, "{readiness:?}");
        assert!(readiness.warm, "a level condition leaves warm alone");
        assert_eq!(readiness.not_ready, vec![reason.to_owned()]);
        ready(&evaluate(healthy, true));
    }

    /// A warm-up condition: unmet it keeps a new node `starting`, with the
    /// reason; met once, it stays met when it fails again.
    fn warm_up(
        evaluate: fn(&ReadinessInputs, bool) -> Readiness,
        healthy: &ReadinessInputs,
        fail: impl Fn(&mut ReadinessInputs),
        reason: &str,
    ) {
        let mut failing = healthy.clone();
        fail(&mut failing);
        let readiness = evaluate(&failing, false);
        assert_eq!(readiness.state, NodeState::Starting, "{readiness:?}");
        assert!(!readiness.warm);
        assert_eq!(readiness.not_ready, vec![reason.to_owned()]);
        let met = evaluate(healthy, false);
        ready(&met);
        ready(&evaluate(&failing, met.warm));
    }

    #[test]
    fn e1_class_threads_and_client_udp_are_level_conditions() {
        let edge = serving_edge();
        level(Readiness::edge, &edge, |inputs| inputs.threads = 1, "class_threads 1/2");
        level(
            Readiness::edge,
            &edge,
            |inputs| inputs.client_udp_running = false,
            "client_udp not running",
        );
        level(
            Readiness::edge,
            &edge,
            |inputs| inputs.http_serving = false,
            "http not serving",
        );
    }

    #[test]
    fn e2_a_parent_heard_is_a_level_condition() {
        let edge = serving_edge();
        level(
            Readiness::edge,
            &edge,
            |inputs| inputs.parent_heard_age_ms = Some(2_001),
            "no parent heard",
        );
        level(
            Readiness::edge,
            &edge,
            |inputs| inputs.parent_heard_age_ms = None,
            "no parent heard",
        );
    }

    #[test]
    fn e3_parents_lost_is_a_level_condition() {
        level(
            Readiness::edge,
            &serving_edge(),
            |inputs| inputs.parents_lost = true,
            "parents lost",
        );
    }

    #[test]
    fn e4_a_regional_view_is_a_warm_up_condition() {
        let mut edge = serving_edge();
        edge.uptime_ms = 1_500;
        warm_up(
            Readiness::edge,
            &edge,
            |inputs| inputs.regional_view = false,
            "no regional view",
        );
        // Two seconds after the start the edge goes without one.
        edge.regional_view = false;
        edge.uptime_ms = 2_000;
        ready(&Readiness::edge(&edge, false));
    }

    #[test]
    fn e5_warm_copies_are_a_warm_up_condition() {
        let copy = WarmCopy {
            stream_id: 3001,
            opened: true,
            decoded: true,
            head_part: Some(900),
            target_head: 880,
            window_parts: 4_500,
            needed_parts: 4_500,
        };
        let mut edge = serving_edge();
        edge.warm_copies = vec![copy];
        for (fail, reason) in [
            (
                WarmCopy {
                    opened: false,
                    ..copy
                },
                "stream 3001 not decoded",
            ),
            (
                WarmCopy {
                    decoded: false,
                    ..copy
                },
                "stream 3001 not decoded",
            ),
            (
                WarmCopy {
                    head_part: Some(870),
                    ..copy
                },
                "stream 3001 head 870/880",
            ),
            (
                WarmCopy {
                    window_parts: 1_200,
                    ..copy
                },
                "window 3001 1200/4500",
            ),
        ] {
            warm_up(
                Readiness::edge,
                &edge,
                |inputs| inputs.warm_copies = vec![fail],
                reason,
            );
        }
        // An idle region has no copy to warm: met at once.
        edge.warm_copies.clear();
        ready(&Readiness::edge(&edge, false));
    }

    /// E5 and F7 are judged only once the region's moving heads are known:
    /// a region that has been heard waits for them whatever the uptime (E4's
    /// two seconds do not stand in for them); a region never heard goes
    /// without after E4's wait.
    #[test]
    fn region_heads_are_a_warm_up_condition_of_their_own() {
        for (evaluate, healthy) in [
            (
                Readiness::edge as fn(&ReadinessInputs, bool) -> Readiness,
                serving_edge(),
            ),
            (Readiness::forwarder, fed_forwarder()),
        ] {
            warm_up(
                evaluate,
                &healthy,
                |inputs| inputs.region_heads_known = false,
                "region heads not known",
            );
            let mut heard_early = healthy.clone();
            heard_early.uptime_ms = 1_500;
            // A forwarder's leases settle in a shorter renewal here (F5).
            heard_early.lease_renewal_ms = 1_000;
            warm_up(
                evaluate,
                &heard_early,
                |inputs| inputs.region_heads_known = false,
                "region heads not known",
            );
            // Never heard: two seconds after the start the node goes
            // without; before them it waits.
            let mut unheard = healthy.clone();
            unheard.regional_view = false;
            unheard.region_heads_known = false;
            ready(&evaluate(&unheard, false));
            unheard.uptime_ms = 1_999;
            let readiness = evaluate(&unheard, false);
            assert_eq!(readiness.state, NodeState::Starting, "{readiness:?}");
            assert!(
                readiness
                    .not_ready
                    .contains(&"region heads not known".to_owned()),
                "{readiness:?}"
            );
        }
    }

    #[test]
    fn a_boot_check_not_complete_is_a_level_condition() {
        let nodes: [(fn(&ReadinessInputs, bool) -> Readiness, ReadinessInputs); 2] = [
            (Readiness::edge, serving_edge()),
            (Readiness::forwarder, fed_forwarder()),
        ];
        for (evaluate, mut healthy) in nodes {
            level(
                evaluate,
                &healthy,
                |inputs| inputs.node_warm_complete = Some(false),
                "node warm check not complete",
            );
            healthy.node_warm_complete = Some(true);
            ready(&evaluate(&healthy, false));
        }
        // A forced edge serves cold, never past a failed check.
        let mut edge = serving_edge();
        edge.ready_forced = true;
        edge.node_warm_complete = Some(false);
        assert!(!Readiness::edge(&edge, false).ready());
    }

    #[test]
    fn f1_class_threads_is_a_level_condition() {
        level(
            Readiness::forwarder,
            &fed_forwarder(),
            |inputs| inputs.threads = 0,
            "class_threads 0/2",
        );
    }

    #[test]
    fn peers_not_admitted_is_not_idle() {
        // F2: datagrams arrive and none is admitted. They are datagrams all
        // the same, so the forwarder is not idle either.
        let mut forwarder = fed_forwarder();
        forwarder.parents.clear();
        level(
            Readiness::forwarder,
            &forwarder,
            |inputs| {
                inputs.fast_plane.admitted_1s = 0;
                inputs.fast_plane.not_admitted_1s = 2_000;
            },
            "peers not admitted",
        );
        // A forwarder whose parents' rows went quiet while not-admitted
        // datagrams arrive is not idle: no parent delivers.
        let mut stale = fed_forwarder();
        for row in stale.parents.iter_mut().flat_map(|parent| &mut parent.streams) {
            row.source_age_ms = Some(3_000);
        }
        stale.fast_plane.admitted_1s = 0;
        let readiness = Readiness::forwarder(&stale, true);
        assert_eq!(readiness.state, NodeState::Degraded);
        assert_eq!(
            readiness.not_ready,
            vec!["peers not admitted".to_owned(), "no parent delivers".to_owned()]
        );
    }

    #[test]
    fn f3_a_parent_that_delivers_is_a_level_condition() {
        let forwarder = fed_forwarder();
        level(
            Readiness::forwarder,
            &forwarder,
            |inputs| {
                for row in inputs
                    .parents
                    .iter_mut()
                    .flat_map(|parent| &mut parent.streams)
                {
                    row.source_age_ms = Some(501);
                }
            },
            "no parent delivers",
        );
        // One parent of two delivering is enough.
        let mut one = forwarder.clone();
        one.parents[0].streams[0].source_age_ms = Some(5_000);
        ready(&Readiness::forwarder(&one, false));
    }

    #[test]
    fn an_idle_forwarder_is_ready_without_media() {
        // No relay datagram at all for a second after a second of uptime:
        // idle, and ready with no parent, no stream and nothing received.
        let mut idle = fed_forwarder();
        idle.parents.clear();
        idle.streams.clear();
        idle.fast_plane = FastPlaneStatus {
            threads: 2,
            threads_expected: 2,
            received_age_ms: None,
            ..FastPlaneStatus::default()
        };
        ready(&Readiness::forwarder(&idle, false));
        // A forwarder whose parents stopped feeding a minute ago is idle
        // too: their rows still show the last ten seconds' objects.
        let mut quiet = fed_forwarder();
        quiet.fast_plane.received_1s = 0;
        quiet.fast_plane.admitted_1s = 0;
        quiet.fast_plane.received_age_ms = Some(1_000);
        for row in quiet.parents.iter_mut().flat_map(|parent| &mut parent.streams) {
            row.source_age_ms = Some(1_000);
        }
        quiet.streams[0].seen_age_ms = Some(1_000);
        ready(&Readiness::forwarder(&quiet, false));
        // Silent for less than a second is not idle.
        quiet.fast_plane.received_age_ms = Some(999);
        assert_eq!(
            Readiness::forwarder(&quiet, true).not_ready,
            vec!["no parent delivers".to_owned()]
        );
    }

    #[test]
    fn f4_a_seen_stream_decoded_is_a_warm_up_condition() {
        warm_up(
            Readiness::forwarder,
            &fed_forwarder(),
            |inputs| inputs.streams[0].decoded = false,
            "stream 3001 seen, not decoded",
        );
        // A stream not seen for over a second is not asked to decode.
        let mut gone = fed_forwarder();
        gone.streams[0].decoded = false;
        gone.streams[0].seen_age_ms = Some(1_001);
        ready(&Readiness::forwarder(&gone, false));
    }

    #[test]
    fn f5_settled_leases_are_a_warm_up_condition() {
        warm_up(
            Readiness::forwarder,
            &fed_forwarder(),
            |inputs| inputs.lease_opened_age_ms = Some(499),
            "leases settling",
        );
        warm_up(
            Readiness::forwarder,
            &fed_forwarder(),
            |inputs| inputs.uptime_ms = 2_199,
            "leases settling",
        );
        let mut none = fed_forwarder();
        none.lease_opened_age_ms = None;
        ready(&Readiness::forwarder(&none, false));
    }

    #[test]
    fn f6_the_window_is_a_warm_up_condition() {
        warm_up(
            Readiness::forwarder,
            &fed_forwarder(),
            |inputs| inputs.streams[0].window_parts = 1_200,
            "window 3001 1200/4500",
        );
        // No warm window asked: met with any window.
        let mut none = fed_forwarder();
        none.streams[0].warm_parts = 0;
        none.streams[0].window_parts = 1;
        ready(&Readiness::forwarder(&none, false));
    }

    #[test]
    fn f7_a_slot_s_start_demand_is_a_warm_up_condition() {
        let mut slot = fed_forwarder();
        slot.slot_streams = vec![3001];
        warm_up(
            Readiness::forwarder,
            &slot,
            |inputs| inputs.slot_streams.push(3003),
            "stream 3003 seen, not decoded",
        );
        let mut quiet = slot.clone();
        quiet.streams.push(StreamStatus {
            seen_age_ms: Some(5_000),
            window_parts: 10,
            ..stream(3002)
        });
        quiet.slot_streams = vec![3002];
        warm_up(
            Readiness::forwarder,
            &fed_forwarder(),
            |inputs| *inputs = quiet.clone(),
            "window 3002 10/4500",
        );
    }

    #[test]
    fn a_level_condition_takes_ready_away_and_gives_it_back() {
        let edge = serving_edge();
        ready(&Readiness::edge(&edge, true));
        let mut lost = edge.clone();
        lost.parents_lost = true;
        lost.parent_heard_age_ms = Some(3_000);
        let readiness = Readiness::edge(&lost, true);
        assert_eq!(readiness.state, NodeState::Degraded);
        assert!(!readiness.ready());
        assert_eq!(
            readiness.not_ready,
            vec!["no parent heard".to_owned(), "parents lost".to_owned()]
        );
        ready(&Readiness::edge(&edge, readiness.warm));
    }

    #[test]
    fn a_warm_up_condition_stays_met() {
        let mut forwarder = fed_forwarder();
        forwarder.streams[0].window_parts = 10;
        let cold = Readiness::forwarder(&forwarder, false);
        assert_eq!(cold.state, NodeState::Starting);
        assert_eq!(cold.not_ready, vec!["window 3001 10/4500".to_owned()]);
        forwarder.streams[0].window_parts = 4_500;
        let warm = Readiness::forwarder(&forwarder, cold.warm);
        ready(&warm);
        // A new stream with no window yet does not make it cold again.
        forwarder.streams.push(StreamStatus {
            decoded: false,
            window_parts: 0,
            ..stream(3002)
        });
        ready(&Readiness::forwarder(&forwarder, warm.warm));
    }

    #[test]
    fn a_drained_node_is_not_ready_and_says_why() {
        let mut edge = serving_edge();
        edge.drain.operator = true;
        let readiness = Readiness::edge(&edge, true);
        assert_eq!(readiness.state, NodeState::Draining);
        assert!(!readiness.ready());
        assert_eq!(readiness.not_ready, vec!["draining: operator".to_owned()]);
        // Starting comes before draining, stopping before both.
        edge.drain.start = true;
        let readiness = Readiness::edge(&edge, false);
        assert_eq!(readiness.state, NodeState::Starting);
        assert_eq!(
            readiness.not_ready,
            vec!["draining: start".to_owned(), "draining: operator".to_owned()]
        );
        edge.drain.stopping = true;
        assert_eq!(Readiness::edge(&edge, true).state, NodeState::Stopping);
        assert_eq!(
            edge.drain.list(),
            vec![DrainReason::Start, DrainReason::Operator, DrainReason::Stopping]
        );
    }

    #[test]
    fn a_forced_edge_serves_cold_and_a_forwarder_is_never_forced() {
        let mut edge = serving_edge();
        edge.warm_copies = vec![WarmCopy {
            stream_id: 3001,
            ..WarmCopy::default()
        }];
        assert_eq!(Readiness::edge(&edge, false).state, NodeState::Starting);
        edge.ready_forced = true;
        let forced = Readiness::edge(&edge, false);
        assert_eq!(forced.state, NodeState::Ready);
        assert!(forced.ready() && !forced.warm);
        assert_eq!(forced.not_ready, vec!["stream 3001 not decoded".to_owned()]);
        let mut forwarder = fed_forwarder();
        forwarder.streams[0].decoded = false;
        forwarder.ready_forced = true;
        assert_eq!(
            Readiness::forwarder(&forwarder, false).state,
            NodeState::Starting
        );
    }

    #[test]
    fn the_arguments_digest_is_hex_of_sha256() {
        let digest = args_sha256();
        assert_eq!(digest.len(), 64);
        assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(digest, args_sha256());
    }

    /// The JSON of `GET /api/node` (a golden file): a change to a name or a
    /// shape here is a change to every tool that reads it.
    #[test]
    fn node_status_json_is_stable() {
        let status = NodeStatus {
            schema: SCHEMA.to_owned(),
            node_id: "tok-dist-a".to_owned(),
            region: "tokyo".to_owned(),
            host: Some("4f1c2a9e07b3".to_owned()),
            role: "distributor".to_owned(),
            build: BuildStatus {
                git: "3f2a9c1d77e0".to_owned(),
                git_full: "3f2a9c1d77e0b4c1aa00112233445566778899aa".to_owned(),
                dirty: false,
                built_unix: 1_763_900_000,
                version: "0.1.0".to_owned(),
            },
            process: ProcessStatus {
                pid: 812,
                boot_id: 1_763_924_000_123_456,
                started_unix_ms: 1_763_924_000_123,
                uptime_ms: 86_400,
                args_sha256: "9a1f".to_owned(),
                cpus: 2,
                cpu_permille: 180,
            },
            state: NodeState::Ready,
            ready: true,
            warm: true,
            ready_unix_ms: Some(1_763_924_006_400),
            ready_forced: false,
            not_ready: Vec::new(),
            drain_reasons: Vec::new(),
            hello_draining: false,
            fast_plane: FastPlaneStatus {
                threads: 2,
                threads_expected: 2,
                realtime_threads: 1,
                pinned_threads: 2,
                received_1s: 4_120,
                admitted_1s: 4_120,
                not_admitted_1s: 0,
                received_age_ms: Some(0),
            },
            ingest: IngestStatus::default(),
            parents: vec![ParentStatus {
                address: "172.236.1.10".to_owned(),
                id: "lon-dist-a".to_owned(),
                path: "primary".to_owned(),
                ..parent("172.236.1.10", 0)
            }],
            streams: vec![stream(3001)],
            children: ChildrenStatus {
                compiled: vec![CompiledChild {
                    target: "139.162.1.5:22200".to_owned(),
                    lane: "source".to_owned(),
                    promoted: false,
                }],
                lease: Some(LeaseStatus {
                    children: 3,
                    pairs: 6,
                    refused: 0,
                    pairs_list: vec![LeasePair {
                        child: "10.80.3.21".to_owned(),
                        stream_id: 3001,
                        primary: Some(true),
                        rank: Some(0),
                        warm_parts_pending: 0,
                    }],
                    streams: vec![LeaseStream {
                        stream_id: 3001,
                        children: 3,
                        fed_age_ms: Some(0),
                    }],
                }),
            },
            cache_mesh: CacheMeshStatus {
                parent_heard_age_ms: Some(40),
                parents_live: 2,
                pending_release: 0,
                peers: vec![CacheMeshPeerStatus {
                    peer: "10.80.3.12:29101".to_owned(),
                    role: Some("distributor".to_owned()),
                    boot_id: Some(1_763_923_000_000_000),
                    build: Some("3f2a9c1d77e0".to_owned()),
                    state: Some("ready".to_owned()),
                    heard_age_ms: Some(40),
                }],
                children: 3,
                pairs: 6,
                subscribed_playlists: vec![3001],
                warm_parts_pending: 0,
                rx_unknown_kinds: 0,
                rx_unknown_roles: 0,
                rx_invalid_frames: 0,
            },
            client_udp: Some(ClientUdpStatus {
                subscribers: Some(120),
                hls_sessions: 4,
                hls_moved_active: 1,
                hls_requests_in_flight: 2,
                refusals: 0,
                load_permille: 310,
                rebalance_moves: 0,
                parents_lost: false,
            }),
            entry: EntryStatus {
                caches_fresh: 3,
                caches_draining: 0,
            },
            node_warm: Some(crate::node_warm::NodeWarmStatus {
                configured: true,
                complete: true,
                verdict: "ok".to_owned(),
                warm: Some(serde_json::json!({"ran": true, "finished": true, "gib": 3.31})),
                check: Some(serde_json::json!({"seconds": 60, "stops_over_20ms": 0})),
            }),
        };
        let json = serde_json::to_string_pretty(&status).unwrap() + "\n";
        let golden = include_str!("tests/node_status.json");
        assert_eq!(json, golden, "the JSON of GET /api/node changed:\n{json}");
        let parsed: NodeStatus = serde_json::from_str(golden).unwrap();
        assert_eq!(parsed, status);
    }
}
