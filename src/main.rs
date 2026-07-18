mod control;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use av_mesh::relay_ingress::{
    ControlledRelayParentSession, RelayIngressOutcome, RelayIngressSnapshot, RelayObjectReceiver,
    RelayObjectReceiverConfig, RelayUdpDispatch, RelayUdpDispatchOutcome,
};
use av_mesh::replication::{
    DemandSignal, MeshNode, ReplicaPlacement, ReplicaReason, ReplicationPolicy, StreamInfo,
};
use av_mesh::udp_fec::{UdpFecPushOutcome, UdpFecReceiver};
use bytes::{BufMut, Bytes, BytesMut};
use clap::{Parser, ValueEnum};
use control::{
    packetize_control_message, reassemble_unsigned_control_packets, MeshControlEvent,
    MeshControlMessage,
};
use futures_util::{SinkExt, StreamExt};
use h3_webtransport::server::{AcceptedBi, WebTransportSession};
use http::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
#[cfg(feature = "linode-provisioner")]
use linode::{regions::REGIONS as LINODE_REGIONS, LinodeClient};
use media_object::{MediaObject, ObjectKind, Stage, WIRE_MAGIC};
use playlists::chunk_cache::{ChunkCache, PutIfAbsentResult};
use playlists::mesh::{CacheMesh, CacheMeshConfig, CacheMeshFecStats, CacheMeshHandle};
use playlists::Options as CacheOptions;
use raptorq_datagram_fec::{
    decode_serialized_media_access_unit, inspect_multichannel_audio_datagram, DecodedMediaFrame,
    MediaCodec, MediaDatagramRole, MediaFecDecoder, MediaFragmentHeader, MediaFrameMetadata,
    DATAGRAM_MAGIC, MEDIA_FRAME_HEADER_LEN,
};
use raptorq_fec_transport::{split_stream_id_prefix, FecDatagramDecoder, STREAM_ID_PREFIX_LEN};
use relay_session::{
    CarrierIdentity, CarrierKind, FailoverForwardMode, FailoverLeaseCommand, NodeId, ParentPath,
    SubscriptionId, TopologyGeneration, TrustMode, FAILOVER_CONTROL_WIRE_LEN,
};
use serde::{de, Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex as StdMutex, RwLock as StdRwLock,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tcp_changes::{
    Client as TcpChangesClient, Message as TcpChangesMessage, Payload as TcpChangesPayload,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::{
    net::UdpSocket,
    sync::{broadcast, mpsc, watch, Mutex as AsyncMutex, RwLock},
    time::{interval, sleep, MissedTickBehavior},
};
use tokio_tungstenite::{tungstenite::Message as WebSocketMessage, WebSocketStream};
use tracing::{debug, info, warn};
use web_service::{
    load_default_tls_base64, load_tls_base64_from_paths, read_length_prefixed_frame,
    write_length_prefixed_frame, BodyStream, H2H3Server, HandlerResponse, HandlerResult,
    RawTcpHandler, Router, Server, ServerBuilder, ServerError, StreamWriter, WebSocketHandler,
    WebTransportHandler,
};

const DEFAULT_STREAM_ID: u64 = 1;
const DEFAULT_MESH_FEC_REPAIR_SYMBOLS: u32 = 1;
const DEFAULT_MESH_FEC_REPAIR_RATIO: f32 = 0.03;
const DEFAULT_MESH_FEC_MAX_REPAIR_SYMBOLS: u32 = 32;
const DEFAULT_MESH_FEC_SYMBOL_SIZE: u16 = 1316;
const DEFAULT_MESH_SYNC_INTERVAL_MS: u64 = 20;
const DEFAULT_MAX_RELAY_DOWNSTREAM_CHILDREN: usize = 4;
const DEFAULT_RELAY_PRIMARY_SILENCE_MS: u64 = 250;
const DEFAULT_RELAY_PRIMARY_RECOVERY_MS: u64 = 2_000;
const DEFAULT_RELAY_SECONDARY_WARM_MS: u64 = 750;
const DEFAULT_RELAY_FAILOVER_HEARTBEAT_MS: u64 = 100;
const DEFAULT_RELAY_FAILOVER_LEASE_MS: u64 = 1_000;
const RELAY_WARM_SOURCE_REPLAY_MAX_OBJECTS_PER_CHILD: usize = 4;
const RELAY_WARM_SOURCE_REPLAY_MAX_DATAGRAMS_PER_CHILD: usize = 2_048;
const RELAY_WARM_SOURCE_REPLAY_MAX_BYTES_PER_CHILD: usize = 4 * 1024 * 1024;
// The reliable subscription/catalog lane will supply this for late joins. The
// local transition path starts each canonical stream at object zero.
const PART_WAIT_MS: u64 = 3_000;
const LLHLS_TAIL_WAIT_MS: u64 = 250;
const CANONICAL_STREAM_IDLE_RETENTION: Duration = Duration::from_secs(5 * 60);
const REPLICA_REQUEST_MIN_INTERVAL_MS: u64 = 1_000;
const MESH_EVENTS_PATH: &str = "/api/mesh/events";
const MESH_WEBSOCKET_PATH: &str = "/ws/mesh";
const MISSION_CONTROL_DIST_ENV: &str = "NEEDLETAIL_MISSION_CONTROL_DIST";
const MESH_METRICS_PATH: &str = "/metrics";
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
const MEDIA_ACCESS_UNIT_CONTENT_TYPE: &str = "application/vnd.wavey.media-access-unit";
const LIVE_FMP4_CONTENT_TYPE: &str = "video/mp4";
const LIVE_TS_CONTENT_TYPE: &str = "video/mp2t";
const AUDIO_EPOCH_SUBSCRIPTION: &[u8] = b"WAVEY-AUDIO-EPOCH/1";
const AUDIO_EPOCH_SUBSCRIPTION_V2_PREFIX: &[u8] = b"WAVEY-AUDIO-EPOCH/2 ";
const AUDIO_EPOCH_BROADCAST_CAPACITY: usize = 2048;
const NATIVE_AUDIO_SUBSCRIBE: &[u8] = b"WAVEY-DAW-SUBSCRIBE/1";
const NATIVE_AUDIO_SUBSCRIBE_ACK: &[u8] = b"WAVEY-DAW-SUBSCRIBED/1";
const NATIVE_AUDIO_SUBSCRIBE_V2_PREFIX: &[u8] = b"WAVEY-DAW-SUBSCRIBE/2 ";
const NATIVE_AUDIO_SUBSCRIBE_ACK_V2_PREFIX: &[u8] = b"WAVEY-DAW-SUBSCRIBED/2 ";
const NATIVE_AUDIO_UNSUBSCRIBE: &[u8] = b"WAVEY-DAW-UNSUBSCRIBE/1";
const NATIVE_AUDIO_UNSUBSCRIBE_V2_PREFIX: &[u8] = b"WAVEY-DAW-UNSUBSCRIBE/2 ";
const NATIVE_AUDIO_SUBSCRIPTION_TTL: Duration = Duration::from_secs(15);
const MULTICHANNEL_AUDIO_TRANSPORT_MAGIC: &[u8] = b"AEP1";

#[derive(Debug, Clone, PartialEq, Eq)]
struct AudioEpochDatagram {
    session_id: Option<u64>,
    bytes: Bytes,
}

fn parse_audio_epoch_subscription(payload: &[u8]) -> Option<Option<u64>> {
    if payload == AUDIO_EPOCH_SUBSCRIPTION {
        return Some(None);
    }
    let session = payload.strip_prefix(AUDIO_EPOCH_SUBSCRIPTION_V2_PREFIX)?;
    let session = std::str::from_utf8(session).ok()?;
    if session.is_empty() || !session.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    session.parse().ok().map(Some)
}

fn parse_native_audio_session_message(payload: &[u8], prefix: &[u8]) -> Option<u64> {
    let session = payload.strip_prefix(prefix)?;
    let session = std::str::from_utf8(session).ok()?;
    if session.is_empty() || !session.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    session.parse().ok()
}

#[derive(Debug, Clone, Copy)]
struct NativeAudioSubscription {
    session_id: Option<u64>,
    expires_at: Instant,
}

#[derive(Debug, Default)]
struct NativeAudioRelay {
    subscriptions: HashMap<SocketAddr, NativeAudioSubscription>,
}

impl NativeAudioRelay {
    fn expire(&mut self, now: Instant) {
        self.subscriptions
            .retain(|_, subscription| subscription.expires_at > now);
    }

    async fn handle_control(
        &mut self,
        socket: &UdpSocket,
        peer: SocketAddr,
        payload: &[u8],
    ) -> bool {
        if payload == NATIVE_AUDIO_SUBSCRIBE {
            self.subscriptions.insert(
                peer,
                NativeAudioSubscription {
                    session_id: None,
                    expires_at: Instant::now() + NATIVE_AUDIO_SUBSCRIPTION_TTL,
                },
            );
            if let Err(error) = socket.send_to(NATIVE_AUDIO_SUBSCRIBE_ACK, peer).await {
                warn!(peer = %peer, error = %error, "failed to acknowledge native audio relay subscription");
            }
            return true;
        }
        if let Some(session_id) =
            parse_native_audio_session_message(payload, NATIVE_AUDIO_SUBSCRIBE_V2_PREFIX)
        {
            self.subscriptions.insert(
                peer,
                NativeAudioSubscription {
                    session_id: Some(session_id),
                    expires_at: Instant::now() + NATIVE_AUDIO_SUBSCRIPTION_TTL,
                },
            );
            let mut ack = Vec::with_capacity(NATIVE_AUDIO_SUBSCRIBE_ACK_V2_PREFIX.len() + 20);
            ack.extend_from_slice(NATIVE_AUDIO_SUBSCRIBE_ACK_V2_PREFIX);
            ack.extend_from_slice(session_id.to_string().as_bytes());
            if let Err(error) = socket.send_to(&ack, peer).await {
                warn!(peer = %peer, session_id, error = %error, "failed to acknowledge session-scoped native audio relay subscription");
            }
            return true;
        }
        if payload.starts_with(NATIVE_AUDIO_SUBSCRIBE_V2_PREFIX) {
            warn!(peer = %peer, "ignored malformed session-scoped native audio relay subscription");
            return true;
        }
        if payload == NATIVE_AUDIO_UNSUBSCRIBE {
            self.subscriptions.remove(&peer);
            return true;
        }
        if let Some(session_id) =
            parse_native_audio_session_message(payload, NATIVE_AUDIO_UNSUBSCRIBE_V2_PREFIX)
        {
            if self
                .subscriptions
                .get(&peer)
                .is_some_and(|subscription| subscription.session_id == Some(session_id))
            {
                self.subscriptions.remove(&peer);
            }
            return true;
        }
        if payload.starts_with(NATIVE_AUDIO_UNSUBSCRIBE_V2_PREFIX) {
            warn!(peer = %peer, "ignored malformed session-scoped native audio relay unsubscription");
            return true;
        }
        false
    }

    fn forward(&self, socket: &UdpSocket, datagram: &[u8], session_id: Option<u64>) {
        for (target, subscription) in &self.subscriptions {
            if subscription.session_id.is_some() && subscription.session_id != session_id {
                continue;
            }
            match socket.try_send_to(datagram, *target) {
                Ok(sent) if sent == datagram.len() => {}
                Ok(sent) => warn!(
                    target = %target,
                    sent,
                    expected = datagram.len(),
                    "native audio relay sent a partial datagram"
                ),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    debug!(target = %target, "native audio relay skipped a datagram under socket backpressure");
                }
                Err(error) => {
                    warn!(target = %target, error = %error, "native audio relay send failed")
                }
            }
        }
    }
}
const MESH_FMP4_SLOT_MAGIC: &[u8; 8] = b"AVFMP4S1";
const MESH_FMP4_SLOT_HEADER_LEN: usize = 16;
const TELEMETRY_TAG: [u8; 4] = *b"AVMT";
const CONTROL_TAG: [u8; 4] = *b"AVMC";
const DEFAULT_TELEMETRY_STALE_MS: u64 = 30_000;
const RAW_MESH_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const MESH_STORAGE_WARN_PCT: u64 = 85;
const MESH_STORAGE_ERROR_PCT: u64 = 95;
const MESH_MIN_STALE_INGEST_ALERT_MS: u64 = 5_000;
const MESH_STREAM_LAG_WARN_PARTS: u64 = 6;
const CANONICAL_EPOCH_ACTIVATION_WARN_US: u64 = 10_000_000;
const RELAY_PROCESSING_P95_WARN_US: u64 = 1_000;
const MESH_ACTIVITY_LIMIT: usize = 64;
const EDGE_RECENT_RESPONSE_LIMIT: usize = 32;
const EDGE_RESPONSE_DURATION_BUCKETS_US: [u64; 13] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000,
];
const PUBLICATION_AVAILABILITY_BUCKETS_US: [u64; 16] = [
    1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 75_000, 100_000, 125_000, 150_000, 175_000,
    200_000, 250_000, 500_000, 1_000_000, 2_000_000,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveMediaKind {
    Fmp4,
    Ts,
}

impl LiveMediaKind {
    fn extension(self) -> &'static str {
        match self {
            Self::Fmp4 => "mp4",
            Self::Ts => "ts",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::Fmp4 => LIVE_FMP4_CONTENT_TYPE,
            Self::Ts => LIVE_TS_CONTENT_TYPE,
        }
    }
}

fn is_multichannel_audio_transport_datagram(datagram: &[u8]) -> bool {
    datagram.starts_with(MULTICHANNEL_AUDIO_TRANSPORT_MAGIC)
}

enum LiveSlotPayload {
    Fmp4 { init: Option<Bytes>, media: Bytes },
    Opaque(Bytes),
    Invalid,
}

impl LiveSlotPayload {
    fn decode(payload: Bytes) -> Self {
        Self::decode_inner(payload, None)
    }

    fn decode_for_stream(payload: Bytes, stream_id: u64) -> Self {
        Self::decode_inner(payload, Some(stream_id))
    }

    fn decode_inner(payload: Bytes, expected_stream_id: Option<u64>) -> Self {
        if payload.starts_with(&WIRE_MAGIC) {
            let Ok(object) = media_object::decode(&payload) else {
                return Self::Invalid;
            };
            if expected_stream_id
                .is_some_and(|stream_id| object.key().stream() != stream_id.to_string())
            {
                return Self::Invalid;
            }
            let is_fmp4 = object
                .metadata()
                .get("container")
                .is_some_and(|container| container.as_slice() == b"fmp4");
            let is_fmp4_slot = object
                .metadata()
                .get("payload-format")
                .is_some_and(|format| format.as_slice() == b"fmp4-slot-v1");
            let object_payload = Bytes::copy_from_slice(object.payload());
            return match object.kind() {
                ObjectKind::Media if is_fmp4 && is_fmp4_slot => {
                    Self::decode_fmp4_slot(object_payload).unwrap_or(Self::Invalid)
                }
                ObjectKind::Media if is_fmp4 => Self::Fmp4 {
                    init: None,
                    media: object_payload,
                },
                ObjectKind::Media => Self::Opaque(object_payload),
                ObjectKind::Initialization | ObjectKind::CodecConfiguration if is_fmp4 => {
                    Self::Fmp4 {
                        init: Some(object_payload),
                        media: Bytes::new(),
                    }
                }
                ObjectKind::Initialization
                | ObjectKind::CodecConfiguration
                | ObjectKind::Discontinuity => Self::Invalid,
            };
        }
        Self::decode_fmp4_slot(payload.clone()).unwrap_or(Self::Opaque(payload))
    }

    fn decode_fmp4_slot(payload: Bytes) -> Option<Self> {
        if payload.len() < MESH_FMP4_SLOT_HEADER_LEN || !payload.starts_with(MESH_FMP4_SLOT_MAGIC) {
            return None;
        }

        let init_len = u32::from_be_bytes(payload[8..12].try_into().unwrap()) as usize;
        let media_len = u32::from_be_bytes(payload[12..16].try_into().unwrap()) as usize;
        let init_end = MESH_FMP4_SLOT_HEADER_LEN.checked_add(init_len)?;
        let media_end = init_end.checked_add(media_len)?;
        if media_end != payload.len() {
            return None;
        }

        let init = (init_len > 0).then(|| payload.slice(MESH_FMP4_SLOT_HEADER_LEN..init_end));
        let media = payload.slice(init_end..media_end);
        Some(Self::Fmp4 { init, media })
    }

    fn media_kind(&self) -> LiveMediaKind {
        match self {
            Self::Fmp4 { .. } => LiveMediaKind::Fmp4,
            Self::Opaque(_) | Self::Invalid => LiveMediaKind::Ts,
        }
    }

    fn init(&self) -> Option<Bytes> {
        match self {
            Self::Fmp4 { init, .. } => init.clone(),
            Self::Opaque(_) | Self::Invalid => None,
        }
    }

    fn media(&self) -> Bytes {
        match self {
            Self::Fmp4 { media, .. } => media.clone(),
            Self::Opaque(payload) => payload.clone(),
            Self::Invalid => Bytes::new(),
        }
    }

    fn has_media(&self) -> bool {
        match self {
            Self::Fmp4 { media, .. } => !media.is_empty(),
            Self::Opaque(payload) => !payload.is_empty(),
            Self::Invalid => false,
        }
    }
}

fn decode_canonical_stream_object(payload: &[u8]) -> Result<Option<MediaObject>> {
    if !payload.starts_with(&WIRE_MAGIC) {
        return Ok(None);
    }
    media_object::decode(payload)
        .map(Some)
        .context("invalid canonical media-object envelope")
}

#[derive(Debug, Clone, Parser)]
#[command(name = "av-mesh", about = "Run a local AV mesh node")]
struct Args {
    #[arg(long, default_value = "uk")]
    region: String,

    #[arg(long)]
    node_id: Option<String>,

    #[arg(long, default_value = "127.0.0.1:9101")]
    mesh_bind: SocketAddr,

    #[arg(long = "peer")]
    peers: Vec<String>,

    #[arg(long, default_value_t = DEFAULT_MESH_SYNC_INTERVAL_MS)]
    mesh_sync_interval_ms: u64,

    #[arg(long, default_value_t = DEFAULT_MESH_FEC_REPAIR_SYMBOLS)]
    mesh_repair_symbols: u32,

    #[arg(long, default_value_t = DEFAULT_MESH_FEC_REPAIR_RATIO)]
    mesh_repair_ratio: f32,

    #[arg(long, default_value_t = DEFAULT_MESH_FEC_MAX_REPAIR_SYMBOLS)]
    mesh_max_repair_symbols: u32,

    #[arg(long, default_value_t = DEFAULT_MESH_FEC_SYMBOL_SIZE)]
    mesh_symbol_size: u16,

    #[cfg(feature = "private-subnet-discovery")]
    #[arg(long)]
    private_subnet_discovery: bool,

    #[cfg(feature = "private-subnet-discovery")]
    #[arg(long, default_value_t = 12345)]
    private_discovery_broadcast_port: u16,

    #[cfg(feature = "private-subnet-discovery")]
    #[arg(long)]
    private_discovery_mesh_port: Option<u16>,

    #[arg(long, default_value = "127.0.0.1:12001")]
    fec_bind: SocketAddr,

    /// Enable deterministic direct-UDP RelaySession qualification. The peer
    /// address bindings below identify controlled endpoints and derive bounded
    /// object announcements from the first validated symbol.
    #[arg(long)]
    relay_controlled_local: bool,

    #[arg(long)]
    relay_primary_peer: Option<SocketAddr>,

    /// Primary RelaySession receive socket. During the transition this equals
    /// `--fec-bind`, so RLS1 and legacy RQD2 coexist on the first socket.
    #[arg(long)]
    relay_primary_bind: Option<SocketAddr>,

    #[arg(long)]
    relay_secondary_peer: Option<SocketAddr>,

    /// Independent secondary repair-lane receive socket owned by the same
    /// object assembler and LL-HLS cache.
    #[arg(long)]
    relay_secondary_bind: Option<SocketAddr>,

    #[arg(long, default_value = "av-contrib-primary")]
    relay_primary_id: String,

    /// Admit source plus repair intent on a fully seeded primary relationship.
    /// This is used by a warm backbone relay that can be activated immediately.
    #[arg(long)]
    relay_primary_promoted: bool,

    #[arg(long, default_value = "av-contrib-secondary")]
    relay_secondary_id: String,

    /// Admit both source and repair intent on the warm secondary carrier while
    /// it is fully seeded for immediate promotion.
    #[arg(long)]
    relay_secondary_promoted: bool,

    #[arg(long, default_value_t = 1)]
    relay_topology_generation: u64,

    #[arg(long, default_value_t = 1)]
    relay_subscription_id: u64,

    /// Forward newly admitted RelaySession symbols to one subscribed child.
    /// Repeat as `BIND=TARGET,ROLE`, where ROLE is source, repair, or all. Each
    /// child gets a stable source socket and an explicit compiled symbol lane.
    #[arg(long = "relay-forward", value_parser = parse_relay_forward_endpoint)]
    relay_forwards: Vec<RelayForwardEndpoint>,

    /// Controlled-private qualification listener for leased warm-secondary
    /// promotion. Repeat as `BIND=CONTROLLER_PEER,FORWARD_TARGET`. Public
    /// deployments carry the same command on an authenticated control stream.
    #[arg(
        long = "relay-failover-listener",
        value_parser = parse_relay_failover_listener_endpoint
    )]
    relay_failover_listeners: Vec<RelayFailoverListenerEndpoint>,

    /// Edge-side controlled-private failover controller as `BIND=TARGET`.
    #[arg(
        long = "relay-failover-controller",
        value_parser = parse_relay_failover_controller_endpoint
    )]
    relay_failover_controller: Option<RelayFailoverControllerEndpoint>,

    /// Promote the warm secondary after this much primary-source silence.
    #[arg(long, default_value_t = DEFAULT_RELAY_PRIMARY_SILENCE_MS)]
    relay_primary_silence_ms: u64,

    /// Keep both source paths active for this continuous primary recovery
    /// window before demoting the warm secondary.
    #[arg(long, default_value_t = DEFAULT_RELAY_PRIMARY_RECOVERY_MS)]
    relay_primary_recovery_ms: u64,

    /// Maximum age of a repair symbol that proves the secondary is warm.
    #[arg(long, default_value_t = DEFAULT_RELAY_SECONDARY_WARM_MS)]
    relay_secondary_warm_ms: u64,

    #[arg(long, default_value_t = DEFAULT_RELAY_FAILOVER_HEARTBEAT_MS)]
    relay_failover_heartbeat_ms: u64,

    #[arg(long, default_value_t = DEFAULT_RELAY_FAILOVER_LEASE_MS)]
    relay_failover_lease_ms: u64,

    /// Hard fanout bound for explicitly compiled downstream subscriptions.
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_RELAY_DOWNSTREAM_CHILDREN
    )]
    relay_max_downstream_children: usize,

    #[arg(long, default_value = "127.0.0.1:12101")]
    media_fec_bind: SocketAddr,

    #[arg(long, default_value_t = 9444)]
    http_port: u16,

    #[arg(long)]
    playback_base_url: Option<String>,

    #[arg(long)]
    edge_websocket: bool,

    #[arg(long)]
    edge_webtransport: bool,

    #[arg(long)]
    raw_tcp_port: Option<u16>,

    #[arg(long)]
    raw_tcp_tls: bool,

    #[arg(long)]
    cert: Option<PathBuf>,

    #[arg(long)]
    key: Option<PathBuf>,

    #[arg(long, default_value_t = DEFAULT_STREAM_ID)]
    stream_id: u64,

    #[arg(long, env = "AV_LL_HLS_PART_MS", default_value_t = 50)]
    part_ms: u64,

    #[arg(long, default_value_t = 4)]
    parts_per_segment: usize,

    #[arg(long, default_value_t = 24)]
    window_parts: usize,

    #[arg(long, default_value_t = 2048)]
    slot_kb: usize,

    #[arg(long, default_value = "eu")]
    continent: String,

    #[arg(long, default_value_t = 51.5074)]
    latitude: f64,

    #[arg(long, default_value_t = -0.1278)]
    longitude: f64,

    #[arg(long, default_value_t = 100_000_000_000)]
    storage_bytes: u64,

    #[arg(long, default_value_t = 10_000_000_000)]
    egress_capacity_bps: u64,

    #[arg(long, default_value_t = 0)]
    baseline_per_region: usize,

    #[arg(long, default_value_t = 1)]
    baseline_per_continent: usize,

    #[arg(long, default_value_t = 300.0)]
    min_mirror_distance_km: f64,

    #[arg(long)]
    telemetry_bind: Option<SocketAddr>,

    #[arg(long = "telemetry-peer")]
    telemetry_peers: Vec<String>,

    #[arg(long, default_value = "local.wavey.ai")]
    telemetry_dns_name: String,

    #[arg(long, default_value_t = Ipv4Addr::LOCALHOST)]
    telemetry_private_ipv4: Ipv4Addr,

    #[arg(long, default_value_t = 1000)]
    telemetry_interval_ms: u64,

    #[arg(long, default_value_t = DEFAULT_TELEMETRY_STALE_MS)]
    telemetry_stale_ms: u64,

    #[arg(long, default_value_t = 1000)]
    replication_plan_interval_ms: u64,

    #[arg(long)]
    provision_command: Option<String>,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long)]
    linode_provision: bool,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long)]
    linode_image_id: Option<String>,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long)]
    linode_instance_type: Option<String>,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long)]
    linode_domain_id: Option<u64>,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long, default_value = "av-mesh")]
    linode_vlan_tag: String,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long, default_value = "LINODE_API_TOKEN")]
    linode_token_env: String,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long, default_value = "LINODE_PUB_KEY")]
    linode_pub_key_env: String,

    #[cfg(feature = "linode-provisioner")]
    #[arg(long = "linode-region-map", value_parser = parse_key_value)]
    linode_region_maps: Vec<(String, String)>,

    #[arg(long, default_value_t = 30_000)]
    provision_timeout_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "av_mesh=info,playlists=info,web_service=info".into()),
        )
        .init();

    let args = Args::parse().normalized()?;
    let mesh_peers = resolve_socket_addr_args("peer", &args.peers).await?;
    let telemetry_peers = resolve_socket_addr_args("telemetry-peer", &args.telemetry_peers).await?;
    let node_id = args.node_id.clone().unwrap_or_else(|| args.region.clone());
    let node_profile = MeshNode {
        node_id: node_id.clone(),
        region: args.region.clone(),
        continent: args.continent.clone(),
        latitude: args.latitude,
        longitude: args.longitude,
        total_storage_bytes: args.storage_bytes,
        used_storage_bytes: 0,
        egress_capacity_bps: args.egress_capacity_bps,
        contributor_streams: 0,
        active_streams: 0,
        draining: false,
    };
    let replication_policy = ReplicationPolicy {
        baseline_per_region: args.baseline_per_region,
        baseline_per_continent: args.baseline_per_continent,
        min_mirror_distance_km: args.min_mirror_distance_km,
        ..ReplicationPolicy::default()
    };
    let control_plane = ControlPlane::default();
    let control_dispatch = ControlDispatch::default();
    let demand_tracker = DemandTracker::default();
    let lifecycle = NodeLifecycle::default();
    let telemetry_aggregator = TelemetryAggregator::new(args.telemetry_stale_ms);
    let telemetry_peer_monitor = TelemetryPeerMonitor::new(&telemetry_peers);
    let playback_base_url = args
        .playback_base_url
        .as_deref()
        .map(normalize_playback_base_url);
    let edge_load = EdgeLoad::default();
    let provision_executor = {
        let executor = ProvisionExecutor::new(
            args.provision_command.clone(),
            Duration::from_millis(args.provision_timeout_ms),
        );
        #[cfg(feature = "linode-provisioner")]
        let executor = executor.with_linode(args.linode_provision_config());
        executor
    };
    #[cfg(feature = "private-subnet-discovery")]
    let private_discovery_status = PrivateDiscoveryStatus::from_args(
        args.private_subnet_discovery,
        args.private_discovery_broadcast_port,
        args.private_discovery_mesh_port
            .unwrap_or_else(|| args.mesh_bind.port()),
    );
    #[cfg(not(feature = "private-subnet-discovery"))]
    let private_discovery_status = PrivateDiscoveryStatus::unavailable();
    let cache = LiveTsCache::new(
        args.stream_id,
        Duration::from_millis(args.part_ms),
        args.parts_per_segment,
        args.window_parts,
        args.slot_kb,
    )
    .await;

    let mesh_transport = MeshTransportConfigSnapshot {
        sync_interval_ms: args.mesh_sync_interval_ms,
        min_repair_symbols: args.mesh_repair_symbols,
        repair_ratio: args.mesh_repair_ratio,
        max_repair_symbols: args.mesh_max_repair_symbols,
        symbol_size: args.mesh_symbol_size,
    };
    let mut mesh_config =
        CacheMeshConfig::new(node_id.clone(), args.region.clone(), args.mesh_bind)
            .with_peers(mesh_peers);
    mesh_config.sync_interval = Duration::from_millis(args.mesh_sync_interval_ms);
    mesh_config.repair_symbols = args.mesh_repair_symbols;
    mesh_config.repair_ratio = args.mesh_repair_ratio;
    mesh_config.max_repair_symbols = args.mesh_max_repair_symbols;
    mesh_config.symbol_size = args.mesh_symbol_size;
    let mesh_handle = Arc::new(
        CacheMesh::new(Arc::clone(&cache.chunk_cache), mesh_config)
            .start()
            .await
            .context("failed to start cache mesh")?,
    );

    let control_packets = packetize_control_message(&MeshControlMessage {
        node_id: node_id.clone(),
        region: args.region.clone(),
        event: MeshControlEvent::NodeStarted {
            mesh_addr: mesh_handle.local_addr().to_string(),
        },
    })?;
    let control_echo = reassemble_unsigned_control_packets(&control_packets)?;
    info!(
        packets = control_packets.len(),
        event = ?control_echo.event,
        "mesh control message packetized"
    );

    let (ingest_shutdown_tx, ingest_shutdown_rx) = watch::channel(());

    #[cfg(feature = "private-subnet-discovery")]
    let private_subnet_discovery = if args.private_subnet_discovery {
        Some(
            start_private_subnet_discovery(
                args.private_discovery_broadcast_port,
                args.private_discovery_mesh_port
                    .unwrap_or_else(|| args.mesh_bind.port()),
                Arc::clone(&mesh_handle),
                ingest_shutdown_rx.clone(),
            )
            .await?,
        )
    } else {
        None
    };

    let fec_socket = UdpSocket::bind(args.fec_bind)
        .await
        .with_context(|| format!("failed to bind UDP-FEC ingest on {}", args.fec_bind))?;
    info!(bind = %fec_socket.local_addr()?, "UDP-FEC mesh byte ingest listening");
    let relay_secondary_socket = if let Some(bind) = args.relay_secondary_bind {
        let socket = UdpSocket::bind(bind)
            .await
            .with_context(|| format!("failed to bind secondary RelaySession ingest on {bind}"))?;
        info!(bind = %socket.local_addr()?, "secondary RelaySession repair ingest listening");
        Some(socket)
    } else {
        None
    };
    let relay_dispatch = configured_relay_udp_dispatch(&args, &node_id)?;
    let relay_forwarder = RelayDownstreamForwarder::bind(&args.relay_forwards).await?;
    cache.update_relay_forward(
        relay_forwarder
            .as_ref()
            .map_or_else(RelayForwardSnapshot::default, |forwarder| {
                forwarder.snapshot()
            }),
    );
    let relay_failover_listener_tasks = start_relay_failover_listeners(
        &args.relay_failover_listeners,
        relay_forwarder.as_ref(),
        TopologyGeneration::new(args.relay_topology_generation)?,
        SubscriptionId::new(args.relay_subscription_id)?,
        &cache,
        ingest_shutdown_rx.clone(),
    )
    .await?;
    let relay_failover_controller = RelayFailoverController::bind(&args).await?;
    cache.update_relay_failover_controller(
        relay_failover_controller
            .as_ref()
            .map_or_else(RelayFailoverControllerSnapshot::default, |controller| {
                controller.snapshot()
            }),
    );
    let (audio_epoch_tx, _) = broadcast::channel(AUDIO_EPOCH_BROADCAST_CAPACITY);
    let fec_ingest_task = tokio::spawn(run_udp_fec_ingest(
        fec_socket,
        Arc::clone(&cache),
        ingest_shutdown_rx.clone(),
        RelayIngestRuntime {
            dispatch: relay_dispatch,
            secondary_socket: relay_secondary_socket,
            forwarder: relay_forwarder,
            audio_epochs: Some(audio_epoch_tx.clone()),
            failover_controller: relay_failover_controller,
            failover_heartbeat: Duration::from_millis(args.relay_failover_heartbeat_ms),
        },
    ));

    let media_fec_socket = UdpSocket::bind(args.media_fec_bind)
        .await
        .with_context(|| {
            format!(
                "failed to bind media UDP-FEC ingest on {}",
                args.media_fec_bind
            )
        })?;
    info!(
        bind = %media_fec_socket.local_addr()?,
        "media UDP-FEC access-unit ingest listening"
    );
    let media_fec_ingest_task = tokio::spawn(run_udp_media_fec_ingest(
        media_fec_socket,
        Arc::clone(&cache),
        audio_epoch_tx.clone(),
        ingest_shutdown_rx.clone(),
    ));

    let (cert, key) = load_tls(&args)?;
    let telemetry_runtime = if let Some(bind) = args.telemetry_bind {
        Some(
            start_telemetry_feed(
                bind,
                args.telemetry_private_ipv4,
                cert.clone(),
                key.clone(),
                args.telemetry_interval_ms,
                Arc::clone(&cache),
                Arc::clone(&mesh_handle),
                node_profile.clone(),
                replication_policy.clone(),
                control_plane.clone(),
                lifecycle.clone(),
                control_dispatch.clone(),
                playback_base_url.clone(),
                edge_load.clone(),
                ingest_shutdown_rx.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    let router = AppRouter::new(
        Arc::clone(&cache),
        Arc::clone(&mesh_handle),
        audio_epoch_tx.clone(),
        mesh_transport,
        node_profile.clone(),
        replication_policy.clone(),
        control_plane.clone(),
        control_dispatch.clone(),
        telemetry_aggregator.clone(),
        demand_tracker.clone(),
        lifecycle.clone(),
        playback_base_url.clone(),
        edge_load.clone(),
        provision_executor.clone(),
        telemetry_peer_monitor.clone(),
        private_discovery_status,
    );
    let telemetry_collector_tasks = start_telemetry_collectors(
        telemetry_peers.clone(),
        args.telemetry_dns_name.clone(),
        cert.clone(),
        router.clone(),
        telemetry_peer_monitor.clone(),
        ingest_shutdown_rx.clone(),
    );
    let replication_planner_task = tokio::spawn(run_replication_planner(
        router.clone(),
        Duration::from_millis(args.replication_plan_interval_ms),
        ingest_shutdown_rx.clone(),
    ));
    let mut server_builder = H2H3Server::builder()
        .with_tls(cert, key)
        .with_port(args.http_port)
        .enable_h2(true)
        .enable_h3(args.edge_webtransport)
        .enable_webtransport(args.edge_webtransport)
        .enable_websocket(args.edge_websocket);
    if let Some(raw_tcp_port) = args.raw_tcp_port {
        server_builder = server_builder
            .enable_raw_tcp(true)
            .with_raw_tcp_port(raw_tcp_port)
            .with_raw_tcp_tls(args.raw_tcp_tls)
            .with_raw_tcp_handler(Box::new(router.clone()));
    }
    let server = server_builder.with_router(Box::new(router)).build()?;
    let handle = server.start().await?;
    let _ = handle.ready_rx.await;

    println!("node:    {} ({})", node_id, args.region);
    println!("mesh:    {}", mesh_handle.local_addr());
    println!("fec:     udp+fec://{}", args.fec_bind);
    println!("media:   udp+media-fec://{}", args.media_fec_bind);
    println!(
        "hls:     https://127.0.0.1:{}/live/{}/stream.m3u8",
        args.http_port, args.stream_id
    );
    println!(
        "hls-default: https://127.0.0.1:{}/live/stream.m3u8",
        args.http_port
    );
    println!(
        "needletail-mission-control: https://127.0.0.1:{}/mesh",
        args.http_port
    );
    if args.edge_websocket {
        println!(
            "edge-ws: wss://127.0.0.1:{}{}",
            args.http_port, MESH_WEBSOCKET_PATH
        );
    }
    if args.edge_webtransport {
        println!(
            "edge-webtransport: https://127.0.0.1:{} (HTTP/3 WebTransport)",
            args.http_port
        );
    }
    if let Some(runtime) = &telemetry_runtime {
        println!("telemetry: tcp+tls://{}", runtime.local_addr);
    }
    if !telemetry_peers.is_empty() {
        println!("telemetry-peers: {}", telemetry_peers.len());
    }
    if let Some(raw_tcp_port) = args.raw_tcp_port {
        println!(
            "raw-tcp: {}://0.0.0.0:{}",
            if args.raw_tcp_tls { "tls+tcp" } else { "tcp" },
            raw_tcp_port
        );
    }
    #[cfg(feature = "private-subnet-discovery")]
    if args.private_subnet_discovery {
        println!(
            "private-discovery: udp-broadcast://0.0.0.0:{} mesh-port={}",
            args.private_discovery_broadcast_port,
            args.private_discovery_mesh_port
                .unwrap_or_else(|| args.mesh_bind.port())
        );
    }
    println!("health:  https://127.0.0.1:{}/up", args.http_port);

    tokio::signal::ctrl_c().await?;
    mesh_handle.shutdown();
    let _ = ingest_shutdown_tx.send(());
    let _ = handle.shutdown_tx.send(());
    let _ = handle.finished_rx.await;
    let _ = fec_ingest_task.await;
    for task in relay_failover_listener_tasks {
        let _ = task.await;
    }
    let _ = media_fec_ingest_task.await;
    let _ = replication_planner_task.await;
    if let Some(runtime) = telemetry_runtime {
        let _ = runtime.shutdown_tx.send(());
        let _ = runtime.finished_rx.await;
        let _ = runtime.publisher_task.await;
    }
    #[cfg(feature = "private-subnet-discovery")]
    if let Some(runtime) = private_subnet_discovery {
        let _ = runtime.shutdown_tx.send(());
        let _ = runtime.task.await;
    }
    for task in telemetry_collector_tasks {
        let _ = task.await;
    }
    Ok(())
}

impl Args {
    fn normalized(mut self) -> Result<Self> {
        if self.part_ms == 0 {
            bail!("--part-ms must be at least 1");
        }
        self.parts_per_segment = self.parts_per_segment.max(1);
        self.window_parts = self.window_parts.max(self.parts_per_segment * 3).max(6);
        self.slot_kb = self.slot_kb.max(64);
        self.storage_bytes = self.storage_bytes.max((self.slot_kb as u64) * 1024);
        self.egress_capacity_bps = self.egress_capacity_bps.max(1);
        self.mesh_sync_interval_ms = self.mesh_sync_interval_ms.max(1);
        self.mesh_symbol_size = self.mesh_symbol_size.max(1);
        self.mesh_max_repair_symbols = self.mesh_max_repair_symbols.max(self.mesh_repair_symbols);
        if !self.mesh_repair_ratio.is_finite() || self.mesh_repair_ratio < 0.0 {
            bail!("--mesh-repair-ratio must be a finite non-negative number");
        }
        if (self.relay_primary_peer.is_some() || self.relay_secondary_peer.is_some())
            && !self.relay_controlled_local
        {
            bail!("--relay-primary-peer/--relay-secondary-peer require --relay-controlled-local");
        }
        if self.relay_primary_bind.is_some() && self.relay_primary_peer.is_none() {
            bail!("--relay-primary-bind requires --relay-primary-peer");
        }
        if self.relay_secondary_bind.is_some() && self.relay_secondary_peer.is_none() {
            bail!("--relay-secondary-bind requires --relay-secondary-peer");
        }
        if self.relay_primary_peer.is_some() && self.relay_primary_bind.is_none() {
            self.relay_primary_bind = Some(self.fec_bind);
        }
        if self
            .relay_primary_bind
            .is_some_and(|bind| bind != self.fec_bind)
        {
            bail!("--relay-primary-bind currently shares the --fec-bind socket");
        }
        if self.relay_secondary_peer.is_some() && self.relay_secondary_bind.is_none() {
            bail!("--relay-secondary-peer requires --relay-secondary-bind");
        }
        if self.relay_secondary_promoted && self.relay_secondary_peer.is_none() {
            bail!("--relay-secondary-promoted requires --relay-secondary-peer");
        }
        if self.relay_primary_promoted && self.relay_primary_peer.is_none() {
            bail!("--relay-primary-promoted requires --relay-primary-peer");
        }
        if self
            .relay_secondary_bind
            .is_some_and(|bind| bind == self.fec_bind)
        {
            bail!("--relay-secondary-bind must differ from --fec-bind");
        }
        if self.relay_controlled_local
            && self.relay_primary_peer.is_none()
            && self.relay_secondary_peer.is_none()
        {
            bail!("--relay-controlled-local requires at least one configured relay peer");
        }
        if self.relay_primary_peer.is_some() && self.relay_primary_peer == self.relay_secondary_peer
        {
            bail!("primary and secondary relay peer addresses must differ");
        }
        if self.relay_max_downstream_children == 0 {
            bail!("--relay-max-downstream-children must be positive");
        }
        if self.relay_forwards.len() > self.relay_max_downstream_children {
            bail!(
                "{} --relay-forward subscriptions exceed the configured child limit {}",
                self.relay_forwards.len(),
                self.relay_max_downstream_children
            );
        }
        let mut relay_forward_binds = HashSet::with_capacity(self.relay_forwards.len());
        let mut relay_forward_targets = HashSet::with_capacity(self.relay_forwards.len());
        for forward in &self.relay_forwards {
            if forward.bind == forward.target {
                bail!(
                    "RelaySession forward bind and target require distinct sockets; both resolve to {}",
                    forward.bind
                );
            }
            if forward.bind == self.fec_bind || Some(forward.bind) == self.relay_secondary_bind {
                bail!(
                    "RelaySession forward bind {} conflicts with a RelaySession receive socket",
                    forward.bind
                );
            }
            if !relay_forward_binds.insert(forward.bind) {
                bail!("duplicate RelaySession forward bind {}", forward.bind);
            }
            if !relay_forward_targets.insert(forward.target) {
                bail!("duplicate RelaySession forward target {}", forward.target);
            }
        }
        if self.relay_failover_controller.is_some() {
            if !self.relay_controlled_local {
                bail!("--relay-failover-controller requires --relay-controlled-local");
            }
            if self.relay_secondary_peer.is_none() || !self.relay_secondary_promoted {
                bail!(
                    "--relay-failover-controller requires a seeded --relay-secondary-peer with --relay-secondary-promoted"
                );
            }
        }
        if !self.relay_failover_listeners.is_empty() && !self.relay_controlled_local {
            bail!("--relay-failover-listener requires --relay-controlled-local");
        }
        let mut control_binds = HashSet::with_capacity(self.relay_failover_listeners.len() + 1);
        let mut controlled_targets = HashSet::with_capacity(self.relay_failover_listeners.len());
        for listener in &self.relay_failover_listeners {
            let Some(forward) = self
                .relay_forwards
                .iter()
                .find(|forward| forward.target == listener.forward_target)
            else {
                bail!(
                    "failover listener target {} is not a compiled --relay-forward target",
                    listener.forward_target
                );
            };
            if forward.role != RelayForwardRole::Repair {
                bail!(
                    "failover listener target {} must select a repair-only warm forward",
                    listener.forward_target
                );
            }
            if listener.bind == listener.peer || listener.bind.ip().is_unspecified() {
                bail!(
                    "failover listener requires a distinct, explicit local bind and controller peer"
                );
            }
            if !control_binds.insert(listener.bind) {
                bail!("duplicate failover control bind {}", listener.bind);
            }
            if !controlled_targets.insert(listener.forward_target) {
                bail!(
                    "duplicate failover control target {}",
                    listener.forward_target
                );
            }
        }
        if let Some(controller) = self.relay_failover_controller {
            if controller.bind == controller.target || controller.bind.ip().is_unspecified() {
                bail!("failover controller requires a distinct, explicit local bind and target");
            }
            if !control_binds.insert(controller.bind) {
                bail!("duplicate failover control bind {}", controller.bind);
            }
        }
        if self.relay_primary_silence_ms == 0
            || self.relay_primary_recovery_ms < self.relay_primary_silence_ms
            || self.relay_secondary_warm_ms < self.relay_primary_silence_ms
        {
            bail!(
                "relay failover windows require positive silence and recovery/warm windows at least as large as silence"
            );
        }
        if self.relay_failover_heartbeat_ms == 0
            || self.relay_failover_lease_ms < self.relay_failover_heartbeat_ms.saturating_mul(3)
            || self.relay_failover_lease_ms > 60_000
        {
            bail!(
                "relay failover lease must be at most 60000ms and at least three heartbeat intervals"
            );
        }
        TopologyGeneration::new(self.relay_topology_generation)
            .context("--relay-topology-generation must be positive")?;
        SubscriptionId::new(self.relay_subscription_id)
            .context("--relay-subscription-id must be positive")?;
        self.telemetry_interval_ms = self.telemetry_interval_ms.max(100);
        self.replication_plan_interval_ms = self.replication_plan_interval_ms.max(100);
        self.provision_timeout_ms = self.provision_timeout_ms.max(100);
        #[cfg(feature = "linode-provisioner")]
        self.validate_linode_provision()?;
        Ok(self)
    }

    #[cfg(feature = "linode-provisioner")]
    fn validate_linode_provision(&self) -> Result<()> {
        if !self.linode_provision {
            return Ok(());
        }
        let mut missing = Vec::new();
        if self.linode_image_id.is_none() {
            missing.push("--linode-image-id");
        }
        if self.linode_instance_type.is_none() {
            missing.push("--linode-instance-type");
        }
        if self.linode_domain_id.is_none() {
            missing.push("--linode-domain-id");
        }
        if missing.is_empty() {
            Ok(())
        } else {
            bail!("--linode-provision requires {}", missing.join(", "))
        }
    }

    #[cfg(feature = "linode-provisioner")]
    fn linode_provision_config(&self) -> Option<LinodeProvisionConfig> {
        if !self.linode_provision {
            return None;
        }
        Some(LinodeProvisionConfig {
            token_env: self.linode_token_env.clone(),
            pub_key_env: self.linode_pub_key_env.clone(),
            image_id: self.linode_image_id.clone().unwrap_or_default(),
            instance_type: self.linode_instance_type.clone().unwrap_or_default(),
            domain_id: self.linode_domain_id.unwrap_or_default(),
            vlan_tag: self.linode_vlan_tag.clone(),
            region_map: self.linode_region_maps.iter().cloned().collect(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RelayForwardEndpoint {
    bind: SocketAddr,
    target: SocketAddr,
    role: RelayForwardRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RelayFailoverListenerEndpoint {
    bind: SocketAddr,
    peer: SocketAddr,
    forward_target: SocketAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RelayFailoverControllerEndpoint {
    bind: SocketAddr,
    target: SocketAddr,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
enum RelayForwardRole {
    #[default]
    All,
    Source,
    Repair,
}

impl RelayForwardRole {
    const fn permits(self, role: MediaDatagramRole) -> bool {
        match self {
            Self::All => true,
            Self::Source => matches!(role, MediaDatagramRole::Source),
            Self::Repair => matches!(role, MediaDatagramRole::Repair),
        }
    }
}

fn parse_relay_forward_endpoint(value: &str) -> std::result::Result<RelayForwardEndpoint, String> {
    let (bind, target_and_role) = value
        .split_once('=')
        .ok_or_else(|| "expected BIND=TARGET,ROLE".to_owned())?;
    let (target, role) = match target_and_role.rsplit_once(',') {
        Some((target, "all")) => (target, RelayForwardRole::All),
        Some((target, "source")) => (target, RelayForwardRole::Source),
        Some((target, "repair")) => (target, RelayForwardRole::Repair),
        Some((_target, role)) => {
            return Err(format!(
                "invalid relay forward role {role}; expected source, repair, or all"
            ));
        }
        None => (target_and_role, RelayForwardRole::All),
    };
    let bind = bind
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid relay forward bind {bind}: {error}"))?;
    let target = target
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid relay forward target {target}: {error}"))?;
    Ok(RelayForwardEndpoint { bind, target, role })
}

fn parse_relay_failover_listener_endpoint(
    value: &str,
) -> std::result::Result<RelayFailoverListenerEndpoint, String> {
    let (bind, peer_and_target) = value
        .split_once('=')
        .ok_or_else(|| "expected BIND=CONTROLLER_PEER,FORWARD_TARGET".to_owned())?;
    let (peer, forward_target) = peer_and_target
        .split_once(',')
        .ok_or_else(|| "expected BIND=CONTROLLER_PEER,FORWARD_TARGET".to_owned())?;
    Ok(RelayFailoverListenerEndpoint {
        bind: bind
            .parse()
            .map_err(|error| format!("invalid failover listener bind {bind}: {error}"))?,
        peer: peer
            .parse()
            .map_err(|error| format!("invalid failover controller peer {peer}: {error}"))?,
        forward_target: forward_target.parse().map_err(|error| {
            format!("invalid failover forward target {forward_target}: {error}")
        })?,
    })
}

fn parse_relay_failover_controller_endpoint(
    value: &str,
) -> std::result::Result<RelayFailoverControllerEndpoint, String> {
    let (bind, target) = value
        .split_once('=')
        .ok_or_else(|| "expected BIND=TARGET".to_owned())?;
    Ok(RelayFailoverControllerEndpoint {
        bind: bind
            .parse()
            .map_err(|error| format!("invalid failover controller bind {bind}: {error}"))?,
        target: target
            .parse()
            .map_err(|error| format!("invalid failover controller target {target}: {error}"))?,
    })
}

#[cfg(feature = "linode-provisioner")]
fn parse_key_value(value: &str) -> std::result::Result<(String, String), String> {
    let (key, val) = value
        .split_once('=')
        .ok_or_else(|| "expected KEY=VALUE".to_string())?;
    let key = key.trim();
    let val = val.trim();
    if key.is_empty() || val.is_empty() {
        return Err("expected non-empty KEY=VALUE".to_string());
    }
    Ok((key.to_string(), val.to_string()))
}

async fn resolve_socket_addr_args(kind: &str, values: &[String]) -> Result<Vec<SocketAddr>> {
    let mut resolved = Vec::new();
    for value in values {
        let addrs = tokio::net::lookup_host(value)
            .await
            .with_context(|| format!("failed to resolve --{kind} {value}"))?
            .collect::<Vec<_>>();
        if addrs.is_empty() {
            bail!("--{kind} {value} resolved no socket addresses");
        }
        resolved.extend(addrs);
    }
    resolved.sort();
    resolved.dedup();
    Ok(resolved)
}

fn load_tls(args: &Args) -> Result<(String, String)> {
    match (&args.cert, &args.key) {
        (Some(cert), Some(key)) => load_tls_base64_from_paths(cert, key).with_context(|| {
            format!(
                "failed to load TLS files {} and {}",
                cert.display(),
                key.display()
            )
        }),
        (None, None) => {
            load_default_tls_base64().context("failed to load default TLS files from av-service")
        }
        _ => bail!("--cert and --key must be provided together"),
    }
}

#[cfg(feature = "private-subnet-discovery")]
struct PrivateSubnetDiscoveryRuntime {
    shutdown_tx: watch::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "private-subnet-discovery")]
async fn start_private_subnet_discovery(
    broadcast_port: u16,
    mesh_port: u16,
    mesh: Arc<CacheMeshHandle>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<PrivateSubnetDiscoveryRuntime> {
    let (up_rx, _finished_rx, discovery_shutdown_tx, nodes) =
        discovery::vlan::discover(broadcast_port)
            .await
            .map_err(|err| anyhow!("private subnet discovery failed to start: {err}"))?;
    let _ = up_rx.await;

    let mut node_rx = nodes.rx();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => return,
                discovered = node_rx.recv() => {
                    match discovered {
                        Ok(node) if node.is_self() => {}
                        Ok(node) => {
                            let peer = node.addr(mesh_port);
                            if mesh.add_peer(peer).await {
                                info!(
                                    peer = %peer,
                                    tag = ?node.tag(),
                                    seq = ?node.seq(),
                                    "private subnet discovery added cache mesh peer"
                                );
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            debug!(skipped, "private subnet discovery receiver lagged");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        }
    });

    Ok(PrivateSubnetDiscoveryRuntime {
        shutdown_tx: discovery_shutdown_tx,
        task,
    })
}

fn configured_relay_udp_dispatch(args: &Args, node_id: &str) -> Result<RelayUdpDispatch> {
    let receiver = RelayObjectReceiver::new(RelayObjectReceiverConfig::default())
        .context("invalid RelaySession receive limits")?;
    let mut dispatch = RelayUdpDispatch::new(receiver);
    if !args.relay_controlled_local {
        return Ok(dispatch);
    }

    let local = NodeId::new(node_id).context("invalid local RelaySession node identity")?;
    let generation = TopologyGeneration::new(args.relay_topology_generation)
        .context("invalid RelaySession topology generation")?;
    let subscription_id = SubscriptionId::new(args.relay_subscription_id)
        .context("invalid RelaySession subscription id")?;
    for (session_id, peer, peer_node_id, path) in [
        (
            1,
            args.relay_primary_peer,
            args.relay_primary_id.as_str(),
            if args.relay_primary_promoted {
                ParentPath::PromotedSecondary
            } else {
                ParentPath::Primary
            },
        ),
        (
            2,
            args.relay_secondary_peer,
            args.relay_secondary_id.as_str(),
            if args.relay_secondary_promoted {
                ParentPath::PromotedSecondary
            } else {
                ParentPath::Secondary
            },
        ),
    ] {
        let Some(peer) = peer else {
            continue;
        };
        let session = ControlledRelayParentSession::new(
            session_id,
            CarrierIdentity {
                local: local.clone(),
                peer: NodeId::new(peer_node_id).with_context(|| {
                    format!("invalid relay parent node identity {peer_node_id}")
                })?,
                kind: CarrierKind::PrivateUdp,
                trust_mode: TrustMode::ControlledPrivateNetwork,
            },
            generation,
            subscription_id,
            path,
        )?;
        dispatch.bind_controlled_peer(peer, session)?;
    }
    Ok(dispatch)
}

#[cfg(test)]
fn empty_relay_udp_dispatch() -> RelayUdpDispatch {
    RelayUdpDispatch::new(
        RelayObjectReceiver::new(RelayObjectReceiverConfig::default())
            .expect("default RelaySession receive limits are valid"),
    )
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RelayForwardSnapshot {
    downstream_children: u64,
    source_datagrams: u64,
    repair_datagrams: u64,
    bytes: u64,
    errors: u64,
    filtered_datagrams: u64,
    warm_source_buffered_datagrams: u64,
    warm_source_buffered_bytes: u64,
    warm_source_replayed_datagrams: u64,
    warm_source_replayed_bytes: u64,
    warm_source_expired_datagrams: u64,
    warm_source_retired_datagrams: u64,
    warm_source_evicted_datagrams: u64,
    duration_count: u64,
    duration_sum_us: u64,
    duration_max_us: u64,
    duration_buckets: [u64; EDGE_RESPONSE_DURATION_BUCKETS_US.len()],
    failover_listeners: u64,
    failover_promoted_children: u64,
    failover_commands_received: u64,
    failover_commands_rejected: u64,
    failover_lease_expirations: u64,
    failover_promotions_applied: u64,
    failover_demotions_applied: u64,
    failover_last_transition_unix_ms: u64,
}

#[derive(Debug)]
struct WarmSourceDatagram {
    object_key: media_object::ObjectKey,
    expires_at_us: u64,
    wire: Bytes,
}

#[derive(Debug, Default)]
struct WarmSourceReplayBuffer {
    datagrams: VecDeque<WarmSourceDatagram>,
    object_order: VecDeque<media_object::ObjectKey>,
    bytes: usize,
}

#[derive(Debug, Default)]
struct WarmSourceBufferMutation {
    added_datagrams: usize,
    added_bytes: usize,
    expired_datagrams: usize,
    expired_bytes: usize,
    retired_datagrams: usize,
    retired_bytes: usize,
    evicted_datagrams: usize,
    evicted_bytes: usize,
}

#[derive(Debug, Default)]
struct WarmSourceReplayBatch {
    datagrams: Vec<Bytes>,
    bytes: usize,
    expired_datagrams: usize,
    expired_bytes: usize,
}

impl WarmSourceReplayBuffer {
    fn push(
        &mut self,
        object_key: &media_object::ObjectKey,
        expires_at_us: u64,
        wire: &[u8],
        now_us: u64,
    ) -> WarmSourceBufferMutation {
        let mut mutation = WarmSourceBufferMutation::default();
        self.remove_expired(now_us, &mut mutation);
        if expires_at_us <= now_us || wire.len() > RELAY_WARM_SOURCE_REPLAY_MAX_BYTES_PER_CHILD {
            return mutation;
        }

        if !self.object_order.contains(object_key) {
            self.object_order.push_back(object_key.clone());
        }
        self.bytes = self.bytes.saturating_add(wire.len());
        self.datagrams.push_back(WarmSourceDatagram {
            object_key: object_key.clone(),
            expires_at_us,
            wire: Bytes::copy_from_slice(wire),
        });
        mutation.added_datagrams = 1;
        mutation.added_bytes = wire.len();

        while self.object_order.len() > RELAY_WARM_SOURCE_REPLAY_MAX_OBJECTS_PER_CHILD {
            if let Some(oldest) = self.object_order.pop_front() {
                self.remove_object(&oldest, &mut mutation);
            }
        }
        while self.datagrams.len() > RELAY_WARM_SOURCE_REPLAY_MAX_DATAGRAMS_PER_CHILD
            || self.bytes > RELAY_WARM_SOURCE_REPLAY_MAX_BYTES_PER_CHILD
        {
            let Some(evicted) = self.datagrams.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(evicted.wire.len());
            mutation.evicted_datagrams = mutation.evicted_datagrams.saturating_add(1);
            mutation.evicted_bytes = mutation.evicted_bytes.saturating_add(evicted.wire.len());
            if !self
                .datagrams
                .iter()
                .any(|entry| entry.object_key == evicted.object_key)
            {
                self.object_order.retain(|key| key != &evicted.object_key);
            }
        }
        mutation
    }

    fn take_live(&mut self, now_us: u64) -> WarmSourceReplayBatch {
        let mut batch = WarmSourceReplayBatch::default();
        for datagram in self.datagrams.drain(..) {
            if datagram.expires_at_us <= now_us {
                batch.expired_datagrams = batch.expired_datagrams.saturating_add(1);
                batch.expired_bytes = batch.expired_bytes.saturating_add(datagram.wire.len());
            } else {
                batch.bytes = batch.bytes.saturating_add(datagram.wire.len());
                batch.datagrams.push(datagram.wire);
            }
        }
        self.object_order.clear();
        self.bytes = 0;
        batch
    }

    fn remove_expired(&mut self, now_us: u64, mutation: &mut WarmSourceBufferMutation) {
        let mut retained = VecDeque::with_capacity(self.datagrams.len());
        while let Some(datagram) = self.datagrams.pop_front() {
            if datagram.expires_at_us <= now_us {
                self.bytes = self.bytes.saturating_sub(datagram.wire.len());
                mutation.expired_datagrams = mutation.expired_datagrams.saturating_add(1);
                mutation.expired_bytes = mutation.expired_bytes.saturating_add(datagram.wire.len());
            } else {
                retained.push_back(datagram);
            }
        }
        self.datagrams = retained;
        self.object_order.retain(|key| {
            self.datagrams
                .iter()
                .any(|datagram| &datagram.object_key == key)
        });
    }

    fn remove_object(
        &mut self,
        object_key: &media_object::ObjectKey,
        mutation: &mut WarmSourceBufferMutation,
    ) {
        let mut retained = VecDeque::with_capacity(self.datagrams.len());
        while let Some(datagram) = self.datagrams.pop_front() {
            if &datagram.object_key == object_key {
                self.bytes = self.bytes.saturating_sub(datagram.wire.len());
                mutation.retired_datagrams = mutation.retired_datagrams.saturating_add(1);
                mutation.retired_bytes = mutation.retired_bytes.saturating_add(datagram.wire.len());
            } else {
                retained.push_back(datagram);
            }
        }
        self.datagrams = retained;
    }
}

struct RelayForwardPath {
    socket: UdpSocket,
    target: SocketAddr,
    role: RelayForwardRole,
    promoted: AtomicBool,
    warm_sources: StdMutex<WarmSourceReplayBuffer>,
}

struct RelayDownstreamForwarder {
    paths: Vec<RelayForwardPath>,
    source_datagrams: AtomicU64,
    repair_datagrams: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    filtered_datagrams: AtomicU64,
    warm_source_buffered_datagrams: AtomicU64,
    warm_source_buffered_bytes: AtomicU64,
    warm_source_replayed_datagrams: AtomicU64,
    warm_source_replayed_bytes: AtomicU64,
    warm_source_expired_datagrams: AtomicU64,
    warm_source_retired_datagrams: AtomicU64,
    warm_source_evicted_datagrams: AtomicU64,
    duration_count: AtomicU64,
    duration_sum_us: AtomicU64,
    duration_max_us: AtomicU64,
    duration_buckets: [AtomicU64; EDGE_RESPONSE_DURATION_BUCKETS_US.len()],
    failover_listeners: AtomicU64,
    failover_commands_received: AtomicU64,
    failover_commands_rejected: AtomicU64,
    failover_lease_expirations: AtomicU64,
    failover_promotions_applied: AtomicU64,
    failover_demotions_applied: AtomicU64,
    failover_last_transition_unix_ms: AtomicU64,
}

impl RelayDownstreamForwarder {
    async fn bind(endpoints: &[RelayForwardEndpoint]) -> Result<Option<Arc<Self>>> {
        if endpoints.is_empty() {
            return Ok(None);
        }
        let mut paths = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            let socket = UdpSocket::bind(endpoint.bind).await.with_context(|| {
                format!(
                    "failed to bind RelaySession downstream source {} for {}",
                    endpoint.bind, endpoint.target
                )
            })?;
            info!(
                bind = %socket.local_addr()?,
                target = %endpoint.target,
                role = ?endpoint.role,
                "RelaySession subscribed downstream forwarding path ready"
            );
            paths.push(RelayForwardPath {
                socket,
                target: endpoint.target,
                role: endpoint.role,
                promoted: AtomicBool::new(false),
                warm_sources: StdMutex::new(WarmSourceReplayBuffer::default()),
            });
        }
        Ok(Some(Arc::new(Self {
            paths,
            source_datagrams: AtomicU64::new(0),
            repair_datagrams: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            filtered_datagrams: AtomicU64::new(0),
            warm_source_buffered_datagrams: AtomicU64::new(0),
            warm_source_buffered_bytes: AtomicU64::new(0),
            warm_source_replayed_datagrams: AtomicU64::new(0),
            warm_source_replayed_bytes: AtomicU64::new(0),
            warm_source_expired_datagrams: AtomicU64::new(0),
            warm_source_retired_datagrams: AtomicU64::new(0),
            warm_source_evicted_datagrams: AtomicU64::new(0),
            duration_count: AtomicU64::new(0),
            duration_sum_us: AtomicU64::new(0),
            duration_max_us: AtomicU64::new(0),
            duration_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            failover_listeners: AtomicU64::new(0),
            failover_commands_received: AtomicU64::new(0),
            failover_commands_rejected: AtomicU64::new(0),
            failover_lease_expirations: AtomicU64::new(0),
            failover_promotions_applied: AtomicU64::new(0),
            failover_demotions_applied: AtomicU64::new(0),
            failover_last_transition_unix_ms: AtomicU64::new(0),
        })))
    }

    async fn forward(
        &self,
        datagram: &[u8],
        role: MediaDatagramRole,
        object_key: Option<&media_object::ObjectKey>,
        expires_at_us: Option<u64>,
    ) {
        for path in &self.paths {
            let promoted = path.promoted.load(Ordering::Relaxed);
            let failover_source = promoted
                && path.role == RelayForwardRole::Repair
                && matches!(role, MediaDatagramRole::Source);
            if !(path.role.permits(role) || failover_source) {
                if path.role == RelayForwardRole::Repair
                    && matches!(role, MediaDatagramRole::Source)
                {
                    if let (Some(object_key), Some(expires_at_us)) = (object_key, expires_at_us) {
                        self.buffer_warm_source(
                            path,
                            object_key,
                            expires_at_us,
                            datagram,
                            now_unix_us(),
                        );
                    }
                }
                self.filtered_datagrams.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let started = Instant::now();
            match path.socket.send_to(datagram, path.target).await {
                Ok(sent) if sent == datagram.len() => {
                    self.record_forward_success(role, sent, started.elapsed());
                }
                Ok(sent) => {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target = %path.target,
                        expected_bytes = datagram.len(),
                        sent_bytes = sent,
                        "RelaySession downstream forwarding sent a partial datagram"
                    );
                }
                Err(error) => {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target = %path.target,
                        error = %error,
                        "RelaySession downstream forwarding failed"
                    );
                }
            }
        }
    }

    fn buffer_warm_source(
        &self,
        path: &RelayForwardPath,
        object_key: &media_object::ObjectKey,
        expires_at_us: u64,
        datagram: &[u8],
        now_us: u64,
    ) {
        let mutation = path
            .warm_sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(object_key, expires_at_us, datagram, now_us);
        self.warm_source_buffered_datagrams
            .fetch_add(mutation.added_datagrams as u64, Ordering::Relaxed);
        self.warm_source_buffered_bytes
            .fetch_add(mutation.added_bytes as u64, Ordering::Relaxed);
        atomic_saturating_sub(
            &self.warm_source_buffered_datagrams,
            mutation
                .expired_datagrams
                .saturating_add(mutation.retired_datagrams)
                .saturating_add(mutation.evicted_datagrams) as u64,
        );
        atomic_saturating_sub(
            &self.warm_source_buffered_bytes,
            mutation
                .expired_bytes
                .saturating_add(mutation.retired_bytes)
                .saturating_add(mutation.evicted_bytes) as u64,
        );
        self.warm_source_expired_datagrams
            .fetch_add(mutation.expired_datagrams as u64, Ordering::Relaxed);
        self.warm_source_retired_datagrams
            .fetch_add(mutation.retired_datagrams as u64, Ordering::Relaxed);
        self.warm_source_evicted_datagrams
            .fetch_add(mutation.evicted_datagrams as u64, Ordering::Relaxed);
    }

    async fn replay_warm_sources(&self, target: SocketAddr) {
        let Some(path) = self.paths.iter().find(|path| path.target == target) else {
            return;
        };
        let batch = path
            .warm_sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_live(now_unix_us());
        let removed_datagrams = batch
            .datagrams
            .len()
            .saturating_add(batch.expired_datagrams);
        let removed_bytes = batch.bytes.saturating_add(batch.expired_bytes);
        atomic_saturating_sub(
            &self.warm_source_buffered_datagrams,
            removed_datagrams as u64,
        );
        atomic_saturating_sub(&self.warm_source_buffered_bytes, removed_bytes as u64);
        self.warm_source_expired_datagrams
            .fetch_add(batch.expired_datagrams as u64, Ordering::Relaxed);

        for datagram in batch.datagrams {
            let started = Instant::now();
            match path.socket.send_to(&datagram, path.target).await {
                Ok(sent) if sent == datagram.len() => {
                    self.record_forward_success(MediaDatagramRole::Source, sent, started.elapsed());
                    self.warm_source_replayed_datagrams
                        .fetch_add(1, Ordering::Relaxed);
                    self.warm_source_replayed_bytes
                        .fetch_add(sent as u64, Ordering::Relaxed);
                }
                Ok(sent) => {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        %target,
                        expected_bytes = datagram.len(),
                        sent_bytes = sent,
                        "RelaySession warm-source replay sent a partial datagram"
                    );
                }
                Err(error) => {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        %target,
                        error = %error,
                        "RelaySession warm-source replay failed"
                    );
                }
            }
        }
    }

    fn record_forward_success(&self, role: MediaDatagramRole, sent: usize, duration: Duration) {
        match role {
            MediaDatagramRole::Source => {
                self.source_datagrams.fetch_add(1, Ordering::Relaxed);
            }
            MediaDatagramRole::Repair => {
                self.repair_datagrams.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.bytes.fetch_add(sent as u64, Ordering::Relaxed);
        self.observe_duration(duration);
    }

    fn register_failover_listener(&self) {
        self.failover_listeners.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failover_command_rejected(&self) {
        self.failover_commands_rejected
            .fetch_add(1, Ordering::Relaxed);
    }

    fn apply_failover_mode(
        &self,
        target: SocketAddr,
        mode: FailoverForwardMode,
        lease_expired: bool,
    ) -> Option<bool> {
        let Some(path) = self.paths.iter().find(|path| path.target == target) else {
            self.record_failover_command_rejected();
            return None;
        };
        if path.role != RelayForwardRole::Repair {
            self.record_failover_command_rejected();
            return None;
        }
        if !lease_expired {
            self.failover_commands_received
                .fetch_add(1, Ordering::Relaxed);
        }
        let promoted = mode == FailoverForwardMode::SourceAndRepair;
        let previous = path.promoted.swap(promoted, Ordering::Relaxed);
        if previous != promoted {
            if promoted {
                self.failover_promotions_applied
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.failover_demotions_applied
                    .fetch_add(1, Ordering::Relaxed);
            }
            if lease_expired {
                self.failover_lease_expirations
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.failover_last_transition_unix_ms
                .store(now_unix_ms(), Ordering::Relaxed);
            info!(
                %target,
                ?mode,
                lease_expired,
                "RelaySession warm-secondary forwarding mode changed"
            );
        }
        Some(!previous && promoted)
    }

    fn observe_duration(&self, duration: Duration) {
        let duration_us = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.duration_count.fetch_add(1, Ordering::Relaxed);
        self.duration_sum_us
            .fetch_add(duration_us, Ordering::Relaxed);
        self.duration_max_us
            .fetch_max(duration_us, Ordering::Relaxed);
        for (index, upper_bound_us) in EDGE_RESPONSE_DURATION_BUCKETS_US.iter().enumerate() {
            if duration_us <= *upper_bound_us {
                self.duration_buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> RelayForwardSnapshot {
        RelayForwardSnapshot {
            downstream_children: self.paths.len() as u64,
            source_datagrams: self.source_datagrams.load(Ordering::Relaxed),
            repair_datagrams: self.repair_datagrams.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            filtered_datagrams: self.filtered_datagrams.load(Ordering::Relaxed),
            warm_source_buffered_datagrams: self
                .warm_source_buffered_datagrams
                .load(Ordering::Relaxed),
            warm_source_buffered_bytes: self.warm_source_buffered_bytes.load(Ordering::Relaxed),
            warm_source_replayed_datagrams: self
                .warm_source_replayed_datagrams
                .load(Ordering::Relaxed),
            warm_source_replayed_bytes: self.warm_source_replayed_bytes.load(Ordering::Relaxed),
            warm_source_expired_datagrams: self
                .warm_source_expired_datagrams
                .load(Ordering::Relaxed),
            warm_source_retired_datagrams: self
                .warm_source_retired_datagrams
                .load(Ordering::Relaxed),
            warm_source_evicted_datagrams: self
                .warm_source_evicted_datagrams
                .load(Ordering::Relaxed),
            duration_count: self.duration_count.load(Ordering::Relaxed),
            duration_sum_us: self.duration_sum_us.load(Ordering::Relaxed),
            duration_max_us: self.duration_max_us.load(Ordering::Relaxed),
            duration_buckets: std::array::from_fn(|index| {
                self.duration_buckets[index].load(Ordering::Relaxed)
            }),
            failover_listeners: self.failover_listeners.load(Ordering::Relaxed),
            failover_promoted_children: self
                .paths
                .iter()
                .filter(|path| path.promoted.load(Ordering::Relaxed))
                .count() as u64,
            failover_commands_received: self.failover_commands_received.load(Ordering::Relaxed),
            failover_commands_rejected: self.failover_commands_rejected.load(Ordering::Relaxed),
            failover_lease_expirations: self.failover_lease_expirations.load(Ordering::Relaxed),
            failover_promotions_applied: self.failover_promotions_applied.load(Ordering::Relaxed),
            failover_demotions_applied: self.failover_demotions_applied.load(Ordering::Relaxed),
            failover_last_transition_unix_ms: self
                .failover_last_transition_unix_ms
                .load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RelayFailoverControllerState {
    #[default]
    Disabled,
    Arming,
    Healthy,
    Promoted,
    Recovering,
    SecondaryUnavailable,
}

impl RelayFailoverControllerState {
    const ALL: [Self; 6] = [
        Self::Disabled,
        Self::Arming,
        Self::Healthy,
        Self::Promoted,
        Self::Recovering,
        Self::SecondaryUnavailable,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Arming => "arming",
            Self::Healthy => "healthy",
            Self::Promoted => "promoted",
            Self::Recovering => "recovering",
            Self::SecondaryUnavailable => "secondary_unavailable",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RelayFailoverControllerSnapshot {
    state: RelayFailoverControllerState,
    enabled: u64,
    commands_sent: u64,
    command_send_errors: u64,
    promotions: u64,
    demotions: u64,
    secondary_unavailable_events: u64,
    primary_source_age_ms: u64,
    secondary_repair_age_ms: u64,
    last_detection_us: u64,
    last_promotion_to_source_us: u64,
    last_media_gap_us: u64,
    max_media_gap_us: u64,
    last_transition_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayIngressParentPath {
    Primary,
    Secondary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RelayDatagramObservation {
    role: MediaDatagramRole,
    decoded: bool,
}

struct RelayFailoverController {
    socket: UdpSocket,
    target: SocketAddr,
    generation: TopologyGeneration,
    subscription_id: SubscriptionId,
    silence: Duration,
    recovery: Duration,
    secondary_warm: Duration,
    lease_duration_us: u64,
    state: RelayFailoverControllerState,
    desired_mode: FailoverForwardMode,
    transition_id: u64,
    last_primary_source: Option<Instant>,
    last_secondary_repair: Option<Instant>,
    recovered_since: Option<Instant>,
    last_decoded: Option<Instant>,
    promotion_gap_base: Option<Instant>,
    promotion_sent_at: Option<Instant>,
    awaiting_secondary_source: bool,
    awaiting_post_promotion_object: bool,
    snapshot: RelayFailoverControllerSnapshot,
}

impl RelayFailoverController {
    async fn bind(args: &Args) -> Result<Option<Self>> {
        let Some(endpoint) = args.relay_failover_controller else {
            return Ok(None);
        };
        let socket = UdpSocket::bind(endpoint.bind).await.with_context(|| {
            format!(
                "failed to bind RelaySession failover controller on {}",
                endpoint.bind
            )
        })?;
        info!(
            bind = %socket.local_addr()?,
            target = %endpoint.target,
            "RelaySession warm-secondary failover controller ready"
        );
        let transition_id = now_unix_us().max(1);
        Ok(Some(Self {
            socket,
            target: endpoint.target,
            generation: TopologyGeneration::new(args.relay_topology_generation)?,
            subscription_id: SubscriptionId::new(args.relay_subscription_id)?,
            silence: Duration::from_millis(args.relay_primary_silence_ms),
            recovery: Duration::from_millis(args.relay_primary_recovery_ms),
            secondary_warm: Duration::from_millis(args.relay_secondary_warm_ms),
            lease_duration_us: args.relay_failover_lease_ms.saturating_mul(1_000),
            state: RelayFailoverControllerState::Arming,
            desired_mode: FailoverForwardMode::RepairOnly,
            transition_id,
            last_primary_source: None,
            last_secondary_repair: None,
            recovered_since: None,
            last_decoded: None,
            promotion_gap_base: None,
            promotion_sent_at: None,
            awaiting_secondary_source: false,
            awaiting_post_promotion_object: false,
            snapshot: RelayFailoverControllerSnapshot {
                state: RelayFailoverControllerState::Arming,
                enabled: 1,
                last_transition_unix_ms: now_unix_ms(),
                ..RelayFailoverControllerSnapshot::default()
            },
        }))
    }

    fn observe(
        &mut self,
        path: RelayIngressParentPath,
        observation: RelayDatagramObservation,
        now: Instant,
    ) {
        match (path, observation.role) {
            (RelayIngressParentPath::Primary, MediaDatagramRole::Source) => {
                self.last_primary_source = Some(now);
            }
            (RelayIngressParentPath::Secondary, MediaDatagramRole::Repair) => {
                self.last_secondary_repair = Some(now);
            }
            (RelayIngressParentPath::Secondary, MediaDatagramRole::Source)
                if self.awaiting_secondary_source =>
            {
                if let Some(sent_at) = self.promotion_sent_at {
                    self.snapshot.last_promotion_to_source_us =
                        duration_us(now.saturating_duration_since(sent_at));
                }
                self.awaiting_secondary_source = false;
            }
            _ => {}
        }
        if observation.decoded {
            if self.awaiting_post_promotion_object {
                if let Some(base) = self.promotion_gap_base {
                    let gap_us = duration_us(now.saturating_duration_since(base));
                    self.snapshot.last_media_gap_us = gap_us;
                    self.snapshot.max_media_gap_us = self.snapshot.max_media_gap_us.max(gap_us);
                }
                self.awaiting_post_promotion_object = false;
            }
            self.last_decoded = Some(now);
        }
    }

    async fn tick(&mut self, now: Instant) {
        let primary_recent = self
            .last_primary_source
            .is_some_and(|seen| now.saturating_duration_since(seen) < self.silence);
        let secondary_recent = self
            .last_secondary_repair
            .is_some_and(|seen| now.saturating_duration_since(seen) < self.secondary_warm);

        match self.state {
            RelayFailoverControllerState::Disabled => {}
            RelayFailoverControllerState::Arming => {
                if primary_recent && secondary_recent {
                    self.set_state(RelayFailoverControllerState::Healthy);
                } else if self.last_primary_source.is_some() && !primary_recent {
                    if secondary_recent {
                        self.promote(now);
                    } else {
                        self.secondary_unavailable();
                    }
                }
            }
            RelayFailoverControllerState::Healthy => {
                if !primary_recent {
                    if secondary_recent {
                        self.promote(now);
                    } else {
                        self.secondary_unavailable();
                    }
                }
            }
            RelayFailoverControllerState::SecondaryUnavailable => {
                if primary_recent {
                    self.set_state(RelayFailoverControllerState::Healthy);
                } else if secondary_recent && self.last_primary_source.is_some() {
                    self.promote(now);
                }
            }
            RelayFailoverControllerState::Promoted | RelayFailoverControllerState::Recovering => {
                if primary_recent {
                    let recovered_since = *self.recovered_since.get_or_insert(now);
                    self.set_state(RelayFailoverControllerState::Recovering);
                    if now.saturating_duration_since(recovered_since) >= self.recovery {
                        self.demote();
                    }
                } else {
                    self.recovered_since = None;
                    self.set_state(RelayFailoverControllerState::Promoted);
                }
            }
        }
        self.refresh_ages(now);
        self.send_desired().await;
    }

    fn promote(&mut self, now: Instant) {
        self.desired_mode = FailoverForwardMode::SourceAndRepair;
        self.advance_transition();
        self.snapshot.promotions = self.snapshot.promotions.saturating_add(1);
        self.snapshot.last_detection_us = self
            .last_primary_source
            .map_or(0, |seen| duration_us(now.saturating_duration_since(seen)));
        self.promotion_sent_at = Some(now);
        self.promotion_gap_base = self.last_decoded;
        self.awaiting_secondary_source = true;
        self.awaiting_post_promotion_object = true;
        self.recovered_since = None;
        self.set_state(RelayFailoverControllerState::Promoted);
    }

    fn demote(&mut self) {
        self.desired_mode = FailoverForwardMode::RepairOnly;
        self.advance_transition();
        self.snapshot.demotions = self.snapshot.demotions.saturating_add(1);
        self.recovered_since = None;
        self.awaiting_secondary_source = false;
        self.set_state(RelayFailoverControllerState::Healthy);
    }

    fn secondary_unavailable(&mut self) {
        if self.state != RelayFailoverControllerState::SecondaryUnavailable {
            self.snapshot.secondary_unavailable_events =
                self.snapshot.secondary_unavailable_events.saturating_add(1);
        }
        self.set_state(RelayFailoverControllerState::SecondaryUnavailable);
    }

    fn set_state(&mut self, state: RelayFailoverControllerState) {
        if self.state != state {
            self.state = state;
            self.snapshot.state = state;
            self.snapshot.last_transition_unix_ms = now_unix_ms();
        }
    }

    fn advance_transition(&mut self) {
        self.transition_id = now_unix_us().max(self.transition_id.saturating_add(1));
    }

    fn refresh_ages(&mut self, now: Instant) {
        self.snapshot.primary_source_age_ms = self
            .last_primary_source
            .map_or(0, |seen| duration_ms(now.saturating_duration_since(seen)));
        self.snapshot.secondary_repair_age_ms = self
            .last_secondary_repair
            .map_or(0, |seen| duration_ms(now.saturating_duration_since(seen)));
    }

    async fn send_desired(&mut self) {
        let issued_at = now_unix_us().max(1);
        let Ok(command) = FailoverLeaseCommand::new(
            self.generation,
            self.subscription_id,
            self.transition_id,
            issued_at,
            self.lease_duration_us,
            self.desired_mode,
        ) else {
            self.snapshot.command_send_errors = self.snapshot.command_send_errors.saturating_add(1);
            return;
        };
        match self.socket.send_to(&command.encode(), self.target).await {
            Ok(sent) if sent == FAILOVER_CONTROL_WIRE_LEN => {
                self.snapshot.commands_sent = self.snapshot.commands_sent.saturating_add(1);
            }
            Ok(_) | Err(_) => {
                self.snapshot.command_send_errors =
                    self.snapshot.command_send_errors.saturating_add(1);
            }
        }
    }

    const fn snapshot(&self) -> RelayFailoverControllerSnapshot {
        self.snapshot
    }
}

fn duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn atomic_saturating_sub(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}

async fn start_relay_failover_listeners(
    endpoints: &[RelayFailoverListenerEndpoint],
    forwarder: Option<&Arc<RelayDownstreamForwarder>>,
    generation: TopologyGeneration,
    subscription_id: SubscriptionId,
    cache: &Arc<LiveTsCache>,
    shutdown_rx: watch::Receiver<()>,
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    if endpoints.is_empty() {
        return Ok(Vec::new());
    }
    let forwarder = forwarder.context("failover listeners require a downstream forwarder")?;
    let mut bound = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        let socket = UdpSocket::bind(endpoint.bind).await.with_context(|| {
            format!(
                "failed to bind RelaySession failover listener on {}",
                endpoint.bind
            )
        })?;
        info!(
            bind = %socket.local_addr()?,
            peer = %endpoint.peer,
            forward_target = %endpoint.forward_target,
            "RelaySession warm-secondary failover listener ready"
        );
        bound.push((*endpoint, socket));
    }
    let mut tasks = Vec::with_capacity(bound.len());
    for (endpoint, socket) in bound {
        forwarder.register_failover_listener();
        cache.update_relay_forward(forwarder.snapshot());
        tasks.push(tokio::spawn(run_relay_failover_listener(
            socket,
            endpoint,
            generation,
            subscription_id,
            Arc::clone(forwarder),
            Arc::clone(cache),
            shutdown_rx.clone(),
        )));
    }
    Ok(tasks)
}

async fn run_relay_failover_listener(
    socket: UdpSocket,
    endpoint: RelayFailoverListenerEndpoint,
    generation: TopologyGeneration,
    subscription_id: SubscriptionId,
    forwarder: Arc<RelayDownstreamForwarder>,
    cache: Arc<LiveTsCache>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut buffer = [0_u8; FAILOVER_CONTROL_WIRE_LEN + 1];
    let mut transition_id = 0_u64;
    let mut transition_mode = FailoverForwardMode::RepairOnly;
    let mut lease_deadline: Option<Instant> = None;
    let mut lease_tick = interval(Duration::from_millis(25));
    lease_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = lease_tick.tick() => {
                if lease_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    let _ = forwarder.apply_failover_mode(
                        endpoint.forward_target,
                        FailoverForwardMode::RepairOnly,
                        true,
                    );
                    cache.update_relay_forward(forwarder.snapshot());
                    lease_deadline = None;
                    transition_mode = FailoverForwardMode::RepairOnly;
                }
            }
            received = socket.recv_from(&mut buffer) => {
                let Ok((len, peer)) = received else {
                    forwarder.record_failover_command_rejected();
                    cache.update_relay_forward(forwarder.snapshot());
                    continue;
                };
                let command = if peer == endpoint.peer {
                    FailoverLeaseCommand::decode(&buffer[..len])
                } else {
                    Err(relay_session::Error::InvalidField {
                        field: "failover_controller_peer",
                        reason: "command arrived from an unassigned controller",
                    })
                };
                let Ok(command) = command else {
                    forwarder.record_failover_command_rejected();
                    cache.update_relay_forward(forwarder.snapshot());
                    continue;
                };
                let stale = command.generation != generation
                    || command.subscription_id != subscription_id
                    || command.transition_id < transition_id
                    || (command.transition_id == transition_id && command.mode != transition_mode);
                if stale {
                    forwarder.record_failover_command_rejected();
                    cache.update_relay_forward(forwarder.snapshot());
                    continue;
                }
                transition_id = command.transition_id;
                transition_mode = command.mode;
                if let Some(promoted_now) =
                    forwarder.apply_failover_mode(endpoint.forward_target, command.mode, false)
                {
                    lease_deadline = (command.mode == FailoverForwardMode::SourceAndRepair)
                        .then(|| Instant::now() + Duration::from_micros(command.lease_duration_us));
                    if promoted_now {
                        forwarder.replay_warm_sources(endpoint.forward_target).await;
                    }
                }
                cache.update_relay_forward(forwarder.snapshot());
            }
        }
    }
}

struct RelayIngestRuntime {
    dispatch: RelayUdpDispatch,
    secondary_socket: Option<UdpSocket>,
    forwarder: Option<Arc<RelayDownstreamForwarder>>,
    audio_epochs: Option<broadcast::Sender<AudioEpochDatagram>>,
    failover_controller: Option<RelayFailoverController>,
    failover_heartbeat: Duration,
}

async fn run_udp_fec_ingest(
    socket: UdpSocket,
    cache: Arc<LiveTsCache>,
    mut shutdown_rx: watch::Receiver<()>,
    runtime: RelayIngestRuntime,
) -> Result<()> {
    let RelayIngestRuntime {
        dispatch: mut relay_dispatch,
        secondary_socket: relay_secondary_socket,
        forwarder: relay_forwarder,
        audio_epochs,
        failover_controller: mut relay_failover_controller,
        failover_heartbeat,
    } = runtime;
    let mut receiver = UdpFecReceiver::new();
    let mut audio_block_sessions = HashMap::<u32, (u64, Instant)>::new();
    let mut native_audio_relay = NativeAudioRelay::default();
    cache.update_relay_ingress(relay_dispatch.receiver().snapshot());
    let mut buf = vec![0u8; 65_536];
    let mut relay_secondary_buf = vec![0u8; 65_536];
    let mut rotate = interval(Duration::from_millis(10));
    rotate.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut expire_fec = interval(Duration::from_secs(1));
    expire_fec.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut failover_tick = interval(failover_heartbeat);
    failover_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                cache.rotate_if_due(true).await?;
                info!("UDP-FEC mesh byte ingest shutting down");
                return Ok(());
            }
            _ = rotate.tick() => {
                cache.rotate_if_due(false).await?;
            }
            _ = expire_fec.tick() => {
                native_audio_relay.expire(Instant::now());
                let expired = receiver.expire_inactive();
                if expired.objects > 0 || expired.flows > 0 {
                    debug!(
                        expired_objects = expired.objects,
                        expired_flows = expired.flows,
                        released_object_bytes = expired.released_object_bytes,
                        released_datagrams = expired.released_datagrams,
                        "expired inactive UDP-FEC receive state"
                    );
                }
                let relay_expired = relay_dispatch.receiver_mut().expire(now_unix_us());
                cache.update_relay_ingress(relay_dispatch.receiver().snapshot());
                if relay_expired.objects > 0 {
                    debug!(
                        expired_objects = relay_expired.objects,
                        released_object_bytes = relay_expired.released_object_bytes,
                        released_datagrams = relay_expired.released_datagrams,
                        "expired deadline-bound RelaySession receive state"
                    );
                }
                let retired_streams = cache
                    .retire_streams_idle_before(
                        now_unix_ms().saturating_sub(
                            CANONICAL_STREAM_IDLE_RETENTION.as_millis() as u64,
                        ),
                    )
                    .await;
                if retired_streams > 0 {
                    info!(retired_streams, "retired idle canonical stream cache state");
                }
            }
            _ = failover_tick.tick(), if relay_failover_controller.is_some() => {
                if let Some(controller) = relay_failover_controller.as_mut() {
                    controller.tick(Instant::now()).await;
                    cache.update_relay_failover_controller(controller.snapshot());
                }
            }
            received = socket.recv_from(&mut buf) => {
                let (len, peer) = received?;
                if native_audio_relay
                    .handle_control(&socket, peer, &buf[..len])
                    .await
                {
                    continue;
                }
                if process_relay_audio_epoch_datagram(
                    peer,
                    &buf[..len],
                    &mut audio_block_sessions,
                    audio_epochs.as_ref(),
                    relay_forwarder.as_deref(),
                    Some((&socket, &native_audio_relay)),
                ).await {
                    continue;
                }
                let observation = process_udp_fec_ingest_datagram(
                    peer,
                    &buf[..len],
                    &cache,
                    &mut receiver,
                    &mut relay_dispatch,
                    relay_forwarder.as_deref(),
                ).await?;
                if let (Some(controller), Some(observation)) =
                    (relay_failover_controller.as_mut(), observation)
                {
                    controller.observe(RelayIngressParentPath::Primary, observation, Instant::now());
                    cache.update_relay_failover_controller(controller.snapshot());
                }
            }
            received = recv_optional_udp(&relay_secondary_socket, &mut relay_secondary_buf) => {
                let (len, peer) = received?;
                if process_relay_audio_epoch_datagram(
                    peer,
                    &relay_secondary_buf[..len],
                    &mut audio_block_sessions,
                    audio_epochs.as_ref(),
                    relay_forwarder.as_deref(),
                    Some((&socket, &native_audio_relay)),
                ).await {
                    continue;
                }
                let observation = process_udp_fec_ingest_datagram(
                    peer,
                    &relay_secondary_buf[..len],
                    &cache,
                    &mut receiver,
                    &mut relay_dispatch,
                    relay_forwarder.as_deref(),
                ).await?;
                if let (Some(controller), Some(observation)) =
                    (relay_failover_controller.as_mut(), observation)
                {
                    controller.observe(RelayIngressParentPath::Secondary, observation, Instant::now());
                    cache.update_relay_failover_controller(controller.snapshot());
                }
            }
        }
    }
}

async fn process_relay_audio_epoch_datagram(
    peer: SocketAddr,
    datagram: &[u8],
    block_sessions: &mut HashMap<u32, (u64, Instant)>,
    audio_epochs: Option<&broadcast::Sender<AudioEpochDatagram>>,
    relay_forwarder: Option<&RelayDownstreamForwarder>,
    native_audio_relay: Option<(&UdpSocket, &NativeAudioRelay)>,
) -> bool {
    if !is_multichannel_audio_transport_datagram(datagram) {
        return false;
    }

    let now = Instant::now();
    block_sessions.retain(|_, (_, expires_at)| *expires_at > now);
    let identity = match inspect_multichannel_audio_datagram(
        &datagram[MULTICHANNEL_AUDIO_TRANSPORT_MAGIC.len()..],
    ) {
        Ok(identity) => identity,
        Err(error) => {
            warn!(peer = %peer, error = %error, "invalid AEP1 datagram rejected at relay ingress");
            return true;
        }
    };
    let session_id = if let Some(session_id) = identity.session_id {
        block_sessions.insert(
            identity.block_id,
            (session_id, now + Duration::from_secs(15)),
        );
        Some(session_id)
    } else {
        block_sessions
            .get(&identity.block_id)
            .map(|(session_id, _)| *session_id)
    };
    let role = if identity.source_index.is_some() {
        MediaDatagramRole::Source
    } else {
        MediaDatagramRole::Repair
    };

    if let Some((socket, native_audio_relay)) = native_audio_relay {
        native_audio_relay.forward(socket, datagram, session_id);
    }

    if let Some(forwarder) = relay_forwarder {
        forwarder.forward(datagram, role, None, None).await;
    }
    if let Some(audio_epochs) = audio_epochs {
        let receivers = audio_epochs.receiver_count();
        let _ = audio_epochs.send(AudioEpochDatagram {
            session_id,
            bytes: Bytes::copy_from_slice(datagram),
        });
        debug!(
            peer = %peer,
            ?session_id,
            ?role,
            receivers,
            datagram_bytes = datagram.len(),
            "relayed AEP1 datagram to playback-edge subscribers"
        );
    }
    true
}

async fn recv_optional_udp(
    socket: &Option<UdpSocket>,
    buffer: &mut [u8],
) -> std::io::Result<(usize, SocketAddr)> {
    match socket {
        Some(socket) => socket.recv_from(buffer).await,
        None => std::future::pending().await,
    }
}

async fn process_udp_fec_ingest_datagram(
    peer: SocketAddr,
    datagram: &[u8],
    cache: &LiveTsCache,
    receiver: &mut UdpFecReceiver,
    relay_dispatch: &mut RelayUdpDispatch,
    relay_forwarder: Option<&RelayDownstreamForwarder>,
) -> Result<Option<RelayDatagramObservation>> {
    let started = Instant::now();
    let result = process_udp_fec_ingest_datagram_inner(
        peer,
        datagram,
        cache,
        receiver,
        relay_dispatch,
        relay_forwarder,
    )
    .await;
    cache.update_relay_runtime(
        relay_dispatch.receiver().snapshot(),
        relay_forwarder.map(RelayDownstreamForwarder::snapshot),
    );
    cache.record_relay_processing(started.elapsed());
    result
}

async fn process_udp_fec_ingest_datagram_inner(
    peer: SocketAddr,
    datagram: &[u8],
    cache: &LiveTsCache,
    receiver: &mut UdpFecReceiver,
    relay_dispatch: &mut RelayUdpDispatch,
    relay_forwarder: Option<&RelayDownstreamForwarder>,
) -> Result<Option<RelayDatagramObservation>> {
    debug!(
        peer = %peer,
        datagram_bytes = datagram.len(),
        "UDP-FEC mesh datagram received"
    );
    let relay_result = relay_dispatch.push(peer, datagram, now_unix_us());
    match relay_result {
        Ok(RelayUdpDispatchOutcome::Legacy) => {}
        Ok(RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Buffered {
            key,
            role,
            deadline,
        })) => {
            if let Some(forwarder) = relay_forwarder {
                forwarder
                    .forward(datagram, role, Some(&key), Some(deadline.expires_at_us))
                    .await;
            }
            debug!(
                peer = %peer,
                stream = key.stream(),
                object = key.object(),
                ?role,
                "RelaySession RaptorQ symbol buffered"
            );
            return Ok(Some(RelayDatagramObservation {
                role,
                decoded: false,
            }));
        }
        Ok(RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Decoded {
            object,
            role,
            deadline,
            parent_count,
            accepted_datagrams,
            ..
        })) => {
            if let Some(forwarder) = relay_forwarder {
                forwarder
                    .forward(
                        datagram,
                        role,
                        Some(object.key()),
                        Some(deadline.expires_at_us),
                    )
                    .await;
            }
            let stream = object.key().stream().to_owned();
            let sequence = object.key().object();
            let publication_clock = relay_publication_clock(&object);
            let decoded = match commit_relay_object(cache, *object).await {
                Ok(_) => {
                    if let Some(clock) = publication_clock {
                        cache.record_relay_availability(clock.observe(now_unix_us()));
                    }
                    debug!(
                        peer = %peer,
                        stream,
                        sequence,
                        parent_count,
                        accepted_datagrams,
                        "committed canonical RelaySession RaptorQ object"
                    );
                    true
                }
                Err(error) => {
                    warn!(
                        peer = %peer,
                        stream,
                        sequence,
                        error = %error,
                        "failed to cache canonical RelaySession object"
                    );
                    false
                }
            };
            return Ok(Some(RelayDatagramObservation { role, decoded }));
        }
        Ok(RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Duplicate {
            key,
            role,
            deadline,
        })) => {
            if let Some(forwarder) = relay_forwarder {
                forwarder
                    .forward(datagram, role, Some(&key), Some(deadline.expires_at_us))
                    .await;
            }
            debug!(
                peer = %peer,
                stream = key.stream(),
                object = key.object(),
                ?role,
                "authenticated duplicate RelaySession symbol admitted for downstream forwarding"
            );
            return Ok(Some(RelayDatagramObservation {
                role,
                decoded: false,
            }));
        }
        Err(error) => {
            warn!(
                peer = %peer,
                datagram_bytes = datagram.len(),
                error = %error,
                "RelaySession datagram rejected at configured dispatch seam"
            );
            return Ok(None);
        }
    }

    match receiver.try_push_payload(peer, datagram) {
        Ok(UdpFecPushOutcome::Decoded {
            block_id,
            payload: decoded,
        }) => {
            let payload_bytes = decoded.payload.len();
            if let Some(stream_id) = decoded.stream_id {
                match cache
                    .commit_stream_payload(stream_id, decoded.payload)
                    .await
                {
                    Ok(sequence) => {
                        debug!(
                            peer = %peer,
                            stream_id,
                            block_id,
                            sequence,
                            payload_bytes,
                            "cached stream-prefixed UDP-FEC mesh byte payload"
                        );
                    }
                    Err(error) => {
                        warn!(peer = %peer, stream_id, block_id, error = %error, "failed to cache stream-prefixed UDP-FEC mesh byte payload");
                    }
                }
            } else if let Err(error) = cache.push_payload(&decoded.payload).await {
                warn!(peer = %peer, block_id, error = %error, "failed to cache UDP-FEC mesh byte payload");
            } else {
                debug!(
                    peer = %peer,
                    block_id,
                    payload_bytes,
                    "cached UDP-FEC mesh byte payload"
                );
            }
        }
        Ok(UdpFecPushOutcome::Buffered {
            stream_id,
            block_id,
        }) => {
            debug!(
                peer = %peer,
                ?stream_id,
                block_id,
                "UDP-FEC symbols buffered awaiting repair/source symbols"
            );
        }
        Ok(UdpFecPushOutcome::Duplicate {
            stream_id,
            block_id,
        }) => {
            debug!(
                peer = %peer,
                ?stream_id,
                block_id,
                "duplicate completed UDP-FEC object ignored"
            );
        }
        Err(error) => {
            warn!(
                peer = %peer,
                datagram_bytes = datagram.len(),
                error = %error,
                "UDP-FEC datagram rejected"
            );
        }
    }
    Ok(None)
}

async fn commit_relay_object(cache: &LiveTsCache, object: MediaObject) -> Result<u64> {
    let stream_id = object
        .key()
        .stream()
        .parse::<u64>()
        .context("RelaySession object stream identity must map to a local numeric stream")?;
    let envelope = media_object::encode(&object).context("failed to encode canonical object")?;
    cache
        .commit_stream_payload(stream_id, Bytes::from(envelope))
        .await
}

async fn run_udp_media_fec_ingest(
    socket: UdpSocket,
    cache: Arc<LiveTsCache>,
    audio_epochs: broadcast::Sender<AudioEpochDatagram>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let mut decoder = MediaFecDecoder::new();
    let mut audio_block_sessions = HashMap::<(SocketAddr, u32), (u64, Instant)>::new();
    let mut native_audio_relay = NativeAudioRelay::default();
    let mut buf = vec![0u8; 65_536];
    let audio_block_ttl = Duration::from_secs(15);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("media UDP-FEC access-unit ingest shutting down");
                return Ok(());
            }
            received = socket.recv_from(&mut buf) => {
                let (len, peer) = received?;
                native_audio_relay.expire(Instant::now());
                if native_audio_relay
                    .handle_control(&socket, peer, &buf[..len])
                    .await
                {
                    continue;
                }
                debug!(
                    peer = %peer,
                    datagram_bytes = len,
                    "media UDP-FEC datagram received"
                );
                if is_multichannel_audio_transport_datagram(&buf[..len]) {
                    let now = Instant::now();
                    audio_block_sessions.retain(|_, (_, expires_at)| *expires_at > now);
                    let identity = inspect_multichannel_audio_datagram(
                        &buf[MULTICHANNEL_AUDIO_TRANSPORT_MAGIC.len()..len],
                    );
                    let session_id = identity.ok().and_then(|identity| {
                        if let Some(session_id) = identity.session_id {
                            audio_block_sessions.insert(
                                (peer, identity.block_id),
                                (session_id, now + audio_block_ttl),
                            );
                            Some(session_id)
                        } else {
                            audio_block_sessions
                                .get(&(peer, identity.block_id))
                                .map(|(session_id, _)| *session_id)
                        }
                    });
                    native_audio_relay.forward(&socket, &buf[..len], session_id);
                    let receivers = audio_epochs.receiver_count();
                    let _ = audio_epochs.send(AudioEpochDatagram {
                        session_id,
                        bytes: Bytes::copy_from_slice(&buf[..len]),
                    });
                    debug!(
                        peer = %peer,
                        ?session_id,
                        datagram_bytes = len,
                        receivers,
                        "broadcast media UDP-FEC multichannel audio epoch datagram"
                    );
                    continue;
                }
                match decoder.push_datagram(&buf[..len]) {
                    Ok(Some(frame)) => {
                        let stream_id = frame.metadata.stream_id;
                        let sequence = frame.metadata.sequence;
                        let payload_bytes = frame.payload.len();
                        if let Err(error) = cache
                            .add_media_access_unit(frame.metadata, Bytes::from(frame.payload))
                            .await
                        {
                            warn!(
                                peer = %peer,
                                stream_id,
                                sequence,
                                error = %error,
                                "failed to cache media UDP-FEC access unit"
                            );
                        } else {
                            debug!(
                                peer = %peer,
                                stream_id,
                                sequence,
                                payload_bytes,
                                "cached media UDP-FEC access unit"
                            );
                        }
                    }
                    Ok(None) => {
                        debug!(
                            peer = %peer,
                            datagram_bytes = len,
                            "media UDP-FEC datagram buffered awaiting complete access unit"
                        );
                    }
                    Err(error) => {
                        warn!(
                            peer = %peer,
                            error = %error,
                            "failed to decode media UDP-FEC access unit"
                        );
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayAvailabilityObservation {
    Measured {
        duration_us: u64,
        clock_error_us: u64,
    },
    UnusableClock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayPublicationClock {
    Measured {
        published_us: u64,
        clock_error_us: u64,
    },
    Unusable,
}

impl RelayPublicationClock {
    fn observe(self, available_at_us: u64) -> RelayAvailabilityObservation {
        match self {
            Self::Measured {
                published_us,
                clock_error_us,
            } => available_at_us
                .checked_sub(published_us)
                .map(|duration_us| RelayAvailabilityObservation::Measured {
                    duration_us,
                    clock_error_us,
                })
                .unwrap_or(RelayAvailabilityObservation::UnusableClock),
            Self::Unusable => RelayAvailabilityObservation::UnusableClock,
        }
    }
}

fn relay_publication_clock(object: &MediaObject) -> Option<RelayPublicationClock> {
    if object.kind() != ObjectKind::Media {
        return None;
    }
    let Some(published) = object
        .stage_timestamps()
        .iter()
        .find(|timestamp| timestamp.stage() == Stage::Published)
        .map(|timestamp| timestamp.timestamp())
    else {
        return Some(RelayPublicationClock::Unusable);
    };
    let Ok(published_ns) = u64::try_from(published.unix_time_ns()) else {
        return Some(RelayPublicationClock::Unusable);
    };
    let Some(clock_error_ns) = published.confidence().maximum_error_ns() else {
        return Some(RelayPublicationClock::Unusable);
    };
    Some(RelayPublicationClock::Measured {
        published_us: published_ns.div_ceil(1_000),
        clock_error_us: clock_error_ns.div_ceil(1_000),
    })
}

#[cfg(test)]
fn relay_availability_observation(
    object: &MediaObject,
    available_at_us: u64,
) -> Option<RelayAvailabilityObservation> {
    relay_publication_clock(object).map(|clock| clock.observe(available_at_us))
}

#[derive(Debug, Clone)]
struct CachedMediaAccessUnit {
    metadata: MediaFrameMetadata,
    payload_bytes: usize,
    serialized: Bytes,
    hash: u64,
}

fn codec_name(codec: MediaCodec) -> &'static str {
    match codec {
        MediaCodec::Unknown => "unknown",
        MediaCodec::H264 => "h264",
        MediaCodec::Opus => "opus",
        MediaCodec::Aac => "aac",
        MediaCodec::Data => "data",
    }
}

struct WebTransportMediaDecoder {
    unprefixed: MediaFecDecoder,
    prefixed_by_stream: HashMap<u64, MediaFecDecoder>,
}

impl WebTransportMediaDecoder {
    fn new() -> Self {
        Self {
            unprefixed: MediaFecDecoder::new(),
            prefixed_by_stream: HashMap::new(),
        }
    }

    fn push_datagram(
        &mut self,
        datagram: &[u8],
    ) -> std::result::Result<Option<DecodedMediaFrame>, String> {
        if datagram.len() == STREAM_ID_PREFIX_LEN && split_stream_id_prefix(datagram).is_some() {
            return Ok(None);
        }

        if let Some((stream_id, payload)) = split_stream_id_prefix(datagram) {
            if payload.starts_with(&DATAGRAM_MAGIC) {
                let decoder = self.prefixed_by_stream.entry(stream_id).or_default();
                let transport = FecDatagramDecoder::webtransport_with_stream_prefix(stream_id);
                let decoded = transport
                    .push_media_datagram(decoder, datagram)
                    .map_err(|error| error.to_string())?;
                if let Some(frame) = decoded.as_ref() {
                    let metadata_stream_id = frame.metadata.stream_id;
                    if metadata_stream_id != stream_id {
                        return Err(format!(
                            "WebTransport stream prefix {stream_id} does not match media stream id {metadata_stream_id}"
                        ));
                    }
                }
                return Ok(decoded);
            }
        }

        self.unprefixed
            .push_datagram(datagram)
            .map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone, Serialize)]
struct MediaAccessUnitResponse {
    stream_id: u64,
    stream_id_text: String,
    sequence: u64,
    pts_ms: u64,
    dts_ms: Option<u64>,
    duration_ms: u32,
    codec: &'static str,
    flags: u16,
    payload_bytes: usize,
    stored_bytes: usize,
}

impl MediaAccessUnitResponse {
    fn from_cached(unit: &CachedMediaAccessUnit) -> Self {
        Self {
            stream_id: unit.metadata.stream_id,
            stream_id_text: stream_id_text(unit.metadata.stream_id),
            sequence: unit.metadata.sequence,
            pts_ms: unit.metadata.pts_ms,
            dts_ms: unit.metadata.dts_ms,
            duration_ms: unit.metadata.duration_ms,
            codec: codec_name(unit.metadata.codec),
            flags: unit.metadata.flags.bits(),
            payload_bytes: unit.payload_bytes,
            stored_bytes: unit.serialized.len(),
        }
    }
}

#[derive(Debug, Clone)]
struct CachedPlaylist {
    stream_id: u64,
    version: u64,
    body: String,
}

struct LiveTsCache {
    chunk_cache: Arc<ChunkCache>,
    stream_id: u64,
    part_target: Duration,
    parts_per_segment: usize,
    window_parts: usize,
    max_part_bytes: usize,
    state: RwLock<LiveState>,
    canonical_commit_locks: StdMutex<HashMap<u64, Arc<AsyncMutex<()>>>>,
    part_updates: watch::Sender<u64>,
    playlist_cache: Vec<StdRwLock<Option<CachedPlaylist>>>,
    relay_ingress: StdRwLock<RelaySessionIngressSnapshot>,
    relay_failover_controller: StdRwLock<RelayFailoverControllerSnapshot>,
    relay_processing: AtomicDurationHistogram,
    relay_availability: RelayAvailabilityTelemetry,
}

impl LiveTsCache {
    async fn new(
        stream_id: u64,
        part_target: Duration,
        parts_per_segment: usize,
        window_parts: usize,
        slot_kb: usize,
    ) -> Arc<Self> {
        let options = CacheOptions {
            num_playlists: 16,
            max_segments: 1,
            max_parts_per_segment: window_parts.saturating_mul(4).max(32),
            buffer_size_kb: slot_kb,
            ..CacheOptions::default()
        };
        let playlist_cache = (0..options.num_playlists)
            .map(|_| StdRwLock::new(None))
            .collect();
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let _ = chunk_cache.get_or_create_stream_idx(stream_id).await;
        let (part_updates, _) = watch::channel(0);
        Arc::new(Self {
            chunk_cache,
            stream_id,
            part_target,
            parts_per_segment,
            window_parts,
            max_part_bytes: slot_kb * 1024,
            state: RwLock::new(LiveState::new()),
            canonical_commit_locks: StdMutex::new(HashMap::new()),
            part_updates,
            playlist_cache,
            relay_ingress: StdRwLock::new(RelaySessionIngressSnapshot::default()),
            relay_failover_controller: StdRwLock::new(RelayFailoverControllerSnapshot::default()),
            relay_processing: AtomicDurationHistogram::default(),
            relay_availability: RelayAvailabilityTelemetry::default(),
        })
    }

    fn update_relay_ingress(&self, snapshot: RelayIngressSnapshot) {
        self.update_relay_runtime(snapshot, None);
    }

    fn update_relay_runtime(
        &self,
        snapshot: RelayIngressSnapshot,
        forward: Option<RelayForwardSnapshot>,
    ) {
        let mut current = self
            .relay_ingress
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let forward = forward.unwrap_or_else(|| current.forward_snapshot());
        *current = snapshot.into();
        current.apply_forward_snapshot(forward);
    }

    fn update_relay_forward(&self, snapshot: RelayForwardSnapshot) {
        let mut current = self
            .relay_ingress
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        current.apply_forward_snapshot(snapshot);
    }

    fn update_relay_failover_controller(&self, snapshot: RelayFailoverControllerSnapshot) {
        *self
            .relay_failover_controller
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = snapshot;
    }

    fn relay_ingress_snapshot(&self) -> RelaySessionIngressSnapshot {
        let mut snapshot = *self
            .relay_ingress
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot.apply_failover_controller_snapshot(
            *self
                .relay_failover_controller
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        snapshot.processing_duration_count = self.relay_processing.count.load(Ordering::Relaxed);
        snapshot.processing_duration_sum_us = self.relay_processing.sum_us.load(Ordering::Relaxed);
        snapshot.processing_duration_max_us = self.relay_processing.max_us.load(Ordering::Relaxed);
        snapshot.processing_duration_buckets = std::array::from_fn(|index| {
            self.relay_processing.buckets[index].load(Ordering::Relaxed)
        });
        self.relay_availability.apply_to(&mut snapshot);
        snapshot
    }

    fn record_relay_processing(&self, duration: Duration) {
        self.relay_processing.record(duration);
    }

    fn record_relay_availability(&self, observation: RelayAvailabilityObservation) {
        self.relay_availability.record(observation);
    }

    fn canonical_commit_lock(&self, stream_id: u64) -> Arc<AsyncMutex<()>> {
        let mut locks = self
            .canonical_commit_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            locks
                .entry(stream_id)
                .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
        )
    }

    async fn push_payload(&self, payload: &[u8]) -> Result<()> {
        let now = Instant::now();
        let now_ms = now_unix_ms();
        let finalized = {
            let mut state = self.state.write().await;
            state.datagrams_received += 1;
            state.last_ingest_unix_ms = Some(now_ms);
            state.bytes_received += payload.len() as u64;

            if state.current.is_empty() {
                state.current_started = now;
                state.current_started_unix_ms = now_ms;
            }
            state.current.extend_from_slice(payload);

            if now.duration_since(state.current_started) >= self.part_target
                || state.current.len() >= self.max_part_bytes
            {
                state.take_current(now, now_ms)
            } else {
                None
            }
        };

        if let Some(part) = finalized {
            self.commit_part(part).await?;
        }
        Ok(())
    }

    async fn rotate_if_due(&self, force: bool) -> Result<()> {
        let now = Instant::now();
        let now_ms = now_unix_ms();
        let finalized = {
            let mut state = self.state.write().await;
            if force || now.duration_since(state.current_started) >= self.part_target {
                state.take_current(now, now_ms)
            } else {
                None
            }
        };

        if let Some(part) = finalized {
            self.commit_part(part).await?;
        }
        Ok(())
    }

    async fn commit_part(&self, part: PendingPart) -> Result<()> {
        self.chunk_cache
            .add_for_stream_id(self.stream_id, part.seq as usize, Bytes::from(part.data))
            .await
            .map_err(|err| anyhow!("chunk cache write failed: {err}"))?;

        let mut state = self.state.write().await;
        state.last_committed_seq = Some(part.seq);
        state.last_committed_unix_ms = Some(part.committed_unix_ms);
        state.last_committed_bytes = Some(part.bytes);
        state.last_committed_duration_ms = Some(part.duration_ms);
        state.record_part_available(self.stream_id, part.seq, now_unix_us(), self.window_parts);
        debug!(
            stream_id = self.stream_id,
            sequence = part.seq,
            bytes = part.bytes,
            duration_ms = part.duration_ms,
            "committed mesh byte part"
        );
        drop(state);
        self.part_updates.send_replace(part.seq);
        Ok(())
    }

    async fn commit_stream_payload(&self, stream_id: u64, payload: Bytes) -> Result<u64> {
        let bytes = payload.len();
        // Stream commits and retirement share this lock so a newly arriving
        // object cannot race cache/state removal for the same stream.
        let commit_lock = self.canonical_commit_lock(stream_id);
        let _commit_guard = commit_lock.lock().await;
        if let Some(object) = decode_canonical_stream_object(&payload)? {
            let expected_stream = stream_id.to_string();
            if object.key().stream() != expected_stream {
                bail!(
                    "canonical media-object stream {} does not match carrier stream {stream_id}",
                    object.key().stream()
                );
            }

            let source_epoch = object.key().epoch();
            let (previous_epoch, process_started_unix_us) = {
                let state = self.state.read().await;
                (
                    state.stream_canonical_epoch.get(&stream_id).copied(),
                    state.process_started_unix_us,
                )
            };
            if let Some(previous) = previous_epoch {
                if source_epoch < previous {
                    bail!(
                        "stale canonical source epoch {source_epoch} for stream {stream_id}; current epoch is {previous}"
                    );
                }
            }
            if previous_epoch != Some(source_epoch) {
                self.chunk_cache.zero_stream_id(stream_id).await;
                // A relay may restart and inherit an epoch that has already
                // been active for minutes or hours. Its source age is not an
                // epoch-activation delay. Measure only epochs observed after
                // this process began, or genuine transitions from a previous
                // epoch already active in this process.
                let activation_delay_us = (previous_epoch.is_some()
                    || source_epoch >= process_started_unix_us)
                    .then(|| now_unix_us().checked_sub(source_epoch))
                    .flatten();
                let mut state = self.state.write().await;
                state.stream_canonical_epoch.insert(stream_id, source_epoch);
                state
                    .stream_canonical_epoch_activation_delay_us
                    .remove(&stream_id);
                if let Some(activation_delay_us) = activation_delay_us {
                    state
                        .stream_canonical_epoch_activation_delay_us
                        .insert(stream_id, activation_delay_us);
                }
                state.stream_subscription_base_object.remove(&stream_id);
                state.stream_latest_canonical_object.remove(&stream_id);
                state.stream_next_seq.remove(&stream_id);
                state
                    .stream_part_available_unix_us
                    .retain(|(retained_stream, _), _| *retained_stream != stream_id);
                state.stream_inits.remove(&stream_id);
                state.stream_media_kinds.remove(&stream_id);
                if stream_id == self.stream_id {
                    state.last_committed_seq = None;
                    state.last_committed_unix_ms = None;
                    state.last_committed_bytes = None;
                    state.last_committed_duration_ms = None;
                }
                info!(
                    stream_id,
                    source_epoch,
                    ?activation_delay_us,
                    ?previous_epoch,
                    "activated canonical media source epoch"
                );
            }

            let seq = object.key().object();
            let kind = object.kind();
            let media_bytes = object.payload().len();
            let is_fmp4 = object
                .metadata()
                .get("container")
                .is_some_and(|container| container.as_slice() == b"fmp4");
            let media_kind = if is_fmp4 {
                LiveMediaKind::Fmp4
            } else {
                LiveMediaKind::Ts
            };
            let now_ms = now_unix_ms();

            {
                let mut state = self.state.write().await;
                state.datagrams_received = state.datagrams_received.saturating_add(1);
                state.bytes_received = state.bytes_received.saturating_add(bytes as u64);
                state.last_ingest_unix_ms = Some(now_ms);
                state.stream_last_ingest_unix_ms.insert(stream_id, now_ms);
                state.observe_stream_seq(stream_id, seq);
                state.stream_media_kinds.insert(stream_id, media_kind);
                if matches!(
                    kind,
                    ObjectKind::Initialization | ObjectKind::CodecConfiguration
                ) {
                    state
                        .stream_inits
                        .insert(stream_id, Bytes::copy_from_slice(object.payload()));
                }
            }

            if matches!(
                kind,
                ObjectKind::Initialization | ObjectKind::CodecConfiguration
            ) {
                self.chunk_cache
                    .set_stream_initialization(stream_id, Bytes::copy_from_slice(object.payload()))
                    .await
                    .map_err(|err| anyhow!("stream initialization cache write failed: {err}"))?;
            }

            if kind != ObjectKind::Media {
                debug!(
                    stream_id,
                    source_epoch,
                    sequence = seq,
                    object_kind = ?kind,
                    bytes = media_bytes,
                    "accepted canonical stream metadata object"
                );
                return Ok(seq);
            }

            let slot_id = usize::try_from(seq).context("stream slot sequence too large")?;
            let subscription_base_object = self
                .state
                .read()
                .await
                .stream_subscription_base_object
                .get(&stream_id)
                .copied()
                .unwrap_or(seq);
            let subscription_base_object = usize::try_from(subscription_base_object)
                .context("stream subscription base object is too large")?;
            let write_result = self
                .chunk_cache
                .put_if_absent_contiguous_for_stream_id(
                    stream_id,
                    slot_id,
                    subscription_base_object,
                    payload,
                )
                .await
                .map_err(|err| anyhow!("canonical stream cache write failed: {err}"))?;
            if write_result == PutIfAbsentResult::HashConflict {
                bail!(
                    "canonical media-object identity conflict for stream {stream_id} sequence {seq}"
                );
            }

            let mut state = self.state.write().await;
            state
                .stream_subscription_base_object
                .entry(stream_id)
                .or_insert(seq);
            state
                .stream_latest_canonical_object
                .entry(stream_id)
                .and_modify(|head| *head = (*head).max(seq))
                .or_insert(seq);
            state.last_committed_seq =
                Some(state.last_committed_seq.map_or(seq, |last| last.max(seq)));
            state.last_committed_unix_ms = Some(now_ms);
            state.last_committed_bytes = Some(media_bytes);
            state.last_committed_duration_ms = None;
            state.record_part_available(stream_id, seq, now_unix_us(), self.window_parts);
            debug!(
                stream_id,
                source_epoch,
                sequence = seq,
                slot_id,
                bytes,
                media_bytes,
                media_kind = ?media_kind,
                cache_write = ?write_result,
                keyframe = object.is_keyframe(),
                "committed canonical RaptorQ media object"
            );
            drop(state);
            self.part_updates.send_replace(seq);
            return Ok(seq);
        }

        let decoded = LiveSlotPayload::decode(payload.clone());
        let media_bytes = decoded.media().len();
        let media_kind = decoded.media_kind();
        let init = decoded.init();
        let now_ms = now_unix_ms();
        let seq = {
            let mut state = self.state.write().await;
            state.datagrams_received = state.datagrams_received.saturating_add(1);
            state.bytes_received = state.bytes_received.saturating_add(bytes as u64);
            state.last_ingest_unix_ms = Some(now_ms);
            state.stream_last_ingest_unix_ms.insert(stream_id, now_ms);
            state.stream_media_kinds.insert(stream_id, media_kind);
            if let Some(init) = init.clone() {
                state.stream_inits.insert(stream_id, init);
            }
            state.next_stream_seq(stream_id)
        };
        if let Some(init) = init {
            self.chunk_cache
                .set_stream_initialization(stream_id, init)
                .await
                .map_err(|err| anyhow!("stream initialization cache write failed: {err}"))?;
        }
        let slot_id = usize::try_from(seq).context("stream slot sequence too large")?;
        self.chunk_cache
            .add_for_stream_id(stream_id, slot_id, payload)
            .await
            .map_err(|err| anyhow!("stream-prefixed chunk cache write failed: {err}"))?;

        let mut state = self.state.write().await;
        state.last_committed_seq = Some(state.last_committed_seq.map_or(seq, |last| last.max(seq)));
        state.last_committed_unix_ms = Some(now_ms);
        state.last_committed_bytes = Some(media_bytes);
        state.last_committed_duration_ms = None;
        state.record_part_available(stream_id, seq, now_unix_us(), self.window_parts);
        debug!(
            stream_id,
            sequence = seq,
            slot_id,
            bytes,
            media_bytes,
            media_kind = ?media_kind,
            "committed stream-prefixed mesh payload"
        );
        drop(state);
        self.part_updates.send_replace(seq);
        Ok(seq)
    }

    async fn retire_streams_idle_before(&self, cutoff_unix_ms: u64) -> usize {
        let candidates: Vec<u64> = {
            let state = self.state.read().await;
            state
                .stream_last_ingest_unix_ms
                .iter()
                .filter_map(|(stream_id, last_ingest_unix_ms)| {
                    (*stream_id != self.stream_id && *last_ingest_unix_ms <= cutoff_unix_ms)
                        .then_some(*stream_id)
                })
                .collect()
        };

        let mut retired = 0;
        for stream_id in candidates {
            let commit_lock = self.canonical_commit_lock(stream_id);
            let commit_guard = commit_lock.lock().await;
            let still_idle = self
                .state
                .read()
                .await
                .stream_last_ingest_unix_ms
                .get(&stream_id)
                .is_some_and(|last_ingest_unix_ms| *last_ingest_unix_ms <= cutoff_unix_ms);
            if !still_idle {
                continue;
            }

            self.chunk_cache.zero_stream_id(stream_id).await;
            self.state.write().await.forget_stream(stream_id);
            for cached_playlist in &self.playlist_cache {
                let mut cached_playlist = cached_playlist
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if cached_playlist
                    .as_ref()
                    .is_some_and(|cached| cached.stream_id == stream_id)
                {
                    *cached_playlist = None;
                }
            }
            retired += 1;
            drop(commit_guard);

            let mut commit_locks = self
                .canonical_commit_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if commit_locks
                .get(&stream_id)
                .is_some_and(|stored| Arc::ptr_eq(stored, &commit_lock))
                && Arc::strong_count(&commit_lock) == 2
            {
                commit_locks.remove(&stream_id);
            }
        }
        retired
    }

    async fn add_media_access_unit(
        &self,
        metadata: MediaFrameMetadata,
        payload: Bytes,
    ) -> Result<CachedMediaAccessUnit> {
        let stream_id = metadata.stream_id;
        let slot_id =
            usize::try_from(metadata.sequence).context("media access-unit sequence too large")?;
        if payload.len() > u32::MAX as usize {
            bail!(
                "media access-unit too large: {} bytes exceeds u32::MAX",
                payload.len()
            );
        }

        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: payload.len() as u32,
            fragment_offset: 0,
        };
        let mut serialized = Vec::with_capacity(MEDIA_FRAME_HEADER_LEN + payload.len());
        serialized.resize(MEDIA_FRAME_HEADER_LEN, 0);
        header
            .encode(&mut serialized[..MEDIA_FRAME_HEADER_LEN])
            .map_err(|err| anyhow!("media access-unit header encode failed: {err}"))?;
        serialized.extend_from_slice(&payload);
        let serialized = Bytes::from(serialized);

        self.chunk_cache
            .add_for_stream_id(stream_id, slot_id, serialized.clone())
            .await
            .map_err(|err| anyhow!("media access-unit cache write failed: {err}"))?;

        {
            let mut state = self.state.write().await;
            state.datagrams_received = state.datagrams_received.saturating_add(1);
            state.last_ingest_unix_ms = Some(now_unix_ms());
            state.bytes_received = state.bytes_received.saturating_add(payload.len() as u64);
        }

        debug!(
            stream_id,
            sequence = metadata.sequence,
            slot_id,
            payload_bytes = payload.len(),
            serialized_bytes = serialized.len(),
            codec = ?metadata.codec,
            keyframe = metadata.flags.is_keyframe(),
            "committed media access unit"
        );

        Ok(CachedMediaAccessUnit {
            metadata,
            payload_bytes: payload.len(),
            serialized,
            hash: 0,
        })
    }

    async fn get_media_access_unit(
        &self,
        stream_id: u64,
        sequence: u64,
    ) -> Option<CachedMediaAccessUnit> {
        let slot_id = usize::try_from(sequence).ok()?;
        let (serialized, hash) = self
            .chunk_cache
            .get_for_stream_id(stream_id, slot_id)
            .await?;
        if serialized.len() < MEDIA_FRAME_HEADER_LEN {
            return None;
        }
        let header = MediaFragmentHeader::decode(&serialized[..MEDIA_FRAME_HEADER_LEN]).ok()?;
        if header.metadata.stream_id != stream_id
            || header.metadata.sequence != sequence
            || header.fragment_index != 0
            || header.fragment_count != 1
            || header.fragment_offset != 0
        {
            return None;
        }
        let payload_bytes = serialized.len().checked_sub(MEDIA_FRAME_HEADER_LEN)?;
        if header.access_unit_len as usize != payload_bytes {
            return None;
        }
        Some(CachedMediaAccessUnit {
            metadata: header.metadata,
            payload_bytes,
            serialized,
            hash,
        })
    }

    async fn playlist(&self) -> String {
        self.playlist_for_stream_id(self.stream_id).await
    }

    async fn playlist_for_stream_id(&self, stream_id: u64) -> String {
        let Some((stream_idx, last)) = self.stream_position_for_id(stream_id).await else {
            let media_kind = self
                .media_kind_hint(stream_id)
                .await
                .unwrap_or(LiveMediaKind::Fmp4);
            let include_map = media_kind == LiveMediaKind::Fmp4
                && self.get_init_for_stream_id(stream_id).await.is_some();
            return self.empty_playlist(0, media_kind, include_map);
        };
        let version = self.chunk_cache.version(stream_idx).unwrap_or_default();
        if let Some(playlist) = self.cached_playlist(stream_id, stream_idx, version) {
            return playlist;
        }
        let first = last.saturating_sub(self.window_parts.saturating_sub(1));
        let mut available = Vec::new();
        let mut saw_fmp4 = false;
        let mut saw_ts = false;
        let mut discovered_init = None;
        for seq in first..=last {
            if let Some((bytes, _hash)) = self.chunk_cache.get(stream_idx, seq).await {
                let slot = LiveSlotPayload::decode_for_stream(bytes, stream_id);
                if slot.has_media() {
                    match slot.media_kind() {
                        LiveMediaKind::Fmp4 => saw_fmp4 = true,
                        LiveMediaKind::Ts => saw_ts = true,
                    }
                    if let Some(init) = slot.init() {
                        discovered_init = Some(init);
                    }
                    available.push(seq as u64);
                }
            }
        }
        if available.is_empty() {
            let media_kind = self
                .media_kind_hint(stream_id)
                .await
                .unwrap_or(LiveMediaKind::Fmp4);
            let include_map = media_kind == LiveMediaKind::Fmp4
                && self.get_init_for_stream_id(stream_id).await.is_some();
            let playlist = self.empty_playlist(last, media_kind, include_map);
            return self.cache_playlist(stream_id, stream_idx, version, playlist);
        }
        if let Some(init) = discovered_init {
            self.remember_stream_init(stream_id, init).await;
        }
        let media_kind = if saw_fmp4 {
            LiveMediaKind::Fmp4
        } else if saw_ts {
            LiveMediaKind::Ts
        } else {
            self.media_kind_hint(stream_id)
                .await
                .unwrap_or(LiveMediaKind::Fmp4)
        };
        self.remember_media_kind(stream_id, media_kind).await;
        let init = if media_kind == LiveMediaKind::Fmp4 {
            self.get_init_for_stream_id(stream_id).await
        } else {
            None
        };

        let first_available = *available.first().unwrap();
        let media_sequence = first_available / self.parts_per_segment as u64;
        let next_part = available.last().copied().unwrap_or(0) + 1;
        let part_target = self.part_target.as_secs_f64();
        let target_duration = (part_target * self.parts_per_segment as f64)
            .ceil()
            .max(1.0) as u64;

        let mut groups: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for seq in available {
            groups
                .entry(seq / self.parts_per_segment as u64)
                .or_default()
                .push(seq);
        }

        let mut out = String::new();
        out.push_str("#EXTM3U\n");
        out.push_str("#EXT-X-VERSION:9\n");
        out.push_str(&format!("#EXT-X-TARGETDURATION:{target_duration}\n"));
        out.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{media_sequence}\n"));
        if media_kind == LiveMediaKind::Fmp4 {
            if init.is_some() {
                out.push_str("#EXT-X-MAP:URI=\"init.mp4\"\n");
            } else {
                warn!(
                    stream_id,
                    "fMP4 live playlist has media fragments but no init segment"
                );
            }
        }
        out.push_str(&format!("#EXT-X-PART-INF:PART-TARGET={part_target:.3}\n"));
        out.push_str(&format!(
            "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:.3},HOLD-BACK={:.3}\n",
            part_target * 3.0,
            (part_target * self.parts_per_segment as f64 * 2.0).max(3.0)
        ));

        let extension = media_kind.extension();
        for (segment, group) in groups {
            let mut duration = 0.0;
            for seq in &group {
                duration += part_target;
                out.push_str(&format!(
                    "#EXT-X-PART:DURATION={part_target:.3},URI=\"part{seq}.{extension}\"\n"
                ));
            }
            if group.len() == self.parts_per_segment {
                out.push_str(&format!("#EXTINF:{duration:.3},\n"));
                out.push_str(&format!("seg{segment}.{extension}\n"));
            }
        }

        out.push_str(&format!(
            "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part{next_part}.{extension}\"\n"
        ));
        self.cache_playlist(stream_id, stream_idx, version, out)
    }

    fn cached_playlist(&self, stream_id: u64, stream_idx: usize, version: u64) -> Option<String> {
        let cached = self.playlist_cache.get(stream_idx)?.read().ok()?;
        cached
            .as_ref()
            .filter(|cached| cached.stream_id == stream_id && cached.version == version)
            .map(|cached| cached.body.clone())
    }

    fn cache_playlist(
        &self,
        stream_id: u64,
        stream_idx: usize,
        version: u64,
        body: String,
    ) -> String {
        if self.chunk_cache.version(stream_idx) != Some(version) {
            return body;
        }
        if let Some(cache) = self.playlist_cache.get(stream_idx) {
            if let Ok(mut cached) = cache.write() {
                *cached = Some(CachedPlaylist {
                    stream_id,
                    version,
                    body: body.clone(),
                });
            }
        }
        body
    }

    fn empty_playlist(
        &self,
        next_part: usize,
        media_kind: LiveMediaKind,
        include_map: bool,
    ) -> String {
        let part_target = self.part_target.as_secs_f64();
        let target_duration = (part_target * self.parts_per_segment as f64)
            .ceil()
            .max(1.0) as u64;
        let extension = media_kind.extension();
        let map = if include_map {
            "#EXT-X-MAP:URI=\"init.mp4\"\n"
        } else {
            ""
        };
        format!(
            "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:{target_duration}\n#EXT-X-MEDIA-SEQUENCE:0\n{map}#EXT-X-PART-INF:PART-TARGET={part_target:.3}\n#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:.3},HOLD-BACK={:.3}\n#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part{next_part}.{extension}\"\n",
            part_target * 3.0,
            (part_target * self.parts_per_segment as f64 * 2.0).max(3.0)
        )
    }

    async fn remember_media_kind(&self, stream_id: u64, media_kind: LiveMediaKind) {
        let mut state = self.state.write().await;
        state.stream_media_kinds.insert(stream_id, media_kind);
    }

    async fn media_kind_hint(&self, stream_id: u64) -> Option<LiveMediaKind> {
        let state = self.state.read().await;
        state.stream_media_kinds.get(&stream_id).copied()
    }

    async fn remember_stream_init(&self, stream_id: u64, init: Bytes) {
        {
            let mut state = self.state.write().await;
            state.stream_inits.insert(stream_id, init.clone());
        }
        if let Err(error) = self
            .chunk_cache
            .set_stream_initialization(stream_id, init)
            .await
        {
            warn!(stream_id, error, "failed to retain stream initialization");
        }
    }

    async fn get_init_for_stream_id(&self, stream_id: u64) -> Option<Bytes> {
        {
            let state = self.state.read().await;
            if let Some(init) = state.stream_inits.get(&stream_id) {
                return Some(init.clone());
            }
        }
        if let Some(init) = self.chunk_cache.stream_initialization(stream_id) {
            self.remember_stream_init(stream_id, init.clone()).await;
            return Some(init);
        }

        let (stream_idx, last) = self.stream_position_for_id(stream_id).await?;
        let first = self.chunk_cache.retained_start(last);
        for seq in (first..=last).rev() {
            let Some((bytes, _hash)) = self.chunk_cache.get(stream_idx, seq).await else {
                continue;
            };
            let slot = LiveSlotPayload::decode_for_stream(bytes, stream_id);
            if let Some(init) = slot.init() {
                self.remember_stream_init(stream_id, init.clone()).await;
                return Some(init);
            }
        }
        None
    }

    async fn stream_position(&self) -> Option<(usize, usize)> {
        self.stream_position_for_id(self.stream_id).await
    }

    async fn stream_position_for_id(&self, stream_id: u64) -> Option<(usize, usize)> {
        let stream_idx = self.chunk_cache.get_stream_idx(stream_id).await?;
        let last = self.chunk_cache.last(stream_idx)?;
        Some((stream_idx, last))
    }

    async fn get_part_for_stream_id(&self, stream_id: u64, seq: u64) -> Option<(Bytes, u64)> {
        let (bytes, hash) = self
            .chunk_cache
            .get_for_stream_id(stream_id, seq as usize)
            .await?;
        let slot = LiveSlotPayload::decode_for_stream(bytes, stream_id);
        if slot.has_media() {
            if let Some(init) = slot.init() {
                self.remember_stream_init(stream_id, init).await;
            }
            self.remember_media_kind(stream_id, slot.media_kind()).await;
            return Some((slot.media(), hash));
        }
        None
    }

    async fn part_available_unix_us(&self, stream_id: u64, seq: u64) -> Option<u64> {
        self.state
            .read()
            .await
            .stream_part_available_unix_us
            .get(&(stream_id, seq))
            .copied()
    }

    async fn next_part_after_for_stream_id(
        &self,
        stream_id: u64,
        after: Option<u64>,
        start_at_oldest: bool,
    ) -> Option<(u64, Bytes, u64)> {
        let (stream_idx, last) = self.stream_position_for_id(stream_id).await?;
        if let Some(after) = after {
            let start = after.checked_add(1)?;
            if start as usize > last {
                return None;
            }
            for seq in start as usize..=last {
                if let Some((bytes, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                    let slot = LiveSlotPayload::decode_for_stream(bytes, stream_id);
                    if slot.has_media() {
                        if let Some(init) = slot.init() {
                            self.remember_stream_init(stream_id, init).await;
                        }
                        self.remember_media_kind(stream_id, slot.media_kind()).await;
                        return Some((seq as u64, slot.media(), hash));
                    }
                }
            }
            return None;
        }

        let first = last.saturating_sub(self.window_parts.saturating_sub(1));
        let sequences: Vec<usize> = if start_at_oldest {
            (first..=last).collect()
        } else {
            (first..=last).rev().collect()
        };
        for seq in sequences {
            if let Some((bytes, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                let slot = LiveSlotPayload::decode_for_stream(bytes, stream_id);
                if slot.has_media() {
                    if let Some(init) = slot.init() {
                        self.remember_stream_init(stream_id, init).await;
                    }
                    self.remember_media_kind(stream_id, slot.media_kind()).await;
                    return Some((seq as u64, slot.media(), hash));
                }
            }
        }
        None
    }

    async fn next_part_after_blocking_for_stream_id(
        &self,
        stream_id: u64,
        after: Option<u64>,
        start_at_oldest: bool,
    ) -> Option<(u64, Bytes, u64)> {
        let deadline = Instant::now() + Duration::from_millis(LLHLS_TAIL_WAIT_MS);
        let mut updates = self.part_updates.subscribe();
        loop {
            if let Some(part) = self
                .next_part_after_for_stream_id(stream_id, after, start_at_oldest)
                .await
            {
                return Some(part);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero()
                || tokio::time::timeout(remaining, updates.changed())
                    .await
                    .is_err()
            {
                return None;
            }
        }
    }

    async fn get_part_blocking(&self, seq: u64) -> Option<(Bytes, u64)> {
        self.get_part_blocking_for_stream_id(self.stream_id, seq)
            .await
    }

    async fn get_part_blocking_for_stream_id(
        &self,
        stream_id: u64,
        seq: u64,
    ) -> Option<(Bytes, u64)> {
        let deadline = Instant::now() + Duration::from_millis(PART_WAIT_MS);
        loop {
            if let Some((bytes, hash)) = self.get_part_for_stream_id(stream_id, seq).await {
                return Some((bytes, hash));
            }
            let waiter = self
                .chunk_cache
                .exact_part_waiter(stream_id, usize::try_from(seq).ok()?)?;
            let notified = waiter.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            // Register before rechecking the cache so a commit between the
            // first lookup and this waiter cannot be missed.
            if let Some((bytes, hash)) = self.get_part_for_stream_id(stream_id, seq).await {
                return Some((bytes, hash));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero()
                || tokio::time::timeout(remaining, &mut notified)
                    .await
                    .is_err()
            {
                return None;
            }
        }
    }

    async fn get_segment(&self, segment: u64) -> Option<Bytes> {
        self.get_segment_for_stream_id(self.stream_id, segment)
            .await
    }

    async fn get_segment_for_stream_id(&self, stream_id: u64, segment: u64) -> Option<Bytes> {
        let first_part = segment.checked_mul(self.parts_per_segment as u64)?;
        let mut out = Vec::new();
        for offset in 0..self.parts_per_segment {
            let seq = first_part + offset as u64;
            let (bytes, _) = self.get_part_blocking_for_stream_id(stream_id, seq).await?;
            out.extend_from_slice(&bytes);
        }
        Some(Bytes::from(out))
    }

    async fn stats(&self, mesh: &CacheMeshHandle) -> StatsSnapshot {
        let now_ms = now_unix_ms();
        let state = self.state.read().await;
        let datagrams_received = state.datagrams_received;
        let bytes_received = state.bytes_received;
        let current_part_bytes = state.current.len();
        let latest_local_part = state.last_committed_seq;
        let latest_local_part_bytes = state.last_committed_bytes;
        let latest_local_part_duration_ms = state.last_committed_duration_ms;
        let subscription_base_object = state
            .stream_subscription_base_object
            .get(&self.stream_id)
            .copied();
        let canonical_epoch = state.stream_canonical_epoch.get(&self.stream_id).copied();
        let canonical_epoch_activation_delay_us = state
            .stream_canonical_epoch_activation_delay_us
            .get(&self.stream_id)
            .copied();
        let head_object = state
            .stream_latest_canonical_object
            .get(&self.stream_id)
            .copied();
        let latest_local_part_age_ms = state
            .last_committed_unix_ms
            .map(|last| now_ms.saturating_sub(last));
        let last_ingest_age_ms = state
            .last_ingest_unix_ms
            .map(|last| now_ms.saturating_sub(last));
        drop(state);

        let latest_mesh_part = match self.stream_position().await {
            Some((stream_idx, last)) => {
                let mut latest = None;
                for seq in (0..=last).rev().take(self.window_parts) {
                    if let Some((bytes, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                        if hash != 0 || !bytes.is_empty() {
                            latest = Some(seq as u64);
                            break;
                        }
                    }
                }
                latest
            }
            None => None,
        };
        let contiguous_object = latest_mesh_part;
        let gap_count = self
            .canonical_gap_count(self.stream_id, subscription_base_object, head_object)
            .await;

        StatsSnapshot {
            stream_id: self.stream_id,
            stream_id_text: stream_id_text(self.stream_id),
            part_target_ms: self.part_target.as_millis() as u64,
            parts_per_segment: self.parts_per_segment,
            window_parts: self.window_parts,
            datagrams_received,
            bytes_received,
            current_part_bytes,
            latest_local_part,
            latest_local_part_bytes,
            latest_local_part_duration_ms,
            latest_mesh_part,
            canonical_epoch,
            canonical_epoch_activation_delay_us,
            contiguous_object,
            head_object,
            gap_count,
            mesh_peers: mesh
                .peers()
                .await
                .into_iter()
                .map(|addr| addr.to_string())
                .collect(),
            latest_local_part_age_ms,
            last_ingest_age_ms,
        }
    }

    async fn canonical_gap_count(
        &self,
        stream_id: u64,
        subscription_base_object: Option<u64>,
        head_object: Option<u64>,
    ) -> Option<u64> {
        let base = subscription_base_object?;
        let head = head_object?;
        let retained_start = head
            .saturating_sub(self.window_parts.saturating_sub(1) as u64)
            .max(base);
        let mut gaps = 0_u64;
        for object in retained_start..=head {
            if self
                .chunk_cache
                .get_for_stream_id(stream_id, usize::try_from(object).ok()?)
                .await
                .is_none()
            {
                gaps = gaps.saturating_add(1);
            }
        }
        Some(gaps)
    }

    async fn estimated_storage_bytes(&self) -> u64 {
        let mut bytes = 0u64;
        for stream_id in self.active_stream_ids().await {
            bytes = bytes.saturating_add(self.estimated_storage_bytes_for_stream(stream_id).await);
        }
        bytes
    }

    async fn estimated_storage_bytes_for_stream(&self, stream_id: u64) -> u64 {
        let Some(stream_idx) = self.chunk_cache.get_stream_idx(stream_id).await else {
            return 0;
        };
        let Some(last) = self.chunk_cache.last(stream_idx) else {
            return 0;
        };
        self.estimated_storage_bytes_for_idx(stream_idx, last).await
    }

    async fn estimated_storage_bytes_for_idx(&self, stream_idx: usize, last: usize) -> u64 {
        let first = last.saturating_sub(self.window_parts.saturating_sub(1));
        let mut bytes = 0u64;
        for seq in first..=last {
            if let Some((payload, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                if hash != 0 || !payload.is_empty() {
                    bytes = bytes.saturating_add(payload.len() as u64);
                }
            }
        }
        bytes
    }

    async fn active_stream_ids(&self) -> Vec<u64> {
        let mut stream_ids = Vec::new();
        for (stream_id, stream_idx) in self.chunk_cache.stream_ids().await {
            let Some(last) = self.chunk_cache.last(stream_idx) else {
                continue;
            };
            let mut active = false;
            for seq in (0..=last).rev().take(self.window_parts) {
                if let Some((payload, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                    if hash != 0 || !payload.is_empty() {
                        active = true;
                        break;
                    }
                }
            }
            if active {
                stream_ids.push(stream_id);
            }
        }
        stream_ids.sort_unstable();
        stream_ids.dedup();
        stream_ids
    }

    async fn stream_telemetry(
        &self,
        node_id: &str,
        default_stats: &StatsSnapshot,
    ) -> Vec<StreamTelemetry> {
        let mut streams = Vec::new();
        let (
            canonical_epochs,
            canonical_epoch_activation_delays_us,
            subscription_bases,
            canonical_heads,
        ) = {
            let state = self.state.read().await;
            (
                state.stream_canonical_epoch.clone(),
                state.stream_canonical_epoch_activation_delay_us.clone(),
                state.stream_subscription_base_object.clone(),
                state.stream_latest_canonical_object.clone(),
            )
        };
        for (stream_id, stream_idx) in self.chunk_cache.stream_ids().await {
            let Some(last) = self.chunk_cache.last(stream_idx) else {
                continue;
            };
            let first = last.saturating_sub(self.window_parts.saturating_sub(1));
            let mut latest_part = None;
            let mut latest_part_bytes = None;
            let mut bytes_received = 0u64;
            let mut datagrams_received = 0u64;

            for seq in first..=last {
                if let Some((payload, hash)) = self.chunk_cache.get(stream_idx, seq).await {
                    if hash != 0 || !payload.is_empty() {
                        latest_part = Some(seq as u64);
                        latest_part_bytes = Some(payload.len());
                        bytes_received = bytes_received.saturating_add(payload.len() as u64);
                        datagrams_received = datagrams_received.saturating_add(1);
                    }
                }
            }

            let Some(latest_part) = latest_part else {
                continue;
            };
            let is_default_stream = stream_id == self.stream_id;
            let contiguous_object = if is_default_stream {
                default_stats.contiguous_object
            } else {
                Some(latest_part)
            };
            let canonical_epoch = if is_default_stream {
                default_stats.canonical_epoch
            } else {
                canonical_epochs.get(&stream_id).copied()
            };
            let canonical_epoch_activation_delay_us = if is_default_stream {
                default_stats.canonical_epoch_activation_delay_us
            } else {
                canonical_epoch_activation_delays_us
                    .get(&stream_id)
                    .copied()
            };
            let head_object = if is_default_stream {
                default_stats.head_object
            } else {
                canonical_heads.get(&stream_id).copied()
            };
            let gap_count = if is_default_stream {
                default_stats.gap_count
            } else {
                self.canonical_gap_count(
                    stream_id,
                    subscription_bases.get(&stream_id).copied(),
                    head_object,
                )
                .await
            };
            streams.push(StreamTelemetry {
                node_id: node_id.to_string(),
                stream_id,
                stream_id_text: stream_id_text(stream_id),
                latest_local_part: if is_default_stream {
                    default_stats.latest_local_part
                } else {
                    None
                },
                latest_local_part_bytes: if is_default_stream {
                    default_stats.latest_local_part_bytes.or(latest_part_bytes)
                } else {
                    latest_part_bytes
                },
                latest_local_part_duration_ms: if is_default_stream {
                    default_stats.latest_local_part_duration_ms
                } else {
                    None
                },
                latest_local_part_age_ms: if is_default_stream {
                    default_stats.latest_local_part_age_ms
                } else {
                    None
                },
                latest_mesh_part: Some(latest_part),
                canonical_epoch,
                canonical_epoch_activation_delay_us,
                contiguous_object,
                head_object,
                gap_count,
                bytes_received: if is_default_stream {
                    default_stats.bytes_received.max(bytes_received)
                } else {
                    bytes_received
                },
                datagrams_received: if is_default_stream {
                    default_stats.datagrams_received.max(datagrams_received)
                } else {
                    datagrams_received
                },
                last_ingest_age_ms: if is_default_stream {
                    default_stats.last_ingest_age_ms
                } else {
                    None
                },
                stale_threshold_ms: Some(stream_stale_threshold_ms(
                    default_stats.part_target_ms,
                    default_stats.window_parts,
                )),
                mesh_lag_parts: None,
            });
        }
        streams.sort_by_key(|stream| stream.stream_id);
        streams
    }

    async fn replica_request_from_slot(&self, stream_id: u64) -> usize {
        let Some(stream_idx) = self.chunk_cache.get_stream_idx(stream_id).await else {
            return 0;
        };
        let Some(last) = self.chunk_cache.last(stream_idx) else {
            return 0;
        };

        let retained_start = self.chunk_cache.retained_start(last);
        let mut latest_present = None;
        for seq in retained_start..=last {
            let present = self
                .chunk_cache
                .get(stream_idx, seq)
                .await
                .map(|(bytes, hash)| hash != 0 || !bytes.is_empty())
                .unwrap_or(false);
            if present {
                latest_present = Some(seq);
            } else {
                return seq;
            }
        }
        latest_present
            .map(|seq| seq.saturating_add(1))
            .unwrap_or(retained_start)
    }

    async fn mesh_snapshot(
        &self,
        mesh: &CacheMeshHandle,
        mut node: MeshNode,
        policy: ReplicationPolicy,
        control: &ControlPlane,
    ) -> MeshSnapshot {
        let stats = self.stats(mesh).await;
        let streams = self.stream_telemetry(&node.node_id, &stats).await;
        node.used_storage_bytes = self
            .estimated_storage_bytes()
            .await
            .min(node.total_storage_bytes);
        node.active_streams = streams.len() as u64;
        node.contributor_streams = streams
            .iter()
            .filter(|stream| stream.latest_local_part.is_some())
            .count() as u64;

        MeshSnapshot {
            updated_unix_ms: now_unix_ms(),
            node,
            mesh_addr: Some(mesh.local_addr().to_string()),
            edge_service: None,
            relay_session: self.relay_ingress_snapshot(),
            peers: stats
                .mesh_peers
                .iter()
                .map(|addr| PeerSnapshot {
                    addr: addr.clone(),
                    state: "discovered".into(),
                })
                .collect(),
            stream: stats,
            streams,
            replication_policy: policy,
            recent_commands: control.recent().await,
        }
    }
}

struct LiveState {
    process_started_unix_us: u64,
    current: Vec<u8>,
    current_started: Instant,
    current_started_unix_ms: u64,
    next_seq: u64,
    datagrams_received: u64,
    bytes_received: u64,
    last_ingest_unix_ms: Option<u64>,
    last_committed_seq: Option<u64>,
    last_committed_unix_ms: Option<u64>,
    last_committed_bytes: Option<usize>,
    last_committed_duration_ms: Option<u64>,
    stream_next_seq: HashMap<u64, u64>,
    stream_canonical_epoch: HashMap<u64, u64>,
    stream_canonical_epoch_activation_delay_us: HashMap<u64, u64>,
    stream_subscription_base_object: HashMap<u64, u64>,
    stream_latest_canonical_object: HashMap<u64, u64>,
    stream_last_ingest_unix_ms: HashMap<u64, u64>,
    stream_part_available_unix_us: HashMap<(u64, u64), u64>,
    stream_inits: HashMap<u64, Bytes>,
    stream_media_kinds: HashMap<u64, LiveMediaKind>,
}

impl LiveState {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            process_started_unix_us: now_unix_us(),
            current: Vec::new(),
            current_started: now,
            current_started_unix_ms: now_unix_ms(),
            next_seq: 0,
            datagrams_received: 0,
            bytes_received: 0,
            last_ingest_unix_ms: None,
            last_committed_seq: None,
            last_committed_unix_ms: None,
            last_committed_bytes: None,
            last_committed_duration_ms: None,
            stream_next_seq: HashMap::new(),
            stream_canonical_epoch: HashMap::new(),
            stream_canonical_epoch_activation_delay_us: HashMap::new(),
            stream_subscription_base_object: HashMap::new(),
            stream_latest_canonical_object: HashMap::new(),
            stream_last_ingest_unix_ms: HashMap::new(),
            stream_part_available_unix_us: HashMap::new(),
            stream_inits: HashMap::new(),
            stream_media_kinds: HashMap::new(),
        }
    }

    fn next_stream_seq(&mut self, stream_id: u64) -> u64 {
        let next = self.stream_next_seq.entry(stream_id).or_insert(0);
        let seq = *next;
        *next = next.saturating_add(1);
        seq
    }

    fn observe_stream_seq(&mut self, stream_id: u64, sequence: u64) {
        let next = self.stream_next_seq.entry(stream_id).or_insert(0);
        *next = (*next).max(sequence.saturating_add(1));
    }

    fn record_part_available(
        &mut self,
        stream_id: u64,
        sequence: u64,
        available_unix_us: u64,
        window_parts: usize,
    ) {
        self.stream_part_available_unix_us
            .entry((stream_id, sequence))
            .or_insert(available_unix_us);
        let oldest_retained = sequence.saturating_sub(window_parts.saturating_sub(1) as u64);
        self.stream_part_available_unix_us
            .retain(|(retained_stream, retained_sequence), _| {
                *retained_stream != stream_id || *retained_sequence >= oldest_retained
            });
    }

    fn forget_stream(&mut self, stream_id: u64) {
        self.stream_next_seq.remove(&stream_id);
        self.stream_canonical_epoch.remove(&stream_id);
        self.stream_canonical_epoch_activation_delay_us
            .remove(&stream_id);
        self.stream_subscription_base_object.remove(&stream_id);
        self.stream_latest_canonical_object.remove(&stream_id);
        self.stream_last_ingest_unix_ms.remove(&stream_id);
        self.stream_part_available_unix_us
            .retain(|(retained_stream, _), _| *retained_stream != stream_id);
        self.stream_inits.remove(&stream_id);
        self.stream_media_kinds.remove(&stream_id);
    }

    fn take_current(&mut self, now: Instant, now_ms: u64) -> Option<PendingPart> {
        if self.current.is_empty() {
            self.current_started = now;
            self.current_started_unix_ms = now_ms;
            return None;
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        let data = std::mem::take(&mut self.current);
        let duration_ms = now
            .duration_since(self.current_started)
            .as_millis()
            .max(1)
            .min(u128::from(u64::MAX)) as u64;
        let part = PendingPart {
            seq,
            bytes: data.len(),
            duration_ms,
            committed_unix_ms: now_ms,
            data,
        };
        self.current_started = now;
        self.current_started_unix_ms = now_ms;
        Some(part)
    }
}

struct PendingPart {
    seq: u64,
    bytes: usize,
    duration_ms: u64,
    committed_unix_ms: u64,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatsSnapshot {
    stream_id: u64,
    #[serde(default)]
    stream_id_text: String,
    part_target_ms: u64,
    parts_per_segment: usize,
    window_parts: usize,
    datagrams_received: u64,
    bytes_received: u64,
    current_part_bytes: usize,
    latest_local_part: Option<u64>,
    latest_local_part_bytes: Option<usize>,
    latest_local_part_duration_ms: Option<u64>,
    latest_mesh_part: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    canonical_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    canonical_epoch_activation_delay_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    contiguous_object: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    head_object: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    gap_count: Option<u64>,
    mesh_peers: Vec<String>,
    latest_local_part_age_ms: Option<u64>,
    last_ingest_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MeshSnapshot {
    updated_unix_ms: u64,
    node: MeshNode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mesh_addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    edge_service: Option<EdgeServiceSnapshot>,
    #[serde(default)]
    relay_session: RelaySessionIngressSnapshot,
    peers: Vec<PeerSnapshot>,
    stream: StatsSnapshot,
    #[serde(default)]
    streams: Vec<StreamTelemetry>,
    replication_policy: ReplicationPolicy,
    recent_commands: Vec<ControlCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeerSnapshot {
    addr: String,
    state: String,
}

#[derive(Debug, Clone, Serialize)]
struct MeshApiSnapshot {
    updated_unix_ms: u64,
    node: MeshNode,
    mesh_transport: MeshTransportConfigSnapshot,
    mesh_fec: MeshFecRuntimeSnapshot,
    relay_session: RelaySessionIngressSnapshot,
    relay_nodes: Vec<RelayNodeSessionSnapshot>,
    peers: Vec<PeerSnapshot>,
    stream: StatsSnapshot,
    replication_policy: ReplicationPolicy,
    recent_commands: Vec<ControlCommand>,
    planned_replicas: Vec<ReplicaPlacementSnapshot>,
    aggregate: AggregateMetrics,
    alerts: Vec<MeshAlert>,
    activity: Vec<MeshActivity>,
    telemetry: TelemetryHealthSnapshot,
    orchestration: OrchestrationStatus,
    topology: TopologyConfidenceSnapshot,
    nodes: Vec<MeshNode>,
    edge_services: Vec<EdgeServiceSnapshot>,
    connections: Vec<ConnectionSnapshot>,
    streams: Vec<StreamTelemetry>,
}

#[derive(Debug, Clone, Serialize)]
struct RelayNodeSessionSnapshot {
    node_id: String,
    region: String,
    relay_session: RelaySessionIngressSnapshot,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
struct RelaySessionIngressSnapshot {
    primary_sessions: u64,
    secondary_sessions: u64,
    authenticated_sessions: u64,
    controlled_sessions: u64,
    active_objects: u64,
    completed_objects: u64,
    active_object_bytes: u64,
    buffered_datagrams: u64,
    datagrams_received: u64,
    datagrams_rejected: u64,
    source_datagrams: u64,
    repair_datagrams: u64,
    duplicate_datagrams: u64,
    decoded_objects: u64,
    #[serde(alias = "repaired_objects")]
    repair_assisted_objects: u64,
    fec_recovered_objects: u64,
    fec_recovered_source_symbols: u64,
    expired_objects: u64,
    conflict_drops: u64,
    authentication_drops: u64,
    deadline_drops: u64,
    downstream_children: u64,
    forwarded_source_datagrams: u64,
    forwarded_repair_datagrams: u64,
    forwarded_bytes: u64,
    forward_errors: u64,
    forward_filtered_datagrams: u64,
    warm_source_buffered_datagrams: u64,
    warm_source_buffered_bytes: u64,
    warm_source_replayed_datagrams: u64,
    warm_source_replayed_bytes: u64,
    warm_source_expired_datagrams: u64,
    warm_source_retired_datagrams: u64,
    warm_source_evicted_datagrams: u64,
    processing_duration_count: u64,
    processing_duration_sum_us: u64,
    processing_duration_max_us: u64,
    processing_duration_buckets: [u64; EDGE_RESPONSE_DURATION_BUCKETS_US.len()],
    forward_duration_count: u64,
    forward_duration_sum_us: u64,
    forward_duration_max_us: u64,
    forward_duration_buckets: [u64; EDGE_RESPONSE_DURATION_BUCKETS_US.len()],
    publication_to_available_count: u64,
    publication_to_available_sum_us: u64,
    publication_to_available_max_us: u64,
    publication_to_available_buckets: [u64; PUBLICATION_AVAILABILITY_BUCKETS_US.len()],
    publication_clock_error_max_us: u64,
    publication_clock_unusable_objects: u64,
    failover_controller_state: RelayFailoverControllerState,
    failover_controller_enabled: u64,
    failover_commands_sent: u64,
    failover_command_send_errors: u64,
    failover_promotions: u64,
    failover_demotions: u64,
    failover_secondary_unavailable_events: u64,
    failover_primary_source_age_ms: u64,
    failover_secondary_repair_age_ms: u64,
    failover_last_detection_us: u64,
    failover_last_promotion_to_source_us: u64,
    failover_last_media_gap_us: u64,
    failover_max_media_gap_us: u64,
    failover_controller_last_transition_unix_ms: u64,
    failover_listeners: u64,
    failover_promoted_children: u64,
    failover_commands_received: u64,
    failover_commands_rejected: u64,
    failover_lease_expirations: u64,
    failover_promotions_applied: u64,
    failover_demotions_applied: u64,
    failover_listener_last_transition_unix_ms: u64,
}

impl RelaySessionIngressSnapshot {
    fn forward_snapshot(self) -> RelayForwardSnapshot {
        RelayForwardSnapshot {
            downstream_children: self.downstream_children,
            source_datagrams: self.forwarded_source_datagrams,
            repair_datagrams: self.forwarded_repair_datagrams,
            bytes: self.forwarded_bytes,
            errors: self.forward_errors,
            filtered_datagrams: self.forward_filtered_datagrams,
            warm_source_buffered_datagrams: self.warm_source_buffered_datagrams,
            warm_source_buffered_bytes: self.warm_source_buffered_bytes,
            warm_source_replayed_datagrams: self.warm_source_replayed_datagrams,
            warm_source_replayed_bytes: self.warm_source_replayed_bytes,
            warm_source_expired_datagrams: self.warm_source_expired_datagrams,
            warm_source_retired_datagrams: self.warm_source_retired_datagrams,
            warm_source_evicted_datagrams: self.warm_source_evicted_datagrams,
            duration_count: self.forward_duration_count,
            duration_sum_us: self.forward_duration_sum_us,
            duration_max_us: self.forward_duration_max_us,
            duration_buckets: self.forward_duration_buckets,
            failover_listeners: self.failover_listeners,
            failover_promoted_children: self.failover_promoted_children,
            failover_commands_received: self.failover_commands_received,
            failover_commands_rejected: self.failover_commands_rejected,
            failover_lease_expirations: self.failover_lease_expirations,
            failover_promotions_applied: self.failover_promotions_applied,
            failover_demotions_applied: self.failover_demotions_applied,
            failover_last_transition_unix_ms: self.failover_listener_last_transition_unix_ms,
        }
    }

    fn apply_forward_snapshot(&mut self, snapshot: RelayForwardSnapshot) {
        self.downstream_children = snapshot.downstream_children;
        self.forwarded_source_datagrams = snapshot.source_datagrams;
        self.forwarded_repair_datagrams = snapshot.repair_datagrams;
        self.forwarded_bytes = snapshot.bytes;
        self.forward_errors = snapshot.errors;
        self.forward_filtered_datagrams = snapshot.filtered_datagrams;
        self.warm_source_buffered_datagrams = snapshot.warm_source_buffered_datagrams;
        self.warm_source_buffered_bytes = snapshot.warm_source_buffered_bytes;
        self.warm_source_replayed_datagrams = snapshot.warm_source_replayed_datagrams;
        self.warm_source_replayed_bytes = snapshot.warm_source_replayed_bytes;
        self.warm_source_expired_datagrams = snapshot.warm_source_expired_datagrams;
        self.warm_source_retired_datagrams = snapshot.warm_source_retired_datagrams;
        self.warm_source_evicted_datagrams = snapshot.warm_source_evicted_datagrams;
        self.forward_duration_count = snapshot.duration_count;
        self.forward_duration_sum_us = snapshot.duration_sum_us;
        self.forward_duration_max_us = snapshot.duration_max_us;
        self.forward_duration_buckets = snapshot.duration_buckets;
        self.failover_listeners = snapshot.failover_listeners;
        self.failover_promoted_children = snapshot.failover_promoted_children;
        self.failover_commands_received = snapshot.failover_commands_received;
        self.failover_commands_rejected = snapshot.failover_commands_rejected;
        self.failover_lease_expirations = snapshot.failover_lease_expirations;
        self.failover_promotions_applied = snapshot.failover_promotions_applied;
        self.failover_demotions_applied = snapshot.failover_demotions_applied;
        self.failover_listener_last_transition_unix_ms = snapshot.failover_last_transition_unix_ms;
    }

    fn apply_failover_controller_snapshot(&mut self, snapshot: RelayFailoverControllerSnapshot) {
        self.failover_controller_state = snapshot.state;
        self.failover_controller_enabled = snapshot.enabled;
        self.failover_commands_sent = snapshot.commands_sent;
        self.failover_command_send_errors = snapshot.command_send_errors;
        self.failover_promotions = snapshot.promotions;
        self.failover_demotions = snapshot.demotions;
        self.failover_secondary_unavailable_events = snapshot.secondary_unavailable_events;
        self.failover_primary_source_age_ms = snapshot.primary_source_age_ms;
        self.failover_secondary_repair_age_ms = snapshot.secondary_repair_age_ms;
        self.failover_last_detection_us = snapshot.last_detection_us;
        self.failover_last_promotion_to_source_us = snapshot.last_promotion_to_source_us;
        self.failover_last_media_gap_us = snapshot.last_media_gap_us;
        self.failover_max_media_gap_us = snapshot.max_media_gap_us;
        self.failover_controller_last_transition_unix_ms = snapshot.last_transition_unix_ms;
    }
}

impl From<RelayIngressSnapshot> for RelaySessionIngressSnapshot {
    fn from(snapshot: RelayIngressSnapshot) -> Self {
        Self {
            primary_sessions: snapshot.primary_sessions as u64,
            secondary_sessions: snapshot.secondary_sessions as u64,
            authenticated_sessions: snapshot.authenticated_sessions as u64,
            controlled_sessions: snapshot.controlled_sessions as u64,
            active_objects: snapshot.active_objects as u64,
            completed_objects: snapshot.completed_objects as u64,
            active_object_bytes: snapshot.active_object_bytes as u64,
            buffered_datagrams: snapshot.buffered_datagrams as u64,
            datagrams_received: snapshot.counters.datagrams_received,
            datagrams_rejected: snapshot.counters.datagrams_rejected,
            source_datagrams: snapshot.counters.source_datagrams,
            repair_datagrams: snapshot.counters.repair_datagrams,
            duplicate_datagrams: snapshot.counters.duplicate_datagrams,
            decoded_objects: snapshot.counters.decoded_objects,
            repair_assisted_objects: snapshot.counters.repair_assisted_objects,
            fec_recovered_objects: snapshot.counters.fec_recovered_objects,
            fec_recovered_source_symbols: snapshot.counters.fec_recovered_source_symbols,
            expired_objects: snapshot.counters.expired_objects,
            conflict_drops: snapshot.counters.conflict_drops,
            authentication_drops: snapshot.counters.authentication_drops,
            deadline_drops: snapshot.counters.deadline_drops,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
struct MeshFecRuntimeSnapshot {
    tx_objects: u64,
    tx_protected_bytes: u64,
    tx_source_datagrams: u64,
    tx_repair_datagrams: u64,
    tx_wire_bytes: u64,
    tx_errors: u64,
    rx_wire_datagrams: u64,
    rx_wire_bytes: u64,
    rx_source_datagrams: u64,
    rx_repair_datagrams: u64,
    rx_decoded_objects: u64,
    rx_decoded_bytes: u64,
    rx_repaired_objects: u64,
    rx_repaired_source_datagrams: u64,
    rx_late_source_datagrams: u64,
    rx_presumed_lost_source_datagrams: u64,
    rx_decode_errors: u64,
    rx_expired_objects: u64,
    rx_inflight_objects: u64,
}

impl From<CacheMeshFecStats> for MeshFecRuntimeSnapshot {
    fn from(stats: CacheMeshFecStats) -> Self {
        Self {
            tx_objects: stats.tx_objects,
            tx_protected_bytes: stats.tx_protected_bytes,
            tx_source_datagrams: stats.tx_source_datagrams,
            tx_repair_datagrams: stats.tx_repair_datagrams,
            tx_wire_bytes: stats.tx_wire_bytes,
            tx_errors: stats.tx_errors,
            rx_wire_datagrams: stats.rx_wire_datagrams,
            rx_wire_bytes: stats.rx_wire_bytes,
            rx_source_datagrams: stats.rx_source_datagrams,
            rx_repair_datagrams: stats.rx_repair_datagrams,
            rx_decoded_objects: stats.rx_decoded_objects,
            rx_decoded_bytes: stats.rx_decoded_bytes,
            rx_repaired_objects: stats.rx_repaired_objects,
            rx_repaired_source_datagrams: stats.rx_repaired_source_datagrams,
            rx_late_source_datagrams: stats.rx_late_source_datagrams,
            rx_presumed_lost_source_datagrams: stats.rx_presumed_lost_source_datagrams,
            rx_decode_errors: stats.rx_decode_errors,
            rx_expired_objects: stats.rx_expired_objects,
            rx_inflight_objects: stats.rx_inflight_objects,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct MeshTransportConfigSnapshot {
    sync_interval_ms: u64,
    min_repair_symbols: u32,
    repair_ratio: f32,
    max_repair_symbols: u32,
    symbol_size: u16,
}

impl Default for MeshTransportConfigSnapshot {
    fn default() -> Self {
        Self {
            sync_interval_ms: DEFAULT_MESH_SYNC_INTERVAL_MS,
            min_repair_symbols: DEFAULT_MESH_FEC_REPAIR_SYMBOLS,
            repair_ratio: DEFAULT_MESH_FEC_REPAIR_RATIO,
            max_repair_symbols: DEFAULT_MESH_FEC_MAX_REPAIR_SYMBOLS,
            symbol_size: DEFAULT_MESH_FEC_SYMBOL_SIZE,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct TelemetryPeerStatus {
    peer: String,
    state: String,
    connect_attempts: u64,
    disconnects: u64,
    payloads: u64,
    bytes: u64,
    last_connected_unix_ms: Option<u64>,
    last_payload_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct ReplicaPlacementSnapshot {
    stream_id: u64,
    stream_id_text: String,
    target_node_id: String,
    reason: ReplicaReason,
    reason_text: String,
    score: f64,
}

impl From<ReplicaPlacement> for ReplicaPlacementSnapshot {
    fn from(placement: ReplicaPlacement) -> Self {
        let reason_text = replica_reason_text(&placement.reason);
        Self {
            stream_id: placement.stream_id,
            stream_id_text: stream_id_text(placement.stream_id),
            target_node_id: placement.target_node_id,
            reason: placement.reason,
            reason_text,
            score: placement.score,
        }
    }
}

fn replica_reason_text(reason: &ReplicaReason) -> String {
    match reason {
        ReplicaReason::BaselineRegion { region } => format!("baseline region {region}"),
        ReplicaReason::BaselineContinent { continent } => {
            format!("baseline continent {continent}")
        }
        ReplicaReason::DemandRegion { region } => format!("demand region {region}"),
        ReplicaReason::DemandContinent { continent } => format!("demand continent {continent}"),
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct AggregateMetrics {
    node_count: usize,
    connection_count: usize,
    total_storage_bytes: u64,
    used_storage_bytes: u64,
    total_egress_capacity_bps: u64,
    contributor_streams: u64,
    active_streams: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct TelemetryHealthSnapshot {
    stale_after_ms: u64,
    fresh_remote_count: usize,
    stale_remote_count: usize,
    stale_nodes: Vec<TelemetryNodeHealth>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TelemetryNodeHealth {
    node_id: String,
    region: String,
    updated_unix_ms: u64,
    age_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct OrchestrationStatus {
    control_dispatch_ready: bool,
    provision: ProvisionStatus,
    telemetry_peers: Vec<TelemetryPeerStatus>,
    private_discovery: PrivateDiscoveryStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct ProvisionStatus {
    enabled: bool,
    backends: Vec<String>,
    timeout_ms: u64,
    backend_statuses: Vec<ProvisionBackendStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct ProvisionBackendStatus {
    name: String,
    state: &'static str,
    details: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct PrivateDiscoveryStatus {
    compiled: bool,
    enabled: bool,
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    broadcast_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mesh_port: Option<u16>,
    details: Vec<String>,
}

impl Default for PrivateDiscoveryStatus {
    fn default() -> Self {
        Self::unavailable()
    }
}

impl PrivateDiscoveryStatus {
    #[cfg(feature = "private-subnet-discovery")]
    fn from_args(enabled: bool, broadcast_port: u16, mesh_port: u16) -> Self {
        if enabled {
            Self {
                compiled: true,
                enabled: true,
                state: "listening",
                broadcast_port: Some(broadcast_port),
                mesh_port: Some(mesh_port),
                details: vec![
                    format!("udp-broadcast://0.0.0.0:{broadcast_port}"),
                    format!("mesh-port={mesh_port}"),
                ],
            }
        } else {
            Self {
                compiled: true,
                enabled: false,
                state: "available",
                broadcast_port: None,
                mesh_port: None,
                details: vec!["pass --private-subnet-discovery to discover VLAN peers".into()],
            }
        }
    }

    fn unavailable() -> Self {
        Self {
            compiled: false,
            enabled: false,
            state: "unavailable",
            broadcast_port: None,
            mesh_port: None,
            details: vec!["build with --features private-subnet-discovery".into()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct MeshAlert {
    level: &'static str,
    code: &'static str,
    message: String,
    count: u64,
    last_seen_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_id_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct MeshActivity {
    level: &'static str,
    code: String,
    message: String,
    count: u64,
    seen_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_id_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ConnectionSnapshot {
    source_node_id: String,
    target_addr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_node_id: Option<String>,
    state: String,
    private_target: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct TopologyConfidenceSnapshot {
    connection_count: usize,
    resolved_peer_count: usize,
    unresolved_peer_count: usize,
    private_peer_count: usize,
    public_peer_count: usize,
}

impl TopologyConfidenceSnapshot {
    fn from_connections(connections: &[ConnectionSnapshot]) -> Self {
        let connection_count = connections.len();
        let resolved_peer_count = connections
            .iter()
            .filter(|connection| connection.target_node_id.is_some())
            .count();
        let private_peer_count = connections
            .iter()
            .filter(|connection| connection.private_target)
            .count();
        Self {
            connection_count,
            resolved_peer_count,
            unresolved_peer_count: connection_count.saturating_sub(resolved_peer_count),
            private_peer_count,
            public_peer_count: connection_count.saturating_sub(private_peer_count),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct EdgeServiceSnapshot {
    node_id: String,
    region: String,
    continent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    playback_base_url: Option<String>,
    active_readers: u64,
    requests_served: u64,
    bytes_served: u64,
    llhls_tail_requests: u64,
    #[serde(default)]
    responses_total: u64,
    #[serde(default)]
    response_errors: u64,
    #[serde(default)]
    response_not_found: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_response_unix_ms: Option<u64>,
    #[serde(default)]
    response_duration_count: u64,
    #[serde(default)]
    response_duration_sum_us: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response_duration_p95_us: Option<u64>,
    #[serde(default)]
    response_duration_buckets: Vec<u64>,
    #[serde(default)]
    recent_responses: Vec<EdgeResponseSnapshot>,
    draining: bool,
}

impl EdgeServiceSnapshot {
    fn from_node(
        node: &MeshNode,
        playback_base_url: Option<String>,
        load: EdgeLoadSnapshot,
    ) -> Self {
        Self {
            node_id: node.node_id.clone(),
            region: node.region.clone(),
            continent: node.continent.clone(),
            playback_base_url,
            active_readers: load.active_readers,
            requests_served: load.requests_served,
            bytes_served: load.bytes_served,
            llhls_tail_requests: load.llhls_tail_requests,
            responses_total: load.responses_total,
            response_errors: load.response_errors,
            response_not_found: load.response_not_found,
            last_response_unix_ms: load.last_response_unix_ms,
            response_duration_count: load.response_duration_count,
            response_duration_sum_us: load.response_duration_sum_us,
            response_duration_p95_us: load.response_duration_p95_us,
            response_duration_buckets: load.response_duration_buckets,
            recent_responses: load.recent_responses,
            draining: node.draining,
        }
    }

    fn fallback_for_node(node: &MeshNode) -> Self {
        Self::from_node(node, None, EdgeLoadSnapshot::default())
    }
}

#[derive(Debug, Clone, Default)]
struct EdgeLoadSnapshot {
    active_readers: u64,
    requests_served: u64,
    bytes_served: u64,
    llhls_tail_requests: u64,
    responses_total: u64,
    response_errors: u64,
    response_not_found: u64,
    last_response_unix_ms: Option<u64>,
    response_duration_count: u64,
    response_duration_sum_us: u64,
    response_duration_p95_us: Option<u64>,
    response_duration_buckets: Vec<u64>,
    recent_responses: Vec<EdgeResponseSnapshot>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct EdgeResponseSnapshot {
    unix_ms: u64,
    method: String,
    path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    status: u16,
    bytes: u64,
    #[serde(default)]
    duration_us: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct EdgeLoad {
    inner: Arc<EdgeLoadInner>,
}

#[derive(Debug, Default)]
struct EdgeLoadInner {
    active_readers: AtomicU64,
    requests_served: AtomicU64,
    bytes_served: AtomicU64,
    llhls_tail_requests: AtomicU64,
    responses_total: AtomicU64,
    response_errors: AtomicU64,
    response_not_found: AtomicU64,
    last_response_unix_ms: AtomicU64,
    response_duration: AtomicDurationHistogram,
    recent_responses: StdMutex<VecDeque<EdgeResponseSnapshot>>,
}

#[derive(Debug)]
struct AtomicDurationHistogram {
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
    buckets: [AtomicU64; EDGE_RESPONSE_DURATION_BUCKETS_US.len()],
}

impl Default for AtomicDurationHistogram {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl AtomicDurationHistogram {
    fn record(&self, duration: Duration) -> u64 {
        let duration_us = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.record_us(duration_us);
        duration_us
    }

    fn record_us(&self, duration_us: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(duration_us, Ordering::Relaxed);
        self.max_us.fetch_max(duration_us, Ordering::Relaxed);
        for (index, upper_bound_us) in EDGE_RESPONSE_DURATION_BUCKETS_US.iter().enumerate() {
            if duration_us <= *upper_bound_us {
                self.buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Debug, Default)]
struct RelayAvailabilityTelemetry {
    duration: AtomicPublicationDurationHistogram,
    clock_error_max_us: AtomicU64,
    unusable_clock_objects: AtomicU64,
}

#[derive(Debug)]
struct AtomicPublicationDurationHistogram {
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
    buckets: [AtomicU64; PUBLICATION_AVAILABILITY_BUCKETS_US.len()],
}

impl Default for AtomicPublicationDurationHistogram {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl AtomicPublicationDurationHistogram {
    fn record_us(&self, duration_us: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(duration_us, Ordering::Relaxed);
        self.max_us.fetch_max(duration_us, Ordering::Relaxed);
        for (index, upper_bound_us) in PUBLICATION_AVAILABILITY_BUCKETS_US.iter().enumerate() {
            if duration_us <= *upper_bound_us {
                self.buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl RelayAvailabilityTelemetry {
    fn record(&self, observation: RelayAvailabilityObservation) {
        match observation {
            RelayAvailabilityObservation::Measured {
                duration_us,
                clock_error_us,
            } => {
                self.duration.record_us(duration_us);
                self.clock_error_max_us
                    .fetch_max(clock_error_us, Ordering::Relaxed);
            }
            RelayAvailabilityObservation::UnusableClock => {
                self.unusable_clock_objects.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn apply_to(&self, snapshot: &mut RelaySessionIngressSnapshot) {
        snapshot.publication_to_available_count = self.duration.count.load(Ordering::Relaxed);
        snapshot.publication_to_available_sum_us = self.duration.sum_us.load(Ordering::Relaxed);
        snapshot.publication_to_available_max_us = self.duration.max_us.load(Ordering::Relaxed);
        snapshot.publication_to_available_buckets =
            std::array::from_fn(|index| self.duration.buckets[index].load(Ordering::Relaxed));
        snapshot.publication_clock_error_max_us = self.clock_error_max_us.load(Ordering::Relaxed);
        snapshot.publication_clock_unusable_objects =
            self.unusable_clock_objects.load(Ordering::Relaxed);
    }
}

fn histogram_percentile_upper_bound_us(
    count: u64,
    buckets: &[u64],
    percentile: u64,
    max_us: u64,
) -> Option<u64> {
    if count == 0 {
        return None;
    }
    let rank = count.saturating_mul(percentile).saturating_add(99) / 100;
    buckets
        .iter()
        .enumerate()
        .find(|(_, bucket_count)| **bucket_count >= rank)
        .map(|(index, _)| EDGE_RESPONSE_DURATION_BUCKETS_US[index])
        .or(Some(max_us))
}

impl EdgeLoad {
    fn begin_read(&self, llhls_tail: bool) -> EdgeReadGuard {
        self.inner.active_readers.fetch_add(1, Ordering::Relaxed);
        self.inner.requests_served.fetch_add(1, Ordering::Relaxed);
        if llhls_tail {
            self.inner
                .llhls_tail_requests
                .fetch_add(1, Ordering::Relaxed);
        }
        EdgeReadGuard {
            load: self.clone(),
            finished: false,
        }
    }

    fn snapshot(&self, node: &MeshNode, playback_base_url: Option<String>) -> EdgeServiceSnapshot {
        let recent_responses = self
            .inner
            .recent_responses
            .lock()
            .map(|responses| responses.iter().cloned().collect())
            .unwrap_or_default();
        let last_response_unix_ms = match self.inner.last_response_unix_ms.load(Ordering::Relaxed) {
            0 => None,
            value => Some(value),
        };
        let response_duration_count = self.inner.response_duration.count.load(Ordering::Relaxed);
        let response_duration_buckets = self
            .inner
            .response_duration
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        let response_duration_p95_us = histogram_percentile_upper_bound_us(
            response_duration_count,
            &response_duration_buckets,
            95,
            self.inner.response_duration.max_us.load(Ordering::Relaxed),
        );
        EdgeServiceSnapshot::from_node(
            node,
            playback_base_url,
            EdgeLoadSnapshot {
                active_readers: self.inner.active_readers.load(Ordering::Relaxed),
                requests_served: self.inner.requests_served.load(Ordering::Relaxed),
                bytes_served: self.inner.bytes_served.load(Ordering::Relaxed),
                llhls_tail_requests: self.inner.llhls_tail_requests.load(Ordering::Relaxed),
                responses_total: self.inner.responses_total.load(Ordering::Relaxed),
                response_errors: self.inner.response_errors.load(Ordering::Relaxed),
                response_not_found: self.inner.response_not_found.load(Ordering::Relaxed),
                last_response_unix_ms,
                response_duration_count,
                response_duration_sum_us: self
                    .inner
                    .response_duration
                    .sum_us
                    .load(Ordering::Relaxed),
                response_duration_p95_us,
                response_duration_buckets,
                recent_responses,
            },
        )
    }

    fn record_response(
        &self,
        method: &Method,
        path: &str,
        query: Option<&str>,
        response: &HandlerResponse,
        duration: Duration,
    ) {
        let unix_ms = now_unix_ms();
        let status = response.status.as_u16();
        let bytes = response
            .body
            .as_ref()
            .map(|body| body.len() as u64)
            .unwrap_or(0);
        let duration_us = self.inner.response_duration.record(duration);
        self.inner.responses_total.fetch_add(1, Ordering::Relaxed);
        if response.status.is_client_error() || response.status.is_server_error() {
            self.inner.response_errors.fetch_add(1, Ordering::Relaxed);
        }
        if response.status == StatusCode::NOT_FOUND {
            self.inner
                .response_not_found
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner
            .last_response_unix_ms
            .store(unix_ms, Ordering::Relaxed);

        if let Ok(mut responses) = self.inner.recent_responses.lock() {
            responses.push_front(EdgeResponseSnapshot {
                unix_ms,
                method: method.as_str().into(),
                path: path.into(),
                query: query.map(ToOwned::to_owned),
                status,
                bytes,
                duration_us,
                content_type: response.content_type.clone().map(Into::into),
            });
            while responses.len() > EDGE_RECENT_RESPONSE_LIMIT {
                responses.pop_back();
            }
        }
    }
}

struct EdgeReadGuard {
    load: EdgeLoad,
    finished: bool,
}

impl EdgeReadGuard {
    fn finish(mut self, bytes_served: usize) {
        self.load
            .inner
            .bytes_served
            .fetch_add(bytes_served as u64, Ordering::Relaxed);
        self.finished = true;
    }
}

impl Drop for EdgeReadGuard {
    fn drop(&mut self) {
        self.load
            .inner
            .active_readers
            .fetch_sub(1, Ordering::Relaxed);
        if !self.finished {
            self.load.inner.bytes_served.fetch_add(0, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamTelemetry {
    node_id: String,
    stream_id: u64,
    #[serde(default)]
    stream_id_text: String,
    latest_local_part: Option<u64>,
    latest_local_part_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_local_part_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_local_part_age_ms: Option<u64>,
    latest_mesh_part: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    canonical_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    canonical_epoch_activation_delay_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    contiguous_object: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    head_object: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    gap_count: Option<u64>,
    bytes_received: u64,
    datagrams_received: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_ingest_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stale_threshold_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mesh_lag_parts: Option<u64>,
}

impl StreamTelemetry {
    fn from_stats(node_id: String, stats: &StatsSnapshot) -> Self {
        Self {
            node_id,
            stream_id: stats.stream_id,
            stream_id_text: stream_id_text(stats.stream_id),
            latest_local_part: stats.latest_local_part,
            latest_local_part_bytes: stats.latest_local_part_bytes,
            latest_local_part_duration_ms: stats.latest_local_part_duration_ms,
            latest_local_part_age_ms: stats.latest_local_part_age_ms,
            latest_mesh_part: stats.latest_mesh_part,
            canonical_epoch: stats.canonical_epoch,
            canonical_epoch_activation_delay_us: stats.canonical_epoch_activation_delay_us,
            contiguous_object: stats.contiguous_object,
            head_object: stats.head_object,
            gap_count: stats.gap_count,
            bytes_received: stats.bytes_received,
            datagrams_received: stats.datagrams_received,
            last_ingest_age_ms: stats.last_ingest_age_ms,
            stale_threshold_ms: Some(stream_stale_threshold_ms(
                stats.part_target_ms,
                stats.window_parts,
            )),
            mesh_lag_parts: None,
        }
    }

    fn active(&self) -> bool {
        self.latest_local_part.is_some()
            || self.latest_mesh_part.is_some()
            || self.head_object.is_some()
    }

    fn stale(&self) -> bool {
        self.active()
            && self.last_ingest_age_ms.is_some_and(|age_ms| {
                age_ms
                    > self
                        .stale_threshold_ms
                        .unwrap_or(MESH_MIN_STALE_INGEST_ALERT_MS)
            })
    }

    fn latest_comparable_object(&self) -> Option<u64> {
        self.contiguous_object.or(self.latest_mesh_part)
    }

    fn lagging(&self) -> bool {
        self.mesh_lag_parts
            .is_some_and(|lag| lag > MESH_STREAM_LAG_WARN_PARTS)
    }
}

fn stream_stale_threshold_ms(part_target_ms: u64, window_parts: usize) -> u64 {
    part_target_ms
        .saturating_mul(window_parts as u64)
        .max(MESH_MIN_STALE_INGEST_ALERT_MS)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MeshProtocolRequest {
    Snapshot,
    ProvisionNode {
        node_id: Option<String>,
        region: Option<String>,
    },
    CloseNode {
        node_id: Option<String>,
        region: Option<String>,
    },
    WarmStream {
        #[serde(default, deserialize_with = "deserialize_optional_u64_from_any")]
        stream_id: Option<u64>,
        region: Option<String>,
    },
}

#[derive(Debug, Serialize)]
struct MeshProtocolResponse {
    ok: bool,
    response_type: &'static str,
    snapshot: Option<MeshApiSnapshot>,
    command: Option<ControlCommand>,
    media_access_unit: Option<MediaAccessUnitResponse>,
    error: Option<String>,
}

impl MeshProtocolResponse {
    fn snapshot(snapshot: MeshApiSnapshot) -> Self {
        Self {
            ok: true,
            response_type: "snapshot",
            snapshot: Some(snapshot),
            command: None,
            media_access_unit: None,
            error: None,
        }
    }

    fn command(command: ControlCommand) -> Self {
        Self {
            ok: true,
            response_type: "command",
            snapshot: None,
            command: Some(command),
            media_access_unit: None,
            error: None,
        }
    }

    fn media_access_unit(unit: MediaAccessUnitResponse) -> Self {
        Self {
            ok: true,
            response_type: "media_access_unit",
            snapshot: None,
            command: None,
            media_access_unit: Some(unit),
            error: None,
        }
    }

    fn error(error: impl Into<String>) -> Self {
        Self {
            ok: false,
            response_type: "error",
            snapshot: None,
            command: None,
            media_access_unit: None,
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlEnvelope {
    id: u64,
    origin_node_id: String,
    kind: ControlKind,
    request: ControlRequest,
    #[serde(default)]
    target_node_ids: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct NodeLifecycle {
    draining: Arc<RwLock<bool>>,
}

impl NodeLifecycle {
    async fn is_draining(&self) -> bool {
        *self.draining.read().await
    }

    async fn set_draining(&self, draining: bool) {
        *self.draining.write().await = draining;
    }
}

#[derive(Debug, Clone, Default)]
struct DemandTracker {
    last_replica_request_unix_ms: Arc<RwLock<HashMap<u64, u64>>>,
}

impl DemandTracker {
    async fn should_request_replica(&self, stream_id: u64, now_ms: u64) -> bool {
        let mut requests = self.last_replica_request_unix_ms.write().await;
        let should_request = requests
            .get(&stream_id)
            .map(|last| now_ms.saturating_sub(*last) >= REPLICA_REQUEST_MIN_INTERVAL_MS)
            .unwrap_or(true);
        if should_request {
            requests.insert(stream_id, now_ms);
        }
        should_request
    }
}

#[derive(Debug, Clone)]
struct TelemetryAggregator {
    snapshots: Arc<RwLock<HashMap<String, MeshSnapshot>>>,
    stale_nodes: Arc<RwLock<Vec<TelemetryNodeHealth>>>,
    stale_after_ms: u64,
}

impl Default for TelemetryAggregator {
    fn default() -> Self {
        Self::new(DEFAULT_TELEMETRY_STALE_MS)
    }
}

impl TelemetryAggregator {
    fn new(stale_after_ms: u64) -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            stale_nodes: Arc::new(RwLock::new(Vec::new())),
            stale_after_ms,
        }
    }

    async fn ingest_payload(&self, payload: TcpChangesPayload) -> Result<bool> {
        if payload.tag != TELEMETRY_TAG {
            return Ok(false);
        }
        let snapshot: MeshSnapshot = serde_json::from_slice(&payload.val)
            .context("failed to decode AVMT mesh telemetry payload")?;
        self.ingest_snapshot(snapshot).await;
        Ok(true)
    }

    async fn ingest_snapshot(&self, snapshot: MeshSnapshot) {
        let node_id = snapshot.node.node_id.clone();
        self.snapshots
            .write()
            .await
            .insert(node_id.clone(), snapshot);
        self.stale_nodes
            .write()
            .await
            .retain(|node| node.node_id != node_id);
    }

    #[cfg(test)]
    async fn snapshot(&self, local: MeshSnapshot) -> MeshApiSnapshot {
        let (snapshots, telemetry) = self.snapshots_with_local(local.clone()).await;
        MeshApiSnapshot::from_snapshots(
            local,
            snapshots,
            telemetry,
            Vec::new(),
            OrchestrationStatus::default(),
        )
    }

    async fn snapshots_with_local(
        &self,
        local: MeshSnapshot,
    ) -> (Vec<MeshSnapshot>, TelemetryHealthSnapshot) {
        let now_ms = now_unix_ms();
        let mut snapshots = self.snapshots.write().await;
        let stale_nodes = snapshots
            .values()
            .filter(|snapshot| self.is_stale(snapshot, now_ms))
            .map(|snapshot| TelemetryNodeHealth::from_snapshot(snapshot, now_ms))
            .collect::<Vec<_>>();
        snapshots.retain(|_, snapshot| !self.is_stale(snapshot, now_ms));
        let fresh_remote_count = snapshots.len();
        if !stale_nodes.is_empty() {
            self.remember_stale_nodes(stale_nodes).await;
        }
        let remembered_stale_nodes = self.stale_nodes.read().await.clone();
        let telemetry = TelemetryHealthSnapshot {
            stale_after_ms: self.stale_after_ms,
            fresh_remote_count,
            stale_remote_count: remembered_stale_nodes.len(),
            stale_nodes: remembered_stale_nodes,
        };

        let mut snapshots_with_local = snapshots.clone();
        snapshots_with_local.insert(local.node.node_id.clone(), local);
        let mut snapshots_with_local = snapshots_with_local.into_values().collect::<Vec<_>>();
        snapshots_with_local.sort_by(|left, right| left.node.node_id.cmp(&right.node.node_id));
        (snapshots_with_local, telemetry)
    }

    fn is_stale(&self, snapshot: &MeshSnapshot, now_ms: u64) -> bool {
        self.stale_after_ms > 0
            && now_ms.saturating_sub(snapshot.updated_unix_ms) > self.stale_after_ms
    }

    async fn remember_stale_nodes(&self, stale_nodes: Vec<TelemetryNodeHealth>) {
        let mut remembered = self.stale_nodes.write().await;
        for stale_node in stale_nodes {
            if let Some(existing) = remembered
                .iter_mut()
                .find(|node| node.node_id == stale_node.node_id)
            {
                *existing = stale_node;
            } else {
                remembered.push(stale_node);
            }
        }
        remembered.sort_by(|left, right| {
            right
                .age_ms
                .cmp(&left.age_ms)
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
        remembered.truncate(32);
    }
}

impl TelemetryNodeHealth {
    fn from_snapshot(snapshot: &MeshSnapshot, now_ms: u64) -> Self {
        Self {
            node_id: snapshot.node.node_id.clone(),
            region: snapshot.node.region.clone(),
            updated_unix_ms: snapshot.updated_unix_ms,
            age_ms: now_ms.saturating_sub(snapshot.updated_unix_ms),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct TelemetryPeerMonitor {
    peers: Arc<RwLock<HashMap<String, TelemetryPeerStatus>>>,
}

impl TelemetryPeerMonitor {
    fn new(peers: &[SocketAddr]) -> Self {
        let peers = peers
            .iter()
            .map(|peer| {
                let peer = peer.to_string();
                (
                    peer.clone(),
                    TelemetryPeerStatus {
                        peer,
                        state: "configured".into(),
                        ..TelemetryPeerStatus::default()
                    },
                )
            })
            .collect();
        Self {
            peers: Arc::new(RwLock::new(peers)),
        }
    }

    async fn record_connecting(&self, peer: SocketAddr) {
        let mut peers = self.peers.write().await;
        let status = peers
            .entry(peer.to_string())
            .or_insert_with(|| TelemetryPeerStatus {
                peer: peer.to_string(),
                state: "configured".into(),
                ..TelemetryPeerStatus::default()
            });
        status.state = "connecting".into();
        status.connect_attempts = status.connect_attempts.saturating_add(1);
    }

    async fn record_connected(&self, peer: SocketAddr) {
        let mut peers = self.peers.write().await;
        let status = peers
            .entry(peer.to_string())
            .or_insert_with(|| TelemetryPeerStatus {
                peer: peer.to_string(),
                ..TelemetryPeerStatus::default()
            });
        status.state = "connected".into();
        status.last_connected_unix_ms = Some(now_unix_ms());
        status.last_error = None;
    }

    async fn record_payload(&self, peer: SocketAddr, bytes: usize) {
        let mut peers = self.peers.write().await;
        let status = peers
            .entry(peer.to_string())
            .or_insert_with(|| TelemetryPeerStatus {
                peer: peer.to_string(),
                ..TelemetryPeerStatus::default()
            });
        status.payloads = status.payloads.saturating_add(1);
        status.bytes = status.bytes.saturating_add(bytes as u64);
        status.last_payload_unix_ms = Some(now_unix_ms());
    }

    async fn record_disconnected(&self, peer: SocketAddr, error: Option<String>) {
        let mut peers = self.peers.write().await;
        let status = peers
            .entry(peer.to_string())
            .or_insert_with(|| TelemetryPeerStatus {
                peer: peer.to_string(),
                ..TelemetryPeerStatus::default()
            });
        status.state = if error.is_some() {
            "error".into()
        } else {
            "disconnected".into()
        };
        status.disconnects = status.disconnects.saturating_add(1);
        status.last_error = error;
    }

    async fn snapshot(&self) -> Vec<TelemetryPeerStatus> {
        let mut peers = self
            .peers
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        peers.sort_by(|left, right| left.peer.cmp(&right.peer));
        peers
    }
}

impl MeshSnapshot {
    fn with_edge_service(mut self, edge_service: EdgeServiceSnapshot) -> Self {
        self.edge_service = Some(edge_service);
        self
    }
}

impl MeshApiSnapshot {
    fn from_snapshots(
        local: MeshSnapshot,
        snapshots: Vec<MeshSnapshot>,
        telemetry: TelemetryHealthSnapshot,
        planned_replicas: Vec<ReplicaPlacement>,
        orchestration: OrchestrationStatus,
    ) -> Self {
        let relay_session = local.relay_session;
        let mut aggregate = AggregateMetrics::default();
        let mut nodes = Vec::with_capacity(snapshots.len());
        let mut edge_services = Vec::with_capacity(snapshots.len());
        let mut relay_nodes = Vec::with_capacity(snapshots.len());
        let mut connections = Vec::new();
        let mut streams = Vec::with_capacity(snapshots.len());
        let mut peer_addr_to_node_id = HashMap::with_capacity(snapshots.len() * 2);

        for snapshot in &snapshots {
            peer_addr_to_node_id
                .insert(snapshot.node.node_id.clone(), snapshot.node.node_id.clone());
            if let Some(mesh_addr) = &snapshot.mesh_addr {
                peer_addr_to_node_id.insert(mesh_addr.clone(), snapshot.node.node_id.clone());
            }
        }

        for snapshot in snapshots {
            aggregate.node_count += 1;
            aggregate.total_storage_bytes = aggregate
                .total_storage_bytes
                .saturating_add(snapshot.node.total_storage_bytes);
            aggregate.used_storage_bytes = aggregate
                .used_storage_bytes
                .saturating_add(snapshot.node.used_storage_bytes);
            aggregate.total_egress_capacity_bps = aggregate
                .total_egress_capacity_bps
                .saturating_add(snapshot.node.egress_capacity_bps);
            aggregate.contributor_streams = aggregate
                .contributor_streams
                .saturating_add(snapshot.node.contributor_streams);
            aggregate.active_streams = aggregate
                .active_streams
                .saturating_add(snapshot.node.active_streams);

            relay_nodes.push(RelayNodeSessionSnapshot {
                node_id: snapshot.node.node_id.clone(),
                region: snapshot.node.region.clone(),
                relay_session: snapshot.relay_session,
            });

            connections.extend(snapshot.peers.iter().map(|peer| ConnectionSnapshot {
                source_node_id: snapshot.node.node_id.clone(),
                target_addr: peer.addr.clone(),
                target_node_id: peer_addr_to_node_id.get(&peer.addr).cloned(),
                state: peer.state.clone(),
                private_target: is_private_mesh_target(&peer.addr),
            }));
            if snapshot.streams.is_empty() {
                streams.push(StreamTelemetry::from_stats(
                    snapshot.node.node_id.clone(),
                    &snapshot.stream,
                ));
            } else {
                streams.extend(snapshot.streams.iter().cloned());
            }
            edge_services.push(
                snapshot
                    .edge_service
                    .clone()
                    .unwrap_or_else(|| EdgeServiceSnapshot::fallback_for_node(&snapshot.node)),
            );
            nodes.push(snapshot.node);
        }

        annotate_stream_lag(&mut streams);

        connections.sort_by(|left, right| {
            left.source_node_id
                .cmp(&right.source_node_id)
                .then_with(|| left.target_addr.cmp(&right.target_addr))
                .then_with(|| left.target_node_id.cmp(&right.target_node_id))
        });
        connections.dedup();
        relay_nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        aggregate.connection_count = connections.len();
        let topology = TopologyConfidenceSnapshot::from_connections(&connections);

        let planned_replicas = planned_replicas
            .into_iter()
            .map(ReplicaPlacementSnapshot::from)
            .collect::<Vec<_>>();
        let recent_commands = local.recent_commands;
        let alerts = derive_mesh_alerts(
            &aggregate,
            &nodes,
            &edge_services,
            &connections,
            &local.stream,
            &local.node.node_id,
            &streams,
            &relay_nodes,
            &recent_commands,
            &telemetry,
            &relay_session,
            &orchestration.provision,
            &orchestration.telemetry_peers,
            &orchestration.private_discovery,
        );
        let activity = derive_mesh_activity(&aggregate, &alerts, &recent_commands);

        MeshApiSnapshot {
            updated_unix_ms: now_unix_ms(),
            node: local.node,
            mesh_transport: MeshTransportConfigSnapshot::default(),
            mesh_fec: MeshFecRuntimeSnapshot::default(),
            relay_session,
            relay_nodes,
            peers: local.peers,
            stream: local.stream,
            replication_policy: local.replication_policy,
            recent_commands,
            planned_replicas,
            aggregate,
            alerts,
            activity,
            telemetry,
            orchestration,
            topology,
            nodes,
            edge_services,
            connections,
            streams,
        }
    }
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn push_prometheus_metric_header(output: &mut String, name: &str, help: &str, metric_type: &str) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push(' ');
    output.push_str(metric_type);
    output.push('\n');
}

fn render_mesh_prometheus_metrics(snapshot: &MeshApiSnapshot) -> String {
    let aggregate = &snapshot.aggregate;
    let mut output = String::with_capacity(16 * 1024);

    for (name, help, value) in [
        (
            "av_mesh_nodes",
            "Mesh nodes currently visible in telemetry.",
            aggregate.node_count as u64,
        ),
        (
            "av_mesh_connections",
            "Directed mesh connections currently visible in telemetry.",
            aggregate.connection_count as u64,
        ),
        (
            "av_mesh_storage_bytes",
            "Total storage capacity visible across the mesh.",
            aggregate.total_storage_bytes,
        ),
        (
            "av_mesh_storage_used_bytes",
            "Storage currently used across the mesh.",
            aggregate.used_storage_bytes,
        ),
        (
            "av_mesh_egress_capacity_bps",
            "Advertised aggregate mesh egress capacity in bits per second.",
            aggregate.total_egress_capacity_bps,
        ),
        (
            "av_mesh_contributor_streams",
            "Contributor streams currently visible across the mesh.",
            aggregate.contributor_streams,
        ),
        (
            "av_mesh_active_streams",
            "Active streams currently visible across the mesh.",
            aggregate.active_streams,
        ),
        (
            "av_mesh_planned_replicas",
            "Replica placements currently requested by mesh policy.",
            snapshot.planned_replicas.len() as u64,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "gauge");
        output.push_str(&format!("{name} {value}\n"));
    }

    for (name, help, value) in [
        (
            "av_mesh_transport_sync_interval_seconds",
            "Configured cache-mesh replication scan interval in seconds.",
            snapshot.mesh_transport.sync_interval_ms as f64 / 1_000.0,
        ),
        (
            "av_mesh_transport_fec_repair_ratio",
            "Configured proportional cache-mesh FEC repair ratio.",
            ((snapshot.mesh_transport.repair_ratio as f64) * 1_000_000.0).round() / 1_000_000.0,
        ),
        (
            "av_mesh_transport_fec_min_repair_symbols",
            "Configured minimum cache-mesh FEC repair symbols per object.",
            snapshot.mesh_transport.min_repair_symbols as f64,
        ),
        (
            "av_mesh_transport_fec_max_repair_symbols",
            "Configured maximum cache-mesh FEC repair symbols per object.",
            snapshot.mesh_transport.max_repair_symbols as f64,
        ),
        (
            "av_mesh_transport_fec_symbol_bytes",
            "Configured cache-mesh FEC symbol size in bytes.",
            snapshot.mesh_transport.symbol_size as f64,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "gauge");
        output.push_str(&format!("{name} {value}\n"));
    }

    for (name, help, value) in [
        (
            "av_mesh_fec_tx_objects_total",
            "Cache-mesh objects encoded and offered to a peer.",
            snapshot.mesh_fec.tx_objects,
        ),
        (
            "av_mesh_fec_tx_protected_bytes_total",
            "Application bytes protected by cache-mesh FEC.",
            snapshot.mesh_fec.tx_protected_bytes,
        ),
        (
            "av_mesh_fec_tx_wire_bytes_total",
            "Cache-mesh FEC datagram bytes sent on the wire.",
            snapshot.mesh_fec.tx_wire_bytes,
        ),
        (
            "av_mesh_fec_tx_errors_total",
            "Cache-mesh FEC encode or datagram send errors.",
            snapshot.mesh_fec.tx_errors,
        ),
        (
            "av_mesh_fec_rx_wire_datagrams_total",
            "Cache-mesh UDP datagrams received, including malformed datagrams.",
            snapshot.mesh_fec.rx_wire_datagrams,
        ),
        (
            "av_mesh_fec_rx_wire_bytes_total",
            "Cache-mesh UDP datagram bytes received on the wire.",
            snapshot.mesh_fec.rx_wire_bytes,
        ),
        (
            "av_mesh_fec_rx_decoded_bytes_total",
            "Application bytes successfully decoded from cache-mesh FEC.",
            snapshot.mesh_fec.rx_decoded_bytes,
        ),
        (
            "av_mesh_fec_rx_repaired_source_datagrams_total",
            "Cache-mesh source symbols absent when successful FEC decoding completed.",
            snapshot.mesh_fec.rx_repaired_source_datagrams,
        ),
        (
            "av_mesh_fec_rx_late_source_datagrams_total",
            "Source symbols that arrived after repair data had already completed cache-mesh decoding.",
            snapshot.mesh_fec.rx_late_source_datagrams,
        ),
        (
            "av_mesh_fec_rx_presumed_lost_source_datagrams_total",
            "FEC-repaired source symbols that remained absent through the bounded late-arrival window.",
            snapshot.mesh_fec.rx_presumed_lost_source_datagrams,
        ),
        (
            "av_mesh_fec_rx_decode_errors_total",
            "Cache-mesh FEC datagrams rejected during decode.",
            snapshot.mesh_fec.rx_decode_errors,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "counter");
        output.push_str(&format!("{name} {value}\n"));
    }

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_fec_tx_datagrams_total",
        "Cache-mesh FEC datagrams sent by symbol kind.",
        "counter",
    );
    output.push_str(&format!(
        "av_mesh_fec_tx_datagrams_total{{kind=\"source\"}} {}\n",
        snapshot.mesh_fec.tx_source_datagrams
    ));
    output.push_str(&format!(
        "av_mesh_fec_tx_datagrams_total{{kind=\"repair\"}} {}\n",
        snapshot.mesh_fec.tx_repair_datagrams
    ));

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_fec_rx_datagrams_total",
        "Structurally valid cache-mesh FEC datagrams received by symbol kind.",
        "counter",
    );
    output.push_str(&format!(
        "av_mesh_fec_rx_datagrams_total{{kind=\"source\"}} {}\n",
        snapshot.mesh_fec.rx_source_datagrams
    ));
    output.push_str(&format!(
        "av_mesh_fec_rx_datagrams_total{{kind=\"repair\"}} {}\n",
        snapshot.mesh_fec.rx_repair_datagrams
    ));

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_fec_rx_objects_total",
        "Cache-mesh FEC object outcomes; repaired objects are a subset of decoded objects.",
        "counter",
    );
    for (outcome, value) in [
        ("decoded", snapshot.mesh_fec.rx_decoded_objects),
        ("repaired", snapshot.mesh_fec.rx_repaired_objects),
        ("expired", snapshot.mesh_fec.rx_expired_objects),
    ] {
        output.push_str(&format!(
            "av_mesh_fec_rx_objects_total{{outcome=\"{outcome}\"}} {value}\n"
        ));
    }

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_fec_rx_inflight_objects",
        "Incomplete cache-mesh FEC objects currently observed.",
        "gauge",
    );
    output.push_str(&format!(
        "av_mesh_fec_rx_inflight_objects {}\n",
        snapshot.mesh_fec.rx_inflight_objects
    ));

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_parent_sessions",
        "Configured RelaySession parent sessions by assigned path role.",
        "gauge",
    );
    output.push_str(&format!(
        "av_mesh_relay_session_parent_sessions{{role=\"primary\"}} {}\n",
        snapshot.relay_session.primary_sessions
    ));
    output.push_str(&format!(
        "av_mesh_relay_session_parent_sessions{{role=\"secondary\"}} {}\n",
        snapshot.relay_session.secondary_sessions
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_security_sessions",
        "RelaySession carrier bindings by established trust boundary.",
        "gauge",
    );
    output.push_str(&format!(
        "av_mesh_relay_session_security_sessions{{mode=\"authenticated\"}} {}\n",
        snapshot.relay_session.authenticated_sessions
    ));
    output.push_str(&format!(
        "av_mesh_relay_session_security_sessions{{mode=\"controlled_qualification\"}} {}\n",
        snapshot.relay_session.controlled_sessions
    ));
    for (name, help, value) in [
        (
            "av_mesh_relay_session_active_objects",
            "Canonical RelaySession objects currently awaiting RaptorQ completion.",
            snapshot.relay_session.active_objects,
        ),
        (
            "av_mesh_relay_session_completed_objects",
            "Completed RelaySession object identities retained for bounded deduplication.",
            snapshot.relay_session.completed_objects,
        ),
        (
            "av_mesh_relay_session_active_object_bytes",
            "Declared transfer bytes reserved by active RelaySession objects.",
            snapshot.relay_session.active_object_bytes,
        ),
        (
            "av_mesh_relay_session_buffered_datagrams",
            "Accepted RelaySession datagrams owned by incomplete objects.",
            snapshot.relay_session.buffered_datagrams,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "gauge");
        output.push_str(&format!("{name} {value}\n"));
    }

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_datagrams_total",
        "RelaySession datagram outcomes and accepted RaptorQ symbol roles.",
        "counter",
    );
    for (outcome, value) in [
        ("received", snapshot.relay_session.datagrams_received),
        ("rejected", snapshot.relay_session.datagrams_rejected),
        ("source", snapshot.relay_session.source_datagrams),
        ("repair", snapshot.relay_session.repair_datagrams),
        ("duplicate", snapshot.relay_session.duplicate_datagrams),
    ] {
        output.push_str(&format!(
            "av_mesh_relay_session_datagrams_total{{outcome=\"{outcome}\"}} {value}\n"
        ));
    }
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_objects_total",
        "RelaySession canonical object outcomes; repair_assisted records symbol use while fec_recovered proves missing source reconstruction.",
        "counter",
    );
    for (outcome, value) in [
        ("decoded", snapshot.relay_session.decoded_objects),
        (
            "repair_assisted",
            snapshot.relay_session.repair_assisted_objects,
        ),
        (
            "fec_recovered",
            snapshot.relay_session.fec_recovered_objects,
        ),
        ("expired", snapshot.relay_session.expired_objects),
    ] {
        output.push_str(&format!(
            "av_mesh_relay_session_objects_total{{outcome=\"{outcome}\"}} {value}\n"
        ));
    }
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_fec_recovered_source_symbols_total",
        "Missing source symbols reconstructed by RaptorQ before object decode.",
        "counter",
    );
    output.push_str(&format!(
        "av_mesh_relay_session_fec_recovered_source_symbols_total {}\n",
        snapshot.relay_session.fec_recovered_source_symbols
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_drops_total",
        "RelaySession datagrams dropped by bounded low-cardinality reason.",
        "counter",
    );
    for (reason, value) in [
        ("conflict", snapshot.relay_session.conflict_drops),
        (
            "authentication",
            snapshot.relay_session.authentication_drops,
        ),
        ("deadline", snapshot.relay_session.deadline_drops),
    ] {
        output.push_str(&format!(
            "av_mesh_relay_session_drops_total{{reason=\"{reason}\"}} {value}\n"
        ));
    }

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_downstream_children",
        "Explicit subscribed RelaySession children served by this relay.",
        "gauge",
    );
    output.push_str(&format!(
        "av_mesh_relay_session_downstream_children {}\n",
        snapshot.relay_session.downstream_children
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_forwarded_datagrams_total",
        "Admitted RaptorQ datagrams forwarded to subscribed children by symbol role.",
        "counter",
    );
    for (role, value) in [
        ("source", snapshot.relay_session.forwarded_source_datagrams),
        ("repair", snapshot.relay_session.forwarded_repair_datagrams),
    ] {
        output.push_str(&format!(
            "av_mesh_relay_session_forwarded_datagrams_total{{role=\"{role}\"}} {value}\n"
        ));
    }
    for (name, help, value) in [
        (
            "av_mesh_relay_session_forwarded_bytes_total",
            "RelaySession wire bytes forwarded to subscribed children.",
            snapshot.relay_session.forwarded_bytes,
        ),
        (
            "av_mesh_relay_session_forward_errors_total",
            "RelaySession downstream carrier send errors.",
            snapshot.relay_session.forward_errors,
        ),
        (
            "av_mesh_relay_session_forward_filtered_datagrams_total",
            "Admitted RelaySession symbols intentionally retained by lane policy rather than forwarded.",
            snapshot.relay_session.forward_filtered_datagrams,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "counter");
        output.push_str(&format!("{name} {value}\n"));
    }
    for (name, help, value) in [
        (
            "av_mesh_relay_warm_source_buffered_datagrams",
            "Unexpired source datagrams retained for immediate warm-secondary promotion.",
            snapshot.relay_session.warm_source_buffered_datagrams,
        ),
        (
            "av_mesh_relay_warm_source_buffered_bytes",
            "Wire bytes retained in the bounded warm-secondary source replay buffer.",
            snapshot.relay_session.warm_source_buffered_bytes,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "gauge");
        output.push_str(&format!("{name} {value}\n"));
    }
    for (name, help, value) in [
        (
            "av_mesh_relay_warm_source_replayed_datagrams_total",
            "Retained source datagrams replayed immediately after secondary promotion.",
            snapshot.relay_session.warm_source_replayed_datagrams,
        ),
        (
            "av_mesh_relay_warm_source_replayed_bytes_total",
            "Retained source bytes replayed immediately after secondary promotion.",
            snapshot.relay_session.warm_source_replayed_bytes,
        ),
        (
            "av_mesh_relay_warm_source_expired_datagrams_total",
            "Retained source datagrams discarded because their object deadline elapsed.",
            snapshot.relay_session.warm_source_expired_datagrams,
        ),
        (
            "av_mesh_relay_warm_source_retired_datagrams_total",
            "Retained source datagrams removed as completed objects leave the four-object replay window.",
            snapshot.relay_session.warm_source_retired_datagrams,
        ),
        (
            "av_mesh_relay_warm_source_evicted_datagrams_total",
            "Retained source datagrams evicted by the fixed object, datagram, or byte bounds.",
            snapshot.relay_session.warm_source_evicted_datagrams,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "counter");
        output.push_str(&format!("{name} {value}\n"));
    }
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_processing_duration_us",
        "Application processing time from RelaySession datagram receipt through forwarding and any completed-object cache commit, in microseconds.",
        "histogram",
    );
    for (upper_bound_us, count) in EDGE_RESPONSE_DURATION_BUCKETS_US
        .iter()
        .zip(snapshot.relay_session.processing_duration_buckets)
    {
        output.push_str(&format!(
            "av_mesh_relay_session_processing_duration_us_bucket{{le=\"{upper_bound_us}\"}} {count}\n"
        ));
    }
    output.push_str(&format!(
        "av_mesh_relay_session_processing_duration_us_bucket{{le=\"+Inf\"}} {}\n",
        snapshot.relay_session.processing_duration_count
    ));
    output.push_str(&format!(
        "av_mesh_relay_session_processing_duration_us_sum {}\n",
        snapshot.relay_session.processing_duration_sum_us
    ));
    output.push_str(&format!(
        "av_mesh_relay_session_processing_duration_us_count {}\n",
        snapshot.relay_session.processing_duration_count
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_processing_duration_max_us",
        "Maximum observed RelaySession application processing time in microseconds.",
        "gauge",
    );
    output.push_str(&format!(
        "av_mesh_relay_session_processing_duration_max_us {}\n",
        snapshot.relay_session.processing_duration_max_us
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_forward_duration_us",
        "Time to submit one admitted RelaySession datagram to one downstream child in microseconds.",
        "histogram",
    );
    for (upper_bound_us, count) in EDGE_RESPONSE_DURATION_BUCKETS_US
        .iter()
        .zip(snapshot.relay_session.forward_duration_buckets)
    {
        output.push_str(&format!(
            "av_mesh_relay_session_forward_duration_us_bucket{{le=\"{upper_bound_us}\"}} {count}\n"
        ));
    }
    output.push_str(&format!(
        "av_mesh_relay_session_forward_duration_us_bucket{{le=\"+Inf\"}} {}\n",
        snapshot.relay_session.forward_duration_count
    ));
    output.push_str(&format!(
        "av_mesh_relay_session_forward_duration_us_sum {}\n",
        snapshot.relay_session.forward_duration_sum_us
    ));
    output.push_str(&format!(
        "av_mesh_relay_session_forward_duration_us_count {}\n",
        snapshot.relay_session.forward_duration_count
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_forward_duration_max_us",
        "Largest observed downstream RelaySession carrier submission time in microseconds.",
        "gauge",
    );
    output.push_str(&format!(
        "av_mesh_relay_session_forward_duration_max_us {}\n",
        snapshot.relay_session.forward_duration_max_us
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_publication_to_available_us",
        "Canonical media time from contributor publication to verified local cache availability in microseconds.",
        "histogram",
    );
    for (upper_bound_us, count) in PUBLICATION_AVAILABILITY_BUCKETS_US
        .iter()
        .zip(snapshot.relay_session.publication_to_available_buckets)
    {
        output.push_str(&format!(
            "av_mesh_relay_session_publication_to_available_us_bucket{{le=\"{upper_bound_us}\"}} {count}\n"
        ));
    }
    output.push_str(&format!(
        "av_mesh_relay_session_publication_to_available_us_bucket{{le=\"+Inf\"}} {}\n",
        snapshot.relay_session.publication_to_available_count
    ));
    output.push_str(&format!(
        "av_mesh_relay_session_publication_to_available_us_sum {}\n",
        snapshot.relay_session.publication_to_available_sum_us
    ));
    output.push_str(&format!(
        "av_mesh_relay_session_publication_to_available_us_count {}\n",
        snapshot.relay_session.publication_to_available_count
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_publication_to_available_max_us",
        "Largest observed contributor-publication to local-availability time in microseconds.",
        "gauge",
    );
    output.push_str(&format!(
        "av_mesh_relay_session_publication_to_available_max_us {}\n",
        snapshot.relay_session.publication_to_available_max_us
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_publication_clock_error_max_us",
        "Largest source-clock error bound attached to publication latency samples in microseconds.",
        "gauge",
    );
    output.push_str(&format!(
        "av_mesh_relay_session_publication_clock_error_max_us {}\n",
        snapshot.relay_session.publication_clock_error_max_us
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_session_publication_clock_unusable_objects_total",
        "Canonical media objects omitted from publication latency because their source clock was missing or unusable.",
        "counter",
    );
    output.push_str(&format!(
        "av_mesh_relay_session_publication_clock_unusable_objects_total {}\n",
        snapshot.relay_session.publication_clock_unusable_objects
    ));

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_failover_state",
        "One-hot edge failover controller state for the compiled warm-secondary relationship.",
        "gauge",
    );
    for state in RelayFailoverControllerState::ALL {
        output.push_str(&format!(
            "av_mesh_relay_failover_state{{state=\"{}\"}} {}\n",
            state.as_str(),
            u8::from(snapshot.relay_session.failover_controller_state == state)
        ));
    }
    for (name, help, value) in [
        (
            "av_mesh_relay_failover_controller_enabled",
            "Whether this node actively detects primary silence and controls a warm secondary.",
            snapshot.relay_session.failover_controller_enabled,
        ),
        (
            "av_mesh_relay_failover_listeners",
            "Compiled warm-secondary child control listeners on this node.",
            snapshot.relay_session.failover_listeners,
        ),
        (
            "av_mesh_relay_failover_promoted_children",
            "Warm-secondary children currently receiving source plus repair symbols.",
            snapshot.relay_session.failover_promoted_children,
        ),
        (
            "av_mesh_relay_failover_primary_source_age_ms",
            "Age of the newest admitted primary source symbol at the edge.",
            snapshot.relay_session.failover_primary_source_age_ms,
        ),
        (
            "av_mesh_relay_failover_secondary_repair_age_ms",
            "Age of the newest admitted secondary repair symbol at the edge.",
            snapshot.relay_session.failover_secondary_repair_age_ms,
        ),
        (
            "av_mesh_relay_failover_last_detection_us",
            "Primary source silence measured when the latest promotion began.",
            snapshot.relay_session.failover_last_detection_us,
        ),
        (
            "av_mesh_relay_failover_last_promotion_to_source_us",
            "Time from the latest promotion command to the first admitted secondary source symbol.",
            snapshot.relay_session.failover_last_promotion_to_source_us,
        ),
        (
            "av_mesh_relay_failover_last_media_gap_us",
            "Cache completion gap spanning the latest primary failure and promotion.",
            snapshot.relay_session.failover_last_media_gap_us,
        ),
        (
            "av_mesh_relay_failover_max_media_gap_us",
            "Largest cache completion gap observed across automatic failovers.",
            snapshot.relay_session.failover_max_media_gap_us,
        ),
        (
            "av_mesh_relay_failover_controller_last_transition_unix_ms",
            "Wall-clock time of the edge controller's latest state transition.",
            snapshot
                .relay_session
                .failover_controller_last_transition_unix_ms,
        ),
        (
            "av_mesh_relay_failover_listener_last_transition_unix_ms",
            "Wall-clock time of the warm forwarder's latest applied mode transition.",
            snapshot
                .relay_session
                .failover_listener_last_transition_unix_ms,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "gauge");
        output.push_str(&format!("{name} {value}\n"));
    }
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_failover_commands_total",
        "Leased failover control commands by endpoint direction and outcome.",
        "counter",
    );
    for (direction, outcome, value) in [
        (
            "sent",
            "success",
            snapshot.relay_session.failover_commands_sent,
        ),
        (
            "sent",
            "error",
            snapshot.relay_session.failover_command_send_errors,
        ),
        (
            "received",
            "accepted",
            snapshot.relay_session.failover_commands_received,
        ),
        (
            "received",
            "rejected",
            snapshot.relay_session.failover_commands_rejected,
        ),
    ] {
        output.push_str(&format!(
            "av_mesh_relay_failover_commands_total{{direction=\"{direction}\",outcome=\"{outcome}\"}} {value}\n"
        ));
    }
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_relay_failover_transitions_total",
        "Warm-secondary promotion and make-before-break demotion transitions by endpoint side.",
        "counter",
    );
    for (side, transition, value) in [
        (
            "controller",
            "promotion",
            snapshot.relay_session.failover_promotions,
        ),
        (
            "controller",
            "demotion",
            snapshot.relay_session.failover_demotions,
        ),
        (
            "forwarder",
            "promotion",
            snapshot.relay_session.failover_promotions_applied,
        ),
        (
            "forwarder",
            "demotion",
            snapshot.relay_session.failover_demotions_applied,
        ),
    ] {
        output.push_str(&format!(
            "av_mesh_relay_failover_transitions_total{{side=\"{side}\",transition=\"{transition}\"}} {value}\n"
        ));
    }
    for (name, help, value) in [
        (
            "av_mesh_relay_failover_secondary_unavailable_total",
            "Primary failures where the secondary did not have a recent repair heartbeat.",
            snapshot.relay_session.failover_secondary_unavailable_events,
        ),
        (
            "av_mesh_relay_failover_lease_expirations_total",
            "Promoted forwarders returned to repair-only after their controller lease expired.",
            snapshot.relay_session.failover_lease_expirations,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "counter");
        output.push_str(&format!("{name} {value}\n"));
    }

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_topology_peers",
        "Mesh peers by topology resolution and address scope.",
        "gauge",
    );
    for (kind, value) in [
        ("resolved", snapshot.topology.resolved_peer_count),
        ("unresolved", snapshot.topology.unresolved_peer_count),
        ("private", snapshot.topology.private_peer_count),
        ("public", snapshot.topology.public_peer_count),
    ] {
        output.push_str(&format!(
            "av_mesh_topology_peers{{kind=\"{kind}\"}} {value}\n"
        ));
    }
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_telemetry_nodes",
        "Remote mesh nodes by telemetry freshness.",
        "gauge",
    );
    output.push_str(&format!(
        "av_mesh_telemetry_nodes{{state=\"fresh\"}} {}\n",
        snapshot.telemetry.fresh_remote_count
    ));
    output.push_str(&format!(
        "av_mesh_telemetry_nodes{{state=\"stale\"}} {}\n",
        snapshot.telemetry.stale_remote_count
    ));

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_canonical_epoch_divergent_streams",
        "Streams whose visible relay nodes disagree on the active canonical source epoch.",
        "gauge",
    );
    output.push_str(&format!(
        "av_mesh_canonical_epoch_divergent_streams {}\n",
        canonical_epoch_divergent_stream_count(&snapshot.streams)
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_mesh_canonical_epoch_activation_delay_max_seconds",
        "Maximum observed delay from contributor source-epoch creation to first canonical object activation across visible nodes and streams.",
        "gauge",
    );
    let canonical_epoch_activation_delay_max_us = snapshot
        .streams
        .iter()
        .filter_map(|stream| stream.canonical_epoch_activation_delay_us)
        .max()
        .unwrap_or(0);
    output.push_str(&format!(
        "av_mesh_canonical_epoch_activation_delay_max_seconds {}\n",
        canonical_epoch_activation_delay_max_us as f64 / 1_000_000.0
    ));

    for (name, help, metric_type) in [
        (
            "av_mesh_node_draining",
            "Whether a mesh node is draining.",
            "gauge",
        ),
        (
            "av_mesh_node_storage_bytes",
            "Storage capacity by mesh node.",
            "gauge",
        ),
        (
            "av_mesh_node_storage_used_bytes",
            "Storage used by mesh node.",
            "gauge",
        ),
        (
            "av_mesh_node_egress_capacity_bps",
            "Advertised egress capacity by mesh node.",
            "gauge",
        ),
        (
            "av_mesh_node_streams",
            "Streams by mesh node and kind.",
            "gauge",
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, metric_type);
    }
    for node in &snapshot.nodes {
        let node_id = prometheus_label_value(&node.node_id);
        let region = prometheus_label_value(&node.region);
        let continent = prometheus_label_value(&node.continent);
        let labels = format!("node_id=\"{node_id}\",region=\"{region}\",continent=\"{continent}\"");
        output.push_str(&format!(
            "av_mesh_node_draining{{{labels}}} {}\n",
            u8::from(node.draining)
        ));
        output.push_str(&format!(
            "av_mesh_node_storage_bytes{{{labels}}} {}\n",
            node.total_storage_bytes
        ));
        output.push_str(&format!(
            "av_mesh_node_storage_used_bytes{{{labels}}} {}\n",
            node.used_storage_bytes
        ));
        output.push_str(&format!(
            "av_mesh_node_egress_capacity_bps{{{labels}}} {}\n",
            node.egress_capacity_bps
        ));
        output.push_str(&format!(
            "av_mesh_node_streams{{{labels},kind=\"contributor\"}} {}\n",
            node.contributor_streams
        ));
        output.push_str(&format!(
            "av_mesh_node_streams{{{labels},kind=\"active\"}} {}\n",
            node.active_streams
        ));
    }

    for (name, help, metric_type) in [
        (
            "av_mesh_edge_active_readers",
            "Current edge readers by mesh node.",
            "gauge",
        ),
        (
            "av_mesh_edge_requests_total",
            "Edge media read requests by mesh node.",
            "counter",
        ),
        (
            "av_mesh_edge_bytes_total",
            "Edge media bytes served by mesh node.",
            "counter",
        ),
        (
            "av_mesh_edge_llhls_tail_requests_total",
            "LL-HLS tail requests by mesh node.",
            "counter",
        ),
        (
            "av_mesh_edge_responses_total",
            "Recorded LL-HLS responses by mesh node and outcome.",
            "counter",
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, metric_type);
    }
    for edge in &snapshot.edge_services {
        let node_id = prometheus_label_value(&edge.node_id);
        let region = prometheus_label_value(&edge.region);
        let labels = format!("node_id=\"{node_id}\",region=\"{region}\"");
        output.push_str(&format!(
            "av_mesh_edge_active_readers{{{labels}}} {}\n",
            edge.active_readers
        ));
        output.push_str(&format!(
            "av_mesh_edge_requests_total{{{labels}}} {}\n",
            edge.requests_served
        ));
        output.push_str(&format!(
            "av_mesh_edge_bytes_total{{{labels}}} {}\n",
            edge.bytes_served
        ));
        output.push_str(&format!(
            "av_mesh_edge_llhls_tail_requests_total{{{labels}}} {}\n",
            edge.llhls_tail_requests
        ));
        for (outcome, value) in [
            ("all", edge.responses_total),
            ("error", edge.response_errors),
            ("not_found", edge.response_not_found),
        ] {
            output.push_str(&format!(
                "av_mesh_edge_responses_total{{{labels},outcome=\"{outcome}\"}} {value}\n"
            ));
        }
    }

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_edge_response_duration_seconds",
        "Time spent producing an LL-HLS edge response.",
        "histogram",
    );
    for edge in &snapshot.edge_services {
        let node_id = prometheus_label_value(&edge.node_id);
        let region = prometheus_label_value(&edge.region);
        let labels = format!("node_id=\"{node_id}\",region=\"{region}\"");
        for (index, upper_bound_us) in EDGE_RESPONSE_DURATION_BUCKETS_US.iter().enumerate() {
            let count = edge
                .response_duration_buckets
                .get(index)
                .copied()
                .unwrap_or(0);
            output.push_str(&format!(
                "av_mesh_edge_response_duration_seconds_bucket{{{labels},le=\"{}\"}} {count}\n",
                *upper_bound_us as f64 / 1_000_000.0
            ));
        }
        output.push_str(&format!(
            "av_mesh_edge_response_duration_seconds_bucket{{{labels},le=\"+Inf\"}} {}\n",
            edge.response_duration_count
        ));
        output.push_str(&format!(
            "av_mesh_edge_response_duration_seconds_sum{{{labels}}} {}\n",
            edge.response_duration_sum_us as f64 / 1_000_000.0
        ));
        output.push_str(&format!(
            "av_mesh_edge_response_duration_seconds_count{{{labels}}} {}\n",
            edge.response_duration_count
        ));
    }

    for (name, help, metric_type) in [
        (
            "av_mesh_stream_bytes_received_total",
            "Mesh ingest bytes received by node and stream.",
            "counter",
        ),
        (
            "av_mesh_stream_datagrams_received_total",
            "Mesh ingest datagrams received by node and stream.",
            "counter",
        ),
        (
            "av_mesh_stream_latest_part",
            "Latest part sequence by node, stream, and source.",
            "gauge",
        ),
        (
            "av_mesh_stream_canonical_epoch",
            "Active canonical media-object source incarnation epoch by node and stream.",
            "gauge",
        ),
        (
            "av_mesh_stream_canonical_epoch_activation_delay_seconds",
            "Delay from contributor source-epoch creation to first canonical object activation by node and stream.",
            "gauge",
        ),
        (
            "av_mesh_stream_canonical_head_object",
            "Latest canonical media-object identity received by node and stream.",
            "gauge",
        ),
        (
            "av_mesh_stream_contiguous_object",
            "Highest canonical media object available through the contiguous publication prefix.",
            "gauge",
        ),
        (
            "av_mesh_stream_known_gap_count",
            "Known missing canonical objects in the retained live window.",
            "gauge",
        ),
        (
            "av_mesh_stream_last_ingest_age_seconds",
            "Age of the latest mesh ingest by node and stream.",
            "gauge",
        ),
        (
            "av_mesh_stream_latest_local_part_age_seconds",
            "Age of the latest locally committed LL-HLS part by node and stream.",
            "gauge",
        ),
        (
            "av_mesh_stream_lag_parts",
            "Canonical contiguous-object lag behind the freshest comparable node by stream.",
            "gauge",
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, metric_type);
    }
    for stream in &snapshot.streams {
        let node_id = prometheus_label_value(&stream.node_id);
        let stream_id = prometheus_label_value(&stream.stream_id_text);
        let labels = format!("node_id=\"{node_id}\",stream_id=\"{stream_id}\"");
        output.push_str(&format!(
            "av_mesh_stream_bytes_received_total{{{labels}}} {}\n",
            stream.bytes_received
        ));
        output.push_str(&format!(
            "av_mesh_stream_datagrams_received_total{{{labels}}} {}\n",
            stream.datagrams_received
        ));
        if let Some(part) = stream.latest_local_part {
            output.push_str(&format!(
                "av_mesh_stream_latest_part{{{labels},source=\"local\"}} {part}\n"
            ));
        }
        if let Some(part) = stream.latest_mesh_part {
            output.push_str(&format!(
                "av_mesh_stream_latest_part{{{labels},source=\"mesh\"}} {part}\n"
            ));
        }
        if let Some(epoch) = stream.canonical_epoch {
            output.push_str(&format!(
                "av_mesh_stream_canonical_epoch{{{labels}}} {epoch}\n"
            ));
        }
        if let Some(delay_us) = stream.canonical_epoch_activation_delay_us {
            output.push_str(&format!(
                "av_mesh_stream_canonical_epoch_activation_delay_seconds{{{labels}}} {}\n",
                delay_us as f64 / 1_000_000.0
            ));
        }
        if let Some(object) = stream.head_object {
            output.push_str(&format!(
                "av_mesh_stream_canonical_head_object{{{labels}}} {object}\n"
            ));
        }
        if let Some(object) = stream.contiguous_object {
            output.push_str(&format!(
                "av_mesh_stream_contiguous_object{{{labels}}} {object}\n"
            ));
        }
        if let Some(gaps) = stream.gap_count {
            output.push_str(&format!(
                "av_mesh_stream_known_gap_count{{{labels}}} {gaps}\n"
            ));
        }
        if let Some(age_ms) = stream.last_ingest_age_ms {
            output.push_str(&format!(
                "av_mesh_stream_last_ingest_age_seconds{{{labels}}} {}\n",
                age_ms as f64 / 1_000.0
            ));
        }
        if let Some(age_ms) = stream.latest_local_part_age_ms {
            output.push_str(&format!(
                "av_mesh_stream_latest_local_part_age_seconds{{{labels}}} {}\n",
                age_ms as f64 / 1_000.0
            ));
        }
        if let Some(lag) = stream.mesh_lag_parts {
            output.push_str(&format!("av_mesh_stream_lag_parts{{{labels}}} {lag}\n"));
        }
    }

    push_prometheus_metric_header(
        &mut output,
        "av_mesh_alerts",
        "Current mesh alert counts by severity and stable alert code.",
        "gauge",
    );
    for alert in &snapshot.alerts {
        let level = prometheus_label_value(alert.level);
        let code = prometheus_label_value(alert.code);
        let node_id = prometheus_label_value(alert.node_id.as_deref().unwrap_or(""));
        let stream_id = prometheus_label_value(alert.stream_id_text.as_deref().unwrap_or(""));
        output.push_str(&format!(
            "av_mesh_alerts{{level=\"{level}\",code=\"{code}\",node_id=\"{node_id}\",stream_id=\"{stream_id}\"}} {}\n",
            alert.count
        ));
    }

    output
}

fn annotate_stream_lag(streams: &mut [StreamTelemetry]) {
    let mut heads = HashMap::<(u64, Option<u64>), u64>::new();
    for stream in streams.iter() {
        if let Some(part) = stream.latest_comparable_object() {
            heads
                .entry((stream.stream_id, stream.canonical_epoch))
                .and_modify(|head| *head = (*head).max(part))
                .or_insert(part);
        }
    }

    for stream in streams.iter_mut() {
        stream.mesh_lag_parts = stream.latest_comparable_object().and_then(|part| {
            heads
                .get(&(stream.stream_id, stream.canonical_epoch))
                .copied()
                .map(|head| head.saturating_sub(part))
        });
    }
}

fn canonical_epoch_divergent_stream_count(streams: &[StreamTelemetry]) -> usize {
    let mut stream_epochs = HashMap::<u64, HashSet<u64>>::new();
    for stream in streams {
        if let Some(epoch) = stream.canonical_epoch {
            stream_epochs
                .entry(stream.stream_id)
                .or_default()
                .insert(epoch);
        }
    }
    stream_epochs
        .values()
        .filter(|epochs| epochs.len() > 1)
        .count()
}

fn is_private_mesh_target(target: &str) -> bool {
    let host = target
        .rsplit_once('@')
        .map(|(_, target)| target)
        .unwrap_or(target)
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(target)
        .trim_matches(['[', ']']);
    host.parse::<std::net::IpAddr>()
        .map(|addr| match addr {
            std::net::IpAddr::V4(addr) => addr.is_private() || addr.is_loopback(),
            std::net::IpAddr::V6(addr) => addr.is_loopback() || addr.is_unique_local(),
        })
        .unwrap_or(false)
}

#[allow(clippy::too_many_arguments)]
fn derive_mesh_alerts(
    aggregate: &AggregateMetrics,
    nodes: &[MeshNode],
    edge_services: &[EdgeServiceSnapshot],
    connections: &[ConnectionSnapshot],
    local_stream: &StatsSnapshot,
    local_node_id: &str,
    streams: &[StreamTelemetry],
    relay_nodes: &[RelayNodeSessionSnapshot],
    recent_commands: &[ControlCommand],
    telemetry: &TelemetryHealthSnapshot,
    relay_session: &RelaySessionIngressSnapshot,
    provision: &ProvisionStatus,
    telemetry_peers: &[TelemetryPeerStatus],
    private_discovery: &PrivateDiscoveryStatus,
) -> Vec<MeshAlert> {
    let now = now_unix_ms();
    let mut alerts = Vec::new();

    if aggregate.node_count <= 1 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "mesh_single_node",
            message:
                "Only one mesh node is visible; failover and regional routing are not available."
                    .into(),
            count: aggregate.node_count as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    } else if aggregate.connection_count == 0
        && relay_session.controlled_sessions == 0
        && relay_session.authenticated_sessions == 0
    {
        alerts.push(MeshAlert {
            level: "error",
            code: "mesh_no_links",
            message: "Multiple mesh nodes are visible, but no mesh links are currently reported."
                .into(),
            count: aggregate.node_count as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    let unknown_peers = connections
        .iter()
        .filter(|connection| connection.target_node_id.is_none())
        .count();
    if unknown_peers > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "mesh_unknown_peers",
            message: "Some mesh peer addresses do not resolve to known node ids.".into(),
            count: unknown_peers as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    let draining_nodes = nodes.iter().filter(|node| node.draining).count();
    if draining_nodes > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "nodes_draining",
            message: "One or more mesh nodes are marked draining.".into(),
            count: draining_nodes as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    let playback_missing = edge_services
        .iter()
        .filter(|edge| edge.playback_base_url.is_none())
        .count();
    if playback_missing > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "edge_playback_missing",
            message: "One or more nodes do not advertise a public LL-HLS playback base URL.".into(),
            count: playback_missing as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    let edge_response_errors = edge_services
        .iter()
        .map(|edge| edge.response_errors)
        .sum::<u64>();
    let edge_recent_errors = edge_services
        .iter()
        .flat_map(|edge| {
            edge.recent_responses
                .iter()
                .filter(|response| response.status >= 400)
                .map(move |response| (edge, response))
        })
        .max_by_key(|(_, response)| response.unix_ms);
    if edge_response_errors > 0 {
        let (node_id, path, status, last_seen) = edge_recent_errors
            .map(|(edge, response)| {
                (
                    Some(edge.node_id.clone()),
                    response.path.clone(),
                    response.status,
                    Some(response.unix_ms),
                )
            })
            .unwrap_or((None, "unknown edge path".into(), 0, Some(now)));
        alerts.push(MeshAlert {
            level: if status >= 500 { "error" } else { "warn" },
            code: "edge_response_errors",
            message: format!(
                "Edge playback/API paths have returned {edge_response_errors} non-success response(s); latest was HTTP {status} for {path}."
            ),
            count: edge_response_errors,
            last_seen_unix_ms: last_seen,
            node_id,
            stream_id_text: None,
        });
    }

    let storage_error_nodes = nodes
        .iter()
        .filter(|node| storage_percent(node) >= MESH_STORAGE_ERROR_PCT)
        .count();
    let storage_warn_nodes = nodes
        .iter()
        .filter(|node| storage_percent(node) >= MESH_STORAGE_WARN_PCT)
        .count();
    if storage_error_nodes > 0 {
        alerts.push(MeshAlert {
            level: "error",
            code: "storage_exhausted",
            message: format!(
                "{storage_error_nodes} node(s) are at or above {MESH_STORAGE_ERROR_PCT}% storage usage."
            ),
            count: storage_error_nodes as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    } else if storage_warn_nodes > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "storage_pressure",
            message: format!(
                "{storage_warn_nodes} node(s) are at or above {MESH_STORAGE_WARN_PCT}% storage usage."
            ),
            count: storage_warn_nodes as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    let mut failed_commands = 0u64;
    let mut skipped_commands = 0u64;
    let mut last_command_issue = None;
    for command in recent_commands {
        let status = command.status.to_ascii_lowercase();
        if status.contains("failed") || status.contains("timed out") || status.contains("error") {
            failed_commands = failed_commands.saturating_add(1);
            last_command_issue = Some(last_command_issue.unwrap_or(0).max(command.created_unix_ms));
        } else if status.contains("skipped") {
            skipped_commands = skipped_commands.saturating_add(1);
            last_command_issue = Some(last_command_issue.unwrap_or(0).max(command.created_unix_ms));
        }
    }
    if failed_commands > 0 {
        alerts.push(MeshAlert {
            level: "error",
            code: "control_failures",
            message: "One or more recent orchestration commands failed.".into(),
            count: failed_commands,
            last_seen_unix_ms: last_command_issue,
            node_id: None,
            stream_id_text: None,
        });
    } else if skipped_commands > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "control_skipped",
            message: "One or more recent orchestration commands were skipped.".into(),
            count: skipped_commands,
            last_seen_unix_ms: last_command_issue,
            node_id: None,
            stream_id_text: None,
        });
    }

    if provision.backends.iter().any(|backend| backend == "linode") && !private_discovery.enabled {
        alerts.push(MeshAlert {
            level: "warn",
            code: "linode_private_discovery_inactive",
            message: "Linode provisioning is configured, but private-subnet discovery is not active; new VLAN nodes may need explicit seed peers.".into(),
            count: 1,
            last_seen_unix_ms: Some(now),
            node_id: Some(local_node_id.to_owned()),
            stream_id_text: None,
        });
    }

    let unavailable_telemetry_peers = telemetry_peers
        .iter()
        .filter(|peer| peer.state != "connected")
        .count();
    if unavailable_telemetry_peers > 0 {
        let latest_error_peer = telemetry_peers
            .iter()
            .filter(|peer| peer.state != "connected")
            .max_by_key(|peer| {
                peer.last_payload_unix_ms
                    .or(peer.last_connected_unix_ms)
                    .unwrap_or(0)
            });
        let peer = latest_error_peer
            .map(|peer| peer.peer.as_str())
            .unwrap_or("unknown peer");
        let state = latest_error_peer
            .map(|peer| peer.state.as_str())
            .unwrap_or("unknown");
        alerts.push(MeshAlert {
            level: "warn",
            code: "telemetry_peer_unavailable",
            message: format!(
                "{unavailable_telemetry_peers} tcp-changes telemetry peer(s) are not connected; latest is {peer} ({state})."
            ),
            count: unavailable_telemetry_peers as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    if telemetry.stale_remote_count > 0 {
        let latest_stale_node = telemetry
            .stale_nodes
            .iter()
            .max_by_key(|node| node.updated_unix_ms);
        let node_id = latest_stale_node.map(|node| node.node_id.clone());
        let detail = latest_stale_node
            .map(|node| {
                format!(
                    "latest stale node {} in {} last updated {} ms ago",
                    node.node_id, node.region, node.age_ms
                )
            })
            .unwrap_or_else(|| "stale telemetry node details unavailable".into());
        alerts.push(MeshAlert {
            level: "warn",
            code: "telemetry_snapshot_stale",
            message: format!(
                "{} mesh telemetry snapshot(s) have aged out; {detail}.",
                telemetry.stale_remote_count
            ),
            count: telemetry.stale_remote_count as u64,
            last_seen_unix_ms: latest_stale_node.map(|node| node.updated_unix_ms),
            node_id,
            stream_id_text: None,
        });
    }

    let blocked_provision_backends = provision
        .backend_statuses
        .iter()
        .filter(|backend| backend.state != "ready")
        .count();
    if blocked_provision_backends > 0 {
        alerts.push(MeshAlert {
            level: "warn",
            code: "provision_backend_blocked",
            message: "One or more configured provision backends are not ready.".into(),
            count: blocked_provision_backends as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: None,
        });
    }

    if let Some(age_ms) = local_stream.last_ingest_age_ms {
        let stale_threshold_ms =
            stream_stale_threshold_ms(local_stream.part_target_ms, local_stream.window_parts);
        if local_stream.latest_local_part.is_some() && age_ms > stale_threshold_ms {
            alerts.push(MeshAlert {
                level: "warn",
                code: "local_ingest_stale",
                message: format!(
                    "Local stream {} has not ingested bytes for {} ms.",
                    local_stream.stream_id_text, age_ms
                ),
                count: 1,
                last_seen_unix_ms: now.checked_sub(age_ms),
                node_id: None,
                stream_id_text: Some(local_stream.stream_id_text.clone()),
            });
        }
    }

    let stale_streams = streams
        .iter()
        .filter(|stream| stream.node_id != local_node_id && stream.stale())
        .collect::<Vec<_>>();
    if let Some((stream, last_seen, age_ms)) = stale_streams
        .iter()
        .filter_map(|stream| {
            stream
                .last_ingest_age_ms
                .map(|age_ms| (*stream, now.saturating_sub(age_ms), age_ms))
        })
        .max_by_key(|(_, last_seen, _)| *last_seen)
    {
        alerts.push(MeshAlert {
            level: "warn",
            code: "mesh_stream_stale",
            message: format!(
                "{} mesh stream(s) are stale; latest stale stream {} on {} has not ingested bytes for {} ms.",
                stale_streams.len(),
                stream.stream_id_text,
                stream.node_id,
                age_ms
            ),
            count: stale_streams.len() as u64,
            last_seen_unix_ms: Some(last_seen),
            node_id: Some(stream.node_id.clone()),
            stream_id_text: Some(stream.stream_id_text.clone()),
        });
    }

    let streams_with_gaps = streams
        .iter()
        .filter(|stream| stream.gap_count.unwrap_or_default() > 0)
        .collect::<Vec<_>>();
    if let Some(stream) = streams_with_gaps
        .iter()
        .max_by_key(|stream| stream.gap_count.unwrap_or_default())
        .copied()
    {
        let gaps = stream.gap_count.unwrap_or_default();
        alerts.push(MeshAlert {
            level: "warn",
            code: "canonical_publication_gap",
            message: format!(
                "{} stream publication(s) have retained canonical gaps; stream {} on {} has {gaps} missing object(s).",
                streams_with_gaps.len(),
                stream.stream_id_text,
                stream.node_id
            ),
            count: streams_with_gaps.len() as u64,
            last_seen_unix_ms: Some(now),
            node_id: Some(stream.node_id.clone()),
            stream_id_text: Some(stream.stream_id_text.clone()),
        });
    }

    let slow_epoch_activations = streams
        .iter()
        .filter(|stream| {
            stream
                .canonical_epoch_activation_delay_us
                .is_some_and(|delay| delay > CANONICAL_EPOCH_ACTIVATION_WARN_US)
        })
        .collect::<Vec<_>>();
    if let Some(stream) = slow_epoch_activations
        .iter()
        .max_by_key(|stream| {
            stream
                .canonical_epoch_activation_delay_us
                .unwrap_or_default()
        })
        .copied()
    {
        let delay_us = stream
            .canonical_epoch_activation_delay_us
            .unwrap_or_default();
        alerts.push(MeshAlert {
            level: "warn",
            code: "canonical_epoch_activation_slow",
            message: format!(
                "{} stream publication(s) exceeded the source-epoch activation target; stream {} on {} took {delay_us} us to accept its first canonical object.",
                slow_epoch_activations.len(),
                stream.stream_id_text,
                stream.node_id
            ),
            count: slow_epoch_activations.len() as u64,
            last_seen_unix_ms: Some(now),
            node_id: Some(stream.node_id.clone()),
            stream_id_text: Some(stream.stream_id_text.clone()),
        });
    }

    let mut stream_epochs = HashMap::<u64, HashSet<u64>>::new();
    for stream in streams {
        if let Some(epoch) = stream.canonical_epoch {
            stream_epochs
                .entry(stream.stream_id)
                .or_default()
                .insert(epoch);
        }
    }
    if let Some((stream_id, epochs)) = stream_epochs.iter().find(|(_, epochs)| epochs.len() > 1) {
        alerts.push(MeshAlert {
            level: "warn",
            code: "canonical_epoch_divergence",
            message: format!(
                "Stream {stream_id} is split across {} canonical source epochs during publication convergence.",
                epochs.len()
            ),
            count: epochs.len() as u64,
            last_seen_unix_ms: Some(now),
            node_id: None,
            stream_id_text: Some(stream_id_text(*stream_id)),
        });
    }

    for node in relay_nodes {
        let relay = &node.relay_session;
        let Some(processing_p95_us) = histogram_percentile_upper_bound_us(
            relay.processing_duration_count,
            &relay.processing_duration_buckets,
            95,
            relay.processing_duration_max_us,
        ) else {
            continue;
        };
        if processing_p95_us > RELAY_PROCESSING_P95_WARN_US {
            alerts.push(MeshAlert {
                level: "warn",
                code: "relay_processing_p95_exceeded",
                message: format!(
                    "Relay {} application processing p95 is {processing_p95_us} us; the interactive and premium limit is {} us.",
                    node.node_id, RELAY_PROCESSING_P95_WARN_US
                ),
                count: processing_p95_us,
                last_seen_unix_ms: Some(now),
                node_id: Some(node.node_id.clone()),
                stream_id_text: None,
            });
        }
    }

    let controlled_relay_node_ids = relay_nodes
        .iter()
        .filter(|node| node.relay_session.controlled_sessions > 0)
        .map(|node| node.node_id.as_str())
        .collect::<HashSet<_>>();
    let lagging_streams = streams
        .iter()
        .filter(|stream| {
            stream.lagging() && !controlled_relay_node_ids.contains(stream.node_id.as_str())
        })
        .collect::<Vec<_>>();
    if let Some(stream) = lagging_streams
        .iter()
        .max_by_key(|stream| stream.mesh_lag_parts.unwrap_or_default())
        .copied()
    {
        let lag = stream.mesh_lag_parts.unwrap_or_default();
        alerts.push(MeshAlert {
            level: "warn",
            code: "mesh_stream_lagging",
            message: format!(
                "{} mesh stream replica(s) are behind the stream head; latest lag is {lag} part(s) for stream {} on {}.",
                lagging_streams.len(),
                stream.stream_id_text,
                stream.node_id
            ),
            count: lagging_streams.len() as u64,
            last_seen_unix_ms: Some(now),
            node_id: Some(stream.node_id.clone()),
            stream_id_text: Some(stream.stream_id_text.clone()),
        });
    }

    alerts
}

fn derive_mesh_activity(
    aggregate: &AggregateMetrics,
    alerts: &[MeshAlert],
    recent_commands: &[ControlCommand],
) -> Vec<MeshActivity> {
    let now = now_unix_ms();
    let mut activity = Vec::with_capacity(1 + alerts.len() + recent_commands.len());

    activity.push(MeshActivity {
        level: "info",
        code: "mesh_snapshot".into(),
        message: format!(
            "Mesh sees {} node(s), {} link(s), and {} active stream(s).",
            aggregate.node_count, aggregate.connection_count, aggregate.active_streams
        ),
        count: aggregate.node_count as u64,
        seen_unix_ms: now,
        node_id: None,
        stream_id_text: None,
    });

    activity.extend(alerts.iter().map(|alert| MeshActivity {
        level: alert.level,
        code: alert.code.into(),
        message: alert.message.clone(),
        count: alert.count,
        seen_unix_ms: alert.last_seen_unix_ms.unwrap_or(now),
        node_id: alert.node_id.clone(),
        stream_id_text: alert.stream_id_text.clone(),
    }));

    activity.extend(recent_commands.iter().map(|command| {
        let status = command.status.to_ascii_lowercase();
        let level = if status.contains("failed")
            || status.contains("timed out")
            || status.contains("error")
        {
            "error"
        } else if status.contains("skipped") {
            "warn"
        } else {
            "info"
        };
        MeshActivity {
            level,
            code: control_kind_code(command.kind).into(),
            message: format!(
                "{} command {}.",
                control_kind_label(command.kind),
                command.status
            ),
            count: 1,
            seen_unix_ms: command.created_unix_ms,
            node_id: command.node_id.clone(),
            stream_id_text: command.stream_id_text.clone(),
        }
    }));

    activity.sort_by(|left, right| {
        right
            .seen_unix_ms
            .cmp(&left.seen_unix_ms)
            .then_with(|| left.code.cmp(&right.code))
    });
    activity.truncate(MESH_ACTIVITY_LIMIT);
    activity
}

fn control_kind_code(kind: ControlKind) -> &'static str {
    match kind {
        ControlKind::ProvisionNode => "provision_node",
        ControlKind::CloseNode => "close_node",
        ControlKind::WarmStream => "warm_stream",
        ControlKind::ReplicaRequest => "replica_request",
    }
}

fn control_kind_label(kind: ControlKind) -> &'static str {
    match kind {
        ControlKind::ProvisionNode => "Provision node",
        ControlKind::CloseNode => "Close node",
        ControlKind::WarmStream => "Warm stream",
        ControlKind::ReplicaRequest => "Replica request",
    }
}

fn storage_percent(node: &MeshNode) -> u64 {
    if node.total_storage_bytes == 0 {
        return 0;
    }
    node.used_storage_bytes.saturating_mul(100) / node.total_storage_bytes
}

fn snapshot_stream_for_id(snapshot: &MeshSnapshot, stream_id: u64) -> Option<StreamTelemetry> {
    if snapshot.streams.is_empty() {
        if snapshot.stream.stream_id == stream_id {
            return Some(StreamTelemetry::from_stats(
                snapshot.node.node_id.clone(),
                &snapshot.stream,
            ));
        }
        return None;
    }

    snapshot
        .streams
        .iter()
        .find(|stream| stream.stream_id == stream_id)
        .cloned()
}

fn snapshot_stream_ids(snapshot: &MeshSnapshot) -> Vec<u64> {
    if snapshot.streams.is_empty() {
        if snapshot.stream.latest_local_part.is_some() || snapshot.stream.latest_mesh_part.is_some()
        {
            return vec![snapshot.stream.stream_id];
        }
        return Vec::new();
    }

    snapshot
        .streams
        .iter()
        .filter(|stream| stream.active())
        .map(|stream| stream.stream_id)
        .collect()
}

#[derive(Debug, Clone, Default)]
struct ControlPlane {
    commands: Arc<RwLock<Vec<ControlCommand>>>,
}

impl ControlPlane {
    async fn record(&self, kind: ControlKind, request: ControlRequest) -> ControlCommand {
        let target_text = control_request_target_text(&request);
        let command = ControlCommand {
            id: now_unix_ms(),
            kind,
            node_id: request.node_id,
            region: request.region,
            stream_id: request.stream_id,
            stream_id_text: request.stream_id.map(stream_id_text),
            target_text,
            created_unix_ms: now_unix_ms(),
            status: "accepted".into(),
        };
        let mut commands = self.commands.write().await;
        commands.push(command.clone());
        if commands.len() > 128 {
            let keep_from = commands.len() - 128;
            commands.drain(0..keep_from);
        }
        command
    }

    async fn recent(&self) -> Vec<ControlCommand> {
        self.commands
            .read()
            .await
            .iter()
            .rev()
            .take(16)
            .cloned()
            .collect()
    }

    async fn replace(&self, command: ControlCommand) {
        let mut commands = self.commands.write().await;
        if let Some(existing) = commands
            .iter_mut()
            .find(|existing| existing.id == command.id)
        {
            *existing = command;
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ControlDispatch {
    tx: Arc<RwLock<Option<mpsc::Sender<TcpChangesMessage>>>>,
}

impl ControlDispatch {
    async fn set_sender(&self, tx: mpsc::Sender<TcpChangesMessage>) {
        *self.tx.write().await = Some(tx);
    }

    async fn ready(&self) -> bool {
        self.tx.read().await.is_some()
    }

    async fn publish(&self, envelope: &ControlEnvelope) -> Result<bool> {
        let Some(tx) = self.tx.read().await.clone() else {
            return Ok(false);
        };
        let json = serde_json::to_vec(envelope).context("failed to encode control envelope")?;
        tx.send(TcpChangesMessage::new(CONTROL_TAG, vec![Bytes::from(json)]))
            .await
            .map_err(|_| anyhow!("tcp changes control feed is closed"))?;
        Ok(true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlRequest {
    node_id: Option<String>,
    region: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_u64_from_any")]
    stream_id: Option<u64>,
}

fn control_request_target_text(request: &ControlRequest) -> String {
    let mut parts = Vec::new();
    if let Some(node_id) = request.node_id.as_deref().filter(|value| !value.is_empty()) {
        parts.push(format!("node {node_id}"));
    }
    if let Some(region) = request.region.as_deref().filter(|value| !value.is_empty()) {
        parts.push(format!("region {region}"));
    }
    if let Some(stream_id) = request.stream_id {
        parts.push(format!("stream {}", stream_id_text(stream_id)));
    }
    if parts.is_empty() {
        "global".to_owned()
    } else {
        parts.join(" / ")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ControlKind {
    ProvisionNode,
    CloseNode,
    WarmStream,
    ReplicaRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlCommand {
    id: u64,
    kind: ControlKind,
    node_id: Option<String>,
    region: Option<String>,
    stream_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stream_id_text: Option<String>,
    target_text: String,
    created_unix_ms: u64,
    status: String,
}

#[derive(Debug, Clone)]
struct ProvisionExecutor {
    command: Option<String>,
    timeout: Duration,
    #[cfg(feature = "linode-provisioner")]
    linode: Option<LinodeProvisionConfig>,
}

impl ProvisionExecutor {
    fn new(command: Option<String>, timeout: Duration) -> Self {
        Self {
            command,
            timeout,
            #[cfg(feature = "linode-provisioner")]
            linode: None,
        }
    }

    #[cfg(feature = "linode-provisioner")]
    fn with_linode(mut self, linode: Option<LinodeProvisionConfig>) -> Self {
        self.linode = linode;
        self
    }

    #[cfg(test)]
    fn disabled() -> Self {
        Self {
            command: None,
            timeout: Duration::from_secs(30),
            #[cfg(feature = "linode-provisioner")]
            linode: None,
        }
    }

    fn status(&self) -> ProvisionStatus {
        let mut backends = Vec::new();
        let mut backend_statuses = Vec::new();
        if self.command.is_some() {
            backends.push("command".to_owned());
            backend_statuses.push(ProvisionBackendStatus {
                name: "command".to_owned(),
                state: "ready",
                details: vec!["shell command configured".to_owned()],
            });
        }
        #[cfg(feature = "linode-provisioner")]
        if let Some(config) = &self.linode {
            backends.push("linode".to_owned());
            backend_statuses.push(config.status());
        }
        ProvisionStatus {
            enabled: !backends.is_empty(),
            backends,
            timeout_ms: self.timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            backend_statuses,
        }
    }

    async fn run(
        &self,
        control_id: u64,
        local_node: &MeshNode,
        request: &ControlRequest,
    ) -> String {
        let mut statuses = Vec::new();
        #[cfg(feature = "linode-provisioner")]
        let mut linode_result = None;

        #[cfg(feature = "linode-provisioner")]
        if let Some(config) = &self.linode {
            let (status, result) = self.run_linode(config, local_node, request).await;
            let failed = status.contains(" failed:") || status.contains(" skipped:");
            statuses.push(status);
            linode_result = result;
            if failed {
                return statuses.join("; ");
            }
        }

        if let Some(script) = &self.command {
            statuses.push(
                self.run_command(
                    script,
                    control_id,
                    local_node,
                    request,
                    #[cfg(feature = "linode-provisioner")]
                    linode_result.as_ref(),
                )
                .await,
            );
        }

        if statuses.is_empty() {
            return "local provision skipped: no provision backend configured".into();
        }

        statuses.join("; ")
    }

    async fn run_command(
        &self,
        script: &str,
        control_id: u64,
        local_node: &MeshNode,
        request: &ControlRequest,
        #[cfg(feature = "linode-provisioner")] linode_result: Option<&linode::ScaleUpResult>,
    ) -> String {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(script)
            .env("AV_MESH_CONTROL_ID", control_id.to_string())
            .env("AV_MESH_LOCAL_NODE_ID", &local_node.node_id)
            .env("AV_MESH_LOCAL_REGION", &local_node.region)
            .env(
                "AV_MESH_PROVISION_NODE_ID",
                request.node_id.as_deref().unwrap_or(""),
            )
            .env(
                "AV_MESH_PROVISION_REGION",
                request.region.as_deref().unwrap_or(""),
            )
            .env(
                "AV_MESH_PROVISION_STREAM_ID",
                request
                    .stream_id
                    .map(|stream_id| stream_id.to_string())
                    .unwrap_or_default(),
            );

        #[cfg(feature = "linode-provisioner")]
        if let Some(result) = linode_result {
            command
                .env("AV_MESH_LINODE_INSTANCE_ID", result.instance_id.to_string())
                .env("AV_MESH_LINODE_LABEL", &result.label)
                .env("AV_MESH_LINODE_PUBLIC_IPV4", &result.public_ipv4)
                .env("AV_MESH_LINODE_PRIVATE_IPAM", &result.private_ipam_address)
                .env("AV_MESH_LINODE_VLAN_LABEL", &result.vlan_label)
                .env(
                    "AV_MESH_LINODE_DNS_NAME",
                    result.dns_name.as_deref().unwrap_or(""),
                )
                .env("AV_MESH_LINODE_REGION_CODE", &result.region_code)
                .env("AV_MESH_LINODE_REGION", &result.linode_region);
        }

        match tokio::time::timeout(self.timeout, command.output()).await {
            Err(_) => format!(
                "local provision failed: command timed out after {} ms",
                self.timeout.as_millis()
            ),
            Ok(Err(error)) => format!("local provision failed: command spawn failed: {error}"),
            Ok(Ok(output)) if output.status.success() => {
                let detail = command_output_detail(&output.stdout, &output.stderr);
                if detail.is_empty() {
                    format!("local provision executed: {}", output.status)
                } else {
                    format!("local provision executed: {}; {detail}", output.status)
                }
            }
            Ok(Ok(output)) => {
                let detail = command_output_detail(&output.stdout, &output.stderr);
                if detail.is_empty() {
                    format!("local provision failed: {}", output.status)
                } else {
                    format!("local provision failed: {}; {detail}", output.status)
                }
            }
        }
    }

    #[cfg(feature = "linode-provisioner")]
    async fn run_linode(
        &self,
        config: &LinodeProvisionConfig,
        local_node: &MeshNode,
        request: &ControlRequest,
    ) -> (String, Option<linode::ScaleUpResult>) {
        let requested_region = request.region.as_deref().unwrap_or(&local_node.region);
        let linode_region_code = config.resolve_region(requested_region);
        let Some(region_info) = LINODE_REGIONS.get(linode_region_code.as_str()) else {
            return (
                format!(
                    "local linode provision skipped: region {requested_region} resolved to unsupported Linode region {linode_region_code}"
                ),
                None,
            );
        };
        let token = match std::env::var(&config.token_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    format!(
                        "local linode provision skipped: missing {}",
                        config.token_env
                    ),
                    None,
                )
            }
        };
        let pub_key = match std::env::var(&config.pub_key_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    format!(
                        "local linode provision skipped: missing {}",
                        config.pub_key_env
                    ),
                    None,
                )
            }
        };
        let client = match LinodeClient::new(token, pub_key) {
            Ok(client) => client,
            Err(error) => {
                return (
                    format!("local linode provision failed: invalid client config: {error}"),
                    None,
                )
            }
        };

        match tokio::time::timeout(
            self.timeout,
            client.scale_up_one(
                &config.image_id,
                &config.instance_type,
                config.domain_id,
                region_info,
                &config.vlan_tag,
            ),
        )
        .await
        {
            Err(_) => (
                format!(
                    "local linode provision failed: timed out after {} ms",
                    self.timeout.as_millis()
                ),
                None,
            ),
            Ok(Err(error)) => (format!("local linode provision failed: {error}"), None),
            Ok(Ok(result)) => (
                format_linode_provision_result(requested_region, &result),
                Some(result),
            ),
        }
    }
}

#[cfg(feature = "linode-provisioner")]
#[derive(Debug, Clone)]
struct LinodeProvisionConfig {
    token_env: String,
    pub_key_env: String,
    image_id: String,
    instance_type: String,
    domain_id: u64,
    vlan_tag: String,
    region_map: BTreeMap<String, String>,
}

#[cfg(feature = "linode-provisioner")]
impl LinodeProvisionConfig {
    fn resolve_region(&self, mesh_region: &str) -> String {
        self.region_map
            .get(mesh_region)
            .cloned()
            .unwrap_or_else(|| mesh_region.to_string())
    }

    fn status(&self) -> ProvisionBackendStatus {
        let token_present = env_value_present(&self.token_env);
        let pub_key_present = env_value_present(&self.pub_key_env);
        let mut details = vec![
            format!(
                "token env {} {}",
                self.token_env,
                if token_present { "present" } else { "missing" }
            ),
            format!(
                "public key env {} {}",
                self.pub_key_env,
                if pub_key_present {
                    "present"
                } else {
                    "missing"
                }
            ),
            format!("image {}", self.image_id),
            format!("type {}", self.instance_type),
            format!("domain {}", self.domain_id),
            format!("private vlan {}", self.vlan_tag),
        ];
        if self.region_map.is_empty() {
            details.push("region map empty".to_owned());
        } else {
            details.push(format!("{} region map entries", self.region_map.len()));
        }

        ProvisionBackendStatus {
            name: "linode".to_owned(),
            state: if token_present && pub_key_present {
                "ready"
            } else {
                "blocked"
            },
            details,
        }
    }
}

#[cfg(feature = "linode-provisioner")]
fn format_linode_provision_result(
    requested_region: &str,
    result: &linode::ScaleUpResult,
) -> String {
    let dns = result.dns_name.as_deref().unwrap_or("none");
    format!(
        "local linode provisioned: requested_region={requested_region} linode_region={} instance_id={} label={} public_ipv4={} private_ipam={} vlan={} dns={dns}",
        result.linode_region,
        result.instance_id,
        result.label,
        result.public_ipv4,
        result.private_ipam_address,
        result.vlan_label
    )
}

fn command_output_detail(stdout: &[u8], stderr: &[u8]) -> String {
    let stdout = truncated_utf8(stdout);
    let stderr = truncated_utf8(stderr);
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => format!("stdout={stdout}"),
        (true, false) => format!("stderr={stderr}"),
        (false, false) => format!("stdout={stdout}; stderr={stderr}"),
    }
}

#[cfg(feature = "linode-provisioner")]
fn env_value_present(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.trim().is_empty())
}

fn truncated_utf8(bytes: &[u8]) -> String {
    const LIMIT: usize = 160;
    let mut text = String::from_utf8_lossy(&bytes[..bytes.len().min(LIMIT)])
        .trim()
        .replace('\n', "\\n");
    if bytes.len() > LIMIT {
        text.push_str("...");
    }
    text
}

struct TelemetryRuntime {
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<()>,
    finished_rx: tokio::sync::oneshot::Receiver<()>,
    publisher_task: tokio::task::JoinHandle<()>,
}

#[allow(clippy::too_many_arguments)]
async fn start_telemetry_feed(
    bind: SocketAddr,
    private_ipv4: Ipv4Addr,
    cert: String,
    key: String,
    interval_ms: u64,
    cache: Arc<LiveTsCache>,
    mesh: Arc<CacheMeshHandle>,
    node: MeshNode,
    policy: ReplicationPolicy,
    control: ControlPlane,
    lifecycle: NodeLifecycle,
    dispatch: ControlDispatch,
    playback_base_url: Option<String>,
    edge_load: EdgeLoad,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<TelemetryRuntime> {
    let server = tcp_changes::Server::new(cert, key, private_ipv4);
    let (up_rx, finished_rx, shutdown_tx, tx) = server
        .start(bind)
        .await
        .map_err(|err| anyhow!("failed to start tcp changes telemetry feed: {err}"))?;
    up_rx
        .await
        .map_err(|_| anyhow!("tcp changes telemetry feed failed before ready"))?;
    dispatch.set_sender(tx.clone()).await;

    let publisher_task = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(interval_ms));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    return;
                }
                _ = ticker.tick() => {
                    let mut node = node.clone();
                    node.draining = lifecycle.is_draining().await;
                    let snapshot = cache
                        .mesh_snapshot(&mesh, node, policy.clone(), &control)
                        .await;
                    let edge_service =
                        edge_load.snapshot(&snapshot.node, playback_base_url.clone());
                    let snapshot = snapshot.with_edge_service(edge_service);
                    match serde_json::to_vec(&snapshot) {
                        Ok(json) => {
                            debug!(
                                node_id = %snapshot.node.node_id,
                                active_streams = snapshot.node.active_streams,
                                stream_telemetry = snapshot.streams.len(),
                                peers = snapshot.peers.len(),
                                bytes = json.len(),
                                "publishing mesh telemetry snapshot"
                            );
                            let message = TcpChangesMessage::new(TELEMETRY_TAG, vec![Bytes::from(json)]);
                            if tx.send(message).await.is_err() {
                                return;
                            }
                        }
                        Err(error) => warn!(error = %error, "failed to encode mesh telemetry snapshot"),
                    }
                }
            }
        }
    });

    Ok(TelemetryRuntime {
        local_addr: bind,
        shutdown_tx,
        finished_rx,
        publisher_task,
    })
}

fn start_telemetry_collectors(
    peers: Vec<SocketAddr>,
    dns_name: String,
    ca_cert: String,
    router: AppRouter,
    telemetry_peers: TelemetryPeerMonitor,
    shutdown_rx: watch::Receiver<()>,
) -> Vec<tokio::task::JoinHandle<()>> {
    peers
        .into_iter()
        .map(|peer| {
            let dns_name = dns_name.clone();
            let ca_cert = ca_cert.clone();
            let router = router.clone();
            let telemetry_peers = telemetry_peers.clone();
            let mut shutdown_rx = shutdown_rx.clone();
            tokio::spawn(async move {
                loop {
                    let connect_shutdown_rx = shutdown_rx.clone();
                    tokio::select! {
                        _ = shutdown_rx.changed() => return,
                        result = connect_telemetry_peer(peer, &dns_name, &ca_cert, router.clone(), telemetry_peers.clone(), connect_shutdown_rx) => {
                            match result {
                                Ok(()) => {
                                    telemetry_peers.record_disconnected(peer, None).await;
                                    info!(peer = %peer, "telemetry peer collector disconnected");
                                }
                                Err(error) => {
                                    let error_text = error.to_string();
                                    telemetry_peers.record_disconnected(peer, Some(error_text.clone())).await;
                                    warn!(peer = %peer, error = %error_text, "telemetry peer collector disconnected");
                                }
                            }
                            tokio::select! {
                                _ = shutdown_rx.changed() => return,
                                _ = sleep(Duration::from_secs(1)) => {}
                            }
                        }
                    }
                }
            })
        })
        .collect()
}

async fn connect_telemetry_peer(
    peer: SocketAddr,
    dns_name: &str,
    ca_cert: &str,
    router: AppRouter,
    telemetry_peers: TelemetryPeerMonitor,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    telemetry_peers.record_connecting(peer).await;
    let client = TcpChangesClient::new(dns_name.to_string(), peer, ca_cert.to_string());
    let (up_rx, _fin_rx, client_shutdown, mut rx) = client
        .start("HELLO")
        .await
        .map_err(|err| anyhow!("failed to connect tcp changes telemetry peer {peer}: {err}"))?;
    up_rx
        .await
        .map_err(|_| anyhow!("tcp changes telemetry peer {peer} ended before ready"))?;
    telemetry_peers.record_connected(peer).await;
    info!(peer = %peer, "telemetry peer collector connected");

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                let _ = client_shutdown.send(());
                return Ok(());
            }
            payload = rx.recv() => {
                let Some(payload) = payload else {
                    return Ok(());
                };
                debug!(
                    peer = %peer,
                    tag = ?payload.tag,
                    bytes = payload.val.len(),
                    "received tcp-changes telemetry payload"
                );
                telemetry_peers.record_payload(peer, payload.val.len()).await;
                if let Err(error) = router.ingest_tcp_changes_payload(payload).await {
                    warn!(peer = %peer, error = %error, "failed to ingest tcp changes payload");
                }
            }
        }
    }
}

#[derive(Clone)]
struct AppRouter {
    cache: Arc<LiveTsCache>,
    mesh: Arc<CacheMeshHandle>,
    audio_epochs: broadcast::Sender<AudioEpochDatagram>,
    mesh_transport: MeshTransportConfigSnapshot,
    node: MeshNode,
    replication_policy: ReplicationPolicy,
    control: ControlPlane,
    dispatch: ControlDispatch,
    telemetry: TelemetryAggregator,
    demand: DemandTracker,
    lifecycle: NodeLifecycle,
    playback_base_url: Option<String>,
    edge_load: EdgeLoad,
    provision: ProvisionExecutor,
    telemetry_peers: TelemetryPeerMonitor,
    private_discovery: PrivateDiscoveryStatus,
}

impl AppRouter {
    #[allow(clippy::too_many_arguments)]
    fn new(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        audio_epochs: broadcast::Sender<AudioEpochDatagram>,
        mesh_transport: MeshTransportConfigSnapshot,
        node: MeshNode,
        replication_policy: ReplicationPolicy,
        control: ControlPlane,
        dispatch: ControlDispatch,
        telemetry: TelemetryAggregator,
        demand: DemandTracker,
        lifecycle: NodeLifecycle,
        playback_base_url: Option<String>,
        edge_load: EdgeLoad,
        provision: ProvisionExecutor,
        telemetry_peers: TelemetryPeerMonitor,
        private_discovery: PrivateDiscoveryStatus,
    ) -> Self {
        Self {
            cache,
            mesh,
            audio_epochs,
            mesh_transport,
            node,
            replication_policy,
            control,
            dispatch,
            telemetry,
            demand,
            lifecycle,
            playback_base_url,
            edge_load,
            provision,
            telemetry_peers,
            private_discovery,
        }
    }

    async fn local_mesh_snapshot(&self) -> MeshSnapshot {
        let mut node = self.node.clone();
        node.draining = self.lifecycle.is_draining().await;
        let snapshot = self
            .cache
            .mesh_snapshot(
                &self.mesh,
                node.clone(),
                self.replication_policy.clone(),
                &self.control,
            )
            .await;
        snapshot.with_edge_service(
            self.edge_load
                .snapshot(&node, self.playback_base_url.clone()),
        )
    }

    async fn mesh_api_snapshot(&self) -> MeshApiSnapshot {
        let local = self.local_mesh_snapshot().await;
        let (snapshots, telemetry) = self.telemetry.snapshots_with_local(local.clone()).await;
        let planned_replicas = self
            .plan_all_active_replicas_from_snapshots(&snapshots)
            .await;
        let mut snapshot = MeshApiSnapshot::from_snapshots(
            local,
            snapshots,
            telemetry,
            planned_replicas,
            self.orchestration_status().await,
        );
        snapshot.mesh_transport = self.mesh_transport.clone();
        snapshot.mesh_fec = self.mesh.fec_stats().into();
        snapshot
    }

    async fn prometheus_metrics(&self) -> Bytes {
        Bytes::from(render_mesh_prometheus_metrics(
            &self.mesh_api_snapshot().await,
        ))
    }

    fn record_edge_response(
        &self,
        method: &Method,
        path: &str,
        query: Option<&str>,
        response: HandlerResponse,
        started: Instant,
    ) -> HandlerResponse {
        if path == "/live/stream.m3u8" || path.starts_with("/live/") {
            self.edge_load
                .record_response(method, path, query, &response, started.elapsed());
        }
        response
    }

    async fn orchestration_status(&self) -> OrchestrationStatus {
        OrchestrationStatus {
            control_dispatch_ready: self.dispatch.ready().await,
            provision: self.provision.status(),
            telemetry_peers: self.telemetry_peers.snapshot().await,
            private_discovery: self.private_discovery.clone(),
        }
    }

    async fn mesh_protocol_response_from_bytes(&self, bytes: &[u8]) -> MeshProtocolResponse {
        match self.parse_mesh_protocol_request(bytes) {
            Ok(request) => self.handle_mesh_protocol_request(request).await,
            Err(error) => MeshProtocolResponse::error(error),
        }
    }

    fn parse_mesh_protocol_request(
        &self,
        bytes: &[u8],
    ) -> std::result::Result<MeshProtocolRequest, String> {
        let text = std::str::from_utf8(bytes).map_err(|err| err.to_string())?;
        if text.trim().eq_ignore_ascii_case("snapshot") {
            return Ok(MeshProtocolRequest::Snapshot);
        }
        serde_json::from_str(text).map_err(|err| err.to_string())
    }

    async fn handle_mesh_protocol_request(
        &self,
        request: MeshProtocolRequest,
    ) -> MeshProtocolResponse {
        match request {
            MeshProtocolRequest::Snapshot => {
                MeshProtocolResponse::snapshot(self.mesh_api_snapshot().await)
            }
            MeshProtocolRequest::ProvisionNode { node_id, region } => {
                let command = self
                    .execute_control(
                        ControlKind::ProvisionNode,
                        ControlRequest {
                            node_id,
                            region,
                            stream_id: None,
                        },
                    )
                    .await;
                MeshProtocolResponse::command(command)
            }
            MeshProtocolRequest::CloseNode { node_id, region } => {
                let command = self
                    .execute_control(
                        ControlKind::CloseNode,
                        ControlRequest {
                            node_id,
                            region,
                            stream_id: None,
                        },
                    )
                    .await;
                MeshProtocolResponse::command(command)
            }
            MeshProtocolRequest::WarmStream { stream_id, region } => {
                let command = self
                    .execute_control(
                        ControlKind::WarmStream,
                        ControlRequest {
                            node_id: None,
                            region,
                            stream_id,
                        },
                    )
                    .await;
                MeshProtocolResponse::command(command)
            }
        }
    }

    async fn mesh_protocol_response_json(&self, bytes: &[u8]) -> HandlerResult<Bytes> {
        let response = self.mesh_protocol_response_from_bytes(bytes).await;
        serde_json::to_vec(&response)
            .map(Bytes::from)
            .map_err(|err| ServerError::Handler(Box::new(err)))
    }

    async fn binary_mesh_response_from_bytes(&self, bytes: Bytes) -> HandlerResult<Bytes> {
        if bytes.is_empty() {
            return serde_json::to_vec(&MeshProtocolResponse::snapshot(
                self.mesh_api_snapshot().await,
            ))
            .map(Bytes::from)
            .map_err(|err| ServerError::Handler(Box::new(err)));
        }

        if let Some(unit) = self
            .ingest_serialized_media_access_unit(bytes.clone())
            .await
            .map_err(ServerError::Config)?
        {
            return serde_json::to_vec(&MeshProtocolResponse::media_access_unit(
                MediaAccessUnitResponse::from_cached(&unit),
            ))
            .map(Bytes::from)
            .map_err(|err| ServerError::Handler(Box::new(err)));
        }

        self.mesh_protocol_response_json(&bytes).await
    }

    async fn webtransport_response_from_bytes(&self, bytes: Bytes) -> HandlerResult<Bytes> {
        self.binary_mesh_response_from_bytes(bytes).await
    }

    async fn ingest_serialized_media_access_unit(
        &self,
        bytes: Bytes,
    ) -> std::result::Result<Option<CachedMediaAccessUnit>, String> {
        let Some(unit) = decode_serialized_media_access_unit(bytes)? else {
            return Ok(None);
        };
        self.cache
            .add_media_access_unit(unit.metadata, unit.payload)
            .await
            .map(Some)
            .map_err(|err| err.to_string())
    }

    async fn ingest_tcp_changes_payload(&self, payload: TcpChangesPayload) -> Result<bool> {
        match payload.tag {
            TELEMETRY_TAG => self.telemetry.ingest_payload(payload).await,
            CONTROL_TAG => self.ingest_control_payload(payload.val).await,
            _ => Ok(false),
        }
    }

    async fn ingest_control_payload(&self, bytes: Bytes) -> Result<bool> {
        let envelope: ControlEnvelope =
            serde_json::from_slice(&bytes).context("failed to decode AVMC control payload")?;
        if envelope.origin_node_id == self.node.node_id {
            return Ok(false);
        }
        if !self.control_targets_local(&envelope.request, &envelope.target_node_ids) {
            return Ok(false);
        }

        let mut command = self
            .execute_control_internal_with_targets(
                envelope.kind,
                envelope.request,
                false,
                envelope.target_node_ids,
            )
            .await;
        command.status = format!(
            "received from {} command {}; {}",
            envelope.origin_node_id, envelope.id, command.status
        );
        self.control.replace(command).await;
        Ok(true)
    }

    async fn execute_control(&self, kind: ControlKind, request: ControlRequest) -> ControlCommand {
        self.execute_control_internal(kind, request, true).await
    }

    async fn execute_control_internal(
        &self,
        kind: ControlKind,
        request: ControlRequest,
        dispatch: bool,
    ) -> ControlCommand {
        let target_node_ids = if dispatch {
            self.selected_control_target_node_ids(kind, &request).await
        } else {
            Vec::new()
        };
        self.execute_control_internal_with_targets(kind, request, dispatch, target_node_ids)
            .await
    }

    async fn execute_control_internal_with_targets(
        &self,
        kind: ControlKind,
        request: ControlRequest,
        dispatch: bool,
        target_node_ids: Vec<String>,
    ) -> ControlCommand {
        let mut command = self.control.record(kind, request.clone()).await;
        let mut statuses = vec![
            self.apply_control_locally(kind, &request, dispatch, command.id, &target_node_ids)
                .await,
        ];

        if dispatch {
            let envelope = ControlEnvelope {
                id: command.id,
                origin_node_id: self.node.node_id.clone(),
                kind,
                request,
                target_node_ids: target_node_ids.clone(),
            };
            let status = match self.dispatch.publish(&envelope).await {
                Ok(true) if target_node_ids.is_empty() => "published AVMC control".to_string(),
                Ok(true) => format!("published AVMC control to {}", target_node_ids.join(",")),
                Ok(false) => "no tcp-changes control publisher".to_string(),
                Err(error) => format!("control publish failed: {error}"),
            };
            statuses.push(status);
        }

        command.status = statuses.join("; ");
        self.control.replace(command.clone()).await;
        command
    }

    async fn apply_control_locally(
        &self,
        kind: ControlKind,
        request: &ControlRequest,
        originated_locally: bool,
        control_id: u64,
        target_node_ids: &[String],
    ) -> String {
        if kind != ControlKind::ProvisionNode
            && !self.control_targets_local(request, target_node_ids)
        {
            return "local skipped: target does not match".into();
        }

        match kind {
            ControlKind::WarmStream => {
                let Some(stream_id) = request.stream_id else {
                    return "local warm skipped: missing stream id".into();
                };
                let _ = self
                    .cache
                    .chunk_cache
                    .get_or_create_stream_idx(stream_id)
                    .await;
                self.request_replica_for_stream(
                    stream_id,
                    "warm-stream",
                    request.region.as_deref(),
                )
                .await;
                "local warm requested".into()
            }
            ControlKind::CloseNode => {
                self.lifecycle.set_draining(true).await;
                "local close requested: draining".into()
            }
            ControlKind::ProvisionNode => {
                if !originated_locally {
                    return "remote provision noted: executor only runs at command origin".into();
                }
                self.provision.run(control_id, &self.node, request).await
            }
            ControlKind::ReplicaRequest => "local replica request command ignored".into(),
        }
    }

    fn control_targets_local(&self, request: &ControlRequest, target_node_ids: &[String]) -> bool {
        if !target_node_ids.is_empty() {
            return target_node_ids
                .iter()
                .any(|node_id| node_id == &self.node.node_id);
        }
        if let Some(node_id) = &request.node_id {
            return node_id == &self.node.node_id;
        }
        if let Some(region) = &request.region {
            return region == &self.node.region;
        }
        true
    }

    async fn selected_control_target_node_ids(
        &self,
        kind: ControlKind,
        request: &ControlRequest,
    ) -> Vec<String> {
        if kind == ControlKind::ProvisionNode {
            return Vec::new();
        }
        if let Some(node_id) = &request.node_id {
            return vec![node_id.clone()];
        }
        let Some(region) = &request.region else {
            return Vec::new();
        };

        let local = self.local_mesh_snapshot().await;
        let (snapshots, _) = self.telemetry.snapshots_with_local(local).await;
        let mut node_ids = snapshots
            .into_iter()
            .filter(|snapshot| snapshot.node.region == *region)
            .map(|snapshot| snapshot.node.node_id)
            .collect::<Vec<_>>();
        node_ids.sort();
        node_ids.dedup();
        node_ids
    }

    async fn mesh_sse_event(&self) -> HandlerResult<Bytes> {
        let snapshot = self.mesh_api_snapshot().await;
        let json =
            serde_json::to_vec(&snapshot).map_err(|err| ServerError::Handler(Box::new(err)))?;
        let mut event = BytesMut::new();
        event.put_slice(b"event: mesh\n");
        event.put_slice(b"data: ");
        event.put_slice(&json);
        event.put_slice(b"\n\n");
        Ok(event.freeze())
    }

    fn plan_replicas_from_snapshots(
        &self,
        stream_id: u64,
        local_bytes: u64,
        snapshots: &[MeshSnapshot],
        demand: &[DemandSignal],
    ) -> Vec<ReplicaPlacement> {
        let nodes = snapshots
            .iter()
            .map(|snapshot| snapshot.node.clone())
            .collect::<Vec<_>>();
        let mut existing_replicas = snapshots
            .iter()
            .filter(|snapshot| {
                snapshot_stream_for_id(snapshot, stream_id)
                    .map(|stream| stream.active())
                    .unwrap_or(false)
            })
            .map(|snapshot| snapshot.node.node_id.clone())
            .collect::<HashSet<_>>();

        if local_bytes > 0 {
            existing_replicas.insert(self.node.node_id.clone());
        }

        let telemetry_bytes = snapshots
            .iter()
            .filter_map(|snapshot| snapshot_stream_for_id(snapshot, stream_id))
            .filter_map(|stream| stream.latest_local_part_bytes)
            .map(|bytes| bytes as u64)
            .max()
            .unwrap_or(0)
            .saturating_mul(self.cache.window_parts as u64);
        let stream = StreamInfo {
            stream_id,
            bytes: local_bytes.max(telemetry_bytes).max(1),
            contributor_node_id: snapshots
                .iter()
                .find(|snapshot| {
                    snapshot_stream_for_id(snapshot, stream_id)
                        .map(|stream| stream.latest_local_part.is_some())
                        .unwrap_or(false)
                })
                .map(|snapshot| snapshot.node.node_id.clone()),
            active: !existing_replicas.is_empty(),
        };
        if !stream.active && demand.is_empty() {
            return Vec::new();
        }

        self.replication_policy
            .plan_replicas(&stream, &nodes, &existing_replicas, demand)
    }

    async fn plan_all_active_replicas_from_snapshots(
        &self,
        snapshots: &[MeshSnapshot],
    ) -> Vec<ReplicaPlacement> {
        let mut stream_ids = self.cache.active_stream_ids().await;
        for snapshot in snapshots {
            stream_ids.extend(snapshot_stream_ids(snapshot));
        }
        stream_ids.sort_unstable();
        stream_ids.dedup();

        let mut planned = Vec::new();
        for stream_id in stream_ids {
            let local_bytes = self
                .cache
                .estimated_storage_bytes_for_stream(stream_id)
                .await;
            planned.extend(self.plan_replicas_from_snapshots(
                stream_id,
                local_bytes,
                snapshots,
                &[],
            ));
        }
        planned
    }

    async fn request_replica_for_stream(
        &self,
        stream_id: u64,
        reason: &'static str,
        demand_region: Option<&str>,
    ) {
        let now_ms = now_unix_ms();
        if !self.demand.should_request_replica(stream_id, now_ms).await {
            return;
        }

        let local = self.local_mesh_snapshot().await;
        let (snapshots, _) = self.telemetry.snapshots_with_local(local).await;
        let region = demand_region.unwrap_or(&self.node.region);
        let continent = snapshots
            .iter()
            .find(|snapshot| snapshot.node.region == region)
            .map(|snapshot| snapshot.node.continent.as_str())
            .unwrap_or(&self.node.continent);
        let demand = [DemandSignal {
            stream_id,
            requester_node_id: self.node.node_id.clone(),
            region: region.to_string(),
            continent: continent.to_string(),
            active_readers: self.replication_policy.demand_active_readers.max(1),
            reads_per_sec: self.replication_policy.demand_reads_per_sec.max(1.0),
            observed_unix_ms: now_ms,
        }];
        let local_bytes = self
            .cache
            .estimated_storage_bytes_for_stream(stream_id)
            .await;
        let planned_replicas =
            self.plan_replicas_from_snapshots(stream_id, local_bytes, &snapshots, &demand);
        let from_slot = self.cache.replica_request_from_slot(stream_id).await;
        match self.mesh.request_replica(stream_id, from_slot).await {
            Ok(peer_count) => {
                let mut command = self
                    .control
                    .record(
                        ControlKind::ReplicaRequest,
                        ControlRequest {
                            node_id: Some(self.node.node_id.clone()),
                            region: Some(region.to_string()),
                            stream_id: Some(stream_id),
                        },
                    )
                    .await;
                command.status =
                    format_replica_request_status(reason, peer_count, &planned_replicas);
                self.control.replace(command).await;
            }
            Err(error) => {
                warn!(
                    stream_id,
                    from_slot,
                    error = %error,
                    "failed to request mesh replica"
                );
            }
        }
    }

    async fn known_active_stream_ids(&self) -> Vec<u64> {
        let local = self.local_mesh_snapshot().await;
        let (snapshots, _) = self.telemetry.snapshots_with_local(local).await;
        let mut stream_ids = self.cache.active_stream_ids().await;
        for snapshot in snapshots {
            stream_ids.extend(snapshot_stream_ids(&snapshot));
        }
        stream_ids.sort_unstable();
        stream_ids.dedup();
        stream_ids
    }

    async fn request_planned_local_replicas(&self, reason: &'static str) -> Vec<u64> {
        let mut requested = Vec::new();
        for stream_id in self.known_active_stream_ids().await {
            if self.request_planned_local_replica(stream_id, reason).await {
                requested.push(stream_id);
            }
        }
        requested
    }

    async fn request_planned_local_replica(&self, stream_id: u64, reason: &'static str) -> bool {
        let local = self.local_mesh_snapshot().await;
        let (snapshots, _) = self.telemetry.snapshots_with_local(local).await;
        let local_bytes = self
            .cache
            .estimated_storage_bytes_for_stream(stream_id)
            .await;
        let planned_replicas =
            self.plan_replicas_from_snapshots(stream_id, local_bytes, &snapshots, &[]);
        if !planned_replicas
            .iter()
            .any(|placement| placement.target_node_id == self.node.node_id)
        {
            return false;
        }

        let now_ms = now_unix_ms();
        if !self.demand.should_request_replica(stream_id, now_ms).await {
            return false;
        }

        let from_slot = self.cache.replica_request_from_slot(stream_id).await;
        match self.mesh.request_replica(stream_id, from_slot).await {
            Ok(peer_count) => {
                if peer_count > 0 {
                    let mut command = self
                        .control
                        .record(
                            ControlKind::ReplicaRequest,
                            ControlRequest {
                                node_id: Some(self.node.node_id.clone()),
                                region: Some(self.node.region.clone()),
                                stream_id: Some(stream_id),
                            },
                        )
                        .await;
                    command.status =
                        format_replica_request_status(reason, peer_count, &planned_replicas);
                    self.control.replace(command).await;
                }
                true
            }
            Err(error) => {
                warn!(
                    stream_id,
                    from_slot,
                    error = %error,
                    "failed to request planned mesh replica"
                );
                false
            }
        }
    }
}

async fn run_replication_planner(
    router: AppRouter,
    plan_interval: Duration,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut ticker = interval(plan_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = ticker.tick() => {
                let requested_streams = router
                    .request_planned_local_replicas("baseline-replication")
                    .await;
                for stream_id in requested_streams {
                    debug!(
                        node_id = router.node.node_id,
                        stream_id,
                        "baseline replication planner requested local replica"
                    );
                }
            }
        }
    }
}

#[async_trait]
impl Router for AppRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        let started = Instant::now();
        let method = req.method().clone();
        let path_owned = req.uri().path().to_owned();
        let query_owned = req.uri().query().map(ToOwned::to_owned);
        let path = path_owned.as_str();
        let query = query_owned.as_deref();

        if req.method() == Method::OPTIONS {
            let response = response(StatusCode::NO_CONTENT, None, None);
            return Ok(self.record_edge_response(&method, path, query, response, started));
        }
        if req.method() != Method::GET && req.method() != Method::HEAD {
            let response = response(StatusCode::METHOD_NOT_ALLOWED, None, None);
            return Ok(self.record_edge_response(&method, path, query, response, started));
        }

        match path {
            "/" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(
                    b"av-mesh playback edge\n\nNeedletail Mission Control: /mesh\nHLS: /live/stream.m3u8\nHealth: /up\nStats: /api/stats\nMetrics: /metrics\n",
                )),
                Some("text/plain; charset=utf-8"),
            )),
            "/mesh" => Ok(mission_control_asset_response(path).unwrap_or_else(|| {
                mission_control_setup_response()
            })),
            "/up" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(b"OK")),
                Some("text/plain"),
            )),
            "/live/stream.m3u8" => {
                self.request_replica_for_stream(self.cache.stream_id, "playlist-demand", None)
                    .await;
                let playlist = self.cache.playlist().await;
                let response = response(
                    StatusCode::OK,
                    Some(Bytes::from(playlist)),
                    Some("application/vnd.apple.mpegurl"),
                )
                .with_no_store();
                Ok(self.record_edge_response(&method, path, query, response, started))
            }
            "/api/stats" => {
                let json = serde_json::to_vec(&self.cache.stats(&self.mesh).await)
                    .map_err(|err| ServerError::Handler(Box::new(err)))?;
                Ok(response(
                    StatusCode::OK,
                    Some(Bytes::from(json)),
                    Some("application/json"),
                )
                .with_no_store())
            }
            "/api/mesh" => {
                let snapshot = self.mesh_api_snapshot().await;
                let json = serde_json::to_vec(&snapshot)
                    .map_err(|err| ServerError::Handler(Box::new(err)))?;
                Ok(response(
                    StatusCode::OK,
                    Some(Bytes::from(json)),
                    Some("application/json"),
                )
                .with_no_store())
            }
            MESH_METRICS_PATH => Ok(response(
                StatusCode::OK,
                Some(self.prometheus_metrics().await),
                Some(PROMETHEUS_CONTENT_TYPE),
            )
            .with_no_store()),
            _ => {
                if let Some(mission_control_asset) = mission_control_asset_response(path) {
                    return Ok(mission_control_asset);
                }

                if let Some(stream_id) = parse_stream_playlist_path(path) {
                    self.request_replica_for_stream(stream_id, "playlist-demand", None)
                        .await;
                    let playlist = self.cache.playlist_for_stream_id(stream_id).await;
                    let response = response(
                        StatusCode::OK,
                        Some(Bytes::from(playlist)),
                        Some("application/vnd.apple.mpegurl"),
                    )
                    .with_no_store();
            return Ok(self.record_edge_response(&method, path, query, response, started));
                }

                if let Some(stream_id) = parse_llhls_tail_path(path) {
                    self.request_replica_for_stream(stream_id, "llhls-tail-demand", None)
                        .await;
                    let read = self.edge_load.begin_read(true);
                    let after = parse_query_u64(query, "after");
                    let start_at_oldest = parse_query_u64(query, "from") == Some(0);
                    let Some((sequence, bytes, hash)) = self
                        .cache
                        .next_part_after_blocking_for_stream_id(
                            stream_id,
                            after,
                            start_at_oldest,
                        )
                        .await
                    else {
                        read.finish(0);
                        let response = response(StatusCode::NO_CONTENT, None, None).with_no_store();
            return Ok(self.record_edge_response(&method, path, query, response, started));
                    };
                    let bytes_len = bytes.len();
                    let media_kind = self
                        .cache
                        .media_kind_hint(stream_id)
                        .await
                        .unwrap_or(LiveMediaKind::Ts);
                    let available_unix_us = self
                        .cache
                        .part_available_unix_us(stream_id, sequence)
                        .await;
                    let mut tail_response =
                        response(StatusCode::OK, Some(bytes), Some(media_kind.content_type()))
                            .with_etag(hash)
                            .with_part_available_unix_us(available_unix_us)
                            .with_no_store();
                    tail_response
                        .headers
                        .push(("x-sequence".into(), sequence.to_string().into()));
                    tail_response
                        .headers
                        .push(("x-av-stream-id".into(), stream_id.to_string().into()));
                    read.finish(bytes_len);
                    return Ok(self.record_edge_response(&method, path, query, tail_response, started));
                }

                if let Some(stream_id) = parse_stream_init_path(path) {
                    self.request_replica_for_stream(stream_id, "playlist-init-demand", None)
                        .await;
                    if let Some(init) = self.cache.get_init_for_stream_id(stream_id).await {
                        let response = response(
                            StatusCode::OK,
                            Some(init),
                            Some(LiveMediaKind::Fmp4.content_type()),
                        )
                        .with_no_store();
            return Ok(self.record_edge_response(&method, path, query, response, started));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
            return Ok(self.record_edge_response(&method, path, query, response, started));
                }

                if let Some((stream_id, sequence)) = parse_media_unit_path(path) {
                    self.request_replica_for_stream(stream_id, "media-demand", None)
                        .await;
                    let Some(unit) = self.cache.get_media_access_unit(stream_id, sequence).await
                    else {
                        let response = response(StatusCode::NOT_FOUND, None, None);
            return Ok(self.record_edge_response(&method, path, query, response, started));
                    };
                    let mut media_response = response(
                        StatusCode::OK,
                        Some(unit.serialized),
                        Some(MEDIA_ACCESS_UNIT_CONTENT_TYPE),
                    )
                    .with_etag(unit.hash)
                    .with_no_store();
                    media_response.headers.push((
                        "x-av-stream-id".into(),
                        unit.metadata.stream_id.to_string().into(),
                    ));
                    media_response
                        .headers
                        .push(("x-av-sequence".into(), unit.metadata.sequence.to_string().into()));
                    media_response
                        .headers
                        .push(("x-av-codec".into(), codec_name(unit.metadata.codec).into()));
                    media_response
                        .headers
                        .push(("x-av-pts-ms".into(), unit.metadata.pts_ms.to_string().into()));
                    media_response.headers.push((
                        "x-av-duration-ms".into(),
                        unit.metadata.duration_ms.to_string().into(),
                    ));
                    media_response
                        .headers
                        .push(("x-av-flags".into(), unit.metadata.flags.bits().to_string().into()));
                    return Ok(self.record_edge_response(&method, path, query, media_response, started));
                }

                if let Some((stream_id, seq, requested_kind)) = parse_stream_part_path(path) {
                    self.request_replica_for_stream(stream_id, "playlist-part-demand", None)
                        .await;
                    if let Some((bytes, hash)) =
                        self.cache.get_part_blocking_for_stream_id(stream_id, seq).await
                    {
                        let media_kind = self
                            .cache
                            .media_kind_hint(stream_id)
                            .await
                            .unwrap_or(requested_kind);
                        let available_unix_us = self
                            .cache
                            .part_available_unix_us(stream_id, seq)
                            .await;
                        let response =
                            response(StatusCode::OK, Some(bytes), Some(media_kind.content_type()))
                                .with_etag(hash)
                                .with_part_available_unix_us(available_unix_us);
            return Ok(self.record_edge_response(&method, path, query, response, started));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
            return Ok(self.record_edge_response(&method, path, query, response, started));
                }

                if let Some((stream_id, segment, requested_kind)) = parse_stream_segment_path(path) {
                    self.request_replica_for_stream(stream_id, "playlist-segment-demand", None)
                        .await;
                    if let Some(bytes) = self.cache.get_segment_for_stream_id(stream_id, segment).await
                    {
                        let media_kind = self
                            .cache
                            .media_kind_hint(stream_id)
                            .await
                            .unwrap_or(requested_kind);
                        let response = response(
                            StatusCode::OK,
                            Some(bytes),
                            Some(media_kind.content_type()),
                        );
            return Ok(self.record_edge_response(&method, path, query, response, started));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
            return Ok(self.record_edge_response(&method, path, query, response, started));
                }

                if parse_init_path(path) {
                    self.request_replica_for_stream(
                        self.cache.stream_id,
                        "playlist-init-demand",
                        None,
                    )
                    .await;
                    if let Some(init) = self.cache.get_init_for_stream_id(self.cache.stream_id).await
                    {
                        let response = response(
                            StatusCode::OK,
                            Some(init),
                            Some(LiveMediaKind::Fmp4.content_type()),
                        )
                        .with_no_store();
            return Ok(self.record_edge_response(&method, path, query, response, started));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
            return Ok(self.record_edge_response(&method, path, query, response, started));
                }

                if let Some((seq, requested_kind)) = parse_part_path(path) {
                    if let Some((bytes, hash)) = self.cache.get_part_blocking(seq).await {
                        let media_kind = self
                            .cache
                            .media_kind_hint(self.cache.stream_id)
                            .await
                            .unwrap_or(requested_kind);
                        let available_unix_us = self
                            .cache
                            .part_available_unix_us(self.cache.stream_id, seq)
                            .await;
                        let response =
                            response(StatusCode::OK, Some(bytes), Some(media_kind.content_type()))
                                .with_etag(hash)
                                .with_part_available_unix_us(available_unix_us);
            return Ok(self.record_edge_response(&method, path, query, response, started));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
            return Ok(self.record_edge_response(&method, path, query, response, started));
                }

                if let Some((segment, requested_kind)) = parse_segment_path(path) {
                    if let Some(bytes) = self.cache.get_segment(segment).await {
                        let media_kind = self
                            .cache
                            .media_kind_hint(self.cache.stream_id)
                            .await
                            .unwrap_or(requested_kind);
                        let response = response(
                            StatusCode::OK,
                            Some(bytes),
                            Some(media_kind.content_type()),
                        );
            return Ok(self.record_edge_response(&method, path, query, response, started));
                    }
                    let response = response(StatusCode::NOT_FOUND, None, None);
            return Ok(self.record_edge_response(&method, path, query, response, started));
                }

                let response = response(StatusCode::NOT_FOUND, None, None);
                Ok(self.record_edge_response(&method, path, query, response, started))
            }
        }
    }

    async fn route_body(
        &self,
        req: Request<()>,
        mut body: BodyStream,
    ) -> HandlerResult<HandlerResponse> {
        let path = req.uri().path().to_string();
        if path.starts_with("/api/control/") {
            if req.method() != Method::POST {
                return Ok(response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    Some(Bytes::from_static(b"use POST for control commands\n")),
                    Some("text/plain"),
                ));
            }

            let request = read_control_request(&mut body).await?;
            let Some(kind) = control_kind_from_path(&path) else {
                return Ok(response(StatusCode::NOT_FOUND, None, None));
            };
            let command = self.execute_control(kind, request).await;
            let json =
                serde_json::to_vec(&command).map_err(|err| ServerError::Handler(Box::new(err)))?;
            return Ok(response(
                StatusCode::ACCEPTED,
                Some(Bytes::from(json)),
                Some("application/json"),
            )
            .with_no_store());
        }

        self.route(req).await
    }

    fn has_body_handler(&self, path: &str) -> bool {
        path.starts_with("/api/control/")
    }

    fn is_streaming(&self, path: &str) -> bool {
        path == MESH_EVENTS_PATH
    }

    async fn route_stream(
        &self,
        req: Request<()>,
        mut stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        if req.uri().path() != MESH_EVENTS_PATH {
            return Err(ServerError::Config("unsupported streaming path".into()));
        }

        let response = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-store, max-age=0")
            .body(())
            .map_err(ServerError::Http)?;
        stream_writer.send_response(response).await?;

        let mut ticker = interval(Duration::from_secs(1));
        loop {
            stream_writer
                .send_data(self.mesh_sse_event().await?)
                .await?;
            ticker.tick().await;
        }
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        Some(self)
    }

    fn websocket_handler(&self, path: &str) -> Option<&dyn WebSocketHandler> {
        if path == MESH_WEBSOCKET_PATH {
            Some(self)
        } else {
            None
        }
    }
}

#[async_trait]
impl WebSocketHandler for AppRouter {
    async fn handle_websocket(
        &self,
        req: Request<()>,
        mut stream: WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
    ) -> HandlerResult<()> {
        if req.uri().path() != MESH_WEBSOCKET_PATH {
            return Err(ServerError::Config("unsupported websocket path".into()));
        }

        let initial = serde_json::to_vec(&MeshProtocolResponse::snapshot(
            self.mesh_api_snapshot().await,
        ))
        .map_err(|err| ServerError::Handler(Box::new(err)))?;
        stream
            .send(WebSocketMessage::Text(
                String::from_utf8(initial)
                    .map_err(|err| ServerError::Handler(Box::new(err)))?
                    .into(),
            ))
            .await
            .map_err(|err| ServerError::Handler(Box::new(err)))?;

        while let Some(frame) = stream.next().await {
            match frame {
                Ok(WebSocketMessage::Text(text)) => {
                    let response = self.mesh_protocol_response_json(text.as_bytes()).await?;
                    stream
                        .send(WebSocketMessage::Text(
                            String::from_utf8(response.to_vec())
                                .map_err(|err| ServerError::Handler(Box::new(err)))?
                                .into(),
                        ))
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                }
                Ok(WebSocketMessage::Binary(bytes)) => {
                    let response = self.binary_mesh_response_from_bytes(bytes).await?;
                    stream
                        .send(WebSocketMessage::Binary(response))
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                }
                Ok(WebSocketMessage::Ping(bytes)) => {
                    stream
                        .send(WebSocketMessage::Pong(bytes))
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                }
                Ok(WebSocketMessage::Close(frame)) => {
                    let _ = stream.close(frame).await;
                    break;
                }
                Ok(_) => {}
                Err(err) => return Err(ServerError::Handler(Box::new(err))),
            }
        }

        Ok(())
    }

    fn can_handle(&self, path: &str) -> bool {
        path == MESH_WEBSOCKET_PATH
    }
}

#[async_trait]
impl WebTransportHandler for AppRouter {
    async fn handle_session(
        &self,
        session: WebTransportSession<h3_quinn::Connection, Bytes>,
    ) -> HandlerResult<()> {
        let mut datagram_reader = session.datagram_reader();
        let mut datagram_sender = session.datagram_sender();
        let mut media_decoder = WebTransportMediaDecoder::new();

        loop {
            tokio::select! {
                accepted = session.accept_bi() => {
                    let accepted = accepted
                        .map_err(|err| ServerError::Config(format!("accept WebTransport bidi: {err}")))?;
                    let Some(AcceptedBi::BidiStream(stream_session_id, mut stream)) = accepted else {
                        return Ok(());
                    };
                    if stream_session_id != session.session_id() {
                        return Err(ServerError::Config(
                            "WebTransport stream used the wrong session id".into(),
                        ));
                    }

                    let mut request = Vec::new();
                    stream
                        .read_to_end(&mut request)
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                    let response = self
                        .webtransport_response_from_bytes(Bytes::from(request))
                        .await?;
                    stream
                        .write_all(&response)
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                    stream
                        .flush()
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                    stream
                        .shutdown()
                        .await
                        .map_err(|err| ServerError::Handler(Box::new(err)))?;
                }
                datagram = datagram_reader.read_datagram() => {
                    let datagram = datagram
                        .map_err(|err| ServerError::Config(format!("read WebTransport datagram: {err}")))?;
                    let payload = datagram.into_payload();
                    if let Some(requested_session_id) = parse_audio_epoch_subscription(&payload) {
                        let mut audio_epochs = self.audio_epochs.subscribe();
                        info!(?requested_session_id, "WebTransport multichannel audio epoch session started");
                        loop {
                            match audio_epochs.recv().await {
                                Ok(datagram) => {
                                    if requested_session_id.is_some()
                                        && datagram.session_id != requested_session_id
                                    {
                                        continue;
                                    }
                                    if let Err(error) = datagram_sender.send_datagram(datagram.bytes) {
                                        debug!("WebTransport audio epoch session closed: {:?}", error);
                                        break;
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                    debug!(
                                        skipped,
                                        "WebTransport audio epoch receiver skipped stale datagrams"
                                    );
                                }
                                Err(broadcast::error::RecvError::Closed) => break,
                            }
                        }
                        return Ok(());
                    }
                    match media_decoder.push_datagram(&payload) {
                        Ok(Some(frame)) => {
                            let unit = self
                                .cache
                                .add_media_access_unit(frame.metadata, Bytes::from(frame.payload))
                                .await
                                .map_err(|err| {
                                    ServerError::Config(format!("WebTransport media datagram ingest failed: {err}"))
                                })?;
                            let response = serde_json::to_vec(&MeshProtocolResponse::media_access_unit(
                                MediaAccessUnitResponse::from_cached(&unit),
                            ))
                            .map(Bytes::from)
                            .map_err(|err| ServerError::Handler(Box::new(err)))?;
                            datagram_sender
                                .send_datagram(response)
                                .map_err(|err| ServerError::Config(format!("send WebTransport media ack datagram: {err:?}")))?;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            let response = serde_json::to_vec(&MeshProtocolResponse::error(format!(
                                "WebTransport media datagram decode failed: {error}"
                            )))
                            .map(Bytes::from)
                            .map_err(|err| ServerError::Handler(Box::new(err)))?;
                            datagram_sender
                                .send_datagram(response)
                                .map_err(|err| ServerError::Config(format!("send WebTransport media error datagram: {err:?}")))?;
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl RawTcpHandler for AppRouter {
    async fn handle_stream(
        &self,
        mut stream: Box<dyn web_service::traits::RawStream>,
        _is_tls: bool,
    ) -> HandlerResult<()> {
        loop {
            let Some(frame) =
                read_length_prefixed_frame(&mut *stream, RAW_MESH_MAX_FRAME_BYTES).await?
            else {
                return Ok(());
            };
            let response = self.binary_mesh_response_from_bytes(frame).await?;
            write_length_prefixed_frame(&mut *stream, response.as_ref()).await?;
        }
    }
}

trait ResponseExt {
    fn with_no_store(self) -> Self;
    fn with_etag(self, etag: u64) -> Self;
    fn with_part_available_unix_us(self, available_unix_us: Option<u64>) -> Self;
}

impl ResponseExt for HandlerResponse {
    fn with_no_store(mut self) -> Self {
        self.headers
            .push(("cache-control".into(), "no-store, max-age=0".into()));
        self
    }

    fn with_etag(mut self, etag: u64) -> Self {
        self.etag = Some(etag);
        self
    }

    fn with_part_available_unix_us(mut self, available_unix_us: Option<u64>) -> Self {
        if let Some(available_unix_us) = available_unix_us {
            self.headers.push((
                "x-needletail-cache-available-unix-us".into(),
                available_unix_us.to_string().into(),
            ));
        }
        self
    }
}

fn response(
    status: StatusCode,
    body: Option<Bytes>,
    content_type: Option<&'static str>,
) -> HandlerResponse {
    HandlerResponse {
        status,
        body,
        content_type: content_type.map(Into::into),
        headers: vec![
            ("access-control-allow-origin".into(), "*".into()),
            (
                "access-control-allow-methods".into(),
                "GET, HEAD, POST, PUT, OPTIONS".into(),
            ),
        ],
        etag: None,
    }
}

fn mission_control_asset_response(path: &str) -> Option<HandlerResponse> {
    let dist_dir = std::env::var_os(MISSION_CONTROL_DIST_ENV).map(PathBuf::from)?;
    mission_control_asset_response_from_dir(&dist_dir, path)
}

fn mission_control_setup_response() -> HandlerResponse {
    response(
        StatusCode::SERVICE_UNAVAILABLE,
        Some(Bytes::from_static(
            b"Needletail Mission Control setup: configure NEEDLETAIL_MISSION_CONTROL_DIST with the built asset directory.\n",
        )),
        Some("text/plain; charset=utf-8"),
    )
    .with_no_store()
}

fn mission_control_asset_response_from_dir(dist_dir: &Path, path: &str) -> Option<HandlerResponse> {
    let relative_path = mission_control_asset_relative_path(path)?;
    let full_path = dist_dir.join(relative_path);
    let bytes = std::fs::read(full_path).ok()?;
    Some(
        response(
            StatusCode::OK,
            Some(Bytes::from(bytes)),
            mission_control_asset_content_type(relative_path),
        )
        .with_no_store(),
    )
}

fn mission_control_asset_relative_path(path: &str) -> Option<&str> {
    match path {
        "/mesh" | "/mesh/" => Some("index.html"),
        _ => {
            let candidate = path.strip_prefix('/')?;
            if candidate.is_empty()
                || candidate.contains('/')
                || candidate.contains("..")
                || !mission_control_asset_extension_allowed(candidate)
            {
                return None;
            }
            Some(candidate)
        }
    }
}

fn mission_control_asset_extension_allowed(path: &str) -> bool {
    path.ends_with(".js")
        || path.ends_with(".wasm")
        || path.ends_with(".css")
        || path.ends_with(".ico")
}

fn mission_control_asset_content_type(path: &str) -> Option<&'static str> {
    if path.ends_with(".html") {
        Some("text/html; charset=utf-8")
    } else if path.ends_with(".js") {
        Some("text/javascript; charset=utf-8")
    } else if path.ends_with(".wasm") {
        Some("application/wasm")
    } else if path.ends_with(".css") {
        Some("text/css; charset=utf-8")
    } else if path.ends_with(".ico") {
        Some("image/x-icon")
    } else {
        None
    }
}

fn stream_id_text(stream_id: u64) -> String {
    stream_id.to_string()
}

fn normalize_playback_base_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

fn parse_llhls_tail_path(path: &str) -> Option<u64> {
    let rest = path.strip_prefix("/live/")?;
    let stream_id = rest.strip_suffix("/tail")?;
    if stream_id.is_empty() || stream_id.contains('/') {
        return None;
    }
    stream_id.parse().ok()
}

fn parse_stream_playlist_path(path: &str) -> Option<u64> {
    let rest = path.strip_prefix("/live/")?;
    let stream_id = rest.strip_suffix("/stream.m3u8")?;
    if stream_id.is_empty() || stream_id.contains('/') {
        return None;
    }
    stream_id.parse().ok()
}

fn parse_stream_init_path(path: &str) -> Option<u64> {
    let rest = path.strip_prefix("/live/")?;
    let stream_id = rest.strip_suffix("/init.mp4")?;
    if stream_id.is_empty() || stream_id.contains('/') {
        return None;
    }
    stream_id.parse().ok()
}

fn parse_stream_part_path(path: &str) -> Option<(u64, u64, LiveMediaKind)> {
    let rest = path.strip_prefix("/live/")?;
    let (stream_id, part) = rest.split_once("/part")?;
    let (seq, media_kind) = strip_live_media_suffix(part)?;
    if stream_id.is_empty() || stream_id.contains('/') || seq.is_empty() || seq.contains('/') {
        return None;
    }
    Some((stream_id.parse().ok()?, seq.parse().ok()?, media_kind))
}

fn parse_stream_segment_path(path: &str) -> Option<(u64, u64, LiveMediaKind)> {
    let rest = path.strip_prefix("/live/")?;
    let (stream_id, segment) = rest.split_once("/seg")?;
    let (seq, media_kind) = strip_live_media_suffix(segment)?;
    if stream_id.is_empty() || stream_id.contains('/') || seq.is_empty() || seq.contains('/') {
        return None;
    }
    Some((stream_id.parse().ok()?, seq.parse().ok()?, media_kind))
}

fn parse_init_path(path: &str) -> bool {
    path == "/live/init.mp4"
}

fn parse_part_path(path: &str) -> Option<(u64, LiveMediaKind)> {
    let part = path.strip_prefix("/live/part")?;
    let (seq, media_kind) = strip_live_media_suffix(part)?;
    Some((seq.parse().ok()?, media_kind))
}

fn parse_segment_path(path: &str) -> Option<(u64, LiveMediaKind)> {
    let segment = path.strip_prefix("/live/seg")?;
    let (seq, media_kind) = strip_live_media_suffix(segment)?;
    Some((seq.parse().ok()?, media_kind))
}

fn strip_live_media_suffix(value: &str) -> Option<(&str, LiveMediaKind)> {
    value
        .strip_suffix(".mp4")
        .map(|seq| (seq, LiveMediaKind::Fmp4))
        .or_else(|| {
            value
                .strip_suffix(".ts")
                .map(|seq| (seq, LiveMediaKind::Ts))
        })
}

fn parse_media_unit_path(path: &str) -> Option<(u64, u64)> {
    let rest = path.strip_prefix("/media/")?;
    let (stream_id, sequence) = rest.split_once("/unit/")?;
    if stream_id.is_empty() || sequence.is_empty() || sequence.contains('/') {
        return None;
    }
    Some((stream_id.parse().ok()?, sequence.parse().ok()?))
}

fn query_value<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
    for part in query?.split('&') {
        let (part_key, value) = part.split_once('=').unwrap_or((part, ""));
        if part_key == key {
            return Some(value);
        }
    }
    None
}

fn parse_query_u64(query: Option<&str>, key: &str) -> Option<u64> {
    query_value(query, key)?.parse().ok()
}

async fn read_body_bytes(body: &mut BodyStream) -> HandlerResult<Bytes> {
    let mut bytes = BytesMut::new();
    while let Some(next) = body.next().await {
        bytes.extend_from_slice(&next?);
    }
    Ok(bytes.freeze())
}

async fn read_control_request(body: &mut BodyStream) -> HandlerResult<ControlRequest> {
    let bytes = read_body_bytes(body).await?;
    if bytes.is_empty() {
        return Ok(ControlRequest {
            node_id: None,
            region: None,
            stream_id: None,
        });
    }
    serde_json::from_slice(&bytes).map_err(|err| ServerError::Handler(Box::new(err)))
}

fn deserialize_optional_u64_from_any<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(value) = Option::<serde_json::Value>::deserialize(deserializer)? else {
        return Ok(None);
    };
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Number(number) => number
            .as_u64()
            .map(Some)
            .ok_or_else(|| de::Error::custom("stream_id must be an unsigned 64-bit integer")),
        serde_json::Value::String(text) => text
            .parse::<u64>()
            .map(Some)
            .map_err(|error| de::Error::custom(format!("invalid stream_id `{text}`: {error}"))),
        _ => Err(de::Error::custom(
            "stream_id must be an unsigned integer or decimal string",
        )),
    }
}

fn control_kind_from_path(path: &str) -> Option<ControlKind> {
    match path {
        "/api/control/provision-node" => Some(ControlKind::ProvisionNode),
        "/api/control/close-node" => Some(ControlKind::CloseNode),
        "/api/control/warm-stream" => Some(ControlKind::WarmStream),
        _ => None,
    }
}

fn format_replica_request_status(
    reason: &str,
    peer_count: usize,
    planned_replicas: &[ReplicaPlacement],
) -> String {
    if planned_replicas.is_empty() {
        return format!("{reason}: requested from {peer_count} peers; no eligible planned targets");
    }

    let mut targets = planned_replicas
        .iter()
        .take(5)
        .map(|placement| placement.target_node_id.as_str())
        .collect::<Vec<_>>()
        .join(",");
    if planned_replicas.len() > 5 {
        targets.push_str(",...");
    }
    format!(
        "{reason}: requested from {peer_count} peers; planned {} targets: {targets}",
        planned_replicas.len()
    )
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn now_unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use raptorq_datagram_fec::MediaFrameFlags;
    use std::{
        io::{self, Cursor},
        pin::Pin,
        sync::Mutex,
        task::{Context as TaskContext, Poll},
    };
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    fn encode_test_fmp4_slot(init: Option<&[u8]>, media: &[u8]) -> Bytes {
        let init = init.unwrap_or_default();
        let mut out = Vec::with_capacity(MESH_FMP4_SLOT_HEADER_LEN + init.len() + media.len());
        out.extend_from_slice(MESH_FMP4_SLOT_MAGIC);
        out.extend_from_slice(&(init.len() as u32).to_be_bytes());
        out.extend_from_slice(&(media.len() as u32).to_be_bytes());
        out.extend_from_slice(init);
        out.extend_from_slice(media);
        Bytes::from(out)
    }

    #[test]
    fn webtransport_audio_subscription_supports_session_filtering() {
        assert_eq!(
            parse_audio_epoch_subscription(AUDIO_EPOCH_SUBSCRIPTION),
            Some(None)
        );
        assert_eq!(
            parse_audio_epoch_subscription(b"WAVEY-AUDIO-EPOCH/2 91"),
            Some(Some(91))
        );
        assert_eq!(
            parse_audio_epoch_subscription(b"WAVEY-AUDIO-EPOCH/2 all"),
            None
        );
        assert_eq!(
            parse_native_audio_session_message(
                b"WAVEY-DAW-SUBSCRIBE/2 91",
                NATIVE_AUDIO_SUBSCRIBE_V2_PREFIX,
            ),
            Some(91)
        );
        assert_eq!(
            parse_native_audio_session_message(
                b"WAVEY-DAW-SUBSCRIBE/2 all",
                NATIVE_AUDIO_SUBSCRIBE_V2_PREFIX,
            ),
            None
        );
    }

    #[test]
    fn relay_availability_uses_published_clock_and_preserves_error_bound() {
        let payload = b"published-object";
        let key =
            media_object::ObjectKey::for_payload("default", "1", "muxed-fmp4", 0, 0, 1, 1, payload)
                .unwrap();
        let published = media_object::ClockTimestamp::new(
            1_000_000_000,
            "source-clock",
            media_object::ClockConfidence::estimated(5_000_000),
        )
        .unwrap();
        let object = MediaObject::builder(key, ObjectKind::Media, payload.to_vec())
            .with_stage_timestamp(media_object::StageTimestamp::new(
                Stage::Published,
                published,
            ))
            .build()
            .unwrap();

        assert_eq!(
            relay_availability_observation(&object, 1_250_000),
            Some(RelayAvailabilityObservation::Measured {
                duration_us: 250_000,
                clock_error_us: 5_000,
            })
        );
        let clock = relay_publication_clock(&object).expect("published media clock");
        assert_eq!(
            clock.observe(1_275_000),
            RelayAvailabilityObservation::Measured {
                duration_us: 275_000,
                clock_error_us: 5_000,
            }
        );

        let telemetry = RelayAvailabilityTelemetry::default();
        telemetry.record(RelayAvailabilityObservation::Measured {
            duration_us: 250_000,
            clock_error_us: 5_000,
        });
        telemetry.record(RelayAvailabilityObservation::UnusableClock);
        let mut snapshot = RelaySessionIngressSnapshot::default();
        telemetry.apply_to(&mut snapshot);
        assert_eq!(snapshot.publication_to_available_count, 1);
        assert_eq!(snapshot.publication_to_available_sum_us, 250_000);
        assert_eq!(snapshot.publication_to_available_max_us, 250_000);
        assert_eq!(snapshot.publication_clock_error_max_us, 5_000);
        assert_eq!(snapshot.publication_clock_unusable_objects, 1);
    }

    fn encode_test_canonical_fmp4_object(
        stream_id: u64,
        sequence: u64,
        kind: ObjectKind,
        keyframe: bool,
        payload: &[u8],
    ) -> Bytes {
        encode_test_canonical_fmp4_object_with_epoch(
            stream_id, 0, sequence, kind, keyframe, payload,
        )
    }

    fn encode_test_canonical_fmp4_object_with_epoch(
        stream_id: u64,
        source_epoch: u64,
        sequence: u64,
        kind: ObjectKind,
        keyframe: bool,
        payload: &[u8],
    ) -> Bytes {
        let track = match kind {
            ObjectKind::Initialization | ObjectKind::CodecConfiguration => "muxed-fmp4-init",
            ObjectKind::Media | ObjectKind::Discontinuity => "muxed-fmp4",
        };
        let key = media_object::ObjectKey::for_payload(
            "default",
            stream_id.to_string(),
            track,
            source_epoch,
            0,
            sequence,
            1,
            payload,
        )
        .unwrap();
        let object = MediaObject::builder(key, kind, payload.to_vec())
            .with_keyframe(keyframe)
            .with_metadata("container", b"fmp4".to_vec())
            .build()
            .unwrap();
        Bytes::from(media_object::encode(&object).unwrap())
    }

    fn encode_test_canonical_fmp4_bundle(
        stream_id: u64,
        sequence: u64,
        init: Option<&[u8]>,
        media: &[u8],
    ) -> Bytes {
        let payload = encode_test_fmp4_slot(init, media);
        let key = media_object::ObjectKey::for_payload(
            "default",
            stream_id.to_string(),
            "muxed-fmp4",
            0,
            0,
            sequence,
            1,
            &payload,
        )
        .unwrap();
        let object = MediaObject::builder(key, ObjectKind::Media, payload.to_vec())
            .with_keyframe(true)
            .with_metadata("container", b"fmp4".to_vec())
            .with_metadata("payload-format", b"fmp4-slot-v1".to_vec())
            .build()
            .unwrap();
        Bytes::from(media_object::encode(&object).unwrap())
    }

    fn deterministic_video_payload(len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| {
                let mixed = (index as u32)
                    .wrapping_mul(1_103_515_245)
                    .wrapping_add(12_345);
                (mixed >> 16) as u8
            })
            .collect()
    }

    #[tokio::test]
    async fn playlist_uses_replicated_cache_parts() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        cache
            .chunk_cache
            .add_for_stream_id(1, 0, Bytes::from_static(b"part0"))
            .await
            .unwrap();
        cache
            .chunk_cache
            .add_for_stream_id(1, 1, Bytes::from_static(b"part1"))
            .await
            .unwrap();

        let playlist = cache.playlist().await;
        assert!(playlist.contains("part0.ts"));
        assert!(playlist.contains("part1.ts"));
        assert!(playlist.contains("seg0.ts"));

        cache
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"stream77-part0"))
            .await
            .unwrap();
        let playlist = cache.playlist_for_stream_id(77).await;
        assert!(playlist.contains("part0.ts"));
        assert!(playlist.contains("#EXT-X-PRELOAD-HINT"));
    }

    #[tokio::test]
    async fn cached_playlist_invalidates_when_a_stream_slot_changes() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        cache
            .chunk_cache
            .add_for_stream_id(1, 0, Bytes::from_static(b"part0"))
            .await
            .unwrap();
        let first = cache.playlist().await;
        assert!(first.contains("part0.ts"));
        assert!(!first.contains("#EXT-X-PART:DURATION=0.500,URI=\"part1.ts\""));

        cache
            .chunk_cache
            .add_for_stream_id(1, 1, Bytes::from_static(b"part1"))
            .await
            .unwrap();
        let second = cache.playlist().await;
        assert!(second.contains("#EXT-X-PART:DURATION=0.500,URI=\"part1.ts\""));
        assert!(second.contains("seg0.ts"));
    }

    #[tokio::test]
    async fn fmp4_stream_slots_emit_mp4_playlist_and_media() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        cache
            .commit_stream_payload(77, encode_test_fmp4_slot(Some(b"ftypmoov"), b"moofmdat-a"))
            .await
            .unwrap();
        cache
            .commit_stream_payload(77, encode_test_fmp4_slot(None, b"moofmdat-b"))
            .await
            .unwrap();

        let playlist = cache.playlist_for_stream_id(77).await;
        assert!(playlist.contains("#EXT-X-MAP:URI=\"init.mp4\""));
        assert!(playlist.contains("part0.mp4"));
        assert!(playlist.contains("part1.mp4"));
        assert!(playlist.contains("seg0.mp4"));
        assert!(!playlist.contains("AVFMP4S1"));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/init.mp4")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.content_type.as_deref(), Some("video/mp4"));
        assert_eq!(response.body.unwrap(), Bytes::from_static(b"ftypmoov"));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/part0.mp4")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.content_type.as_deref(), Some("video/mp4"));
        assert!(response.headers.iter().any(|(name, value)| {
            name == "x-needletail-cache-available-unix-us"
                && value.parse::<u64>().is_ok_and(|value| value > 0)
        }));
        assert_eq!(response.body.unwrap(), Bytes::from_static(b"moofmdat-a"));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/seg0.mp4")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.content_type.as_deref(), Some("video/mp4"));
        assert_eq!(
            response.body.unwrap(),
            Bytes::from_static(b"moofmdat-amoofmdat-b")
        );

        mesh.shutdown();
    }

    #[tokio::test]
    async fn idle_stream_retirement_releases_cache_and_auxiliary_state() {
        let cache = LiveTsCache::new(1, Duration::from_millis(5), 200, 600, 64).await;
        let stream_id = 77;
        let source_epoch = now_unix_us();
        cache
            .commit_stream_payload(
                stream_id,
                encode_test_canonical_fmp4_object_with_epoch(
                    stream_id,
                    source_epoch,
                    0,
                    ObjectKind::Initialization,
                    false,
                    b"ftypmoov",
                ),
            )
            .await
            .unwrap();
        cache
            .commit_stream_payload(
                stream_id,
                encode_test_canonical_fmp4_object_with_epoch(
                    stream_id,
                    source_epoch,
                    1,
                    ObjectKind::Media,
                    true,
                    b"moofmdat",
                ),
            )
            .await
            .unwrap();
        let _ = cache.playlist_for_stream_id(stream_id).await;

        assert!(cache.chunk_cache.get_stream_idx(stream_id).await.is_some());
        assert!(cache.part_available_unix_us(stream_id, 1).await.is_some());
        assert!(cache
            .canonical_commit_locks
            .lock()
            .unwrap()
            .contains_key(&stream_id));

        assert_eq!(
            cache
                .retire_streams_idle_before(now_unix_ms().saturating_add(1))
                .await,
            1
        );
        assert!(cache.chunk_cache.get_stream_idx(stream_id).await.is_none());
        assert!(cache.playlist_cache.iter().all(|cached| cached
            .read()
            .unwrap()
            .as_ref()
            .is_none_or(|entry| entry.stream_id != stream_id)));
        assert!(!cache
            .canonical_commit_locks
            .lock()
            .unwrap()
            .contains_key(&stream_id));

        let state = cache.state.read().await;
        assert!(!state.stream_next_seq.contains_key(&stream_id));
        assert!(!state.stream_canonical_epoch.contains_key(&stream_id));
        assert!(!state
            .stream_canonical_epoch_activation_delay_us
            .contains_key(&stream_id));
        assert!(!state
            .stream_subscription_base_object
            .contains_key(&stream_id));
        assert!(!state
            .stream_latest_canonical_object
            .contains_key(&stream_id));
        assert!(!state.stream_last_ingest_unix_ms.contains_key(&stream_id));
        assert!(!state
            .stream_part_available_unix_us
            .keys()
            .any(|(retained_stream, _)| *retained_stream == stream_id));
        assert!(!state.stream_inits.contains_key(&stream_id));
        assert!(!state.stream_media_kinds.contains_key(&stream_id));
    }

    #[tokio::test]
    async fn canonical_rq_objects_commit_by_source_sequence_under_reordering() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let stream_id = 77;
        let initialization = encode_test_canonical_fmp4_object(
            stream_id,
            0,
            ObjectKind::Initialization,
            false,
            b"ftypmoov",
        );
        cache
            .commit_stream_payload(stream_id, initialization)
            .await
            .unwrap();

        let part_one = encode_test_canonical_fmp4_object(
            stream_id,
            1,
            ObjectKind::Media,
            false,
            b"moofmdat-one",
        );
        let part_zero = encode_test_canonical_fmp4_object(
            stream_id,
            0,
            ObjectKind::Media,
            true,
            b"moofmdat-zero",
        );
        assert_eq!(
            cache
                .commit_stream_payload(stream_id, part_one.clone())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            cache
                .commit_stream_payload(stream_id, part_zero.clone())
                .await
                .unwrap(),
            0
        );

        assert_eq!(
            cache.get_init_for_stream_id(stream_id).await.unwrap(),
            Bytes::from_static(b"ftypmoov")
        );
        assert_eq!(
            cache.get_part_for_stream_id(stream_id, 0).await.unwrap().0,
            Bytes::from_static(b"moofmdat-zero")
        );
        assert_eq!(
            cache.get_part_for_stream_id(stream_id, 1).await.unwrap().0,
            Bytes::from_static(b"moofmdat-one")
        );
        let stream_idx = cache.chunk_cache.get_stream_idx(stream_id).await.unwrap();
        assert_eq!(cache.chunk_cache.last(stream_idx), Some(1));

        let version = cache.chunk_cache.version(stream_idx).unwrap();
        cache
            .commit_stream_payload(stream_id, part_zero)
            .await
            .unwrap();
        assert_eq!(cache.chunk_cache.version(stream_idx), Some(version));

        let conflicting_zero = encode_test_canonical_fmp4_object(
            stream_id,
            0,
            ObjectKind::Media,
            true,
            b"different-object-at-zero",
        );
        assert!(cache
            .commit_stream_payload(stream_id, conflicting_zero)
            .await
            .is_err());
        assert_eq!(
            cache.get_part_for_stream_id(stream_id, 0).await.unwrap().0,
            Bytes::from_static(b"moofmdat-zero")
        );

        let cross_stream = encode_test_canonical_fmp4_object(
            stream_id + 1,
            2,
            ObjectKind::Media,
            false,
            b"cross-stream-object",
        );
        assert!(cache
            .commit_stream_payload(stream_id, cross_stream.clone())
            .await
            .is_err());
        cache
            .chunk_cache
            .add_for_stream_id(stream_id, 2, cross_stream)
            .await
            .unwrap();
        assert!(cache.get_part_for_stream_id(stream_id, 2).await.is_none());
    }

    #[tokio::test]
    async fn canonical_source_epoch_switch_resets_only_the_stream_and_rejects_stale_objects() {
        let cache = LiveTsCache::new(1, Duration::from_millis(50), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let stream_id = 1;

        for sequence in 0..=1 {
            cache
                .commit_stream_payload(
                    stream_id,
                    encode_test_canonical_fmp4_object_with_epoch(
                        stream_id,
                        41,
                        sequence,
                        ObjectKind::Media,
                        sequence == 0,
                        format!("epoch-41-{sequence}").as_bytes(),
                    ),
                )
                .await
                .unwrap();
        }
        let inherited = cache.stats(&mesh).await;
        assert_eq!(
            inherited.contiguous_object,
            Some(1),
            "the first source incarnation should publish contiguously"
        );
        assert_eq!(inherited.canonical_epoch, Some(41));
        assert_eq!(
            inherited.canonical_epoch_activation_delay_us, None,
            "an epoch inherited by a newly started relay has no activation measurement"
        );

        cache
            .commit_stream_payload(
                stream_id,
                encode_test_canonical_fmp4_object_with_epoch(
                    stream_id,
                    42,
                    0,
                    ObjectKind::Initialization,
                    false,
                    b"epoch-42-init",
                ),
            )
            .await
            .unwrap();
        cache
            .commit_stream_payload(
                stream_id,
                encode_test_canonical_fmp4_object_with_epoch(
                    stream_id,
                    42,
                    0,
                    ObjectKind::Media,
                    true,
                    b"epoch-42-0",
                ),
            )
            .await
            .unwrap();

        let stats = cache.stats(&mesh).await;
        assert_eq!(stats.canonical_epoch, Some(42));
        assert!(stats.canonical_epoch_activation_delay_us.is_some());
        assert_eq!(stats.head_object, Some(0));
        assert_eq!(stats.contiguous_object, Some(0));
        assert_eq!(stats.gap_count, Some(0));
        assert!(cache.get_part_for_stream_id(stream_id, 1).await.is_none());

        let stale = encode_test_canonical_fmp4_object_with_epoch(
            stream_id,
            41,
            2,
            ObjectKind::Media,
            false,
            b"stale-epoch-41-2",
        );
        assert!(cache
            .commit_stream_payload(stream_id, stale)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale canonical source epoch"));
        assert_eq!(cache.stats(&mesh).await.canonical_epoch, Some(42));

        cache
            .commit_stream_payload(
                stream_id,
                encode_test_canonical_fmp4_object_with_epoch(
                    stream_id,
                    42,
                    1,
                    ObjectKind::Media,
                    false,
                    b"epoch-42-1",
                ),
            )
            .await
            .unwrap();
        let recovered = cache.stats(&mesh).await;
        assert_eq!(recovered.contiguous_object, Some(1));
        assert_eq!(recovered.head_object, Some(1));
        assert_eq!(recovered.gap_count, Some(0));

        cache
            .commit_stream_payload(
                stream_id,
                encode_test_canonical_fmp4_object_with_epoch(
                    stream_id,
                    42,
                    3,
                    ObjectKind::Media,
                    false,
                    b"epoch-42-3",
                ),
            )
            .await
            .unwrap();
        let gapped = cache.stats(&mesh).await;
        assert_eq!(gapped.contiguous_object, Some(1));
        assert_eq!(gapped.head_object, Some(3));
        assert_eq!(gapped.gap_count, Some(1));

        cache
            .commit_stream_payload(
                stream_id,
                encode_test_canonical_fmp4_object_with_epoch(
                    stream_id,
                    42,
                    2,
                    ObjectKind::Media,
                    false,
                    b"epoch-42-2",
                ),
            )
            .await
            .unwrap();
        let filled = cache.stats(&mesh).await;
        assert_eq!(filled.contiguous_object, Some(3));
        assert_eq!(filled.head_object, Some(3));
        assert_eq!(filled.gap_count, Some(0));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn canonical_initialization_survives_the_rolling_media_window() {
        let cache = LiveTsCache::new(1, Duration::from_millis(50), 2, 4, 64).await;
        let stream_id = 91;
        cache
            .commit_stream_payload(
                stream_id,
                encode_test_canonical_fmp4_object(
                    stream_id,
                    0,
                    ObjectKind::Initialization,
                    false,
                    b"ftyp-moov-durable",
                ),
            )
            .await
            .unwrap();

        for sequence in 0..40 {
            cache
                .commit_stream_payload(
                    stream_id,
                    encode_test_canonical_fmp4_object(
                        stream_id,
                        sequence,
                        ObjectKind::Media,
                        sequence == 0,
                        format!("moof-mdat-{sequence}").as_bytes(),
                    ),
                )
                .await
                .unwrap();
        }

        assert!(cache.get_part_for_stream_id(stream_id, 0).await.is_none());
        assert_eq!(
            cache.get_init_for_stream_id(stream_id).await.unwrap(),
            Bytes::from_static(b"ftyp-moov-durable")
        );
        assert!(cache
            .playlist_for_stream_id(stream_id)
            .await
            .contains("#EXT-X-MAP:URI=\"init.mp4\""));
    }

    #[tokio::test]
    async fn canonical_fmp4_bundle_preserves_init_across_cache_replication() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let stream_id = 88;
        let sequence = 7;
        let envelope = encode_test_canonical_fmp4_bundle(
            stream_id,
            sequence,
            Some(b"ftypmoov-replicated"),
            b"moofmdat-replicated",
        );

        cache
            .chunk_cache
            .add_for_stream_id(stream_id, sequence as usize, envelope)
            .await
            .unwrap();

        assert_eq!(
            cache
                .get_part_for_stream_id(stream_id, sequence)
                .await
                .unwrap()
                .0,
            Bytes::from_static(b"moofmdat-replicated")
        );
        assert_eq!(
            cache.get_init_for_stream_id(stream_id).await.unwrap(),
            Bytes::from_static(b"ftypmoov-replicated")
        );
    }

    #[test]
    fn parses_live_paths() {
        assert_eq!(parse_stream_playlist_path("/live/77/stream.m3u8"), Some(77));
        assert_eq!(parse_stream_init_path("/live/77/init.mp4"), Some(77));
        assert_eq!(
            parse_stream_part_path("/live/77/part42.ts"),
            Some((77, 42, LiveMediaKind::Ts))
        );
        assert_eq!(
            parse_stream_part_path("/live/77/part42.mp4"),
            Some((77, 42, LiveMediaKind::Fmp4))
        );
        assert_eq!(
            parse_stream_segment_path("/live/77/seg7.ts"),
            Some((77, 7, LiveMediaKind::Ts))
        );
        assert_eq!(
            parse_stream_segment_path("/live/77/seg7.mp4"),
            Some((77, 7, LiveMediaKind::Fmp4))
        );
        assert_eq!(parse_stream_playlist_path("/live/stream.m3u8"), None);
        assert_eq!(parse_stream_playlist_path("/live/77/part42.ts"), None);
        assert_eq!(parse_stream_part_path("/live/part42.ts"), None);
        assert_eq!(parse_stream_segment_path("/live/seg7.ts"), None);
        assert_eq!(
            parse_part_path("/live/part42.ts"),
            Some((42, LiveMediaKind::Ts))
        );
        assert_eq!(
            parse_part_path("/live/part42.mp4"),
            Some((42, LiveMediaKind::Fmp4))
        );
        assert_eq!(
            parse_segment_path("/live/seg7.ts"),
            Some((7, LiveMediaKind::Ts))
        );
        assert_eq!(
            parse_segment_path("/live/seg7.mp4"),
            Some((7, LiveMediaKind::Fmp4))
        );
        assert_eq!(parse_part_path("/live/seg7.ts"), None);
        assert_eq!(parse_llhls_tail_path("/live/77/tail"), Some(77));
        assert_eq!(parse_llhls_tail_path("/live/not-number/tail"), None);
        assert_eq!(
            parse_query_u64(Some("mode=part&after=41"), "after"),
            Some(41)
        );
        assert_eq!(
            normalize_playback_base_url("https://node/live/"),
            "https://node/live"
        );
    }

    #[test]
    fn default_args_keep_edge_protocols_opt_in() {
        let args = Args::try_parse_from(["av-mesh"])
            .unwrap()
            .normalized()
            .unwrap();

        assert!(!args.edge_websocket);
        assert!(!args.edge_webtransport);
        assert!(args.raw_tcp_port.is_none());
        assert_eq!(args.telemetry_dns_name, "local.wavey.ai");
        assert_eq!(args.mesh_sync_interval_ms, 20);
        assert_eq!(args.mesh_repair_symbols, 1);
        assert_eq!(args.mesh_repair_ratio, 0.03);
        assert_eq!(args.mesh_max_repair_symbols, 32);
        assert_eq!(args.mesh_symbol_size, 1316);
    }

    #[test]
    fn parses_edge_protocol_opt_ins() {
        let args = Args::try_parse_from([
            "av-mesh",
            "--edge-websocket",
            "--edge-webtransport",
            "--raw-tcp-port",
            "19000",
        ])
        .unwrap()
        .normalized()
        .unwrap();

        assert!(args.edge_websocket);
        assert!(args.edge_webtransport);
        assert_eq!(args.raw_tcp_port, Some(19000));
    }

    #[test]
    fn controlled_relay_cli_binds_two_parent_lanes_to_one_receiver() {
        let args = Args::try_parse_from([
            "av-mesh",
            "--node-id",
            "edge-london",
            "--fec-bind",
            "127.0.0.1:12001",
            "--relay-controlled-local",
            "--relay-primary-bind",
            "127.0.0.1:12001",
            "--relay-primary-peer",
            "127.0.0.1:13001",
            "--relay-primary-id",
            "contrib-primary",
            "--relay-secondary-bind",
            "127.0.0.1:12002",
            "--relay-secondary-peer",
            "127.0.0.1:13002",
            "--relay-secondary-id",
            "contrib-secondary",
            "--relay-topology-generation",
            "7",
            "--relay-subscription-id",
            "19",
        ])
        .unwrap()
        .normalized()
        .unwrap();
        let dispatch = configured_relay_udp_dispatch(&args, "edge-london").unwrap();
        let snapshot = dispatch.receiver().snapshot();

        assert_eq!(args.relay_primary_bind, Some(args.fec_bind));
        assert_eq!(
            args.relay_secondary_bind,
            Some("127.0.0.1:12002".parse().unwrap())
        );
        assert_eq!(snapshot.primary_sessions, 1);
        assert_eq!(snapshot.secondary_sessions, 1);
        assert_eq!(snapshot.controlled_sessions, 2);
        assert_eq!(snapshot.authenticated_sessions, 0);
    }

    #[test]
    fn relay_peer_configuration_requires_explicit_controlled_mode() {
        let error = Args::try_parse_from(["av-mesh", "--relay-primary-peer", "127.0.0.1:13001"])
            .unwrap()
            .normalized()
            .unwrap_err();
        assert!(error.to_string().contains("--relay-controlled-local"));
    }

    #[test]
    fn parses_dns_peer_targets_for_orchestrated_deployments() {
        let args = Args::try_parse_from([
            "av-mesh",
            "--peer",
            "av-mesh-us.av-mesh.svc.cluster.local:9101",
            "--telemetry-peer",
            "av-mesh-us.av-mesh.svc.cluster.local:7300",
        ])
        .unwrap()
        .normalized()
        .unwrap();

        assert_eq!(
            args.peers,
            vec!["av-mesh-us.av-mesh.svc.cluster.local:9101"]
        );
        assert_eq!(
            args.telemetry_peers,
            vec!["av-mesh-us.av-mesh.svc.cluster.local:7300"]
        );
    }

    #[cfg(feature = "linode-provisioner")]
    #[test]
    fn parses_linode_provision_flags_and_region_maps() {
        let args = Args::try_parse_from([
            "av-mesh",
            "--linode-provision",
            "--linode-image-id",
            "linode/arch",
            "--linode-instance-type",
            "g6-dedicated-2",
            "--linode-domain-id",
            "2958920",
            "--linode-vlan-tag",
            "avmesh",
            "--linode-token-env",
            "TEST_LINODE_TOKEN",
            "--linode-pub-key-env",
            "TEST_LINODE_PUB_KEY",
            "--linode-region-map",
            "uk=gb-lon",
            "--linode-region-map",
            "us=us-east",
        ])
        .unwrap()
        .normalized()
        .unwrap();
        let config = args.linode_provision_config().unwrap();

        assert_eq!(config.image_id, "linode/arch");
        assert_eq!(config.instance_type, "g6-dedicated-2");
        assert_eq!(config.domain_id, 2_958_920);
        assert_eq!(config.vlan_tag, "avmesh");
        assert_eq!(config.token_env, "TEST_LINODE_TOKEN");
        assert_eq!(config.pub_key_env, "TEST_LINODE_PUB_KEY");
        assert_eq!(config.resolve_region("uk"), "gb-lon");
        assert_eq!(config.resolve_region("us"), "us-east");
        assert_eq!(config.resolve_region("jp-osa"), "jp-osa");
    }

    #[cfg(feature = "linode-provisioner")]
    #[test]
    fn linode_provision_requires_provider_config() {
        let error = Args::try_parse_from(["av-mesh", "--linode-provision"])
            .unwrap()
            .normalized()
            .unwrap_err()
            .to_string();

        assert!(error.contains("--linode-image-id"));
        assert!(error.contains("--linode-instance-type"));
        assert!(error.contains("--linode-domain-id"));
    }

    fn serialized_media_access_unit_for_tests(
        metadata: MediaFrameMetadata,
        payload: &'static [u8],
    ) -> Bytes {
        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: payload.len() as u32,
            fragment_offset: 0,
        };
        let mut bytes = vec![0; MEDIA_FRAME_HEADER_LEN];
        header.encode(&mut bytes[..]).unwrap();
        bytes.extend_from_slice(payload);
        Bytes::from(bytes)
    }

    fn push_raw_mesh_frame(frames: &mut Vec<u8>, payload: &[u8]) {
        let len = u32::try_from(payload.len()).unwrap();
        frames.extend_from_slice(&len.to_be_bytes());
        frames.extend_from_slice(payload);
    }

    fn pop_raw_mesh_frame(frames: &mut &[u8]) -> Bytes {
        let (len_buf, rest) = frames.split_at(4);
        *frames = rest;
        let len_buf = <[u8; 4]>::try_from(len_buf).unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let (payload, rest) = frames.split_at(len);
        *frames = rest;
        Bytes::copy_from_slice(payload)
    }

    struct MemoryRawStream {
        read: Cursor<Vec<u8>>,
        written: Arc<Mutex<Vec<u8>>>,
    }

    impl MemoryRawStream {
        fn new(read: Vec<u8>) -> (Self, Arc<Mutex<Vec<u8>>>) {
            let written = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    read: Cursor::new(read),
                    written: Arc::clone(&written),
                },
                written,
            )
        }
    }

    impl AsyncRead for MemoryRawStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let pos = self.read.position() as usize;
            let source_len = self.read.get_ref().len();
            if pos >= source_len {
                return Poll::Ready(Ok(()));
            }

            let to_copy = buf.remaining().min(source_len - pos);
            buf.put_slice(&self.read.get_ref()[pos..pos + to_copy]);
            self.read.set_position((pos + to_copy) as u64);
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for MemoryRawStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.written.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn http_ingest_is_not_served_by_mesh_node() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/ingest")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::NOT_FOUND);
        assert!(!router.has_body_handler("/ingest"));

        let req = Request::builder()
            .method(Method::POST)
            .uri("/ingest")
            .body(())
            .unwrap();
        let body: BodyStream = Box::pin(futures_util::stream::iter(vec![Ok(Bytes::from_static(
            b"http-body-owned-by-av-contrib",
        ))]));
        let response = router.route_body(req, body).await.unwrap();
        assert_eq!(response.status, StatusCode::METHOD_NOT_ALLOWED);
        assert!(cache.get_part_blocking(0).await.is_none());
        mesh.shutdown();
    }

    #[tokio::test]
    async fn http_media_access_unit_ingest_is_not_served_by_mesh_node() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let stream_id = u64::from(u32::MAX) + 55;
        let rejected_metadata = MediaFrameMetadata {
            duration_ms: 33,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(stream_id, 7, 1234, MediaCodec::H264)
        };
        let req = Request::builder()
            .method(Method::GET)
            .uri("/media/access-unit")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::NOT_FOUND);
        assert!(!router.has_body_handler("/media/access-unit"));

        let req = Request::builder()
            .method(Method::POST)
            .uri("/media/access-unit")
            .body(())
            .unwrap();
        let body: BodyStream = Box::pin(futures_util::stream::iter(vec![Ok(
            serialized_media_access_unit_for_tests(rejected_metadata, b"h264-access-unit"),
        )]));

        let response = router.route_body(req, body).await.unwrap();

        assert_eq!(response.status, StatusCode::METHOD_NOT_ALLOWED);
        assert!(cache.get_media_access_unit(stream_id, 7).await.is_none());

        let metadata = MediaFrameMetadata {
            duration_ms: 33,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(stream_id, 7, 1234, MediaCodec::H264)
        };
        let cached = cache
            .add_media_access_unit(metadata, Bytes::from_static(b"h264-access-unit"))
            .await
            .unwrap();
        assert_eq!(cached.metadata.codec, MediaCodec::H264);
        assert!(cached.metadata.flags.is_keyframe());
        assert_eq!(cached.payload_bytes, b"h264-access-unit".len());

        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("/media/{stream_id}/unit/7"))
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.content_type.as_deref(),
            Some(MEDIA_ACCESS_UNIT_CONTENT_TYPE)
        );
        let body = response.body.unwrap();
        let header = MediaFragmentHeader::decode(&body[..MEDIA_FRAME_HEADER_LEN]).unwrap();
        assert_eq!(header.metadata.stream_id, stream_id);
        assert_eq!(header.metadata.sequence, 7);
        assert_eq!(header.metadata.codec, MediaCodec::H264);
        assert_eq!(&body[MEDIA_FRAME_HEADER_LEN..], b"h264-access-unit");
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_returns_node_capacity_and_stream_counts() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        cache.push_payload(b"mission-control-part").await.unwrap();
        cache.rotate_if_due(true).await.unwrap();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/mesh")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();
        let body = response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(json["node"]["node_id"], "test-node");
        assert_eq!(json["stream"]["stream_id"], 1);
        assert_eq!(json["stream"]["stream_id_text"], "1");
        assert!(json["streams"]
            .as_array()
            .unwrap()
            .iter()
            .any(|stream| stream["stream_id"] == 1 && stream["stream_id_text"] == "1"));
        assert_eq!(json["node"]["active_streams"], 1);
        assert_eq!(json["edge_services"][0]["node_id"], "test-node");
        assert_eq!(
            json["edge_services"][0]["playback_base_url"],
            "https://test-node.local/live"
        );
        assert_eq!(json["edge_services"][0]["active_readers"], 0);
        assert_eq!(json["relay_session"]["active_objects"], 0);
        assert_eq!(json["relay_session"]["source_datagrams"], 0);
        assert_eq!(json["relay_session"]["repair_assisted_objects"], 0);
        assert_eq!(json["relay_session"]["fec_recovered_objects"], 0);
        assert_eq!(json["relay_session"]["fec_recovered_source_symbols"], 0);
        assert!(json["relay_session"].get("repaired_objects").is_none());
        assert_eq!(json["relay_session"]["authentication_drops"], 0);
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_prometheus_metrics_expose_topology_edge_and_stream_health() {
        let mut local = telemetry_snapshot_for_tests(
            "uk-1",
            "uk",
            "eu",
            51.5,
            -0.1,
            vec![PeerSnapshot {
                addr: "us-1".into(),
                state: "discovered".into(),
            }],
            77,
        );
        local.edge_service = Some(EdgeServiceSnapshot {
            node_id: "uk-1".into(),
            region: "uk".into(),
            continent: "eu".into(),
            playback_base_url: Some("https://uk.example/live".into()),
            active_readers: 2,
            requests_served: 10,
            bytes_served: 20_000,
            llhls_tail_requests: 5,
            responses_total: 12,
            response_errors: 1,
            response_not_found: 1,
            last_response_unix_ms: Some(now_unix_ms()),
            response_duration_count: 12,
            response_duration_sum_us: 18_000,
            response_duration_p95_us: Some(2_500),
            response_duration_buckets: EDGE_RESPONSE_DURATION_BUCKETS_US
                .iter()
                .map(|upper_bound| u64::from(*upper_bound >= 2_500) * 12)
                .collect(),
            recent_responses: Vec::new(),
            draining: false,
        });
        let mut snapshot = TelemetryAggregator::default().snapshot(local).await;
        snapshot.mesh_fec = MeshFecRuntimeSnapshot {
            tx_objects: 8,
            tx_source_datagrams: 16,
            tx_repair_datagrams: 4,
            rx_decoded_objects: 7,
            rx_repaired_objects: 2,
            rx_repaired_source_datagrams: 3,
            rx_late_source_datagrams: 1,
            rx_presumed_lost_source_datagrams: 2,
            rx_inflight_objects: 1,
            ..MeshFecRuntimeSnapshot::default()
        };
        snapshot.relay_session = RelaySessionIngressSnapshot {
            primary_sessions: 1,
            secondary_sessions: 1,
            authenticated_sessions: 2,
            active_objects: 3,
            active_object_bytes: 24_000,
            source_datagrams: 40,
            repair_datagrams: 8,
            decoded_objects: 9,
            repair_assisted_objects: 4,
            fec_recovered_objects: 3,
            fec_recovered_source_symbols: 7,
            expired_objects: 2,
            conflict_drops: 1,
            authentication_drops: 2,
            deadline_drops: 3,
            downstream_children: 1,
            forwarded_source_datagrams: 20,
            forwarded_repair_datagrams: 4,
            forwarded_bytes: 32_000,
            forward_errors: 2,
            warm_source_buffered_datagrams: 12,
            warm_source_buffered_bytes: 18_000,
            warm_source_replayed_datagrams: 9,
            warm_source_replayed_bytes: 13_500,
            warm_source_expired_datagrams: 2,
            warm_source_retired_datagrams: 8,
            warm_source_evicted_datagrams: 1,
            processing_duration_count: 48,
            processing_duration_sum_us: 4_800,
            processing_duration_max_us: 320,
            processing_duration_buckets: [48; EDGE_RESPONSE_DURATION_BUCKETS_US.len()],
            forward_duration_count: 24,
            forward_duration_sum_us: 2_400,
            forward_duration_max_us: 240,
            forward_duration_buckets: [24; EDGE_RESPONSE_DURATION_BUCKETS_US.len()],
            publication_to_available_count: 9,
            publication_to_available_sum_us: 2_250_000,
            publication_to_available_max_us: 280_000,
            publication_to_available_buckets: [9; PUBLICATION_AVAILABILITY_BUCKETS_US.len()],
            publication_clock_error_max_us: 5_000,
            publication_clock_unusable_objects: 1,
            failover_controller_state: RelayFailoverControllerState::Promoted,
            failover_controller_enabled: 1,
            failover_commands_sent: 7,
            failover_command_send_errors: 1,
            failover_promotions: 2,
            failover_demotions: 1,
            failover_secondary_unavailable_events: 3,
            failover_primary_source_age_ms: 351,
            failover_secondary_repair_age_ms: 24,
            failover_last_detection_us: 351_000,
            failover_last_promotion_to_source_us: 88_000,
            failover_last_media_gap_us: 103_000,
            failover_max_media_gap_us: 119_000,
            failover_controller_last_transition_unix_ms: 1_784_102_400_123,
            failover_listeners: 1,
            failover_promoted_children: 1,
            failover_commands_received: 6,
            failover_commands_rejected: 2,
            failover_lease_expirations: 1,
            failover_promotions_applied: 2,
            failover_demotions_applied: 2,
            failover_listener_last_transition_unix_ms: 1_784_102_400_456,
            ..RelaySessionIngressSnapshot::default()
        };

        let metrics = render_mesh_prometheus_metrics(&snapshot);

        assert!(metrics.contains("# TYPE av_mesh_nodes gauge\n"));
        assert!(metrics.contains("av_mesh_nodes 1\n"));
        assert!(metrics.contains("av_mesh_canonical_epoch_divergent_streams 0\n"));
        assert!(metrics.contains("av_mesh_canonical_epoch_activation_delay_max_seconds 0.25\n"));
        assert!(metrics.contains("av_mesh_transport_sync_interval_seconds 0.02\n"));
        assert!(metrics.contains("av_mesh_transport_fec_repair_ratio 0.03\n"));
        assert!(metrics.contains("av_mesh_transport_fec_min_repair_symbols 1\n"));
        assert!(metrics.contains("av_mesh_transport_fec_max_repair_symbols 32\n"));
        assert!(metrics.contains("av_mesh_fec_tx_objects_total 8\n"));
        assert!(metrics.contains("av_mesh_fec_tx_datagrams_total{kind=\"source\"} 16\n"));
        assert!(metrics.contains("av_mesh_fec_tx_datagrams_total{kind=\"repair\"} 4\n"));
        assert!(metrics.contains("av_mesh_fec_rx_objects_total{outcome=\"repaired\"} 2\n"));
        assert!(metrics.contains("av_mesh_fec_rx_repaired_source_datagrams_total 3\n"));
        assert!(metrics.contains("av_mesh_fec_rx_late_source_datagrams_total 1\n"));
        assert!(metrics.contains("av_mesh_fec_rx_presumed_lost_source_datagrams_total 2\n"));
        assert!(metrics.contains("av_mesh_fec_rx_inflight_objects 1\n"));
        assert!(metrics.contains("av_mesh_relay_session_parent_sessions{role=\"primary\"} 1\n"));
        assert!(metrics.contains("av_mesh_relay_session_active_object_bytes 24000\n"));
        assert!(metrics.contains("av_mesh_relay_session_datagrams_total{outcome=\"repair\"} 8\n"));
        assert!(metrics
            .contains("av_mesh_relay_session_objects_total{outcome=\"repair_assisted\"} 4\n"));
        assert!(
            metrics.contains("av_mesh_relay_session_objects_total{outcome=\"fec_recovered\"} 3\n")
        );
        assert!(metrics.contains("av_mesh_relay_session_fec_recovered_source_symbols_total 7\n"));
        assert!(metrics.contains("av_mesh_relay_session_drops_total{reason=\"deadline\"} 3\n"));
        assert!(metrics.contains("av_mesh_relay_session_downstream_children 1\n"));
        assert!(metrics
            .contains("av_mesh_relay_session_forwarded_datagrams_total{role=\"source\"} 20\n"));
        assert!(metrics.contains("av_mesh_relay_session_forwarded_bytes_total 32000\n"));
        assert!(metrics.contains("av_mesh_relay_session_forward_errors_total 2\n"));
        assert!(metrics.contains("av_mesh_relay_warm_source_buffered_datagrams 12\n"));
        assert!(metrics.contains("av_mesh_relay_warm_source_buffered_bytes 18000\n"));
        assert!(metrics.contains("av_mesh_relay_warm_source_replayed_datagrams_total 9\n"));
        assert!(metrics.contains("av_mesh_relay_warm_source_replayed_bytes_total 13500\n"));
        assert!(metrics.contains("av_mesh_relay_warm_source_expired_datagrams_total 2\n"));
        assert!(metrics.contains("av_mesh_relay_warm_source_retired_datagrams_total 8\n"));
        assert!(metrics.contains("av_mesh_relay_warm_source_evicted_datagrams_total 1\n"));
        assert!(metrics.contains("av_mesh_relay_session_processing_duration_us_count 48\n"));
        assert!(metrics.contains("av_mesh_relay_session_processing_duration_max_us 320\n"));
        assert!(metrics.contains("av_mesh_relay_session_forward_duration_us_count 24\n"));
        assert!(metrics.contains("av_mesh_relay_session_forward_duration_max_us 240\n"));
        assert!(metrics.contains("av_mesh_relay_session_publication_to_available_us_count 9\n"));
        assert!(metrics.contains("av_mesh_relay_session_publication_to_available_max_us 280000\n"));
        assert!(metrics.contains("av_mesh_relay_session_publication_clock_error_max_us 5000\n"));
        assert!(
            metrics.contains("av_mesh_relay_session_publication_clock_unusable_objects_total 1\n")
        );
        assert!(metrics.contains("av_mesh_relay_failover_state{state=\"promoted\"} 1\n"));
        assert!(metrics.contains("av_mesh_relay_failover_state{state=\"healthy\"} 0\n"));
        assert!(metrics.contains("av_mesh_relay_failover_controller_enabled 1\n"));
        assert!(metrics.contains("av_mesh_relay_failover_listeners 1\n"));
        assert!(metrics.contains("av_mesh_relay_failover_promoted_children 1\n"));
        assert!(metrics.contains("av_mesh_relay_failover_primary_source_age_ms 351\n"));
        assert!(metrics.contains("av_mesh_relay_failover_secondary_repair_age_ms 24\n"));
        assert!(metrics.contains("av_mesh_relay_failover_last_detection_us 351000\n"));
        assert!(metrics.contains("av_mesh_relay_failover_last_promotion_to_source_us 88000\n"));
        assert!(metrics.contains("av_mesh_relay_failover_last_media_gap_us 103000\n"));
        assert!(metrics.contains("av_mesh_relay_failover_max_media_gap_us 119000\n"));
        assert!(metrics.contains(
            "av_mesh_relay_failover_commands_total{direction=\"sent\",outcome=\"success\"} 7\n"
        ));
        assert!(metrics.contains(
            "av_mesh_relay_failover_commands_total{direction=\"received\",outcome=\"rejected\"} 2\n"
        ));
        assert!(metrics.contains(
            "av_mesh_relay_failover_transitions_total{side=\"controller\",transition=\"promotion\"} 2\n"
        ));
        assert!(metrics.contains(
            "av_mesh_relay_failover_transitions_total{side=\"forwarder\",transition=\"demotion\"} 2\n"
        ));
        assert!(metrics.contains("av_mesh_relay_failover_secondary_unavailable_total 3\n"));
        assert!(metrics.contains("av_mesh_relay_failover_lease_expirations_total 1\n"));
        assert!(metrics.contains("av_mesh_edge_active_readers{node_id=\"uk-1\",region=\"uk\"} 2\n"));
        assert!(metrics.contains(
            "av_mesh_stream_bytes_received_total{node_id=\"uk-1\",stream_id=\"77\"} 20000\n"
        ));
        assert!(metrics
            .contains("av_mesh_stream_canonical_epoch{node_id=\"uk-1\",stream_id=\"77\"} 1\n"));
        assert!(metrics.contains(
            "av_mesh_stream_canonical_epoch_activation_delay_seconds{node_id=\"uk-1\",stream_id=\"77\"} 0.25\n"
        ));
        assert!(metrics
            .contains("av_mesh_stream_contiguous_object{node_id=\"uk-1\",stream_id=\"77\"} 1\n"));
        assert!(metrics
            .contains("av_mesh_stream_known_gap_count{node_id=\"uk-1\",stream_id=\"77\"} 0\n"));
        assert!(metrics.contains("av_mesh_stream_lag_parts{node_id=\"uk-1\",stream_id=\"77\"} 0\n"));
        assert!(metrics.contains(
            "av_mesh_edge_response_duration_seconds_count{node_id=\"uk-1\",region=\"uk\"} 12\n"
        ));
        assert_eq!(prometheus_label_value("node\\\"\n"), "node\\\\\\\"\\n");
    }

    #[tokio::test]
    async fn mesh_metrics_route_serves_prometheus_exposition_and_edge_latency() {
        let cache = LiveTsCache::new(1, Duration::from_millis(50), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        cache.push_payload(b"latency-metric-part").await.unwrap();
        cache.rotate_if_due(true).await.unwrap();

        let playlist_req = Request::builder()
            .method(Method::GET)
            .uri("/live/stream.m3u8")
            .body(())
            .unwrap();
        assert_eq!(
            router.route(playlist_req).await.unwrap().status,
            StatusCode::OK
        );

        let metrics_req = Request::builder()
            .method(Method::GET)
            .uri(MESH_METRICS_PATH)
            .body(())
            .unwrap();
        let response = router.route(metrics_req).await.unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.content_type.as_deref(),
            Some(PROMETHEUS_CONTENT_TYPE)
        );
        let metrics = String::from_utf8(response.body.unwrap().to_vec()).unwrap();
        assert!(metrics.contains("av_mesh_nodes 1\n"));
        assert!(metrics.contains(
            "av_mesh_edge_response_duration_seconds_count{node_id=\"test-node\",region=\"test-region\"} 1\n"
        ));

        mesh.shutdown();
    }

    #[test]
    fn mission_control_setup_response_is_concise_and_actionable() {
        let response = mission_control_setup_response();
        assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.content_type.as_deref(),
            Some("text/plain; charset=utf-8")
        );
        let body = String::from_utf8(response.body.unwrap().to_vec()).unwrap();
        assert_eq!(
            body,
            "Needletail Mission Control setup: configure NEEDLETAIL_MISSION_CONTROL_DIST with the built asset directory.\n"
        );
    }

    #[test]
    fn mission_control_asset_response_serves_leptos_assets_when_present() {
        let temp_dir = std::env::temp_dir().join(format!(
            "needletail-mission-control-dist-test-{}",
            now_unix_ms()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        std::fs::write(
            temp_dir.join("index.html"),
            r#"<html><body><script type="module" src="/app.js"></script></body></html>"#,
        )
        .unwrap();
        std::fs::write(temp_dir.join("app.js"), "export default {};").unwrap();
        std::fs::write(temp_dir.join("app_bg.wasm"), b"\0asm").unwrap();

        let index = mission_control_asset_response_from_dir(&temp_dir, "/mesh").unwrap();
        assert_eq!(index.status, StatusCode::OK);
        assert_eq!(
            index.content_type.as_deref(),
            Some("text/html; charset=utf-8")
        );
        assert!(String::from_utf8(index.body.unwrap().to_vec())
            .unwrap()
            .contains("type=\"module\""));
        assert!(index
            .headers
            .iter()
            .any(|(name, value)| name == "cache-control" && value.contains("no-store")));

        let js = mission_control_asset_response_from_dir(&temp_dir, "/app.js").unwrap();
        assert_eq!(
            js.content_type.as_deref(),
            Some("text/javascript; charset=utf-8")
        );
        let wasm = mission_control_asset_response_from_dir(&temp_dir, "/app_bg.wasm").unwrap();
        assert_eq!(wasm.content_type.as_deref(), Some("application/wasm"));
        assert!(mission_control_asset_response_from_dir(&temp_dir, "/live/stream.m3u8").is_none());

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn telemetry_aggregator_consumes_avmt_payloads_from_global_nodes() {
        let aggregator = TelemetryAggregator::default();
        let local = telemetry_snapshot_for_tests(
            "uk-1",
            "uk",
            "eu",
            51.5,
            -0.1,
            vec![PeerSnapshot {
                addr: "us-1".into(),
                state: "discovered".into(),
            }],
            1,
        );
        let mut us = telemetry_snapshot_for_tests(
            "us-1",
            "us-east",
            "na",
            37.4,
            -78.6,
            vec![PeerSnapshot {
                addr: "uk-1".into(),
                state: "discovered".into(),
            }],
            2,
        );
        us.relay_session = RelaySessionIngressSnapshot {
            primary_sessions: 1,
            controlled_sessions: 1,
            downstream_children: 1,
            forwarded_source_datagrams: 144,
            forward_duration_count: 144,
            forward_duration_max_us: 73,
            ..RelaySessionIngressSnapshot::default()
        };
        let apac = telemetry_snapshot_for_tests(
            "jp-1",
            "jp-east",
            "apac",
            35.6,
            139.6,
            vec![PeerSnapshot {
                addr: "uk-1".into(),
                state: "discovered".into(),
            }],
            3,
        );

        assert!(!aggregator
            .ingest_payload(TcpChangesPayload {
                tag: *b"myip",
                val: Bytes::from_static(b"ignored"),
            })
            .await
            .unwrap());
        for snapshot in [us, apac] {
            let payload = TcpChangesPayload {
                tag: TELEMETRY_TAG,
                val: Bytes::from(serde_json::to_vec(&snapshot).unwrap()),
            };
            assert!(aggregator.ingest_payload(payload).await.unwrap());
        }

        let aggregate = aggregator.snapshot(local).await;

        assert_eq!(aggregate.aggregate.node_count, 3);
        assert_eq!(aggregate.aggregate.connection_count, 3);
        assert_eq!(aggregate.aggregate.active_streams, 3);
        assert!(aggregate.nodes.iter().any(|node| node.node_id == "jp-1"));
        let us_relay = aggregate
            .relay_nodes
            .iter()
            .find(|relay| relay.node_id == "us-1")
            .expect("remote relay telemetry");
        assert_eq!(us_relay.relay_session.downstream_children, 1);
        assert_eq!(us_relay.relay_session.forwarded_source_datagrams, 144);
        assert_eq!(us_relay.relay_session.forward_duration_max_us, 73);
        assert!(aggregate
            .connections
            .iter()
            .any(|connection| connection.source_node_id == "us-1"
                && connection.target_addr == "uk-1"
                && connection.target_node_id.as_deref() == Some("uk-1")));
    }

    #[tokio::test]
    async fn telemetry_aggregator_resolves_peer_addresses_to_node_ids() {
        let aggregator = TelemetryAggregator::default();
        let mut local = telemetry_snapshot_for_tests(
            "uk-1",
            "uk",
            "eu",
            51.5,
            -0.1,
            vec![PeerSnapshot {
                addr: "10.0.0.2:9100".into(),
                state: "discovered".into(),
            }],
            1,
        );
        local.mesh_addr = Some("10.0.0.1:9100".into());
        let mut us = telemetry_snapshot_for_tests(
            "us-1",
            "us-east",
            "na",
            37.4,
            -78.6,
            vec![PeerSnapshot {
                addr: "10.0.0.1:9100".into(),
                state: "discovered".into(),
            }],
            1,
        );
        us.mesh_addr = Some("10.0.0.2:9100".into());
        aggregator.ingest_snapshot(us).await;

        let aggregate = aggregator.snapshot(local).await;
        let connection = aggregate
            .connections
            .iter()
            .find(|connection| connection.source_node_id == "uk-1")
            .expect("missing uk connection");

        assert_eq!(connection.target_addr, "10.0.0.2:9100");
        assert_eq!(connection.target_node_id.as_deref(), Some("us-1"));
        assert!(connection.private_target);
        assert_eq!(aggregate.topology.connection_count, 2);
        assert_eq!(aggregate.topology.resolved_peer_count, 2);
        assert_eq!(aggregate.topology.unresolved_peer_count, 0);
        assert_eq!(aggregate.topology.private_peer_count, 2);
        assert_eq!(aggregate.topology.public_peer_count, 0);
    }

    #[test]
    fn mesh_target_scope_classifies_private_addresses() {
        assert!(is_private_mesh_target("10.0.0.2:9100"));
        assert!(is_private_mesh_target("127.0.0.1:9100"));
        assert!(is_private_mesh_target("[fd00::1]:9100"));
        assert!(!is_private_mesh_target("203.0.113.10:9100"));
        assert!(!is_private_mesh_target("mesh.example.com:9100"));
    }

    #[tokio::test]
    async fn stale_telemetry_is_pruned_from_api_and_control_targets() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::new(1_000);
        let now_ms = now_unix_ms();
        let mut stale = telemetry_snapshot_for_tests(
            "jp-edge-old",
            "jp-east",
            "apac",
            35.6,
            139.6,
            Vec::new(),
            7,
        );
        stale.updated_unix_ms = now_ms.saturating_sub(2_000);
        let mut live = telemetry_snapshot_for_tests(
            "jp-edge-live",
            "jp-east",
            "apac",
            35.7,
            139.7,
            Vec::new(),
            7,
        );
        live.updated_unix_ms = now_ms;
        telemetry.ingest_snapshot(stale).await;
        telemetry.ingest_snapshot(live).await;

        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let snapshot = router.mesh_api_snapshot().await;
        let targets = router
            .selected_control_target_node_ids(
                ControlKind::WarmStream,
                &ControlRequest {
                    node_id: None,
                    region: Some("jp-east".into()),
                    stream_id: Some(7),
                },
            )
            .await;

        assert_eq!(snapshot.aggregate.node_count, 2);
        assert_eq!(snapshot.telemetry.fresh_remote_count, 1);
        assert_eq!(snapshot.telemetry.stale_remote_count, 1);
        assert_eq!(snapshot.telemetry.stale_nodes[0].node_id, "jp-edge-old");
        assert_eq!(snapshot.telemetry.stale_nodes[0].region, "jp-east");
        assert!(snapshot.telemetry.stale_nodes[0].age_ms >= 1_000);
        assert!(snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "telemetry_snapshot_stale"
                && alert.node_id.as_deref() == Some("jp-edge-old")));
        assert!(snapshot
            .nodes
            .iter()
            .any(|node| node.node_id == "test-node"));
        assert!(snapshot
            .nodes
            .iter()
            .any(|node| node.node_id == "jp-edge-live"));
        assert!(!snapshot
            .nodes
            .iter()
            .any(|node| node.node_id == "jp-edge-old"));
        assert_eq!(targets, vec!["jp-edge-live"]);
        assert!(!router
            .telemetry
            .snapshots
            .read()
            .await
            .contains_key("jp-edge-old"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_includes_remote_telemetry_nodes() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        telemetry
            .ingest_snapshot(telemetry_snapshot_for_tests(
                "us-1",
                "us-east",
                "na",
                37.4,
                -78.6,
                Vec::new(),
                5,
            ))
            .await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/mesh")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();
        let body = response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(json["aggregate"]["node_count"], 2);
        assert!(json["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|node| node["node_id"] == "us-1"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_reports_operational_alerts() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        let mut remote = telemetry_snapshot_for_tests(
            "full-node",
            "us-east",
            "na",
            37.4,
            -78.6,
            vec![PeerSnapshot {
                addr: "10.0.0.9:29101".into(),
                state: "discovered".into(),
            }],
            5,
        );
        remote.node.used_storage_bytes = 960_000;
        telemetry.ingest_snapshot(remote).await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);

        router
            .execute_control(
                ControlKind::ProvisionNode,
                ControlRequest {
                    node_id: Some("new-node".into()),
                    region: Some("us-east".into()),
                    stream_id: None,
                },
            )
            .await;

        let response = router
            .route(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/mesh")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let alert_codes = json["alerts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|alert| alert["code"].as_str().unwrap())
            .collect::<HashSet<_>>();
        let activity_codes = json["activity"]
            .as_array()
            .unwrap()
            .iter()
            .map(|activity| activity["code"].as_str().unwrap())
            .collect::<HashSet<_>>();

        assert_eq!(response.status, StatusCode::OK);
        assert!(alert_codes.contains("mesh_unknown_peers"));
        assert!(alert_codes.contains("edge_playback_missing"));
        assert!(alert_codes.contains("storage_exhausted"));
        assert!(alert_codes.contains("control_skipped"));
        assert!(activity_codes.contains("mesh_snapshot"));
        assert!(activity_codes.contains("storage_exhausted"));
        assert!(activity_codes.contains("provision_node"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_reports_telemetry_peer_data_hose_status() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let peer = unused_tcp_loopback_addr();
        let monitor = TelemetryPeerMonitor::new(&[peer]);
        monitor.record_connecting(peer).await;
        monitor
            .record_disconnected(peer, Some("dial failed".into()))
            .await;
        let router = app_router_for_tests_with_telemetry_monitor(
            Arc::clone(&cache),
            Arc::clone(&mesh),
            monitor.clone(),
        );

        let snapshot = router.mesh_api_snapshot().await;
        assert_eq!(snapshot.orchestration.telemetry_peers.len(), 1);
        let peer_status = &snapshot.orchestration.telemetry_peers[0];
        assert_eq!(peer_status.peer, peer.to_string());
        assert_eq!(peer_status.state, "error");
        assert_eq!(peer_status.connect_attempts, 1);
        assert_eq!(peer_status.disconnects, 1);
        assert_eq!(peer_status.last_error.as_deref(), Some("dial failed"));
        assert!(snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "telemetry_peer_unavailable"));

        monitor.record_connecting(peer).await;
        monitor.record_connected(peer).await;
        monitor.record_payload(peer, 512).await;
        let snapshot = router.mesh_api_snapshot().await;
        let peer_status = &snapshot.orchestration.telemetry_peers[0];
        assert_eq!(peer_status.state, "connected");
        assert_eq!(peer_status.connect_attempts, 2);
        assert_eq!(peer_status.payloads, 1);
        assert_eq!(peer_status.bytes, 512);
        assert!(!snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "telemetry_peer_unavailable"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_reports_edge_response_errors() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        let missing_response = router
            .route(
                Request::builder()
                    .method(Method::GET)
                    .uri("/live/77/init.mp4")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_response.status, StatusCode::NOT_FOUND);

        let api_response = router
            .route(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/mesh")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = api_response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let edge = json["edge_services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|edge| edge["node_id"] == "test-node")
            .unwrap();
        let alert_codes = json["alerts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|alert| alert["code"].as_str().unwrap())
            .collect::<HashSet<_>>();

        assert_eq!(api_response.status, StatusCode::OK);
        assert_eq!(edge["responses_total"], 1);
        assert_eq!(edge["response_errors"], 1);
        assert_eq!(edge["response_not_found"], 1);
        assert_eq!(edge["recent_responses"][0]["path"], "/live/77/init.mp4");
        assert_eq!(edge["recent_responses"][0]["status"], 404);
        assert!(alert_codes.contains("edge_response_errors"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_reports_orchestration_status() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let disabled_router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let disabled = disabled_router.mesh_api_snapshot().await;
        assert!(!disabled.orchestration.control_dispatch_ready);
        assert!(!disabled.orchestration.provision.enabled);
        assert!(disabled.orchestration.provision.backends.is_empty());
        assert!(disabled.orchestration.provision.backend_statuses.is_empty());
        assert!(!disabled.orchestration.private_discovery.enabled);
        assert_eq!(
            disabled.orchestration.private_discovery.state,
            "unavailable"
        );

        let provision = ProvisionExecutor::new(
            Some("printf provision-ready".into()),
            Duration::from_millis(1_500),
        );
        let router =
            app_router_for_tests_with_provision(Arc::clone(&cache), Arc::clone(&mesh), provision);
        let (tx, _rx) = mpsc::channel(1);
        router.dispatch.set_sender(tx).await;
        let enabled = router.mesh_api_snapshot().await;

        assert!(enabled.orchestration.control_dispatch_ready);
        assert!(enabled.orchestration.provision.enabled);
        assert_eq!(enabled.orchestration.provision.backends, vec!["command"]);
        assert_eq!(enabled.orchestration.provision.timeout_ms, 1_500);
        assert_eq!(enabled.orchestration.provision.backend_statuses.len(), 1);
        assert_eq!(
            enabled.orchestration.provision.backend_statuses[0].name,
            "command"
        );
        assert_eq!(
            enabled.orchestration.provision.backend_statuses[0].state,
            "ready"
        );
        mesh.shutdown();
    }

    #[test]
    fn mesh_alerts_when_linode_provisioning_lacks_private_discovery() {
        let local_stream =
            telemetry_snapshot_for_tests("uk-local", "uk", "eu", 51.5, -0.1, Vec::new(), 1).stream;
        let provision = ProvisionStatus {
            enabled: true,
            backends: vec!["linode".into()],
            timeout_ms: 1_000,
            backend_statuses: Vec::new(),
        };
        let private_discovery = PrivateDiscoveryStatus {
            compiled: true,
            enabled: false,
            state: "available",
            broadcast_port: None,
            mesh_port: None,
            details: vec!["pass --private-subnet-discovery".into()],
        };
        let alerts = derive_mesh_alerts(
            &AggregateMetrics {
                node_count: 2,
                connection_count: 1,
                ..AggregateMetrics::default()
            },
            &[],
            &[],
            &[],
            &local_stream,
            "uk-local",
            &[],
            &[],
            &[],
            &TelemetryHealthSnapshot::default(),
            &RelaySessionIngressSnapshot::default(),
            &provision,
            &[],
            &private_discovery,
        );

        assert!(alerts.iter().any(|alert| {
            alert.code == "linode_private_discovery_inactive"
                && alert.node_id.as_deref() == Some("uk-local")
        }));
    }

    #[test]
    fn mesh_alerts_when_measured_relay_processing_exceeds_interactive_limit() {
        let local_stream =
            telemetry_snapshot_for_tests("edge", "jp", "apac", 35.7, 139.7, Vec::new(), 1).stream;
        let processing_duration_buckets =
            std::array::from_fn(|index| if index < 4 { 94 } else { 100 });
        let relay_nodes = vec![RelayNodeSessionSnapshot {
            node_id: "relay-primary".into(),
            region: "eu-west".into(),
            relay_session: RelaySessionIngressSnapshot {
                processing_duration_count: 100,
                processing_duration_sum_us: 50_000,
                processing_duration_max_us: 2_500,
                processing_duration_buckets,
                ..RelaySessionIngressSnapshot::default()
            },
        }];

        let alerts = derive_mesh_alerts(
            &AggregateMetrics {
                node_count: 2,
                connection_count: 1,
                ..AggregateMetrics::default()
            },
            &[],
            &[],
            &[],
            &local_stream,
            "edge",
            &[],
            &relay_nodes,
            &[],
            &TelemetryHealthSnapshot::default(),
            &RelaySessionIngressSnapshot::default(),
            &ProvisionStatus::default(),
            &[],
            &PrivateDiscoveryStatus::default(),
        );

        assert!(alerts.iter().any(|alert| {
            alert.code == "relay_processing_p95_exceeded"
                && alert.node_id.as_deref() == Some("relay-primary")
                && alert.count == 2_500
        }));
    }

    #[cfg(feature = "private-subnet-discovery")]
    #[test]
    fn private_discovery_status_reports_enabled_ports() {
        let status = PrivateDiscoveryStatus::from_args(true, 12_345, 9_101);

        assert!(status.compiled);
        assert!(status.enabled);
        assert_eq!(status.state, "listening");
        assert_eq!(status.broadcast_port, Some(12_345));
        assert_eq!(status.mesh_port, Some(9_101));
        assert!(status.details.iter().any(|detail| detail.contains("12345")));
        assert!(status.details.iter().any(|detail| detail.contains("9101")));
    }

    #[tokio::test]
    async fn mesh_api_includes_baseline_replica_plan_from_telemetry() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        telemetry
            .ingest_snapshot(telemetry_snapshot_for_tests(
                "us-1",
                "us-east",
                "na",
                37.4,
                -78.6,
                Vec::new(),
                1,
            ))
            .await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/mesh")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();
        let body = response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(response.status, StatusCode::OK);
        let planned = json["planned_replicas"].as_array().unwrap();
        assert!(planned.iter().any(|placement| placement["stream_id"] == 1
            && placement["stream_id_text"] == "1"
            && placement["target_node_id"] == "test-node"
            && placement["reason_text"] == "baseline continent test-continent"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_includes_non_default_stream_replica_plan_from_telemetry() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        let mut remote =
            telemetry_snapshot_for_tests("us-1", "us-east", "na", 37.4, -78.6, Vec::new(), 1);
        remote.streams = vec![StreamTelemetry {
            node_id: "us-1".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(4),
            latest_local_part_bytes: Some(2048),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(250),
            latest_mesh_part: Some(4),
            canonical_epoch: Some(1),
            canonical_epoch_activation_delay_us: None,
            contiguous_object: Some(4),
            head_object: Some(4),
            gap_count: Some(0),
            bytes_received: 8192,
            datagrams_received: 4,
            last_ingest_age_ms: Some(250),
            stale_threshold_ms: Some(5_000),
            mesh_lag_parts: None,
        }];
        telemetry.ingest_snapshot(remote).await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/mesh")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();
        let body = response.body.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(response.status, StatusCode::OK);
        let planned = json["planned_replicas"].as_array().unwrap();
        assert!(planned.iter().any(|placement| {
            placement["stream_id"] == 77
                && placement["stream_id_text"] == "77"
                && placement["target_node_id"] == "test-node"
                && placement["reason_text"] == "baseline continent test-continent"
        }));
        assert!(json["streams"].as_array().unwrap().iter().any(|stream| {
            stream["node_id"] == "us-1"
                && stream["stream_id"] == 77
                && stream["stream_id_text"] == "77"
        }));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_alerts_on_stale_remote_streams() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        let mut remote =
            telemetry_snapshot_for_tests("us-1", "us-east", "na", 37.4, -78.6, Vec::new(), 1);
        remote.streams = vec![StreamTelemetry {
            node_id: "us-1".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(4),
            latest_local_part_bytes: Some(2048),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(6_000),
            latest_mesh_part: Some(4),
            canonical_epoch: Some(1),
            canonical_epoch_activation_delay_us: None,
            contiguous_object: Some(4),
            head_object: Some(4),
            gap_count: Some(0),
            bytes_received: 8192,
            datagrams_received: 4,
            last_ingest_age_ms: Some(6_000),
            stale_threshold_ms: Some(5_000),
            mesh_lag_parts: None,
        }];
        telemetry.ingest_snapshot(remote).await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let snapshot = router.mesh_api_snapshot().await;

        assert!(snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "mesh_stream_stale"
                && alert.node_id.as_deref() == Some("us-1")
                && alert.stream_id_text.as_deref() == Some("77")));
        assert!(snapshot.streams.iter().any(|stream| {
            stream.node_id == "us-1"
                && stream.stream_id == 77
                && stream.last_ingest_age_ms == Some(6_000)
                && stream.stale_threshold_ms == Some(5_000)
        }));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_reports_lagging_stream_replicas() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        let mut head =
            telemetry_snapshot_for_tests("us-head", "us-east", "na", 37.4, -78.6, Vec::new(), 1);
        head.streams = vec![StreamTelemetry {
            node_id: "us-head".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(20),
            latest_local_part_bytes: Some(4096),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(250),
            latest_mesh_part: Some(20),
            canonical_epoch: Some(1),
            canonical_epoch_activation_delay_us: None,
            contiguous_object: Some(20),
            head_object: Some(20),
            gap_count: Some(0),
            bytes_received: 4096,
            datagrams_received: 1,
            last_ingest_age_ms: Some(250),
            stale_threshold_ms: Some(5_000),
            mesh_lag_parts: None,
        }];
        let mut lagging =
            telemetry_snapshot_for_tests("eu-lag", "eu-west", "eu", 51.5, -0.1, Vec::new(), 1);
        lagging.streams = vec![StreamTelemetry {
            node_id: "eu-lag".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(11),
            latest_local_part_bytes: Some(2048),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(250),
            latest_mesh_part: Some(11),
            canonical_epoch: Some(1),
            canonical_epoch_activation_delay_us: None,
            contiguous_object: Some(11),
            head_object: Some(11),
            gap_count: Some(0),
            bytes_received: 2048,
            datagrams_received: 1,
            last_ingest_age_ms: Some(250),
            stale_threshold_ms: Some(5_000),
            mesh_lag_parts: None,
        }];
        telemetry.ingest_snapshot(head).await;
        telemetry.ingest_snapshot(lagging).await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let snapshot = router.mesh_api_snapshot().await;

        assert!(snapshot.alerts.iter().any(|alert| {
            alert.code == "mesh_stream_lagging"
                && alert.node_id.as_deref() == Some("eu-lag")
                && alert.stream_id_text.as_deref() == Some("77")
        }));
        assert!(snapshot.streams.iter().any(|stream| {
            stream.node_id == "us-head"
                && stream.stream_id == 77
                && stream.mesh_lag_parts == Some(0)
        }));
        assert!(snapshot.streams.iter().any(|stream| {
            stream.node_id == "eu-lag" && stream.stream_id == 77 && stream.mesh_lag_parts == Some(9)
        }));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_reports_canonical_epoch_divergence_and_publication_gaps() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();

        for (node_id, epoch, gaps) in [("relay-a", 8, 0), ("relay-b", 9, 2)] {
            let mut remote = telemetry_snapshot_for_tests(
                node_id,
                "test-region",
                "test-continent",
                1.0,
                2.0,
                Vec::new(),
                77,
            );
            remote.streams = vec![StreamTelemetry {
                node_id: node_id.into(),
                stream_id: 77,
                stream_id_text: stream_id_text(77),
                latest_local_part: Some(4),
                latest_local_part_bytes: Some(2_048),
                latest_local_part_duration_ms: Some(500),
                latest_local_part_age_ms: Some(10),
                latest_mesh_part: Some(4),
                canonical_epoch: Some(epoch),
                canonical_epoch_activation_delay_us: Some(if node_id == "relay-b" {
                    CANONICAL_EPOCH_ACTIVATION_WARN_US + 1
                } else {
                    250_000
                }),
                contiguous_object: Some(4),
                head_object: Some(4 + gaps),
                gap_count: Some(gaps),
                bytes_received: 8_192,
                datagrams_received: 4,
                last_ingest_age_ms: Some(10),
                stale_threshold_ms: Some(5_000),
                mesh_lag_parts: None,
            }];
            telemetry.ingest_snapshot(remote).await;
        }

        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let snapshot = router.mesh_api_snapshot().await;

        assert!(snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "canonical_epoch_divergence"));
        assert!(snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "canonical_epoch_activation_slow"));
        assert!(snapshot.alerts.iter().any(|alert| {
            alert.code == "canonical_publication_gap"
                && alert.node_id.as_deref() == Some("relay-b")
                && alert.stream_id_text.as_deref() == Some("77")
        }));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_api_compares_canonical_objects_instead_of_process_local_counters() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        let mut head = telemetry_snapshot_for_tests(
            "edge-head",
            "ap-east",
            "apac",
            35.7,
            139.7,
            Vec::new(),
            77,
        );
        head.streams = vec![StreamTelemetry {
            node_id: "edge-head".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(20_000),
            latest_local_part_bytes: Some(4_096),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(20),
            latest_mesh_part: Some(500),
            canonical_epoch: Some(9),
            canonical_epoch_activation_delay_us: None,
            contiguous_object: Some(500),
            head_object: Some(500),
            gap_count: Some(0),
            bytes_received: 4_096,
            datagrams_received: 1,
            last_ingest_age_ms: Some(20),
            stale_threshold_ms: Some(5_000),
            mesh_lag_parts: None,
        }];
        let mut restarted_relay = telemetry_snapshot_for_tests(
            "relay-primary",
            "eu-west",
            "eu",
            52.3,
            4.9,
            Vec::new(),
            77,
        );
        restarted_relay.relay_session.controlled_sessions = 1;
        restarted_relay.streams = vec![StreamTelemetry {
            node_id: "relay-primary".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(200),
            latest_local_part_bytes: Some(4_096),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(20),
            latest_mesh_part: Some(500),
            canonical_epoch: Some(9),
            canonical_epoch_activation_delay_us: None,
            contiguous_object: Some(500),
            head_object: Some(500),
            gap_count: Some(0),
            bytes_received: 4_096,
            datagrams_received: 1,
            last_ingest_age_ms: Some(20),
            stale_threshold_ms: Some(5_000),
            mesh_lag_parts: None,
        }];
        telemetry.ingest_snapshot(head).await;
        telemetry.ingest_snapshot(restarted_relay).await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let snapshot = router.mesh_api_snapshot().await;

        assert!(snapshot.streams.iter().any(|stream| {
            stream.node_id == "relay-primary"
                && stream.contiguous_object == Some(500)
                && stream.mesh_lag_parts == Some(0)
        }));
        assert!(!snapshot.alerts.iter().any(|alert| {
            alert.code == "mesh_stream_lagging" && alert.node_id.as_deref() == Some("relay-primary")
        }));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_protocol_snapshot_and_sse_expose_mesh_state() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        assert!(router.is_streaming(MESH_EVENTS_PATH));

        let response = router.mesh_protocol_response_from_bytes(b"snapshot").await;
        assert!(response.ok);
        assert!(response.snapshot.is_some());

        let event = router.mesh_sse_event().await.unwrap();
        let event = std::str::from_utf8(&event).unwrap();
        assert!(event.starts_with("event: mesh\n"));
        assert!(event.contains("\"node_id\":\"test-node\""));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn mesh_protocol_warm_stream_executes_control_request() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        let response = router
            .mesh_protocol_response_from_bytes(
                br#"{"type":"warm_stream","stream_id":88,"region":"test-region"}"#,
            )
            .await;

        assert!(response.ok);
        assert!(cache.chunk_cache.get_stream_idx(88).await.is_some());
        assert_eq!(response.command.unwrap().kind, ControlKind::WarmStream);
        assert!(router
            .control
            .recent()
            .await
            .iter()
            .any(|command| command.kind == ControlKind::ReplicaRequest));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn raw_tcp_mesh_protocol_handles_snapshot_and_media_frames() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let mut input = Vec::new();
        push_raw_mesh_frame(&mut input, b"snapshot");

        let metadata = MediaFrameMetadata {
            duration_ms: 25,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(93, 4, 800, MediaCodec::Opus)
        };
        let request = serialized_media_access_unit_for_tests(metadata, b"raw-tcp-opus-frame");
        push_raw_mesh_frame(&mut input, &request);

        let (stream, written) = MemoryRawStream::new(input);
        tokio::time::timeout(
            Duration::from_secs(2),
            router.handle_stream(Box::new(stream), false),
        )
        .await
        .unwrap()
        .unwrap();
        let output = written.lock().unwrap().clone();
        let mut output = output.as_slice();

        let response = pop_raw_mesh_frame(&mut output);
        let json: serde_json::Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(json["response_type"], "snapshot");
        assert_eq!(json["snapshot"]["node"]["node_id"], "test-node");

        let response = pop_raw_mesh_frame(&mut output);
        let json: serde_json::Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(json["response_type"], "media_access_unit");
        assert_eq!(json["media_access_unit"]["stream_id"], 93);
        assert_eq!(json["media_access_unit"]["sequence"], 4);
        let unit = cache.get_media_access_unit(93, 4).await.unwrap();
        assert_eq!(
            unit.serialized.slice(MEDIA_FRAME_HEADER_LEN..),
            Bytes::from_static(b"raw-tcp-opus-frame")
        );
        assert!(output.is_empty());
        mesh.shutdown();
    }

    #[tokio::test]
    async fn webtransport_binary_media_request_ingests_access_unit() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let metadata = MediaFrameMetadata {
            duration_ms: 20,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(91, 2, 400, MediaCodec::Opus)
        };
        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: b"webtransport-opus-frame".len() as u32,
            fragment_offset: 0,
        };
        let mut request = vec![0; MEDIA_FRAME_HEADER_LEN];
        header.encode(&mut request[..]).unwrap();
        request.extend_from_slice(b"webtransport-opus-frame");

        let response = router
            .webtransport_response_from_bytes(Bytes::from(request))
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&response).unwrap();

        assert_eq!(json["response_type"], "media_access_unit");
        assert_eq!(json["media_access_unit"]["stream_id"], 91);
        assert_eq!(json["media_access_unit"]["sequence"], 2);
        let unit = cache.get_media_access_unit(91, 2).await.unwrap();
        assert_eq!(unit.metadata.codec, MediaCodec::Opus);
        assert_eq!(
            unit.serialized.slice(MEDIA_FRAME_HEADER_LEN..),
            Bytes::from_static(b"webtransport-opus-frame")
        );
        mesh.shutdown();
    }

    #[test]
    fn webtransport_media_decoder_accepts_stream_prefixed_raptorq_datagrams() {
        use raptorq_datagram_fec::{MediaFecEncoder, MediaFrame};
        use raptorq_fec_transport::{webtransport_subscription_datagram, FecDatagramEncoder};

        let mut metadata = MediaFrameMetadata::new(77, 9, 1200, MediaCodec::H264);
        metadata.duration_ms = 33;
        metadata.flags = MediaFrameFlags::keyframe();

        let transport = FecDatagramEncoder::webtransport_with_stream_prefix(77);
        let mut media_encoder = MediaFecEncoder::default();
        let encoded = transport
            .encode_media_frame(
                &mut media_encoder,
                MediaFrame {
                    metadata,
                    payload: b"prefixed-h264-access-unit",
                },
            )
            .unwrap();

        let mut decoder = WebTransportMediaDecoder::new();
        assert!(decoder
            .push_datagram(&webtransport_subscription_datagram(77))
            .unwrap()
            .is_none());

        let mut decoded = None;
        for datagram in encoded.datagrams {
            if let Some(frame) = decoder.push_datagram(&datagram).unwrap() {
                decoded = Some(frame);
            }
        }

        let frame = decoded.unwrap();
        assert_eq!(frame.metadata.stream_id, 77);
        assert_eq!(frame.metadata.sequence, 9);
        assert_eq!(frame.metadata.codec, MediaCodec::H264);
        assert!(frame.metadata.flags.is_keyframe());
        assert_eq!(frame.payload, b"prefixed-h264-access-unit");
    }

    #[tokio::test]
    async fn websocket_binary_media_request_ingests_access_unit() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let metadata = MediaFrameMetadata {
            duration_ms: 33,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(92, 3, 600, MediaCodec::Opus)
        };
        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: b"websocket-opus-frame".len() as u32,
            fragment_offset: 0,
        };
        let mut request = vec![0; MEDIA_FRAME_HEADER_LEN];
        header.encode(&mut request[..]).unwrap();
        request.extend_from_slice(b"websocket-opus-frame");

        let response = router
            .binary_mesh_response_from_bytes(Bytes::from(request))
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&response).unwrap();

        assert_eq!(json["response_type"], "media_access_unit");
        assert_eq!(json["media_access_unit"]["stream_id"], 92);
        assert_eq!(json["media_access_unit"]["sequence"], 3);
        let unit = cache.get_media_access_unit(92, 3).await.unwrap();
        assert_eq!(unit.metadata.codec, MediaCodec::Opus);
        assert_eq!(
            unit.serialized.slice(MEDIA_FRAME_HEADER_LEN..),
            Bytes::from_static(b"websocket-opus-frame")
        );
        mesh.shutdown();
    }

    #[tokio::test]
    async fn local_control_publishes_avmc_when_feed_is_available() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let (tx, mut rx) = mpsc::channel(1);
        router.dispatch.set_sender(tx).await;

        let command = router
            .execute_control(
                ControlKind::WarmStream,
                ControlRequest {
                    node_id: None,
                    region: Some("test-region".into()),
                    stream_id: Some(91),
                },
            )
            .await;

        assert!(command.status.contains("published AVMC control"));
        assert_eq!(command.target_text, "region test-region / stream 91");
        let command_json = serde_json::to_value(&command).unwrap();
        assert_eq!(
            command_json["target_text"],
            "region test-region / stream 91"
        );
        assert!(command_json["created_unix_ms"].as_u64().unwrap() > 0);
        let message = rx.recv().await.unwrap();
        assert_eq!(message.tag(), CONTROL_TAG);
        let envelope: ControlEnvelope = serde_json::from_slice(&message.data()[0]).unwrap();
        assert_eq!(envelope.id, command.id);
        assert_eq!(envelope.origin_node_id, "test-node");
        assert_eq!(envelope.kind, ControlKind::WarmStream);
        mesh.shutdown();
    }

    #[tokio::test]
    async fn tcp_changes_feed_carries_avmt_and_avmc_between_nodes() {
        use tokio::time::timeout;

        let (cert, key) = tls_pair_for_tests();
        let source_cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let collector_cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let source_mesh = mesh_handle_for_tests(Arc::clone(&source_cache.chunk_cache)).await;
        let collector_mesh = mesh_handle_for_tests(Arc::clone(&collector_cache.chunk_cache)).await;

        let source_router = app_router_for_tests_with_node(
            Arc::clone(&source_cache),
            Arc::clone(&source_mesh),
            mesh_node_for_tests("eu-source", "eu-west", "eu", 51.5, -0.1),
        );
        let collector_router = app_router_for_tests_with_node(
            Arc::clone(&collector_cache),
            Arc::clone(&collector_mesh),
            mesh_node_for_tests("jp-edge", "jp-east", "apac", 35.7, 139.7),
        );

        let telemetry_bind = unused_tcp_loopback_addr();
        let (publisher_shutdown_tx, publisher_shutdown_rx) = watch::channel(());
        let telemetry_runtime = start_telemetry_feed(
            telemetry_bind,
            Ipv4Addr::LOCALHOST,
            cert.clone(),
            key,
            50,
            Arc::clone(&source_cache),
            Arc::clone(&source_mesh),
            source_router.node.clone(),
            source_router.replication_policy.clone(),
            source_router.control.clone(),
            source_router.lifecycle.clone(),
            source_router.dispatch.clone(),
            source_router.playback_base_url.clone(),
            source_router.edge_load.clone(),
            publisher_shutdown_rx,
        )
        .await
        .unwrap();

        let (collector_shutdown_tx, collector_shutdown_rx) = watch::channel(());
        let mut collector_task = {
            let collector_router = collector_router.clone();
            let cert = cert.clone();
            tokio::spawn(async move {
                connect_telemetry_peer(
                    telemetry_bind,
                    "local.wavey.ai",
                    &cert,
                    collector_router,
                    TelemetryPeerMonitor::new(&[telemetry_bind]),
                    collector_shutdown_rx,
                )
                .await
            })
        };

        timeout(Duration::from_secs(3), async {
            loop {
                if collector_task.is_finished() {
                    let result = (&mut collector_task).await;
                    panic!("telemetry collector exited early: {result:?}");
                }
                let snapshot = collector_router.mesh_api_snapshot().await;
                if snapshot
                    .nodes
                    .iter()
                    .any(|node| node.node_id == "eu-source")
                {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();

        let command = source_router
            .execute_control(
                ControlKind::WarmStream,
                ControlRequest {
                    node_id: None,
                    region: Some("jp-east".into()),
                    stream_id: Some(77),
                },
            )
            .await;
        assert!(command.status.contains("published AVMC control"));

        timeout(Duration::from_secs(3), async {
            loop {
                if collector_cache
                    .chunk_cache
                    .get_stream_idx(77)
                    .await
                    .is_some()
                {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();

        assert!(collector_router
            .control
            .recent()
            .await
            .iter()
            .any(|command| {
                command.kind == ControlKind::WarmStream
                    && command.status.contains("received from eu-source")
            }));

        let _ = collector_shutdown_tx.send(());
        let _ = publisher_shutdown_tx.send(());
        let _ = telemetry_runtime.shutdown_tx.send(());
        timeout(Duration::from_secs(2), &mut collector_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), telemetry_runtime.finished_rx)
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), telemetry_runtime.publisher_task)
            .await
            .unwrap()
            .unwrap();
        source_mesh.shutdown();
        collector_mesh.shutdown();
    }

    #[tokio::test]
    async fn provision_node_control_runs_configured_executor() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests_with_provision(
            Arc::clone(&cache),
            Arc::clone(&mesh),
            ProvisionExecutor::new(
                Some(
                    "test \"$AV_MESH_PROVISION_REGION\" = eu-test && test \"$AV_MESH_PROVISION_NODE_ID\" = eu-test-2 && printf provision-ok"
                        .into(),
                ),
                Duration::from_secs(1),
            ),
        );

        let command = router
            .execute_control(
                ControlKind::ProvisionNode,
                ControlRequest {
                    node_id: Some("eu-test-2".into()),
                    region: Some("eu-test".into()),
                    stream_id: None,
                },
            )
            .await;

        assert!(command.status.contains("local provision executed"));
        assert!(command.status.contains("provision-ok"));
        mesh.shutdown();
    }

    #[cfg(feature = "linode-provisioner")]
    #[tokio::test]
    async fn linode_provision_reports_missing_env_without_network() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let provision = ProvisionExecutor::new(None, Duration::from_secs(1)).with_linode(Some(
            LinodeProvisionConfig {
                token_env: "AV_MESH_TEST_MISSING_LINODE_TOKEN".into(),
                pub_key_env: "AV_MESH_TEST_MISSING_LINODE_PUB_KEY".into(),
                image_id: "linode/arch".into(),
                instance_type: "g6-dedicated-2".into(),
                domain_id: 2_958_920,
                vlan_tag: "avmesh".into(),
                region_map: BTreeMap::from([("uk".into(), "gb-lon".into())]),
            },
        ));
        let router =
            app_router_for_tests_with_provision(Arc::clone(&cache), Arc::clone(&mesh), provision);

        let status = router.mesh_api_snapshot().await.orchestration.provision;
        assert!(status.enabled);
        assert_eq!(status.backends, vec!["linode"]);
        assert_eq!(status.backend_statuses.len(), 1);
        assert_eq!(status.backend_statuses[0].name, "linode");
        assert_eq!(status.backend_statuses[0].state, "blocked");
        assert!(status.backend_statuses[0]
            .details
            .iter()
            .any(|detail| detail.contains("AV_MESH_TEST_MISSING_LINODE_TOKEN missing")));
        assert!(router
            .mesh_api_snapshot()
            .await
            .alerts
            .iter()
            .any(|alert| alert.code == "provision_backend_blocked"));

        let command = router
            .execute_control(
                ControlKind::ProvisionNode,
                ControlRequest {
                    node_id: Some("uk-test-2".into()),
                    region: Some("uk".into()),
                    stream_id: None,
                },
            )
            .await;

        assert!(command
            .status
            .contains("local linode provision skipped: missing AV_MESH_TEST_MISSING_LINODE_TOKEN"));
        mesh.shutdown();
    }

    #[cfg(feature = "linode-provisioner")]
    #[test]
    fn linode_provision_status_reports_private_subnet_details() {
        let result = linode::ScaleUpResult {
            instance_id: 42,
            label: "gb-lon-test".into(),
            public_ipv4: "203.0.113.10".into(),
            private_ipam_address: "10.0.0.5/24".into(),
            vlan_label: "avmesh".into(),
            dns_name: Some("avmesh-gb-lon-1".into()),
            region_code: "gb-lon".into(),
            linode_region: "gb-lon".into(),
        };

        let status = format_linode_provision_result("uk", &result);

        assert!(status.contains("requested_region=uk"));
        assert!(status.contains("instance_id=42"));
        assert!(status.contains("private_ipam=10.0.0.5/24"));
        assert!(status.contains("vlan=avmesh"));
        assert!(status.contains("dns=avmesh-gb-lon-1"));
    }

    #[tokio::test]
    async fn warm_stream_control_dispatches_selected_nodes_from_telemetry() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let telemetry = TelemetryAggregator::default();
        telemetry
            .ingest_snapshot(telemetry_snapshot_for_tests(
                "jp-edge-1",
                "jp-east",
                "apac",
                35.6,
                139.6,
                Vec::new(),
                77,
            ))
            .await;
        let router =
            app_router_for_tests_with_telemetry(Arc::clone(&cache), Arc::clone(&mesh), telemetry);
        let (tx, mut rx) = mpsc::channel(1);
        router.dispatch.set_sender(tx).await;

        let command = router
            .execute_control(
                ControlKind::WarmStream,
                ControlRequest {
                    node_id: None,
                    region: Some("jp-east".into()),
                    stream_id: Some(77),
                },
            )
            .await;

        assert!(command
            .status
            .contains("published AVMC control to jp-edge-1"));
        let message = rx.recv().await.unwrap();
        let envelope: ControlEnvelope = serde_json::from_slice(&message.data()[0]).unwrap();
        assert_eq!(envelope.target_node_ids, vec!["jp-edge-1"]);
        assert_eq!(envelope.request.region.as_deref(), Some("jp-east"));
        assert!(cache.chunk_cache.get_stream_idx(77).await.is_none());
        mesh.shutdown();
    }

    #[tokio::test]
    async fn local_global_distribution_scenario_uses_avmt_and_avmc() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let policy = ReplicationPolicy {
            baseline_per_region: 1,
            baseline_per_continent: 1,
            min_mirror_distance_km: 700.0,
            max_new_replicas_per_plan: 16,
            ..ReplicationPolicy::default()
        };
        let router = app_router_for_tests_with_policy_and_telemetry(
            Arc::clone(&cache),
            Arc::clone(&mesh),
            policy,
            TelemetryAggregator::default(),
        );

        let mut eu_source =
            telemetry_snapshot_for_tests("eu-source", "eu-west", "eu", 51.5, -0.1, Vec::new(), 1);
        eu_source.mesh_addr = Some("10.0.0.10:9100".into());
        eu_source.peers = vec![PeerSnapshot {
            addr: "10.0.0.22:9100".into(),
            state: "discovered".into(),
        }];
        eu_source.streams = vec![StreamTelemetry {
            node_id: "eu-source".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(8),
            latest_local_part_bytes: Some(16_384),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(250),
            latest_mesh_part: Some(8),
            canonical_epoch: Some(1),
            canonical_epoch_activation_delay_us: None,
            contiguous_object: Some(8),
            head_object: Some(8),
            gap_count: Some(0),
            bytes_received: 262_144,
            datagrams_received: 128,
            last_ingest_age_ms: Some(250),
            stale_threshold_ms: Some(5_000),
            mesh_lag_parts: None,
        }];

        let mut na_edge =
            telemetry_snapshot_for_tests("na-edge", "us-east", "na", 37.4, -78.6, Vec::new(), 1);
        na_edge.mesh_addr = Some("10.0.0.20:9100".into());

        let mut jp_full = telemetry_snapshot_for_tests(
            "jp-full",
            "jp-east",
            "apac",
            35.60,
            139.60,
            Vec::new(),
            1,
        );
        jp_full.mesh_addr = Some("10.0.0.21:9100".into());
        jp_full.node.used_storage_bytes = jp_full.node.total_storage_bytes.saturating_sub(1_000);

        let mut jp_healthy = telemetry_snapshot_for_tests(
            "jp-healthy",
            "jp-east",
            "apac",
            35.70,
            139.75,
            vec![PeerSnapshot {
                addr: "10.0.0.10:9100".into(),
                state: "discovered".into(),
            }],
            1,
        );
        jp_healthy.mesh_addr = Some("10.0.0.22:9100".into());
        jp_healthy.node.used_storage_bytes = 50_000;
        jp_healthy.node.egress_capacity_bps = 20_000_000_000;

        for snapshot in [eu_source, na_edge, jp_full, jp_healthy] {
            assert!(router
                .ingest_tcp_changes_payload(TcpChangesPayload {
                    tag: TELEMETRY_TAG,
                    val: Bytes::from(serde_json::to_vec(&snapshot).unwrap()),
                })
                .await
                .unwrap());
        }

        let snapshot = router.mesh_api_snapshot().await;
        assert_eq!(snapshot.aggregate.node_count, 5);
        assert!(snapshot.connections.iter().any(|connection| {
            connection.source_node_id == "jp-healthy"
                && connection.target_addr == "10.0.0.10:9100"
                && connection.target_node_id.as_deref() == Some("eu-source")
        }));

        let stream_77_targets = snapshot
            .planned_replicas
            .iter()
            .filter(|placement| placement.stream_id == 77)
            .map(|placement| placement.target_node_id.as_str())
            .collect::<HashSet<_>>();
        assert!(stream_77_targets.contains("jp-healthy"));
        assert!(!stream_77_targets.contains("jp-full"));

        let (tx, mut rx) = mpsc::channel(1);
        router.dispatch.set_sender(tx).await;
        let command = router
            .execute_control(
                ControlKind::WarmStream,
                ControlRequest {
                    node_id: None,
                    region: Some("jp-east".into()),
                    stream_id: Some(77),
                },
            )
            .await;
        assert!(command.status.contains("published AVMC control to"));

        let message = rx.recv().await.unwrap();
        assert_eq!(message.tag(), CONTROL_TAG);
        let envelope: ControlEnvelope = serde_json::from_slice(&message.data()[0]).unwrap();
        assert_eq!(envelope.kind, ControlKind::WarmStream);
        assert_eq!(envelope.request.stream_id, Some(77));
        assert_eq!(envelope.request.region.as_deref(), Some("jp-east"));
        assert_eq!(envelope.target_node_ids, vec!["jp-full", "jp-healthy"]);
        assert!(cache.chunk_cache.get_stream_idx(77).await.is_none());
        mesh.shutdown();
    }

    #[tokio::test]
    async fn remote_avmc_warm_stream_runs_on_matching_region() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let envelope = ControlEnvelope {
            id: 12,
            origin_node_id: "central".into(),
            kind: ControlKind::WarmStream,
            request: ControlRequest {
                node_id: None,
                region: Some("test-region".into()),
                stream_id: Some(92),
            },
            target_node_ids: Vec::new(),
        };
        let payload = TcpChangesPayload {
            tag: CONTROL_TAG,
            val: Bytes::from(serde_json::to_vec(&envelope).unwrap()),
        };

        assert!(router.ingest_tcp_changes_payload(payload).await.unwrap());
        assert!(cache.chunk_cache.get_stream_idx(92).await.is_some());
        assert!(router.control.recent().await.iter().any(|command| {
            command.kind == ControlKind::WarmStream && command.status.contains("central")
        }));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn remote_avmc_ignores_non_matching_targets() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let envelope = ControlEnvelope {
            id: 13,
            origin_node_id: "central".into(),
            kind: ControlKind::WarmStream,
            request: ControlRequest {
                node_id: Some("other-node".into()),
                region: Some("test-region".into()),
                stream_id: Some(93),
            },
            target_node_ids: Vec::new(),
        };
        let payload = TcpChangesPayload {
            tag: CONTROL_TAG,
            val: Bytes::from(serde_json::to_vec(&envelope).unwrap()),
        };

        assert!(!router.ingest_tcp_changes_payload(payload).await.unwrap());
        assert!(cache.chunk_cache.get_stream_idx(93).await.is_none());
        mesh.shutdown();
    }

    #[tokio::test]
    async fn close_node_control_marks_local_node_draining() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));

        let command = router
            .execute_control(
                ControlKind::CloseNode,
                ControlRequest {
                    node_id: Some("test-node".into()),
                    region: None,
                    stream_id: None,
                },
            )
            .await;

        assert!(command.status.contains("draining"));
        assert!(router.mesh_api_snapshot().await.node.draining);
        mesh.shutdown();
    }

    #[tokio::test]
    async fn warm_stream_control_creates_stream_and_records_command() {
        const SNOWFLAKE_STREAM_ID: u64 = 9_007_199_254_741_993;

        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/control/warm-stream")
            .body(())
            .unwrap();
        let body: BodyStream = Box::pin(futures_util::stream::iter(vec![Ok(Bytes::from_static(
            br#"{"stream_id":"9007199254741993","region":"test-region"}"#,
        ))]));

        let response = router.route_body(req, body).await.unwrap();

        assert_eq!(response.status, StatusCode::ACCEPTED);
        assert!(cache
            .chunk_cache
            .get_stream_idx(SNOWFLAKE_STREAM_ID)
            .await
            .is_some());
        let commands = router.control.recent().await;
        assert_eq!(commands.len(), 2);
        assert!(commands
            .iter()
            .any(|command| command.kind == ControlKind::WarmStream
                && command.stream_id == Some(SNOWFLAKE_STREAM_ID)
                && command.stream_id_text.as_deref() == Some("9007199254741993")));
        assert!(commands.iter().any(|command| {
            command.kind == ControlKind::ReplicaRequest
                && command.stream_id == Some(SNOWFLAKE_STREAM_ID)
                && command.stream_id_text.as_deref() == Some("9007199254741993")
        }));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn warm_stream_control_dispatches_regional_target() {
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh));
        let (tx, mut rx) = mpsc::channel(1);
        router.dispatch.set_sender(tx).await;
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/control/warm-stream")
            .body(())
            .unwrap();
        let body: BodyStream = Box::pin(futures_util::stream::iter(vec![Ok(Bytes::from_static(
            br#"{"stream_id":77,"region":"jp-east"}"#,
        ))]));

        let response = router.route_body(req, body).await.unwrap();

        assert_eq!(response.status, StatusCode::ACCEPTED);
        let commands = router.control.recent().await;
        assert!(commands
            .iter()
            .any(|command| command.kind == ControlKind::WarmStream
                && command.status.contains("published AVMC control")));
        assert!(cache.chunk_cache.get_stream_idx(77).await.is_none());
        let message = rx.recv().await.unwrap();
        assert_eq!(message.tag(), CONTROL_TAG);
        let envelope: ControlEnvelope = serde_json::from_slice(&message.data()[0]).unwrap();
        assert_eq!(envelope.kind, ControlKind::WarmStream);
        assert_eq!(envelope.request.region.as_deref(), Some("jp-east"));
        mesh.shutdown();
    }

    #[tokio::test]
    async fn baseline_planner_fetches_when_local_node_is_selected_replica() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-baseline", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("test-node", "test-region", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a.push_payload(b"baseline-part").await.unwrap();
        cache_a.rotate_if_due(true).await.unwrap();

        let telemetry = TelemetryAggregator::default();
        telemetry
            .ingest_snapshot(telemetry_snapshot_for_tests(
                "uk-baseline",
                "uk",
                "eu",
                51.5,
                -0.1,
                Vec::new(),
                1,
            ))
            .await;
        let router = app_router_for_tests_with_telemetry(
            Arc::clone(&cache_b),
            Arc::clone(&mesh_b),
            telemetry,
        );

        assert!(
            router
                .request_planned_local_replica(1, "baseline-replication")
                .await
        );

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) = cache_b.get_part_blocking(0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"baseline-part"));
        assert!(router.control.recent().await.iter().any(|command| {
            command.kind == ControlKind::ReplicaRequest
                && command.status.contains("baseline-replication")
        }));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn baseline_planner_fetches_non_default_stream_from_telemetry() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-baseline", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("test-node", "test-region", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"baseline-stream-77"))
            .await
            .unwrap();

        let telemetry = TelemetryAggregator::default();
        let mut remote =
            telemetry_snapshot_for_tests("uk-baseline", "uk", "eu", 51.5, -0.1, Vec::new(), 1);
        remote.streams = vec![StreamTelemetry {
            node_id: "uk-baseline".into(),
            stream_id: 77,
            stream_id_text: stream_id_text(77),
            latest_local_part: Some(0),
            latest_local_part_bytes: Some(b"baseline-stream-77".len()),
            latest_local_part_duration_ms: Some(500),
            latest_local_part_age_ms: Some(250),
            latest_mesh_part: Some(0),
            canonical_epoch: Some(1),
            canonical_epoch_activation_delay_us: None,
            contiguous_object: Some(0),
            head_object: Some(0),
            gap_count: Some(0),
            bytes_received: b"baseline-stream-77".len() as u64,
            datagrams_received: 1,
            last_ingest_age_ms: Some(250),
            stale_threshold_ms: Some(5_000),
            mesh_lag_parts: None,
        }];
        telemetry.ingest_snapshot(remote).await;

        let router = app_router_for_tests_with_telemetry(
            Arc::clone(&cache_b),
            Arc::clone(&mesh_b),
            telemetry,
        );

        let requested = router
            .request_planned_local_replicas("baseline-replication")
            .await;
        assert_eq!(requested, vec![77]);

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) = cache_b.chunk_cache.get_for_stream_id(77, 0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"baseline-stream-77"));
        assert!(router.control.recent().await.iter().any(|command| {
            command.kind == ControlKind::ReplicaRequest && command.stream_id == Some(77)
        }));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn mesh_byte_slots_replicate_to_peer_cache() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mesh_a = CacheMesh::new(
            Arc::clone(&cache_a.chunk_cache),
            CacheMeshConfig::new("uk-http-test", "uk", mesh_a_addr).with_peer(mesh_b_addr),
        )
        .start()
        .await
        .unwrap();
        let mesh_b = CacheMesh::new(
            Arc::clone(&cache_b.chunk_cache),
            CacheMeshConfig::new("us-http-test", "us", mesh_b_addr).with_peer(mesh_a_addr),
        )
        .start()
        .await
        .unwrap();

        cache_a
            .chunk_cache
            .add_for_stream_id(1, 0, Bytes::from_static(b"mesh-byte-part"))
            .await
            .unwrap();

        let bytes = timeout(Duration::from_secs(5), async {
            loop {
                if let Some((bytes, hash)) = cache_b.chunk_cache.get_for_stream_id(1, 0).await {
                    if hash != 0 || !bytes.is_empty() {
                        break bytes;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"mesh-byte-part"));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn media_access_unit_demand_replicates_non_ts_stream_slot() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-media", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("us-media", "us", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = Arc::new(
            CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
                .start()
                .await
                .unwrap(),
        );
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );
        let router_b = app_router_for_tests(Arc::clone(&cache_b), Arc::clone(&mesh_b));

        let metadata = MediaFrameMetadata {
            duration_ms: 20,
            ..MediaFrameMetadata::new(88, 0, 900, MediaCodec::Opus)
        };
        cache_a
            .add_media_access_unit(metadata, Bytes::from_static(b"opus-frame"))
            .await
            .unwrap();

        let req = Request::builder()
            .method(Method::GET)
            .uri("/media/88/unit/0")
            .body(())
            .unwrap();
        let response = router_b.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::NOT_FOUND);

        let unit = timeout(Duration::from_secs(3), async {
            loop {
                if let Some(unit) = cache_b.get_media_access_unit(88, 0).await {
                    break unit;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(unit.metadata.codec, MediaCodec::Opus);
        assert_eq!(&unit.serialized[MEDIA_FRAME_HEADER_LEN..], b"opus-frame");
        let req = Request::builder()
            .method(Method::GET)
            .uri("/media/88/unit/0")
            .body(())
            .unwrap();
        let response = router_b.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.body.unwrap().slice(MEDIA_FRAME_HEADER_LEN..),
            Bytes::from_static(b"opus-frame")
        );
        assert!(router_b
            .control
            .recent()
            .await
            .iter()
            .any(|command| command.kind == ControlKind::ReplicaRequest));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn playlist_demand_requests_mesh_replica() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-demand", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("jp-demand", "jp", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a.push_payload(b"playlist-demand-part").await.unwrap();
        cache_a.rotate_if_due(true).await.unwrap();

        let router = app_router_for_tests(Arc::clone(&cache_b), Arc::clone(&mesh_b));
        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/stream.m3u8")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) = cache_b.get_part_blocking(0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"playlist-demand-part"));
        assert!(router
            .control
            .recent()
            .await
            .iter()
            .any(|command| command.kind == ControlKind::ReplicaRequest));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn stream_specific_playlist_can_read_playlist_id_from_any_mesh_node() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-playlist", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("jp-playlist", "jp", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"playlist-77-part0"))
            .await
            .unwrap();
        cache_a
            .chunk_cache
            .add_for_stream_id(77, 1, Bytes::from_static(b"playlist-77-part1"))
            .await
            .unwrap();

        let router = app_router_for_tests(Arc::clone(&cache_b), Arc::clone(&mesh_b));
        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/stream.m3u8")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);

        timeout(Duration::from_secs(3), async {
            loop {
                if cache_b.chunk_cache.get_for_stream_id(77, 1).await.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/stream.m3u8")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        let playlist = String::from_utf8(response.body.unwrap().to_vec()).unwrap();
        assert!(playlist.contains("part0.ts"));
        assert!(playlist.contains("part1.ts"));
        assert!(playlist.contains("seg0.ts"));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/part0.ts")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.body.unwrap(),
            Bytes::from_static(b"playlist-77-part0")
        );

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/seg0.ts")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.body.unwrap(),
            Bytes::from_static(b"playlist-77-part0playlist-77-part1")
        );

        assert!(router.control.recent().await.iter().any(|command| {
            command.kind == ControlKind::ReplicaRequest && command.stream_id == Some(77)
        }));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn llhls_tail_can_read_playlist_id_from_any_mesh_node() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-llhls", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("jp-llhls", "jp", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"llhls-stream-77"))
            .await
            .unwrap();

        let router = app_router_for_tests(Arc::clone(&cache_b), Arc::clone(&mesh_b));
        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/tail?mode=part")
            .body(())
            .unwrap();
        let first_response = router.route(req).await.unwrap();
        assert!(
            first_response.status == StatusCode::NO_CONTENT
                || first_response.status == StatusCode::OK
        );

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) = cache_b.chunk_cache.get_for_stream_id(77, 0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"llhls-stream-77"));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/tail?mode=part")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.body.unwrap(),
            Bytes::from_static(b"llhls-stream-77")
        );
        assert_eq!(
            response
                .headers
                .iter()
                .find(|(name, _)| name == "x-sequence")
                .map(|(_, value)| value.as_ref()),
            Some("0")
        );
        assert_eq!(
            response
                .headers
                .iter()
                .find(|(name, _)| name == "x-av-stream-id")
                .map(|(_, value)| value.as_ref()),
            Some("77")
        );

        let req = Request::builder()
            .method(Method::GET)
            .uri("/live/77/tail?mode=part&after=0")
            .body(())
            .unwrap();
        let response = router.route(req).await.unwrap();
        assert_eq!(response.status, StatusCode::NO_CONTENT);

        let snapshot = router.mesh_api_snapshot().await;
        let edge = snapshot
            .edge_services
            .iter()
            .find(|service| service.node_id == "test-node")
            .unwrap();
        assert!(edge.llhls_tail_requests >= 3);
        assert!(edge.requests_served >= 3);
        assert!(edge.bytes_served >= b"llhls-stream-77".len() as u64);
        assert!(router.control.recent().await.iter().any(|command| {
            command.kind == ControlKind::ReplicaRequest && command.stream_id == Some(77)
        }));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn llhls_tail_waits_for_the_next_part_without_a_polling_sleep() {
        use tokio::time::timeout;

        let cache = LiveTsCache::new(1, Duration::from_millis(5), 200, 600, 64).await;
        assert_eq!(
            cache
                .commit_stream_payload(77, Bytes::from_static(b"first"))
                .await
                .unwrap(),
            0
        );

        let waiting_cache = Arc::clone(&cache);
        let waiter = tokio::spawn(async move {
            waiting_cache
                .next_part_after_blocking_for_stream_id(77, Some(0), false)
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        assert_eq!(
            cache
                .commit_stream_payload(77, Bytes::from_static(b"second"))
                .await
                .unwrap(),
            1
        );
        let (sequence, bytes, _) = timeout(Duration::from_millis(100), waiter)
            .await
            .expect("blocking tail should wake immediately")
            .unwrap()
            .expect("next LL-HLS part");
        assert_eq!(sequence, 1);
        assert_eq!(bytes, Bytes::from_static(b"second"));
    }

    #[tokio::test]
    async fn exact_llhls_part_waiters_wake_only_for_the_requested_sequence() {
        use tokio::time::timeout;

        let cache = LiveTsCache::new(1, Duration::from_millis(5), 200, 600, 64).await;
        let first_cache = Arc::clone(&cache);
        let first =
            tokio::spawn(async move { first_cache.get_part_blocking_for_stream_id(77, 0).await });
        let second_cache = Arc::clone(&cache);
        let second =
            tokio::spawn(async move { second_cache.get_part_blocking_for_stream_id(77, 1).await });
        tokio::task::yield_now().await;

        assert_eq!(
            cache
                .commit_stream_payload(77, Bytes::from_static(b"first"))
                .await
                .unwrap(),
            0
        );
        let (bytes, _) = timeout(Duration::from_millis(100), first)
            .await
            .expect("sequence zero waiter should wake immediately")
            .unwrap()
            .expect("sequence zero should be cached");
        assert_eq!(bytes, Bytes::from_static(b"first"));
        assert!(!second.is_finished());

        assert_eq!(
            cache
                .commit_stream_payload(77, Bytes::from_static(b"second"))
                .await
                .unwrap(),
            1
        );
        let (bytes, _) = timeout(Duration::from_millis(100), second)
            .await
            .expect("sequence one waiter should wake immediately")
            .unwrap()
            .expect("sequence one should be cached");
        assert_eq!(bytes, Bytes::from_static(b"second"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "release-mode isolated capacity qualification"]
    async fn isolated_cached_pcm_part_router_capacity() {
        const STREAM_IDS: [u64; 2] = [77, 78];
        const PARTS: usize = 512;
        const PART_BYTES: usize = 5_760;
        const STEP_DURATION: Duration = Duration::from_secs(2);

        let cache = LiveTsCache::new(1, Duration::from_millis(5), 200, 600, 64).await;
        let payload = vec![0x5a_u8; PART_BYTES];
        for stream_id in STREAM_IDS {
            cache
                .chunk_cache
                .set_stream_initialization(stream_id, Bytes::from_static(b"qualification-init"))
                .await
                .unwrap();
            cache
                .remember_media_kind(stream_id, LiveMediaKind::Fmp4)
                .await;
            for sequence in 0..PARTS {
                cache
                    .chunk_cache
                    .add_for_stream_id(stream_id, sequence, encode_test_fmp4_slot(None, &payload))
                    .await
                    .unwrap();
            }
        }

        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = Arc::new(app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh)));
        let mut steps = Vec::new();
        for workers in [1_usize, 2, 4, 8] {
            let barrier = Arc::new(tokio::sync::Barrier::new(workers + 1));
            let deadline = Instant::now() + STEP_DURATION;
            let mut tasks = Vec::new();
            for worker in 0..workers {
                let router = Arc::clone(&router);
                let barrier = Arc::clone(&barrier);
                tasks.push(tokio::spawn(async move {
                    barrier.wait().await;
                    let mut requests = 0_u64;
                    let mut failures = 0_u64;
                    let mut latency_ns = Vec::new();
                    let mut sequence = worker % PARTS;
                    let mut stream = worker % STREAM_IDS.len();
                    loop {
                        let sampled = requests.is_multiple_of(1_024);
                        let sample_started = sampled.then(Instant::now);
                        let request = Request::builder()
                            .method(Method::GET)
                            .uri(format!("/live/{}/part{sequence}.mp4", STREAM_IDS[stream]))
                            .body(())
                            .unwrap();
                        match router.route(request).await {
                            Ok(response)
                                if response.status == StatusCode::OK
                                    && response
                                        .body
                                        .as_ref()
                                        .is_some_and(|body| body.len() == PART_BYTES) =>
                            {
                                requests += 1;
                            }
                            _ => failures += 1,
                        }
                        if let Some(sample_started) = sample_started {
                            latency_ns.push(
                                sample_started
                                    .elapsed()
                                    .as_nanos()
                                    .min(u128::from(u64::MAX))
                                    as u64,
                            );
                        }
                        sequence = (sequence + 1) % PARTS;
                        if sequence == 0 {
                            stream = (stream + 1) % STREAM_IDS.len();
                        }
                        if (requests + failures).is_multiple_of(256) && Instant::now() >= deadline {
                            break;
                        }
                    }
                    (requests, failures, latency_ns)
                }));
            }
            barrier.wait().await;
            let started = Instant::now();
            let mut requests = 0_u64;
            let mut failures = 0_u64;
            let mut latency_ns = Vec::new();
            for task in tasks {
                let (worker_requests, worker_failures, worker_latency_ns) = task.await.unwrap();
                requests += worker_requests;
                failures += worker_failures;
                latency_ns.extend(worker_latency_ns);
            }
            let elapsed_seconds = started.elapsed().as_secs_f64();
            latency_ns.sort_unstable();
            let percentile_us = |percentile: usize| {
                let rank = latency_ns.len().saturating_mul(percentile).div_ceil(100);
                latency_ns[rank.clamp(1, latency_ns.len()) - 1] as f64 / 1_000.0
            };
            steps.push(serde_json::json!({
                "workers": workers,
                "duration_seconds": elapsed_seconds,
                "requests": requests,
                "failures": failures,
                "requests_per_second": requests as f64 / elapsed_seconds,
                "customer_equivalents_at_400_part_requests_per_second":
                    requests as f64 / elapsed_seconds / 400.0,
                "logical_payload_gbit_per_second":
                    requests as f64 * PART_BYTES as f64 * 8.0 / elapsed_seconds / 1e9,
                "sampled_route_latency_us": {
                    "samples": latency_ns.len(),
                    "p50": percentile_us(50),
                    "p95": percentile_us(95),
                    "p99": percentile_us(99),
                    "max": latency_ns.last().copied().unwrap_or(0) as f64 / 1_000.0,
                }
            }));
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "schema": "needletail.av-mesh.router-capacity.v1",
                "boundary": "B3_seeded_cached_pcm_part_AppRouter_route",
                "bytes_per_part": PART_BYTES,
                "part_ms": 5,
                "streams": STREAM_IDS,
                "production_costs_included": [
                    "request_replica_for_stream",
                    "DemandTracker",
                    "LiveTsCache part decode and media-kind tracking",
                    "EdgeLoad response telemetry",
                    "path and response construction"
                ],
                "production_costs_excluded": ["HTTP/3", "TLS", "QUIC", "UDP", "network"],
                "steps": steps
            }))
            .unwrap()
        );
        mesh.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "release-mode isolated capacity qualification"]
    async fn isolated_cached_five_ms_playlist_router_capacity() {
        const STREAM_ID: u64 = 77;
        const PARTS: usize = 600;
        const PART_BYTES: usize = 5_760;
        const STEP_DURATION: Duration = Duration::from_secs(2);

        let cache = LiveTsCache::new(1, Duration::from_millis(5), 200, PARTS, 64).await;
        cache
            .chunk_cache
            .set_stream_initialization(STREAM_ID, Bytes::from_static(b"qualification-init"))
            .await
            .unwrap();
        cache
            .remember_media_kind(STREAM_ID, LiveMediaKind::Fmp4)
            .await;
        let payload = vec![0x5a_u8; PART_BYTES];
        for sequence in 0..PARTS {
            cache
                .chunk_cache
                .add_for_stream_id(STREAM_ID, sequence, encode_test_fmp4_slot(None, &payload))
                .await
                .unwrap();
        }
        let expected_playlist = cache.playlist_for_stream_id(STREAM_ID).await;
        assert!(expected_playlist.contains("PART-TARGET=0.005"));
        let playlist_bytes = expected_playlist.len();

        let mesh = mesh_handle_for_tests(Arc::clone(&cache.chunk_cache)).await;
        let router = Arc::new(app_router_for_tests(Arc::clone(&cache), Arc::clone(&mesh)));
        let mut steps = Vec::new();
        for workers in [1_usize, 2, 4, 8] {
            let barrier = Arc::new(tokio::sync::Barrier::new(workers + 1));
            let deadline = Instant::now() + STEP_DURATION;
            let mut tasks = Vec::new();
            for _ in 0..workers {
                let router = Arc::clone(&router);
                let barrier = Arc::clone(&barrier);
                tasks.push(tokio::spawn(async move {
                    barrier.wait().await;
                    let mut requests = 0_u64;
                    let mut failures = 0_u64;
                    let mut latency_ns = Vec::new();
                    loop {
                        let sampled = requests.is_multiple_of(1_024);
                        let sample_started = sampled.then(Instant::now);
                        let request = Request::builder()
                            .method(Method::GET)
                            .uri("/live/77/stream.m3u8")
                            .body(())
                            .unwrap();
                        match router.route(request).await {
                            Ok(response)
                                if response.status == StatusCode::OK
                                    && response
                                        .body
                                        .as_ref()
                                        .is_some_and(|body| body.len() == playlist_bytes) =>
                            {
                                requests += 1;
                            }
                            _ => failures += 1,
                        }
                        if let Some(sample_started) = sample_started {
                            latency_ns.push(
                                sample_started
                                    .elapsed()
                                    .as_nanos()
                                    .min(u128::from(u64::MAX))
                                    as u64,
                            );
                        }
                        if (requests + failures).is_multiple_of(256) && Instant::now() >= deadline {
                            break;
                        }
                    }
                    (requests, failures, latency_ns)
                }));
            }
            barrier.wait().await;
            let started = Instant::now();
            let mut requests = 0_u64;
            let mut failures = 0_u64;
            let mut latency_ns = Vec::new();
            for task in tasks {
                let (worker_requests, worker_failures, worker_latency_ns) = task.await.unwrap();
                requests += worker_requests;
                failures += worker_failures;
                latency_ns.extend(worker_latency_ns);
            }
            let elapsed_seconds = started.elapsed().as_secs_f64();
            latency_ns.sort_unstable();
            let percentile_us = |percentile: usize| {
                let rank = latency_ns.len().saturating_mul(percentile).div_ceil(100);
                latency_ns[rank.clamp(1, latency_ns.len()) - 1] as f64 / 1_000.0
            };
            steps.push(serde_json::json!({
                "workers": workers,
                "duration_seconds": elapsed_seconds,
                "requests": requests,
                "failures": failures,
                "requests_per_second": requests as f64 / elapsed_seconds,
                "logical_body_gbit_per_second":
                    requests as f64 * playlist_bytes as f64 * 8.0 / elapsed_seconds / 1e9,
                "sampled_route_latency_us": {
                    "samples": latency_ns.len(),
                    "p50": percentile_us(50),
                    "p95": percentile_us(95),
                    "p99": percentile_us(99),
                    "max": latency_ns.last().copied().unwrap_or(0) as f64 / 1_000.0,
                }
            }));
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "schema": "needletail.av-mesh.playlist-router-capacity.v1",
                "boundary": "B3_stable_cached_five_ms_playlist_AppRouter_route",
                "part_ms": 5,
                "retained_parts": PARTS,
                "playlist_bytes": playlist_bytes,
                "production_costs_included": [
                    "cached playlist String clone",
                    "request_replica_for_stream",
                    "DemandTracker",
                    "EdgeLoad response telemetry",
                    "path and response construction"
                ],
                "production_costs_excluded": ["HTTP/3", "TLS", "QUIC", "UDP", "network"],
                "steps": steps
            }))
            .unwrap()
        );
        mesh.shutdown();
    }

    #[tokio::test]
    async fn warm_stream_control_requests_mesh_replica() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-warm", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("jp-warm", "jp", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = Arc::new(
            CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
                .start()
                .await
                .unwrap(),
        );

        cache_a
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"warm-stream-part"))
            .await
            .unwrap();

        let router = app_router_for_tests(Arc::clone(&cache_b), Arc::clone(&mesh_b));
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/control/warm-stream")
            .body(())
            .unwrap();
        let body: BodyStream = Box::pin(futures_util::stream::iter(vec![Ok(Bytes::from_static(
            br#"{"stream_id":77,"region":"test-region"}"#,
        ))]));

        let response = router.route_body(req, body).await.unwrap();
        assert_eq!(response.status, StatusCode::ACCEPTED);

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, hash)) = cache_b.chunk_cache.get_for_stream_id(77, 0).await {
                    if hash != 0 || !bytes.is_empty() {
                        break bytes;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"warm-stream-part"));
        let commands = router.control.recent().await;
        assert!(commands
            .iter()
            .any(|command| command.kind == ControlKind::WarmStream));
        assert!(commands
            .iter()
            .any(|command| command.kind == ControlKind::ReplicaRequest));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn replica_request_backfills_missing_retained_stream_slot() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;

        let mut config_a =
            CacheMeshConfig::new("uk-backfill", "uk", mesh_a_addr).with_peer(mesh_b_addr);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b =
            CacheMeshConfig::new("us-backfill", "us", mesh_b_addr).with_peer(mesh_a_addr);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a.chunk_cache), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = CacheMesh::new(Arc::clone(&cache_b.chunk_cache), config_b)
            .start()
            .await
            .unwrap();

        cache_a
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"backfill-part-0"))
            .await
            .unwrap();
        cache_a
            .chunk_cache
            .add_for_stream_id(77, 1, Bytes::from_static(b"backfill-part-1"))
            .await
            .unwrap();
        cache_a
            .chunk_cache
            .add_for_stream_id(77, 2, Bytes::from_static(b"backfill-part-2"))
            .await
            .unwrap();
        cache_b
            .chunk_cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"backfill-part-0"))
            .await
            .unwrap();
        cache_b
            .chunk_cache
            .add_for_stream_id(77, 2, Bytes::from_static(b"backfill-part-2"))
            .await
            .unwrap();

        let from_slot = cache_b.replica_request_from_slot(77).await;
        assert_eq!(from_slot, 1);
        assert_eq!(mesh_b.request_replica(77, from_slot).await.unwrap(), 1);

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, hash)) = cache_b.chunk_cache.get_for_stream_id(77, 1).await {
                    if hash != 0 || !bytes.is_empty() {
                        break bytes;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"backfill-part-1"));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn udp_fec_ingest_writes_cache_parts() {
        use av_mesh::udp_fec::UdpFecSender;
        use tokio::time::timeout;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_fec_ingest(
            socket,
            Arc::clone(&cache),
            shutdown_rx,
            RelayIngestRuntime {
                dispatch: empty_relay_udp_dispatch(),
                secondary_socket: None,
                forwarder: None,
                audio_epochs: None,
                failover_controller: None,
                failover_heartbeat: Duration::from_millis(100),
            },
        ));
        let mut sender = UdpFecSender::new(bind).await.unwrap();

        sender.send(b"fec-part-0").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) = cache.get_part_blocking(0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"fec-part-0"));
    }

    #[test]
    fn relay_forward_endpoint_requires_explicit_bind_and_target() {
        assert_eq!(
            parse_relay_forward_endpoint("127.0.0.1:24001=127.0.0.1:25001,source").unwrap(),
            RelayForwardEndpoint {
                bind: "127.0.0.1:24001".parse().unwrap(),
                target: "127.0.0.1:25001".parse().unwrap(),
                role: RelayForwardRole::Source,
            }
        );
        assert!(parse_relay_forward_endpoint("127.0.0.1:24001").is_err());
        assert!(parse_relay_forward_endpoint("invalid=127.0.0.1:25001").is_err());
    }

    #[test]
    fn relay_failover_endpoints_bind_control_to_an_exact_peer_and_child() {
        assert_eq!(
            parse_relay_failover_listener_endpoint(
                "127.0.0.1:22502=127.0.0.1:22501,127.0.0.1:22004"
            )
            .unwrap(),
            RelayFailoverListenerEndpoint {
                bind: "127.0.0.1:22502".parse().unwrap(),
                peer: "127.0.0.1:22501".parse().unwrap(),
                forward_target: "127.0.0.1:22004".parse().unwrap(),
            }
        );
        assert_eq!(
            parse_relay_failover_controller_endpoint("127.0.0.1:22501=127.0.0.1:22502").unwrap(),
            RelayFailoverControllerEndpoint {
                bind: "127.0.0.1:22501".parse().unwrap(),
                target: "127.0.0.1:22502".parse().unwrap(),
            }
        );
        assert!(parse_relay_failover_listener_endpoint("127.0.0.1:22502=127.0.0.1:22501").is_err());
        assert!(
            parse_relay_failover_listener_endpoint("127.0.0.1:22502=invalid,127.0.0.1:22004")
                .is_err()
        );
        assert!(parse_relay_failover_controller_endpoint("127.0.0.1:22501").is_err());
    }

    #[tokio::test]
    async fn relay_forwarder_preserves_exact_raptorq_wire_datagram_and_role_metrics() {
        use tokio::time::timeout;

        let child = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = RelayForwardEndpoint {
            bind: "127.0.0.1:0".parse().unwrap(),
            target: child.local_addr().unwrap(),
            role: RelayForwardRole::Repair,
        };
        let forwarder = RelayDownstreamForwarder::bind(&[endpoint])
            .await
            .unwrap()
            .unwrap();
        let expected = b"RLS1-exact-canonical-symbol";
        forwarder
            .forward(
                b"RLS1-warm-source-state",
                MediaDatagramRole::Source,
                None,
                None,
            )
            .await;
        assert!(timeout(Duration::from_millis(20), async {
            let mut discarded = [0u8; 64];
            child.recv_from(&mut discarded).await
        })
        .await
        .is_err());
        forwarder
            .forward(expected, MediaDatagramRole::Repair, None, None)
            .await;

        let mut received = [0u8; 64];
        let (len, _) = timeout(Duration::from_secs(1), child.recv_from(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&received[..len], expected);
        let snapshot = forwarder.snapshot();
        assert_eq!(snapshot.downstream_children, 1);
        assert_eq!(snapshot.source_datagrams, 0);
        assert_eq!(snapshot.repair_datagrams, 1);
        assert_eq!(snapshot.bytes, expected.len() as u64);
        assert_eq!(snapshot.errors, 0);
        assert_eq!(snapshot.filtered_datagrams, 1);
        assert_eq!(snapshot.duration_count, 1);
    }

    #[tokio::test]
    async fn relay_fabric_forwards_exact_aep1_source_and_repair_to_the_edge() {
        use raptorq_datagram_fec::{
            AudioPayloadKind, AudioSampleFormat, MultichannelAudioEpoch,
            MultichannelAudioFecConfig, MultichannelAudioFecEncoder, MultichannelAudioGroup,
        };
        use raptorq_fec_transport::MultichannelAudioTransportAdapter;
        use tokio::time::timeout;

        let source_child = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let repair_child = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let forwarder = RelayDownstreamForwarder::bind(&[
            RelayForwardEndpoint {
                bind: "127.0.0.1:0".parse().unwrap(),
                target: source_child.local_addr().unwrap(),
                role: RelayForwardRole::Source,
            },
            RelayForwardEndpoint {
                bind: "127.0.0.1:0".parse().unwrap(),
                target: repair_child.local_addr().unwrap(),
                role: RelayForwardRole::Repair,
            },
        ])
        .await
        .unwrap()
        .unwrap();
        let (audio_tx, mut audio_rx) = broadcast::channel(8);
        let mut block_sessions = HashMap::new();

        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let mut encoder = MultichannelAudioFecEncoder::new(transport.prepare_fec_config(
            MultichannelAudioFecConfig {
                repair_symbols: 2,
                ..MultichannelAudioFecConfig::default()
            },
        ));
        let pcm = vec![9_u8; 2_400];
        let groups = [MultichannelAudioGroup {
            group_id: 3,
            channel_start: 48,
            channel_count: 2,
            payload_kind: AudioPayloadKind::Pcm,
            sample_format: AudioSampleFormat::S24Le,
            flags: 0,
            payload: &pcm,
        }];
        let encoded = encoder
            .encode_epoch(MultichannelAudioEpoch {
                session_id: 444,
                config_generation: 2,
                epoch_id: 8,
                pts_samples: 1_920,
                sample_rate: 48_000,
                frame_count: 240,
                groups: &groups,
            })
            .unwrap();
        let wrapped = transport.wrap_epoch(encoded).unwrap();
        let source = wrapped.source_datagrams().next().unwrap().payload.clone();
        let repair = wrapped.repair_datagrams().next().unwrap().payload.clone();
        let peer: SocketAddr = "127.0.0.1:41001".parse().unwrap();

        assert!(
            process_relay_audio_epoch_datagram(
                peer,
                &source,
                &mut block_sessions,
                Some(&audio_tx),
                Some(&forwarder),
                None,
            )
            .await
        );
        let mut received = vec![0_u8; 1_500];
        let (source_len, _) = timeout(
            Duration::from_secs(1),
            source_child.recv_from(&mut received),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&received[..source_len], source.as_ref());
        let edge_source = audio_rx.recv().await.unwrap();
        assert_eq!(edge_source.session_id, Some(444));
        assert_eq!(edge_source.bytes, source);

        assert!(
            process_relay_audio_epoch_datagram(
                peer,
                &repair,
                &mut block_sessions,
                Some(&audio_tx),
                Some(&forwarder),
                None,
            )
            .await
        );
        let (repair_len, _) = timeout(
            Duration::from_secs(1),
            repair_child.recv_from(&mut received),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&received[..repair_len], repair.as_ref());
        let edge_repair = audio_rx.recv().await.unwrap();
        assert_eq!(edge_repair.session_id, Some(444));
        assert_eq!(edge_repair.bytes, repair);
    }

    #[test]
    fn warm_source_replay_buffer_retains_only_latest_bounded_objects() {
        let mut buffer = WarmSourceReplayBuffer::default();
        let now_us = 1_000_000;
        let mut retired = 0;
        for sequence in 0..=RELAY_WARM_SOURCE_REPLAY_MAX_OBJECTS_PER_CHILD {
            let payload = sequence.to_be_bytes();
            let key = media_object::ObjectKey::for_payload(
                "default",
                "1",
                "muxed-fmp4",
                1,
                0,
                sequence as u64,
                1,
                &payload,
            )
            .unwrap();
            let mutation = buffer.push(&key, now_us + 1_000_000, &payload, now_us);
            retired += mutation.retired_datagrams;
        }

        assert_eq!(
            buffer.object_order.len(),
            RELAY_WARM_SOURCE_REPLAY_MAX_OBJECTS_PER_CHILD
        );
        assert_eq!(
            buffer.datagrams.len(),
            RELAY_WARM_SOURCE_REPLAY_MAX_OBJECTS_PER_CHILD
        );
        assert_eq!(retired, 1);
        let batch = buffer.take_live(now_us);
        assert_eq!(
            batch.datagrams.len(),
            RELAY_WARM_SOURCE_REPLAY_MAX_OBJECTS_PER_CHILD
        );
        assert_eq!(buffer.bytes, 0);
    }

    #[tokio::test]
    async fn leased_failover_promotes_one_warm_child_and_expires_to_repair_only() {
        use tokio::time::timeout;

        let child = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let forward_target = child.local_addr().unwrap();
        let forwarder = RelayDownstreamForwarder::bind(&[RelayForwardEndpoint {
            bind: "127.0.0.1:0".parse().unwrap(),
            target: forward_target,
            role: RelayForwardRole::Repair,
        }])
        .await
        .unwrap()
        .unwrap();
        let controller = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listener_bind = unused_loopback_addr();
        let cache = LiveTsCache::new(1, Duration::from_millis(50), 2, 6, 64).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let generation = TopologyGeneration::new(7).unwrap();
        let subscription = SubscriptionId::new(9).unwrap();
        let tasks = start_relay_failover_listeners(
            &[RelayFailoverListenerEndpoint {
                bind: listener_bind,
                peer: controller.local_addr().unwrap(),
                forward_target,
            }],
            Some(&forwarder),
            generation,
            subscription,
            &cache,
            shutdown_rx,
        )
        .await
        .unwrap();

        let buffered_source = b"source-buffered-before-promotion";
        let buffered_key = media_object::ObjectKey::for_payload(
            "default",
            "1",
            "muxed-fmp4",
            1,
            0,
            1,
            1,
            buffered_source,
        )
        .unwrap();
        forwarder
            .forward(
                buffered_source,
                MediaDatagramRole::Source,
                Some(&buffered_key),
                Some(now_unix_us() + 1_000_000),
            )
            .await;
        assert_eq!(forwarder.snapshot().warm_source_buffered_datagrams, 1);

        let command = FailoverLeaseCommand::new(
            generation,
            subscription,
            11,
            now_unix_us(),
            75_000,
            FailoverForwardMode::SourceAndRepair,
        )
        .unwrap();
        controller
            .send_to(&command.encode(), listener_bind)
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if forwarder.snapshot().failover_promoted_children == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let mut received = [0_u8; 64];
        let (len, _) = timeout(Duration::from_secs(1), child.recv_from(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&received[..len], buffered_source);

        forwarder
            .forward(b"promoted-source", MediaDatagramRole::Source, None, None)
            .await;
        let (len, _) = timeout(Duration::from_secs(1), child.recv_from(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&received[..len], b"promoted-source");

        timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = forwarder.snapshot();
                if snapshot.failover_promoted_children == 0
                    && snapshot.failover_lease_expirations == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let snapshot = forwarder.snapshot();
        assert_eq!(snapshot.failover_commands_received, 1);
        assert_eq!(snapshot.failover_promotions_applied, 1);
        assert_eq!(snapshot.failover_demotions_applied, 1);
        assert_eq!(snapshot.warm_source_buffered_datagrams, 0);
        assert_eq!(snapshot.warm_source_replayed_datagrams, 1);
        assert_eq!(
            snapshot.warm_source_replayed_bytes,
            buffered_source.len() as u64
        );

        let _ = shutdown_tx.send(());
        for task in tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn failover_controller_detects_promotes_and_demotes_make_before_break() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let base = Instant::now();
        let mut controller = RelayFailoverController {
            socket: sender,
            target: receiver.local_addr().unwrap(),
            generation: TopologyGeneration::new(7).unwrap(),
            subscription_id: SubscriptionId::new(9).unwrap(),
            silence: Duration::from_millis(100),
            recovery: Duration::from_millis(40),
            secondary_warm: Duration::from_millis(250),
            lease_duration_us: 1_000_000,
            state: RelayFailoverControllerState::Arming,
            desired_mode: FailoverForwardMode::RepairOnly,
            transition_id: 1,
            last_primary_source: None,
            last_secondary_repair: None,
            recovered_since: None,
            last_decoded: None,
            promotion_gap_base: None,
            promotion_sent_at: None,
            awaiting_secondary_source: false,
            awaiting_post_promotion_object: false,
            snapshot: RelayFailoverControllerSnapshot {
                state: RelayFailoverControllerState::Arming,
                enabled: 1,
                ..RelayFailoverControllerSnapshot::default()
            },
        };
        let source = RelayDatagramObservation {
            role: MediaDatagramRole::Source,
            decoded: true,
        };
        let repair = RelayDatagramObservation {
            role: MediaDatagramRole::Repair,
            decoded: false,
        };
        controller.observe(RelayIngressParentPath::Primary, source, base);
        controller.observe(RelayIngressParentPath::Secondary, repair, base);
        controller.tick(base).await;
        assert_eq!(
            controller.snapshot().state,
            RelayFailoverControllerState::Healthy
        );

        let failed_at = base + Duration::from_millis(101);
        controller.observe(RelayIngressParentPath::Secondary, repair, failed_at);
        controller.tick(failed_at).await;
        assert_eq!(
            controller.snapshot().state,
            RelayFailoverControllerState::Promoted
        );
        assert_eq!(controller.snapshot().promotions, 1);
        assert!(controller.snapshot().last_detection_us >= 100_000);

        let recovery_started = failed_at + Duration::from_millis(10);
        controller.observe(RelayIngressParentPath::Primary, source, recovery_started);
        controller.tick(recovery_started).await;
        assert_eq!(
            controller.snapshot().state,
            RelayFailoverControllerState::Recovering
        );
        let recovered = recovery_started + Duration::from_millis(41);
        controller.observe(RelayIngressParentPath::Primary, source, recovered);
        controller.tick(recovered).await;
        assert_eq!(
            controller.snapshot().state,
            RelayFailoverControllerState::Healthy
        );
        assert_eq!(controller.snapshot().demotions, 1);
    }

    #[tokio::test]
    async fn udp_fec_ingest_commits_one_exact_canonical_object_after_loss_and_reordering() {
        use raptorq_datagram_fec::DatagramFecEncoder;
        use tokio::time::timeout;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_fec_ingest(
            socket,
            Arc::clone(&cache),
            shutdown_rx,
            RelayIngestRuntime {
                dispatch: empty_relay_udp_dispatch(),
                secondary_socket: None,
                forwarder: None,
                audio_epochs: None,
                failover_controller: None,
                failover_heartbeat: Duration::from_millis(100),
            },
        ));
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stream_id = 77u64;
        let payload = (0..6_001)
            .map(|index| ((index * 31 + 17) % 251) as u8)
            .collect::<Vec<_>>();
        let sequence = 42;
        let envelope = encode_test_canonical_fmp4_bundle(
            stream_id,
            sequence,
            Some(b"ftypmoov-loss-test"),
            &payload,
        );
        let mut encoder = DatagramFecEncoder::new().with_symbol_size(1_316);
        let datagrams = encoder
            .encode_object_with_repair_symbols(&envelope, 3)
            .unwrap();
        assert_eq!(encoder.block_id(), 1);
        assert_eq!(datagrams.len(), 8);

        for datagram in datagrams.into_iter().skip(1).rev() {
            let mut framed = Vec::with_capacity(8 + datagram.len());
            framed.extend_from_slice(&stream_id.to_be_bytes());
            framed.extend_from_slice(&datagram);
            sender.send_to(&framed, bind).await.unwrap();
        }

        let bytes = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _hash)) =
                    cache.get_part_for_stream_id(stream_id, sequence).await
                {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes.as_ref(), payload.as_slice());
        assert_eq!(
            cache.get_init_for_stream_id(stream_id).await.unwrap(),
            Bytes::from_static(b"ftypmoov-loss-test")
        );
        // A fresh live subscriber begins its contiguous publication domain at
        // the first canonical media object it observes, so a relay restart can
        // resume current LL-HLS publication without backfilling object zero.
        assert_eq!(
            cache.stream_position_for_id(stream_id).await,
            Some((
                cache.chunk_cache.get_stream_idx(stream_id).await.unwrap(),
                sequence as usize
            ))
        );
        assert!(cache
            .get_part_for_stream_id(stream_id, sequence - 1)
            .await
            .is_none());
        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn relay_session_two_socket_ingest_combines_primary_source_and_secondary_repair() {
        use relay_session::{
            encode_datagram, AdaptiveFecController, AdaptiveFecPolicy, CongestionConfig,
            MediaDeadline, MediaPriority, RaptorQObjectEncoder, RelayLimits, RepairRequest,
            RequestId, SecondaryRepairResponder,
        };
        use tokio::time::timeout;

        let primary_ingest = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let primary_target = primary_ingest.local_addr().unwrap();
        let secondary_ingest = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let secondary_target = secondary_ingest.local_addr().unwrap();
        let primary_sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let secondary_sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let primary_peer = primary_sender.local_addr().unwrap();
        let secondary_peer = secondary_sender.local_addr().unwrap();
        let generation = TopologyGeneration::new(7).unwrap();
        let subscription_id = SubscriptionId::new(19).unwrap();

        let receiver = RelayObjectReceiver::new(RelayObjectReceiverConfig::default()).unwrap();
        let mut dispatch = RelayUdpDispatch::new(receiver);
        for (session_id, peer, peer_id, path) in [
            (1, primary_peer, "contrib-primary", ParentPath::Primary),
            (
                2,
                secondary_peer,
                "contrib-secondary",
                ParentPath::Secondary,
            ),
        ] {
            dispatch
                .bind_controlled_peer(
                    peer,
                    ControlledRelayParentSession::new(
                        session_id,
                        CarrierIdentity {
                            local: NodeId::new("edge-london").unwrap(),
                            peer: NodeId::new(peer_id).unwrap(),
                            kind: CarrierKind::PrivateUdp,
                            trust_mode: TrustMode::ControlledPrivateNetwork,
                        },
                        generation,
                        subscription_id,
                        path,
                    )
                    .unwrap(),
                )
                .unwrap();
        }

        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_fec_ingest(
            primary_ingest,
            Arc::clone(&cache),
            shutdown_rx,
            RelayIngestRuntime {
                dispatch,
                secondary_socket: Some(secondary_ingest),
                forwarder: None,
                audio_epochs: None,
                failover_controller: None,
                failover_heartbeat: Duration::from_millis(100),
            },
        ));

        let stream_id = 77u64;
        let media = (0..12_001)
            .map(|index| ((index * 31 + 17) % 251) as u8)
            .collect::<Vec<_>>();
        let envelope = encode_test_canonical_fmp4_bundle(
            stream_id,
            0,
            Some(b"ftypmoov-relay-session"),
            &media,
        );
        let object = media_object::decode(&envelope).unwrap();
        let policy = AdaptiveFecPolicy {
            min_repair_symbols: 1,
            max_repair_symbols: 1,
            min_repair_ratio: 0.0,
            max_repair_ratio: 0.0,
            symbol_size: 400,
            ..AdaptiveFecPolicy::default()
        };
        let mut encoder = RaptorQObjectEncoder::new(
            AdaptiveFecController::new(policy, CongestionConfig::default()),
            RelayLimits::default(),
        )
        .unwrap();
        let encoded = encoder
            .encode_object(
                &object,
                generation,
                subscription_id,
                MediaDeadline::from_micros(now_unix_us().saturating_add(5_000_000)),
                MediaPriority::VideoKey,
            )
            .unwrap();
        let mut responder = SecondaryRepairResponder::new(
            &object,
            encoded.announcement.clone(),
            RelayLimits::default(),
        )
        .unwrap();
        let repairs = responder
            .fulfill(
                &RepairRequest {
                    request_id: RequestId::new(1).unwrap(),
                    generation,
                    subscription_id,
                    key: object.key().clone(),
                    block_id: encoded.announcement.coding.block_id(),
                    next_repair_ordinal: encoded.announcement.initial_repair_symbols,
                    additional_symbols: 7,
                    deadline: encoded.announcement.deadline,
                },
                now_unix_us(),
            )
            .unwrap();

        for (index, symbol) in encoded.source_symbols.iter().enumerate() {
            if matches!(index, 1 | 5 | 9 | 13 | 17) {
                continue;
            }
            let wire = encode_datagram(symbol, RelayLimits::default()).unwrap();
            primary_sender.send_to(&wire, primary_target).await.unwrap();
        }
        for symbol in repairs {
            let wire = encode_datagram(&symbol, RelayLimits::default()).unwrap();
            secondary_sender
                .send_to(&wire, secondary_target)
                .await
                .unwrap();
        }

        let recovered = timeout(Duration::from_secs(3), async {
            loop {
                if let Some((bytes, _)) = cache.get_part_for_stream_id(stream_id, 0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(recovered.as_ref(), media.as_slice());
        assert_eq!(
            cache.get_init_for_stream_id(stream_id).await.unwrap(),
            Bytes::from_static(b"ftypmoov-relay-session")
        );
        let playlist = cache.playlist_for_stream_id(stream_id).await;
        assert!(playlist.contains("#EXT-X-MAP:URI=\"init.mp4\""));
        assert!(playlist.contains("part0.mp4"));
        let relay = cache.relay_ingress_snapshot();
        assert_eq!(relay.primary_sessions, 1);
        assert_eq!(relay.secondary_sessions, 1);
        assert_eq!(relay.controlled_sessions, 2);
        assert_eq!(relay.decoded_objects, 1);
        assert_eq!(relay.repair_assisted_objects, 1);
        assert_eq!(relay.fec_recovered_objects, 1);
        assert!(relay.fec_recovered_source_symbols > 0);
        assert!(relay.source_datagrams > 0);
        assert!(relay.repair_datagrams > 0);
        assert!(relay.processing_duration_count > 0);
        assert!(relay.processing_duration_sum_us > 0);
        assert!(relay.processing_duration_max_us > 0);
        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn media_udp_fec_ingest_writes_access_unit_stream() {
        use raptorq_datagram_fec::{MediaFecEncoder, MediaFrame};
        use tokio::time::timeout;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_media_fec_ingest(
            socket,
            Arc::clone(&cache),
            broadcast::channel(AUDIO_EPOCH_BROADCAST_CAPACITY).0,
            shutdown_rx,
        ));

        let mut metadata = MediaFrameMetadata::new(66, 3, 777, MediaCodec::H264);
        metadata.duration_ms = 33;
        metadata.flags = MediaFrameFlags::keyframe();
        let mut encoder = MediaFecEncoder::default();
        let encoded = encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: b"fec-h264-access-unit",
            })
            .unwrap();

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for datagram in encoded.datagrams {
            sender.send_to(&datagram, bind).await.unwrap();
        }

        let unit = timeout(Duration::from_secs(3), async {
            loop {
                if let Some(unit) = cache.get_media_access_unit(66, 3).await {
                    break unit;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(unit.metadata.codec, MediaCodec::H264);
        assert!(unit.metadata.flags.is_keyframe());
        assert_eq!(unit.metadata.pts_ms, 777);
        assert_eq!(unit.metadata.duration_ms, 33);
        assert_eq!(
            unit.serialized.slice(MEDIA_FRAME_HEADER_LEN..),
            Bytes::from_static(b"fec-h264-access-unit")
        );

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn media_udp_fec_ingest_broadcasts_audio_epoch_datagrams() {
        use raptorq_datagram_fec::{
            AudioPayloadKind, AudioSampleFormat, MultichannelAudioEpoch,
            MultichannelAudioFecConfig, MultichannelAudioFecEncoder, MultichannelAudioGroup,
        };
        use raptorq_fec_transport::MultichannelAudioTransportAdapter;
        use tokio::time::timeout;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let (audio_epoch_tx, mut audio_epoch_rx) = broadcast::channel(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_media_fec_ingest(
            socket,
            Arc::clone(&cache),
            audio_epoch_tx,
            shutdown_rx,
        ));

        let subscriber = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        subscriber
            .send_to(b"WAVEY-DAW-SUBSCRIBE/2 55", bind)
            .await
            .unwrap();
        let mut subscriber_buf = vec![0_u8; 1_500];
        let (ack_len, ack_peer) = timeout(
            Duration::from_secs(1),
            subscriber.recv_from(&mut subscriber_buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(ack_peer, bind);
        assert_eq!(&subscriber_buf[..ack_len], b"WAVEY-DAW-SUBSCRIBED/2 55");

        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let mut encoder = MultichannelAudioFecEncoder::new(
            transport.prepare_fec_config(MultichannelAudioFecConfig::default()),
        );
        let pcm = vec![5_u8; 240 * 2 * 2];
        let groups = [MultichannelAudioGroup {
            group_id: 0,
            channel_start: 0,
            channel_count: 2,
            payload_kind: AudioPayloadKind::Pcm,
            sample_format: AudioSampleFormat::S16Le,
            flags: 0,
            payload: &pcm,
        }];
        let encoded = encoder
            .encode_epoch(MultichannelAudioEpoch {
                session_id: 55,
                config_generation: 1,
                epoch_id: 0,
                pts_samples: 0,
                sample_rate: 48_000,
                frame_count: 240,
                groups: &groups,
            })
            .unwrap();
        let epoch_datagram = transport.wrap_epoch(encoded).unwrap().datagrams[0]
            .payload
            .clone();
        assert!(is_multichannel_audio_transport_datagram(&epoch_datagram));
        sender.send_to(&epoch_datagram, bind).await.unwrap();

        let (relay_len, relay_peer) = timeout(
            Duration::from_secs(1),
            subscriber.recv_from(&mut subscriber_buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(relay_peer, bind);
        assert_eq!(&subscriber_buf[..relay_len], epoch_datagram.as_ref());

        let received = timeout(Duration::from_secs(3), audio_epoch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.session_id, Some(55));
        assert_eq!(received.bytes, epoch_datagram);
        assert!(cache.get_media_access_unit(1, 0).await.is_none());

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn media_udp_fec_ingest_recovers_video_access_units_with_lost_datagrams() {
        use raptorq_datagram_fec::{MediaFecEncoder, MediaFrame, NetworkMetrics};
        use tokio::time::timeout;

        struct LossyFrame {
            sequence: u64,
            pts_ms: u64,
            payload_len: usize,
            flags: MediaFrameFlags,
            dropped_indexes: &'static [usize],
        }

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 64).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_media_fec_ingest(
            socket,
            Arc::clone(&cache),
            broadcast::channel(AUDIO_EPOCH_BROADCAST_CAPACITY).0,
            shutdown_rx,
        ));

        let mut encoder = MediaFecEncoder::default();
        encoder
            .controller_mut()
            .update_network_metrics(NetworkMetrics {
                loss_fraction: 0.08,
                rtt_ms: 70.0,
                jitter_ms: 25.0,
                queue_delay_ms: 20.0,
                available_bitrate_bps: Some(8_000_000),
            });
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let frames = [
            LossyFrame {
                sequence: 10,
                pts_ms: 1_000,
                payload_len: 40_000,
                flags: MediaFrameFlags::keyframe(),
                dropped_indexes: &[3, 4, 5, 6, 7, 8, 9, 10],
            },
            LossyFrame {
                sequence: 11,
                pts_ms: 1_016,
                payload_len: 18_000,
                flags: MediaFrameFlags::default(),
                dropped_indexes: &[2, 8, 12],
            },
            LossyFrame {
                sequence: 12,
                pts_ms: 1_032,
                payload_len: 18_000,
                flags: MediaFrameFlags::default(),
                dropped_indexes: &[1, 5],
            },
        ];

        for frame in &frames {
            let payload = deterministic_video_payload(frame.payload_len);
            let mut metadata =
                MediaFrameMetadata::new(66, frame.sequence, frame.pts_ms, MediaCodec::H264);
            metadata.duration_ms = 16;
            metadata.flags = frame.flags;
            let encoded = encoder
                .encode_frame(MediaFrame {
                    metadata,
                    payload: &payload,
                })
                .unwrap();
            assert!(
                encoded.decision.config.repair_symbols as usize >= frame.dropped_indexes.len(),
                "test loss must stay inside the repair budget for sequence {}",
                frame.sequence
            );

            for (index, datagram) in encoded.datagrams.iter().enumerate() {
                if frame.dropped_indexes.contains(&index) {
                    continue;
                }
                sender.send_to(datagram, bind).await.unwrap();
            }

            let unit = timeout(Duration::from_secs(3), async {
                loop {
                    if let Some(unit) = cache.get_media_access_unit(66, frame.sequence).await {
                        break unit;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();

            assert_eq!(unit.metadata.codec, MediaCodec::H264);
            assert_eq!(unit.metadata.sequence, frame.sequence);
            assert_eq!(unit.metadata.pts_ms, frame.pts_ms);
            assert_eq!(unit.metadata.duration_ms, 16);
            assert_eq!(
                &unit.serialized[MEDIA_FRAME_HEADER_LEN..],
                payload.as_slice()
            );
        }

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn media_udp_fec_ingest_recovers_multiblock_video_stream_with_reordered_loss() {
        use raptorq_datagram_fec::{
            DatagramFecHeader, EncodedMediaFrame, MediaFecEncoder, MediaFrame, NetworkMetrics,
        };
        use tokio::time::timeout;

        #[derive(Debug)]
        struct StreamFrame {
            sequence: u64,
            pts_ms: u64,
            payload_len: usize,
            flags: MediaFrameFlags,
            max_loss_per_block: usize,
        }

        #[derive(Debug)]
        struct ScheduledDatagram {
            ordinal: usize,
            delay_ms: u64,
            bytes: Vec<u8>,
        }

        fn bounded_source_loss(
            encoded: &EncodedMediaFrame,
            max_loss_per_block: usize,
        ) -> HashSet<usize> {
            let mut blocks = BTreeMap::<u32, (u16, Vec<usize>, usize)>::new();
            for (datagram_index, datagram) in encoded.datagrams.iter().enumerate() {
                let header =
                    DatagramFecHeader::decode(datagram).expect("decode FEC datagram header");
                let entry =
                    blocks
                        .entry(header.block_id)
                        .or_insert((header.source_symbols, Vec::new(), 0));
                assert_eq!(entry.0, header.source_symbols);
                if entry.2 < usize::from(header.source_symbols) {
                    entry.1.push(datagram_index);
                }
                entry.2 += 1;
            }

            let mut dropped = HashSet::new();
            assert_eq!(blocks.len(), usize::from(encoded.fragment_count));
            for (_block_id, (source_symbols, source_indices, datagram_count)) in blocks {
                let source_symbols = usize::from(source_symbols);
                let repair_symbols = datagram_count.saturating_sub(source_symbols);
                let drop_count = repair_symbols.min(max_loss_per_block);
                assert!(drop_count > 0, "test frame should have repair symbols");
                dropped.extend(source_indices.into_iter().take(drop_count));
            }
            dropped
        }

        fn reorder_delay_ms(ordinal: usize) -> u64 {
            match ordinal % 11 {
                0 => 8,
                3 => 5,
                7 => 2,
                _ => 0,
            }
        }

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bind = socket.local_addr().unwrap();
        let cache = LiveTsCache::new(1, Duration::from_millis(500), 2, 6, 256).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_udp_media_fec_ingest(
            socket,
            Arc::clone(&cache),
            broadcast::channel(AUDIO_EPOCH_BROADCAST_CAPACITY).0,
            shutdown_rx,
        ));

        let frames = [
            StreamFrame {
                sequence: 100,
                pts_ms: 2_000,
                payload_len: 96_000,
                flags: MediaFrameFlags::keyframe(),
                max_loss_per_block: 4,
            },
            StreamFrame {
                sequence: 101,
                pts_ms: 2_016,
                payload_len: 18_000,
                flags: MediaFrameFlags::default(),
                max_loss_per_block: 2,
            },
            StreamFrame {
                sequence: 102,
                pts_ms: 2_032,
                payload_len: 40_000,
                flags: MediaFrameFlags::keyframe(),
                max_loss_per_block: 3,
            },
            StreamFrame {
                sequence: 103,
                pts_ms: 2_048,
                payload_len: 9_000,
                flags: MediaFrameFlags::default(),
                max_loss_per_block: 1,
            },
        ];

        let mut encoder = MediaFecEncoder::default();
        encoder
            .controller_mut()
            .update_network_metrics(NetworkMetrics {
                loss_fraction: 0.08,
                rtt_ms: 70.0,
                jitter_ms: 25.0,
                queue_delay_ms: 20.0,
                available_bitrate_bps: Some(8_000_000),
            });
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut expected_payloads = BTreeMap::new();
        let mut scheduled = Vec::new();
        let mut ordinal = 0usize;
        let mut dropped_datagrams = 0usize;

        for frame in &frames {
            let payload = deterministic_video_payload(frame.payload_len);
            let mut metadata =
                MediaFrameMetadata::new(66, frame.sequence, frame.pts_ms, MediaCodec::H264);
            metadata.duration_ms = 16;
            metadata.flags = frame.flags;
            let encoded = encoder
                .encode_frame(MediaFrame {
                    metadata,
                    payload: &payload,
                })
                .unwrap();
            assert!(
                encoded.fragment_count > 1 || frame.payload_len < 64_000,
                "large access units should exercise multi-block frame reconstruction"
            );
            let dropped = bounded_source_loss(&encoded, frame.max_loss_per_block);
            dropped_datagrams += dropped.len();

            for (index, datagram) in encoded.datagrams.into_iter().enumerate() {
                if dropped.contains(&index) {
                    continue;
                }
                scheduled.push(ScheduledDatagram {
                    ordinal,
                    delay_ms: reorder_delay_ms(ordinal),
                    bytes: datagram,
                });
                ordinal += 1;
            }
            expected_payloads.insert(frame.sequence, payload);
        }

        assert!(
            dropped_datagrams >= 8,
            "test should exercise repeated datagram loss"
        );
        scheduled.sort_by_key(|datagram| (datagram.delay_ms, datagram.ordinal));
        for datagram in scheduled {
            if datagram.delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(datagram.delay_ms)).await;
            }
            sender.send_to(&datagram.bytes, bind).await.unwrap();
        }

        for frame in &frames {
            let unit = timeout(Duration::from_secs(3), async {
                loop {
                    if let Some(unit) = cache.get_media_access_unit(66, frame.sequence).await {
                        break unit;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let expected_payload = expected_payloads
                .get(&frame.sequence)
                .expect("expected payload for frame");

            assert_eq!(unit.metadata.codec, MediaCodec::H264);
            assert_eq!(unit.metadata.sequence, frame.sequence);
            assert_eq!(unit.metadata.pts_ms, frame.pts_ms);
            assert_eq!(unit.metadata.duration_ms, 16);
            assert_eq!(
                &unit.serialized[MEDIA_FRAME_HEADER_LEN..],
                expected_payload.as_slice()
            );
        }

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn raptorq_mesh_replicates_opaque_stream_slots() {
        use playlists::mesh::{CacheMesh, CacheMeshConfig};
        use tokio::time::timeout;

        let mesh_a_addr = unused_loopback_addr();
        let mesh_b_addr = unused_loopback_addr();
        let cache_a = LiveTsCache::new(7, Duration::from_millis(500), 2, 6, 64).await;
        let cache_b = LiveTsCache::new(7, Duration::from_millis(500), 2, 6, 64).await;
        let mesh_a = CacheMesh::new(
            Arc::clone(&cache_a.chunk_cache),
            CacheMeshConfig::new("uk-opaque", "uk", mesh_a_addr).with_peer(mesh_b_addr),
        )
        .start()
        .await
        .unwrap();
        let mesh_b = CacheMesh::new(
            Arc::clone(&cache_b.chunk_cache),
            CacheMeshConfig::new("us-opaque", "us", mesh_b_addr).with_peer(mesh_a_addr),
        )
        .start()
        .await
        .unwrap();

        cache_a
            .chunk_cache
            .add_for_stream_id(7, 0, Bytes::from_static(b"raptorq-mesh-bytes-0"))
            .await
            .unwrap();

        let bytes = timeout(Duration::from_secs(5), async {
            loop {
                if let Some((bytes, _hash)) = cache_b.get_part_blocking(0).await {
                    break bytes;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"raptorq-mesh-bytes-0"));
        assert!(cache_b.playlist().await.contains("part0.ts"));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    async fn mesh_handle_for_tests(cache: Arc<ChunkCache>) -> Arc<CacheMeshHandle> {
        let mesh = CacheMesh::new(
            cache,
            CacheMeshConfig::new("test-node", "test", unused_loopback_addr()),
        )
        .start()
        .await
        .unwrap();
        Arc::new(mesh)
    }

    fn app_router_for_tests(cache: Arc<LiveTsCache>, mesh: Arc<CacheMeshHandle>) -> AppRouter {
        app_router_for_tests_with_telemetry(cache, mesh, TelemetryAggregator::default())
    }

    fn app_router_for_tests_with_telemetry(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        telemetry: TelemetryAggregator,
    ) -> AppRouter {
        app_router_for_tests_with_policy_and_telemetry(
            cache,
            mesh,
            ReplicationPolicy::default(),
            telemetry,
        )
    }

    fn app_router_for_tests_with_policy_and_telemetry(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        replication_policy: ReplicationPolicy,
        telemetry: TelemetryAggregator,
    ) -> AppRouter {
        app_router_for_tests_with_policy_telemetry_and_provision(
            cache,
            mesh,
            replication_policy,
            telemetry,
            ProvisionExecutor::disabled(),
        )
    }

    fn app_router_for_tests_with_provision(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        provision: ProvisionExecutor,
    ) -> AppRouter {
        app_router_for_tests_with_policy_telemetry_and_provision(
            cache,
            mesh,
            ReplicationPolicy::default(),
            TelemetryAggregator::default(),
            provision,
        )
    }

    fn app_router_for_tests_with_node(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        node: MeshNode,
    ) -> AppRouter {
        app_router_for_tests_with_node_policy_telemetry_and_provision(
            cache,
            mesh,
            node,
            ReplicationPolicy::default(),
            TelemetryAggregator::default(),
            ProvisionExecutor::disabled(),
            TelemetryPeerMonitor::default(),
        )
    }

    fn app_router_for_tests_with_policy_telemetry_and_provision(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        replication_policy: ReplicationPolicy,
        telemetry: TelemetryAggregator,
        provision: ProvisionExecutor,
    ) -> AppRouter {
        app_router_for_tests_with_policy_telemetry_provision_and_monitor(
            cache,
            mesh,
            replication_policy,
            telemetry,
            provision,
            TelemetryPeerMonitor::default(),
        )
    }

    fn app_router_for_tests_with_telemetry_monitor(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        monitor: TelemetryPeerMonitor,
    ) -> AppRouter {
        app_router_for_tests_with_policy_telemetry_provision_and_monitor(
            cache,
            mesh,
            ReplicationPolicy::default(),
            TelemetryAggregator::default(),
            ProvisionExecutor::disabled(),
            monitor,
        )
    }

    fn app_router_for_tests_with_policy_telemetry_provision_and_monitor(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        replication_policy: ReplicationPolicy,
        telemetry: TelemetryAggregator,
        provision: ProvisionExecutor,
        monitor: TelemetryPeerMonitor,
    ) -> AppRouter {
        app_router_for_tests_with_node_policy_telemetry_and_provision(
            cache,
            mesh,
            mesh_node_for_tests("test-node", "test-region", "test-continent", 51.5, -0.1),
            replication_policy,
            telemetry,
            provision,
            monitor,
        )
    }

    fn app_router_for_tests_with_node_policy_telemetry_and_provision(
        cache: Arc<LiveTsCache>,
        mesh: Arc<CacheMeshHandle>,
        node: MeshNode,
        replication_policy: ReplicationPolicy,
        telemetry: TelemetryAggregator,
        provision: ProvisionExecutor,
        monitor: TelemetryPeerMonitor,
    ) -> AppRouter {
        AppRouter::new(
            cache,
            mesh,
            broadcast::channel(AUDIO_EPOCH_BROADCAST_CAPACITY).0,
            MeshTransportConfigSnapshot::default(),
            node,
            replication_policy,
            ControlPlane::default(),
            ControlDispatch::default(),
            telemetry,
            DemandTracker::default(),
            NodeLifecycle::default(),
            Some("https://test-node.local/live".into()),
            EdgeLoad::default(),
            provision,
            monitor,
            PrivateDiscoveryStatus::default(),
        )
    }

    fn mesh_node_for_tests(
        node_id: &str,
        region: &str,
        continent: &str,
        latitude: f64,
        longitude: f64,
    ) -> MeshNode {
        MeshNode {
            node_id: node_id.into(),
            region: region.into(),
            continent: continent.into(),
            latitude,
            longitude,
            total_storage_bytes: 1_000_000,
            used_storage_bytes: 0,
            egress_capacity_bps: 10_000_000_000,
            contributor_streams: 0,
            active_streams: 0,
            draining: false,
        }
    }

    fn telemetry_snapshot_for_tests(
        node_id: &str,
        region: &str,
        continent: &str,
        latitude: f64,
        longitude: f64,
        peers: Vec<PeerSnapshot>,
        stream_id: u64,
    ) -> MeshSnapshot {
        MeshSnapshot {
            updated_unix_ms: now_unix_ms(),
            node: MeshNode {
                node_id: node_id.into(),
                region: region.into(),
                continent: continent.into(),
                latitude,
                longitude,
                total_storage_bytes: 1_000_000,
                used_storage_bytes: 100_000,
                egress_capacity_bps: 10_000_000_000,
                contributor_streams: 1,
                active_streams: 1,
                draining: false,
            },
            mesh_addr: Some(node_id.into()),
            edge_service: None,
            relay_session: RelaySessionIngressSnapshot::default(),
            peers,
            stream: StatsSnapshot {
                stream_id,
                stream_id_text: stream_id_text(stream_id),
                part_target_ms: 500,
                parts_per_segment: 2,
                window_parts: 6,
                datagrams_received: 10,
                bytes_received: 20_000,
                current_part_bytes: 0,
                latest_local_part: Some(1),
                latest_local_part_bytes: Some(1024),
                latest_local_part_duration_ms: Some(500),
                latest_mesh_part: Some(1),
                canonical_epoch: Some(1),
                canonical_epoch_activation_delay_us: Some(250_000),
                contiguous_object: Some(1),
                head_object: Some(1),
                gap_count: Some(0),
                mesh_peers: Vec::new(),
                latest_local_part_age_ms: Some(10),
                last_ingest_age_ms: Some(10),
            },
            streams: Vec::new(),
            replication_policy: ReplicationPolicy::default(),
            recent_commands: Vec::new(),
        }
    }

    fn unused_loopback_addr() -> SocketAddr {
        static NEXT_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(24_000);

        loop {
            let port = NEXT_PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
            if std::net::UdpSocket::bind(addr).is_ok() {
                return addr;
            }
        }
    }

    fn unused_tcp_loopback_addr() -> SocketAddr {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.local_addr().unwrap()
    }

    fn tls_pair_for_tests() -> (String, String) {
        use base64::engine::general_purpose::STANDARD as base64_engine;
        use base64::Engine;

        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["local.wavey.ai".into()]).unwrap();
        (
            base64_engine.encode(cert.pem()),
            base64_engine.encode(key_pair.serialize_pem()),
        )
    }
}
