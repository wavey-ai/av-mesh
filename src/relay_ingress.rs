//! Bounded `RelaySession` receive state for the Needletail media fabric.
//!
//! The adapter combines compatible `RaptorQ` symbols by canonical
//! [`ObjectKey`], independent of the authenticated parent that delivered each
//! symbol. A reliable object announcement and an authenticated carrier-session
//! identity must be registered before live datagrams are admitted.

use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error as StdError;
use std::fmt;
use std::net::SocketAddr;

use media_object::{MediaObject, ObjectKey, ObjectKind, PayloadHash, Stage};
use playlists::compact_object::{self, CompactTemplates};
use raptorq_datagram_fec::{
    DatagramFecHeader, MediaDatagramRole, MediaDeadline, MediaObjectKind, MediaPathIntent,
};
use relay_session::{
    decode_datagram, CarrierIdentity, CarrierKind, ObjectAnnouncement, ObjectAssembler, ParentPath,
    RelayLimits, SubscriptionId, TopologyGeneration, TrustMode,
};
use serde::{Deserialize, Serialize};

use crate::media_clock::{MediaClocks, Verdict};

/// Wire prefix used by `RelaySession`'s RLS1 envelope.
pub const RELAY_SESSION_DATAGRAM_MAGIC: [u8; 4] = *b"RLS1";

const HARD_MAX_PARENT_SESSIONS: usize = 65_536;
const HARD_MAX_ACTIVE_OBJECTS: usize = 65_536;
/// Primary, secondary and the ranked parents of an edge, with room: every
/// promoted session may bring symbols of one object.
const HARD_MAX_PARENTS_PER_OBJECT: usize = 8;
const HARD_MAX_DATAGRAMS_PER_OBJECT: usize = 65_536;
const HARD_MAX_BUFFERED_DATAGRAMS: usize = 262_144;
const HARD_MAX_BUFFERED_OBJECT_BYTES: usize = 512 * 1024 * 1024;
const HARD_MAX_COMPLETED_OBJECTS: usize = 65_536;
/// Streams whose media clock a receiver keeps.
const MEDIA_CLOCK_STREAMS: usize = 4_096;

/// Operational receive ceilings. Every value is checked before allocation or
/// decoder admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayObjectReceiverConfig {
    pub relay_limits: RelayLimits,
    pub max_parent_sessions: usize,
    pub max_active_objects: usize,
    pub max_parents_per_object: usize,
    pub max_datagrams_per_object: usize,
    pub max_buffered_datagrams: usize,
    pub max_buffered_object_bytes: usize,
    pub max_completed_objects: usize,
    pub object_inactivity_timeout_us: u64,
    pub expiry_scan_interval_us: u64,
    /// `--relay-media-deadline-ms`: how far behind the head of its stream,
    /// in the stream's own time, a part may be and still be taken.
    pub media_deadline_us: u64,
}

impl Default for RelayObjectReceiverConfig {
    fn default() -> Self {
        Self {
            relay_limits: RelayLimits::default(),
            max_parent_sessions: 4_096,
            max_active_objects: 4_096,
            max_parents_per_object: 4,
            max_datagrams_per_object: 32_768,
            max_buffered_datagrams: 131_072,
            max_buffered_object_bytes: 128 * 1024 * 1024,
            max_completed_objects: 4_096,
            object_inactivity_timeout_us: 10_000_000,
            expiry_scan_interval_us: 250_000,
            media_deadline_us: 1_000_000,
        }
    }
}

impl RelayObjectReceiverConfig {
    fn validate(self) -> Result<(), RelayIngressError> {
        self.relay_limits
            .validate()
            .map_err(RelayIngressError::RelaySession)?;
        validate_nonzero_bound(
            "max_parent_sessions",
            self.max_parent_sessions,
            HARD_MAX_PARENT_SESSIONS,
        )?;
        validate_nonzero_bound(
            "max_active_objects",
            self.max_active_objects,
            HARD_MAX_ACTIVE_OBJECTS,
        )?;
        validate_nonzero_bound(
            "max_parents_per_object",
            self.max_parents_per_object,
            HARD_MAX_PARENTS_PER_OBJECT,
        )?;
        validate_nonzero_bound(
            "max_datagrams_per_object",
            self.max_datagrams_per_object,
            HARD_MAX_DATAGRAMS_PER_OBJECT,
        )?;
        validate_nonzero_bound(
            "max_buffered_datagrams",
            self.max_buffered_datagrams,
            HARD_MAX_BUFFERED_DATAGRAMS,
        )?;
        validate_nonzero_bound(
            "max_buffered_object_bytes",
            self.max_buffered_object_bytes,
            HARD_MAX_BUFFERED_OBJECT_BYTES,
        )?;
        validate_nonzero_bound(
            "max_completed_objects",
            self.max_completed_objects,
            HARD_MAX_COMPLETED_OBJECTS,
        )?;
        if self.object_inactivity_timeout_us == 0 {
            return Err(RelayIngressError::InvalidConfig {
                field: "object_inactivity_timeout_us",
                reason: "bound must be positive",
            });
        }
        if self.expiry_scan_interval_us == 0 {
            return Err(RelayIngressError::InvalidConfig {
                field: "expiry_scan_interval_us",
                reason: "bound must be positive",
            });
        }
        if self.media_deadline_us == 0 {
            return Err(RelayIngressError::InvalidConfig {
                field: "media_deadline_us",
                reason: "bound must be positive",
            });
        }
        Ok(())
    }
}

fn validate_nonzero_bound(
    field: &'static str,
    value: usize,
    maximum: usize,
) -> Result<(), RelayIngressError> {
    if value == 0 {
        return Err(RelayIngressError::InvalidConfig {
            field,
            reason: "bound must be positive",
        });
    }
    if value > maximum {
        return Err(RelayIngressError::LimitExceeded {
            field,
            actual: value,
            maximum,
        });
    }
    Ok(())
}

/// Identity issued by the caller after the carrier session has authenticated
/// its peer. `session_id` is a node-local, non-zero handle and is never accepted
/// from the media datagram itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedRelayParentSession {
    session_id: u64,
    identity: CarrierIdentity,
    generation: TopologyGeneration,
    subscription_id: SubscriptionId,
    parent_path: ParentPath,
    controlled_qualification: bool,
}

impl AuthenticatedRelayParentSession {
    /// Bind an authenticated QUIC or authenticated private-underlay carrier to
    /// one topology generation, subscription, and assigned parent role.
    pub fn new(
        session_id: u64,
        identity: CarrierIdentity,
        generation: TopologyGeneration,
        subscription_id: SubscriptionId,
        parent_path: ParentPath,
    ) -> Result<Self, RelayIngressError> {
        if session_id == 0 {
            return Err(RelayIngressError::InvalidSessionId);
        }
        if identity.local == identity.peer {
            return Err(RelayIngressError::InvalidCarrierIdentity(
                "local and peer node identities must differ",
            ));
        }
        if !matches!(
            (identity.kind, identity.trust_mode),
            (CarrierKind::QuicDatagram, TrustMode::AuthenticatedSession)
                | (
                    CarrierKind::PrivateUdp,
                    TrustMode::PrivateAuthenticatedNetwork
                )
        ) {
            return Err(RelayIngressError::AuthenticationRequired);
        }
        Ok(Self {
            session_id,
            identity,
            generation,
            subscription_id,
            parent_path,
            controlled_qualification: false,
        })
    }

    #[must_use]
    pub const fn session_id(&self) -> u64 {
        self.session_id
    }

    #[must_use]
    pub const fn identity(&self) -> &CarrierIdentity {
        &self.identity
    }

    #[must_use]
    pub const fn generation(&self) -> TopologyGeneration {
        self.generation
    }

    #[must_use]
    pub const fn subscription_id(&self) -> SubscriptionId {
        self.subscription_id
    }

    #[must_use]
    pub const fn parent_path(&self) -> ParentPath {
        self.parent_path
    }
}

/// Explicit socket-address binding for deterministic loopback or controlled
/// private-network qualification. This profile makes no authentication or
/// encryption claim and enables local first-symbol announcement derivation only
/// when registered through [`RelayUdpDispatch::bind_controlled_peer`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlledRelayParentSession(AuthenticatedRelayParentSession);

impl ControlledRelayParentSession {
    pub fn new(
        session_id: u64,
        identity: CarrierIdentity,
        generation: TopologyGeneration,
        subscription_id: SubscriptionId,
        parent_path: ParentPath,
    ) -> Result<Self, RelayIngressError> {
        if session_id == 0 {
            return Err(RelayIngressError::InvalidSessionId);
        }
        if identity.local == identity.peer {
            return Err(RelayIngressError::InvalidCarrierIdentity(
                "local and peer node identities must differ",
            ));
        }
        if !matches!(
            (identity.kind, identity.trust_mode),
            (CarrierKind::PrivateUdp, TrustMode::ControlledPrivateNetwork)
        ) {
            return Err(RelayIngressError::InvalidCarrierIdentity(
                "controlled qualification uses private UDP with ControlledPrivateNetwork trust",
            ));
        }
        Ok(Self(AuthenticatedRelayParentSession {
            session_id,
            identity,
            generation,
            subscription_id,
            parent_path,
            controlled_qualification: true,
        }))
    }

    #[must_use]
    pub const fn session_id(&self) -> u64 {
        self.0.session_id
    }
}

/// Result of adding a reliable object announcement to the receive table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelayAnnouncementOutcome {
    Started,
    Joined,
    AlreadyComplete,
}

/// Result of receiving one authenticated RLS1/RQD2 datagram.
#[derive(Debug)]
pub enum RelayIngressOutcome {
    Buffered {
        key: ObjectKey,
        role: MediaDatagramRole,
        deadline: MediaDeadline,
    },
    Decoded {
        object: Box<MediaObject>,
        /// Exact bounded canonical envelope reconstructed and validated by the
        /// object assembler. Callers may commit these bytes directly instead
        /// of encoding and decoding the validated object again.
        envelope: Vec<u8>,
        /// Role of the symbol that completed the object. Relay executables use
        /// this to preserve source/repair observability while forwarding the
        /// admitted wire datagram without changing its coding geometry.
        role: MediaDatagramRole,
        deadline: MediaDeadline,
        parent_count: usize,
        accepted_datagrams: usize,
    },
    Duplicate {
        key: ObjectKey,
        /// The authenticated symbol remains useful to a downstream child even
        /// when this relay has already completed the object. Preserving the
        /// role lets a warm relay forward late repair without reopening local
        /// decoder state.
        role: MediaDatagramRole,
        deadline: MediaDeadline,
    },
}

/// Snapshot of bounded receiver ownership.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RelayObjectReceiverState {
    pub parent_sessions: usize,
    pub active_objects: usize,
    pub completed_objects: usize,
    pub buffered_object_bytes: usize,
    pub buffered_datagrams: usize,
}

/// Low-cardinality cumulative outcomes for one relay ingress. Canonical object
/// identifiers and parent identities stay in traces rather than metric labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RelayIngressCounters {
    pub datagrams_received: u64,
    pub datagrams_rejected: u64,
    pub source_datagrams: u64,
    pub repair_datagrams: u64,
    pub duplicate_datagrams: u64,
    pub decoded_objects: u64,
    /// Decoded objects that had at least one repair symbol admitted. This is
    /// useful for scheduler attribution, but does not prove source loss.
    pub repair_assisted_objects: u64,
    /// Decoded objects for which RaptorQ replaced one or more missing source
    /// symbols before the object deadline.
    pub fec_recovered_objects: u64,
    /// Exact number of missing source symbols reconstructed across recovered
    /// objects.
    pub fec_recovered_source_symbols: u64,
    pub expired_objects: u64,
    pub conflict_drops: u64,
    pub authentication_drops: u64,
    pub deadline_drops: u64,
    /// Objects rebuilt from a compact (RLC1) datagram (wave 7 item 3).
    pub compact_decoded_objects: u64,
    /// Compact datagrams of a descriptor not learnt yet: admitted and
    /// forwarded, not decoded here.
    pub compact_unknown_descriptor: u64,
    /// Whole audio objects whose descriptor was learnt (or seen again).
    pub compact_descriptors_learned: u64,
    /// Media objects decoded more than the media budget after their
    /// Published time by this node's wall clock, and the longest any decoded
    /// object was behind its Published time (µs). Nothing acts on either:
    /// they say what a wall-clock deadline would have shed.
    pub wall_late_parts: u64,
    pub wall_late_max_us: u64,
    /// Every rejection, by the kind of its error.
    pub rejected_by_kind: RejectedByKind,
}

/// The kinds of rejection and their counters, from one list: the name of a
/// kind in a log line is the name of its counter in the status JSON.
macro_rules! rejection_kinds {
    ($($variant:ident => $field:ident),+ $(,)?) => {
        /// What kind of error rejected a datagram or an announcement.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum RejectionKind {
            $($variant),+
        }

        impl RejectionKind {
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => stringify!($field)),+
                }
            }
        }

        /// Rejections counted by [`RejectionKind`].
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(default)]
        pub struct RejectedByKind {
            $(pub $field: u64),+
        }

        impl RejectedByKind {
            #[must_use]
            pub const fn get(&self, kind: RejectionKind) -> u64 {
                match kind {
                    $(RejectionKind::$variant => self.$field),+
                }
            }

            fn count(&mut self, kind: RejectionKind) {
                let counter = match kind {
                    $(RejectionKind::$variant => &mut self.$field),+
                };
                *counter = counter.saturating_add(1);
            }

            /// Add another receiver's counts (the audio and the video tee
            /// each have a receiver).
            pub fn add(&mut self, other: &Self) {
                $(self.$field = self.$field.saturating_add(other.$field);)+
            }

            /// Each kind's name and count, for the metrics.
            pub fn named(&self) -> impl Iterator<Item = (&'static str, u64)> {
                [$((stringify!($field), self.$field)),+].into_iter()
            }
        }
    };
}

rejection_kinds! {
    Invalid => invalid,
    LimitExceeded => limit_exceeded,
    Authentication => authentication,
    ParentSessionConflict => parent_session_conflict,
    AnnouncementRequired => announcement_required,
    AnnouncementConflict => announcement_conflict,
    ObjectIdentityConflict => object_identity_conflict,
    ParentLimitExceeded => parent_limit_exceeded,
    ParentRoleConflict => parent_role_conflict,
    UnauthorizedParent => unauthorized_parent,
    StaleMedia => stale_media,
    StaleEpoch => stale_epoch,
    DatagramLimitExceeded => datagram_limit_exceeded,
    BufferedDatagramLimitExceeded => buffered_datagram_limit_exceeded,
    SymbolReplayConflict => symbol_replay_conflict,
    CodingProfileConflict => coding_profile_conflict,
    Malformed => malformed,
}

/// Receiver state and counters exported to the executable observability layer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RelayIngressSnapshot {
    pub primary_sessions: usize,
    pub secondary_sessions: usize,
    pub authenticated_sessions: usize,
    pub controlled_sessions: usize,
    pub active_objects: usize,
    pub completed_objects: usize,
    pub active_object_bytes: usize,
    pub buffered_datagrams: usize,
    pub counters: RelayIngressCounters,
}

/// Work released by an explicit or amortized expiry scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RelayObjectExpiry {
    pub objects: usize,
    pub released_object_bytes: usize,
    pub released_datagrams: usize,
}

/// Explicit errors at authentication, announcement, identity, deadline, and
/// resource-admission boundaries.
#[derive(Debug)]
pub enum RelayIngressError {
    InvalidConfig {
        field: &'static str,
        reason: &'static str,
    },
    LimitExceeded {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    InvalidSessionId,
    InvalidCarrierIdentity(&'static str),
    AuthenticationRequired,
    ParentSessionMissing(u64),
    ParentSessionConflict(u64),
    UdpPeerConflict(SocketAddr),
    AnnouncementRequired,
    AnnouncementConflict,
    ObjectIdentityConflict,
    ParentLimitExceeded,
    ParentRoleConflict,
    UnauthorizedParent,
    /// The head of the object's stream has passed its deadline by more than
    /// the media budget.
    DeadlineExpired,
    /// The object belongs to an epoch that its stream has left.
    StaleEpoch,
    DatagramLimitExceeded,
    BufferedDatagramLimitExceeded,
    SymbolReplayConflict,
    RelaySession(relay_session::Error),
    /// A compact (RLC1) datagram that does not read or rebuild.
    Compact(&'static str),
}

impl fmt::Display for RelayIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, reason } => write!(formatter, "invalid {field}: {reason}"),
            Self::LimitExceeded {
                field,
                actual,
                maximum,
            } => write!(formatter, "{field} exceeds limit: {actual} > {maximum}"),
            Self::InvalidSessionId => formatter.write_str("relay parent session id must be non-zero"),
            Self::InvalidCarrierIdentity(reason) => {
                write!(formatter, "invalid relay carrier identity: {reason}")
            }
            Self::AuthenticationRequired => formatter.write_str(
                "relay ingress requires an authenticated QUIC session or authenticated private underlay",
            ),
            Self::ParentSessionMissing(id) => {
                write!(formatter, "relay parent session {id} is absent")
            }
            Self::ParentSessionConflict(id) => {
                write!(formatter, "relay parent session {id} conflicts with its registration")
            }
            Self::UdpPeerConflict(peer) => {
                write!(formatter, "UDP peer {peer} conflicts with its authenticated session binding")
            }
            Self::AnnouncementRequired => {
                formatter.write_str("reliable object announcement is required before media symbols")
            }
            Self::AnnouncementConflict => {
                formatter.write_str("reliable object announcement conflicts with active state")
            }
            Self::ObjectIdentityConflict => {
                formatter.write_str("canonical media object identity conflicts with active state")
            }
            Self::ParentLimitExceeded => {
                formatter.write_str("object already has the maximum authenticated parents")
            }
            Self::ParentRoleConflict => {
                formatter.write_str("object already has a parent assigned to that path role")
            }
            Self::UnauthorizedParent => {
                formatter.write_str("relay parent session is unauthorized for this object or path")
            }
            Self::DeadlineExpired => formatter
                .write_str("media object deadline is more than the budget behind its stream"),
            Self::StaleEpoch => {
                formatter.write_str("media object belongs to an epoch its stream has left")
            }
            Self::DatagramLimitExceeded => {
                formatter.write_str("media object datagram admission limit reached")
            }
            Self::BufferedDatagramLimitExceeded => {
                formatter.write_str("aggregate buffered relay datagram limit reached")
            }
            Self::SymbolReplayConflict => {
                formatter.write_str("packet sequence was replayed with different symbol content")
            }
            Self::RelaySession(error) => write!(formatter, "relay-session error: {error}"),
            Self::Compact(reason) => write!(formatter, "compact relay datagram: {reason}"),
        }
    }
}

impl RelayIngressError {
    /// The kind this rejection is counted and logged under.
    #[must_use]
    pub const fn kind(&self) -> RejectionKind {
        match self {
            Self::InvalidConfig { .. }
            | Self::InvalidSessionId
            | Self::InvalidCarrierIdentity(_) => RejectionKind::Invalid,
            Self::LimitExceeded { .. } => RejectionKind::LimitExceeded,
            Self::AuthenticationRequired | Self::ParentSessionMissing(_) => {
                RejectionKind::Authentication
            }
            Self::ParentSessionConflict(_) | Self::UdpPeerConflict(_) => {
                RejectionKind::ParentSessionConflict
            }
            Self::AnnouncementRequired => RejectionKind::AnnouncementRequired,
            Self::AnnouncementConflict => RejectionKind::AnnouncementConflict,
            Self::ObjectIdentityConflict
            | Self::RelaySession(relay_session::Error::ObjectIdentityConflict) => {
                RejectionKind::ObjectIdentityConflict
            }
            Self::ParentLimitExceeded => RejectionKind::ParentLimitExceeded,
            Self::ParentRoleConflict => RejectionKind::ParentRoleConflict,
            Self::UnauthorizedParent => RejectionKind::UnauthorizedParent,
            Self::DeadlineExpired => RejectionKind::StaleMedia,
            Self::StaleEpoch => RejectionKind::StaleEpoch,
            Self::DatagramLimitExceeded => RejectionKind::DatagramLimitExceeded,
            Self::BufferedDatagramLimitExceeded => RejectionKind::BufferedDatagramLimitExceeded,
            Self::SymbolReplayConflict => RejectionKind::SymbolReplayConflict,
            Self::RelaySession(relay_session::Error::CodingProfileConflict) => {
                RejectionKind::CodingProfileConflict
            }
            Self::RelaySession(_) | Self::Compact(_) => RejectionKind::Malformed,
        }
    }
}

impl StdError for RelayIngressError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::RelaySession(error) => Some(error),
            _ => None,
        }
    }
}

impl From<relay_session::Error> for RelayIngressError {
    fn from(error: relay_session::Error) -> Self {
        Self::RelaySession(error)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct LogicalObjectIdentity {
    tenant: String,
    stream: String,
    track: String,
    epoch: u64,
    group: u64,
    object: u64,
    version: u32,
}

impl From<&ObjectKey> for LogicalObjectIdentity {
    fn from(key: &ObjectKey) -> Self {
        Self {
            tenant: key.tenant().to_owned(),
            stream: key.stream().to_owned(),
            track: key.track().to_owned(),
            epoch: key.epoch(),
            group: key.group(),
            object: key.object(),
            version: key.version(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SymbolFingerprint {
    datagram_hash: PayloadHash,
    role: MediaDatagramRole,
}

#[derive(Debug)]
struct ActiveObject {
    announcement: ObjectAnnouncement,
    assembler: ObjectAssembler,
    parent_sessions: HashSet<u64>,
    symbols: HashMap<u32, SymbolFingerprint>,
    source_symbols: usize,
    repair_symbols: usize,
    reserved_bytes: usize,
    last_activity_us: u64,
}

#[derive(Debug)]
struct CompletedObject {
    announcement: ObjectAnnouncement,
    parent_sessions: HashSet<u64>,
}

/// The identity of a compact object, without its payload hash (a compact
/// datagram carries none) and without an allocation: its strings digested.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CompactIdentity {
    names: u64,
    epoch: u64,
    group: u64,
    object: u64,
    version: u32,
}

impl CompactIdentity {
    fn new(
        tenant: &str,
        stream: &str,
        track: &str,
        epoch: u64,
        group: u64,
        object: u64,
        version: u32,
    ) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        (tenant, stream, track).hash(&mut hasher);
        Self {
            names: hasher.finish(),
            epoch,
            group,
            object,
            version,
        }
    }

    fn of(key: &ObjectKey) -> Self {
        Self::new(
            key.tenant(),
            key.stream(),
            key.track(),
            key.epoch(),
            key.group(),
            key.object(),
            key.version(),
        )
    }
}

/// The relay tags of a compact datagram, as the RLS1 types.
fn compact_tags(
    header: &compact_object::CompactHeader<'_>,
) -> Result<(MediaDatagramRole, MediaPathIntent, MediaObjectKind), RelayIngressError> {
    let (role, path) = match (header.role, header.path) {
        (0, 0) => (MediaDatagramRole::Source, MediaPathIntent::PrimarySource),
        (1, 1) => (MediaDatagramRole::Repair, MediaPathIntent::SecondaryRepair),
        _ => return Err(RelayIngressError::Compact("path intent")),
    };
    let kind = match header.kind {
        0 => MediaObjectKind::Initialization,
        1 => MediaObjectKind::CodecConfig,
        2 => MediaObjectKind::VideoKeyframe,
        3 => MediaObjectKind::Audio,
        4 => MediaObjectKind::VideoDelta,
        _ => MediaObjectKind::Data,
    };
    Ok((role, path, kind))
}

/// The largest whole object whose descriptor a receiver learns: audio
/// parts that fit one datagram.
const COMPACT_TEMPLATE_MAX_ENVELOPE: usize = 2_048;

/// Bounded cross-parent object receiver.
pub struct RelayObjectReceiver {
    config: RelayObjectReceiverConfig,
    sessions: HashMap<u64, AuthenticatedRelayParentSession>,
    objects: HashMap<ObjectKey, ActiveObject>,
    logical_keys: HashMap<LogicalObjectIdentity, ObjectKey>,
    completed: HashMap<ObjectKey, CompletedObject>,
    completed_order: VecDeque<ObjectKey>,
    buffered_object_bytes: usize,
    buffered_datagrams: usize,
    counters: RelayIngressCounters,
    next_expiry_scan_us: Option<u64>,
    /// The descriptors of compact objects, learnt from whole ones.
    templates: CompactTemplates,
    /// Compact objects decoded, for the duplicates of their other copies.
    compact_completed: HashMap<CompactIdentity, ObjectKey>,
    compact_order: VecDeque<CompactIdentity>,
    /// The objects in `completed`, as a compact datagram names one: a part
    /// that came whole is not rebuilt from its compact copy.
    whole_done: HashSet<CompactIdentity>,
    /// Each stream's place on its own timeline: what a deadline is measured
    /// against (never this node's wall clock).
    clocks: MediaClocks,
}

impl fmt::Debug for RelayObjectReceiver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayObjectReceiver")
            .field("config", &self.config)
            .field("sessions", &self.sessions.len())
            .field("active_objects", &self.objects.len())
            .field("completed_objects", &self.completed.len())
            .field("counters", &self.counters)
            .finish_non_exhaustive()
    }
}

impl RelayObjectReceiver {
    /// Construct a receiver after validating every configured ceiling.
    pub fn new(config: RelayObjectReceiverConfig) -> Result<Self, RelayIngressError> {
        config.validate()?;
        Ok(Self {
            config,
            sessions: HashMap::new(),
            objects: HashMap::new(),
            // A completed object keeps its logical key until it is evicted:
            // both tables hold up to `max_completed_objects` and are sized
            // for that once, so they never grow under the tee (growing them
            // rehashed thousands of string keys, which held a 1 ms stream's
            // commits for 2-5 ms at its 1,792nd and 3,584th part).
            logical_keys: HashMap::with_capacity(config.max_completed_objects),
            completed: HashMap::with_capacity(config.max_completed_objects),
            completed_order: VecDeque::with_capacity(config.max_completed_objects),
            buffered_object_bytes: 0,
            buffered_datagrams: 0,
            counters: RelayIngressCounters::default(),
            next_expiry_scan_us: None,
            templates: CompactTemplates::new(),
            compact_completed: HashMap::with_capacity(config.max_completed_objects),
            compact_order: VecDeque::with_capacity(config.max_completed_objects),
            whole_done: HashSet::with_capacity(config.max_completed_objects),
            clocks: MediaClocks::new(MEDIA_CLOCK_STREAMS),
        })
    }

    #[must_use]
    pub const fn config(&self) -> &RelayObjectReceiverConfig {
        &self.config
    }

    #[must_use]
    pub fn state(&self) -> RelayObjectReceiverState {
        RelayObjectReceiverState {
            parent_sessions: self.sessions.len(),
            active_objects: self.objects.len(),
            completed_objects: self.completed.len(),
            buffered_object_bytes: self.buffered_object_bytes,
            buffered_datagrams: self.buffered_datagrams,
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> RelayIngressSnapshot {
        let primary_sessions = self
            .sessions
            .values()
            .filter(|session| session.parent_path == ParentPath::Primary)
            .count();
        let secondary_sessions = self
            .sessions
            .values()
            .filter(|session| {
                matches!(
                    session.parent_path,
                    ParentPath::Secondary | ParentPath::PromotedSecondary
                )
            })
            .count();
        let controlled_sessions = self
            .sessions
            .values()
            .filter(|session| session.controlled_qualification)
            .count();
        RelayIngressSnapshot {
            primary_sessions,
            secondary_sessions,
            authenticated_sessions: self.sessions.len().saturating_sub(controlled_sessions),
            controlled_sessions,
            active_objects: self.objects.len(),
            completed_objects: self.completed.len(),
            active_object_bytes: self.buffered_object_bytes,
            buffered_datagrams: self.buffered_datagrams,
            counters: self.counters,
        }
    }

    /// The cumulative counters alone (the snapshot also walks the sessions).
    #[must_use]
    pub const fn counters(&self) -> &RelayIngressCounters {
        &self.counters
    }

    /// Register a caller-authenticated carrier session. Replaying an identical
    /// registration is idempotent; changing any binding is rejected.
    pub fn register_parent_session(
        &mut self,
        session: AuthenticatedRelayParentSession,
    ) -> Result<(), RelayIngressError> {
        debug_assert!(!session.controlled_qualification);
        self.register_parent_session_inner(session)
    }

    /// Register an explicitly controlled qualification session. Production
    /// public carriers use [`Self::register_parent_session`] with authenticated
    /// QUIC identity plus a reliable announcement.
    pub fn register_controlled_parent_session(
        &mut self,
        session: ControlledRelayParentSession,
    ) -> Result<(), RelayIngressError> {
        self.register_parent_session_inner(session.0)
    }

    fn register_parent_session_inner(
        &mut self,
        session: AuthenticatedRelayParentSession,
    ) -> Result<(), RelayIngressError> {
        if let Some(existing) = self.sessions.get(&session.session_id) {
            return if existing == &session {
                Ok(())
            } else {
                Err(RelayIngressError::ParentSessionConflict(session.session_id))
            };
        }
        if self.sessions.len() >= self.config.max_parent_sessions {
            return Err(RelayIngressError::LimitExceeded {
                field: "parent_sessions",
                actual: self.sessions.len().saturating_add(1),
                maximum: self.config.max_parent_sessions,
            });
        }
        self.sessions.insert(session.session_id, session);
        Ok(())
    }

    /// Admit one reliable announcement from an authenticated assigned parent.
    /// Compatible primary and secondary announcements join the same canonical
    /// object assembly.
    pub fn announce_object(
        &mut self,
        session_id: u64,
        announcement: ObjectAnnouncement,
        now_us: u64,
    ) -> Result<RelayAnnouncementOutcome, RelayIngressError> {
        let result = self.try_announce_object(session_id, announcement, now_us);
        if let Err(error) = &result {
            self.record_rejection(error, false);
        }
        result
    }

    fn try_announce_object(
        &mut self,
        session_id: u64,
        announcement: ObjectAnnouncement,
        now_us: u64,
    ) -> Result<RelayAnnouncementOutcome, RelayIngressError> {
        self.expire_if_due(now_us);
        let session = self.session(session_id)?.clone();
        validate_session_announcement(&session, &announcement)?;
        announcement.coding.validate(self.config.relay_limits)?;
        if announcement.initial_repair_symbols > self.config.relay_limits.max_extra_repair_symbols {
            return Err(RelayIngressError::LimitExceeded {
                field: "initial_repair_symbols",
                actual: announcement.initial_repair_symbols as usize,
                maximum: self.config.relay_limits.max_extra_repair_symbols as usize,
            });
        }
        let logical = LogicalObjectIdentity::from(&announcement.key);
        if self
            .logical_keys
            .get(&logical)
            .is_some_and(|existing| existing != &announcement.key)
        {
            return Err(RelayIngressError::ObjectIdentityConflict);
        }

        if let Some(completed) = self.completed.get_mut(&announcement.key) {
            if completed.announcement != announcement {
                return Err(RelayIngressError::AnnouncementConflict);
            }
            add_parent_to_set(
                &self.sessions,
                &mut completed.parent_sessions,
                &session,
                self.config.max_parents_per_object,
            )?;
            return Ok(RelayAnnouncementOutcome::AlreadyComplete);
        }
        if self.completed_compact(&announcement.key)? {
            return Ok(RelayAnnouncementOutcome::AlreadyComplete);
        }

        if let Some(active) = self.objects.get_mut(&announcement.key) {
            if active.announcement != announcement {
                return Err(RelayIngressError::AnnouncementConflict);
            }
            add_parent_to_set(
                &self.sessions,
                &mut active.parent_sessions,
                &session,
                self.config.max_parents_per_object,
            )?;
            active.last_activity_us = now_us;
            return Ok(RelayAnnouncementOutcome::Joined);
        }

        // No assembly is opened for an object that its stream has left
        // behind. An announcement moves no clock: the head of a stream
        // follows the datagrams that arrive.
        verdict_result(self.clocks.peek(
            MediaClocks::key(announcement.key.tenant(), announcement.key.stream()),
            announcement.key.epoch(),
            announcement.deadline.expires_at_us,
            self.config.media_deadline_us,
        ))?;

        if self.objects.len() >= self.config.max_active_objects {
            return Err(RelayIngressError::LimitExceeded {
                field: "active_objects",
                actual: self.objects.len().saturating_add(1),
                maximum: self.config.max_active_objects,
            });
        }
        let reserved_bytes = announcement.coding.transfer_length() as usize;
        let requested = self
            .buffered_object_bytes
            .checked_add(reserved_bytes)
            .ok_or(RelayIngressError::LimitExceeded {
                field: "buffered_object_bytes",
                actual: usize::MAX,
                maximum: self.config.max_buffered_object_bytes,
            })?;
        if requested > self.config.max_buffered_object_bytes {
            return Err(RelayIngressError::LimitExceeded {
                field: "buffered_object_bytes",
                actual: requested,
                maximum: self.config.max_buffered_object_bytes,
            });
        }

        let assembler = ObjectAssembler::new(announcement.clone(), self.config.relay_limits)?;
        let mut parent_sessions = HashSet::with_capacity(self.config.max_parents_per_object);
        parent_sessions.insert(session_id);
        self.logical_keys.insert(logical, announcement.key.clone());
        self.objects.insert(
            announcement.key.clone(),
            ActiveObject {
                announcement,
                assembler,
                parent_sessions,
                symbols: HashMap::new(),
                source_symbols: 0,
                repair_symbols: 0,
                reserved_bytes,
                last_activity_us: now_us,
            },
        );
        self.buffered_object_bytes = requested;
        Ok(RelayAnnouncementOutcome::Started)
    }

    fn ensure_controlled_announcement(
        &mut self,
        session_id: u64,
        symbol: &relay_session::RelayDatagram,
        now_us: u64,
    ) -> Result<(), RelayIngressError> {
        {
            let session = self.session(session_id)?;
            if !session.controlled_qualification {
                return Err(RelayIngressError::AnnouncementRequired);
            }
            validate_session_symbol(session, symbol)?;
        }
        // The object's symbols after its first (fix4-video): announced for
        // this session already with the same fields, which is all the
        // announcement below would find (it would only join the session to
        // the object's parents, which it is in, and touch the assembly).
        // No copy of the key, the session or the announcement. (Expired
        // state first, as the announcement does: an assembly that went
        // inactive is announced afresh.)
        self.expire_if_due(now_us);
        if let Some(active) = self.objects.get_mut(&symbol.object_key) {
            if active.parent_sessions.contains(&session_id)
                && derived_announcement_matches(&active.announcement, symbol)
            {
                active.last_activity_us = now_us;
                return Ok(());
            }
        } else if self.completed.get(&symbol.object_key).is_some_and(|completed| {
            completed.parent_sessions.contains(&session_id)
                && derived_announcement_matches(&completed.announcement, symbol)
        }) {
            return Ok(());
        }
        let announcement = ObjectAnnouncement {
            generation: symbol.generation,
            subscription_id: symbol.subscription_id,
            key: symbol.object_key.clone(),
            kind: symbol.object_kind,
            deadline: symbol.deadline,
            coding: symbol.coding,
            // RLS1 does not carry repair-cursor state. These fields remain zero
            // in qualification-derived announcements and are never used to
            // produce additional repair symbols at the receiver.
            initial_repair_symbols: 0,
            next_packet_sequence: 0,
        };
        self.try_announce_object(session_id, announcement, now_us)?;
        Ok(())
    }

    /// Decode and admit one RLS1 datagram from a registered authenticated
    /// parent. `now_us` is this node's clock: it bounds an assembly's
    /// inactivity and nothing else (a deadline is measured on its stream's
    /// own timeline).
    pub fn push_wire_datagram(
        &mut self,
        session_id: u64,
        wire: &[u8],
        now_us: u64,
    ) -> Result<RelayIngressOutcome, RelayIngressError> {
        self.counters.datagrams_received = self.counters.datagrams_received.saturating_add(1);
        let result = self.try_push_wire_datagram(session_id, wire, now_us);
        if let Err(error) = &result {
            self.record_rejection(error, true);
        }
        result
    }

    fn try_push_wire_datagram(
        &mut self,
        session_id: u64,
        wire: &[u8],
        now_us: u64,
    ) -> Result<RelayIngressOutcome, RelayIngressError> {
        self.session(session_id)?;
        let symbol = decode_datagram(wire, self.config.relay_limits)?;
        self.try_push_symbol(session_id, symbol, now_us)
    }

    /// [`Self::push_wire_datagram`] of a datagram already decoded (the
    /// dispatch decodes each once, fix4-video).
    fn push_decoded_datagram(
        &mut self,
        session_id: u64,
        symbol: relay_session::RelayDatagram,
        now_us: u64,
    ) -> Result<RelayIngressOutcome, RelayIngressError> {
        self.counters.datagrams_received = self.counters.datagrams_received.saturating_add(1);
        let result = self.try_push_symbol(session_id, symbol, now_us);
        if let Err(error) = &result {
            self.record_rejection(error, true);
        }
        result
    }

    fn try_push_symbol(
        &mut self,
        session_id: u64,
        symbol: relay_session::RelayDatagram,
        now_us: u64,
    ) -> Result<RelayIngressOutcome, RelayIngressError> {
        self.expire_if_due(now_us);
        // Borrowed, not copied (its identity is two node names): what the
        // checks below need of it is its path.
        let parent_path = {
            let session = self.session(session_id)?;
            validate_session_symbol(session, &symbol)?;
            session.parent_path
        };

        // An object being assembled has its logical identity registered
        // with its own key since its announcement (and an announcement of
        // another key of that identity is refused), so only a symbol of an
        // object not under way is looked up, which copies the key's names.
        if !self.objects.contains_key(&symbol.object_key) {
            let logical = LogicalObjectIdentity::from(&symbol.object_key);
            if self
                .logical_keys
                .get(&logical)
                .is_some_and(|existing| existing != &symbol.object_key)
            {
                return Err(RelayIngressError::ObjectIdentityConflict);
            }
        }

        // A whole copy of a part that was rebuilt from its compact one is
        // not assembled or decoded again.
        let duplicate = if self.completed_compact(&symbol.object_key)? {
            true
        } else if let Some(completed) = self.completed.get(&symbol.object_key) {
            validate_completed_symbol(completed, self.session(session_id)?, &symbol)?;
            true
        } else {
            false
        };
        if duplicate {
            self.counters.duplicate_datagrams = self.counters.duplicate_datagrams.saturating_add(1);
            match symbol.role {
                MediaDatagramRole::Source => {
                    self.counters.source_datagrams =
                        self.counters.source_datagrams.saturating_add(1);
                }
                MediaDatagramRole::Repair => {
                    self.counters.repair_datagrams =
                        self.counters.repair_datagrams.saturating_add(1);
                }
            }
            return Ok(RelayIngressOutcome::Duplicate {
                key: symbol.object_key,
                role: symbol.role,
                deadline: symbol.deadline,
            });
        }

        // Deadline admission happens before any decoder mutation, on the
        // stream's own timeline. Completed objects above remain tombstones,
        // so an authenticated symbol that races decode completion is
        // classified as a duplicate instead of a deadline or authentication
        // failure. A symbol the stream has left behind is dropped; its
        // object's assembly ends with it only when the stream has left the
        // assembly's own announced deadline behind (a symbol's deadline is
        // its own claim, and one corrupt symbol must not cost the object).
        let clock = MediaClocks::key(symbol.object_key.tenant(), symbol.object_key.stream());
        let verdict = self.clocks.observe(
            clock,
            symbol.object_key.epoch(),
            symbol.deadline.expires_at_us,
            self.config.media_deadline_us,
        );
        if let Err(error) = verdict_result(verdict) {
            let left_behind = self.objects.get(&symbol.object_key).is_some_and(|state| {
                self.clocks.peek(
                    clock,
                    symbol.object_key.epoch(),
                    state.announcement.deadline.expires_at_us,
                    self.config.media_deadline_us,
                ) != Verdict::Fresh
            });
            if left_behind
                && self
                    .remove_active_object(&symbol.object_key, true)
                    .is_some()
            {
                self.counters.expired_objects = self.counters.expired_objects.saturating_add(1);
            }
            return Err(error);
        }

        let state = self
            .objects
            .get_mut(&symbol.object_key)
            .ok_or(RelayIngressError::AnnouncementRequired)?;
        if !state.parent_sessions.contains(&session_id) || !parent_path.permits(symbol.path_intent)
        {
            return Err(RelayIngressError::UnauthorizedParent);
        }

        let header =
            DatagramFecHeader::decode(&symbol.fec_datagram).map_err(relay_session::Error::from)?;
        let fingerprint = SymbolFingerprint {
            datagram_hash: PayloadHash::digest(&symbol.fec_datagram),
            role: symbol.role,
        };
        if let Some(existing) = state.symbols.get(&header.packet_sequence) {
            return if existing == &fingerprint {
                self.counters.duplicate_datagrams =
                    self.counters.duplicate_datagrams.saturating_add(1);
                match symbol.role {
                    MediaDatagramRole::Source => {
                        self.counters.source_datagrams =
                            self.counters.source_datagrams.saturating_add(1);
                    }
                    MediaDatagramRole::Repair => {
                        self.counters.repair_datagrams =
                            self.counters.repair_datagrams.saturating_add(1);
                    }
                }
                Ok(RelayIngressOutcome::Duplicate {
                    key: symbol.object_key,
                    role: symbol.role,
                    deadline: symbol.deadline,
                })
            } else {
                Err(RelayIngressError::SymbolReplayConflict)
            };
        }
        if state.symbols.len() >= self.config.max_datagrams_per_object {
            return Err(RelayIngressError::DatagramLimitExceeded);
        }
        if self.buffered_datagrams >= self.config.max_buffered_datagrams {
            return Err(RelayIngressError::BufferedDatagramLimitExceeded);
        }

        let decoded = state.assembler.push_symbol(&symbol)?;
        state.symbols.insert(header.packet_sequence, fingerprint);
        match symbol.role {
            MediaDatagramRole::Source => {
                state.source_symbols = state.source_symbols.saturating_add(1);
                self.counters.source_datagrams = self.counters.source_datagrams.saturating_add(1);
            }
            MediaDatagramRole::Repair => {
                state.repair_symbols = state.repair_symbols.saturating_add(1);
                self.counters.repair_datagrams = self.counters.repair_datagrams.saturating_add(1);
            }
        }
        state.last_activity_us = now_us;
        self.buffered_datagrams = self.buffered_datagrams.saturating_add(1);

        let Some(object) = decoded else {
            return Ok(RelayIngressOutcome::Buffered {
                key: symbol.object_key,
                role: symbol.role,
                deadline: symbol.deadline,
            });
        };
        validate_object_kind(&object, state.announcement.kind)?;
        let envelope = state
            .assembler
            .take_completed_envelope()
            .expect("a completed object must retain its validated canonical envelope");
        // A whole audio part carries its stream's descriptor for the
        // compact parts that follow it.
        if state.announcement.kind == MediaObjectKind::Audio
            && envelope.len() <= COMPACT_TEMPLATE_MAX_ENVELOPE
            && self.templates.learn(&object).is_some()
        {
            self.counters.compact_descriptors_learned =
                self.counters.compact_descriptors_learned.saturating_add(1);
        }
        self.note_decoded(&object, now_us);
        let key = object.key().clone();
        let active = self
            .remove_active_object(&key, false)
            .expect("decoded object has active receive state");
        let parent_count = active.parent_sessions.len();
        let accepted_datagrams = active.symbols.len();
        self.counters.decoded_objects = self.counters.decoded_objects.saturating_add(1);
        if active.repair_symbols > 0 {
            self.counters.repair_assisted_objects =
                self.counters.repair_assisted_objects.saturating_add(1);
        }
        let missing_source_symbols = usize::from(active.announcement.coding.source_symbols())
            .saturating_sub(active.source_symbols);
        if missing_source_symbols > 0 {
            self.counters.fec_recovered_objects =
                self.counters.fec_recovered_objects.saturating_add(1);
            self.counters.fec_recovered_source_symbols = self
                .counters
                .fec_recovered_source_symbols
                .saturating_add(missing_source_symbols as u64);
        }
        self.insert_completed(
            key,
            CompletedObject {
                announcement: active.announcement,
                parent_sessions: active.parent_sessions,
            },
        );
        Ok(RelayIngressOutcome::Decoded {
            object: Box::new(object),
            envelope,
            role: symbol.role,
            deadline: symbol.deadline,
            parent_count,
            accepted_datagrams,
        })
    }

    /// Receive one compact (RLC1) datagram of an authenticated session: the
    /// object rebuilt from its slim object and the stream's descriptor
    /// (`Decoded`, with the exact envelope of the whole object), a
    /// `Duplicate` for another copy of a decoded one, or `Buffered` when
    /// the descriptor is not known yet (the datagram is still admitted, so
    /// a relay forwards it).
    ///
    /// # Errors
    ///
    /// An unknown session, a session that does not admit the datagram, a
    /// deadline that its stream has left behind, or a datagram that does
    /// not read or rebuild.
    pub fn push_compact_datagram(
        &mut self,
        session_id: u64,
        wire: &[u8],
        now_us: u64,
    ) -> Result<RelayIngressOutcome, RelayIngressError> {
        self.counters.datagrams_received = self.counters.datagrams_received.saturating_add(1);
        let result = self.try_push_compact_datagram(session_id, wire, now_us);
        if let Err(error) = &result {
            self.record_rejection(error, true);
        }
        result
    }

    fn try_push_compact_datagram(
        &mut self,
        session_id: u64,
        wire: &[u8],
        now_us: u64,
    ) -> Result<RelayIngressOutcome, RelayIngressError> {
        let session = self.session(session_id)?;
        let (header, slim) = compact_object::parse_datagram(wire)
            .map_err(|error| RelayIngressError::Compact(error.0))?;
        let (role, path, kind) = compact_tags(&header)?;
        if header.generation != session.generation.get()
            || header.subscription_id != session.subscription_id.get()
            || !session.parent_path.permits(path)
        {
            return Err(RelayIngressError::UnauthorizedParent);
        }
        self.expire_if_due(now_us);
        let deadline = MediaDeadline::from_micros(header.deadline_us);
        let identity = CompactIdentity::new(
            header.tenant,
            header.stream,
            header.track,
            header.epoch,
            header.group,
            header.object,
            header.version,
        );
        let count_role = |counters: &mut RelayIngressCounters| match role {
            MediaDatagramRole::Source => {
                counters.source_datagrams = counters.source_datagrams.saturating_add(1);
            }
            MediaDatagramRole::Repair => {
                counters.repair_datagrams = counters.repair_datagrams.saturating_add(1);
            }
        };
        // A compact datagram carries no payload hash: where its object is
        // not at hand, its key has none.
        let unhashed_key = || {
            ObjectKey::new(
                header.tenant,
                header.stream,
                header.track,
                header.epoch,
                header.group,
                header.object,
                header.version,
                PayloadHash::from_bytes([0; 32]),
            )
            .map_err(|_| RelayIngressError::Compact("object key"))
        };
        // Another copy of a part rebuilt already, or the compact copy of a
        // part that came whole: not rebuilt or committed again.
        let completed = match self.compact_completed.get(&identity) {
            Some(key) => Some(key.clone()),
            None if self.whole_done.contains(&identity) => Some(unhashed_key()?),
            None => None,
        };
        if let Some(key) = completed {
            self.counters.duplicate_datagrams = self.counters.duplicate_datagrams.saturating_add(1);
            count_role(&mut self.counters);
            return Ok(RelayIngressOutcome::Duplicate {
                key,
                role,
                deadline,
            });
        }
        verdict_result(self.clocks.observe(
            MediaClocks::key(header.tenant, header.stream),
            header.epoch,
            header.deadline_us,
            self.config.media_deadline_us,
        ))?;
        let object = match self.templates.expand(header.stream, slim) {
            Ok(object) => object,
            Err(error) if error.0 == "unknown descriptor" => {
                self.counters.compact_unknown_descriptor =
                    self.counters.compact_unknown_descriptor.saturating_add(1);
                count_role(&mut self.counters);
                return Ok(RelayIngressOutcome::Buffered {
                    key: unhashed_key()?,
                    role,
                    deadline,
                });
            }
            Err(error) => return Err(RelayIngressError::Compact(error.0)),
        };
        let key = object.key();
        if key.tenant() != header.tenant
            || key.stream() != header.stream
            || key.track() != header.track
            || key.epoch() != header.epoch
            || key.group() != header.group
            || key.object() != header.object
            || key.version() != header.version
        {
            return Err(RelayIngressError::ObjectIdentityConflict);
        }
        validate_object_kind(&object, kind)?;
        let envelope =
            media_object::encode(&object).map_err(|_| RelayIngressError::Compact("envelope"))?;
        self.note_decoded(&object, now_us);
        if self.compact_completed.len() >= self.config.max_completed_objects {
            if let Some(evicted) = self.compact_order.pop_front() {
                self.compact_completed.remove(&evicted);
            }
        }
        self.compact_order.push_back(identity);
        self.compact_completed.insert(identity, key.clone());
        self.counters.decoded_objects = self.counters.decoded_objects.saturating_add(1);
        self.counters.compact_decoded_objects =
            self.counters.compact_decoded_objects.saturating_add(1);
        count_role(&mut self.counters);
        Ok(RelayIngressOutcome::Decoded {
            object: Box::new(object),
            envelope,
            role,
            deadline,
            parent_count: 1,
            accepted_datagrams: 1,
        })
    }

    /// Remove the object assemblies that their stream has left behind (its
    /// head has passed their deadline by the media budget, or it has moved
    /// to a later epoch) and those with no symbol for the inactivity bound
    /// of `now_us`, this node's clock.
    pub fn expire(&mut self, now_us: u64) -> RelayObjectExpiry {
        self.clocks.tick(now_us);
        let expired = self
            .objects
            .iter()
            .filter_map(|(key, state)| {
                let left_behind = self.clocks.peek(
                    MediaClocks::key(key.tenant(), key.stream()),
                    key.epoch(),
                    state.announcement.deadline.expires_at_us,
                    self.config.media_deadline_us,
                ) != Verdict::Fresh;
                let inactive = now_us.saturating_sub(state.last_activity_us)
                    >= self.config.object_inactivity_timeout_us;
                (left_behind || inactive).then(|| key.clone())
            })
            .collect::<Vec<_>>();
        let mut result = RelayObjectExpiry::default();
        for key in expired {
            if let Some(state) = self.remove_active_object(&key, true) {
                result.objects = result.objects.saturating_add(1);
                result.released_object_bytes = result
                    .released_object_bytes
                    .saturating_add(state.reserved_bytes);
                result.released_datagrams = result
                    .released_datagrams
                    .saturating_add(state.symbols.len());
            }
        }
        self.counters.expired_objects = self
            .counters
            .expired_objects
            .saturating_add(result.objects as u64);
        self.next_expiry_scan_us = Some(now_us.saturating_add(self.config.expiry_scan_interval_us));
        result
    }

    fn expire_if_due(&mut self, now_us: u64) {
        // The clocks age on this node's time (an idle clock's slot may be
        // reused): every datagram and announcement passes here first.
        self.clocks.tick(now_us);
        let due = match self.next_expiry_scan_us {
            Some(next_scan) => now_us >= next_scan,
            None => true,
        };
        if due {
            self.expire(now_us);
        }
    }

    fn record_rejection(&mut self, error: &RelayIngressError, datagram: bool) {
        if datagram {
            self.counters.datagrams_rejected = self.counters.datagrams_rejected.saturating_add(1);
        }
        self.counters.rejected_by_kind.count(error.kind());
        match error {
            RelayIngressError::AuthenticationRequired
            | RelayIngressError::ParentSessionMissing(_)
            | RelayIngressError::UnauthorizedParent => {
                self.counters.authentication_drops =
                    self.counters.authentication_drops.saturating_add(1);
            }
            RelayIngressError::DeadlineExpired => {
                self.counters.deadline_drops = self.counters.deadline_drops.saturating_add(1);
            }
            RelayIngressError::AnnouncementConflict
            | RelayIngressError::ObjectIdentityConflict
            | RelayIngressError::SymbolReplayConflict
            | RelayIngressError::RelaySession(relay_session::Error::CodingProfileConflict)
            | RelayIngressError::RelaySession(relay_session::Error::ObjectIdentityConflict) => {
                self.counters.conflict_drops = self.counters.conflict_drops.saturating_add(1);
            }
            _ => {}
        }
    }

    /// Whether the object was rebuilt from a compact datagram already. A
    /// whole object of the same names and number with another payload is a
    /// conflict, as it is against a whole one.
    fn completed_compact(&self, key: &ObjectKey) -> Result<bool, RelayIngressError> {
        // No table to look in on a receiver that sees no compact datagrams
        // (video): its symbols pay nothing for this.
        if self.compact_completed.is_empty() {
            return Ok(false);
        }
        match self.compact_completed.get(&CompactIdentity::of(key)) {
            Some(completed) if completed == key => Ok(true),
            Some(_) => Err(RelayIngressError::ObjectIdentityConflict),
            None => Ok(false),
        }
    }

    /// An object decoded: its epoch is validated, so its stream's clock may
    /// take it up; and how late the object is by this node's wall clock is
    /// counted, for the record alone.
    fn note_decoded(&mut self, object: &MediaObject, now_us: u64) {
        let key = object.key();
        self.clocks
            .set_epoch(MediaClocks::key(key.tenant(), key.stream()), key.epoch());
        if object.kind() != ObjectKind::Media {
            return;
        }
        let Some(published_us) = object
            .stage_timestamps()
            .iter()
            .find(|timestamp| timestamp.stage() == Stage::Published)
            .and_then(|timestamp| u64::try_from(timestamp.timestamp().unix_time_ns()).ok())
            .map(|published_ns| published_ns / 1_000)
        else {
            return;
        };
        let late_us = now_us.saturating_sub(published_us);
        if late_us > self.config.media_deadline_us {
            self.counters.wall_late_parts = self.counters.wall_late_parts.saturating_add(1);
        }
        self.counters.wall_late_max_us = self.counters.wall_late_max_us.max(late_us);
    }

    fn session(
        &self,
        session_id: u64,
    ) -> Result<&AuthenticatedRelayParentSession, RelayIngressError> {
        self.sessions
            .get(&session_id)
            .ok_or(RelayIngressError::ParentSessionMissing(session_id))
    }

    fn remove_active_object(
        &mut self,
        key: &ObjectKey,
        remove_logical_key: bool,
    ) -> Option<ActiveObject> {
        let state = self.objects.remove(key)?;
        self.buffered_object_bytes = self
            .buffered_object_bytes
            .saturating_sub(state.reserved_bytes);
        self.buffered_datagrams = self.buffered_datagrams.saturating_sub(state.symbols.len());
        if remove_logical_key {
            self.logical_keys.remove(&LogicalObjectIdentity::from(key));
        }
        Some(state)
    }

    fn insert_completed(&mut self, key: ObjectKey, completed: CompletedObject) {
        while self.completed.len() >= self.config.max_completed_objects {
            let Some(evicted) = self.completed_order.pop_front() else {
                break;
            };
            if self.completed.remove(&evicted).is_some() {
                self.logical_keys
                    .remove(&LogicalObjectIdentity::from(&evicted));
                self.whole_done.remove(&CompactIdentity::of(&evicted));
            }
        }
        self.whole_done.insert(CompactIdentity::of(&key));
        self.completed_order.push_back(key.clone());
        self.completed.insert(key, completed);
    }
}

fn add_parent_to_set(
    sessions: &HashMap<u64, AuthenticatedRelayParentSession>,
    parent_sessions: &mut HashSet<u64>,
    candidate: &AuthenticatedRelayParentSession,
    max_parents: usize,
) -> Result<(), RelayIngressError> {
    if parent_sessions.contains(&candidate.session_id) {
        return Ok(());
    }
    if parent_sessions.len() >= max_parents {
        return Err(RelayIngressError::ParentLimitExceeded);
    }
    for existing_id in parent_sessions.iter() {
        let existing = sessions
            .get(existing_id)
            .expect("object parent sessions remain registered");
        if existing.identity.peer == candidate.identity.peer
            && !(existing.controlled_qualification && candidate.controlled_qualification)
        {
            return Err(RelayIngressError::ParentSessionConflict(
                candidate.session_id,
            ));
        }
        // Two sessions on the primary path, or two on the repair path, are
        // a miscompiled plan. Promoted sessions each carry sources and
        // repair, and any number of them may bring symbols of one object:
        // two contributors at a first forwarder, every parent at an edge.
        if existing.parent_path == candidate.parent_path
            && candidate.parent_path != ParentPath::PromotedSecondary
        {
            return Err(RelayIngressError::ParentRoleConflict);
        }
    }
    parent_sessions.insert(candidate.session_id);
    Ok(())
}

/// The media clock's verdict as the receiver's result.
fn verdict_result(verdict: Verdict) -> Result<(), RelayIngressError> {
    match verdict {
        Verdict::Fresh => Ok(()),
        Verdict::StaleEpoch => Err(RelayIngressError::StaleEpoch),
        Verdict::StaleMedia => Err(RelayIngressError::DeadlineExpired),
    }
}

fn validate_session_announcement(
    session: &AuthenticatedRelayParentSession,
    announcement: &ObjectAnnouncement,
) -> Result<(), RelayIngressError> {
    if announcement.generation != session.generation
        || announcement.subscription_id != session.subscription_id
    {
        return Err(RelayIngressError::UnauthorizedParent);
    }
    Ok(())
}

fn validate_session_symbol(
    session: &AuthenticatedRelayParentSession,
    symbol: &relay_session::RelayDatagram,
) -> Result<(), RelayIngressError> {
    if symbol.generation != session.generation
        || symbol.subscription_id != session.subscription_id
        || !session.parent_path.permits(symbol.path_intent)
    {
        return Err(RelayIngressError::UnauthorizedParent);
    }
    Ok(())
}

/// The announcement a controlled session's symbol makes of its object
/// (`ensure_controlled_announcement`) equals `announcement`, its key aside.
fn derived_announcement_matches(
    announcement: &ObjectAnnouncement,
    symbol: &relay_session::RelayDatagram,
) -> bool {
    announcement.generation == symbol.generation
        && announcement.subscription_id == symbol.subscription_id
        && announcement.kind == symbol.object_kind
        && announcement.deadline == symbol.deadline
        && announcement.coding == symbol.coding
        && announcement.initial_repair_symbols == 0
        && announcement.next_packet_sequence == 0
}

fn validate_completed_symbol(
    completed: &CompletedObject,
    session: &AuthenticatedRelayParentSession,
    symbol: &relay_session::RelayDatagram,
) -> Result<(), RelayIngressError> {
    if !completed.parent_sessions.contains(&session.session_id)
        || symbol.generation != completed.announcement.generation
        || symbol.subscription_id != completed.announcement.subscription_id
        || symbol.object_kind != completed.announcement.kind
        || symbol.deadline != completed.announcement.deadline
        || symbol.coding != completed.announcement.coding
        || !session.parent_path.permits(symbol.path_intent)
    {
        return Err(RelayIngressError::UnauthorizedParent);
    }
    Ok(())
}

fn validate_object_kind(
    object: &MediaObject,
    announced: MediaObjectKind,
) -> Result<(), RelayIngressError> {
    let compatible = match object.kind() {
        ObjectKind::Initialization => announced == MediaObjectKind::Initialization,
        ObjectKind::CodecConfiguration => announced == MediaObjectKind::CodecConfig,
        ObjectKind::Discontinuity => announced == MediaObjectKind::Data,
        ObjectKind::Media if object.is_keyframe() => announced == MediaObjectKind::VideoKeyframe,
        ObjectKind::Media => matches!(
            announced,
            MediaObjectKind::Audio
                | MediaObjectKind::VideoDelta
                | MediaObjectKind::Data
                | MediaObjectKind::VideoKeyframe
        ),
    };
    if compatible {
        Ok(())
    } else {
        Err(RelayIngressError::ObjectIdentityConflict)
    }
}

/// Result of classifying one UDP payload at the executable compatibility seam.
#[derive(Debug)]
pub enum RelayUdpDispatchOutcome {
    Legacy,
    Relay(RelayIngressOutcome),
}

/// Adapter for an authenticated encrypted private-UDP underlay. Public QUIC
/// carriers call [`RelayObjectReceiver::push_wire_datagram`] after session
/// authentication instead of identifying sessions by socket address.
#[derive(Debug)]
pub struct RelayUdpDispatch {
    receiver: RelayObjectReceiver,
    sessions_by_peer: HashMap<SocketAddr, u64>,
    controlled_announcement_sessions: HashSet<u64>,
}

impl RelayUdpDispatch {
    pub fn new(receiver: RelayObjectReceiver) -> Self {
        Self {
            receiver,
            sessions_by_peer: HashMap::new(),
            controlled_announcement_sessions: HashSet::new(),
        }
    }

    #[must_use]
    pub const fn receiver(&self) -> &RelayObjectReceiver {
        &self.receiver
    }

    pub const fn receiver_mut(&mut self) -> &mut RelayObjectReceiver {
        &mut self.receiver
    }

    /// Bind an address supplied by an authenticated encrypted private underlay
    /// to a controller-issued session identity.
    pub fn bind_authenticated_peer(
        &mut self,
        peer: SocketAddr,
        session: AuthenticatedRelayParentSession,
    ) -> Result<(), RelayIngressError> {
        if !matches!(
            (session.identity.kind, session.identity.trust_mode),
            (
                CarrierKind::PrivateUdp,
                TrustMode::PrivateAuthenticatedNetwork
            )
        ) {
            return Err(RelayIngressError::AuthenticationRequired);
        }
        if self
            .sessions_by_peer
            .get(&peer)
            .is_some_and(|existing| *existing != session.session_id)
        {
            return Err(RelayIngressError::UdpPeerConflict(peer));
        }
        let session_id = session.session_id;
        self.receiver.register_parent_session(session)?;
        self.sessions_by_peer.insert(peer, session_id);
        Ok(())
    }

    /// Bind a deterministic loopback or controlled-network endpoint and enable
    /// first-symbol announcement derivation for local qualification only.
    pub fn bind_controlled_peer(
        &mut self,
        peer: SocketAddr,
        session: ControlledRelayParentSession,
    ) -> Result<(), RelayIngressError> {
        if self
            .sessions_by_peer
            .get(&peer)
            .is_some_and(|existing| *existing != session.session_id())
        {
            return Err(RelayIngressError::UdpPeerConflict(peer));
        }
        let session_id = session.session_id();
        self.receiver.register_controlled_parent_session(session)?;
        self.sessions_by_peer.insert(peer, session_id);
        self.controlled_announcement_sessions.insert(session_id);
        Ok(())
    }

    /// A dispatch with the same parent sessions and peer bindings and no
    /// receive state. The fast plane gives each traffic class a receive task
    /// of its own, and each task its own copy, so that a video object's
    /// assembly never holds an audio object back.
    pub fn twin(&self) -> Result<Self, RelayIngressError> {
        let mut receiver = RelayObjectReceiver::new(self.receiver.config)?;
        let mut sessions = self.receiver.sessions.values().cloned().collect::<Vec<_>>();
        sessions.sort_by_key(|session| session.session_id);
        for session in sessions {
            receiver.register_parent_session_inner(session)?;
        }
        Ok(Self {
            receiver,
            sessions_by_peer: self.sessions_by_peer.clone(),
            controlled_announcement_sessions: self.controlled_announcement_sessions.clone(),
        })
    }

    /// What each bound peer may send: the checks of a symbol against its
    /// session (generation, subscription and parent path) that the fast plane
    /// makes before it forwards, without decoding.
    #[must_use]
    pub fn peer_admissions(&self) -> HashMap<SocketAddr, PeerAdmission> {
        self.sessions_by_peer
            .iter()
            .filter_map(|(peer, session_id)| {
                let session = self.receiver.sessions.get(session_id)?;
                Some((
                    *peer,
                    PeerAdmission {
                        generation: session.generation.get(),
                        subscription_id: session.subscription_id.get(),
                        parent_path: session.parent_path,
                    },
                ))
            })
            .collect()
    }

    /// Classify legacy RQD2 framing separately from RLS1 relay-session framing.
    pub fn push(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
        now_us: u64,
    ) -> Result<RelayUdpDispatchOutcome, RelayIngressError> {
        let compact = compact_object::is_compact_datagram(datagram);
        if !compact && !is_relay_session_datagram(datagram) {
            return Ok(RelayUdpDispatchOutcome::Legacy);
        }
        let Some(session_id) = self.sessions_by_peer.get(&peer).copied() else {
            self.receiver.counters.datagrams_received =
                self.receiver.counters.datagrams_received.saturating_add(1);
            self.receiver
                .record_rejection(&RelayIngressError::AuthenticationRequired, true);
            return Err(RelayIngressError::AuthenticationRequired);
        };
        if compact {
            return self
                .receiver
                .push_compact_datagram(session_id, datagram, now_us)
                .map(RelayUdpDispatchOutcome::Relay);
        }
        if !self.controlled_announcement_sessions.contains(&session_id) {
            return self
                .receiver
                .push_wire_datagram(session_id, datagram, now_us)
                .map(RelayUdpDispatchOutcome::Relay);
        }
        // A controlled session's datagram announces its object: decoded
        // once for both (fix4-video: it was decoded for the announcement and
        // again for the push).
        let decoded = self
            .receiver
            .session(session_id)
            .map(|_| ())
            .and_then(|()| {
                decode_datagram(datagram, self.receiver.config.relay_limits).map_err(Into::into)
            })
            .and_then(|symbol| {
                self.receiver
                    .ensure_controlled_announcement(session_id, &symbol, now_us)
                    .map(|()| symbol)
            });
        let symbol = match decoded {
            Ok(symbol) => symbol,
            Err(error) => {
                self.receiver.counters.datagrams_received =
                    self.receiver.counters.datagrams_received.saturating_add(1);
                self.receiver.record_rejection(&error, true);
                return Err(error);
            }
        };
        self.receiver
            .push_decoded_datagram(session_id, symbol, now_us)
            .map(RelayUdpDispatchOutcome::Relay)
    }
}

/// The session of one bound peer, as the fast plane checks it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerAdmission {
    pub generation: u64,
    pub subscription_id: u64,
    pub parent_path: ParentPath,
}

impl PeerAdmission {
    /// The checks of `validate_session_symbol`, on raw header fields.
    #[must_use]
    pub const fn admits(
        &self,
        generation: u64,
        subscription_id: u64,
        intent: raptorq_datagram_fec::MediaPathIntent,
    ) -> bool {
        self.generation == generation
            && self.subscription_id == subscription_id
            && self.parent_path.permits(intent)
    }
}

#[must_use]
pub fn is_relay_session_datagram(datagram: &[u8]) -> bool {
    datagram.starts_with(&RELAY_SESSION_DATAGRAM_MAGIC)
}

/// An RLS1 datagram or a compact (RLC1) one: what a relay session carries.
#[must_use]
pub fn is_relay_datagram(datagram: &[u8]) -> bool {
    is_relay_session_datagram(datagram) || compact_object::is_compact_datagram(datagram)
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_session::{
        encode_datagram, AdaptiveFecController, AdaptiveFecPolicy, CongestionConfig, MediaDeadline,
        MediaPriority, NodeId, RaptorQObjectEncoder, RepairRequest, RequestId,
        SecondaryRepairResponder,
    };

    fn generation() -> TopologyGeneration {
        TopologyGeneration::new(4).expect("generation")
    }

    fn subscription() -> SubscriptionId {
        SubscriptionId::new(9).expect("subscription")
    }

    fn parent(session_id: u64, peer: &str, path: ParentPath) -> AuthenticatedRelayParentSession {
        AuthenticatedRelayParentSession::new(
            session_id,
            CarrierIdentity {
                local: NodeId::new("edge-london").expect("local node"),
                peer: NodeId::new(peer).expect("peer node"),
                kind: CarrierKind::QuicDatagram,
                trust_mode: TrustMode::AuthenticatedSession,
            },
            generation(),
            subscription(),
            path,
        )
        .expect("authenticated parent")
    }

    fn private_parent(
        session_id: u64,
        peer: &str,
        path: ParentPath,
    ) -> AuthenticatedRelayParentSession {
        AuthenticatedRelayParentSession::new(
            session_id,
            CarrierIdentity {
                local: NodeId::new("edge-london").expect("local node"),
                peer: NodeId::new(peer).expect("peer node"),
                kind: CarrierKind::PrivateUdp,
                trust_mode: TrustMode::PrivateAuthenticatedNetwork,
            },
            generation(),
            subscription(),
            path,
        )
        .expect("authenticated private parent")
    }

    fn controlled_parent(
        session_id: u64,
        peer: &str,
        path: ParentPath,
    ) -> ControlledRelayParentSession {
        ControlledRelayParentSession::new(
            session_id,
            CarrierIdentity {
                local: NodeId::new("edge-london").expect("local node"),
                peer: NodeId::new(peer).expect("peer node"),
                kind: CarrierKind::PrivateUdp,
                trust_mode: TrustMode::ControlledPrivateNetwork,
            },
            generation(),
            subscription(),
            path,
        )
        .expect("controlled parent")
    }

    fn fmp4_object(sequence: u64, payload_len: usize) -> MediaObject {
        fmp4_object_of_epoch(1, sequence, payload_len)
    }

    fn fmp4_object_of_epoch(epoch: u64, sequence: u64, payload_len: usize) -> MediaObject {
        let media = (0..payload_len)
            .map(|index| ((index * 31 + 17) % 251) as u8)
            .collect::<Vec<_>>();
        let mut slot = Vec::with_capacity(16 + 9 + media.len());
        slot.extend_from_slice(b"AVFMP4S1");
        slot.extend_from_slice(&9u32.to_be_bytes());
        slot.extend_from_slice(&(media.len() as u32).to_be_bytes());
        slot.extend_from_slice(b"ftyp-moov");
        slot.extend_from_slice(&media);
        let key =
            ObjectKey::for_payload("tenant", "77", "muxed-fmp4", epoch, 3, sequence, 1, &slot)
                .expect("key");
        MediaObject::builder(key, ObjectKind::Media, slot)
            .with_keyframe(true)
            .with_metadata("container", b"fmp4".to_vec())
            .with_metadata("payload-format", b"fmp4-slot-v1".to_vec())
            .build()
            .expect("media object")
    }

    fn encoder() -> RaptorQObjectEncoder {
        let policy = AdaptiveFecPolicy {
            min_repair_symbols: 1,
            max_repair_symbols: 1,
            min_repair_ratio: 0.0,
            max_repair_ratio: 0.0,
            symbol_size: 400,
            ..AdaptiveFecPolicy::default()
        };
        RaptorQObjectEncoder::new(
            AdaptiveFecController::new(policy, CongestionConfig::default()),
            RelayLimits::default(),
        )
        .expect("encoder")
    }

    #[test]
    fn primary_sources_and_new_secondary_repairs_recover_exact_fmp4_object() {
        let object = fmp4_object(42, 12_000);
        let mut encoder = encoder();
        let encoded = encoder
            .encode_object(
                &object,
                generation(),
                subscription(),
                MediaDeadline::from_micros(2_000_000),
                MediaPriority::VideoKey,
            )
            .expect("primary symbols");
        let mut responder = SecondaryRepairResponder::new(
            &object,
            encoded.announcement.clone(),
            RelayLimits::default(),
        )
        .expect("secondary responder");
        let repairs = responder
            .fulfill(
                &RepairRequest {
                    request_id: RequestId::new(1).expect("request"),
                    generation: generation(),
                    subscription_id: subscription(),
                    key: object.key().clone(),
                    block_id: encoded.announcement.coding.block_id(),
                    next_repair_ordinal: encoded.announcement.initial_repair_symbols,
                    additional_symbols: 7,
                    deadline: encoded.announcement.deadline,
                },
                1_000,
            )
            .expect("new disjoint secondary repairs");

        let mut receiver =
            RelayObjectReceiver::new(RelayObjectReceiverConfig::default()).expect("receiver");
        receiver
            .register_parent_session(parent(1, "primary-amsterdam", ParentPath::Primary))
            .expect("primary");
        receiver
            .register_parent_session(parent(2, "secondary-paris", ParentPath::Secondary))
            .expect("secondary");
        assert_eq!(
            receiver
                .announce_object(1, encoded.announcement.clone(), 1_000)
                .expect("primary announcement"),
            RelayAnnouncementOutcome::Started
        );
        assert_eq!(
            receiver
                .announce_object(2, encoded.announcement.clone(), 1_000)
                .expect("secondary announcement"),
            RelayAnnouncementOutcome::Joined
        );

        for (index, symbol) in encoded.source_symbols.iter().enumerate() {
            if matches!(index, 1 | 5 | 9 | 13 | 17) {
                continue;
            }
            let wire = encode_datagram(symbol, RelayLimits::default()).expect("source wire");
            let outcome = receiver
                .push_wire_datagram(1, &wire, 1_100)
                .expect("primary source");
            assert!(matches!(outcome, RelayIngressOutcome::Buffered { .. }));
        }

        let mut decoded = None;
        for symbol in &repairs {
            let wire = encode_datagram(symbol, RelayLimits::default()).expect("repair wire");
            if let RelayIngressOutcome::Decoded {
                object,
                envelope,
                parent_count,
                ..
            } = receiver
                .push_wire_datagram(2, &wire, 1_200)
                .expect("secondary repair")
            {
                assert_eq!(parent_count, 2);
                assert_eq!(
                    envelope,
                    media_object::encode(&object).expect("canonical envelope")
                );
                decoded = Some(*object);
                break;
            }
        }
        assert_eq!(decoded.expect("cross-parent RaptorQ recovery"), object);
        assert_eq!(receiver.state().active_objects, 0);
        assert_eq!(receiver.state().completed_objects, 1);
        assert_eq!(receiver.state().buffered_datagrams, 0);
        let counters = receiver.snapshot().counters;
        assert_eq!(counters.repair_assisted_objects, 1);
        assert_eq!(counters.fec_recovered_objects, 1);
        assert_eq!(counters.fec_recovered_source_symbols, 5);
    }

    #[test]
    fn controlled_secondary_repairs_after_source_completion_are_duplicates_not_auth_drops() {
        let object = fmp4_object(43, 12_000);
        let mut encoder = encoder();
        let encoded = encoder
            .encode_object(
                &object,
                generation(),
                subscription(),
                MediaDeadline::from_micros(2_000_000),
                MediaPriority::VideoKey,
            )
            .expect("relay symbols");
        assert!(!encoded.repair_symbols.is_empty());

        let primary_peer: SocketAddr = "127.0.0.1:41001".parse().expect("primary peer");
        let secondary_peer: SocketAddr = "127.0.0.1:41002".parse().expect("secondary peer");
        let receiver =
            RelayObjectReceiver::new(RelayObjectReceiverConfig::default()).expect("receiver");
        let mut dispatch = RelayUdpDispatch::new(receiver);
        dispatch
            .bind_controlled_peer(
                primary_peer,
                controlled_parent(1, "contributor", ParentPath::Primary),
            )
            .expect("controlled primary");
        dispatch
            .bind_controlled_peer(
                secondary_peer,
                controlled_parent(2, "contributor", ParentPath::Secondary),
            )
            .expect("controlled secondary");

        let mut decoded = 0;
        for symbol in &encoded.source_symbols {
            let wire = encode_datagram(symbol, RelayLimits::default()).expect("source wire");
            match dispatch
                .push(primary_peer, &wire, 1_000)
                .expect("controlled primary source")
            {
                RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Buffered { .. }) => {}
                RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Decoded {
                    object: received,
                    parent_count,
                    ..
                }) => {
                    assert_eq!(*received, object);
                    assert_eq!(parent_count, 1);
                    decoded += 1;
                }
                outcome => panic!("unexpected primary outcome: {outcome:?}"),
            }
        }
        assert_eq!(decoded, 1);

        // These symbols arrive after source-only decode and even after the
        // media deadline. The matching bounded tombstone authorizes the
        // assigned secondary and classifies the no-longer-needed repairs as
        // duplicates without reopening decoder state.
        for symbol in &encoded.repair_symbols {
            let wire = encode_datagram(symbol, RelayLimits::default()).expect("repair wire");
            assert!(matches!(
                dispatch
                    .push(secondary_peer, &wire, 2_000_001)
                    .expect("late controlled secondary repair"),
                RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Duplicate { ref key, role, .. })
                    if key == object.key() && role == MediaDatagramRole::Repair
            ));
        }

        let snapshot = dispatch.receiver().snapshot();
        assert_eq!(snapshot.active_objects, 0);
        assert_eq!(snapshot.completed_objects, 1);
        assert_eq!(
            snapshot.counters.datagrams_received,
            (encoded.source_symbols.len() + encoded.repair_symbols.len()) as u64
        );
        assert_eq!(
            snapshot.counters.source_datagrams,
            encoded.source_symbols.len() as u64
        );
        assert_eq!(
            snapshot.counters.repair_datagrams,
            encoded.repair_symbols.len() as u64
        );
        assert_eq!(
            snapshot.counters.duplicate_datagrams,
            encoded.repair_symbols.len() as u64
        );
        assert_eq!(snapshot.counters.datagrams_rejected, 0);
        assert_eq!(snapshot.counters.authentication_drops, 0);
        assert_eq!(snapshot.counters.deadline_drops, 0);
        assert_eq!(snapshot.counters.repair_assisted_objects, 0);
        assert_eq!(snapshot.counters.fec_recovered_objects, 0);
        assert_eq!(snapshot.counters.fec_recovered_source_symbols, 0);
    }

    #[test]
    fn completed_object_tombstones_remain_strictly_bounded() {
        let config = RelayObjectReceiverConfig {
            max_completed_objects: 1,
            ..RelayObjectReceiverConfig::default()
        };
        let mut receiver = RelayObjectReceiver::new(config).expect("receiver");
        receiver
            .register_parent_session(parent(1, "primary-amsterdam", ParentPath::Primary))
            .expect("primary");
        let mut encoder = encoder();

        for sequence in [44, 45] {
            let object = fmp4_object(sequence, 4_000);
            let encoded = encoder
                .encode_object(
                    &object,
                    generation(),
                    subscription(),
                    MediaDeadline::from_micros(2_000_000),
                    MediaPriority::VideoKey,
                )
                .expect("relay symbols");
            receiver
                .announce_object(1, encoded.announcement, 1_000)
                .expect("announcement");
            let mut decoded = false;
            for symbol in &encoded.source_symbols {
                let wire = encode_datagram(symbol, RelayLimits::default()).expect("source wire");
                decoded |= matches!(
                    receiver
                        .push_wire_datagram(1, &wire, 1_100)
                        .expect("source"),
                    RelayIngressOutcome::Decoded { .. }
                );
            }
            assert!(decoded);
            assert!(receiver.state().completed_objects <= 1);
        }

        assert_eq!(receiver.state().completed_objects, 1);
        assert_eq!(receiver.state().active_objects, 0);
        assert_eq!(receiver.state().buffered_object_bytes, 0);
        assert_eq!(receiver.state().buffered_datagrams, 0);
    }

    #[test]
    fn the_completed_tables_are_sized_once_and_never_grow() {
        let config = RelayObjectReceiverConfig {
            max_completed_objects: 64,
            ..RelayObjectReceiverConfig::default()
        };
        let mut receiver = RelayObjectReceiver::new(config).expect("receiver");
        let capacity = |receiver: &RelayObjectReceiver| {
            (
                receiver.completed.capacity(),
                receiver.logical_keys.capacity(),
                receiver.completed_order.capacity(),
            )
        };
        let sized = capacity(&receiver);
        assert!(sized.0 >= 64 && sized.1 >= 64 && sized.2 >= 64);
        receiver
            .register_parent_session(parent(1, "primary-amsterdam", ParentPath::Primary))
            .expect("primary");
        let mut encoder = encoder();
        for sequence in 0..200 {
            let object = fmp4_object(sequence, 600);
            let encoded = encoder
                .encode_object(
                    &object,
                    generation(),
                    subscription(),
                    MediaDeadline::from_micros(2_000_000),
                    MediaPriority::VideoKey,
                )
                .expect("relay symbols");
            receiver
                .announce_object(1, encoded.announcement, 1_000)
                .expect("announcement");
            for symbol in &encoded.source_symbols {
                let wire = encode_datagram(symbol, RelayLimits::default()).expect("source wire");
                receiver
                    .push_wire_datagram(1, &wire, 1_100)
                    .expect("source");
            }
        }
        assert_eq!(receiver.state().completed_objects, 64);
        assert_eq!(receiver.logical_keys.len(), 64);
        // A table that grew would at least double its capacity (removals
        // can only leave it lower, until a rehash in place).
        let after = capacity(&receiver);
        assert!(
            after.0 <= sized.0 && after.1 <= sized.1 && after.2 == sized.2,
            "no table grew: {sized:?} -> {after:?}"
        );
    }

    #[test]
    fn conflicting_logical_identity_and_coding_are_rejected() {
        let object = fmp4_object(42, 4_000);
        let conflict = fmp4_object(42, 4_001);
        let mut encoder = encoder();
        let encoded = encoder
            .encode_object(
                &object,
                generation(),
                subscription(),
                MediaDeadline::from_micros(2_000_000),
                MediaPriority::VideoKey,
            )
            .expect("object symbols");
        let conflicting = encoder
            .encode_object(
                &conflict,
                generation(),
                subscription(),
                MediaDeadline::from_micros(2_000_000),
                MediaPriority::VideoKey,
            )
            .expect("conflicting symbols");
        let mut receiver =
            RelayObjectReceiver::new(RelayObjectReceiverConfig::default()).expect("receiver");
        receiver
            .register_parent_session(parent(1, "primary-amsterdam", ParentPath::Primary))
            .expect("primary");
        receiver
            .announce_object(1, encoded.announcement.clone(), 1_000)
            .expect("announcement");

        assert!(matches!(
            receiver.announce_object(1, conflicting.announcement.clone(), 1_000),
            Err(RelayIngressError::ObjectIdentityConflict)
        ));

        let mut hostile = conflicting.source_symbols[0].clone();
        hostile.object_key = object.key().clone();
        let wire = encode_datagram(&hostile, RelayLimits::default()).expect("hostile wire");
        assert!(matches!(
            receiver.push_wire_datagram(1, &wire, 1_100),
            Err(RelayIngressError::RelaySession(
                relay_session::Error::CodingProfileConflict
            ))
        ));
    }

    #[test]
    fn inactivity_expiry_releases_all_state() {
        let object = fmp4_object(42, 4_000);
        let mut encoder = encoder();
        let encoded = encoder
            .encode_object(
                &object,
                generation(),
                subscription(),
                MediaDeadline::from_micros(100),
                MediaPriority::VideoKey,
            )
            .expect("symbols");
        let config = RelayObjectReceiverConfig {
            object_inactivity_timeout_us: 10,
            ..RelayObjectReceiverConfig::default()
        };
        let mut receiver = RelayObjectReceiver::new(config).expect("receiver");
        receiver
            .register_parent_session(parent(1, "primary-amsterdam", ParentPath::Primary))
            .expect("primary");
        receiver
            .announce_object(1, encoded.announcement.clone(), 1)
            .expect("announcement");
        let wire = encode_datagram(&encoded.source_symbols[0], RelayLimits::default())
            .expect("source wire");
        // This node's clock is long past the deadline: no reason to refuse
        // the symbol.
        assert!(matches!(
            receiver.push_wire_datagram(1, &wire, 100_000),
            Ok(RelayIngressOutcome::Buffered { .. })
        ));
        assert_eq!(receiver.expire(100_009).objects, 0);
        let expiry = receiver.expire(100_010);
        assert_eq!(expiry.objects, 1);
        assert!(expiry.released_object_bytes > 0);
        assert_eq!(expiry.released_datagrams, 1);
        assert_eq!(receiver.state().active_objects, 0);
        assert_eq!(receiver.state().buffered_object_bytes, 0);
        assert_eq!(receiver.snapshot().counters.deadline_drops, 0);
        assert_eq!(receiver.snapshot().counters.expired_objects, 1);
    }

    #[test]
    fn configured_object_and_datagram_bounds_are_enforced() {
        let object = fmp4_object(42, 4_000);
        let other = fmp4_object(43, 4_000);
        let mut encoder = encoder();
        let first = encoder
            .encode_object(
                &object,
                generation(),
                subscription(),
                MediaDeadline::from_micros(2_000_000),
                MediaPriority::VideoKey,
            )
            .expect("first");
        let second = encoder
            .encode_object(
                &other,
                generation(),
                subscription(),
                MediaDeadline::from_micros(2_000_000),
                MediaPriority::VideoKey,
            )
            .expect("second");
        let config = RelayObjectReceiverConfig {
            max_active_objects: 1,
            max_datagrams_per_object: 1,
            ..RelayObjectReceiverConfig::default()
        };
        let mut receiver = RelayObjectReceiver::new(config).expect("receiver");
        receiver
            .register_parent_session(parent(1, "primary-amsterdam", ParentPath::Primary))
            .expect("primary");
        receiver
            .announce_object(1, first.announcement.clone(), 1_000)
            .expect("first announcement");
        assert!(matches!(
            receiver.announce_object(1, second.announcement, 1_000),
            Err(RelayIngressError::LimitExceeded {
                field: "active_objects",
                ..
            })
        ));
        for (index, symbol) in first.source_symbols.iter().take(2).enumerate() {
            let wire = encode_datagram(symbol, RelayLimits::default()).expect("wire");
            let result = receiver.push_wire_datagram(1, &wire, 1_100);
            if index == 0 {
                assert!(matches!(result, Ok(RelayIngressOutcome::Buffered { .. })));
            } else {
                assert!(matches!(
                    result,
                    Err(RelayIngressError::DatagramLimitExceeded)
                ));
            }
        }
    }

    #[test]
    fn udp_dispatch_keeps_legacy_framing_and_requires_authenticated_binding() {
        let peer: SocketAddr = "127.0.0.1:40000".parse().expect("peer");
        let receiver =
            RelayObjectReceiver::new(RelayObjectReceiverConfig::default()).expect("receiver");
        let mut dispatch = RelayUdpDispatch::new(receiver);
        assert!(matches!(
            dispatch.push(peer, b"RQD2 legacy", 1),
            Ok(RelayUdpDispatchOutcome::Legacy)
        ));
        assert!(matches!(
            dispatch.push(peer, b"RLS1 relay", 1),
            Err(RelayIngressError::AuthenticationRequired)
        ));
        dispatch
            .bind_authenticated_peer(
                peer,
                private_parent(1, "primary-amsterdam", ParentPath::Primary),
            )
            .expect("authenticated UDP binding");
        assert!(matches!(
            dispatch.push(peer, b"RLS1 relay", 1),
            Err(RelayIngressError::RelaySession(_))
        ));
        let snapshot = dispatch.receiver().snapshot();
        assert_eq!(snapshot.counters.datagrams_received, 2);
        assert_eq!(snapshot.counters.datagrams_rejected, 2);
        assert_eq!(snapshot.counters.authentication_drops, 1);
        assert_eq!(snapshot.counters.duplicate_datagrams, 0);
    }

    /// An opaque 1 ms audio part as av-contrib publishes it.
    fn opaque_audio(sequence: u64) -> MediaObject {
        use media_object::{ClockConfidence, ClockTimestamp, Stage, StageTimestamp};
        let payload = (0..56)
            .map(|index| ((index * 31 + sequence as usize) % 251) as u8)
            .collect::<Vec<_>>();
        let key = ObjectKey::for_payload(
            "default",
            "3001",
            "opaque-audio",
            1_763_812_345,
            0,
            sequence,
            1,
            &payload,
        )
        .expect("key");
        let stamp = |ns: i64| {
            ClockTimestamp::new(
                ns,
                "system:realtime:av-contrib",
                ClockConfidence::estimated(1_000),
            )
            .expect("stamp")
        };
        let published = 1_763_812_345_000_000_000 + sequence as i64 * 1_000_000;
        MediaObject::builder(key, ObjectKind::Media, payload)
            .with_deadline(stamp(published + 5_000_000_000))
            .with_stage_timestamp(StageTimestamp::new(
                Stage::Packaged,
                stamp(published - 40_000),
            ))
            .with_stage_timestamp(StageTimestamp::new(Stage::Published, stamp(published)))
            .with_metadata("container", b"opaque".to_vec())
            .with_metadata("duration-us", b"1000".to_vec())
            .with_metadata("payload-format", b"opaque-part-v1".to_vec())
            .with_metadata("hls-msn", (sequence / 1_000).to_string().into_bytes())
            .with_metadata("hls-part", (sequence % 1_000).to_string().into_bytes())
            .build()
            .expect("object")
    }

    fn compact_wire(
        object: &MediaObject,
        role: u8,
        subscription_id: u64,
        sequence: u32,
    ) -> Vec<u8> {
        let key = object.key();
        let id = compact_object::descriptor_id(object).expect("compactable");
        let slim = compact_object::encode_slim(id, object).expect("slim");
        compact_object::encode_datagram(
            &compact_object::CompactHeader {
                generation: generation().get(),
                subscription_id,
                deadline_us: 4_000_000_000_000_000,
                role,
                kind: 3,
                path: role,
                tenant: "default",
                stream: "3001",
                track: "opaque-audio",
                epoch: key.epoch(),
                group: key.group(),
                object: key.object(),
                version: key.version(),
                packet_sequence: sequence,
                esi: u32::from(role),
            },
            &slim,
        )
        .expect("datagram")
    }

    /// A compact datagram rebuilds the exact envelope of its whole object
    /// once a whole part of the stream taught the receiver the descriptor;
    /// before that it is admitted (for forwarding) but not decoded; a copy
    /// of a decoded part is a duplicate; and a datagram of another
    /// subscription is refused like an RLS1 symbol.
    #[test]
    fn compact_datagrams_rebuild_the_whole_envelope_after_a_whole_part() {
        let peer: SocketAddr = "127.0.0.1:40002".parse().expect("peer");
        let receiver =
            RelayObjectReceiver::new(RelayObjectReceiverConfig::default()).expect("receiver");
        let mut dispatch = RelayUdpDispatch::new(receiver);
        dispatch
            .bind_controlled_peer(
                peer,
                controlled_parent(1, "contributor", ParentPath::Primary),
            )
            .expect("binding");
        let secondary: SocketAddr = "127.0.0.1:40003".parse().expect("peer");
        dispatch
            .bind_controlled_peer(
                secondary,
                controlled_parent(2, "contributor-secondary", ParentPath::Secondary),
            )
            .expect("binding");
        let now = 1_000;
        // Before any whole part: admitted, not decoded.
        let early = compact_wire(&opaque_audio(41), 0, subscription().get(), 1);
        assert!(matches!(
            dispatch.push(peer, &early, now),
            Ok(RelayUdpDispatchOutcome::Relay(
                RelayIngressOutcome::Buffered { .. }
            ))
        ));
        // A whole part (RLS1), as the contributor sends one every 100 ms.
        let whole = opaque_audio(42);
        let encoded = encoder()
            .encode_object(
                &whole,
                generation(),
                subscription(),
                MediaDeadline::from_micros(4_000_000_000_000_000),
                MediaPriority::Audio,
            )
            .expect("encode");
        let mut decoded = false;
        for symbol in &encoded.source_symbols {
            let wire = encode_datagram(symbol, RelayLimits::default()).unwrap();
            decoded = matches!(
                dispatch.push(peer, &wire, now),
                Ok(RelayUdpDispatchOutcome::Relay(
                    RelayIngressOutcome::Decoded { .. }
                ))
            );
        }
        assert!(decoded, "the whole part decodes");
        for sequence in [43_u64, 999, 1_000, 123_456] {
            let object = opaque_audio(sequence);
            let source = compact_wire(&object, 0, subscription().get(), sequence as u32 * 2);
            assert!(source.len() < 56 + 80, "{} B", source.len());
            let Ok(RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Decoded {
                object: rebuilt,
                envelope,
                ..
            })) = dispatch.push(peer, &source, now)
            else {
                panic!("part {sequence} decodes");
            };
            assert_eq!(envelope, media_object::encode(&object).unwrap());
            assert_eq!(*rebuilt, object);
            let copy = compact_wire(&object, 1, subscription().get(), sequence as u32 * 2 + 1);
            assert!(matches!(
                dispatch.push(secondary, &copy, now),
                Ok(RelayUdpDispatchOutcome::Relay(
                    RelayIngressOutcome::Duplicate { .. }
                ))
            ));
        }
        let foreign = compact_wire(&opaque_audio(50), 0, subscription().get() + 1, 7);
        assert!(matches!(
            dispatch.push(peer, &foreign, now),
            Err(RelayIngressError::UnauthorizedParent)
        ));
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(counters.compact_decoded_objects, 4);
        assert_eq!(counters.compact_unknown_descriptor, 1);
        assert_eq!(counters.compact_descriptors_learned, 1);
        assert_eq!(counters.duplicate_datagrams, 4);
        assert_eq!(counters.authentication_drops, 1);
    }

    /// A dispatch whose controlled sessions 1.. have `paths`, each at a peer
    /// address of its own.
    fn dispatch_with(paths: &[ParentPath]) -> (RelayUdpDispatch, Vec<SocketAddr>) {
        let receiver =
            RelayObjectReceiver::new(RelayObjectReceiverConfig::default()).expect("receiver");
        let mut dispatch = RelayUdpDispatch::new(receiver);
        let mut peers = Vec::new();
        for (index, path) in paths.iter().enumerate() {
            let peer: SocketAddr = format!("127.0.0.1:{}", 41_100 + index)
                .parse()
                .expect("peer");
            dispatch
                .bind_controlled_peer(
                    peer,
                    controlled_parent(index as u64 + 1, &format!("parent-{index}"), *path),
                )
                .expect("binding");
            peers.push(peer);
        }
        (dispatch, peers)
    }

    /// The RLS1 wire symbols of an object, sources then repair: an audio
    /// part in one symbol, as the contributor sends it, anything else in
    /// symbols of 400 bytes.
    fn wire_symbols(
        object: &MediaObject,
        deadline_us: u64,
        priority: MediaPriority,
        repair: u32,
    ) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let policy = AdaptiveFecPolicy {
            min_repair_symbols: repair,
            max_repair_symbols: repair,
            min_repair_ratio: 0.0,
            max_repair_ratio: 0.0,
            symbol_size: if matches!(priority, MediaPriority::Audio) {
                1_200
            } else {
                400
            },
            ..AdaptiveFecPolicy::default()
        };
        let encoded = RaptorQObjectEncoder::new(
            AdaptiveFecController::new(policy, CongestionConfig::default()),
            RelayLimits::default(),
        )
        .expect("encoder")
        .encode_object(
            object,
            generation(),
            subscription(),
            MediaDeadline::from_micros(deadline_us),
            priority,
        )
        .expect("symbols");
        let wire = |symbols: &[relay_session::RelayDatagram]| {
            symbols
                .iter()
                .map(|symbol| encode_datagram(symbol, RelayLimits::default()).expect("wire"))
                .collect::<Vec<_>>()
        };
        (wire(&encoded.source_symbols), wire(&encoded.repair_symbols))
    }

    fn decoded(outcome: Result<RelayUdpDispatchOutcome, RelayIngressError>) -> bool {
        match outcome.expect("admitted") {
            RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Decoded { .. }) => true,
            RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Buffered { .. }) => false,
            outcome => panic!("unexpected outcome: {outcome:?}"),
        }
    }

    fn is_duplicate(outcome: Result<RelayUdpDispatchOutcome, RelayIngressError>) -> bool {
        matches!(
            outcome,
            Ok(RelayUdpDispatchOutcome::Relay(
                RelayIngressOutcome::Duplicate { .. }
            ))
        )
    }

    const FAR_DEADLINE_US: u64 = 4_000_000_000_000_000;

    /// Two contributors at a first forwarder (and the two parents of an
    /// edge) are both promoted sessions: the symbols of one object may come
    /// first from either.
    #[test]
    fn two_promoted_sessions_assemble_one_object() {
        let (mut dispatch, peers) = dispatch_with(&[ParentPath::PromotedSecondary; 2]);
        let object = fmp4_object(42, 12_000);
        let (sources, _) = wire_symbols(&object, FAR_DEADLINE_US, MediaPriority::VideoKey, 1);
        assert!(sources.len() > 20);
        let mut complete = false;
        for (index, symbol) in sources.iter().enumerate() {
            assert!(!complete);
            complete = match dispatch
                .push(peers[index % 2], symbol, 1_000)
                .expect("a symbol of either session")
            {
                RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Decoded {
                    object: received,
                    parent_count,
                    accepted_datagrams,
                    ..
                }) => {
                    assert_eq!(*received, object);
                    assert_eq!(parent_count, 2);
                    assert_eq!(accepted_datagrams, sources.len());
                    true
                }
                RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Buffered { .. }) => false,
                outcome => panic!("unexpected outcome: {outcome:?}"),
            };
        }
        assert!(complete);
        // The same symbol from the other session is a duplicate, before and
        // after the object completes.
        assert!(is_duplicate(dispatch.push(peers[1], &sources[0], 1_000)));
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(counters.datagrams_rejected, 0);
        assert_eq!(counters.rejected_by_kind, RejectedByKind::default());
    }

    #[test]
    fn the_copy_of_a_whole_part_may_come_from_the_other_session() {
        let (mut dispatch, peers) = dispatch_with(&[ParentPath::PromotedSecondary; 2]);
        let part = opaque_audio(42);
        let (sources, copies) = wire_symbols(&part, FAR_DEADLINE_US, MediaPriority::Audio, 2);
        assert_eq!(sources.len(), 1, "a one-symbol part");
        assert!(decoded(dispatch.push(peers[0], &sources[0], 1_000)));
        // The other contributor's source, and its copies of the part.
        assert!(is_duplicate(dispatch.push(peers[1], &sources[0], 1_000)));
        for copy in &copies {
            assert!(is_duplicate(dispatch.push(peers[1], copy, 1_000)));
        }
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(counters.decoded_objects, 1);
        assert_eq!(counters.duplicate_datagrams, 3);
        assert_eq!(counters.datagrams_rejected, 0);
    }

    /// An edge's two lease parents are both promoted. The primary sends a
    /// video object in full and one source symbol is lost; the secondary
    /// sends repair. The edge's tee rebuilds the object from both (it used
    /// to reject every symbol of the second parent).
    #[test]
    fn an_edge_decodes_a_video_object_from_its_second_parents_repair() {
        let (mut dispatch, peers) = dispatch_with(&[ParentPath::PromotedSecondary; 2]);
        let object = fmp4_object(7, 12_000);
        let (sources, repair) = wire_symbols(&object, FAR_DEADLINE_US, MediaPriority::VideoKey, 4);
        for (index, symbol) in sources.iter().enumerate() {
            if index == 3 {
                continue;
            }
            assert!(!decoded(dispatch.push(peers[0], symbol, 1_000)));
        }
        let mut complete = false;
        for symbol in &repair {
            if complete {
                assert!(is_duplicate(dispatch.push(peers[1], symbol, 1_100)));
            } else {
                complete = decoded(dispatch.push(peers[1], symbol, 1_100));
            }
        }
        assert!(complete, "the second parent's repair completes the object");
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(counters.fec_recovered_objects, 1);
        assert_eq!(counters.fec_recovered_source_symbols, 1);
        assert_eq!(counters.repair_assisted_objects, 1);
        assert_eq!(counters.datagrams_rejected, 0);
        assert_eq!(counters.rejected_by_kind, RejectedByKind::default());
    }

    /// A ranked parent is a third promoted session; four parents may bring
    /// symbols of one object, and a fifth is refused and counted by kind.
    #[test]
    fn a_third_promoted_parent_joins() {
        let (mut dispatch, peers) = dispatch_with(&[ParentPath::PromotedSecondary; 5]);
        let object = fmp4_object(9, 12_000);
        let (sources, _) = wire_symbols(&object, FAR_DEADLINE_US, MediaPriority::VideoKey, 1);
        for (index, symbol) in sources.iter().take(4).enumerate() {
            assert!(!decoded(dispatch.push(peers[index], symbol, 1_000)));
        }
        assert!(matches!(
            dispatch.push(peers[4], &sources[4], 1_000),
            Err(RelayIngressError::ParentLimitExceeded)
        ));
        let mut parents = 0;
        for (index, symbol) in sources.iter().enumerate().skip(4) {
            if let RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Decoded {
                parent_count,
                ..
            }) = dispatch
                .push(peers[index % 3], symbol, 1_000)
                .expect("a symbol of an admitted parent")
            {
                parents = parent_count;
            }
        }
        assert_eq!(parents, 4);
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(counters.datagrams_rejected, 1);
        assert_eq!(
            counters
                .rejected_by_kind
                .get(RejectionKind::ParentLimitExceeded),
            1
        );
        assert_eq!(
            RejectionKind::ParentLimitExceeded.as_str(),
            "parent_limit_exceeded"
        );
    }

    #[test]
    fn two_primaries_still_conflict() {
        let object = fmp4_object(11, 4_000);
        let (sources, repair) = wire_symbols(&object, FAR_DEADLINE_US, MediaPriority::VideoKey, 2);
        // Two sessions on the primary path.
        let (mut dispatch, peers) = dispatch_with(&[ParentPath::Primary; 2]);
        assert!(!decoded(dispatch.push(peers[0], &sources[0], 1_000)));
        assert!(matches!(
            dispatch.push(peers[1], &sources[1], 1_000),
            Err(RelayIngressError::ParentRoleConflict)
        ));
        // Two on the repair path.
        let (mut dispatch, peers) = dispatch_with(&[
            ParentPath::Primary,
            ParentPath::Secondary,
            ParentPath::Secondary,
        ]);
        assert!(!decoded(dispatch.push(peers[0], &sources[0], 1_000)));
        assert!(!decoded(dispatch.push(peers[1], &repair[0], 1_000)));
        assert!(matches!(
            dispatch.push(peers[2], &repair[1], 1_000),
            Err(RelayIngressError::ParentRoleConflict)
        ));
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(
            counters
                .rejected_by_kind
                .get(RejectionKind::ParentRoleConflict),
            1
        );
        // A promoted session beside a primary is no conflict.
        let (mut dispatch, peers) =
            dispatch_with(&[ParentPath::Primary, ParentPath::PromotedSecondary]);
        assert!(!decoded(dispatch.push(peers[0], &sources[0], 1_000)));
        assert!(!decoded(dispatch.push(peers[1], &sources[1], 1_000)));
    }

    #[test]
    fn a_part_seen_whole_is_not_decoded_again_compact() {
        let (mut dispatch, peers) = dispatch_with(&[ParentPath::PromotedSecondary; 2]);
        let part = opaque_audio(42);
        let (sources, _) = wire_symbols(&part, FAR_DEADLINE_US, MediaPriority::Audio, 0);
        let mut complete = false;
        for symbol in &sources {
            complete = decoded(dispatch.push(peers[0], symbol, 1_000));
        }
        assert!(complete);
        // The other contributor sent the same part compact.
        for (role, sequence) in [(0, 1), (1, 2)] {
            let compact = compact_wire(&part, role, subscription().get(), sequence);
            let Ok(RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Duplicate { key, .. })) =
                dispatch.push(peers[1], &compact, 1_000)
            else {
                panic!("the compact copy of a whole part is a duplicate");
            };
            assert_eq!((key.stream(), key.object()), ("3001", 42));
        }
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(counters.decoded_objects, 1);
        assert_eq!(counters.compact_decoded_objects, 0);
        assert_eq!(counters.duplicate_datagrams, 2);
        assert_eq!(counters.datagrams_rejected, 0);
    }

    #[test]
    fn a_part_seen_compact_is_not_decoded_again_whole() {
        let (mut dispatch, peers) = dispatch_with(&[ParentPath::PromotedSecondary; 2]);
        // A whole part teaches the descriptor.
        for symbol in &wire_symbols(&opaque_audio(41), FAR_DEADLINE_US, MediaPriority::Audio, 0).0 {
            dispatch.push(peers[0], symbol, 1_000).expect("whole part");
        }
        let part = opaque_audio(42);
        let compact = compact_wire(&part, 0, subscription().get(), 1);
        assert!(decoded(dispatch.push(peers[0], &compact, 1_000)));
        // The other contributor sent part 42 whole (its first part after a
        // restart): every symbol and copy is a duplicate, and no assembly
        // is opened for it.
        let (sources, copies) = wire_symbols(&part, FAR_DEADLINE_US, MediaPriority::Audio, 2);
        for symbol in sources.iter().chain(&copies) {
            assert!(is_duplicate(dispatch.push(peers[1], symbol, 1_000)));
        }
        assert_eq!(dispatch.receiver().state().active_objects, 0);
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(counters.decoded_objects, 2);
        assert_eq!(counters.compact_decoded_objects, 1);
        assert_eq!(counters.datagrams_rejected, 0);
        // Other bytes under the number of a part that came compact are a
        // conflict, as they are against a whole part.
        let mut other = opaque_audio(43);
        let compact = compact_wire(&other, 0, subscription().get(), 5);
        assert!(decoded(dispatch.push(peers[0], &compact, 1_000)));
        other = MediaObject::builder(
            ObjectKey::for_payload(
                "default",
                "3001",
                "opaque-audio",
                other.key().epoch(),
                0,
                43,
                1,
                b"other bytes",
            )
            .expect("key"),
            ObjectKind::Media,
            b"other bytes".to_vec(),
        )
        .build()
        .expect("object");
        let (conflicting, _) = wire_symbols(&other, FAR_DEADLINE_US, MediaPriority::Audio, 0);
        assert!(matches!(
            dispatch.push(peers[1], &conflicting[0], 1_000),
            Err(RelayIngressError::ObjectIdentityConflict)
        ));
    }

    /// The published time of `opaque_audio(sequence)`, in µs.
    fn opaque_published_us(sequence: u64) -> u64 {
        1_763_812_345_000_000 + sequence * 1_000
    }

    /// A stream that arrives late as a whole (a source clock behind this
    /// node's, a paused DAW, a slow path) is taken, not shed: a deadline is
    /// a point on the stream's timeline.
    #[test]
    fn a_stream_behind_the_wall_clock_is_not_expired() {
        let (mut dispatch, peers) = dispatch_with(&[ParentPath::PromotedSecondary; 2]);
        let hour_us = 3_600_000_000;
        for sequence in 0..50 {
            let part = opaque_audio(sequence);
            let published_us = opaque_published_us(sequence);
            let now_us = published_us + hour_us;
            let (sources, _) =
                wire_symbols(&part, published_us + 1_000_000, MediaPriority::Audio, 0);
            let mut complete = false;
            for symbol in &sources {
                complete = decoded(dispatch.push(peers[0], symbol, now_us));
            }
            assert!(complete, "part {sequence} is an hour late and is taken");
        }
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(counters.decoded_objects, 50);
        assert_eq!(counters.deadline_drops, 0);
        assert_eq!(counters.datagrams_rejected, 0);
        // What a wall-clock deadline would have shed is counted.
        assert_eq!(counters.wall_late_parts, 50);
        assert_eq!(counters.wall_late_max_us, hour_us);
    }

    #[test]
    fn a_part_a_budget_behind_the_head_is_dropped() {
        let (mut dispatch, peers) = dispatch_with(&[ParentPath::PromotedSecondary; 2]);
        let budget_us = dispatch.receiver().config().media_deadline_us;
        assert_eq!(budget_us, 1_000_000);
        let push = |dispatch: &mut RelayUdpDispatch, sequence: u64, peer: usize| {
            let published_us = opaque_published_us(sequence);
            let (sources, _) = wire_symbols(
                &opaque_audio(sequence),
                published_us + budget_us,
                MediaPriority::Audio,
                0,
            );
            // This node's clock is in step with the stream: nothing is
            // late by the wall clock.
            dispatch.push(peers[peer], &sources[0], published_us + 2_000)
        };
        // The first part that decodes starts the stream's clock; the next
        // parts move its head.
        assert!(decoded(push(&mut dispatch, 0, 0)));
        assert!(decoded(push(&mut dispatch, 1, 0)));
        // 400 ms of parts lost, then the stream goes on: the head follows
        // in steps of half a budget at most.
        for sequence in [400, 800, 1_200, 1_600, 2_000, 2_400, 2_800, 3_000] {
            assert!(decoded(push(&mut dispatch, sequence, 0)), "part {sequence}");
        }
        // Half a second behind the head: taken (a repair, the other
        // contributor). A budget behind, or more: dropped, from either
        // session.
        assert!(decoded(push(&mut dispatch, 2_500, 1)));
        assert!(decoded(push(&mut dispatch, 2_001, 1)));
        for (sequence, peer) in [(2_000 - 1, 0), (1_500, 1), (2, 1)] {
            assert!(
                matches!(
                    push(&mut dispatch, sequence, peer),
                    Err(RelayIngressError::DeadlineExpired)
                ),
                "part {sequence}"
            );
        }
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(counters.deadline_drops, 3);
        assert_eq!(counters.datagrams_rejected, 3);
        assert_eq!(counters.rejected_by_kind.get(RejectionKind::StaleMedia), 3);
        assert_eq!(counters.wall_late_parts, 0);
        assert_eq!(dispatch.receiver().state().active_objects, 0);
    }

    /// An assembly that waits for symbols ends when its stream's head has
    /// passed it by the budget, whatever this node's clock says.
    #[test]
    fn an_open_assembly_ends_when_the_head_passes_it() {
        let (mut dispatch, peers) = dispatch_with(&[ParentPath::PromotedSecondary; 2]);
        let base_us = 1_000_000_000;
        let (first, _) = wire_symbols(&fmp4_object(0, 4_000), base_us, MediaPriority::VideoKey, 0);
        let mut complete = false;
        for symbol in &first {
            complete = decoded(dispatch.push(peers[0], symbol, 5));
        }
        assert!(complete);
        // One symbol of the next object comes; the rest is lost.
        let (held, _) = wire_symbols(
            &fmp4_object(1, 4_000),
            base_us + 40_000,
            MediaPriority::VideoKey,
            0,
        );
        assert!(!decoded(dispatch.push(peers[0], &held[0], 6)));
        assert_eq!(dispatch.receiver().state().active_objects, 1);
        // The stream goes on for 900 ms: the assembly still waits.
        let (later, _) = wire_symbols(
            &fmp4_object(2, 4_000),
            base_us + 940_000,
            MediaPriority::VideoKey,
            0,
        );
        for symbol in &later {
            dispatch.push(peers[0], symbol, 7).expect("later object");
        }
        assert_eq!(dispatch.receiver_mut().expire(8).objects, 0);
        assert!(!decoded(dispatch.push(peers[1], &held[1], 9)));
        // A budget past its deadline it ends, and its late symbols are
        // dropped.
        let (latest, _) = wire_symbols(
            &fmp4_object(3, 4_000),
            base_us + 1_040_000,
            MediaPriority::VideoKey,
            0,
        );
        for symbol in &latest {
            dispatch.push(peers[0], symbol, 10).expect("latest object");
        }
        let expiry = dispatch.receiver_mut().expire(11);
        assert_eq!(expiry.objects, 1);
        assert_eq!(expiry.released_datagrams, 2);
        assert_eq!(dispatch.receiver().state().active_objects, 0);
        assert!(matches!(
            dispatch.push(peers[1], &held[2], 12),
            Err(RelayIngressError::DeadlineExpired)
        ));
        assert_eq!(dispatch.receiver().state().active_objects, 0);
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(counters.expired_objects, 1);
        assert_eq!(counters.deadline_drops, 1);
    }

    /// A symbol's deadline is its own claim. One that its stream has left
    /// behind is dropped, and the object it names goes on assembling until
    /// the stream leaves the object's announced deadline behind: a corrupt
    /// deadline that passed the UDP checksum costs the datagram, not the
    /// object.
    #[test]
    fn a_stale_symbol_does_not_end_an_assembly_its_announcement_keeps_fresh() {
        let mut receiver =
            RelayObjectReceiver::new(RelayObjectReceiverConfig::default()).expect("receiver");
        receiver
            .register_parent_session(parent(1, "primary-amsterdam", ParentPath::Primary))
            .expect("primary");
        let mut encoder = encoder();
        let mut encode = |sequence: u64| {
            encoder
                .encode_object(
                    &fmp4_object(sequence, 4_000),
                    generation(),
                    subscription(),
                    MediaDeadline::from_micros(1_000_000_000 + sequence * 40_000),
                    MediaPriority::VideoKey,
                )
                .expect("relay symbols")
        };
        let wire = |symbol: &relay_session::RelayDatagram| {
            encode_datagram(symbol, RelayLimits::default()).expect("wire")
        };
        // Two objects decode: the stream's clock has its epoch and its head.
        for sequence in 0..2 {
            let encoded = encode(sequence);
            receiver
                .announce_object(1, encoded.announcement, 1_000)
                .expect("announcement");
            let mut complete = false;
            for symbol in &encoded.source_symbols {
                complete = matches!(
                    receiver.push_wire_datagram(1, &wire(symbol), 1_100),
                    Ok(RelayIngressOutcome::Decoded { .. })
                );
            }
            assert!(complete, "object {sequence}");
        }
        let encoded = encode(2);
        assert!(encoded.source_symbols.len() >= 3);
        receiver
            .announce_object(1, encoded.announcement.clone(), 1_200)
            .expect("announcement");
        assert!(matches!(
            receiver.push_wire_datagram(1, &wire(&encoded.source_symbols[0]), 1_200),
            Ok(RelayIngressOutcome::Buffered { .. })
        ));
        let mut corrupt = encoded.source_symbols[1].clone();
        corrupt.deadline = MediaDeadline::from_micros(1);
        assert!(matches!(
            receiver.push_wire_datagram(1, &wire(&corrupt), 1_300),
            Err(RelayIngressError::DeadlineExpired)
        ));
        assert_eq!(receiver.state().active_objects, 1);
        let mut complete = false;
        for symbol in &encoded.source_symbols[1..] {
            complete = matches!(
                receiver.push_wire_datagram(1, &wire(symbol), 1_400),
                Ok(RelayIngressOutcome::Decoded { .. })
            );
        }
        assert!(complete, "the object decodes");
        let counters = receiver.snapshot().counters;
        assert_eq!(counters.expired_objects, 0);
        assert_eq!(counters.deadline_drops, 1);
    }

    /// Only an object that decodes moves a stream to a later epoch; what
    /// then comes of the epoch before is refused and counted, and nothing
    /// of the new epoch is dropped for it.
    #[test]
    fn a_datagram_of_an_epoch_its_stream_left_is_refused() {
        let (mut dispatch, peers) = dispatch_with(&[ParentPath::PromotedSecondary; 2]);
        let symbols = |epoch: u64, sequence: u64, deadline_us: u64| {
            wire_symbols(
                &fmp4_object_of_epoch(epoch, sequence, 1_500),
                deadline_us,
                MediaPriority::VideoKey,
                0,
            )
            .0
        };
        let old = symbols(41, 0, 9_000_000_000);
        for symbol in &old {
            dispatch.push(peers[0], symbol, 1).expect("the first epoch");
        }
        // A symbol that claims a later epoch does not retire the running
        // one: an object of the running epoch still decodes.
        let new = symbols(42, 0, 1_000_000);
        assert!(!decoded(dispatch.push(peers[1], &new[0], 2)));
        let mut complete = false;
        for symbol in &symbols(41, 1, 9_000_040_000) {
            complete = decoded(dispatch.push(peers[0], symbol, 3));
        }
        assert!(complete);
        // The new epoch's object decodes (its deadlines start lower: a new
        // session of the source). The stream has left epoch 41.
        for symbol in &new[1..] {
            complete = decoded(dispatch.push(peers[1], symbol, 4));
        }
        assert!(complete);
        let stale = symbols(41, 2, 9_000_080_000);
        assert!(matches!(
            dispatch.push(peers[0], &stale[0], 5),
            Err(RelayIngressError::StaleEpoch)
        ));
        let mut complete = false;
        for symbol in &symbols(42, 1, 1_040_000) {
            complete = decoded(dispatch.push(peers[1], symbol, 6));
        }
        assert!(complete);
        let counters = dispatch.receiver().snapshot().counters;
        assert_eq!(counters.rejected_by_kind.get(RejectionKind::StaleEpoch), 1);
        assert_eq!(counters.deadline_drops, 0);
        assert_eq!(counters.datagrams_rejected, 1);
        assert_eq!(dispatch.receiver().state().active_objects, 0);
    }
}
