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

use media_object::{MediaObject, ObjectKey, ObjectKind, PayloadHash};
use raptorq_datagram_fec::{DatagramFecHeader, MediaDatagramRole, MediaObjectKind};
use relay_session::{
    decode_datagram, CarrierIdentity, CarrierKind, ObjectAnnouncement, ObjectAssembler, ParentPath,
    RelayLimits, SubscriptionId, TopologyGeneration, TrustMode,
};

/// Wire prefix used by `RelaySession`'s RLS1 envelope.
pub const RELAY_SESSION_DATAGRAM_MAGIC: [u8; 4] = *b"RLS1";

const HARD_MAX_PARENT_SESSIONS: usize = 65_536;
const HARD_MAX_ACTIVE_OBJECTS: usize = 65_536;
const HARD_MAX_PARENTS_PER_OBJECT: usize = 2;
const HARD_MAX_DATAGRAMS_PER_OBJECT: usize = 65_536;
const HARD_MAX_BUFFERED_DATAGRAMS: usize = 262_144;
const HARD_MAX_BUFFERED_OBJECT_BYTES: usize = 512 * 1024 * 1024;
const HARD_MAX_COMPLETED_OBJECTS: usize = 65_536;

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
}

impl Default for RelayObjectReceiverConfig {
    fn default() -> Self {
        Self {
            relay_limits: RelayLimits::default(),
            max_parent_sessions: 4_096,
            max_active_objects: 4_096,
            max_parents_per_object: 2,
            max_datagrams_per_object: 32_768,
            max_buffered_datagrams: 131_072,
            max_buffered_object_bytes: 128 * 1024 * 1024,
            max_completed_objects: 4_096,
            object_inactivity_timeout_us: 10_000_000,
            expiry_scan_interval_us: 250_000,
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
    },
    Decoded {
        object: Box<MediaObject>,
        /// Role of the symbol that completed the object. Relay executables use
        /// this to preserve source/repair observability while forwarding the
        /// admitted wire datagram without changing its coding geometry.
        role: MediaDatagramRole,
        parent_count: usize,
        accepted_datagrams: usize,
        /// SHA-256 over the exact canonical envelope reconstructed by RaptorQ.
        /// The announcement schema still needs a matching field for prior
        /// end-to-end envelope binding.
        envelope_hash: PayloadHash,
    },
    Duplicate {
        key: ObjectKey,
        /// The authenticated symbol remains useful to a downstream child even
        /// when this relay has already completed the object. Preserving the
        /// role lets a warm relay forward late repair without reopening local
        /// decoder state.
        role: MediaDatagramRole,
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
    pub repaired_objects: u64,
    pub expired_objects: u64,
    pub conflict_drops: u64,
    pub authentication_drops: u64,
    pub deadline_drops: u64,
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
    DeadlineExpired,
    DatagramLimitExceeded,
    BufferedDatagramLimitExceeded,
    SymbolReplayConflict,
    RelaySession(relay_session::Error),
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
            Self::DeadlineExpired => formatter.write_str("media object deadline has expired"),
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
    repair_symbols: usize,
    reserved_bytes: usize,
    last_activity_us: u64,
}

#[derive(Debug)]
struct CompletedObject {
    announcement: ObjectAnnouncement,
    parent_sessions: HashSet<u64>,
}

/// Bounded cross-parent object receiver.
#[derive(Debug)]
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
}

impl RelayObjectReceiver {
    /// Construct a receiver after validating every configured ceiling.
    pub fn new(config: RelayObjectReceiverConfig) -> Result<Self, RelayIngressError> {
        config.validate()?;
        Ok(Self {
            config,
            sessions: HashMap::new(),
            objects: HashMap::new(),
            logical_keys: HashMap::new(),
            completed: HashMap::new(),
            completed_order: VecDeque::new(),
            buffered_object_bytes: 0,
            buffered_datagrams: 0,
            counters: RelayIngressCounters::default(),
            next_expiry_scan_us: None,
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

        if announcement.deadline.is_expired_at(now_us) {
            return Err(RelayIngressError::DeadlineExpired);
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
        wire: &[u8],
        now_us: u64,
    ) -> Result<(), RelayIngressError> {
        let session = self.session(session_id)?.clone();
        if !session.controlled_qualification {
            return Err(RelayIngressError::AnnouncementRequired);
        }
        let symbol = decode_datagram(wire, self.config.relay_limits)?;
        validate_session_symbol(&session, &symbol)?;
        let announcement = ObjectAnnouncement {
            generation: symbol.generation,
            subscription_id: symbol.subscription_id,
            key: symbol.object_key,
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
    /// parent. `now_us` uses the same clock domain as the announced deadline.
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
        let session = self.session(session_id)?.clone();
        let symbol = decode_datagram(wire, self.config.relay_limits)?;

        self.expire_if_due(now_us);
        validate_session_symbol(&session, &symbol)?;

        let logical = LogicalObjectIdentity::from(&symbol.object_key);
        if self
            .logical_keys
            .get(&logical)
            .is_some_and(|existing| existing != &symbol.object_key)
        {
            return Err(RelayIngressError::ObjectIdentityConflict);
        }

        if let Some(completed) = self.completed.get(&symbol.object_key) {
            validate_completed_symbol(completed, &session, &symbol)?;
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
            });
        }

        // Deadline admission happens before any decoder mutation. Completed
        // objects above remain tombstones, so an authenticated symbol that
        // races decode completion is classified as a duplicate instead of a
        // deadline or authentication failure.
        if symbol.deadline.is_expired_at(now_us) {
            self.expire(now_us);
            return Err(RelayIngressError::DeadlineExpired);
        }

        let state = self
            .objects
            .get_mut(&symbol.object_key)
            .ok_or(RelayIngressError::AnnouncementRequired)?;
        if !state.parent_sessions.contains(&session_id)
            || !session.parent_path.permits(symbol.path_intent)
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
            });
        };
        validate_object_kind(&object, state.announcement.kind)?;
        let envelope = media_object::encode(&object).map_err(relay_session::Error::from)?;
        let envelope_hash = PayloadHash::digest(&envelope);
        let key = object.key().clone();
        let active = self
            .remove_active_object(&key, false)
            .expect("decoded object has active receive state");
        let parent_count = active.parent_sessions.len();
        let accepted_datagrams = active.symbols.len();
        self.counters.decoded_objects = self.counters.decoded_objects.saturating_add(1);
        if active.repair_symbols > 0 {
            self.counters.repaired_objects = self.counters.repaired_objects.saturating_add(1);
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
            role: symbol.role,
            parent_count,
            accepted_datagrams,
            envelope_hash,
        })
    }

    /// Remove deadline-expired and inactive object assemblies.
    pub fn expire(&mut self, now_us: u64) -> RelayObjectExpiry {
        let expired = self
            .objects
            .iter()
            .filter_map(|(key, state)| {
                let deadline_expired = state.announcement.deadline.is_expired_at(now_us);
                let inactive = now_us.saturating_sub(state.last_activity_us)
                    >= self.config.object_inactivity_timeout_us;
                (deadline_expired || inactive).then(|| key.clone())
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
            }
        }
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
        if existing.parent_path == candidate.parent_path {
            return Err(RelayIngressError::ParentRoleConflict);
        }
    }
    parent_sessions.insert(candidate.session_id);
    Ok(())
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

    /// Classify legacy RQD2 framing separately from RLS1 relay-session framing.
    pub fn push(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
        now_us: u64,
    ) -> Result<RelayUdpDispatchOutcome, RelayIngressError> {
        if !is_relay_session_datagram(datagram) {
            return Ok(RelayUdpDispatchOutcome::Legacy);
        }
        let Some(session_id) = self.sessions_by_peer.get(&peer).copied() else {
            self.receiver.counters.datagrams_received =
                self.receiver.counters.datagrams_received.saturating_add(1);
            self.receiver
                .record_rejection(&RelayIngressError::AuthenticationRequired, true);
            return Err(RelayIngressError::AuthenticationRequired);
        };
        if self.controlled_announcement_sessions.contains(&session_id) {
            if let Err(error) = self
                .receiver
                .ensure_controlled_announcement(session_id, datagram, now_us)
            {
                self.receiver.counters.datagrams_received =
                    self.receiver.counters.datagrams_received.saturating_add(1);
                self.receiver.record_rejection(&error, true);
                return Err(error);
            }
        }
        self.receiver
            .push_wire_datagram(session_id, datagram, now_us)
            .map(RelayUdpDispatchOutcome::Relay)
    }
}

#[must_use]
pub fn is_relay_session_datagram(datagram: &[u8]) -> bool {
    datagram.starts_with(&RELAY_SESSION_DATAGRAM_MAGIC)
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
        let media = (0..payload_len)
            .map(|index| ((index * 31 + 17) % 251) as u8)
            .collect::<Vec<_>>();
        let mut slot = Vec::with_capacity(16 + 9 + media.len());
        slot.extend_from_slice(b"AVFMP4S1");
        slot.extend_from_slice(&9u32.to_be_bytes());
        slot.extend_from_slice(&(media.len() as u32).to_be_bytes());
        slot.extend_from_slice(b"ftyp-moov");
        slot.extend_from_slice(&media);
        let key = ObjectKey::for_payload("tenant", "77", "muxed-fmp4", 1, 3, sequence, 1, &slot)
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
                parent_count,
                envelope_hash,
                ..
            } = receiver
                .push_wire_datagram(2, &wire, 1_200)
                .expect("secondary repair")
            {
                assert_eq!(parent_count, 2);
                assert_eq!(
                    envelope_hash,
                    PayloadHash::digest(&media_object::encode(&object).expect("envelope"))
                );
                decoded = Some(*object);
                break;
            }
        }
        assert_eq!(decoded.expect("cross-parent RaptorQ recovery"), object);
        assert_eq!(receiver.state().active_objects, 0);
        assert_eq!(receiver.state().completed_objects, 1);
        assert_eq!(receiver.state().buffered_datagrams, 0);
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
                RelayUdpDispatchOutcome::Relay(RelayIngressOutcome::Duplicate { ref key, role })
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
    fn receive_time_deadline_and_inactivity_expiry_release_all_state() {
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
        assert!(matches!(
            receiver.push_wire_datagram(1, &wire, 100),
            Err(RelayIngressError::DeadlineExpired)
        ));
        assert_eq!(receiver.state().active_objects, 0);
        assert_eq!(receiver.state().buffered_object_bytes, 0);

        let mut later = encoded.announcement.clone();
        later.deadline = MediaDeadline::from_micros(1_000);
        receiver
            .announce_object(1, later, 200)
            .expect("fresh announcement");
        let expiry = receiver.expire(210);
        assert_eq!(expiry.objects, 1);
        assert!(expiry.released_object_bytes > 0);
        assert_eq!(receiver.state().active_objects, 0);
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
}
