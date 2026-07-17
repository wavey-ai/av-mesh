//! Capability-enforced subscription, catalog filtering, and delivery fencing.
//!
//! P03 publishes canonical `MediaObject` values containing the exact frozen
//! frame configuration and envelope. This module verifies a subscribe
//! capability against current edge state, filters those objects by source or
//! talkback audience, and produces exact `relay-session` subscription scopes.
//! It deliberately does not accept raw browser headers: a carrier adapter must
//! supply an authenticated connection identity and key thumbprint.

use std::collections::{btree_map::Entry, BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use media_capability::{
    AuthorizedMediaCapability, CapabilityVerifierError, CapabilityVerifierErrorCode,
    CurrentMediaAuthorizationContextV1, EdgeId, EndpointId, MediaCapabilityVerifier, MediaClass,
    ParticipantId, ReplayAdmissionGuard, ReplayAdmissionRejection, ReplayAdmissionV1, SessionId,
    SourceId, TenantId, MAX_COMPACT_JWS_BYTES,
};
use media_object::{
    AudienceId, ClockConfidence, MediaFrameConfigurationV1, MediaFrameEnvelopeV1, MediaObject,
    ObjectKey, ObjectKind, Operation, SessionMediaIdentityV1, MEDIA_CONTROL_MAX_CLOCK_SKEW_SECONDS,
    MEDIA_CONTROL_MAX_GENERATION,
};
use relay_session::{
    SubscriptionChange, SubscriptionId, SubscriptionOp, SubscriptionScope, TopologyGeneration,
};

const MAX_ACTIVE_SUBSCRIPTIONS: usize = 65_536;
const DEFAULT_MAX_CATALOG_OBJECTS: usize = 65_536;
const MAX_CONFIGURED_CATALOG_OBJECTS: usize = 1_000_000;
const DEFAULT_MAX_BUFFERED_OBJECTS: usize = 256;
const DEFAULT_MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
const MAX_CONNECTION_ID_BYTES: usize = 128;
const ERROR_CODE_COUNT: usize = 24;
const REDACTED: &str = "[REDACTED]";

const CONTRACT_METADATA: &str = "media-control-contract";
const OPERATION_METADATA: &str = "media-operation-v1";
const CONFIGURATION_METADATA: &str = "media-frame-configuration-v1";
const ENVELOPE_METADATA: &str = "media-frame-envelope-v1";

/// Requested delivery lane. Talkback frames use `talkback_lane`, never this
/// module's retained `MediaObject` catalogue.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CatalogLane {
    Program,
    Talkback,
}

/// Stable, finite failure classes safe for metrics and response mapping.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum EdgeSubscribeErrorCode {
    InvalidConfiguration = 0,
    UnknownBinding = 1,
    RevokedBinding = 2,
    InvalidationGap = 3,
    CapabilityTooLarge = 4,
    InvalidCapability = 5,
    InvalidSignature = 6,
    WrongScope = 7,
    CapabilityReplay = 8,
    CapabilityExpired = 9,
    ProofRequired = 10,
    ProofMismatch = 11,
    MalformedObject = 12,
    NonCanonicalObject = 13,
    CatalogLaneMismatch = 14,
    SourceNotAllowed = 15,
    AudienceNotAllowed = 16,
    ChannelLimit = 17,
    BitrateLimit = 18,
    ObjectExpired = 19,
    Capacity = 20,
    RelayScope = 21,
    DeliveryPurged = 22,
    DatagramLimit = 23,
}

impl EdgeSubscribeErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "invalid_configuration",
            Self::UnknownBinding => "unknown_binding",
            Self::RevokedBinding => "revoked_binding",
            Self::InvalidationGap => "invalidation_gap",
            Self::CapabilityTooLarge => "capability_too_large",
            Self::InvalidCapability => "invalid_capability",
            Self::InvalidSignature => "invalid_signature",
            Self::WrongScope => "wrong_scope",
            Self::CapabilityReplay => "capability_replay",
            Self::CapabilityExpired => "capability_expired",
            Self::ProofRequired => "proof_required",
            Self::ProofMismatch => "proof_mismatch",
            Self::MalformedObject => "malformed_object",
            Self::NonCanonicalObject => "non_canonical_object",
            Self::CatalogLaneMismatch => "catalog_lane_mismatch",
            Self::SourceNotAllowed => "source_not_allowed",
            Self::AudienceNotAllowed => "audience_not_allowed",
            Self::ChannelLimit => "channel_limit",
            Self::BitrateLimit => "bitrate_limit",
            Self::ObjectExpired => "object_expired",
            Self::Capacity => "capacity",
            Self::RelayScope => "relay_scope",
            Self::DeliveryPurged => "delivery_purged",
            Self::DatagramLimit => "datagram_limit",
        }
    }
}

/// A bounded, value-free edge rejection.
#[derive(Clone, Eq, PartialEq)]
pub struct EdgeSubscribeError {
    code: EdgeSubscribeErrorCode,
    field: &'static str,
}

impl EdgeSubscribeError {
    const fn new(code: EdgeSubscribeErrorCode, field: &'static str) -> Self {
        Self { code, field }
    }

    #[must_use]
    pub const fn code(&self) -> EdgeSubscribeErrorCode {
        self.code
    }

    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Debug for EdgeSubscribeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EdgeSubscribeError")
            .field("code", &self.code)
            .field("field", &self.field)
            .finish()
    }
}

impl fmt::Display for EdgeSubscribeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: subscription rejected", self.code.as_str())
    }
}

impl std::error::Error for EdgeSubscribeError {}

pub type Result<T> = std::result::Result<T, EdgeSubscribeError>;

/// Exact current authorization state installed by the authenticated controller.
#[derive(Clone)]
pub struct CurrentSubscribeBinding {
    tenant_id: TenantId,
    session_id: SessionId,
    session_epoch: u64,
    media_authorization_epoch: u64,
    subject_grant_epoch: u64,
    media_policy_version: u64,
    class_authorization_epoch: Option<u64>,
    binding_generation: u64,
    topology_generation: u64,
    participant_id: ParticipantId,
    endpoint_id: EndpointId,
    media_class: MediaClass,
    edge_id: EdgeId,
    clock_skew_seconds: i64,
}

impl CurrentSubscribeBinding {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        session_id: SessionId,
        session_epoch: u64,
        media_authorization_epoch: u64,
        subject_grant_epoch: u64,
        media_policy_version: u64,
        class_authorization_epoch: Option<u64>,
        binding_generation: u64,
        topology_generation: u64,
        participant_id: ParticipantId,
        endpoint_id: EndpointId,
        media_class: MediaClass,
        edge_id: EdgeId,
        clock_skew_seconds: i64,
    ) -> Result<Self> {
        for generation in [
            session_epoch,
            media_authorization_epoch,
            subject_grant_epoch,
            media_policy_version,
            binding_generation,
            topology_generation,
        ] {
            if generation == 0 || generation > MEDIA_CONTROL_MAX_GENERATION {
                return Err(EdgeSubscribeError::new(
                    EdgeSubscribeErrorCode::InvalidConfiguration,
                    "authorization_generation",
                ));
            }
        }
        if class_authorization_epoch
            .is_some_and(|value| value == 0 || value > MEDIA_CONTROL_MAX_GENERATION)
            || !(0..=MEDIA_CONTROL_MAX_CLOCK_SKEW_SECONDS).contains(&clock_skew_seconds)
            || media_class == MediaClass::TakeChunk
        {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidConfiguration,
                "subscribe_binding",
            ));
        }
        Ok(Self {
            tenant_id,
            session_id,
            session_epoch,
            media_authorization_epoch,
            subject_grant_epoch,
            media_policy_version,
            class_authorization_epoch,
            binding_generation,
            topology_generation,
            participant_id,
            endpoint_id,
            media_class,
            edge_id,
            clock_skew_seconds,
        })
    }

    fn key(&self) -> SubscribeBindingKey {
        SubscribeBindingKey {
            endpoint_id: self.endpoint_id.as_str().to_owned(),
            media_class: self.media_class,
            binding_generation: self.binding_generation,
        }
    }
}

impl fmt::Debug for CurrentSubscribeBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CurrentSubscribeBinding")
            .field("session_epoch", &self.session_epoch)
            .field("media_authorization_epoch", &self.media_authorization_epoch)
            .field("subject_grant_epoch", &self.subject_grant_epoch)
            .field("media_policy_version", &self.media_policy_version)
            .field("class_authorization_epoch", &self.class_authorization_epoch)
            .field("binding_generation", &self.binding_generation)
            .field("topology_generation", &self.topology_generation)
            .field("media_class", &self.media_class)
            .field("identity", &REDACTED)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SubscribeBindingKey {
    endpoint_id: String,
    media_class: MediaClass,
    binding_generation: u64,
}

#[derive(Clone)]
struct RegisteredSubscribeBinding {
    binding: Arc<CurrentSubscribeBinding>,
    revision: u64,
    active: bool,
}

struct RegistryState {
    next_revision: u64,
    last_invalidation_sequence: u64,
    required_snapshot_boundary: u64,
    synchronized: bool,
    bindings: BTreeMap<SubscribeBindingKey, RegisteredSubscribeBinding>,
}

/// One ordered generation update from the identity/controller invalidation lane.
pub struct SubscribeInvalidationV1 {
    pub delivery_sequence: u64,
    pub session_id: SessionId,
    pub session_epoch: u64,
    pub media_authorization_epoch: u64,
    pub media_policy_version: u64,
    pub endpoint_id: Option<EndpointId>,
    pub subject_grant_epoch: Option<u64>,
}

/// Idempotent result for one ordered invalidation event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidationOutcome {
    Applied { invalidated_bindings: usize },
    Duplicate,
}

/// Current subscribe contexts plus an ordered invalidation-gap fence.
pub struct CurrentSubscribeRegistry {
    state: RwLock<RegistryState>,
}

impl CurrentSubscribeRegistry {
    pub fn new(snapshot_boundary_sequence: u64) -> Result<Self> {
        if snapshot_boundary_sequence > MEDIA_CONTROL_MAX_GENERATION {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidConfiguration,
                "snapshot_boundary_sequence",
            ));
        }
        Ok(Self {
            state: RwLock::new(RegistryState {
                next_revision: 0,
                last_invalidation_sequence: snapshot_boundary_sequence,
                required_snapshot_boundary: 0,
                synchronized: true,
                bindings: BTreeMap::new(),
            }),
        })
    }

    pub fn install(&self, binding: CurrentSubscribeBinding) -> Result<u64> {
        let key = binding.key();
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.synchronized {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidationGap,
                "invalidation_sequence",
            ));
        }
        if state.bindings.contains_key(&key) {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidConfiguration,
                "subscribe_binding",
            ));
        }
        if state.bindings.len() >= MAX_ACTIVE_SUBSCRIPTIONS {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::Capacity,
                "subscribe_bindings",
            ));
        }
        state.next_revision = state.next_revision.saturating_add(1).max(1);
        let revision = state.next_revision;
        state.bindings.insert(
            key,
            RegisteredSubscribeBinding {
                binding: Arc::new(binding),
                revision,
                active: true,
            },
        );
        Ok(revision)
    }

    /// Apply one global ordered invalidation. A detected gap fences every lease.
    pub fn apply_invalidation(
        &self,
        event: SubscribeInvalidationV1,
    ) -> Result<InvalidationOutcome> {
        if event.delivery_sequence == 0
            || event.delivery_sequence > MEDIA_CONTROL_MAX_GENERATION
            || event.session_epoch == 0
            || event.session_epoch > MEDIA_CONTROL_MAX_GENERATION
            || event.media_authorization_epoch == 0
            || event.media_authorization_epoch > MEDIA_CONTROL_MAX_GENERATION
            || event.media_policy_version == 0
            || event.media_policy_version > MEDIA_CONTROL_MAX_GENERATION
            || event.endpoint_id.is_some() != event.subject_grant_epoch.is_some()
            || event.subject_grant_epoch.is_some_and(|generation| {
                generation == 0 || generation > MEDIA_CONTROL_MAX_GENERATION
            })
        {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidConfiguration,
                "invalidation_event",
            ));
        }
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if event.delivery_sequence <= state.last_invalidation_sequence {
            return Ok(InvalidationOutcome::Duplicate);
        }
        if !state.synchronized
            || event.delivery_sequence != state.last_invalidation_sequence.saturating_add(1)
        {
            state.synchronized = false;
            state.required_snapshot_boundary = state
                .required_snapshot_boundary
                .max(event.delivery_sequence);
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidationGap,
                "invalidation_sequence",
            ));
        }
        state.last_invalidation_sequence = event.delivery_sequence;
        let mut invalidated = 0usize;
        let mut revision = state.next_revision;
        for registered in state.bindings.values_mut() {
            let binding = &registered.binding;
            if binding.session_id != event.session_id || !registered.active {
                continue;
            }
            let session_stale = binding.session_epoch < event.session_epoch
                || binding.media_authorization_epoch < event.media_authorization_epoch
                || binding.media_policy_version < event.media_policy_version;
            let subject_stale = event.endpoint_id.as_ref().is_some_and(|endpoint_id| {
                &binding.endpoint_id == endpoint_id
                    && event
                        .subject_grant_epoch
                        .is_some_and(|epoch| binding.subject_grant_epoch < epoch)
            });
            if session_stale || subject_stale {
                revision = revision.saturating_add(1).max(1);
                registered.revision = revision;
                registered.active = false;
                invalidated = invalidated.saturating_add(1);
            }
        }
        state.next_revision = revision;
        Ok(InvalidationOutcome::Applied {
            invalidated_bindings: invalidated,
        })
    }

    /// Install an exact snapshot at a controller-provided outbox boundary.
    pub fn install_snapshot(
        &self,
        boundary_sequence: u64,
        bindings: Vec<CurrentSubscribeBinding>,
    ) -> Result<()> {
        if boundary_sequence > MEDIA_CONTROL_MAX_GENERATION {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidConfiguration,
                "snapshot_boundary_sequence",
            ));
        }
        if bindings.len() > MAX_ACTIVE_SUBSCRIPTIONS {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::Capacity,
                "snapshot_bindings",
            ));
        }
        let mut unique = BTreeMap::new();
        for binding in bindings {
            let key = binding.key();
            if unique.insert(key, binding).is_some() {
                return Err(EdgeSubscribeError::new(
                    EdgeSubscribeErrorCode::InvalidConfiguration,
                    "snapshot_binding",
                ));
            }
        }
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if boundary_sequence < state.last_invalidation_sequence
            || boundary_sequence < state.required_snapshot_boundary
        {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidConfiguration,
                "snapshot_boundary_sequence",
            ));
        }
        let mut revision = state.next_revision;
        let mut replacement = BTreeMap::new();
        for (key, binding) in unique {
            revision = revision.saturating_add(1).max(1);
            replacement.insert(
                key,
                RegisteredSubscribeBinding {
                    binding: Arc::new(binding),
                    revision,
                    active: true,
                },
            );
        }
        state.next_revision = revision;
        state.last_invalidation_sequence = boundary_sequence;
        state.required_snapshot_boundary = 0;
        state.synchronized = true;
        state.bindings = replacement;
        Ok(())
    }

    #[must_use]
    pub fn acknowledged_sequence(&self) -> Option<u64> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .synchronized
            .then_some(state.last_invalidation_sequence)
    }

    fn resolve(
        &self,
        endpoint_id: &EndpointId,
        media_class: MediaClass,
        binding_generation: u64,
    ) -> Result<RegisteredSubscribeBinding> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.synchronized {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidationGap,
                "invalidation_sequence",
            ));
        }
        let key = SubscribeBindingKey {
            endpoint_id: endpoint_id.as_str().to_owned(),
            media_class,
            binding_generation,
        };
        let registered = state.bindings.get(&key).ok_or_else(|| {
            EdgeSubscribeError::new(EdgeSubscribeErrorCode::UnknownBinding, "subscribe_binding")
        })?;
        if !registered.active {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::RevokedBinding,
                "subscribe_binding",
            ));
        }
        Ok(registered.clone())
    }

    fn revalidate(&self, lease: &SubscriptionLease) -> Result<()> {
        let current = self.resolve(
            &lease.binding.endpoint_id,
            lease.binding.media_class,
            lease.binding.binding_generation,
        )?;
        if current.revision != lease.binding_revision {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::RevokedBinding,
                "subscribe_binding",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for CurrentSubscribeRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        formatter
            .debug_struct("CurrentSubscribeRegistry")
            .field("binding_count", &state.bindings.len())
            .field(
                "last_invalidation_sequence",
                &state.last_invalidation_sequence,
            )
            .field("synchronized", &state.synchronized)
            .finish()
    }
}

/// Authenticated carrier facts. Raw browser-controlled headers are not this type.
pub struct EdgeSubscribeRequest<'a> {
    pub compact_jws: &'a str,
    pub endpoint_id: &'a EndpointId,
    pub media_class: MediaClass,
    pub binding_generation: u64,
    /// One exact signed source selector for program/source/screen/metadata.
    pub requested_source_id: Option<&'a SourceId>,
    /// One exact signed audience selector for talkback.
    pub requested_audience_id: Option<&'a AudienceId>,
    pub connection_id: &'a str,
    pub authenticated_client_key_thumbprint: Option<&'a str>,
    pub now_unix_seconds: i64,
}

impl fmt::Debug for EdgeSubscribeRequest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EdgeSubscribeRequest")
            .field("compact_jws", &REDACTED)
            .field("endpoint_id", &REDACTED)
            .field("media_class", &self.media_class)
            .field("binding_generation", &self.binding_generation)
            .field("requested_source_id", &REDACTED)
            .field("requested_audience_id", &REDACTED)
            .field("connection_id", &REDACTED)
            .field("authenticated_client_key_thumbprint", &REDACTED)
            .field("now_unix_seconds", &self.now_unix_seconds)
            .finish()
    }
}

pub trait SubscribeCapabilityVerifier: Send + Sync {
    fn authorize(
        &self,
        compact_jws: &str,
        context: &CurrentMediaAuthorizationContextV1<'_>,
        guard: &mut dyn ReplayAdmissionGuard,
    ) -> std::result::Result<VerifiedSubscribeCapability, CapabilityVerifierError>;
}

impl SubscribeCapabilityVerifier for MediaCapabilityVerifier {
    fn authorize(
        &self,
        compact_jws: &str,
        context: &CurrentMediaAuthorizationContextV1<'_>,
        guard: &mut dyn ReplayAdmissionGuard,
    ) -> std::result::Result<VerifiedSubscribeCapability, CapabilityVerifierError> {
        self.authorize(compact_jws, context, guard)
            .map(VerifiedSubscribeCapability::from_authorized)
    }
}

pub struct VerifiedSubscribeCapability {
    capability_id: String,
    source_ids: Vec<SourceId>,
    audience_ids: Vec<AudienceId>,
    max_channels: u16,
    max_bitrate: u64,
    max_datagram_bytes: u32,
    client_key_thumbprint: Option<String>,
    expires_at: i64,
}

impl VerifiedSubscribeCapability {
    fn from_authorized(authorized: AuthorizedMediaCapability) -> Self {
        let claims = authorized.claims();
        Self {
            capability_id: claims.capability_id().as_str().to_owned(),
            source_ids: claims.source_ids().to_vec(),
            audience_ids: claims.audience_ids().to_vec(),
            max_channels: claims.max_channels(),
            max_bitrate: claims.max_bitrate(),
            max_datagram_bytes: claims.max_datagram_bytes(),
            client_key_thumbprint: claims.client_key_thumbprint().map(ToOwned::to_owned),
            expires_at: claims.expires_at(),
        }
    }
}

impl fmt::Debug for VerifiedSubscribeCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedSubscribeCapability")
            .field("source_count", &self.source_ids.len())
            .field("audience_count", &self.audience_ids.len())
            .field("max_channels", &self.max_channels)
            .field("max_bitrate", &self.max_bitrate)
            .field("max_datagram_bytes", &self.max_datagram_bytes)
            .field("proof_bound", &self.client_key_thumbprint.is_some())
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
struct SubscribeAdmissionFingerprint {
    endpoint_id: String,
    media_class: MediaClass,
    binding_generation: u64,
    connection_id: String,
}

struct StoredAdmission {
    fingerprint: SubscribeAdmissionFingerprint,
    expires_at: i64,
    state: AdmissionState,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum AdmissionState {
    Pending(u64),
    Committed,
}

struct SubscribeAdmissionGuard<'a> {
    admissions: &'a Mutex<BTreeMap<String, StoredAdmission>>,
    fingerprint: SubscribeAdmissionFingerprint,
    now: i64,
    reservation_id: u64,
    pending_capability_id: Option<String>,
}

impl SubscribeAdmissionGuard<'_> {
    fn commit(&mut self) {
        let Some(capability_id) = self.pending_capability_id.take() else {
            return;
        };
        let mut admissions = self
            .admissions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(stored) = admissions.get_mut(&capability_id) {
            if stored.state == AdmissionState::Pending(self.reservation_id) {
                stored.state = AdmissionState::Committed;
            }
        }
    }

    fn rollback(&mut self) {
        let Some(capability_id) = self.pending_capability_id.take() else {
            return;
        };
        let mut admissions = self
            .admissions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let owns_pending = admissions
            .get(&capability_id)
            .is_some_and(|stored| stored.state == AdmissionState::Pending(self.reservation_id));
        if owns_pending {
            admissions.remove(&capability_id);
        }
    }
}

impl Drop for SubscribeAdmissionGuard<'_> {
    fn drop(&mut self) {
        self.rollback();
    }
}

impl ReplayAdmissionGuard for SubscribeAdmissionGuard<'_> {
    fn check_and_admit(
        &mut self,
        admission: ReplayAdmissionV1<'_>,
    ) -> std::result::Result<(), ReplayAdmissionRejection> {
        let mut admissions = self
            .admissions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        admissions.retain(|_, previous| previous.expires_at > self.now);
        let capability_id = admission.capability_id.as_str().to_owned();
        if !admissions.contains_key(&capability_id) && admissions.len() >= MAX_ACTIVE_SUBSCRIPTIONS
        {
            return Err(ReplayAdmissionRejection::Capacity);
        }
        match admissions.entry(capability_id) {
            Entry::Vacant(entry) => {
                self.pending_capability_id = Some(entry.key().clone());
                entry.insert(StoredAdmission {
                    fingerprint: self.fingerprint.clone(),
                    expires_at: admission.expires_at,
                    state: AdmissionState::Pending(self.reservation_id),
                });
                Ok(())
            }
            Entry::Occupied(mut entry)
                if entry.get().fingerprint == self.fingerprint
                    && entry.get().state == AdmissionState::Committed =>
            {
                entry.get_mut().expires_at = admission.expires_at;
                Ok(())
            }
            Entry::Occupied(entry)
                if entry.get().fingerprint == self.fingerprint
                    && entry.get().state == AdmissionState::Pending(self.reservation_id) =>
            {
                Ok(())
            }
            Entry::Occupied(_) => Err(ReplayAdmissionRejection::Replay),
        }
    }
}

/// Immutable authorization bound to one authenticated carrier connection.
#[derive(Clone)]
pub struct SubscriptionLease {
    binding: Arc<CurrentSubscribeBinding>,
    binding_revision: u64,
    capability_id: String,
    connection_id: String,
    source_ids: Vec<SourceId>,
    audience_ids: Vec<AudienceId>,
    max_channels: u16,
    max_bitrate: u64,
    max_datagram_bytes: u32,
    expires_at: i64,
}

impl SubscriptionLease {
    #[must_use]
    pub const fn expires_at(&self) -> i64 {
        self.expires_at
    }

    #[must_use]
    pub fn media_class(&self) -> MediaClass {
        self.binding.media_class
    }

    #[must_use]
    pub fn topology_generation(&self) -> u64 {
        self.binding.topology_generation
    }

    #[must_use]
    pub const fn max_datagram_bytes(&self) -> u32 {
        self.max_datagram_bytes
    }

    /// Apply the signed carrier ceiling to every encoded outgoing datagram.
    pub fn authorize_datagram_bytes(&self, encoded_bytes: usize) -> Result<()> {
        if encoded_bytes > self.max_datagram_bytes as usize {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::DatagramLimit,
                "max_datagram_bytes",
            ));
        }
        Ok(())
    }

    /// Confirm that a carrier still presents the exact admission identity
    /// frozen into this lease without exposing either identifier to logs or
    /// telemetry.
    #[must_use]
    pub fn matches_admission(&self, capability_id: &str, connection_id: &str) -> bool {
        self.capability_id == capability_id && self.connection_id == connection_id
    }
}

impl fmt::Debug for SubscriptionLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubscriptionLease")
            .field("binding", &self.binding)
            .field("capability_id", &REDACTED)
            .field("connection_id", &REDACTED)
            .field("source_count", &self.source_ids.len())
            .field("audience_count", &self.audience_ids.len())
            .field("max_channels", &self.max_channels)
            .field("max_bitrate", &self.max_bitrate)
            .field("max_datagram_bytes", &self.max_datagram_bytes)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

struct EdgeSubscribeMetrics {
    admitted: AtomicU64,
    rejected: AtomicU64,
    rejection_reasons: [AtomicU64; ERROR_CODE_COUNT],
}

impl Default for EdgeSubscribeMetrics {
    fn default() -> Self {
        Self {
            admitted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            rejection_reasons: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

/// Capability verifier and current-state gate used by edge carrier adapters.
pub struct EdgeSubscribeGate {
    verifier: Arc<dyn SubscribeCapabilityVerifier>,
    registry: Arc<CurrentSubscribeRegistry>,
    admissions: Mutex<BTreeMap<String, StoredAdmission>>,
    next_reservation_id: AtomicU64,
    metrics: EdgeSubscribeMetrics,
}

impl EdgeSubscribeGate {
    #[must_use]
    pub fn new(
        verifier: Arc<dyn SubscribeCapabilityVerifier>,
        registry: Arc<CurrentSubscribeRegistry>,
    ) -> Self {
        Self {
            verifier,
            registry,
            admissions: Mutex::new(BTreeMap::new()),
            next_reservation_id: AtomicU64::new(1),
            metrics: EdgeSubscribeMetrics::default(),
        }
    }

    #[must_use]
    pub fn registry(&self) -> &Arc<CurrentSubscribeRegistry> {
        &self.registry
    }

    pub fn authorize(&self, request: &EdgeSubscribeRequest<'_>) -> Result<SubscriptionLease> {
        let result = self.authorize_strict(request);
        match &result {
            Ok(_) => {
                self.metrics.admitted.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => self.record_rejection(error),
        }
        result
    }

    fn authorize_strict(&self, request: &EdgeSubscribeRequest<'_>) -> Result<SubscriptionLease> {
        if request.compact_jws.is_empty() || request.compact_jws.len() > MAX_COMPACT_JWS_BYTES {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::CapabilityTooLarge,
                "authorization",
            ));
        }
        if request.connection_id.is_empty()
            || request.connection_id.len() > MAX_CONNECTION_ID_BYTES
            || !request.connection_id.is_ascii()
            || request
                .connection_id
                .bytes()
                .any(|byte| !byte.is_ascii_graphic())
        {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidConfiguration,
                "connection_id",
            ));
        }
        let exact_selector = match request.media_class {
            MediaClass::Talkback => {
                request.requested_source_id.is_none() && request.requested_audience_id.is_some()
            }
            MediaClass::Program
            | MediaClass::Source
            | MediaClass::Screen
            | MediaClass::Metadata => {
                request.requested_source_id.is_some() && request.requested_audience_id.is_none()
            }
            MediaClass::TakeChunk => false,
        };
        if !exact_selector {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidConfiguration,
                "requested_scope",
            ));
        }
        let registered = self.registry.resolve(
            request.endpoint_id,
            request.media_class,
            request.binding_generation,
        )?;
        let binding = &registered.binding;
        let context = CurrentMediaAuthorizationContextV1 {
            tenant_id: &binding.tenant_id,
            session_id: &binding.session_id,
            session_epoch: binding.session_epoch,
            media_authorization_epoch: binding.media_authorization_epoch,
            subject_grant_epoch: binding.subject_grant_epoch,
            media_policy_version: binding.media_policy_version,
            class_authorization_epoch: binding.class_authorization_epoch,
            binding_generation: binding.binding_generation,
            topology_generation: binding.topology_generation,
            participant_id: &binding.participant_id,
            endpoint_id: &binding.endpoint_id,
            contributor_id: None,
            operation: Operation::Subscribe,
            media_class: binding.media_class,
            source_id: request.requested_source_id,
            audience_id: request.requested_audience_id,
            take_id: None,
            edge_id: &binding.edge_id,
            now: request.now_unix_seconds,
            clock_skew_seconds: binding.clock_skew_seconds,
        };
        let fingerprint = SubscribeAdmissionFingerprint {
            endpoint_id: request.endpoint_id.as_str().to_owned(),
            media_class: request.media_class,
            binding_generation: request.binding_generation,
            connection_id: request.connection_id.to_owned(),
        };
        let reservation_id = self.next_reservation_id.fetch_add(1, Ordering::Relaxed);
        let mut guard = SubscribeAdmissionGuard {
            admissions: &self.admissions,
            fingerprint,
            now: request.now_unix_seconds,
            reservation_id,
            pending_capability_id: None,
        };
        let verified = self
            .verifier
            .authorize(request.compact_jws, &context, &mut guard)
            .map_err(map_verifier_error)?;

        if request.now_unix_seconds >= verified.expires_at {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::CapabilityExpired,
                "expires_at",
            ));
        }
        match (
            verified.client_key_thumbprint.as_deref(),
            request.authenticated_client_key_thumbprint,
        ) {
            (Some(_), None) => {
                return Err(EdgeSubscribeError::new(
                    EdgeSubscribeErrorCode::ProofRequired,
                    "client_key_thumbprint",
                ));
            }
            (Some(expected), Some(actual)) if expected != actual => {
                return Err(EdgeSubscribeError::new(
                    EdgeSubscribeErrorCode::ProofMismatch,
                    "client_key_thumbprint",
                ));
            }
            _ => {}
        }
        guard.commit();
        Ok(SubscriptionLease {
            binding: Arc::clone(binding),
            binding_revision: registered.revision,
            capability_id: verified.capability_id,
            connection_id: request.connection_id.to_owned(),
            source_ids: verified.source_ids,
            audience_ids: verified.audience_ids,
            max_channels: verified.max_channels,
            max_bitrate: verified.max_bitrate,
            max_datagram_bytes: verified.max_datagram_bytes,
            expires_at: verified.expires_at,
        })
    }

    pub fn revalidate(&self, lease: &SubscriptionLease, now_unix_seconds: i64) -> Result<()> {
        if now_unix_seconds >= lease.expires_at {
            let error =
                EdgeSubscribeError::new(EdgeSubscribeErrorCode::CapabilityExpired, "expires_at");
            self.record_rejection(&error);
            return Err(error);
        }
        self.registry.revalidate(lease).inspect_err(|error| {
            self.record_rejection(error);
        })
    }

    /// Return an exact relay-session subscription after object and lease checks.
    pub fn relay_subscription_change(
        &self,
        lease: &SubscriptionLease,
        entry: &CanonicalCatalogEntry,
        subscription_id: SubscriptionId,
        operation: SubscriptionOp,
        now_unix_seconds: i64,
    ) -> Result<SubscriptionChange> {
        if operation == SubscriptionOp::Subscribe {
            entry.authorize(self, lease, entry.lane, now_unix_seconds)?;
        }
        let generation =
            TopologyGeneration::new(lease.binding.topology_generation).map_err(|_| {
                EdgeSubscribeError::new(EdgeSubscribeErrorCode::RelayScope, "generation")
            })?;
        let scope = SubscriptionScope::new(
            entry.object.key().tenant(),
            entry.object.key().stream(),
            Some(entry.object.key().track()),
        )
        .map_err(|_| EdgeSubscribeError::new(EdgeSubscribeErrorCode::RelayScope, "object_key"))?;
        Ok(SubscriptionChange {
            generation,
            id: subscription_id,
            operation,
            scope,
        })
    }

    #[must_use]
    pub fn prometheus_metrics(&self) -> String {
        let mut output = format!(
            "# HELP av_mesh_subscribe_authorization_total Edge subscription authorization decisions.\n# TYPE av_mesh_subscribe_authorization_total counter\nav_mesh_subscribe_authorization_total{{decision=\"admit\"}} {}\nav_mesh_subscribe_authorization_total{{decision=\"reject\"}} {}\n# HELP av_mesh_subscribe_authorization_rejections_total Edge subscription rejections by finite reason.\n# TYPE av_mesh_subscribe_authorization_rejections_total counter\n",
            self.metrics.admitted.load(Ordering::Relaxed),
            self.metrics.rejected.load(Ordering::Relaxed),
        );
        for code in ALL_ERROR_CODES {
            output.push_str(&format!(
                "av_mesh_subscribe_authorization_rejections_total{{reason=\"{}\"}} {}\n",
                code.as_str(),
                self.metrics.rejection_reasons[code as usize].load(Ordering::Relaxed),
            ));
        }
        output
    }

    fn record_rejection(&self, error: &EdgeSubscribeError) {
        self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
        self.metrics.rejection_reasons[error.code as usize].fetch_add(1, Ordering::Relaxed);
    }
}

const ALL_ERROR_CODES: [EdgeSubscribeErrorCode; ERROR_CODE_COUNT] = [
    EdgeSubscribeErrorCode::InvalidConfiguration,
    EdgeSubscribeErrorCode::UnknownBinding,
    EdgeSubscribeErrorCode::RevokedBinding,
    EdgeSubscribeErrorCode::InvalidationGap,
    EdgeSubscribeErrorCode::CapabilityTooLarge,
    EdgeSubscribeErrorCode::InvalidCapability,
    EdgeSubscribeErrorCode::InvalidSignature,
    EdgeSubscribeErrorCode::WrongScope,
    EdgeSubscribeErrorCode::CapabilityReplay,
    EdgeSubscribeErrorCode::CapabilityExpired,
    EdgeSubscribeErrorCode::ProofRequired,
    EdgeSubscribeErrorCode::ProofMismatch,
    EdgeSubscribeErrorCode::MalformedObject,
    EdgeSubscribeErrorCode::NonCanonicalObject,
    EdgeSubscribeErrorCode::CatalogLaneMismatch,
    EdgeSubscribeErrorCode::SourceNotAllowed,
    EdgeSubscribeErrorCode::AudienceNotAllowed,
    EdgeSubscribeErrorCode::ChannelLimit,
    EdgeSubscribeErrorCode::BitrateLimit,
    EdgeSubscribeErrorCode::ObjectExpired,
    EdgeSubscribeErrorCode::Capacity,
    EdgeSubscribeErrorCode::RelayScope,
    EdgeSubscribeErrorCode::DeliveryPurged,
    EdgeSubscribeErrorCode::DatagramLimit,
];

/// Canonical retained-program catalog value parsed from P03 metadata before it
/// can be revealed. A talkback object is rejected before insertion.
#[derive(Clone)]
pub struct CanonicalCatalogEntry {
    object: MediaObject,
    configuration: MediaFrameConfigurationV1,
    envelope: MediaFrameEnvelopeV1,
    lane: CatalogLane,
}

impl CanonicalCatalogEntry {
    pub fn from_media_object(object: MediaObject) -> Result<Self> {
        if object.kind() != ObjectKind::Media
            || object.verify_payload_hash().is_err()
            || object.is_keyframe()
            || object.capture_timestamp().is_some()
            || !object.stage_timestamps().is_empty()
            || !object.dependencies().is_empty()
            || object.metadata().len() != 4
        {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::MalformedObject,
                "media_object",
            ));
        }
        let metadata = object.metadata();
        if metadata.get(CONTRACT_METADATA).map(Vec::as_slice) != Some(b"v1")
            || metadata.get(OPERATION_METADATA).map(Vec::as_slice) != Some(b"publish")
        {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::MalformedObject,
                "media_operation",
            ));
        }
        let configuration_bytes = metadata.get(CONFIGURATION_METADATA).ok_or_else(|| {
            EdgeSubscribeError::new(EdgeSubscribeErrorCode::MalformedObject, "configuration")
        })?;
        let envelope_bytes = metadata.get(ENVELOPE_METADATA).ok_or_else(|| {
            EdgeSubscribeError::new(EdgeSubscribeErrorCode::MalformedObject, "frame_envelope")
        })?;
        let configuration = MediaFrameConfigurationV1::from_json_slice(configuration_bytes)
            .map_err(|_| {
                EdgeSubscribeError::new(EdgeSubscribeErrorCode::MalformedObject, "configuration")
            })?;
        let envelope = MediaFrameEnvelopeV1::from_json_slice(envelope_bytes).map_err(|_| {
            EdgeSubscribeError::new(EdgeSubscribeErrorCode::MalformedObject, "frame_envelope")
        })?;
        if configuration
            .to_canonical_json_vec()
            .map_or(true, |canonical| {
                canonical.as_slice() != configuration_bytes.as_slice()
            })
            || envelope.to_canonical_json_vec().map_or(true, |canonical| {
                canonical.as_slice() != envelope_bytes.as_slice()
            })
            || envelope.resolve(&configuration).is_err()
        {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::NonCanonicalObject,
                "media_control_metadata",
            ));
        }
        let identity = configuration.identity();
        let key = object.key();
        if key.tenant() != identity.tenant_id().as_str()
            || key.track() != configuration.configuration_id().as_str()
            || key.epoch() != envelope.binding_generation()
            || key.group() != u64::from(envelope.configuration_ref())
            || key.object() != envelope.sequence()
            || key.version() != 1
            || object.configuration_epoch() != envelope.configuration_epoch()
            || object.payload().len() != envelope.payload_bytes() as usize
            || object.deadline().is_none()
            || object.deadline().is_some_and(|deadline| {
                deadline.clock_id() != "media-capability:issuer"
                    || deadline.confidence() != ClockConfidence::unknown()
            })
            || identity.media_class() == MediaClass::TakeChunk
        {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::NonCanonicalObject,
                "canonical_identity",
            ));
        }
        if identity.media_class() == MediaClass::Talkback {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::CatalogLaneMismatch,
                "ephemeral_talkback_lane",
            ));
        }
        let lane = CatalogLane::Program;
        Ok(Self {
            object,
            configuration,
            envelope,
            lane,
        })
    }

    #[must_use]
    pub const fn object(&self) -> &MediaObject {
        &self.object
    }

    #[must_use]
    pub const fn identity(&self) -> &SessionMediaIdentityV1 {
        self.configuration.identity()
    }

    #[must_use]
    pub const fn lane(&self) -> CatalogLane {
        self.lane
    }

    pub fn authorize(
        &self,
        gate: &EdgeSubscribeGate,
        lease: &SubscriptionLease,
        lane: CatalogLane,
        now_unix_seconds: i64,
    ) -> Result<()> {
        gate.revalidate(lease, now_unix_seconds)?;
        if self.lane != lane {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::CatalogLaneMismatch,
                "catalog_lane",
            ));
        }
        let identity = self.configuration.identity();
        if identity.tenant_id() != &lease.binding.tenant_id
            || identity.session_id() != &lease.binding.session_id
            || identity.session_epoch() != lease.binding.session_epoch
            || identity.topology_generation() != lease.binding.topology_generation
            || identity.media_class() != lease.binding.media_class
        {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::WrongScope,
                "media_identity",
            ));
        }
        match self.lane {
            CatalogLane::Program => {
                let source_id = identity.source_id().ok_or_else(|| {
                    EdgeSubscribeError::new(EdgeSubscribeErrorCode::SourceNotAllowed, "source_id")
                })?;
                if lease.source_ids.binary_search(source_id).is_err() {
                    return Err(EdgeSubscribeError::new(
                        EdgeSubscribeErrorCode::SourceNotAllowed,
                        "source_id",
                    ));
                }
            }
            CatalogLane::Talkback => {
                let audience_id = identity.audience_id().ok_or_else(|| {
                    EdgeSubscribeError::new(
                        EdgeSubscribeErrorCode::AudienceNotAllowed,
                        "audience_id",
                    )
                })?;
                if lease.audience_ids.binary_search(audience_id).is_err() {
                    return Err(EdgeSubscribeError::new(
                        EdgeSubscribeErrorCode::AudienceNotAllowed,
                        "audience_id",
                    ));
                }
            }
        }
        if self.configuration.channel_count() > lease.max_channels {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::ChannelLimit,
                "channel_count",
            ));
        }
        let required_bits = u128::from(self.envelope.payload_bytes())
            .saturating_mul(8)
            .saturating_mul(u128::from(self.configuration.capture_timebase_hz()));
        let allowed_bits = u128::from(lease.max_bitrate)
            .saturating_mul(u128::from(self.envelope.duration_ticks()));
        if required_bits > allowed_bits {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::BitrateLimit,
                "max_bitrate",
            ));
        }
        let now_ns = now_unix_seconds.checked_mul(1_000_000_000).ok_or_else(|| {
            EdgeSubscribeError::new(EdgeSubscribeErrorCode::InvalidConfiguration, "now")
        })?;
        if self
            .object
            .deadline()
            .is_none_or(|deadline| deadline.unix_time_ns() <= now_ns)
        {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::ObjectExpired,
                "object_deadline",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for CanonicalCatalogEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CanonicalCatalogEntry")
            .field("lane", &self.lane)
            .field("media_class", &self.configuration.identity().media_class())
            .field("payload_bytes", &self.object.payload().len())
            .finish_non_exhaustive()
    }
}

/// Bounded retained catalog. The talkback partition remains empty under P09;
/// its accessor is retained for migration observability.
pub struct PartitionedCatalog {
    max_objects: usize,
    program: BTreeMap<ObjectKey, CanonicalCatalogEntry>,
    talkback: BTreeMap<ObjectKey, CanonicalCatalogEntry>,
}

impl Default for PartitionedCatalog {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_CATALOG_OBJECTS).expect("default catalog bound is valid")
    }
}

impl PartitionedCatalog {
    pub fn new(max_objects: usize) -> Result<Self> {
        if max_objects == 0 || max_objects > MAX_CONFIGURED_CATALOG_OBJECTS {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidConfiguration,
                "max_catalog_objects",
            ));
        }
        Ok(Self {
            max_objects,
            program: BTreeMap::new(),
            talkback: BTreeMap::new(),
        })
    }

    pub fn insert(&mut self, entry: CanonicalCatalogEntry) -> Result<bool> {
        let total = self.program.len().saturating_add(self.talkback.len());
        let target = match entry.lane {
            CatalogLane::Program => &mut self.program,
            CatalogLane::Talkback => &mut self.talkback,
        };
        if let Some(existing) = target.get(entry.object.key()) {
            if existing.object == entry.object {
                return Ok(false);
            }
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::NonCanonicalObject,
                "object_identity",
            ));
        }
        if total >= self.max_objects {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::Capacity,
                "catalog",
            ));
        }
        target.insert(entry.object.key().clone(), entry);
        Ok(true)
    }

    pub fn visible<'a>(
        &'a self,
        gate: &EdgeSubscribeGate,
        lease: &SubscriptionLease,
        lane: CatalogLane,
        now_unix_seconds: i64,
        maximum: usize,
    ) -> Result<Vec<&'a CanonicalCatalogEntry>> {
        if maximum == 0 || maximum > self.max_objects {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidConfiguration,
                "catalog_limit",
            ));
        }
        gate.revalidate(lease, now_unix_seconds)?;
        let source = match lane {
            CatalogLane::Program => &self.program,
            CatalogLane::Talkback => &self.talkback,
        };
        let mut visible = Vec::with_capacity(maximum.min(source.len()));
        for entry in source.values() {
            match entry.authorize(gate, lease, lane, now_unix_seconds) {
                Ok(()) => visible.push(entry),
                Err(error)
                    if matches!(
                        error.code(),
                        EdgeSubscribeErrorCode::WrongScope
                            | EdgeSubscribeErrorCode::CatalogLaneMismatch
                            | EdgeSubscribeErrorCode::SourceNotAllowed
                            | EdgeSubscribeErrorCode::AudienceNotAllowed
                            | EdgeSubscribeErrorCode::ChannelLimit
                            | EdgeSubscribeErrorCode::BitrateLimit
                            | EdgeSubscribeErrorCode::ObjectExpired
                    ) => {}
                Err(error) => return Err(error),
            }
            if visible.len() == maximum {
                break;
            }
        }
        gate.revalidate(lease, now_unix_seconds)?;
        Ok(visible)
    }

    #[must_use]
    pub fn lane_len(&self, lane: CatalogLane) -> usize {
        match lane {
            CatalogLane::Program => self.program.len(),
            CatalogLane::Talkback => self.talkback.len(),
        }
    }
}

/// Per-subscriber decoded/future delivery buffer with immediate purge semantics.
pub struct AuthorizedDeliveryBuffer {
    max_objects: usize,
    max_bytes: usize,
    buffered_bytes: usize,
    entries: VecDeque<CanonicalCatalogEntry>,
}

impl Default for AuthorizedDeliveryBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_BUFFERED_OBJECTS, DEFAULT_MAX_BUFFERED_BYTES)
            .expect("default delivery buffer bounds are valid")
    }
}

impl AuthorizedDeliveryBuffer {
    pub fn new(max_objects: usize, max_bytes: usize) -> Result<Self> {
        if max_objects == 0 || max_bytes == 0 || max_objects > DEFAULT_MAX_CATALOG_OBJECTS {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::InvalidConfiguration,
                "delivery_buffer",
            ));
        }
        Ok(Self {
            max_objects,
            max_bytes,
            buffered_bytes: 0,
            entries: VecDeque::new(),
        })
    }

    pub fn push(
        &mut self,
        gate: &EdgeSubscribeGate,
        lease: &SubscriptionLease,
        entry: CanonicalCatalogEntry,
        now_unix_seconds: i64,
    ) -> Result<()> {
        entry.authorize(gate, lease, entry.lane, now_unix_seconds)?;
        let bytes = entry.object.payload().len();
        if self.entries.len() >= self.max_objects
            || self.buffered_bytes.saturating_add(bytes) > self.max_bytes
        {
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::Capacity,
                "delivery_buffer",
            ));
        }
        self.buffered_bytes = self.buffered_bytes.saturating_add(bytes);
        self.entries.push_back(entry);
        Ok(())
    }

    pub fn pop(
        &mut self,
        gate: &EdgeSubscribeGate,
        lease: &SubscriptionLease,
        now_unix_seconds: i64,
    ) -> Result<Option<CanonicalCatalogEntry>> {
        if gate.revalidate(lease, now_unix_seconds).is_err() {
            self.clear();
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::DeliveryPurged,
                "subscription_lease",
            ));
        }
        let Some(front) = self.entries.front() else {
            return Ok(None);
        };
        if front
            .authorize(gate, lease, front.lane, now_unix_seconds)
            .is_err()
        {
            self.clear();
            return Err(EdgeSubscribeError::new(
                EdgeSubscribeErrorCode::DeliveryPurged,
                "buffered_object",
            ));
        }
        let entry = self.entries.pop_front().expect("front was present");
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_sub(entry.object.payload().len());
        Ok(Some(entry))
    }

    pub fn purge_if_invalid(
        &mut self,
        gate: &EdgeSubscribeGate,
        lease: &SubscriptionLease,
        now_unix_seconds: i64,
    ) -> usize {
        if gate.revalidate(lease, now_unix_seconds).is_ok() {
            return 0;
        }
        let purged = self.entries.len();
        self.clear();
        purged
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.buffered_bytes = 0;
    }
}

fn map_verifier_error(error: CapabilityVerifierError) -> EdgeSubscribeError {
    let code = if error.claims_code() == Some(media_object::MediaControlErrorCode::Expired) {
        EdgeSubscribeErrorCode::CapabilityExpired
    } else {
        match error.code() {
            CapabilityVerifierErrorCode::InvalidSignature => {
                EdgeSubscribeErrorCode::InvalidSignature
            }
            CapabilityVerifierErrorCode::AuthorizationRejected => {
                EdgeSubscribeErrorCode::WrongScope
            }
            CapabilityVerifierErrorCode::ReplayAdmissionRejected => {
                EdgeSubscribeErrorCode::CapabilityReplay
            }
            CapabilityVerifierErrorCode::SegmentTooLarge => {
                EdgeSubscribeErrorCode::CapabilityTooLarge
            }
            _ => EdgeSubscribeErrorCode::InvalidCapability,
        }
    };
    EdgeSubscribeError::new(code, error.field())
}
