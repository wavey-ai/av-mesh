#![cfg(feature = "media_capability_enforce_subscribe_v1")]

use std::sync::Arc;

use av_mesh::subscribe_authorization::{
    AuthorizedDeliveryBuffer, CanonicalCatalogEntry, CatalogLane, CurrentSubscribeBinding,
    CurrentSubscribeRegistry, EdgeSubscribeErrorCode, EdgeSubscribeGate, EdgeSubscribeRequest,
    InvalidationOutcome, PartitionedCatalog, SubscribeInvalidationV1, SubscriptionLease,
};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ed25519_dalek::{Signer as _, SigningKey};
use media_capability::{
    EdgeId, MediaCapabilityVerifier, VerificationKeyring, PROTECTED_ALGORITHM, PROTECTED_TOKEN_TYPE,
};
use media_object::{
    AudienceId, CapabilityId, ClockConfidence, ClockTimestamp, ContributorId, EndpointId,
    MediaCapabilityClaimsV1, MediaCapabilityClaimsV1Params, MediaCaptureDisposition, MediaClass,
    MediaConfigurationId, MediaFrameConfigurationV1, MediaFrameConfigurationV1Params,
    MediaFrameEnvelopeV1, MediaFrameEnvelopeV1Params, MediaFramePayloadFormat, MediaObject,
    ObjectKey, ObjectKind, Operation, ParticipantId, SessionId, SessionMediaIdentityV1,
    SessionMediaIdentityV1Params, SourceId, TenantId,
};
use relay_session::{
    RelayLimits, SubscriptionId, SubscriptionOp, SubscriptionRegistry, TopologyGeneration,
};

const NOW: i64 = 1_784_131_220;
const ISSUER: &str = "https://control.infidelity.io";
const AUDIENCE: &str = "av-mesh";
const KID: &str = "key_active_01";
const PROOF: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

struct Fixture {
    gate: EdgeSubscribeGate,
    registry: Arc<CurrentSubscribeRegistry>,
    signing_key: SigningKey,
    endpoint: EndpointId,
    source: SourceId,
    other_source: SourceId,
    audience: AudienceId,
    other_audience: AudienceId,
    media_class: MediaClass,
}

fn subscribe_binding(media_class: MediaClass) -> CurrentSubscribeBinding {
    CurrentSubscribeBinding::new(
        TenantId::new("ten_wavey").unwrap(),
        SessionId::new("ses_mix").unwrap(),
        9,
        14,
        3,
        7,
        Some(4),
        12,
        52,
        ParticipantId::new("par_listener").unwrap(),
        EndpointId::new("ep_listener").unwrap(),
        media_class,
        EdgeId::new("edge_lon").unwrap(),
        0,
    )
    .unwrap()
}

fn fixture(media_class: MediaClass) -> Fixture {
    let signing_key = SigningKey::from_bytes(&[7; 32]);
    let mut keyring = VerificationKeyring::new();
    keyring
        .insert_active(KID, signing_key.verifying_key().to_bytes())
        .unwrap();
    let verifier = MediaCapabilityVerifier::new(keyring, ISSUER, AUDIENCE).unwrap();
    let registry = Arc::new(CurrentSubscribeRegistry::new(0).unwrap());
    registry.install(subscribe_binding(media_class)).unwrap();
    Fixture {
        gate: EdgeSubscribeGate::new(Arc::new(verifier), Arc::clone(&registry)),
        registry,
        signing_key,
        endpoint: EndpointId::new("ep_listener").unwrap(),
        source: SourceId::new("src_mix").unwrap(),
        other_source: SourceId::new("src_aux").unwrap(),
        audience: AudienceId::new("aud_listener").unwrap(),
        other_audience: AudienceId::new("aud_other").unwrap(),
        media_class,
    }
}

fn claims_params(capability_id: &str, media_class: MediaClass) -> MediaCapabilityClaimsV1Params {
    let (source_ids, audience_ids) = if media_class == MediaClass::Talkback {
        (
            Vec::new(),
            vec![
                AudienceId::new("aud_listener").unwrap(),
                AudienceId::new("aud_other").unwrap(),
            ],
        )
    } else {
        (
            vec![
                SourceId::new("src_mix").unwrap(),
                SourceId::new("src_aux").unwrap(),
            ],
            Vec::new(),
        )
    };
    MediaCapabilityClaimsV1Params {
        issuer: ISSUER.to_owned(),
        audience: AUDIENCE.to_owned(),
        capability_id: CapabilityId::new(capability_id).unwrap(),
        tenant_id: TenantId::new("ten_wavey").unwrap(),
        session_id: SessionId::new("ses_mix").unwrap(),
        session_epoch: 9,
        media_authorization_epoch: 14,
        subject_grant_epoch: 3,
        media_policy_version: 7,
        class_authorization_epoch: Some(4),
        binding_generation: 12,
        participant_id: ParticipantId::new("par_listener").unwrap(),
        endpoint_id: EndpointId::new("ep_listener").unwrap(),
        contributor_id: None,
        operation: Operation::Subscribe,
        media_class,
        source_ids,
        audience_ids,
        take_id: None,
        topology_generation: 52,
        edge_ids: vec![EdgeId::new("edge_lon").unwrap()],
        max_channels: 2,
        max_bitrate: 512_000,
        max_datagram_bytes: 1_200,
        client_key_thumbprint: Some(PROOF.to_owned()),
        issued_at: NOW - 20,
        not_before: NOW - 20,
        expires_at: NOW + 40,
    }
}

fn sign(signing_key: &SigningKey, params: MediaCapabilityClaimsV1Params) -> String {
    let header = format!(
        r#"{{"alg":"{PROTECTED_ALGORITHM}","kid":"{KID}","typ":"{PROTECTED_TOKEN_TYPE}"}}"#
    );
    let claims = MediaCapabilityClaimsV1::new(params)
        .unwrap()
        .to_canonical_json_vec()
        .unwrap();
    let protected = URL_SAFE_NO_PAD.encode(header.as_bytes());
    let claims = URL_SAFE_NO_PAD.encode(claims);
    let input = format!("{protected}.{claims}");
    let signature = URL_SAFE_NO_PAD.encode(signing_key.sign(input.as_bytes()).to_bytes());
    format!("{input}.{signature}")
}

fn authorize<'a>(
    fixture: &'a Fixture,
    token: &'a str,
    connection_id: &'a str,
    proof: Option<&'a str>,
    source: Option<&'a SourceId>,
    audience: Option<&'a AudienceId>,
    now: i64,
) -> Result<SubscriptionLease, av_mesh::subscribe_authorization::EdgeSubscribeError> {
    fixture.gate.authorize(&EdgeSubscribeRequest {
        compact_jws: token,
        endpoint_id: &fixture.endpoint,
        media_class: fixture.media_class,
        binding_generation: 12,
        requested_source_id: source,
        requested_audience_id: audience,
        connection_id,
        authenticated_client_key_thumbprint: proof,
        now_unix_seconds: now,
    })
}

#[allow(clippy::too_many_arguments)]
fn canonical_object(
    media_class: MediaClass,
    source_id: Option<&str>,
    audience_id: Option<&str>,
    configuration_id: &str,
    sequence: u64,
    deadline: i64,
    channels: u16,
    duration_ticks: u32,
    payload_bytes: usize,
) -> MediaObject {
    let identity = SessionMediaIdentityV1::new(SessionMediaIdentityV1Params {
        tenant_id: TenantId::new("ten_wavey").unwrap(),
        session_id: SessionId::new("ses_mix").unwrap(),
        session_epoch: 9,
        participant_id: ParticipantId::new("par_producer").unwrap(),
        endpoint_id: EndpointId::new("ep_logic").unwrap(),
        contributor_id: ContributorId::new("con_logic").unwrap(),
        source_id: source_id.map(|source| SourceId::new(source).unwrap()),
        media_class,
        audience_id: audience_id.map(|audience| AudienceId::new(audience).unwrap()),
        take_id: None,
        topology_generation: 52,
    })
    .unwrap();
    let configuration = MediaFrameConfigurationV1::new(MediaFrameConfigurationV1Params {
        configuration_id: MediaConfigurationId::new(configuration_id).unwrap(),
        binding_generation: 8,
        configuration_ref: 1,
        configuration_epoch: 11,
        identity,
        payload_format: MediaFramePayloadFormat::Opus,
        capture_timebase_hz: 48_000,
        channel_count: channels,
        max_payload_bytes: 4_096,
        capture_disposition: if media_class == MediaClass::Talkback {
            MediaCaptureDisposition::MonitorOnly
        } else {
            MediaCaptureDisposition::Recordable
        },
    })
    .unwrap();
    let envelope = MediaFrameEnvelopeV1::new(MediaFrameEnvelopeV1Params {
        binding_generation: 8,
        configuration_ref: 1,
        configuration_epoch: 11,
        sequence,
        capture_pts: 48_000,
        duration_ticks,
        payload_bytes: u32::try_from(payload_bytes).unwrap(),
    })
    .unwrap();
    let payload = vec![0x5a; payload_bytes];
    let key = ObjectKey::for_payload(
        "ten_wavey",
        "77",
        configuration_id,
        8,
        1,
        sequence,
        1,
        &payload,
    )
    .unwrap();
    let deadline = ClockTimestamp::new(
        deadline * 1_000_000_000,
        "media-capability:issuer",
        ClockConfidence::unknown(),
    )
    .unwrap();
    MediaObject::builder(key, ObjectKind::Media, payload)
        .with_configuration_epoch(11)
        .with_deadline(deadline)
        .with_metadata("media-control-contract", b"v1".to_vec())
        .with_metadata("media-operation-v1", b"publish".to_vec())
        .with_metadata(
            "media-frame-configuration-v1",
            configuration.to_canonical_json_vec().unwrap(),
        )
        .with_metadata(
            "media-frame-envelope-v1",
            envelope.to_canonical_json_vec().unwrap(),
        )
        .build()
        .unwrap()
}

fn rebuild_object(
    object: &MediaObject,
    key: ObjectKey,
    operation: &[u8],
    configuration: Vec<u8>,
    include_deadline: bool,
) -> MediaObject {
    let mut builder = MediaObject::builder(key, ObjectKind::Media, object.payload().to_vec())
        .with_configuration_epoch(object.configuration_epoch())
        .with_metadata("media-control-contract", b"v1".to_vec())
        .with_metadata("media-operation-v1", operation.to_vec())
        .with_metadata("media-frame-configuration-v1", configuration)
        .with_metadata(
            "media-frame-envelope-v1",
            object
                .metadata()
                .get("media-frame-envelope-v1")
                .unwrap()
                .clone(),
        );
    if include_deadline {
        builder = builder.with_deadline(object.deadline().unwrap().clone());
    }
    builder.build().unwrap()
}

fn source_entry(configuration: &str, source: &str, sequence: u64) -> CanonicalCatalogEntry {
    CanonicalCatalogEntry::from_media_object(canonical_object(
        MediaClass::Source,
        Some(source),
        None,
        configuration,
        sequence,
        NOW + 80,
        2,
        960,
        480,
    ))
    .unwrap()
}

#[test]
fn exact_signed_scope_round_trips_from_p03_and_drives_exact_relay_scope() {
    let fixture = fixture(MediaClass::Source);
    let token = sign(
        &fixture.signing_key,
        claims_params("cap_subscribe_exact", MediaClass::Source),
    );
    let lease = authorize(
        &fixture,
        &token,
        "conn-exact",
        Some(PROOF),
        Some(&fixture.source),
        None,
        NOW,
    )
    .unwrap();
    assert!(lease.matches_admission("cap_subscribe_exact", "conn-exact"));
    assert!(!lease.matches_admission("cap_subscribe_exact", "conn-other"));
    assert_eq!(lease.max_datagram_bytes(), 1_200);
    lease.authorize_datagram_bytes(1_200).unwrap();
    assert_eq!(
        lease.authorize_datagram_bytes(1_201).unwrap_err().code(),
        EdgeSubscribeErrorCode::DatagramLimit
    );

    let object = canonical_object(
        MediaClass::Source,
        Some("src_mix"),
        None,
        "cfg_source_1",
        9,
        NOW + 80,
        2,
        960,
        480,
    );
    let wire = media_object::encode(&object).unwrap();
    let entry =
        CanonicalCatalogEntry::from_media_object(media_object::decode(&wire).unwrap()).unwrap();
    entry
        .authorize(&fixture.gate, &lease, CatalogLane::Program, NOW)
        .unwrap();

    let change = fixture
        .gate
        .relay_subscription_change(
            &lease,
            &entry,
            SubscriptionId::new(41).unwrap(),
            SubscriptionOp::Subscribe,
            NOW,
        )
        .unwrap();
    assert_eq!(change.generation.get(), 52);
    assert_eq!(change.scope.tenant(), "ten_wavey");
    assert_eq!(change.scope.stream(), "77");
    assert_eq!(change.scope.track(), Some("cfg_source_1"));
    assert!(change.scope.matches(entry.object().key()));
    let other_key = ObjectKey::for_payload(
        "ten_wavey",
        "77",
        "cfg_source_2",
        8,
        1,
        9,
        1,
        entry.object().payload(),
    )
    .unwrap();
    assert!(!change.scope.matches(&other_key));

    let mut relay =
        SubscriptionRegistry::new(TopologyGeneration::new(52).unwrap(), RelayLimits::default())
            .unwrap();
    relay.apply(change.clone()).unwrap();
    assert_eq!(relay.len(), 1);
    let unsubscribe = fixture
        .gate
        .relay_subscription_change(&lease, &entry, change.id, SubscriptionOp::Unsubscribe, NOW)
        .unwrap();
    relay.apply(unsubscribe).unwrap();
    assert!(relay.is_empty());
}

#[test]
fn wrong_scope_signature_expiry_replay_and_proof_fail_closed() {
    let fixture = fixture(MediaClass::Source);

    let mut params = claims_params("cap_wrong_edge", MediaClass::Source);
    params.edge_ids = vec![EdgeId::new("edge_other").unwrap()];
    let token = sign(&fixture.signing_key, params);
    assert_eq!(
        authorize(
            &fixture,
            &token,
            "conn-edge",
            Some(PROOF),
            Some(&fixture.source),
            None,
            NOW,
        )
        .unwrap_err()
        .code(),
        EdgeSubscribeErrorCode::WrongScope
    );

    let mut params = claims_params("cap_wrong_generation", MediaClass::Source);
    params.topology_generation = 51;
    let token = sign(&fixture.signing_key, params);
    assert_eq!(
        authorize(
            &fixture,
            &token,
            "conn-generation",
            Some(PROOF),
            Some(&fixture.source),
            None,
            NOW,
        )
        .unwrap_err()
        .code(),
        EdgeSubscribeErrorCode::WrongScope
    );

    let mut params = claims_params("cap_wrong_operation", MediaClass::Source);
    params.operation = Operation::AcknowledgePlayout;
    let token = sign(&fixture.signing_key, params);
    assert_eq!(
        authorize(
            &fixture,
            &token,
            "conn-operation",
            Some(PROOF),
            Some(&fixture.source),
            None,
            NOW,
        )
        .unwrap_err()
        .code(),
        EdgeSubscribeErrorCode::WrongScope
    );

    let token = sign(
        &fixture.signing_key,
        claims_params("cap_bad_signature", MediaClass::Source),
    );
    let (input, signature) = token.rsplit_once('.').unwrap();
    let mut signature = URL_SAFE_NO_PAD.decode(signature).unwrap();
    signature[0] ^= 0x01;
    let token = format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature));
    assert_eq!(
        authorize(
            &fixture,
            &token,
            "conn-signature",
            Some(PROOF),
            Some(&fixture.source),
            None,
            NOW,
        )
        .unwrap_err()
        .code(),
        EdgeSubscribeErrorCode::InvalidSignature
    );

    let mut params = claims_params("cap_expired", MediaClass::Source);
    params.issued_at = NOW - 60;
    params.not_before = NOW - 60;
    params.expires_at = NOW;
    let token = sign(&fixture.signing_key, params);
    assert_eq!(
        authorize(
            &fixture,
            &token,
            "conn-expired",
            Some(PROOF),
            Some(&fixture.source),
            None,
            NOW,
        )
        .unwrap_err()
        .code(),
        EdgeSubscribeErrorCode::CapabilityExpired
    );

    let mut params = claims_params("cap_forbidden_source", MediaClass::Source);
    params.source_ids = vec![fixture.source.clone()];
    let token = sign(&fixture.signing_key, params);
    let forbidden = SourceId::new("src_forbidden").unwrap();
    assert_eq!(
        authorize(
            &fixture,
            &token,
            "conn-source",
            Some(PROOF),
            Some(&forbidden),
            None,
            NOW,
        )
        .unwrap_err()
        .code(),
        EdgeSubscribeErrorCode::WrongScope
    );

    let token = sign(
        &fixture.signing_key,
        claims_params("cap_proof_rollback", MediaClass::Source),
    );
    assert_eq!(
        authorize(
            &fixture,
            &token,
            "conn-proof",
            None,
            Some(&fixture.source),
            None,
            NOW,
        )
        .unwrap_err()
        .code(),
        EdgeSubscribeErrorCode::ProofRequired
    );
    assert_eq!(
        authorize(
            &fixture,
            &token,
            "conn-proof",
            Some("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
            Some(&fixture.source),
            None,
            NOW,
        )
        .unwrap_err()
        .code(),
        EdgeSubscribeErrorCode::ProofMismatch
    );
    authorize(
        &fixture,
        &token,
        "conn-proof",
        Some(PROOF),
        Some(&fixture.source),
        None,
        NOW,
    )
    .unwrap();
    assert_eq!(
        authorize(
            &fixture,
            &token,
            "conn-replay",
            Some(PROOF),
            Some(&fixture.source),
            None,
            NOW,
        )
        .unwrap_err()
        .code(),
        EdgeSubscribeErrorCode::CapabilityReplay
    );

    let token = sign(
        &fixture.signing_key,
        claims_params("cap_missing_selector", MediaClass::Source),
    );
    assert_eq!(
        authorize(
            &fixture,
            &token,
            "conn-selector",
            Some(PROOF),
            None,
            None,
            NOW,
        )
        .unwrap_err()
        .code(),
        EdgeSubscribeErrorCode::InvalidConfiguration
    );
}

#[test]
fn partitioned_catalog_filters_sources_and_talkback_audiences() {
    let source_fixture = fixture(MediaClass::Source);
    let mut params = claims_params("cap_catalog_source", MediaClass::Source);
    params.source_ids = vec![source_fixture.source.clone()];
    let token = sign(&source_fixture.signing_key, params);
    let source_lease = authorize(
        &source_fixture,
        &token,
        "conn-catalog-source",
        Some(PROOF),
        Some(&source_fixture.source),
        None,
        NOW,
    )
    .unwrap();

    let mut catalog = PartitionedCatalog::new(8).unwrap();
    catalog
        .insert(source_entry("cfg_source_mix", "src_mix", 1))
        .unwrap();
    catalog
        .insert(source_entry("cfg_source_aux", "src_aux", 2))
        .unwrap();
    let talkback = CanonicalCatalogEntry::from_media_object(canonical_object(
        MediaClass::Talkback,
        None,
        Some("aud_listener"),
        "cfg_talkback_listener",
        3,
        NOW + 80,
        1,
        960,
        240,
    ))
    .unwrap();
    assert_eq!(
        talkback
            .authorize(
                &source_fixture.gate,
                &source_lease,
                CatalogLane::Program,
                NOW,
            )
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::CatalogLaneMismatch
    );
    catalog.insert(talkback).unwrap();
    assert_eq!(catalog.lane_len(CatalogLane::Program), 2);
    assert_eq!(catalog.lane_len(CatalogLane::Talkback), 1);
    let visible = catalog
        .visible(
            &source_fixture.gate,
            &source_lease,
            CatalogLane::Program,
            NOW,
            8,
        )
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        visible[0].identity().source_id().unwrap().as_str(),
        "src_mix"
    );
    assert!(catalog
        .visible(
            &source_fixture.gate,
            &source_lease,
            CatalogLane::Talkback,
            NOW,
            8,
        )
        .unwrap()
        .is_empty());

    let talkback_fixture = fixture(MediaClass::Talkback);
    let mut params = claims_params("cap_catalog_talkback", MediaClass::Talkback);
    params.audience_ids = vec![talkback_fixture.audience.clone()];
    let token = sign(&talkback_fixture.signing_key, params);
    let talkback_lease = authorize(
        &talkback_fixture,
        &token,
        "conn-catalog-talkback",
        Some(PROOF),
        None,
        Some(&talkback_fixture.audience),
        NOW,
    )
    .unwrap();
    let mut talkback_catalog = PartitionedCatalog::new(4).unwrap();
    for (audience, configuration, sequence) in [
        ("aud_listener", "cfg_tb_listener", 4),
        ("aud_other", "cfg_tb_other", 5),
    ] {
        talkback_catalog
            .insert(
                CanonicalCatalogEntry::from_media_object(canonical_object(
                    MediaClass::Talkback,
                    None,
                    Some(audience),
                    configuration,
                    sequence,
                    NOW + 80,
                    1,
                    960,
                    240,
                ))
                .unwrap(),
            )
            .unwrap();
    }
    let visible = talkback_catalog
        .visible(
            &talkback_fixture.gate,
            &talkback_lease,
            CatalogLane::Talkback,
            NOW,
            4,
        )
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        visible[0].identity().audience_id().unwrap().as_str(),
        "aud_listener"
    );
}

#[test]
fn canonical_metadata_operation_identity_and_deadline_tampering_fail_closed() {
    let object = canonical_object(
        MediaClass::Source,
        Some("src_mix"),
        None,
        "cfg_tamper",
        21,
        NOW + 80,
        2,
        960,
        480,
    );
    let configuration = object
        .metadata()
        .get("media-frame-configuration-v1")
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(configuration).unwrap();
    let noncanonical = serde_json::to_vec_pretty(&value).unwrap();
    let rebuilt = rebuild_object(
        &object,
        object.key().clone(),
        b"publish",
        noncanonical,
        true,
    );
    assert_eq!(
        CanonicalCatalogEntry::from_media_object(rebuilt)
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::NonCanonicalObject
    );

    let extra_metadata = MediaObject::builder(
        object.key().clone(),
        ObjectKind::Media,
        object.payload().to_vec(),
    )
    .with_configuration_epoch(object.configuration_epoch())
    .with_deadline(object.deadline().unwrap().clone())
    .with_metadata("media-control-contract", b"v1".to_vec())
    .with_metadata("media-operation-v1", b"publish".to_vec())
    .with_metadata("media-frame-configuration-v1", configuration.clone())
    .with_metadata(
        "media-frame-envelope-v1",
        object
            .metadata()
            .get("media-frame-envelope-v1")
            .unwrap()
            .clone(),
    )
    .with_metadata("untrusted-extra", b"poison".to_vec())
    .build()
    .unwrap();
    assert_eq!(
        CanonicalCatalogEntry::from_media_object(extra_metadata)
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::MalformedObject
    );

    let rebuilt = rebuild_object(
        &object,
        object.key().clone(),
        b"upload_take",
        configuration.clone(),
        true,
    );
    assert_eq!(
        CanonicalCatalogEntry::from_media_object(rebuilt)
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::MalformedObject
    );

    let wrong_key = ObjectKey::for_payload(
        "ten_wavey",
        "77",
        "cfg_wrong",
        8,
        1,
        21,
        1,
        object.payload(),
    )
    .unwrap();
    let rebuilt = rebuild_object(&object, wrong_key, b"publish", configuration.clone(), true);
    assert_eq!(
        CanonicalCatalogEntry::from_media_object(rebuilt)
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::NonCanonicalObject
    );

    let rebuilt = rebuild_object(
        &object,
        object.key().clone(),
        b"publish",
        configuration.clone(),
        false,
    );
    assert_eq!(
        CanonicalCatalogEntry::from_media_object(rebuilt)
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::NonCanonicalObject
    );
}

#[test]
fn ordered_invalidation_gaps_and_snapshot_recovery_fence_old_leases() {
    let invalidation_fixture = fixture(MediaClass::Source);
    let token = sign(
        &invalidation_fixture.signing_key,
        claims_params("cap_invalidation", MediaClass::Source),
    );
    let lease = authorize(
        &invalidation_fixture,
        &token,
        "conn-invalidation",
        Some(PROOF),
        Some(&invalidation_fixture.source),
        None,
        NOW,
    )
    .unwrap();
    let event = SubscribeInvalidationV1 {
        delivery_sequence: 1,
        session_id: SessionId::new("ses_mix").unwrap(),
        session_epoch: 9,
        media_authorization_epoch: 14,
        media_policy_version: 7,
        endpoint_id: Some(invalidation_fixture.endpoint.clone()),
        subject_grant_epoch: Some(4),
    };
    assert_eq!(
        invalidation_fixture
            .registry
            .apply_invalidation(event)
            .unwrap(),
        InvalidationOutcome::Applied {
            invalidated_bindings: 1
        }
    );
    assert_eq!(
        invalidation_fixture
            .gate
            .revalidate(&lease, NOW)
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::RevokedBinding
    );
    assert_eq!(
        invalidation_fixture
            .registry
            .apply_invalidation(SubscribeInvalidationV1 {
                delivery_sequence: 1,
                session_id: SessionId::new("ses_mix").unwrap(),
                session_epoch: 9,
                media_authorization_epoch: 14,
                media_policy_version: 7,
                endpoint_id: Some(invalidation_fixture.endpoint.clone()),
                subject_grant_epoch: Some(4),
            })
            .unwrap(),
        InvalidationOutcome::Duplicate
    );

    let gap_fixture = fixture(MediaClass::Source);
    let token = sign(
        &gap_fixture.signing_key,
        claims_params("cap_before_gap", MediaClass::Source),
    );
    let old_lease = authorize(
        &gap_fixture,
        &token,
        "conn-before-gap",
        Some(PROOF),
        Some(&gap_fixture.source),
        None,
        NOW,
    )
    .unwrap();
    assert_eq!(
        gap_fixture
            .registry
            .apply_invalidation(SubscribeInvalidationV1 {
                delivery_sequence: 2,
                session_id: SessionId::new("ses_mix").unwrap(),
                session_epoch: 9,
                media_authorization_epoch: 14,
                media_policy_version: 7,
                endpoint_id: None,
                subject_grant_epoch: None,
            })
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::InvalidationGap
    );
    assert_eq!(gap_fixture.registry.acknowledged_sequence(), None);
    assert_eq!(
        gap_fixture
            .gate
            .revalidate(&old_lease, NOW)
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::InvalidationGap
    );
    assert_eq!(
        gap_fixture
            .registry
            .install_snapshot(0, vec![subscribe_binding(MediaClass::Source)])
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::InvalidConfiguration
    );
    gap_fixture
        .registry
        .install_snapshot(2, vec![subscribe_binding(MediaClass::Source)])
        .unwrap();
    assert_eq!(gap_fixture.registry.acknowledged_sequence(), Some(2));
    assert_eq!(
        gap_fixture
            .gate
            .revalidate(&old_lease, NOW)
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::RevokedBinding
    );
    let token = sign(
        &gap_fixture.signing_key,
        claims_params("cap_after_snapshot", MediaClass::Source),
    );
    authorize(
        &gap_fixture,
        &token,
        "conn-after-snapshot",
        Some(PROOF),
        Some(&gap_fixture.source),
        None,
        NOW,
    )
    .unwrap();
}

#[test]
fn expiry_limits_capacity_and_buffer_purge_are_enforced_at_delivery_time() {
    let fixture = fixture(MediaClass::Source);
    let token = sign(
        &fixture.signing_key,
        claims_params("cap_delivery", MediaClass::Source),
    );
    let lease = authorize(
        &fixture,
        &token,
        "conn-delivery",
        Some(PROOF),
        Some(&fixture.source),
        None,
        NOW,
    )
    .unwrap();
    let mut buffer = AuthorizedDeliveryBuffer::new(4, 4_096).unwrap();
    buffer
        .push(
            &fixture.gate,
            &lease,
            source_entry("cfg_buffer_1", "src_mix", 31),
            NOW,
        )
        .unwrap();
    buffer
        .push(
            &fixture.gate,
            &lease,
            source_entry("cfg_buffer_2", "src_mix", 32),
            NOW,
        )
        .unwrap();
    assert_eq!(buffer.purge_if_invalid(&fixture.gate, &lease, NOW + 40), 2);
    assert!(buffer.is_empty());

    let expired_object = CanonicalCatalogEntry::from_media_object(canonical_object(
        MediaClass::Source,
        Some("src_mix"),
        None,
        "cfg_expired_object",
        33,
        NOW,
        2,
        960,
        480,
    ))
    .unwrap();
    assert_eq!(
        expired_object
            .authorize(&fixture.gate, &lease, CatalogLane::Program, NOW)
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::ObjectExpired
    );

    let mut params = claims_params("cap_channel_limit", MediaClass::Source);
    params.max_channels = 1;
    let token = sign(&fixture.signing_key, params);
    let channel_lease = authorize(
        &fixture,
        &token,
        "conn-channels",
        Some(PROOF),
        Some(&fixture.source),
        None,
        NOW,
    )
    .unwrap();
    assert_eq!(
        source_entry("cfg_channels", "src_mix", 34)
            .authorize(&fixture.gate, &channel_lease, CatalogLane::Program, NOW,)
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::ChannelLimit
    );

    let mut params = claims_params("cap_bitrate_limit", MediaClass::Source);
    params.max_bitrate = 100_000;
    let token = sign(&fixture.signing_key, params);
    let bitrate_lease = authorize(
        &fixture,
        &token,
        "conn-bitrate",
        Some(PROOF),
        Some(&fixture.source),
        None,
        NOW,
    )
    .unwrap();
    assert_eq!(
        source_entry("cfg_bitrate", "src_mix", 35)
            .authorize(&fixture.gate, &bitrate_lease, CatalogLane::Program, NOW,)
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::BitrateLimit
    );

    let mut bounded_buffer = AuthorizedDeliveryBuffer::new(1, 1_000).unwrap();
    bounded_buffer
        .push(
            &fixture.gate,
            &lease,
            source_entry("cfg_bound_1", "src_mix", 36),
            NOW,
        )
        .unwrap();
    assert_eq!(
        bounded_buffer
            .push(
                &fixture.gate,
                &lease,
                source_entry("cfg_bound_2", "src_mix", 37),
                NOW,
            )
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::Capacity
    );
    let mut catalog = PartitionedCatalog::new(1).unwrap();
    catalog
        .insert(source_entry("cfg_catalog_bound_1", "src_mix", 38))
        .unwrap();
    assert_eq!(
        catalog
            .insert(source_entry("cfg_catalog_bound_2", "src_mix", 39))
            .unwrap_err()
            .code(),
        EdgeSubscribeErrorCode::Capacity
    );
}

#[test]
fn diagnostics_and_metrics_do_not_expose_tokens_or_subject_identifiers() {
    let fixture = fixture(MediaClass::Source);
    let token = "secret-token-that-must-not-appear";
    let request = EdgeSubscribeRequest {
        compact_jws: token,
        endpoint_id: &fixture.endpoint,
        media_class: MediaClass::Source,
        binding_generation: 12,
        requested_source_id: Some(&fixture.source),
        requested_audience_id: None,
        connection_id: "secret-connection",
        authenticated_client_key_thumbprint: Some(PROOF),
        now_unix_seconds: NOW,
    };
    let debug = format!("{request:?}");
    assert!(!debug.contains(token));
    assert!(!debug.contains(fixture.endpoint.as_str()));
    assert!(!debug.contains("secret-connection"));
    fixture.gate.authorize(&request).unwrap_err();
    let metrics = fixture.gate.prometheus_metrics();
    assert!(!metrics.contains(token));
    assert!(!metrics.contains(fixture.endpoint.as_str()));
    assert!(metrics.contains("decision=\"reject\""));
    assert!(metrics.contains("reason=\"invalid_capability\""));
}

#[test]
fn fixture_scope_values_are_distinct() {
    let source = fixture(MediaClass::Source);
    assert_ne!(source.source, source.other_source);
    let talkback = fixture(MediaClass::Talkback);
    assert_ne!(talkback.audience, talkback.other_audience);
}
