//! Ephemeral audience-indexed talkback fan-out.
//!
//! This module intentionally accepts only `EphemeralTalkbackFrameV1`. It has no
//! cache, object, HLS, replica, archive, or backfill adapter.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use talkback_media::EphemeralTalkbackFrameV1;

pub const TALKBACK_ACK_FRESHNESS_US: u64 = 3_000_000;
const SEQUENCE_WINDOW_BITS: u64 = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TalkbackLaneErrorCode {
    InvalidConfiguration,
    RouteUnavailable,
    AuthorizationEpoch,
    PublisherNotAllowed,
    SubscriberNotAllowed,
    FrameReplay,
    FrameExpired,
    PlayoutBeyondDelivery,
    StaleAcknowledgement,
}

#[derive(Clone, Eq, PartialEq)]
pub struct TalkbackLaneError {
    code: TalkbackLaneErrorCode,
    field: &'static str,
}

impl TalkbackLaneError {
    const fn new(code: TalkbackLaneErrorCode, field: &'static str) -> Self {
        Self { code, field }
    }

    #[must_use]
    pub const fn code(&self) -> TalkbackLaneErrorCode {
        self.code
    }

    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Debug for TalkbackLaneError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TalkbackLaneError")
            .field("code", &self.code)
            .field("field", &self.field)
            .finish()
    }
}

impl fmt::Display for TalkbackLaneError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: talkback frame rejected", self.code)
    }
}

impl std::error::Error for TalkbackLaneError {}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TalkbackPublisherV1 {
    pub participant_id: String,
    pub endpoint_id: String,
    pub subject_grant_epoch: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TalkbackSubscriberV1 {
    pub participant_id: String,
    pub endpoint_id: String,
}

#[derive(Clone)]
pub struct TalkbackRoutingSnapshotV1Params {
    pub session_id: String,
    pub session_epoch: u64,
    pub media_authorization_epoch: u64,
    pub talkback_epoch: u64,
    pub policy_version: u64,
    pub audience_id: String,
    pub publishers: Vec<TalkbackPublisherV1>,
    pub subscribers: Vec<TalkbackSubscriberV1>,
}

/// Immutable cue membership installed by authenticated control state.
#[derive(Clone, Eq, PartialEq)]
pub struct TalkbackRoutingSnapshotV1 {
    session_id: String,
    session_epoch: u64,
    media_authorization_epoch: u64,
    talkback_epoch: u64,
    policy_version: u64,
    audience_id: String,
    publishers: BTreeSet<TalkbackPublisherV1>,
    subscribers: BTreeMap<String, TalkbackSubscriberV1>,
}

impl TalkbackRoutingSnapshotV1 {
    pub fn new(params: TalkbackRoutingSnapshotV1Params) -> Result<Self, TalkbackLaneError> {
        validate_identifier("session_id", &params.session_id)?;
        validate_identifier("audience_id", &params.audience_id)?;
        if params.session_epoch == 0
            || params.media_authorization_epoch == 0
            || params.talkback_epoch == 0
            || params.policy_version == 0
        {
            return Err(TalkbackLaneError::new(
                TalkbackLaneErrorCode::InvalidConfiguration,
                "authorization_generation",
            ));
        }
        if params.publishers.is_empty()
            || params.publishers.len() > 64
            || params.subscribers.len() > 256
        {
            return Err(TalkbackLaneError::new(
                TalkbackLaneErrorCode::InvalidConfiguration,
                "membership",
            ));
        }
        let mut publishers = BTreeSet::new();
        let mut publisher_endpoints = BTreeSet::new();
        for publisher in params.publishers {
            validate_identifier("publisher_participant_id", &publisher.participant_id)?;
            validate_identifier("publisher_endpoint_id", &publisher.endpoint_id)?;
            if publisher.subject_grant_epoch == 0
                || !publisher_endpoints.insert(publisher.endpoint_id.clone())
                || !publishers.insert(publisher)
            {
                return Err(TalkbackLaneError::new(
                    TalkbackLaneErrorCode::InvalidConfiguration,
                    "publisher",
                ));
            }
        }
        let mut subscribers = BTreeMap::new();
        for subscriber in params.subscribers {
            validate_identifier("subscriber_participant_id", &subscriber.participant_id)?;
            validate_identifier("subscriber_endpoint_id", &subscriber.endpoint_id)?;
            if subscribers
                .insert(subscriber.endpoint_id.clone(), subscriber)
                .is_some()
            {
                return Err(TalkbackLaneError::new(
                    TalkbackLaneErrorCode::InvalidConfiguration,
                    "subscriber_endpoint_id",
                ));
            }
        }
        Ok(Self {
            session_id: params.session_id,
            session_epoch: params.session_epoch,
            media_authorization_epoch: params.media_authorization_epoch,
            talkback_epoch: params.talkback_epoch,
            policy_version: params.policy_version,
            audience_id: params.audience_id,
            publishers,
            subscribers,
        })
    }

    fn audience_key(&self) -> AudienceKey {
        AudienceKey {
            session_id: self.session_id.clone(),
            audience_id: self.audience_id.clone(),
        }
    }

    fn publisher_allowed(&self, frame: &EphemeralTalkbackFrameV1) -> bool {
        let frame = frame.frame();
        self.publishers.contains(&TalkbackPublisherV1 {
            participant_id: frame.publisher_participant_id().to_owned(),
            endpoint_id: frame.publisher_endpoint_id().to_owned(),
            subject_grant_epoch: frame.subject_grant_epoch(),
        })
    }
}

impl fmt::Debug for TalkbackRoutingSnapshotV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TalkbackRoutingSnapshotV1")
            .field("session_epoch", &self.session_epoch)
            .field("talkback_epoch", &self.talkback_epoch)
            .field("policy_version", &self.policy_version)
            .field("publisher_count", &self.publishers.len())
            .field("subscriber_count", &self.subscribers.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TalkbackLaneConfig {
    pub max_frames_per_publisher_receiver: usize,
    pub acknowledgement_freshness_us: u64,
}

impl Default for TalkbackLaneConfig {
    fn default() -> Self {
        Self {
            max_frames_per_publisher_receiver: 20,
            acknowledgement_freshness_us: TALKBACK_ACK_FRESHNESS_US,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TalkbackFanoutV1 {
    pub queued_receiver_endpoints: Vec<String>,
    pub mix_minus_excluded_endpoints: Vec<String>,
}

/// Identity-free live-lane health suitable for bounded metrics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TalkbackLaneHealthV1 {
    pub active_audiences: usize,
    pub active_publishers: usize,
    pub active_subscribers: usize,
    pub queued_frames: usize,
    pub admitted_frames: u64,
    pub queued_deliveries: u64,
    pub mix_minus_exclusions: u64,
    pub queue_evictions: u64,
    pub expired_rejections: u64,
    pub replay_rejections: u64,
    pub authorization_rejections: u64,
    pub pulled_frames: u64,
    pub accepted_playout_acknowledgements: u64,
    pub rejected_playout_acknowledgements: u64,
    pub purged_frames: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TalkbackPlayoutAckV1 {
    pub session_id: String,
    pub receiver_participant_id: String,
    pub receiver_endpoint_id: String,
    pub publisher_participant_id: String,
    pub publisher_endpoint_id: String,
    pub audience_id: String,
    pub sequence: u64,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct AudienceKey {
    session_id: String,
    audience_id: String,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct PublisherKey {
    audience: AudienceKey,
    participant_id: String,
    endpoint_id: String,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct DeliveryKey {
    publisher: PublisherKey,
    receiver_participant_id: String,
    receiver_endpoint_id: String,
}

#[derive(Clone, Copy)]
struct SequenceWindow {
    highest: u64,
    seen: u128,
}

impl SequenceWindow {
    const fn first(sequence: u64) -> Self {
        Self {
            highest: sequence,
            seen: 1,
        }
    }

    fn admit(&mut self, sequence: u64) -> bool {
        if sequence > self.highest {
            let shift = sequence - self.highest;
            self.seen = if shift >= SEQUENCE_WINDOW_BITS {
                1
            } else {
                (self.seen << shift) | 1
            };
            self.highest = sequence;
            return true;
        }
        let distance = self.highest - sequence;
        if distance >= SEQUENCE_WINDOW_BITS {
            return false;
        }
        let bit = 1_u128 << distance;
        if self.seen & bit != 0 {
            return false;
        }
        self.seen |= bit;
        true
    }
}

#[derive(Clone, Copy)]
struct PlayedHighWater {
    delivered_sequence: u64,
    acknowledged_sequence: Option<u64>,
    acknowledged_at_unix_us: Option<u64>,
}

/// Bounded live relay state. All members are process-memory-only.
pub struct TalkbackLane {
    config: TalkbackLaneConfig,
    routes: BTreeMap<AudienceKey, Arc<TalkbackRoutingSnapshotV1>>,
    sequences: BTreeMap<PublisherKey, SequenceWindow>,
    queues: BTreeMap<DeliveryKey, VecDeque<EphemeralTalkbackFrameV1>>,
    played: BTreeMap<DeliveryKey, PlayedHighWater>,
    counters: TalkbackLaneHealthV1,
}

impl TalkbackLane {
    pub fn new(config: TalkbackLaneConfig) -> Result<Self, TalkbackLaneError> {
        if config.max_frames_per_publisher_receiver == 0
            || config.max_frames_per_publisher_receiver > 256
            || config.acknowledgement_freshness_us == 0
            || config.acknowledgement_freshness_us > TALKBACK_ACK_FRESHNESS_US
        {
            return Err(TalkbackLaneError::new(
                TalkbackLaneErrorCode::InvalidConfiguration,
                "lane_config",
            ));
        }
        Ok(Self {
            config,
            routes: BTreeMap::new(),
            sequences: BTreeMap::new(),
            queues: BTreeMap::new(),
            played: BTreeMap::new(),
            counters: TalkbackLaneHealthV1::default(),
        })
    }

    /// Atomically replace one audience snapshot and purge all prior live state.
    pub fn install_snapshot(&mut self, snapshot: TalkbackRoutingSnapshotV1) {
        let key = snapshot.audience_key();
        let purged = self.purge_audience(&key) as u64;
        self.counters.purged_frames = self.counters.purged_frames.saturating_add(purged);
        self.routes.insert(key, Arc::new(snapshot));
    }

    pub fn remove_audience(&mut self, session_id: &str, audience_id: &str) -> bool {
        let key = AudienceKey {
            session_id: session_id.to_owned(),
            audience_id: audience_id.to_owned(),
        };
        let removed = self.routes.remove(&key).is_some();
        let purged = self.purge_audience(&key) as u64;
        self.counters.purged_frames = self.counters.purged_frames.saturating_add(purged);
        removed
    }

    pub fn end_session(&mut self, session_id: &str) {
        let keys = self
            .routes
            .keys()
            .filter(|key| key.session_id == session_id)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            self.routes.remove(&key);
            let purged = self.purge_audience(&key) as u64;
            self.counters.purged_frames = self.counters.purged_frames.saturating_add(purged);
        }
    }

    pub fn publish(
        &mut self,
        frame: EphemeralTalkbackFrameV1,
        now_unix_us: u64,
    ) -> Result<TalkbackFanoutV1, TalkbackLaneError> {
        let result = self.publish_inner(frame, now_unix_us);
        match &result {
            Ok(fanout) => {
                self.counters.admitted_frames = self.counters.admitted_frames.saturating_add(1);
                self.counters.queued_deliveries = self
                    .counters
                    .queued_deliveries
                    .saturating_add(fanout.queued_receiver_endpoints.len() as u64);
                self.counters.mix_minus_exclusions = self
                    .counters
                    .mix_minus_exclusions
                    .saturating_add(fanout.mix_minus_excluded_endpoints.len() as u64);
            }
            Err(error) => match error.code {
                TalkbackLaneErrorCode::FrameExpired => {
                    self.counters.expired_rejections =
                        self.counters.expired_rejections.saturating_add(1);
                }
                TalkbackLaneErrorCode::FrameReplay => {
                    self.counters.replay_rejections =
                        self.counters.replay_rejections.saturating_add(1);
                }
                TalkbackLaneErrorCode::RouteUnavailable
                | TalkbackLaneErrorCode::AuthorizationEpoch
                | TalkbackLaneErrorCode::PublisherNotAllowed => {
                    self.counters.authorization_rejections =
                        self.counters.authorization_rejections.saturating_add(1);
                }
                _ => {}
            },
        }
        result
    }

    fn publish_inner(
        &mut self,
        frame: EphemeralTalkbackFrameV1,
        now_unix_us: u64,
    ) -> Result<TalkbackFanoutV1, TalkbackLaneError> {
        if frame.is_expired(now_unix_us) {
            return Err(TalkbackLaneError::new(
                TalkbackLaneErrorCode::FrameExpired,
                "deadline",
            ));
        }
        let audience = AudienceKey {
            session_id: frame.frame().session_id().to_owned(),
            audience_id: frame.frame().audience_id().to_owned(),
        };
        let route = Arc::clone(self.routes.get(&audience).ok_or_else(|| {
            TalkbackLaneError::new(TalkbackLaneErrorCode::RouteUnavailable, "audience")
        })?);
        if route.session_epoch != frame.frame().session_epoch()
            || route.media_authorization_epoch != frame.frame().media_authorization_epoch()
            || route.talkback_epoch != frame.frame().talkback_epoch()
            || route.policy_version != frame.frame().policy_version()
        {
            return Err(TalkbackLaneError::new(
                TalkbackLaneErrorCode::AuthorizationEpoch,
                "authorization_generation",
            ));
        }
        if !route.publisher_allowed(&frame) {
            return Err(TalkbackLaneError::new(
                TalkbackLaneErrorCode::PublisherNotAllowed,
                "publisher",
            ));
        }
        let publisher = PublisherKey {
            audience,
            participant_id: frame.frame().publisher_participant_id().to_owned(),
            endpoint_id: frame.frame().publisher_endpoint_id().to_owned(),
        };
        if let Some(window) = self.sequences.get_mut(&publisher) {
            if !window.admit(frame.frame().sequence()) {
                return Err(TalkbackLaneError::new(
                    TalkbackLaneErrorCode::FrameReplay,
                    "sequence",
                ));
            }
        } else {
            self.sequences.insert(
                publisher.clone(),
                SequenceWindow::first(frame.frame().sequence()),
            );
        }

        let mut queued = Vec::new();
        let mut excluded = Vec::new();
        for subscriber in route.subscribers.values() {
            if subscriber.participant_id == publisher.participant_id {
                excluded.push(subscriber.endpoint_id.clone());
                continue;
            }
            let key = DeliveryKey {
                publisher: publisher.clone(),
                receiver_participant_id: subscriber.participant_id.clone(),
                receiver_endpoint_id: subscriber.endpoint_id.clone(),
            };
            let queue = self.queues.entry(key).or_default();
            queue.retain(|queued| !queued.is_expired(now_unix_us));
            if queue.len() == self.config.max_frames_per_publisher_receiver {
                queue.pop_front();
                self.counters.queue_evictions = self.counters.queue_evictions.saturating_add(1);
            }
            queue.push_back(frame.clone());
            queued.push(subscriber.endpoint_id.clone());
        }
        Ok(TalkbackFanoutV1 {
            queued_receiver_endpoints: queued,
            mix_minus_excluded_endpoints: excluded,
        })
    }

    /// Pull the oldest valid frame across independent per-publisher queues.
    pub fn pull_next(
        &mut self,
        receiver_endpoint_id: &str,
        now_unix_us: u64,
    ) -> Option<EphemeralTalkbackFrameV1> {
        for (key, queue) in &mut self.queues {
            if key.receiver_endpoint_id == receiver_endpoint_id {
                while queue
                    .front()
                    .is_some_and(|frame| frame.is_expired(now_unix_us))
                {
                    queue.pop_front();
                }
            }
        }
        let key = self
            .queues
            .iter()
            .filter(|(key, queue)| {
                key.receiver_endpoint_id == receiver_endpoint_id && !queue.is_empty()
            })
            .min_by_key(|(_, queue)| {
                let frame = queue.front().expect("nonempty queue");
                (frame.accepted_at_unix_us(), frame.frame().sequence())
            })
            .map(|(key, _)| key.clone())?;
        let frame = self.queues.get_mut(&key)?.pop_front()?;
        self.counters.pulled_frames = self.counters.pulled_frames.saturating_add(1);
        self.played
            .entry(key)
            .and_modify(|played| {
                played.delivered_sequence = played.delivered_sequence.max(frame.frame().sequence());
            })
            .or_insert(PlayedHighWater {
                delivered_sequence: frame.frame().sequence(),
                acknowledged_sequence: None,
                acknowledged_at_unix_us: None,
            });
        Some(frame)
    }

    /// Admit an acknowledgement only after the output callback pulled that sequence.
    pub fn acknowledge_playout(
        &mut self,
        acknowledgement: &TalkbackPlayoutAckV1,
        now_unix_us: u64,
    ) -> Result<(), TalkbackLaneError> {
        let result = self.acknowledge_playout_inner(acknowledgement, now_unix_us);
        if result.is_ok() {
            self.counters.accepted_playout_acknowledgements = self
                .counters
                .accepted_playout_acknowledgements
                .saturating_add(1);
        } else {
            self.counters.rejected_playout_acknowledgements = self
                .counters
                .rejected_playout_acknowledgements
                .saturating_add(1);
        }
        result
    }

    fn acknowledge_playout_inner(
        &mut self,
        acknowledgement: &TalkbackPlayoutAckV1,
        now_unix_us: u64,
    ) -> Result<(), TalkbackLaneError> {
        let key = self.acknowledgement_key(acknowledgement)?;
        let played = self.played.get_mut(&key).ok_or_else(|| {
            TalkbackLaneError::new(
                TalkbackLaneErrorCode::PlayoutBeyondDelivery,
                "played_sequence",
            )
        })?;
        if acknowledgement.sequence > played.delivered_sequence {
            return Err(TalkbackLaneError::new(
                TalkbackLaneErrorCode::PlayoutBeyondDelivery,
                "played_sequence",
            ));
        }
        if played
            .acknowledged_sequence
            .is_some_and(|previous| acknowledgement.sequence <= previous)
        {
            return Err(TalkbackLaneError::new(
                TalkbackLaneErrorCode::StaleAcknowledgement,
                "played_sequence",
            ));
        }
        played.acknowledged_sequence = Some(acknowledgement.sequence);
        played.acknowledged_at_unix_us = Some(now_unix_us);
        Ok(())
    }

    #[must_use]
    pub fn is_receiving(&self, acknowledgement: &TalkbackPlayoutAckV1, now_unix_us: u64) -> bool {
        let Ok(key) = self.acknowledgement_key(acknowledgement) else {
            return false;
        };
        self.played.get(&key).is_some_and(|played| {
            played.acknowledged_sequence == Some(acknowledgement.sequence)
                && played.acknowledged_at_unix_us.is_some_and(|at| {
                    now_unix_us.saturating_sub(at) < self.config.acknowledgement_freshness_us
                })
        })
    }

    #[must_use]
    pub fn queued_frames(&self, receiver_endpoint_id: &str) -> usize {
        self.queues
            .iter()
            .filter(|(key, _)| key.receiver_endpoint_id == receiver_endpoint_id)
            .map(|(_, queue)| queue.len())
            .sum()
    }

    #[must_use]
    pub fn health(&self) -> TalkbackLaneHealthV1 {
        let mut health = self.counters;
        health.active_audiences = self.routes.len();
        health.active_publishers = self
            .routes
            .values()
            .map(|snapshot| snapshot.publishers.len())
            .sum();
        health.active_subscribers = self
            .routes
            .values()
            .map(|snapshot| snapshot.subscribers.len())
            .sum();
        health.queued_frames = self.queues.values().map(VecDeque::len).sum();
        health
    }

    fn acknowledgement_key(
        &self,
        acknowledgement: &TalkbackPlayoutAckV1,
    ) -> Result<DeliveryKey, TalkbackLaneError> {
        let audience = AudienceKey {
            session_id: acknowledgement.session_id.clone(),
            audience_id: acknowledgement.audience_id.clone(),
        };
        let route = self.routes.get(&audience).ok_or_else(|| {
            TalkbackLaneError::new(TalkbackLaneErrorCode::RouteUnavailable, "audience")
        })?;
        if !route
            .subscribers
            .get(&acknowledgement.receiver_endpoint_id)
            .is_some_and(|subscriber| {
                subscriber.participant_id == acknowledgement.receiver_participant_id
            })
        {
            return Err(TalkbackLaneError::new(
                TalkbackLaneErrorCode::SubscriberNotAllowed,
                "receiver",
            ));
        }
        Ok(DeliveryKey {
            publisher: PublisherKey {
                audience,
                participant_id: acknowledgement.publisher_participant_id.clone(),
                endpoint_id: acknowledgement.publisher_endpoint_id.clone(),
            },
            receiver_participant_id: acknowledgement.receiver_participant_id.clone(),
            receiver_endpoint_id: acknowledgement.receiver_endpoint_id.clone(),
        })
    }

    fn purge_audience(&mut self, audience: &AudienceKey) -> usize {
        let queued = self
            .queues
            .iter()
            .filter(|(delivery, _)| &delivery.publisher.audience == audience)
            .map(|(_, queue)| queue.len())
            .sum();
        self.sequences
            .retain(|publisher, _| &publisher.audience != audience);
        self.queues
            .retain(|delivery, _| &delivery.publisher.audience != audience);
        self.played
            .retain(|delivery, _| &delivery.publisher.audience != audience);
        queued
    }
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), TalkbackLaneError> {
    if value.is_empty()
        || value.len() > 128
        || !value.is_ascii()
        || value.bytes().any(|byte| !byte.is_ascii_graphic())
    {
        return Err(TalkbackLaneError::new(
            TalkbackLaneErrorCode::InvalidConfiguration,
            field,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use talkback_media::{
        TalkbackCodecV1, TalkbackFrameV1, TalkbackFrameV1Params, TALKBACK_CHANNELS,
        TALKBACK_FRAME_SAMPLES, TALKBACK_SAMPLE_RATE,
    };

    const NOW: u64 = 1_800_000_000_000_000;

    fn snapshot(epoch: u64) -> TalkbackRoutingSnapshotV1 {
        TalkbackRoutingSnapshotV1::new(TalkbackRoutingSnapshotV1Params {
            session_id: "ses_mix".into(),
            session_epoch: 9,
            media_authorization_epoch: 14,
            talkback_epoch: epoch,
            policy_version: 7,
            audience_id: "aud_session_cue".into(),
            publishers: vec![
                TalkbackPublisherV1 {
                    participant_id: "par_one".into(),
                    endpoint_id: "pub_one".into(),
                    subject_grant_epoch: 3,
                },
                TalkbackPublisherV1 {
                    participant_id: "par_two".into(),
                    endpoint_id: "pub_two".into(),
                    subject_grant_epoch: 4,
                },
            ],
            subscribers: vec![
                TalkbackSubscriberV1 {
                    participant_id: "par_one".into(),
                    endpoint_id: "recv_one".into(),
                },
                TalkbackSubscriberV1 {
                    participant_id: "par_two".into(),
                    endpoint_id: "recv_two".into(),
                },
                TalkbackSubscriberV1 {
                    participant_id: "par_three".into(),
                    endpoint_id: "recv_three".into(),
                },
            ],
        })
        .unwrap()
    }

    fn frame(
        publisher: &str,
        endpoint: &str,
        grant: u64,
        sequence: u64,
    ) -> EphemeralTalkbackFrameV1 {
        EphemeralTalkbackFrameV1::new(
            TalkbackFrameV1::new(TalkbackFrameV1Params {
                session_id: "ses_mix".into(),
                session_epoch: 9,
                media_authorization_epoch: 14,
                subject_grant_epoch: grant,
                talkback_epoch: 4,
                policy_version: 7,
                publisher_participant_id: publisher.into(),
                publisher_endpoint_id: endpoint.into(),
                audience_id: "aud_session_cue".into(),
                sequence,
                capture_pts_us: i64::try_from(sequence.saturating_mul(5_000)).unwrap_or(i64::MAX),
                codec: TalkbackCodecV1::Opus,
                sample_rate: TALKBACK_SAMPLE_RATE,
                channels: TALKBACK_CHANNELS,
                frame_samples: TALKBACK_FRAME_SAMPLES,
                payload: Bytes::from(vec![u8::try_from(sequence).unwrap_or(0); 24]),
            })
            .unwrap(),
            NOW,
            NOW + 100_000,
        )
        .unwrap()
    }

    fn ack(sequence: u64) -> TalkbackPlayoutAckV1 {
        TalkbackPlayoutAckV1 {
            session_id: "ses_mix".into(),
            receiver_participant_id: "par_three".into(),
            receiver_endpoint_id: "recv_three".into(),
            publisher_participant_id: "par_one".into(),
            publisher_endpoint_id: "pub_one".into(),
            audience_id: "aud_session_cue".into(),
            sequence,
        }
    }

    #[test]
    fn fanout_is_audience_scoped_and_mix_minus_is_participant_wide() {
        let mut lane = TalkbackLane::new(TalkbackLaneConfig::default()).unwrap();
        lane.install_snapshot(snapshot(4));
        let one = lane
            .publish(frame("par_one", "pub_one", 3, 1), NOW)
            .unwrap();
        assert_eq!(one.queued_receiver_endpoints, ["recv_three", "recv_two"]);
        assert_eq!(one.mix_minus_excluded_endpoints, ["recv_one"]);
        let two = lane
            .publish(frame("par_two", "pub_two", 4, 1), NOW)
            .unwrap();
        assert_eq!(two.queued_receiver_endpoints, ["recv_one", "recv_three"]);
        assert_eq!(two.mix_minus_excluded_endpoints, ["recv_two"]);
    }

    #[test]
    fn queues_are_bounded_per_publisher_and_keep_newest_valid_audio() {
        let mut lane = TalkbackLane::new(TalkbackLaneConfig {
            max_frames_per_publisher_receiver: 2,
            acknowledgement_freshness_us: TALKBACK_ACK_FRESHNESS_US,
        })
        .unwrap();
        lane.install_snapshot(snapshot(4));
        for sequence in 1..=3 {
            lane.publish(frame("par_one", "pub_one", 3, sequence), NOW)
                .unwrap();
        }
        lane.publish(frame("par_two", "pub_two", 4, 1), NOW)
            .unwrap();
        assert_eq!(lane.queued_frames("recv_three"), 3);
        assert_eq!(
            lane.pull_next("recv_three", NOW)
                .unwrap()
                .frame()
                .sequence(),
            1,
            "the second publisher keeps its independent queue"
        );
        let remaining = [
            lane.pull_next("recv_three", NOW)
                .unwrap()
                .frame()
                .sequence(),
            lane.pull_next("recv_three", NOW)
                .unwrap()
                .frame()
                .sequence(),
        ];
        assert_eq!(remaining, [2, 3]);
    }

    #[test]
    fn playout_acknowledgements_cannot_outrun_output_or_live_forever() {
        let mut lane = TalkbackLane::new(TalkbackLaneConfig::default()).unwrap();
        lane.install_snapshot(snapshot(4));
        lane.publish(frame("par_one", "pub_one", 3, 8), NOW)
            .unwrap();
        assert_eq!(
            lane.acknowledge_playout(&ack(8), NOW).unwrap_err().code(),
            TalkbackLaneErrorCode::PlayoutBeyondDelivery
        );
        assert_eq!(
            lane.pull_next("recv_three", NOW)
                .unwrap()
                .frame()
                .sequence(),
            8
        );
        lane.acknowledge_playout(&ack(8), NOW + 1).unwrap();
        assert!(lane.is_receiving(&ack(8), NOW + TALKBACK_ACK_FRESHNESS_US));
        assert!(!lane.is_receiving(&ack(8), NOW + TALKBACK_ACK_FRESHNESS_US + 1));
        assert_eq!(
            lane.acknowledge_playout(&ack(8), NOW + 2)
                .unwrap_err()
                .code(),
            TalkbackLaneErrorCode::StaleAcknowledgement
        );
    }

    #[test]
    fn replay_epoch_membership_and_session_end_fail_closed() {
        let mut lane = TalkbackLane::new(TalkbackLaneConfig::default()).unwrap();
        lane.install_snapshot(snapshot(4));
        lane.publish(frame("par_one", "pub_one", 3, 2), NOW)
            .unwrap();
        assert_eq!(
            lane.publish(frame("par_one", "pub_one", 3, 2), NOW)
                .unwrap_err()
                .code(),
            TalkbackLaneErrorCode::FrameReplay
        );
        assert_eq!(
            lane.publish(frame("par_one", "pub_one", 99, 3), NOW)
                .unwrap_err()
                .code(),
            TalkbackLaneErrorCode::PublisherNotAllowed
        );
        lane.install_snapshot(snapshot(5));
        assert_eq!(lane.queued_frames("recv_three"), 0);
        assert_eq!(
            lane.publish(frame("par_one", "pub_one", 3, 3), NOW)
                .unwrap_err()
                .code(),
            TalkbackLaneErrorCode::AuthorizationEpoch
        );
        lane.end_session("ses_mix");
        assert_eq!(
            lane.publish(frame("par_one", "pub_one", 3, 4), NOW)
                .unwrap_err()
                .code(),
            TalkbackLaneErrorCode::RouteUnavailable
        );
    }

    #[test]
    fn expired_frames_drop_without_affecting_other_live_state() {
        let mut lane = TalkbackLane::new(TalkbackLaneConfig::default()).unwrap();
        lane.install_snapshot(snapshot(4));
        assert_eq!(
            lane.publish(frame("par_one", "pub_one", 3, 1), NOW + 100_000)
                .unwrap_err()
                .code(),
            TalkbackLaneErrorCode::FrameExpired
        );
        lane.publish(frame("par_two", "pub_two", 4, 1), NOW)
            .unwrap();
        assert_eq!(lane.queued_frames("recv_three"), 1);
        let health = lane.health();
        assert_eq!(health.expired_rejections, 1);
        assert_eq!(health.admitted_frames, 1);
        assert_eq!(health.queued_deliveries, 2);
        assert_eq!(health.mix_minus_exclusions, 1);
        assert_eq!(health.active_audiences, 1);
        assert_eq!(health.active_publishers, 2);
        assert_eq!(health.active_subscribers, 3);
    }

    #[test]
    fn sequence_wrap_requires_a_new_talkback_epoch() {
        let mut lane = TalkbackLane::new(TalkbackLaneConfig::default()).unwrap();
        lane.install_snapshot(snapshot(4));
        lane.publish(frame("par_one", "pub_one", 3, u64::MAX), NOW)
            .unwrap();
        assert_eq!(
            lane.publish(frame("par_one", "pub_one", 3, 0), NOW)
                .unwrap_err()
                .code(),
            TalkbackLaneErrorCode::FrameReplay
        );
        lane.install_snapshot(snapshot(5));
        let base = frame("par_one", "pub_one", 3, 0);
        let params = TalkbackFrameV1Params {
            session_id: base.frame().session_id().to_owned(),
            session_epoch: base.frame().session_epoch(),
            media_authorization_epoch: base.frame().media_authorization_epoch(),
            subject_grant_epoch: base.frame().subject_grant_epoch(),
            talkback_epoch: 5,
            policy_version: base.frame().policy_version(),
            publisher_participant_id: base.frame().publisher_participant_id().to_owned(),
            publisher_endpoint_id: base.frame().publisher_endpoint_id().to_owned(),
            audience_id: base.frame().audience_id().to_owned(),
            sequence: 0,
            capture_pts_us: 0,
            codec: TalkbackCodecV1::Opus,
            sample_rate: TALKBACK_SAMPLE_RATE,
            channels: TALKBACK_CHANNELS,
            frame_samples: TALKBACK_FRAME_SAMPLES,
            payload: Bytes::from_static(b"opus"),
        };
        let wrapped = EphemeralTalkbackFrameV1::new(
            TalkbackFrameV1::new(params).unwrap(),
            NOW,
            NOW + 100_000,
        )
        .unwrap();
        lane.publish(wrapped, NOW).unwrap();
    }

    #[test]
    fn health_exposes_only_bounded_counts_and_tracks_purge() {
        let mut lane = TalkbackLane::new(TalkbackLaneConfig {
            max_frames_per_publisher_receiver: 1,
            acknowledgement_freshness_us: TALKBACK_ACK_FRESHNESS_US,
        })
        .unwrap();
        lane.install_snapshot(snapshot(4));
        lane.publish(frame("par_one", "pub_one", 3, 1), NOW)
            .unwrap();
        lane.publish(frame("par_one", "pub_one", 3, 2), NOW)
            .unwrap();
        assert_eq!(lane.health().queue_evictions, 2);
        assert_eq!(lane.health().queued_frames, 2);
        lane.install_snapshot(snapshot(5));
        let health = lane.health();
        assert_eq!(health.queued_frames, 0);
        assert_eq!(health.purged_frames, 2);
        let diagnostic = format!("{health:?}");
        for private in ["ses_mix", "par_one", "recv_three", "aud_session_cue"] {
            assert!(!diagnostic.contains(private));
        }
    }
}
