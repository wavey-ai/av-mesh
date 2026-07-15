use bytes::Bytes;
use raptorq_datagram_fec::{
    DatagramFecDecoder, DatagramFecError, DatagramFecHeader, DATAGRAM_MAGIC,
};
pub use raptorq_datagram_fec::{
    SequenceStats, UdpFecSender, DEFAULT_REPAIR_SYMBOLS, DEFAULT_SOURCE_SYMBOLS,
    DEFAULT_SYMBOL_SIZE, HEADER_LEN,
};
use raptorq_fec_transport::split_stream_id_prefix;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Largest application object accepted by the live UDP RaptorQ ingress.
pub const DEFAULT_MAX_FEC_OBJECT_BYTES: usize = 8 * 1024 * 1024;
/// Aggregate transfer lengths reserved by incomplete RaptorQ objects.
pub const DEFAULT_MAX_BUFFERED_FEC_OBJECT_BYTES: usize = 128 * 1024 * 1024;
/// Maximum UDP payload accepted by the ingress parser.
pub const DEFAULT_MAX_FEC_DATAGRAM_BYTES: usize = 65_535;
pub const DEFAULT_MAX_FEC_FLOWS: usize = 4_096;
pub const DEFAULT_MAX_ACTIVE_FEC_OBJECTS: usize = 4_096;
pub const DEFAULT_MAX_ACTIVE_FEC_OBJECTS_PER_FLOW: usize = 32;
pub const DEFAULT_MAX_COMPLETED_FEC_OBJECTS_PER_FLOW: usize = 64;
pub const DEFAULT_MAX_FEC_DATAGRAMS_PER_OBJECT: usize = 32_768;
pub const DEFAULT_MAX_BUFFERED_FEC_DATAGRAMS: usize = 131_072;
pub const DEFAULT_MAX_TRACKED_SEQUENCE_GAPS_PER_FLOW: usize = 1_024;
pub const DEFAULT_FEC_OBJECT_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_FEC_FLOW_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(120);
pub const DEFAULT_FEC_EXPIRY_SCAN_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedUdpFecPayload {
    pub stream_id: Option<u64>,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpFecReceiverConfig {
    pub max_datagram_bytes: usize,
    pub max_object_bytes: usize,
    pub max_buffered_object_bytes: usize,
    pub max_flows: usize,
    pub max_active_objects: usize,
    pub max_active_objects_per_flow: usize,
    pub max_completed_objects_per_flow: usize,
    pub max_datagrams_per_object: usize,
    pub max_buffered_datagrams: usize,
    pub max_tracked_sequence_gaps_per_flow: usize,
    pub object_inactivity_timeout: Duration,
    pub flow_inactivity_timeout: Duration,
    pub expiry_scan_interval: Duration,
}

impl Default for UdpFecReceiverConfig {
    fn default() -> Self {
        Self {
            max_datagram_bytes: DEFAULT_MAX_FEC_DATAGRAM_BYTES,
            max_object_bytes: DEFAULT_MAX_FEC_OBJECT_BYTES,
            max_buffered_object_bytes: DEFAULT_MAX_BUFFERED_FEC_OBJECT_BYTES,
            max_flows: DEFAULT_MAX_FEC_FLOWS,
            max_active_objects: DEFAULT_MAX_ACTIVE_FEC_OBJECTS,
            max_active_objects_per_flow: DEFAULT_MAX_ACTIVE_FEC_OBJECTS_PER_FLOW,
            max_completed_objects_per_flow: DEFAULT_MAX_COMPLETED_FEC_OBJECTS_PER_FLOW,
            max_datagrams_per_object: DEFAULT_MAX_FEC_DATAGRAMS_PER_OBJECT,
            max_buffered_datagrams: DEFAULT_MAX_BUFFERED_FEC_DATAGRAMS,
            max_tracked_sequence_gaps_per_flow: DEFAULT_MAX_TRACKED_SEQUENCE_GAPS_PER_FLOW,
            object_inactivity_timeout: DEFAULT_FEC_OBJECT_INACTIVITY_TIMEOUT,
            flow_inactivity_timeout: DEFAULT_FEC_FLOW_INACTIVITY_TIMEOUT,
            expiry_scan_interval: DEFAULT_FEC_EXPIRY_SCAN_INTERVAL,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UdpFecPushOutcome {
    Buffered {
        stream_id: Option<u64>,
        block_id: u32,
    },
    Decoded {
        block_id: u32,
        payload: DecodedUdpFecPayload,
    },
    Duplicate {
        stream_id: Option<u64>,
        block_id: u32,
    },
}

impl UdpFecPushOutcome {
    pub fn into_decoded(self) -> Option<DecodedUdpFecPayload> {
        match self {
            Self::Decoded { payload, .. } => Some(payload),
            Self::Buffered { .. } | Self::Duplicate { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UdpFecReceiveError {
    DatagramTooLarge {
        actual: usize,
        max: usize,
    },
    ObjectTooLarge {
        stream_id: Option<u64>,
        block_id: u32,
        actual: usize,
        max: usize,
    },
    FlowLimitExceeded {
        max: usize,
    },
    ActiveObjectLimitExceeded {
        max: usize,
    },
    FlowObjectLimitExceeded {
        stream_id: Option<u64>,
        max: usize,
    },
    ObjectDatagramLimitExceeded {
        stream_id: Option<u64>,
        block_id: u32,
        max: usize,
    },
    BufferedDatagramLimitExceeded {
        active: usize,
        max: usize,
    },
    BufferedObjectBytesLimitExceeded {
        active: usize,
        requested: usize,
        max: usize,
    },
    Fec {
        stream_id: Option<u64>,
        error: DatagramFecError,
    },
}

impl fmt::Display for UdpFecReceiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DatagramTooLarge { actual, max } => {
                write!(formatter, "UDP FEC datagram is {actual} bytes; limit is {max}")
            }
            Self::ObjectTooLarge {
                stream_id,
                block_id,
                actual,
                max,
            } => write!(
                formatter,
                "UDP FEC object {block_id} for stream {stream_id:?} is {actual} bytes; limit is {max}"
            ),
            Self::FlowLimitExceeded { max } => {
                write!(formatter, "UDP FEC flow limit reached ({max})")
            }
            Self::ActiveObjectLimitExceeded { max } => {
                write!(formatter, "UDP FEC active-object limit reached ({max})")
            }
            Self::FlowObjectLimitExceeded { stream_id, max } => write!(
                formatter,
                "UDP FEC active-object limit reached for stream {stream_id:?} ({max})"
            ),
            Self::ObjectDatagramLimitExceeded {
                stream_id,
                block_id,
                max,
            } => write!(
                formatter,
                "UDP FEC datagram limit reached for object {block_id} on stream {stream_id:?} ({max})"
            ),
            Self::BufferedDatagramLimitExceeded { active, max } => write!(
                formatter,
                "UDP FEC buffered-datagram budget reached ({active}/{max})"
            ),
            Self::BufferedObjectBytesLimitExceeded {
                active,
                requested,
                max,
            } => write!(
                formatter,
                "UDP FEC buffered-object budget exceeded: {active} active + {requested} requested > {max}"
            ),
            Self::Fec { stream_id, error } => {
                write!(formatter, "invalid UDP FEC datagram for stream {stream_id:?}: {error}")
            }
        }
    }
}

impl std::error::Error for UdpFecReceiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Fec { error, .. } => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UdpFecReceiverCounters {
    pub datagrams_received: u64,
    pub datagrams_accepted: u64,
    pub datagrams_rejected: u64,
    pub datagrams_buffered: u64,
    pub duplicate_datagrams: u64,
    pub objects_decoded: u64,
    pub objects_expired: u64,
    pub flows_expired: u64,
    pub expiry_scans: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UdpFecReceiverState {
    pub active_flows: usize,
    pub active_objects: usize,
    pub buffered_object_bytes: usize,
    pub buffered_datagrams: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UdpFecExpiry {
    pub objects: usize,
    pub flows: usize,
    pub released_object_bytes: usize,
    pub released_datagrams: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FlowKey {
    Unprefixed(SocketAddr),
    Stream(SocketAddr, u64),
}

impl FlowKey {
    fn stream_id(self) -> Option<u64> {
        match self {
            Self::Unprefixed(_) => None,
            Self::Stream(_, stream_id) => Some(stream_id),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ObjectKey {
    flow: FlowKey,
    block_id: u32,
}

#[derive(Debug)]
struct ObjectState {
    decoder: DatagramFecDecoder,
    transfer_length: usize,
    accepted_datagrams: usize,
    last_activity: Instant,
}

#[derive(Debug, Default)]
struct BoundedSequenceTracker {
    next_expected: Option<u32>,
    missing: HashSet<u32>,
    stats: SequenceStats,
}

impl BoundedSequenceTracker {
    fn observe(&mut self, sequence: u32, max_tracked_gaps: usize) {
        self.stats.received = self.stats.received.saturating_add(1);
        self.stats.highest_seen = Some(self.stats.highest_seen.map_or(sequence, |highest| {
            let diff = sequence.wrapping_sub(highest);
            if diff <= i32::MAX as u32 {
                sequence
            } else {
                highest
            }
        }));

        let Some(expected) = self.next_expected else {
            self.next_expected = Some(sequence.wrapping_add(1));
            return;
        };
        let diff = sequence.wrapping_sub(expected);
        if diff <= i32::MAX as u32 {
            if diff > 0 {
                self.missing.clear();
                let tracked = usize::try_from(diff)
                    .unwrap_or(usize::MAX)
                    .min(max_tracked_gaps);
                let first = sequence.wrapping_sub(tracked as u32);
                for offset in 0..tracked {
                    self.missing.insert(first.wrapping_add(offset as u32));
                }
            }
            self.next_expected = Some(sequence.wrapping_add(1));
        } else {
            self.missing.remove(&sequence);
            self.stats.duplicate_or_reordered = self.stats.duplicate_or_reordered.saturating_add(1);
        }
        self.stats.missing = self.missing.len() as u64;
    }

    fn stats(&self) -> SequenceStats {
        self.stats
    }
}

#[derive(Debug)]
struct FlowState {
    sequence_tracker: BoundedSequenceTracker,
    active_objects: usize,
    completed_order: VecDeque<u32>,
    completed: HashSet<u32>,
    last_activity: Instant,
}

impl FlowState {
    fn new(now: Instant) -> Self {
        Self {
            sequence_tracker: BoundedSequenceTracker::default(),
            active_objects: 0,
            completed_order: VecDeque::new(),
            completed: HashSet::new(),
            last_activity: now,
        }
    }

    fn mark_completed(&mut self, block_id: u32, limit: usize) {
        if limit == 0 || !self.completed.insert(block_id) {
            return;
        }

        self.completed_order.push_back(block_id);
        while self.completed_order.len() > limit {
            if let Some(expired) = self.completed_order.pop_front() {
                self.completed.remove(&expired);
            }
        }
    }
}

/// Bounded receiver for raw and stream-prefixed live RaptorQ datagrams.
///
/// Each in-flight object owns an independent decoder. This permits object-level
/// inactivity expiry even while the containing stream remains busy.
#[derive(Debug)]
pub struct UdpFecReceiver {
    config: UdpFecReceiverConfig,
    flows: HashMap<FlowKey, FlowState>,
    objects: HashMap<ObjectKey, ObjectState>,
    buffered_object_bytes: usize,
    buffered_datagrams: usize,
    counters: UdpFecReceiverCounters,
    last_error: Option<UdpFecReceiveError>,
    next_expiry_scan: Option<Instant>,
}

impl UdpFecReceiver {
    pub fn new() -> Self {
        Self::with_config(UdpFecReceiverConfig::default())
    }

    pub fn with_config(config: UdpFecReceiverConfig) -> Self {
        Self {
            config,
            flows: HashMap::new(),
            objects: HashMap::new(),
            buffered_object_bytes: 0,
            buffered_datagrams: 0,
            counters: UdpFecReceiverCounters::default(),
            last_error: None,
            next_expiry_scan: None,
        }
    }

    pub fn config(&self) -> &UdpFecReceiverConfig {
        &self.config
    }

    /// Compatibility wrapper. New ingest paths should use
    /// [`Self::try_push_payload`] so malformed and capacity-limited datagrams
    /// remain visible.
    pub fn push(&mut self, peer: SocketAddr, datagram: &[u8]) -> Option<Bytes> {
        self.push_payload(peer, datagram)
            .and_then(|decoded| decoded.stream_id.is_none().then_some(decoded.payload))
    }

    /// Compatibility wrapper around [`Self::try_push_payload`]. A rejected
    /// datagram remains available through [`Self::last_error`] and counters.
    pub fn push_payload(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
    ) -> Option<DecodedUdpFecPayload> {
        self.try_push_payload(peer, datagram)
            .ok()
            .and_then(UdpFecPushOutcome::into_decoded)
    }

    pub fn try_push_payload(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
    ) -> Result<UdpFecPushOutcome, UdpFecReceiveError> {
        self.try_push_payload_at(peer, datagram, Instant::now())
    }

    fn try_push_payload_at(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
        now: Instant,
    ) -> Result<UdpFecPushOutcome, UdpFecReceiveError> {
        self.expire_inactive_if_due(now);
        self.counters.datagrams_received = self.counters.datagrams_received.saturating_add(1);

        if datagram.len() > self.config.max_datagram_bytes {
            return self.reject(UdpFecReceiveError::DatagramTooLarge {
                actual: datagram.len(),
                max: self.config.max_datagram_bytes,
            });
        }

        let (flow, fec_datagram) = match split_stream_id_prefix(datagram) {
            Some((stream_id, payload)) if payload.starts_with(&DATAGRAM_MAGIC) => {
                (FlowKey::Stream(peer, stream_id), payload)
            }
            _ => (FlowKey::Unprefixed(peer), datagram),
        };
        let stream_id = flow.stream_id();
        let header = match DatagramFecHeader::decode(fec_datagram) {
            Ok(header) => header,
            Err(error) => return self.reject(UdpFecReceiveError::Fec { stream_id, error }),
        };
        if let Err(error) = header.payload(fec_datagram) {
            return self.reject(UdpFecReceiveError::Fec { stream_id, error });
        }

        let transfer_length = header.transfer_length as usize;
        if transfer_length > self.config.max_object_bytes {
            return self.reject(UdpFecReceiveError::ObjectTooLarge {
                stream_id,
                block_id: header.block_id,
                actual: transfer_length,
                max: self.config.max_object_bytes,
            });
        }

        let object_key = ObjectKey {
            flow,
            block_id: header.block_id,
        };
        if self
            .flows
            .get(&flow)
            .is_some_and(|state| state.completed.contains(&header.block_id))
        {
            let state = self
                .flows
                .get_mut(&flow)
                .expect("completed object belongs to an existing flow");
            state.last_activity = now;
            state.sequence_tracker.observe(
                header.packet_sequence,
                self.config.max_tracked_sequence_gaps_per_flow,
            );
            self.counters.datagrams_accepted = self.counters.datagrams_accepted.saturating_add(1);
            self.counters.duplicate_datagrams = self.counters.duplicate_datagrams.saturating_add(1);
            return Ok(UdpFecPushOutcome::Duplicate {
                stream_id,
                block_id: header.block_id,
            });
        }

        let is_new_object = !self.objects.contains_key(&object_key);
        let is_new_flow = !self.flows.contains_key(&flow);
        if self.buffered_datagrams >= self.config.max_buffered_datagrams {
            return self.reject(UdpFecReceiveError::BufferedDatagramLimitExceeded {
                active: self.buffered_datagrams,
                max: self.config.max_buffered_datagrams,
            });
        }
        if is_new_object && self.config.max_datagrams_per_object == 0 {
            return self.reject(UdpFecReceiveError::ObjectDatagramLimitExceeded {
                stream_id,
                block_id: header.block_id,
                max: self.config.max_datagrams_per_object,
            });
        }
        if self
            .objects
            .get(&object_key)
            .is_some_and(|state| state.accepted_datagrams >= self.config.max_datagrams_per_object)
        {
            self.remove_object(object_key);
            return self.reject(UdpFecReceiveError::ObjectDatagramLimitExceeded {
                stream_id,
                block_id: header.block_id,
                max: self.config.max_datagrams_per_object,
            });
        }
        if is_new_object {
            if is_new_flow && self.flows.len() >= self.config.max_flows {
                return self.reject(UdpFecReceiveError::FlowLimitExceeded {
                    max: self.config.max_flows,
                });
            }
            if self.objects.len() >= self.config.max_active_objects {
                return self.reject(UdpFecReceiveError::ActiveObjectLimitExceeded {
                    max: self.config.max_active_objects,
                });
            }
            let active_in_flow = self
                .flows
                .get(&flow)
                .map_or(0, |state| state.active_objects);
            if active_in_flow >= self.config.max_active_objects_per_flow {
                return self.reject(UdpFecReceiveError::FlowObjectLimitExceeded {
                    stream_id,
                    max: self.config.max_active_objects_per_flow,
                });
            }
            let Some(reserved_after_insert) = self
                .buffered_object_bytes
                .checked_add(transfer_length)
                .filter(|reserved| *reserved <= self.config.max_buffered_object_bytes)
            else {
                return self.reject(UdpFecReceiveError::BufferedObjectBytesLimitExceeded {
                    active: self.buffered_object_bytes,
                    requested: transfer_length,
                    max: self.config.max_buffered_object_bytes,
                });
            };

            let flow_state = self
                .flows
                .entry(flow)
                .or_insert_with(|| FlowState::new(now));
            flow_state.active_objects = flow_state.active_objects.saturating_add(1);
            self.objects.insert(
                object_key,
                ObjectState {
                    decoder: DatagramFecDecoder::new(),
                    transfer_length,
                    accepted_datagrams: 0,
                    last_activity: now,
                },
            );
            self.buffered_object_bytes = reserved_after_insert;
        }

        let flow_state = self
            .flows
            .get_mut(&flow)
            .expect("an active object belongs to an existing flow");
        flow_state.last_activity = now;
        flow_state.sequence_tracker.observe(
            header.packet_sequence,
            self.config.max_tracked_sequence_gaps_per_flow,
        );

        let decode_result = {
            let object = self
                .objects
                .get_mut(&object_key)
                .expect("object was present or inserted above");
            object.last_activity = now;
            object.accepted_datagrams = object.accepted_datagrams.saturating_add(1);
            self.buffered_datagrams = self.buffered_datagrams.saturating_add(1);
            object.decoder.push_datagram(fec_datagram)
        };

        match decode_result {
            Ok(Some(payload)) => {
                self.remove_object(object_key);
                self.flows
                    .get_mut(&flow)
                    .expect("decoded object belongs to an existing flow")
                    .mark_completed(header.block_id, self.config.max_completed_objects_per_flow);
                self.counters.datagrams_accepted =
                    self.counters.datagrams_accepted.saturating_add(1);
                self.counters.objects_decoded = self.counters.objects_decoded.saturating_add(1);
                Ok(UdpFecPushOutcome::Decoded {
                    block_id: header.block_id,
                    payload: DecodedUdpFecPayload {
                        stream_id,
                        payload: Bytes::from(payload),
                    },
                })
            }
            Ok(None) => {
                self.counters.datagrams_accepted =
                    self.counters.datagrams_accepted.saturating_add(1);
                self.counters.datagrams_buffered =
                    self.counters.datagrams_buffered.saturating_add(1);
                Ok(UdpFecPushOutcome::Buffered {
                    stream_id,
                    block_id: header.block_id,
                })
            }
            Err(error) => {
                self.remove_object(object_key);
                if is_new_flow {
                    self.remove_empty_flow(flow);
                }
                self.reject(UdpFecReceiveError::Fec { stream_id, error })
            }
        }
    }

    pub fn sequence_stats(&self, peer: SocketAddr) -> Option<SequenceStats> {
        self.flows
            .get(&FlowKey::Unprefixed(peer))
            .map(|state| state.sequence_tracker.stats())
    }

    pub fn stream_sequence_stats(&self, peer: SocketAddr, stream_id: u64) -> Option<SequenceStats> {
        self.flows
            .get(&FlowKey::Stream(peer, stream_id))
            .map(|state| state.sequence_tracker.stats())
    }

    pub fn counters(&self) -> UdpFecReceiverCounters {
        self.counters
    }

    pub fn state(&self) -> UdpFecReceiverState {
        UdpFecReceiverState {
            active_flows: self.flows.len(),
            active_objects: self.objects.len(),
            buffered_object_bytes: self.buffered_object_bytes,
            buffered_datagrams: self.buffered_datagrams,
        }
    }

    pub fn last_error(&self) -> Option<&UdpFecReceiveError> {
        self.last_error.as_ref()
    }

    pub fn take_last_error(&mut self) -> Option<UdpFecReceiveError> {
        self.last_error.take()
    }

    pub fn expire_inactive(&mut self) -> UdpFecExpiry {
        self.expire_inactive_at(Instant::now())
    }

    fn expire_inactive_at(&mut self, now: Instant) -> UdpFecExpiry {
        let expiry = self.scan_inactive_at(now);
        self.schedule_next_expiry_scan(now);
        expiry
    }

    fn expire_inactive_if_due(&mut self, now: Instant) -> UdpFecExpiry {
        if self.next_expiry_scan.is_some_and(|deadline| now < deadline) {
            return UdpFecExpiry::default();
        }
        self.expire_inactive_at(now)
    }

    fn scan_inactive_at(&mut self, now: Instant) -> UdpFecExpiry {
        self.counters.expiry_scans = self.counters.expiry_scans.saturating_add(1);
        let expired_objects = self
            .objects
            .iter()
            .filter_map(|(key, state)| {
                elapsed(now, state.last_activity)
                    .ge(&self.config.object_inactivity_timeout)
                    .then_some(*key)
            })
            .collect::<Vec<_>>();

        let mut expiry = UdpFecExpiry::default();
        for key in expired_objects {
            if let Some(state) = self.remove_object(key) {
                expiry.objects += 1;
                expiry.released_object_bytes = expiry
                    .released_object_bytes
                    .saturating_add(state.transfer_length);
                expiry.released_datagrams = expiry
                    .released_datagrams
                    .saturating_add(state.accepted_datagrams);
            }
        }

        let expired_flows = self
            .flows
            .iter()
            .filter_map(|(key, state)| {
                (state.active_objects == 0
                    && elapsed(now, state.last_activity) >= self.config.flow_inactivity_timeout)
                    .then_some(*key)
            })
            .collect::<Vec<_>>();
        for flow in expired_flows {
            if self.flows.remove(&flow).is_some() {
                expiry.flows += 1;
            }
        }

        self.counters.objects_expired = self
            .counters
            .objects_expired
            .saturating_add(expiry.objects as u64);
        self.counters.flows_expired = self
            .counters
            .flows_expired
            .saturating_add(expiry.flows as u64);
        expiry
    }

    fn schedule_next_expiry_scan(&mut self, now: Instant) {
        let interval = self
            .config
            .expiry_scan_interval
            .min(self.config.object_inactivity_timeout)
            .min(self.config.flow_inactivity_timeout);
        self.next_expiry_scan = now.checked_add(interval);
    }

    fn remove_object(&mut self, key: ObjectKey) -> Option<ObjectState> {
        let state = self.objects.remove(&key)?;
        self.buffered_object_bytes = self
            .buffered_object_bytes
            .saturating_sub(state.transfer_length);
        self.buffered_datagrams = self
            .buffered_datagrams
            .saturating_sub(state.accepted_datagrams);
        if let Some(flow) = self.flows.get_mut(&key.flow) {
            flow.active_objects = flow.active_objects.saturating_sub(1);
        }
        Some(state)
    }

    fn remove_empty_flow(&mut self, flow: FlowKey) {
        if self
            .flows
            .get(&flow)
            .is_some_and(|state| state.active_objects == 0 && state.completed.is_empty())
        {
            self.flows.remove(&flow);
        }
    }

    fn reject<T>(&mut self, error: UdpFecReceiveError) -> Result<T, UdpFecReceiveError> {
        self.counters.datagrams_rejected = self.counters.datagrams_rejected.saturating_add(1);
        self.last_error = Some(error.clone());
        Err(error)
    }
}

impl Default for UdpFecReceiver {
    fn default() -> Self {
        Self::new()
    }
}

fn elapsed(now: Instant, then: Instant) -> Duration {
    now.checked_duration_since(then).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use raptorq_datagram_fec::DatagramFecEncoder;
    use raptorq_fec_transport::encode_stream_id_prefix;
    use tokio::net::UdpSocket;

    fn peer(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn encoded_object(
        encoder: &mut DatagramFecEncoder,
        payload: &[u8],
        repair_symbols: u32,
    ) -> Vec<Vec<u8>> {
        encoder
            .encode_object_with_repair_symbols(payload, repair_symbols)
            .unwrap()
    }

    fn prefixed(stream_id: u64, datagram: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + datagram.len());
        bytes.extend_from_slice(&encode_stream_id_prefix(stream_id));
        bytes.extend_from_slice(datagram);
        bytes
    }

    #[tokio::test]
    async fn udp_fec_sender_receiver_roundtrip() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let mut sender = UdpFecSender::new(addr)
            .await
            .unwrap()
            .with_repair_symbols(1)
            .with_symbol_size(DEFAULT_SYMBOL_SIZE);
        let mut receiver = UdpFecReceiver::new();
        let payload = Bytes::from_static(b"fec-protected-media");

        sender.send(&payload).await.unwrap();

        let mut buf = vec![0u8; 65_536];
        loop {
            let (len, peer) = socket.recv_from(&mut buf).await.unwrap();
            if let Some(decoded) = receiver.push(peer, &buf[..len]) {
                assert_eq!(decoded, payload);
                let stats = receiver.sequence_stats(peer).unwrap();
                assert!(stats.received > 0);
                assert_eq!(receiver.state().active_objects, 0);
                break;
            }
        }
    }

    #[test]
    fn strict_outcomes_preserve_exact_large_object_after_loss_and_reordering() {
        let payload = (0..6_001)
            .map(|index| ((index * 37 + 11) % 251) as u8)
            .collect::<Vec<_>>();
        let mut encoder = DatagramFecEncoder::new()
            .with_source_symbols(DEFAULT_SOURCE_SYMBOLS)
            .with_repair_symbols(3)
            .with_symbol_size(DEFAULT_SYMBOL_SIZE);
        let mut datagrams = encoded_object(&mut encoder, &payload, 3);
        datagrams.remove(1);
        datagrams.reverse();

        let mut receiver = UdpFecReceiver::new();
        let mut decoded = None;
        for datagram in datagrams {
            let outcome = receiver.try_push_payload(peer(10_001), &datagram).unwrap();
            if let UdpFecPushOutcome::Decoded { payload, .. } = outcome {
                decoded = Some(payload.payload);
            }
        }

        assert_eq!(decoded.as_deref(), Some(payload.as_slice()));
        assert_eq!(receiver.counters().objects_decoded, 1);
        assert_eq!(receiver.state().active_objects, 0);
    }

    #[test]
    fn stream_prefixed_objects_report_stream_and_exact_bytes() {
        let expected = b"stream-prefixed-RaptorQ-object";
        let mut encoder = DatagramFecEncoder::default();
        let datagrams = encoded_object(&mut encoder, expected, 1);
        let mut receiver = UdpFecReceiver::new();
        let mut decoded = None;

        for datagram in datagrams {
            let datagram = prefixed(42, &datagram);
            if let UdpFecPushOutcome::Decoded { payload, .. } =
                receiver.try_push_payload(peer(10_002), &datagram).unwrap()
            {
                decoded = Some(payload);
            }
        }

        assert_eq!(decoded.unwrap().stream_id, Some(42));
        assert_eq!(
            receiver
                .stream_sequence_stats(peer(10_002), 42)
                .unwrap()
                .missing,
            0
        );
    }

    #[test]
    fn malformed_datagrams_are_explicit_and_do_not_allocate_state() {
        let mut receiver = UdpFecReceiver::new();
        let error = receiver
            .try_push_payload(peer(10_003), b"hostile")
            .unwrap_err();

        assert!(matches!(
            error,
            UdpFecReceiveError::Fec {
                error: DatagramFecError::HeaderTooShort { .. },
                ..
            }
        ));
        assert_eq!(receiver.state(), UdpFecReceiverState::default());
        assert_eq!(receiver.counters().datagrams_rejected, 1);
        assert_eq!(receiver.last_error(), Some(&error));
    }

    #[test]
    fn object_and_aggregate_byte_budgets_are_enforced_before_decoder_allocation() {
        let payload = vec![7; 2_000];
        let mut encoder = DatagramFecEncoder::new()
            .with_source_symbols(4)
            .with_repair_symbols(1)
            .with_symbol_size(1_000);
        let first = encoded_object(&mut encoder, &payload, 1);

        let mut too_small = UdpFecReceiver::with_config(UdpFecReceiverConfig {
            max_object_bytes: payload.len() - 1,
            ..UdpFecReceiverConfig::default()
        });
        assert!(matches!(
            too_small.try_push_payload(peer(10_004), &first[0]),
            Err(UdpFecReceiveError::ObjectTooLarge { .. })
        ));
        assert_eq!(too_small.state().active_objects, 0);

        let mut budgeted = UdpFecReceiver::with_config(UdpFecReceiverConfig {
            max_buffered_object_bytes: 3_000,
            ..UdpFecReceiverConfig::default()
        });
        assert!(matches!(
            budgeted.try_push_payload(peer(10_004), &first[0]),
            Ok(UdpFecPushOutcome::Buffered { .. })
        ));
        let second = encoded_object(&mut encoder, &payload, 1);
        assert!(matches!(
            budgeted.try_push_payload(peer(10_005), &second[0]),
            Err(UdpFecReceiveError::BufferedObjectBytesLimitExceeded { .. })
        ));
        assert_eq!(budgeted.state().active_objects, 1);
        assert_eq!(budgeted.state().buffered_object_bytes, 2_000);
    }

    #[test]
    fn flow_and_per_flow_object_limits_bound_hostile_identifiers() {
        let payload = vec![9; 2_000];
        let mut encoder = DatagramFecEncoder::new()
            .with_source_symbols(4)
            .with_repair_symbols(1)
            .with_symbol_size(1_000);
        let first = encoded_object(&mut encoder, &payload, 1);
        let second = encoded_object(&mut encoder, &payload, 1);
        let mut receiver = UdpFecReceiver::with_config(UdpFecReceiverConfig {
            max_flows: 1,
            max_active_objects_per_flow: 1,
            ..UdpFecReceiverConfig::default()
        });

        receiver.try_push_payload(peer(10_006), &first[0]).unwrap();
        assert!(matches!(
            receiver.try_push_payload(peer(10_006), &second[0]),
            Err(UdpFecReceiveError::FlowObjectLimitExceeded { max: 1, .. })
        ));
        assert!(matches!(
            receiver.try_push_payload(peer(10_007), &second[0]),
            Err(UdpFecReceiveError::FlowLimitExceeded { max: 1 })
        ));
        assert_eq!(receiver.state().active_flows, 1);
        assert_eq!(receiver.state().active_objects, 1);
    }

    #[test]
    fn per_object_and_aggregate_datagram_budgets_release_or_preserve_bounded_state() {
        let payload = vec![6; 2_000];
        let mut encoder = DatagramFecEncoder::new().with_symbol_size(1_000);
        let first = encoded_object(&mut encoder, &payload, 1);
        let second = encoded_object(&mut encoder, &payload, 1);

        let mut per_object = UdpFecReceiver::with_config(UdpFecReceiverConfig {
            max_datagrams_per_object: 1,
            ..UdpFecReceiverConfig::default()
        });
        per_object
            .try_push_payload(peer(10_012), &first[0])
            .unwrap();
        assert!(matches!(
            per_object.try_push_payload(peer(10_012), &first[1]),
            Err(UdpFecReceiveError::ObjectDatagramLimitExceeded { max: 1, .. })
        ));
        assert_eq!(per_object.state().active_objects, 0);
        assert_eq!(per_object.state().buffered_datagrams, 0);

        let mut aggregate = UdpFecReceiver::with_config(UdpFecReceiverConfig {
            max_buffered_datagrams: 1,
            ..UdpFecReceiverConfig::default()
        });
        aggregate.try_push_payload(peer(10_013), &first[0]).unwrap();
        assert!(matches!(
            aggregate.try_push_payload(peer(10_014), &second[0]),
            Err(UdpFecReceiveError::BufferedDatagramLimitExceeded { active: 1, max: 1 })
        ));
        assert_eq!(aggregate.state().active_flows, 1);
        assert_eq!(aggregate.state().active_objects, 1);
        assert_eq!(aggregate.state().buffered_datagrams, 1);
    }

    #[test]
    fn sequence_gap_tracking_has_a_fixed_window() {
        let mut tracker = BoundedSequenceTracker::default();
        tracker.observe(0, 3);
        tracker.observe(10_000, 3);
        tracker.observe(20_000, 3);

        assert_eq!(tracker.missing.len(), 3);
        assert_eq!(tracker.stats().missing, 3);
        tracker.observe(19_999, 3);
        assert_eq!(tracker.missing.len(), 2);
        assert_eq!(tracker.stats().duplicate_or_reordered, 1);
    }

    #[test]
    fn stream_id_flood_stops_at_flow_limit() {
        let payload = vec![3; 2_000];
        let mut encoder = DatagramFecEncoder::new()
            .with_source_symbols(4)
            .with_repair_symbols(1)
            .with_symbol_size(1_000);
        let datagram = encoded_object(&mut encoder, &payload, 1).remove(0);
        let mut receiver = UdpFecReceiver::with_config(UdpFecReceiverConfig {
            max_flows: 4,
            ..UdpFecReceiverConfig::default()
        });

        for stream_id in 0..4 {
            receiver
                .try_push_payload(peer(10_008), &prefixed(stream_id, &datagram))
                .unwrap();
        }
        let error = receiver
            .try_push_payload(peer(10_008), &prefixed(4, &datagram))
            .unwrap_err();

        assert_eq!(error, UdpFecReceiveError::FlowLimitExceeded { max: 4 });
        assert_eq!(receiver.state().active_flows, 4);
        assert_eq!(receiver.state().active_objects, 4);
    }

    #[test]
    fn malformed_encoding_packet_releases_reserved_object_state() {
        let mut encoder = DatagramFecEncoder::default();
        let mut datagram = encoded_object(&mut encoder, b"bad source block", 1).remove(0);
        let mut header = DatagramFecHeader::decode(&datagram).unwrap();
        datagram[HEADER_LEN] = 1;
        header.packet_crc32 = header
            .compute_packet_crc32(&datagram[HEADER_LEN..])
            .unwrap();
        header.encode(&mut datagram[..HEADER_LEN]).unwrap();

        let mut receiver = UdpFecReceiver::new();
        let error = receiver
            .try_push_payload(peer(10_009), &datagram)
            .unwrap_err();

        assert!(matches!(
            error,
            UdpFecReceiveError::Fec {
                error: DatagramFecError::UnsupportedSourceBlockNumber(1),
                ..
            }
        ));
        assert_eq!(receiver.state(), UdpFecReceiverState::default());
    }

    #[test]
    fn completed_object_duplicates_do_not_recreate_decoder_state() {
        let mut encoder = DatagramFecEncoder::default();
        let datagrams = encoded_object(&mut encoder, b"duplicate-safe", 1);
        let mut receiver = UdpFecReceiver::new();
        let mut complete = false;
        for datagram in &datagrams {
            complete = matches!(
                receiver.try_push_payload(peer(10_010), datagram).unwrap(),
                UdpFecPushOutcome::Decoded { .. }
            );
            if complete {
                break;
            }
        }
        assert!(complete);

        let duplicate = receiver
            .try_push_payload(peer(10_010), &datagrams[0])
            .unwrap();
        assert!(matches!(duplicate, UdpFecPushOutcome::Duplicate { .. }));
        assert_eq!(receiver.state().active_objects, 0);
        assert_eq!(receiver.counters().duplicate_datagrams, 1);
    }

    #[test]
    fn inactive_objects_and_then_empty_flows_expire_deterministically() {
        let payload = vec![5; 2_000];
        let mut encoder = DatagramFecEncoder::new()
            .with_source_symbols(4)
            .with_repair_symbols(1)
            .with_symbol_size(1_000);
        let datagram = encoded_object(&mut encoder, &payload, 1).remove(0);
        let mut receiver = UdpFecReceiver::with_config(UdpFecReceiverConfig {
            object_inactivity_timeout: Duration::from_secs(5),
            flow_inactivity_timeout: Duration::from_secs(10),
            ..UdpFecReceiverConfig::default()
        });
        let started = Instant::now();
        receiver
            .try_push_payload_at(peer(10_011), &datagram, started)
            .unwrap();

        let object_expiry = receiver.expire_inactive_at(started + Duration::from_secs(5));
        assert_eq!(object_expiry.objects, 1);
        assert_eq!(object_expiry.released_object_bytes, payload.len());
        assert_eq!(object_expiry.released_datagrams, 1);
        assert_eq!(receiver.state().active_objects, 0);
        assert_eq!(receiver.state().active_flows, 1);

        let flow_expiry = receiver.expire_inactive_at(started + Duration::from_secs(10));
        assert_eq!(flow_expiry.flows, 1);
        assert_eq!(receiver.state(), UdpFecReceiverState::default());
        assert_eq!(receiver.counters().objects_expired, 1);
        assert_eq!(receiver.counters().flows_expired, 1);
    }

    #[test]
    fn automatic_expiry_scans_are_amortized_and_explicit_scans_are_forced() {
        let payload = vec![8; 2_000];
        let mut encoder = DatagramFecEncoder::new().with_symbol_size(1_000);
        let datagram = encoded_object(&mut encoder, &payload, 1).remove(0);
        let mut receiver = UdpFecReceiver::with_config(UdpFecReceiverConfig {
            object_inactivity_timeout: Duration::from_secs(10),
            flow_inactivity_timeout: Duration::from_secs(20),
            expiry_scan_interval: Duration::from_millis(250),
            ..UdpFecReceiverConfig::default()
        });
        let started = Instant::now();

        receiver
            .try_push_payload_at(peer(10_015), &datagram, started)
            .unwrap();
        assert_eq!(receiver.counters().expiry_scans, 1);
        receiver
            .try_push_payload_at(
                peer(10_015),
                &datagram,
                started + Duration::from_millis(100),
            )
            .unwrap();
        assert_eq!(receiver.counters().expiry_scans, 1);

        receiver
            .try_push_payload_at(
                peer(10_015),
                &datagram,
                started + Duration::from_millis(250),
            )
            .unwrap();
        assert_eq!(receiver.counters().expiry_scans, 2);
        receiver.expire_inactive_at(started + Duration::from_millis(251));
        assert_eq!(receiver.counters().expiry_scans, 3);
    }
}
