//! Bounded RaptorQ transport primitives for low-priority fleet telemetry.

use raptorq_datagram_fec::{
    source_symbol_count, DatagramFecDecoder, DatagramFecEncoder, DatagramFecError,
    RaptorQBlockProfile,
};
use std::collections::{HashMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

pub const TELEMETRY_ENVELOPE_MAGIC: [u8; 4] = *b"NTF1";
pub const TELEMETRY_ENVELOPE_VERSION: u8 = 1;
pub const TELEMETRY_KIND_MESH_SNAPSHOT: u8 = 1;
pub const TELEMETRY_SYMBOL_SIZE: u16 = 1_152;
pub const MAX_TELEMETRY_ENVELOPE_BYTES: usize = 32 * 1024;
pub const MAX_TELEMETRY_NODE_ID_BYTES: usize = 128;
pub const MAX_TELEMETRY_QUEUE_BLOCKS: usize = 2;
pub const MAX_TELEMETRY_QUEUE_BYTES: usize = 64 * 1024;
pub const DEFAULT_TELEMETRY_REPAIR_PERCENT: u8 = 20;
pub const DEFAULT_MAX_TELEMETRY_PEERS: usize = 256;
pub const MAX_IN_FLIGHT_BLOCKS_PER_PEER: usize = 2;
pub const DEFAULT_IN_FLIGHT_BLOCK_TTL: Duration = Duration::from_secs(15);

const ENVELOPE_HEADER_LEN: usize = 48;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryEnvelope {
    pub kind: u8,
    pub boot_id: u64,
    pub sequence: u64,
    pub captured_unix_ms: u64,
    pub period_ms: u32,
    pub node_id: String,
    pub payload: Vec<u8>,
}

impl TelemetryEnvelope {
    pub fn mesh_snapshot(
        boot_id: u64,
        sequence: u64,
        captured_unix_ms: u64,
        period_ms: u32,
        node_id: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            kind: TELEMETRY_KIND_MESH_SNAPSHOT,
            boot_id,
            sequence,
            captured_unix_ms,
            period_ms,
            node_id: node_id.into(),
            payload,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, TelemetryFecError> {
        let node_id = self.node_id.as_bytes();
        if node_id.is_empty() || node_id.len() > MAX_TELEMETRY_NODE_ID_BYTES {
            return Err(TelemetryFecError::InvalidNodeIdLength(node_id.len()));
        }
        if self.payload.is_empty() {
            return Err(TelemetryFecError::EmptyPayload);
        }
        let encoded_len = ENVELOPE_HEADER_LEN
            .checked_add(node_id.len())
            .and_then(|len| len.checked_add(self.payload.len()))
            .ok_or(TelemetryFecError::EnvelopeTooLarge(usize::MAX))?;
        if encoded_len > MAX_TELEMETRY_ENVELOPE_BYTES {
            return Err(TelemetryFecError::EnvelopeTooLarge(encoded_len));
        }

        let node_id_len = u16::try_from(node_id.len())
            .map_err(|_| TelemetryFecError::InvalidNodeIdLength(node_id.len()))?;
        let payload_len = u32::try_from(self.payload.len())
            .map_err(|_| TelemetryFecError::EnvelopeTooLarge(encoded_len))?;
        let mut encoded = Vec::with_capacity(encoded_len);
        encoded.extend_from_slice(&TELEMETRY_ENVELOPE_MAGIC);
        encoded.push(TELEMETRY_ENVELOPE_VERSION);
        encoded.push(self.kind);
        encoded.extend_from_slice(&0_u16.to_le_bytes());
        encoded.extend_from_slice(&self.boot_id.to_le_bytes());
        encoded.extend_from_slice(&self.sequence.to_le_bytes());
        encoded.extend_from_slice(&self.captured_unix_ms.to_le_bytes());
        encoded.extend_from_slice(&self.period_ms.to_le_bytes());
        encoded.extend_from_slice(&node_id_len.to_le_bytes());
        encoded.extend_from_slice(&0_u16.to_le_bytes());
        encoded.extend_from_slice(&payload_len.to_le_bytes());
        encoded.extend_from_slice(&crc32fast::hash(&self.payload).to_le_bytes());
        encoded.extend_from_slice(node_id);
        encoded.extend_from_slice(&self.payload);
        debug_assert_eq!(encoded.len(), encoded_len);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, TelemetryFecError> {
        if encoded.len() < ENVELOPE_HEADER_LEN {
            return Err(TelemetryFecError::EnvelopeTooShort(encoded.len()));
        }
        if encoded.len() > MAX_TELEMETRY_ENVELOPE_BYTES {
            return Err(TelemetryFecError::EnvelopeTooLarge(encoded.len()));
        }
        if encoded[0..4] != TELEMETRY_ENVELOPE_MAGIC {
            return Err(TelemetryFecError::InvalidMagic);
        }
        if encoded[4] != TELEMETRY_ENVELOPE_VERSION {
            return Err(TelemetryFecError::UnsupportedVersion(encoded[4]));
        }

        let node_id_len = usize::from(u16::from_le_bytes([encoded[36], encoded[37]]));
        if node_id_len == 0 || node_id_len > MAX_TELEMETRY_NODE_ID_BYTES {
            return Err(TelemetryFecError::InvalidNodeIdLength(node_id_len));
        }
        let payload_len =
            u32::from_le_bytes([encoded[40], encoded[41], encoded[42], encoded[43]]) as usize;
        let expected_len = ENVELOPE_HEADER_LEN
            .checked_add(node_id_len)
            .and_then(|len| len.checked_add(payload_len))
            .ok_or(TelemetryFecError::EnvelopeTooLarge(usize::MAX))?;
        if expected_len != encoded.len() {
            return Err(TelemetryFecError::LengthMismatch {
                declared: expected_len,
                actual: encoded.len(),
            });
        }
        if payload_len == 0 {
            return Err(TelemetryFecError::EmptyPayload);
        }

        let node_end = ENVELOPE_HEADER_LEN + node_id_len;
        let node_id = std::str::from_utf8(&encoded[ENVELOPE_HEADER_LEN..node_end])
            .map_err(|_| TelemetryFecError::InvalidNodeIdEncoding)?
            .to_string();
        let payload = encoded[node_end..].to_vec();
        let expected_crc = u32::from_le_bytes([encoded[44], encoded[45], encoded[46], encoded[47]]);
        let actual_crc = crc32fast::hash(&payload);
        if actual_crc != expected_crc {
            return Err(TelemetryFecError::PayloadChecksumMismatch);
        }

        Ok(Self {
            kind: encoded[5],
            boot_id: read_u64(encoded, 8),
            sequence: read_u64(encoded, 16),
            captured_unix_ms: read_u64(encoded, 24),
            period_ms: u32::from_le_bytes([encoded[32], encoded[33], encoded[34], encoded[35]]),
            node_id,
            payload,
        })
    }
}

fn read_u64(encoded: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        encoded[offset..offset + 8]
            .try_into()
            .expect("validated telemetry envelope header"),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePushOutcome {
    Queued,
    Replaced { blocks: usize },
}

#[derive(Debug, Default)]
struct QueueState {
    blocks: VecDeque<Vec<u8>>,
    bytes: usize,
}

#[derive(Debug, Clone, Default)]
pub struct LatestTelemetryQueue {
    state: Arc<Mutex<QueueState>>,
    notify: Arc<Notify>,
}

impl LatestTelemetryQueue {
    pub fn push(&self, envelope: Vec<u8>) -> Result<QueuePushOutcome, TelemetryFecError> {
        if envelope.len() > MAX_TELEMETRY_ENVELOPE_BYTES {
            return Err(TelemetryFecError::EnvelopeTooLarge(envelope.len()));
        }
        let mut state = self.state.lock().expect("telemetry queue lock poisoned");
        let mut replaced = 0;
        while state.blocks.len() >= MAX_TELEMETRY_QUEUE_BLOCKS
            || state.bytes.saturating_add(envelope.len()) > MAX_TELEMETRY_QUEUE_BYTES
        {
            let Some(discarded) = state.blocks.pop_front() else {
                break;
            };
            state.bytes = state.bytes.saturating_sub(discarded.len());
            replaced += 1;
        }
        state.bytes = state.bytes.saturating_add(envelope.len());
        state.blocks.push_back(envelope);
        drop(state);
        self.notify.notify_one();
        if replaced == 0 {
            Ok(QueuePushOutcome::Queued)
        } else {
            Ok(QueuePushOutcome::Replaced { blocks: replaced })
        }
    }

    pub fn try_pop(&self) -> Option<Vec<u8>> {
        let mut state = self.state.lock().expect("telemetry queue lock poisoned");
        let envelope = state.blocks.pop_front()?;
        state.bytes = state.bytes.saturating_sub(envelope.len());
        Some(envelope)
    }

    pub fn has_pending(&self) -> bool {
        !self
            .state
            .lock()
            .expect("telemetry queue lock poisoned")
            .blocks
            .is_empty()
    }

    pub async fn notified(&self) {
        self.notify.notified().await;
    }

    pub fn len(&self) -> usize {
        self.state
            .lock()
            .expect("telemetry queue lock poisoned")
            .blocks
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn bytes(&self) -> usize {
        self.state
            .lock()
            .expect("telemetry queue lock poisoned")
            .bytes
    }
}

#[derive(Debug)]
pub struct EncodedTelemetryBlock {
    pub source_datagrams: usize,
    pub datagrams: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct TelemetryFecEncoder {
    encoder: DatagramFecEncoder,
    repair_percent: u8,
}

impl Default for TelemetryFecEncoder {
    fn default() -> Self {
        Self::new(DEFAULT_TELEMETRY_REPAIR_PERCENT)
    }
}

impl TelemetryFecEncoder {
    pub fn new(repair_percent: u8) -> Self {
        Self {
            encoder: DatagramFecEncoder::new().with_symbol_size(TELEMETRY_SYMBOL_SIZE),
            repair_percent,
        }
    }

    pub fn encode(&mut self, envelope: &[u8]) -> Result<EncodedTelemetryBlock, TelemetryFecError> {
        if envelope.is_empty() {
            return Err(TelemetryFecError::EmptyPayload);
        }
        if envelope.len() > MAX_TELEMETRY_ENVELOPE_BYTES {
            return Err(TelemetryFecError::EnvelopeTooLarge(envelope.len()));
        }
        let source_datagrams =
            usize::from(source_symbol_count(envelope.len(), TELEMETRY_SYMBOL_SIZE));
        self.encoder.set_source_symbols(
            u16::try_from(source_datagrams).expect("telemetry envelope source count is bounded"),
        );
        let repair_datagrams = source_datagrams
            .saturating_mul(usize::from(self.repair_percent))
            .div_ceil(100)
            .max(1);
        let datagrams = self.encoder.encode_object_with_repair_symbols(
            envelope,
            u32::try_from(repair_datagrams).expect("telemetry repair count fits the RaptorQ API"),
        )?;
        Ok(EncodedTelemetryBlock {
            source_datagrams,
            datagrams,
        })
    }
}

#[derive(Debug)]
struct PeerDecoder {
    decoder: DatagramFecDecoder,
    blocks: HashMap<u32, Instant>,
    node_id: Option<String>,
    accepted_sequence: Option<AcceptedSequence>,
    last_seen: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AcceptedSequence {
    boot_id: u64,
    sequence: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TelemetryDecodeOutcome {
    Pending,
    Duplicate,
    Complete(TelemetryEnvelope),
}

#[derive(Debug)]
pub struct TelemetryFecDecoder {
    peers: HashMap<SocketAddr, PeerDecoder>,
    max_peers: usize,
    block_ttl: Duration,
}

impl Default for TelemetryFecDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_TELEMETRY_PEERS, DEFAULT_IN_FLIGHT_BLOCK_TTL)
    }
}

impl TelemetryFecDecoder {
    pub fn new(max_peers: usize, block_ttl: Duration) -> Self {
        Self {
            peers: HashMap::new(),
            max_peers: max_peers.max(1),
            block_ttl,
        }
    }

    pub fn push_datagram(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
    ) -> Result<TelemetryDecodeOutcome, TelemetryFecError> {
        self.push_datagram_at(peer, datagram, Instant::now())
    }

    fn push_datagram_at(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
        now: Instant,
    ) -> Result<TelemetryDecodeOutcome, TelemetryFecError> {
        let profile = RaptorQBlockProfile::from_datagram(datagram)?;
        if profile.transfer_length() as usize > MAX_TELEMETRY_ENVELOPE_BYTES {
            return Err(TelemetryFecError::EnvelopeTooLarge(
                profile.transfer_length() as usize,
            ));
        }
        if profile.symbol_size() != TELEMETRY_SYMBOL_SIZE {
            return Err(TelemetryFecError::UnexpectedSymbolSize(
                profile.symbol_size(),
            ));
        }

        self.prune_idle_peers(now);
        if !self.peers.contains_key(&peer) && self.peers.len() >= self.max_peers {
            return Err(TelemetryFecError::PeerLimitExceeded(self.max_peers));
        }
        let state = self.peers.entry(peer).or_insert_with(|| PeerDecoder {
            decoder: DatagramFecDecoder::new(),
            blocks: HashMap::new(),
            node_id: None,
            accepted_sequence: None,
            last_seen: now,
        });
        state.last_seen = now;

        let expired = state
            .blocks
            .iter()
            .filter_map(|(block_id, started)| {
                (now.saturating_duration_since(*started) >= self.block_ttl).then_some(*block_id)
            })
            .collect::<Vec<_>>();
        for block_id in expired {
            state.blocks.remove(&block_id);
            state.decoder.expire_block(block_id);
        }

        if !state.blocks.contains_key(&profile.block_id())
            && state.blocks.len() >= MAX_IN_FLIGHT_BLOCKS_PER_PEER
        {
            if let Some(oldest) = state
                .blocks
                .iter()
                .min_by_key(|(_, started)| **started)
                .map(|(block_id, _)| *block_id)
            {
                state.blocks.remove(&oldest);
                state.decoder.expire_block(oldest);
            }
        }
        state.blocks.entry(profile.block_id()).or_insert(now);

        let Some(decoded) = state.decoder.push_datagram(datagram)? else {
            return Ok(TelemetryDecodeOutcome::Pending);
        };
        state.blocks.remove(&profile.block_id());
        let envelope = TelemetryEnvelope::decode(&decoded)?;
        if envelope.kind != TELEMETRY_KIND_MESH_SNAPSHOT {
            return Err(TelemetryFecError::UnsupportedKind(envelope.kind));
        }
        if let Some(node_id) = &state.node_id {
            if node_id != &envelope.node_id {
                return Err(TelemetryFecError::PeerIdentityChanged);
            }
        } else {
            state.node_id = Some(envelope.node_id.clone());
        }

        let sequence = AcceptedSequence {
            boot_id: envelope.boot_id,
            sequence: envelope.sequence,
        };
        if state.accepted_sequence.is_some_and(|accepted| {
            accepted.boot_id == sequence.boot_id && accepted.sequence >= sequence.sequence
        }) {
            return Ok(TelemetryDecodeOutcome::Duplicate);
        }
        state.accepted_sequence = Some(sequence);
        Ok(TelemetryDecodeOutcome::Complete(envelope))
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn in_flight_block_count(&self, peer: SocketAddr) -> usize {
        self.peers.get(&peer).map_or(0, |state| state.blocks.len())
    }

    fn prune_idle_peers(&mut self, now: Instant) {
        let idle_ttl = self.block_ttl.saturating_mul(4);
        self.peers
            .retain(|_, peer| now.saturating_duration_since(peer.last_seen) < idle_ttl);
    }
}

#[derive(Debug)]
pub enum TelemetryFecError {
    EnvelopeTooShort(usize),
    EnvelopeTooLarge(usize),
    InvalidMagic,
    UnsupportedVersion(u8),
    UnsupportedKind(u8),
    InvalidNodeIdLength(usize),
    InvalidNodeIdEncoding,
    EmptyPayload,
    LengthMismatch { declared: usize, actual: usize },
    PayloadChecksumMismatch,
    UnexpectedSymbolSize(u16),
    PeerLimitExceeded(usize),
    PeerIdentityChanged,
    RaptorQ(DatagramFecError),
}

impl Display for TelemetryFecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EnvelopeTooShort(actual) => {
                write!(formatter, "telemetry envelope is too short: {actual} bytes")
            }
            Self::EnvelopeTooLarge(actual) => write!(
                formatter,
                "telemetry envelope exceeds {MAX_TELEMETRY_ENVELOPE_BYTES} bytes: {actual}"
            ),
            Self::InvalidMagic => formatter.write_str("invalid telemetry envelope magic"),
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported telemetry envelope version {version}"
                )
            }
            Self::UnsupportedKind(kind) => {
                write!(formatter, "unsupported telemetry envelope kind {kind}")
            }
            Self::InvalidNodeIdLength(actual) => {
                write!(formatter, "invalid telemetry node id length {actual}")
            }
            Self::InvalidNodeIdEncoding => {
                formatter.write_str("telemetry node id is not valid UTF-8")
            }
            Self::EmptyPayload => formatter.write_str("telemetry payload is empty"),
            Self::LengthMismatch { declared, actual } => write!(
                formatter,
                "telemetry envelope length mismatch: declared {declared}, actual {actual}"
            ),
            Self::PayloadChecksumMismatch => {
                formatter.write_str("telemetry payload checksum mismatch")
            }
            Self::UnexpectedSymbolSize(actual) => write!(
                formatter,
                "unexpected telemetry RaptorQ symbol size {actual}"
            ),
            Self::PeerLimitExceeded(maximum) => {
                write!(formatter, "telemetry peer limit {maximum} exceeded")
            }
            Self::PeerIdentityChanged => {
                formatter.write_str("telemetry peer changed its node identity")
            }
            Self::RaptorQ(error) => write!(formatter, "telemetry RaptorQ error: {error}"),
        }
    }
}

impl std::error::Error for TelemetryFecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RaptorQ(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DatagramFecError> for TelemetryFecError {
    fn from(error: DatagramFecError) -> Self {
        Self::RaptorQ(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(sequence: u64, payload_len: usize) -> TelemetryEnvelope {
        TelemetryEnvelope::mesh_snapshot(
            44,
            sequence,
            1_700_000_000_000,
            5_000,
            "uk-edge-1",
            vec![sequence as u8; payload_len],
        )
    }

    #[test]
    fn envelope_round_trip_and_checksum_rejection() {
        let expected = envelope(7, 4_096);
        let mut encoded = expected.encode().expect("encode envelope");
        assert_eq!(TelemetryEnvelope::decode(&encoded).unwrap(), expected);

        *encoded.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            TelemetryEnvelope::decode(&encoded),
            Err(TelemetryFecError::PayloadChecksumMismatch)
        ));
    }

    #[test]
    fn envelope_enforces_total_bound_before_fec_allocation() {
        let oversized = envelope(1, MAX_TELEMETRY_ENVELOPE_BYTES);
        assert!(matches!(
            oversized.encode(),
            Err(TelemetryFecError::EnvelopeTooLarge(_))
        ));
    }

    #[test]
    fn latest_queue_replaces_oldest_without_exceeding_bounds() {
        let queue = LatestTelemetryQueue::default();
        let first = envelope(1, 20_000).encode().unwrap();
        let second = envelope(2, 20_000).encode().unwrap();
        let third = envelope(3, 20_000).encode().unwrap();

        assert_eq!(queue.push(first).unwrap(), QueuePushOutcome::Queued);
        assert_eq!(queue.push(second).unwrap(), QueuePushOutcome::Queued);
        assert_eq!(
            queue.push(third).unwrap(),
            QueuePushOutcome::Replaced { blocks: 1 }
        );
        assert_eq!(queue.len(), 2);
        assert!(queue.bytes() <= MAX_TELEMETRY_QUEUE_BYTES);
        assert_eq!(
            TelemetryEnvelope::decode(&queue.try_pop().unwrap())
                .unwrap()
                .sequence,
            2
        );
        assert_eq!(
            TelemetryEnvelope::decode(&queue.try_pop().unwrap())
                .unwrap()
                .sequence,
            3
        );
    }

    #[test]
    fn fec_recovers_one_missing_source_and_rejects_replay() {
        let peer: SocketAddr = "127.0.0.1:7300".parse().unwrap();
        let expected = envelope(9, 12_000);
        let encoded = expected.encode().unwrap();
        let mut encoder = TelemetryFecEncoder::new(25);
        let block = encoder.encode(&encoded).unwrap();
        assert!(block.datagrams.len() > block.source_datagrams);

        let mut source_only = DatagramFecDecoder::new();
        let mut source_decoded = None;
        for datagram in block.datagrams.iter().take(block.source_datagrams) {
            source_decoded = source_only
                .push_datagram(datagram)
                .unwrap()
                .or(source_decoded);
        }
        assert_eq!(source_decoded, Some(encoded.clone()));

        let mut decoder = TelemetryFecDecoder::default();
        let mut complete = None;
        for (index, datagram) in block.datagrams.iter().enumerate() {
            if index == 1 {
                continue;
            }
            if let TelemetryDecodeOutcome::Complete(decoded) =
                decoder.push_datagram(peer, datagram).unwrap()
            {
                complete = Some(decoded);
            }
        }
        assert_eq!(complete, Some(expected.clone()));

        let replay = encoder.encode(&encoded).unwrap();
        let mut saw_duplicate = false;
        for datagram in &replay.datagrams {
            if decoder.push_datagram(peer, datagram).unwrap() == TelemetryDecodeOutcome::Duplicate {
                saw_duplicate = true;
            }
        }
        assert!(saw_duplicate);

        let restarted = TelemetryEnvelope::mesh_snapshot(
            expected.boot_id + 1,
            1,
            expected.captured_unix_ms + 5_000,
            expected.period_ms,
            expected.node_id.clone(),
            expected.payload.clone(),
        );
        let restarted_block = encoder.encode(&restarted.encode().unwrap()).unwrap();
        let mut accepted_restart = false;
        for datagram in &restarted_block.datagrams {
            if decoder.push_datagram(peer, datagram).unwrap()
                == TelemetryDecodeOutcome::Complete(restarted.clone())
            {
                accepted_restart = true;
            }
        }
        assert!(accepted_restart);
    }

    #[test]
    fn decoder_rejects_oversized_fec_geometry_before_retaining_a_block() {
        let peer: SocketAddr = "127.0.0.1:7300".parse().unwrap();
        let mut encoder = DatagramFecEncoder::new().with_symbol_size(TELEMETRY_SYMBOL_SIZE);
        let datagrams = encoder
            .encode_object(&vec![0_u8; MAX_TELEMETRY_ENVELOPE_BYTES + 1])
            .unwrap();
        let mut decoder = TelemetryFecDecoder::default();
        assert!(matches!(
            decoder.push_datagram(peer, &datagrams[0]),
            Err(TelemetryFecError::EnvelopeTooLarge(_))
        ));
        assert_eq!(decoder.in_flight_block_count(peer), 0);
    }

    #[test]
    fn five_percent_deterministic_loss_delivers_at_least_ninety_nine_percent() {
        const SNAPSHOTS: u64 = 200;
        let peer: SocketAddr = "127.0.0.1:7300".parse().unwrap();
        let mut encoder = TelemetryFecEncoder::default();
        let mut decoder = TelemetryFecDecoder::default();
        let mut datagram_index = 0_u64;
        let mut decoded = 0_u64;

        for sequence in 1..=SNAPSHOTS {
            let encoded = envelope(sequence, 12_000).encode().unwrap();
            let block = encoder.encode(&encoded).unwrap();
            let mut completed = false;
            for datagram in &block.datagrams {
                datagram_index = datagram_index.saturating_add(1);
                if datagram_index.is_multiple_of(20) {
                    continue;
                }
                if matches!(
                    decoder.push_datagram(peer, datagram).unwrap(),
                    TelemetryDecodeOutcome::Complete(_)
                ) {
                    completed = true;
                }
            }
            decoded = decoded.saturating_add(u64::from(completed));
        }

        assert!(
            decoded * 100 >= SNAPSHOTS * 99,
            "decoded {decoded}/{SNAPSHOTS}"
        );
    }

    #[test]
    fn decoder_bounds_peers_and_in_flight_blocks() {
        let now = Instant::now();
        let peer_a: SocketAddr = "127.0.0.1:7300".parse().unwrap();
        let peer_b: SocketAddr = "127.0.0.1:7301".parse().unwrap();
        let mut encoder = TelemetryFecEncoder::default();
        let one = encoder
            .encode(&envelope(1, 4_000).encode().unwrap())
            .unwrap();
        let two = encoder
            .encode(&envelope(2, 4_000).encode().unwrap())
            .unwrap();
        let three = encoder
            .encode(&envelope(3, 4_000).encode().unwrap())
            .unwrap();
        let mut decoder = TelemetryFecDecoder::new(1, Duration::from_secs(15));

        decoder
            .push_datagram_at(peer_a, &one.datagrams[0], now)
            .unwrap();
        decoder
            .push_datagram_at(peer_a, &two.datagrams[0], now)
            .unwrap();
        decoder
            .push_datagram_at(peer_a, &three.datagrams[0], now)
            .unwrap();
        assert_eq!(
            decoder.in_flight_block_count(peer_a),
            MAX_IN_FLIGHT_BLOCKS_PER_PEER
        );
        assert!(matches!(
            decoder.push_datagram_at(peer_b, &one.datagrams[0], now),
            Err(TelemetryFecError::PeerLimitExceeded(1))
        ));
    }

    #[test]
    fn decoder_expires_idle_peer_state() {
        let now = Instant::now();
        let peer_a: SocketAddr = "127.0.0.1:7300".parse().unwrap();
        let peer_b: SocketAddr = "127.0.0.1:7301".parse().unwrap();
        let mut encoder = TelemetryFecEncoder::default();
        let block = encoder
            .encode(&envelope(1, 4_000).encode().unwrap())
            .unwrap();
        let mut decoder = TelemetryFecDecoder::new(1, Duration::from_secs(1));

        decoder
            .push_datagram_at(peer_a, &block.datagrams[0], now)
            .unwrap();
        decoder
            .push_datagram_at(peer_b, &block.datagrams[0], now + Duration::from_secs(5))
            .unwrap();
        assert_eq!(decoder.peer_count(), 1);
    }
}
