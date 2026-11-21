//! A zero-allocation view of one RLS1 datagram.
//!
//! The fast plane forwards a RelaySession datagram byte for byte, so it needs
//! only the header fields that admission, deduplication and the per-path
//! lane rule read. `RlsView::parse` reads them in place and accepts exactly
//! the datagrams that `relay_session::decode_datagram` accepts, without the
//! owned strings, the copy of the RQD2 datagram and the RaptorQ packet that
//! the full decode builds.

use raptorq_datagram_fec::{
    DatagramFecHeader, MediaDatagramRole, MediaObjectKind, MediaPathIntent,
    DATAGRAM_AUDIO_SESSION_ID_LEN, DATAGRAM_FLAG_AUDIO_SESSION_ID, DATAGRAM_FLAG_PACKET_CRC32,
};
use relay_session::{CodingProfile, RelayLimits, SubscriptionId, TopologyGeneration};

const HEADER_LEN: usize = 8;
const RQD2_HEADER_LEN: usize = 32;
const PAYLOAD_ID_LEN: usize = 4;
/// media_object's hard identifier ceiling, which `ObjectKey::new` applies.
const MEDIA_OBJECT_MAX_IDENTIFIER_BYTES: usize = 128;

/// The fields of an RLS1 datagram that the fast plane reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RlsView<'a> {
    pub generation: u64,
    pub subscription_id: u64,
    pub deadline_us: u64,
    pub role: MediaDatagramRole,
    pub kind: MediaObjectKind,
    pub path_intent: MediaPathIntent,
    pub tenant: &'a str,
    pub stream: &'a str,
    pub track: &'a str,
    pub epoch: u64,
    pub group: u64,
    pub object: u64,
    pub version: u32,
    pub payload_hash: &'a [u8; 32],
    /// The RQD2 datagram that the RLS1 header carries.
    pub fec: &'a [u8],
    pub block_id: u32,
    pub packet_sequence: u32,
    pub source_symbols: u16,
    /// The encoding symbol id of the RaptorQ payload id.
    pub esi: u32,
}

/// Why a datagram is not a valid RLS1 datagram. The fast plane only counts
/// these; the tee's full decode reports the reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewError(pub &'static str);

struct Reader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], ViewError> {
        let end = self.offset.checked_add(len).ok_or(ViewError("truncated"))?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or(ViewError("truncated"))?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, ViewError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ViewError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> Result<u32, ViewError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64, ViewError> {
        let bytes = self.take(8)?;
        let mut value = [0_u8; 8];
        value.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(value))
    }

    fn identifier(&mut self, maximum: usize) -> Result<&'a str, ViewError> {
        let len = usize::from(self.u16()?);
        if len > maximum {
            return Err(ViewError("identifier too long"));
        }
        let bytes = self.take(len)?;
        let value = std::str::from_utf8(bytes).map_err(|_| ViewError("identifier not UTF-8"))?;
        if value.is_empty() || len > MEDIA_OBJECT_MAX_IDENTIFIER_BYTES {
            return Err(ViewError("identifier empty or too long"));
        }
        if value.chars().any(char::is_control) {
            return Err(ViewError("identifier has a control character"));
        }
        Ok(value)
    }
}

/// `DatagramFecHeader::payload`, with the CRC taken in one pass over the
/// wire bytes: the header prefix that the CRC covers is exactly the first 28
/// bytes of a header that decoded.
fn rqd2_payload<'a>(header: &DatagramFecHeader, fec: &'a [u8]) -> Result<&'a [u8], ViewError> {
    let extension = if header.packet_flags & DATAGRAM_FLAG_AUDIO_SESSION_ID != 0 {
        DATAGRAM_AUDIO_SESSION_ID_LEN
    } else {
        0
    };
    let payload_len = header.payload_len as usize;
    if fec.len() - RQD2_HEADER_LEN != payload_len.saturating_add(extension) {
        return Err(ViewError("fec payload length"));
    }
    if header.packet_flags & DATAGRAM_FLAG_PACKET_CRC32 != 0 {
        let mut crc = crc32fast::Hasher::new();
        crc.update(&fec[..RQD2_HEADER_LEN - 4]);
        crc.update(&fec[RQD2_HEADER_LEN..]);
        if crc.finalize() != header.packet_crc32 {
            return Err(ViewError("fec crc"));
        }
    }
    Ok(&fec[RQD2_HEADER_LEN..RQD2_HEADER_LEN + payload_len])
}

impl<'a> RlsView<'a> {
    /// Read an RLS1 datagram in place. Accepts exactly what
    /// `relay_session::decode_datagram(input, limits)` accepts.
    pub fn parse(input: &'a [u8], limits: RelayLimits) -> Result<Self, ViewError> {
        if input.len() > limits.max_datagram_bytes {
            return Err(ViewError("datagram too long"));
        }
        if input.len() < HEADER_LEN {
            return Err(ViewError("truncated"));
        }
        if input[..4] != *b"RLS1" {
            return Err(ViewError("not RLS1"));
        }
        if input[4] != 1 || input[5] != 0 {
            return Err(ViewError("unsupported version"));
        }
        let declared = usize::from(u16::from_be_bytes([input[6], input[7]]));
        if declared != input.len() - HEADER_LEN {
            return Err(ViewError("declared length"));
        }
        let mut reader = Reader {
            input: &input[HEADER_LEN..],
            offset: 0,
        };
        let generation = reader.u64()?;
        TopologyGeneration::new(generation).map_err(|_| ViewError("generation"))?;
        let subscription_id = reader.u64()?;
        SubscriptionId::new(subscription_id).map_err(|_| ViewError("subscription"))?;
        let deadline_us = reader.u64()?;
        let role = match reader.u8()? {
            0 => MediaDatagramRole::Source,
            1 => MediaDatagramRole::Repair,
            _ => return Err(ViewError("role")),
        };
        let kind = match reader.u8()? {
            0 => MediaObjectKind::Initialization,
            1 => MediaObjectKind::CodecConfig,
            2 => MediaObjectKind::VideoKeyframe,
            3 => MediaObjectKind::Audio,
            4 => MediaObjectKind::VideoDelta,
            5 => MediaObjectKind::Data,
            _ => return Err(ViewError("kind")),
        };
        let path_intent = match reader.u8()? {
            0 => MediaPathIntent::PrimarySource,
            1 => MediaPathIntent::SecondaryRepair,
            2 => MediaPathIntent::ReliableObjectFetch,
            _ => return Err(ViewError("path")),
        };
        if reader.u8()? != 0 {
            return Err(ViewError("reserved"));
        }
        let tenant = reader.identifier(limits.max_identifier_bytes)?;
        let stream = reader.identifier(limits.max_identifier_bytes)?;
        let track = reader.identifier(limits.max_identifier_bytes)?;
        let epoch = reader.u64()?;
        let group = reader.u64()?;
        let object = reader.u64()?;
        let version = reader.u32()?;
        let payload_hash: &[u8; 32] = reader
            .take(32)?
            .try_into()
            .map_err(|_| ViewError("truncated"))?;
        let fec_len = usize::from(reader.u16()?);
        if fec_len > limits.max_fec_datagram_bytes {
            return Err(ViewError("fec datagram too long"));
        }
        let fec = reader.take(fec_len)?;
        if reader.offset != reader.input.len() {
            return Err(ViewError("trailing bytes"));
        }

        // CodingProfile::from_fec_datagram, without the copy of the symbol
        // that its RaptorQ packet makes.
        if fec.len() < RQD2_HEADER_LEN + PAYLOAD_ID_LEN {
            return Err(ViewError("fec datagram too short"));
        }
        let header = DatagramFecHeader::decode(fec).map_err(|_| ViewError("fec header"))?;
        let payload = rqd2_payload(&header, fec)?;
        if payload.len() < PAYLOAD_ID_LEN {
            return Err(ViewError("fec payload too short"));
        }
        if payload[0] != 0 {
            return Err(ViewError("source block number"));
        }
        if payload.len() - PAYLOAD_ID_LEN != usize::from(header.symbol_size) {
            return Err(ViewError("symbol size"));
        }
        let block = header
            .block_profile()
            .map_err(|_| ViewError("fec geometry"))?;
        CodingProfile::from_block(block)
            .validate(limits)
            .map_err(|_| ViewError("coding profile"))?;
        let intent_matches = matches!(
            (role, path_intent),
            (MediaDatagramRole::Source, MediaPathIntent::PrimarySource)
                | (MediaDatagramRole::Repair, MediaPathIntent::SecondaryRepair)
        );
        if !intent_matches {
            return Err(ViewError("path intent"));
        }
        Ok(Self {
            generation,
            subscription_id,
            deadline_us,
            role,
            kind,
            path_intent,
            tenant,
            stream,
            track,
            epoch,
            group,
            object,
            version,
            payload_hash,
            fec,
            block_id: header.block_id,
            packet_sequence: header.packet_sequence,
            source_symbols: header.source_symbols,
            esi: u32::from_be_bytes([0, payload[1], payload[2], payload[3]]),
        })
    }

    /// The numeric stream id, as the node's caches key it.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn stream_id(&self) -> Option<u64> {
        self.stream.parse().ok()
    }

    /// A video object kind. A muxed delta part can be tagged audio, so a
    /// stream is video when any of its objects says so, or when it arrives
    /// on the video port.
    pub const fn is_video_kind(&self) -> bool {
        matches!(
            self.kind,
            MediaObjectKind::VideoKeyframe | MediaObjectKind::VideoDelta
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use media_object::{ObjectKey, PayloadHash};
    use raptorq_datagram_fec::MediaDeadline;
    use relay_session::{decode_datagram, encode_datagram, RelayDatagram};

    pub(crate) fn sample_datagram(
        stream: &str,
        object: u64,
        role: MediaDatagramRole,
        payload_len: usize,
    ) -> Vec<u8> {
        let symbol_size = 64_u16;
        let transfer_length = payload_len.max(1) as u32;
        let source_symbols = transfer_length.div_ceil(u32::from(symbol_size)) as u16;
        let data = vec![0x5a_u8; usize::from(symbol_size)];
        let esi = if role == MediaDatagramRole::Source {
            0
        } else {
            u32::from(source_symbols)
        };
        let mut payload = vec![0, (esi >> 16) as u8, (esi >> 8) as u8, esi as u8];
        payload.extend_from_slice(&data);
        let header = DatagramFecHeader::raptorq(
            7,
            transfer_length,
            42 + esi,
            source_symbols,
            symbol_size,
            &payload,
        )
        .unwrap();
        let mut fec = vec![0_u8; RQD2_HEADER_LEN];
        header.encode(&mut fec).unwrap();
        fec.extend_from_slice(&payload);
        let key = ObjectKey::new(
            "tenant",
            stream,
            "track",
            3,
            4,
            object,
            1,
            PayloadHash::from_bytes([9; 32]),
        )
        .unwrap();
        let datagram = RelayDatagram::new(
            TopologyGeneration::new(5).unwrap(),
            SubscriptionId::new(6).unwrap(),
            key,
            MediaObjectKind::Audio,
            role,
            if role == MediaDatagramRole::Source {
                MediaPathIntent::PrimarySource
            } else {
                MediaPathIntent::SecondaryRepair
            },
            MediaDeadline::from_micros(123_456),
            fec,
            RelayLimits::default(),
        )
        .unwrap();
        encode_datagram(&datagram, RelayLimits::default()).unwrap()
    }

    fn assert_same(input: &[u8]) {
        let limits = RelayLimits::default();
        let full = decode_datagram(input, limits);
        let view = RlsView::parse(input, limits);
        match (&full, &view) {
            (Ok(full), Ok(view)) => {
                assert_eq!(view.generation, full.generation.get());
                assert_eq!(view.subscription_id, full.subscription_id.get());
                assert_eq!(view.deadline_us, full.deadline.expires_at_us);
                assert_eq!(view.role, full.role);
                assert_eq!(view.kind, full.object_kind);
                assert_eq!(view.path_intent, full.path_intent);
                assert_eq!(view.tenant, full.object_key.tenant());
                assert_eq!(view.stream, full.object_key.stream());
                assert_eq!(view.track, full.object_key.track());
                assert_eq!(view.epoch, full.object_key.epoch());
                assert_eq!(view.group, full.object_key.group());
                assert_eq!(view.object, full.object_key.object());
                assert_eq!(view.version, full.object_key.version());
                assert_eq!(view.payload_hash, full.object_key.payload_hash().as_bytes());
                assert_eq!(view.fec, full.fec_datagram.as_slice());
                assert_eq!(view.block_id, full.coding.block_id());
                assert_eq!(view.source_symbols, full.coding.source_symbols());
            }
            (Err(_), Err(_)) => {}
            _ => panic!(
                "view and decode disagree on {input:02x?}: decode {:?}, view {:?}",
                full.as_ref().err(),
                view.as_ref().err()
            ),
        }
    }

    /// A small xorshift, so that the property test needs no new crate.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, bound: usize) -> usize {
            (self.next() % bound as u64) as usize
        }
    }

    #[test]
    fn the_view_reads_what_the_full_decode_reads() {
        for (role, len) in [
            (MediaDatagramRole::Source, 60),
            (MediaDatagramRole::Repair, 60),
            (MediaDatagramRole::Source, 700),
            (MediaDatagramRole::Repair, 2_000),
        ] {
            let wire = sample_datagram("4001", 77, role, len);
            let view = RlsView::parse(&wire, RelayLimits::default()).unwrap();
            assert_eq!(view.stream_id(), Some(4001));
            assert_eq!(view.object, 77);
            assert_eq!(view.role, role);
            assert_same(&wire);
        }
    }

    #[test]
    fn the_view_accepts_and_rejects_exactly_what_the_full_decode_does() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let bases = [
            sample_datagram("4001", 1, MediaDatagramRole::Source, 60),
            sample_datagram("1", 2, MediaDatagramRole::Repair, 900),
            sample_datagram("video-x", 3, MediaDatagramRole::Source, 3_000),
        ];
        for round in 0..30_000 {
            let mut wire = bases[round % bases.len()].clone();
            match rng.below(5) {
                // One byte changed anywhere: header fields, identifiers,
                // lengths, the RQD2 header and its CRC.
                0 | 1 => {
                    let at = rng.below(wire.len());
                    wire[at] = rng.next() as u8;
                }
                // A few bytes of the first 80, where most fields live.
                2 => {
                    for _ in 0..3 {
                        let at = rng.below(wire.len().min(80));
                        wire[at] = rng.next() as u8;
                    }
                }
                // Truncated or extended.
                3 => {
                    let len = rng.below(wire.len() + 1);
                    wire.truncate(len);
                }
                _ => {
                    for _ in 0..rng.below(4) + 1 {
                        wire.push(rng.next() as u8);
                    }
                }
            }
            assert_same(&wire);
        }
        // Random bytes behind a valid magic.
        for _ in 0..5_000 {
            let len = rng.below(200);
            let mut wire = b"RLS1\x01\x00".to_vec();
            wire.extend_from_slice(&((len as u16).to_be_bytes()));
            for _ in 0..len {
                wire.push(rng.next() as u8);
            }
            assert_same(&wire);
        }
    }

    #[test]
    fn a_view_takes_well_under_300_ns_per_datagram() {
        let wire = sample_datagram("4001", 1, MediaDatagramRole::Source, 1_000);
        let rounds = 20_000_u32;
        let mut objects = 0_u64;
        // The best of a few runs, so that other tests running beside this
        // one do not decide it.
        let best = (0..10)
            .map(|_| {
                let started = std::time::Instant::now();
                for _ in 0..rounds {
                    let view = RlsView::parse(std::hint::black_box(&wire), RelayLimits::default())
                        .unwrap();
                    objects = objects.wrapping_add(view.object);
                }
                started.elapsed() / rounds
            })
            .min()
            .unwrap();
        assert!(objects > 0);
        eprintln!("RlsView::parse: {best:?} per datagram");
        // The bound holds in a release build; a debug build is slower.
        if !cfg!(debug_assertions) {
            assert!(
                best < std::time::Duration::from_nanos(300),
                "{best:?} per datagram"
            );
        }
    }
}
