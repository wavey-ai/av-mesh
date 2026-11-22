//! The canonical live slot: how a node stores a canonical media object in
//! its cache, and how the bytes of an LL-HLS part are read back out of it.
//!
//! One module for both ends of the fast client lane. The commit of every
//! node (relay, distributor, edge) builds the slot of a decoded RelaySession
//! object with [`encode_canonical_live_slot`], and the LL-HLS part is
//! [`slot_media`] of that slot. A UDP client on the fast lane decodes the
//! contributor's own symbols into the same canonical envelope, builds the
//! slot with the same function and reads the part with the same function,
//! so the client's part bytes equal the edge's LL-HLS part bytes by
//! construction, not by a second implementation that has to agree.
//!
//! No tokio, no sockets, no clock: the module builds for wasm32 (see the
//! `wasm-check` target of the Makefile), where a browser client runs it.
//!
//! A canonical slot is a 40-byte index and then the exact envelope:
//!
//! | bytes  | field                                                    |
//! | ------ | -------------------------------------------------------- |
//! | 0..8   | `AVMOBJS1`                                               |
//! | 8..16  | stream id, big endian                                    |
//! | 16     | media kind tag (0 fMP4, 1 opaque, 2 TS)                  |
//! | 17..20 | zero                                                     |
//! | 20..24 | envelope length                                          |
//! | 24..32 | init offset and length in the slot (0, 0 for none)       |
//! | 32..40 | media offset and length in the slot                      |
//! | 40..   | the canonical media-object envelope, byte for byte       |

use bytes::{BufMut, Bytes, BytesMut};
use media_object::{MediaObject, ObjectKind, Stage, WIRE_MAGIC};
use std::fmt;

pub const CANONICAL_LIVE_SLOT_MAGIC: &[u8; 8] = b"AVMOBJS1";
pub const CANONICAL_LIVE_SLOT_HEADER_LEN: usize = 40;
/// An fMP4 slot (init and fragment) without an envelope, as the cache mesh
/// and the fMP4 rebox of a Soundkit bundle store it.
pub const MESH_FMP4_SLOT_MAGIC: &[u8; 8] = b"AVFMP4S1";
pub const MESH_FMP4_SLOT_HEADER_LEN: usize = 16;
/// The payload format of a Soundkit v2 bundle. Its slot is reboxed at the
/// commit, so its part bytes are not a slice of the envelope: it stays off
/// the fast client lane.
pub const SOUNDKIT_V2_BUNDLE_FORMAT: &[u8] = b"soundkit-v2-bundle";

pub const FMP4_CONTENT_TYPE: &str = "video/mp4";
pub const TS_CONTENT_TYPE: &str = "video/mp2t";
pub const OPAQUE_CONTENT_TYPE: &str = "application/octet-stream";

/// How a part's bytes are served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveMediaKind {
    Fmp4,
    Opaque,
    Ts,
}

impl LiveMediaKind {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Fmp4 => "mp4",
            Self::Opaque => "bin",
            Self::Ts => "ts",
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            Self::Fmp4 => FMP4_CONTENT_TYPE,
            Self::Opaque => OPAQUE_CONTENT_TYPE,
            Self::Ts => TS_CONTENT_TYPE,
        }
    }

    pub const fn canonical_slot_tag(self) -> u8 {
        match self {
            Self::Fmp4 => 0,
            Self::Opaque => 1,
            Self::Ts => 2,
        }
    }

    pub const fn from_canonical_slot_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Fmp4),
            1 => Some(Self::Opaque),
            2 => Some(Self::Ts),
            _ => None,
        }
    }
}

/// Why a canonical slot could not be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotError(&'static str);

impl SlotError {
    pub const fn reason(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for SlotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for SlotError {}

fn metadata_is(object: &MediaObject, key: &str, value: &[u8]) -> bool {
    object
        .metadata()
        .get(key)
        .is_some_and(|found| found.as_slice() == value)
}

/// Whether the object is a Soundkit v2 bundle (reboxed at the commit).
pub fn is_soundkit_v2_bundle(object: &MediaObject) -> bool {
    metadata_is(object, "payload-format", SOUNDKIT_V2_BUNDLE_FORMAT)
}

/// The media kind of a canonical object, as the commit stores it: fMP4 for
/// an fMP4 container, opaque for an opaque part, TS for anything else. (A
/// Soundkit bundle, reboxed to fMP4, is the commit's own case.)
pub fn live_media_kind(object: &MediaObject) -> LiveMediaKind {
    if metadata_is(object, "container", b"fmp4") {
        LiveMediaKind::Fmp4
    } else if metadata_is(object, "container", b"opaque")
        && metadata_is(object, "payload-format", b"opaque-part-v1")
    {
        LiveMediaKind::Opaque
    } else {
        LiveMediaKind::Ts
    }
}

/// The slot of a canonical media object: the 40-byte index, then `envelope`
/// (the object's exact wire bytes) unchanged.
pub fn encode_canonical_live_slot(
    stream_id: u64,
    object: &MediaObject,
    envelope: Bytes,
    media_kind: LiveMediaKind,
) -> Result<Bytes, SlotError> {
    if object.kind() != ObjectKind::Media || object.payload().is_empty() {
        return Err(SlotError(
            "only non-empty canonical media objects can enter the live media cache",
        ));
    }
    let envelope_len =
        u32::try_from(envelope.len()).map_err(|_| SlotError("canonical envelope is too large"))?;
    let object_payload_offset = CANONICAL_LIVE_SLOT_HEADER_LEN
        .checked_add(
            envelope
                .len()
                .checked_sub(object.payload().len())
                .ok_or(SlotError(
                    "canonical payload is not contained in its envelope",
                ))?,
        )
        .ok_or(SlotError("canonical live-slot payload offset overflow"))?;
    let is_fmp4_slot =
        media_kind == LiveMediaKind::Fmp4 && metadata_is(object, "payload-format", b"fmp4-slot-v1");
    let (init_offset, init_len, media_offset, media_len) = if is_fmp4_slot {
        let slot = object.payload();
        if slot.len() < MESH_FMP4_SLOT_HEADER_LEN || !slot.starts_with(MESH_FMP4_SLOT_MAGIC) {
            return Err(SlotError("canonical fMP4 slot has an invalid header"));
        }
        let init_len = u32::from_be_bytes(slot[8..12].try_into().unwrap()) as usize;
        let media_len = u32::from_be_bytes(slot[12..16].try_into().unwrap()) as usize;
        let init_start = object_payload_offset
            .checked_add(MESH_FMP4_SLOT_HEADER_LEN)
            .ok_or(SlotError("canonical fMP4 init offset overflow"))?;
        let media_start = init_start
            .checked_add(init_len)
            .ok_or(SlotError("canonical fMP4 media offset overflow"))?;
        let media_end = media_start
            .checked_add(media_len)
            .ok_or(SlotError("canonical fMP4 media length overflow"))?;
        if media_end != CANONICAL_LIVE_SLOT_HEADER_LEN + envelope.len() {
            return Err(SlotError(
                "canonical fMP4 slot lengths do not match its envelope",
            ));
        }
        (
            if init_len > 0 { init_start } else { 0 },
            init_len,
            media_start,
            media_len,
        )
    } else {
        (0, 0, object_payload_offset, object.payload().len())
    };
    let field =
        |value: usize, what: &'static str| u32::try_from(value).map_err(|_| SlotError(what));

    let mut cached = BytesMut::with_capacity(CANONICAL_LIVE_SLOT_HEADER_LEN + envelope.len());
    cached.put_slice(CANONICAL_LIVE_SLOT_MAGIC);
    cached.put_u64(stream_id);
    cached.put_u8(media_kind.canonical_slot_tag());
    cached.put_bytes(0, 3);
    cached.put_u32(envelope_len);
    cached.put_u32(field(init_offset, "canonical init offset is too large")?);
    cached.put_u32(field(init_len, "canonical init is too large")?);
    cached.put_u32(field(media_offset, "canonical media offset is too large")?);
    cached.put_u32(field(media_len, "canonical media is too large")?);
    cached.put_slice(&envelope);
    debug_assert_eq!(
        cached.len(),
        CANONICAL_LIVE_SLOT_HEADER_LEN + envelope.len()
    );
    Ok(cached.freeze())
}

/// A canonical envelope as the fast client lane delivers it: the decoded
/// object and the slot that the commit of every node stores for it.
#[derive(Debug, Clone)]
pub struct CanonicalPart {
    pub object: MediaObject,
    pub stream_id: u64,
    pub slot: Bytes,
}

/// Decode a canonical media-object envelope (the RelaySession transfer
/// object) and build its slot exactly as the commit does. Refuses what the
/// commit would not store as a canonical slot: an object that is not media,
/// a stream that is not numeric, and a Soundkit bundle (reboxed at the
/// commit). The envelope's payload hash is verified by the decode.
pub fn canonical_part_from_envelope(envelope: Bytes) -> Result<CanonicalPart, SlotError> {
    if !envelope.starts_with(&WIRE_MAGIC) {
        return Err(SlotError("not a canonical media-object envelope"));
    }
    let object =
        media_object::decode(&envelope).map_err(|_| SlotError("invalid canonical envelope"))?;
    if object.kind() != ObjectKind::Media {
        return Err(SlotError("not a media object"));
    }
    if is_soundkit_v2_bundle(&object) {
        return Err(SlotError(
            "a Soundkit bundle is reboxed at the commit and stays off the fast lane",
        ));
    }
    let stream_id = object
        .key()
        .stream()
        .parse::<u64>()
        .map_err(|_| SlotError("the stream id is not numeric"))?;
    let kind = live_media_kind(&object);
    let slot = encode_canonical_live_slot(stream_id, &object, envelope, kind)?;
    Ok(CanonicalPart {
        object,
        stream_id,
        slot,
    })
}

/// The part bytes, the init segment and the kind of a canonical slot, read
/// from its index: slices of the slot, no copy and no decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSlotView {
    pub kind: LiveMediaKind,
    pub init: Option<Bytes>,
    pub media: Bytes,
}

/// Read a canonical slot's index, checking every offset; `None` for a slot
/// that is not canonical, is of another stream (when `expected_stream_id`
/// is given) or is malformed.
pub fn decode_canonical_live_slot(
    payload: &Bytes,
    expected_stream_id: Option<u64>,
) -> Option<CanonicalSlotView> {
    if payload.len() < CANONICAL_LIVE_SLOT_HEADER_LEN
        || !payload.starts_with(CANONICAL_LIVE_SLOT_MAGIC)
        || payload[17..20] != [0, 0, 0]
    {
        return None;
    }
    let be32 = |at: usize| -> Option<usize> {
        Some(u32::from_be_bytes(payload.get(at..at + 4)?.try_into().ok()?) as usize)
    };
    let stream_id = u64::from_be_bytes(payload[8..16].try_into().ok()?);
    if expected_stream_id.is_some_and(|expected| expected != stream_id) {
        return None;
    }
    let kind = LiveMediaKind::from_canonical_slot_tag(payload[16])?;
    if be32(20)? != payload.len().checked_sub(CANONICAL_LIVE_SLOT_HEADER_LEN)?
        || !payload[CANONICAL_LIVE_SLOT_HEADER_LEN..].starts_with(&WIRE_MAGIC)
    {
        return None;
    }
    let (init_offset, init_len) = (be32(24)?, be32(28)?);
    let (media_offset, media_len) = (be32(32)?, be32(36)?);
    let range = |offset: usize, len: usize| {
        let end = offset.checked_add(len)?;
        (offset >= CANONICAL_LIVE_SLOT_HEADER_LEN && end <= payload.len())
            .then(|| payload.slice(offset..end))
    };
    let init = if init_len == 0 {
        if init_offset != 0 {
            return None;
        }
        None
    } else {
        Some(range(init_offset, init_len)?)
    };
    if init.is_some() && kind != LiveMediaKind::Fmp4 {
        return None;
    }
    let media = range(media_offset, media_len)?;
    Some(CanonicalSlotView { kind, init, media })
}

/// The init segment and the fragment of an fMP4 slot (`AVFMP4S1`).
pub fn decode_fmp4_slot(payload: &Bytes) -> Option<(Bytes, Option<Bytes>)> {
    if payload.len() < MESH_FMP4_SLOT_HEADER_LEN || !payload.starts_with(MESH_FMP4_SLOT_MAGIC) {
        return None;
    }
    let init_len = u32::from_be_bytes(payload[8..12].try_into().ok()?) as usize;
    let media_len = u32::from_be_bytes(payload[12..16].try_into().ok()?) as usize;
    let init_end = MESH_FMP4_SLOT_HEADER_LEN.checked_add(init_len)?;
    let media_end = init_end.checked_add(media_len)?;
    if media_end != payload.len() {
        return None;
    }
    let init = (init_len > 0).then(|| payload.slice(MESH_FMP4_SLOT_HEADER_LEN..init_end));
    Some((payload.slice(init_end..media_end), init))
}

/// The bytes of the LL-HLS part, and the init segment if the slot carries
/// one, of a cache slot as a node stores it: a canonical slot, a bare
/// envelope, an fMP4 slot, or raw bytes (TS). The same rules as the node's
/// own slot decoding (`LiveSlotPayload` in the av-mesh binary), which reads
/// a canonical slot with [`decode_canonical_live_slot`] as this does.
pub fn slot_media(payload: &Bytes, stream_id: u64) -> Option<(Bytes, Option<Bytes>)> {
    if payload.starts_with(CANONICAL_LIVE_SLOT_MAGIC) {
        let view = decode_canonical_live_slot(payload, Some(stream_id))?;
        return Some((view.media, view.init));
    }
    if payload.starts_with(&WIRE_MAGIC) {
        let object = media_object::decode(payload).ok()?;
        if object.key().stream() != stream_id.to_string() {
            return None;
        }
        let body = Bytes::copy_from_slice(object.payload());
        return match object.kind() {
            ObjectKind::Media
                if metadata_is(&object, "container", b"fmp4")
                    && metadata_is(&object, "payload-format", b"fmp4-slot-v1") =>
            {
                decode_fmp4_slot(&body)
            }
            ObjectKind::Media => Some((body, None)),
            _ => None,
        };
    }
    decode_fmp4_slot(payload).or_else(|| Some((payload.clone(), None)))
}

/// The part of a canonical media-object envelope, read in place (see
/// [`envelope_part`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopePart<'a> {
    pub stream_id: u64,
    pub object: u64,
    /// The `Published` stage timestamp in µs since the epoch, 0 without one
    /// (as [`object_published_unix_us`] reads it).
    pub published_unix_us: u64,
    /// The LL-HLS part bytes: [`slot_media`] of the slot that
    /// [`canonical_part_from_envelope`] builds.
    pub media: &'a [u8],
}

/// Reads a canonical media-object envelope in place.
struct EnvelopeReader<'a> {
    input: &'a [u8],
    at: usize,
}

impl<'a> EnvelopeReader<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(len)?;
        let bytes = self.input.get(self.at..end)?;
        self.at = end;
        Some(bytes)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn flag(&mut self) -> Option<bool> {
        match self.u8()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }

    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.take(2)?.try_into().ok()?))
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    fn string(&mut self) -> Option<&'a [u8]> {
        let len = usize::from(self.u16()?);
        self.take(len)
    }

    /// An object key: (stream, object, payload hash).
    fn key(&mut self) -> Option<(&'a [u8], u64, &'a [u8])> {
        self.string()?; // tenant
        let stream = self.string()?;
        self.string()?; // track
        self.u64()?; // epoch
        self.u64()?; // group
        let object = self.u64()?;
        self.u32()?; // version
        let hash = self.take(32)?;
        Some((stream, object, hash))
    }

    /// A clock timestamp: its time in ns.
    fn timestamp(&mut self) -> Option<i64> {
        let ns = i64::from_be_bytes(self.take(8)?.try_into().ok()?);
        self.string()?; // clock id
        if self.u8()? > 3 {
            return None; // confidence level
        }
        if self.flag()? {
            self.u64()?; // maximum error
        }
        Some(ns)
    }
}

/// The part bytes and publication time of a canonical media-object envelope,
/// read in place: no allocation, no copy, and nothing decoded beyond what
/// the part needs. The same bytes as [`slot_media`] of the slot that
/// [`canonical_part_from_envelope`] builds, and the same time as
/// [`object_published_unix_us`] of its object; the payload hash is checked,
/// as the decode checks it.
///
/// `None` for whatever is not plainly such a part (not media, not a numeric
/// stream, a Soundkit bundle, an empty payload, a malformed layout): the
/// caller then takes the full decode, which decides. The full decode also
/// checks the envelope's other canonical rules (orders, limits), which this
/// does not; the node's commit applies them to the same object.
pub fn envelope_part(envelope: &[u8]) -> Option<EnvelopePart<'_>> {
    if envelope.len() < 12 || envelope[..4] != WIRE_MAGIC {
        return None;
    }
    if envelope[4..6] != media_object::WIRE_VERSION.to_be_bytes() || envelope[6..8] != [0, 0] {
        return None;
    }
    let declared = u32::from_be_bytes(envelope[8..12].try_into().ok()?) as usize;
    if declared != envelope.len() - 12 {
        return None;
    }
    let mut reader = EnvelopeReader {
        input: envelope,
        at: 12,
    };
    if reader.u8()? != ObjectKind::Media as u8 {
        return None;
    }
    reader.flag()?; // keyframe
    if reader.u16()? != 0 {
        return None;
    }
    let (stream, object, hash) = reader.key()?;
    let stream_id = std::str::from_utf8(stream).ok()?.parse::<u64>().ok()?;
    reader.u64()?; // configuration epoch
    for _ in 0..2 {
        // The deadline and the capture timestamp.
        if reader.flag()? {
            reader.timestamp()?;
        }
    }
    let mut published_ns: Option<i64> = None;
    for _ in 0..reader.u16()? {
        let stage = reader.u8()?;
        if stage > Stage::PlayerPresented as u8 {
            return None;
        }
        let ns = reader.timestamp()?;
        if stage == Stage::Published as u8 && published_ns.is_none() {
            published_ns = Some(ns);
        }
    }
    for _ in 0..reader.u16()? {
        reader.key()?;
    }
    let (mut container, mut payload_format): (Option<&[u8]>, Option<&[u8]>) = (None, None);
    for _ in 0..reader.u16()? {
        let key = reader.string()?;
        let len = reader.u32()? as usize;
        let value = reader.take(len)?;
        match key {
            b"container" => container = Some(value),
            b"payload-format" => payload_format = Some(value),
            _ => {}
        }
    }
    let len = reader.u32()? as usize;
    let payload = reader.take(len)?;
    if reader.at != envelope.len() || payload.is_empty() {
        return None;
    }
    if payload_format == Some(SOUNDKIT_V2_BUNDLE_FORMAT) {
        return None;
    }
    if media_object::PayloadHash::digest(payload)
        .as_bytes()
        .as_slice()
        != hash
    {
        return None;
    }
    let media = if container == Some(b"fmp4".as_slice())
        && payload_format == Some(b"fmp4-slot-v1".as_slice())
    {
        if payload.len() < MESH_FMP4_SLOT_HEADER_LEN || !payload.starts_with(MESH_FMP4_SLOT_MAGIC) {
            return None;
        }
        let init_len = u32::from_be_bytes(payload[8..12].try_into().ok()?) as usize;
        let media_len = u32::from_be_bytes(payload[12..16].try_into().ok()?) as usize;
        let start = MESH_FMP4_SLOT_HEADER_LEN.checked_add(init_len)?;
        if start.checked_add(media_len)? != payload.len() {
            return None;
        }
        &payload[start..]
    } else {
        payload
    };
    let published_unix_us = published_ns
        .and_then(|ns| u64::try_from(ns).ok())
        .map_or(0, |ns| ns / 1_000);
    Some(EnvelopePart {
        stream_id,
        object,
        published_unix_us,
        media,
    })
}

/// When the contributor published the object, in µs since the epoch: its
/// `Published` stage timestamp.
pub fn object_published_unix_us(object: &MediaObject) -> Option<u64> {
    object
        .stage_timestamps()
        .iter()
        .find(|timestamp| timestamp.stage() == Stage::Published)
        .and_then(|timestamp| u64::try_from(timestamp.timestamp().unix_time_ns()).ok())
        .map(|ns| ns / 1_000)
        .filter(|us| *us > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use media_object::ObjectKey;

    fn object(metadata: &[(&str, &[u8])], payload: &[u8], kind: ObjectKind) -> MediaObject {
        let key = ObjectKey::for_payload("default", "4001", "main", 3, 0, 9, 1, payload).unwrap();
        let mut builder = MediaObject::builder(key, kind, payload.to_vec());
        for (name, value) in metadata {
            builder = builder.with_metadata(*name, value.to_vec());
        }
        builder.build().unwrap()
    }

    fn envelope(object: &MediaObject) -> Bytes {
        Bytes::from(media_object::encode(object).unwrap())
    }

    #[test]
    fn a_slot_holds_its_envelope_and_reads_back_its_part() {
        let mut fmp4 = MESH_FMP4_SLOT_MAGIC.to_vec();
        fmp4.extend_from_slice(&4_u32.to_be_bytes());
        fmp4.extend_from_slice(&5_u32.to_be_bytes());
        fmp4.extend_from_slice(b"initmedia");
        for (metadata, payload, kind, media, init) in [
            (
                vec![
                    ("container", b"opaque".as_slice()),
                    ("payload-format", b"opaque-part-v1".as_slice()),
                ],
                b"pcm-bytes".to_vec(),
                LiveMediaKind::Opaque,
                b"pcm-bytes".to_vec(),
                None,
            ),
            (
                vec![
                    ("container", b"fmp4".as_slice()),
                    ("payload-format", b"fmp4-slot-v1".as_slice()),
                ],
                fmp4.clone(),
                LiveMediaKind::Fmp4,
                b"media".to_vec(),
                Some(Bytes::from_static(b"init")),
            ),
            (
                vec![],
                b"\x47ts".to_vec(),
                LiveMediaKind::Ts,
                b"\x47ts".to_vec(),
                None,
            ),
        ] {
            let object = object(&metadata, &payload, ObjectKind::Media);
            assert_eq!(live_media_kind(&object), kind);
            let envelope = envelope(&object);
            let part = canonical_part_from_envelope(envelope.clone()).unwrap();
            assert_eq!(part.stream_id, 4001);
            assert_eq!(
                &part.slot[CANONICAL_LIVE_SLOT_HEADER_LEN..],
                envelope.as_ref()
            );
            let view = decode_canonical_live_slot(&part.slot, Some(4001)).unwrap();
            assert_eq!(view.kind, kind);
            assert_eq!(view.media.as_ref(), media.as_slice());
            assert_eq!(view.init, init);
            assert_eq!(slot_media(&part.slot, 4001), Some((view.media, init)));
            assert!(decode_canonical_live_slot(&part.slot, Some(4002)).is_none());
        }
    }

    #[test]
    fn what_the_commit_does_not_store_canonically_is_refused() {
        let bundle = object(
            &[("payload-format", SOUNDKIT_V2_BUNDLE_FORMAT)],
            b"NTV2PART....",
            ObjectKind::Media,
        );
        assert_eq!(
            canonical_part_from_envelope(envelope(&bundle)).unwrap_err(),
            SlotError("a Soundkit bundle is reboxed at the commit and stays off the fast lane")
        );
        let init = object(
            &[("container", b"fmp4")],
            b"ftyp",
            ObjectKind::Initialization,
        );
        assert!(canonical_part_from_envelope(envelope(&init)).is_err());
        assert!(canonical_part_from_envelope(Bytes::from_static(b"not an envelope")).is_err());
        // A corrupt envelope fails its payload hash.
        let good = object(&[], b"payload", ObjectKind::Media);
        let mut bad = envelope(&good).to_vec();
        let last = bad.len() - 1;
        bad[last] ^= 1;
        assert!(canonical_part_from_envelope(Bytes::from(bad)).is_err());
    }

    /// The slow path that the in-place reader stands for: the decode, the
    /// slot, its part bytes and the publication time.
    fn decoded_part(envelope: &[u8]) -> Option<(u64, u64, u64, Bytes)> {
        let part = canonical_part_from_envelope(Bytes::copy_from_slice(envelope)).ok()?;
        let (media, _) = slot_media(&part.slot, part.stream_id)?;
        Some((
            part.stream_id,
            part.object.key().object(),
            object_published_unix_us(&part.object).unwrap_or(0),
            media,
        ))
    }

    #[test]
    fn the_in_place_reader_finds_the_part_that_the_decode_finds() {
        use media_object::{ClockConfidence, ClockTimestamp, StageTimestamp};
        let stamp = |ns: i64, confidence: ClockConfidence| {
            ClockTimestamp::new(ns, "system:realtime:av-contrib", confidence).unwrap()
        };
        let mut fmp4 = MESH_FMP4_SLOT_MAGIC.to_vec();
        fmp4.extend_from_slice(&4_u32.to_be_bytes());
        fmp4.extend_from_slice(&5_u32.to_be_bytes());
        fmp4.extend_from_slice(b"initmedia");
        let mut fmp4_no_init = MESH_FMP4_SLOT_MAGIC.to_vec();
        fmp4_no_init.extend_from_slice(&0_u32.to_be_bytes());
        fmp4_no_init.extend_from_slice(&5_u32.to_be_bytes());
        fmp4_no_init.extend_from_slice(b"media");
        let opaque: [(&str, &[u8]); 2] = [
            ("container", b"opaque"),
            ("payload-format", b"opaque-part-v1"),
        ];
        let fmp4_meta: [(&str, &[u8]); 2] =
            [("container", b"fmp4"), ("payload-format", b"fmp4-slot-v1")];
        let cases: Vec<(Vec<(&str, &[u8])>, Vec<u8>, bool, bool)> = vec![
            // (metadata, payload, stage stamps and a deadline, a dependency)
            (
                opaque.to_vec(),
                (0..304).map(|i| i as u8).collect(),
                true,
                false,
            ),
            (opaque.to_vec(), b"opus".to_vec(), false, false),
            (fmp4_meta.to_vec(), fmp4.clone(), true, true),
            (fmp4_meta.to_vec(), fmp4_no_init, true, false),
            // An fMP4 container without the slot format: the whole payload.
            (vec![("container", b"fmp4".as_slice())], fmp4, true, false),
            (
                vec![("content-type", b"video/mp2t".as_slice())],
                b"ts".to_vec(),
                true,
                true,
            ),
            (Vec::new(), b"bare".to_vec(), false, false),
        ];
        for (index, (metadata, payload, stamped, dependent)) in cases.into_iter().enumerate() {
            let key = ObjectKey::for_payload(
                "default",
                "4001",
                "main",
                3,
                1,
                9 + index as u64,
                1,
                &payload,
            )
            .unwrap();
            let mut builder = MediaObject::builder(key, ObjectKind::Media, payload.clone());
            if stamped {
                builder = builder
                    .with_deadline(stamp(
                        1_700_000_005_000_000_000,
                        ClockConfidence::estimated(1_000),
                    ))
                    .with_capture_timestamp(stamp(
                        1_699_999_999_000_000_000,
                        ClockConfidence::unknown(),
                    ))
                    .with_stage_timestamp(StageTimestamp::new(
                        Stage::Packaged,
                        stamp(
                            1_700_000_000_000_000_000,
                            ClockConfidence::estimated(1_000_000),
                        ),
                    ))
                    .with_stage_timestamp(StageTimestamp::new(
                        Stage::Published,
                        stamp(
                            1_700_000_000_000_123_456,
                            ClockConfidence::estimated(1_000_000),
                        ),
                    ));
            }
            if dependent {
                builder = builder.with_dependency(
                    ObjectKey::for_payload("default", "4001", "main-init", 3, 0, 0, 1, b"init")
                        .unwrap(),
                );
            }
            for (name, value) in &metadata {
                builder = builder.with_metadata(*name, value.to_vec());
            }
            let object = builder.build().unwrap();
            let wire = envelope(&object);
            let fast =
                envelope_part(&wire).unwrap_or_else(|| panic!("case {index}: read in place"));
            let (stream_id, sequence, published, media) =
                decoded_part(&wire).unwrap_or_else(|| panic!("case {index}: decoded"));
            assert_eq!(fast.stream_id, stream_id, "case {index}");
            assert_eq!(fast.object, sequence, "case {index}");
            assert_eq!(fast.published_unix_us, published, "case {index}");
            assert_eq!(fast.media, media.as_ref(), "case {index}");
            if stamped {
                assert_eq!(fast.published_unix_us, 1_700_000_000_000_123);
            }
        }
    }

    #[test]
    fn the_in_place_reader_refuses_what_the_decode_refuses() {
        let opaque: [(&str, &[u8]); 2] = [
            ("container", b"opaque"),
            ("payload-format", b"opaque-part-v1"),
        ];
        let good = object(&opaque, b"payload", ObjectKind::Media);
        let wire = envelope(&good);
        assert!(envelope_part(&wire).is_some());
        // A corrupt payload fails its hash; a cut envelope its length.
        let mut corrupt = wire.to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(envelope_part(&corrupt).is_none());
        assert!(decoded_part(&corrupt).is_none());
        assert!(envelope_part(&wire[..wire.len() - 1]).is_none());
        assert!(envelope_part(b"not an envelope").is_none());
        // Not media; a Soundkit bundle; a stream that is not numeric.
        let init = object(&[], b"ftyp", ObjectKind::Initialization);
        assert!(envelope_part(&envelope(&init)).is_none());
        let bundle = object(
            &[("payload-format", SOUNDKIT_V2_BUNDLE_FORMAT)],
            b"bundle",
            ObjectKind::Media,
        );
        assert!(envelope_part(&envelope(&bundle)).is_none());
        assert!(decoded_part(&envelope(&bundle)).is_none());
        let key =
            ObjectKey::for_payload("default", "studio", "main", 3, 0, 9, 1, b"payload").unwrap();
        let named = MediaObject::builder(key, ObjectKind::Media, b"payload".to_vec())
            .build()
            .unwrap();
        assert!(envelope_part(&envelope(&named)).is_none());
        assert!(decoded_part(&envelope(&named)).is_none());
        // A slot whose lengths do not match its payload.
        let mut slot = MESH_FMP4_SLOT_MAGIC.to_vec();
        slot.extend_from_slice(&4_u32.to_be_bytes());
        slot.extend_from_slice(&9_u32.to_be_bytes());
        slot.extend_from_slice(b"initmedia");
        let fmp4: [(&str, &[u8]); 2] =
            [("container", b"fmp4"), ("payload-format", b"fmp4-slot-v1")];
        let bad_slot = object(&fmp4, &slot, ObjectKind::Media);
        assert!(envelope_part(&envelope(&bad_slot)).is_none());
        assert!(decoded_part(&envelope(&bad_slot)).is_none());
    }

    /// The module stays free of anything that does not build for wasm32.
    #[test]
    fn the_module_has_no_runtime_or_socket_dependency() {
        let source = include_str!("canonical_slot.rs");
        let code = source.split("#[cfg(test)]").next().unwrap();
        for forbidden in [
            "tokio::",
            "use tokio",
            "std::net",
            "std::time",
            "std::thread",
            "libc::",
        ] {
            assert!(!code.contains(forbidden), "canonical_slot uses {forbidden}");
        }
    }
}
