//! RaptorQ FEC UDP from an edge to a client: the second way a client
//! receives a stream, beside LL-HLS over HTTP.
//!
//! A client is assigned an edge exactly as an LL-HLS reader is (the
//! assignment API or the Multivariant Playlist of an entry point). It reads
//! the media playlist of its stream from that edge, which names the stream
//! and its parts, and then subscribes to the stream at the edge's client UDP
//! port. The edge sends every part of the stream that its cache holds, the
//! same bytes that its LL-HLS part responses carry, as one RaptorQ object:
//! the source symbols of the part and enough repair symbols to recover the
//! loss that the client reported.
//!
//! The protocol has two kinds of datagram.
//!
//! - Control datagrams are one line of ASCII text that starts with `NTU/1`:
//!
//!   | from   | text                                                          |
//!   | ------ | ------------------------------------------------------------- |
//!   | client | `SUB <stream> <from part or -> <repair ‰> <min repair> <loss ppm> <token or -> [F1] [C<copies>.<window>.<repair>]` |
//!   | client | `UNSUB <stream> <token>`                                      |
//!   | client | `NACK <stream> <token> <part> [<part> ...]`                   |
//!   | edge   | `TOKEN <stream> <token>`                                      |
//!   | edge   | `OK <stream> <next part> [C]`                                 |
//!   | edge   | `WAIT <stream> <next part>`                                   |
//!   | edge   | `REFUSED <stream> <reason>`                                   |
//!   | edge   | `MOVE <stream> <reason>`                                      |
//!
//!   An edge sends media only to an address that has shown that it receives
//!   at that address: the first `SUB` of a client is answered with a `TOKEN`,
//!   shorter than the request, and the client repeats its `SUB` with it. A
//!   forged source address therefore cannot turn an edge into a reflector.
//!   A client repeats `SUB` at least every [`REFRESH`], and the edge answers
//!   each with `OK`; a subscription that is not refreshed for
//!   [`SUBSCRIPTION_TTL`] ends. While a stream has no new part the edge sends
//!   `WAIT` at least every [`KEEPALIVE`], so that a client can tell a quiet
//!   stream from a dead edge. `MOVE` tells the client to go to the next cache
//!   of its assignment (the edge drains, or has lost its parents); `REFUSED`
//!   that this edge cannot take it. A `SUB` that ends in the word `F1` asks
//!   for the fast lane (see below); an edge without it, or a client that
//!   does not ask, uses the cache-fed lane alone.
//! - Media datagrams are RaptorQ datagrams of `raptorq-datagram-fec` (they
//!   start with its magic). Each decodes to one [`PartObject`]: a part, the
//!   initialisation segment of an fMP4 stream, or the notice that the edge
//!   skipped a part that it never received (a gap, as LL-HLS has
//!   `EXT-X-GAP`). The block id is the part number, so a client knows which
//!   part a lost block was. The object carries a CRC-32 of its payload.
//!
//! The fast lane: an edge fed by its forwarders' RelaySession symbols sends
//! each one on to its `F1` clients as it arrives, the contributor's own
//! RaptorQ datagram with only its block id rewritten ([`fast_block_id`], the
//! fourth quarter of the block id space) and its CRC recomputed. Such a
//! block decodes to the canonical media-object envelope; the client builds
//! the part from it with the edge's own slot functions
//! (`crate::canonical_slot`), so its bytes are the edge's LL-HLS part. The
//! init segment, parts older than the fast lane's first and gaps come on the
//! cache-fed lane as before.
//!
//! The compact lane (`crate::client_compact`): a `SUB` with a `C` word asks
//! for it, and an edge that has it answers `OK ... C`. A part that fits one
//! datagram then comes as one compact datagram, not as a RaptorQ block,
//! with the copies and the repair across parts that the word asks for, and
//! the client asks for a part it misses with `NACK`. Parts that do not fit
//! (4K video), and the init segment, keep their RaptorQ blocks, with the
//! repair of the `SUB`'s policy.
//!
//! One client socket carries one stream: block ids are per stream. A client
//! that moves to another edge starts a new decoder ([`ObjectReceiver::new_flow`]):
//! two edges encode one part into different symbols.

pub use crate::client_compact::CompactPlan;
use crate::client_compact::{self as compact, CompactReceiver};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use raptorq_datagram_fec::{DatagramFecDecoder, DatagramFecEncoder, DatagramFecHeader};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::time::{Duration, Instant};

pub const CONTROL_PREFIX: &str = "NTU/1 ";
/// A client repeats its subscription this often.
pub const REFRESH: Duration = Duration::from_secs(2);
/// A subscription that is not repeated for this long ends.
pub const SUBSCRIPTION_TTL: Duration = Duration::from_secs(10);
/// An edge sends something to every subscriber at least this often.
pub const KEEPALIVE: Duration = Duration::from_millis(300);
/// Largest symbol of the media datagrams: a datagram stays under 1,280 bytes
/// with its headers, the smallest IPv6 path MTU. A small part is sent in
/// symbols of its own size.
pub const DEFAULT_SYMBOL_SIZE: u16 = 1_200;
/// Repair of a RaptorQ block on a path with no measured loss, for a client
/// that the edge serves the compact lane: one symbol (the loss that a
/// client measures adds to it, see [`RepairPolicy::for_loss`]). Its parts
/// that fit one datagram go compact, and NACK backs up the rest.
pub const DEFAULT_REPAIR_PERMILLE: u32 = 0;
pub const DEFAULT_MIN_REPAIR_SYMBOLS: u32 = 1;
/// The repair of a client on the RaptorQ lane alone (it did not ask for the
/// compact lane, or its edge does not serve it): a quarter of the source
/// symbols and at least three, as before wave 5. That lane has no NACK, so
/// it keeps the margin against bursts that it had.
pub const RAPTORQ_LANE_REPAIR_PERMILLE: u32 = 250;
pub const RAPTORQ_LANE_MIN_REPAIR_SYMBOLS: u32 = 3;
/// The most parts of one `NACK`.
pub const MAX_NACK_PARTS: usize = 64;
/// The most repair an edge sends for one subscriber.
pub const MAX_REPAIR_PERMILLE: u32 = 1_000;
pub const MAX_MIN_REPAIR_SYMBOLS: u32 = 16;
pub const MAX_LOSS_PPM: u32 = 300_000;
/// Part loss that the repair aims below.
pub const TARGET_PART_LOSS: f64 = 1e-7;
/// An edge answers a request without a token only if it is at least this
/// long: longer than the answer.
pub const MIN_FIRST_REQUEST_BYTES: usize = 64;

const OBJECT_MAGIC: &[u8; 4] = b"NTO2";
const OBJECT_HEADER_LEN: usize = 4 + 1 + 1 + 4 + 4 + 8 + 8 + 8 + 8;
const MIN_SYMBOL_SIZE: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RepairPolicy {
    /// Repair symbols per thousand source symbols, at least.
    pub permille: u32,
    /// Repair symbols of every part, at least.
    pub min_symbols: u32,
    /// Datagram loss of the client's path in parts per million, as the
    /// client plans for it. Each part gets the repair that keeps its loss
    /// below [`TARGET_PART_LOSS`] at this loss.
    pub loss_ppm: u32,
}

impl Default for RepairPolicy {
    fn default() -> Self {
        Self {
            permille: DEFAULT_REPAIR_PERMILLE,
            min_symbols: DEFAULT_MIN_REPAIR_SYMBOLS,
            loss_ppm: 0,
        }
    }
}

/// Smallest `r` such that a part of `k` source symbols sent with `r` repair
/// symbols is lost with a probability below [`TARGET_PART_LOSS`] when each
/// datagram is lost with probability `p`. A RaptorQ block of more than one
/// symbol is counted as decoded only with one symbol more than `k`.
pub fn repair_for_loss(k: u32, p: f64) -> u32 {
    if !(p > 0.0) {
        return 0;
    }
    let p = p.min(0.5);
    let margin = u32::from(k > 1);
    let limit = k.saturating_mul(3).saturating_add(64);
    let (ln_p, ln_q) = (p.ln(), (1.0 - p).ln());
    let mut r = margin;
    while r < limit {
        let n = k + r;
        let allowed = r - margin; // datagrams that may be lost
                                  // P(lost <= allowed), with the binomial terms in log space.
        let mut ln_term = f64::from(n) * ln_q;
        let mut delivered = ln_term.exp();
        for j in 0..allowed {
            ln_term += (f64::from(n - j) / f64::from(j + 1)).ln() + ln_p - ln_q;
            delivered += ln_term.exp();
        }
        if 1.0 - delivered <= TARGET_PART_LOSS {
            return r;
        }
        r += 1;
    }
    limit
}

impl RepairPolicy {
    /// The base repair of a client: the compact lane's when the edge serves
    /// it the compact lane, else the RaptorQ lane's. `permille` and
    /// `min_symbols` given by the user stand either way.
    pub fn base_for_lane(compact: bool, permille: Option<u32>, min_symbols: Option<u32>) -> Self {
        let (default_permille, default_min) = if compact {
            (DEFAULT_REPAIR_PERMILLE, DEFAULT_MIN_REPAIR_SYMBOLS)
        } else {
            (
                RAPTORQ_LANE_REPAIR_PERMILLE,
                RAPTORQ_LANE_MIN_REPAIR_SYMBOLS,
            )
        };
        Self {
            permille: permille.unwrap_or(default_permille),
            min_symbols: min_symbols.unwrap_or(default_min),
            loss_ppm: 0,
        }
        .bounded()
    }

    pub fn bounded(self) -> Self {
        Self {
            permille: self.permille.min(MAX_REPAIR_PERMILLE),
            min_symbols: self.min_symbols.min(MAX_MIN_REPAIR_SYMBOLS),
            loss_ppm: self.loss_ppm.min(MAX_LOSS_PPM),
        }
    }

    /// The policy of a client that has measured `loss` on its path, never
    /// below `base`. The estimate is taken as half as much again, plus 0.2 %.
    pub fn for_loss(loss: f64, base: Self) -> Self {
        if !(loss > 0.0) {
            return base.bounded();
        }
        // Planned in steps, so that clients on similar paths share one
        // encoding of each part at the edge.
        const STEPS: [f64; 10] = [0.005, 0.01, 0.02, 0.03, 0.05, 0.08, 0.12, 0.18, 0.25, 0.3];
        let wanted = (loss * 1.5 + 0.002).min(0.3);
        let planned = STEPS
            .iter()
            .copied()
            .find(|step| *step >= wanted)
            .unwrap_or(0.3);
        Self {
            loss_ppm: ((planned * 1e6).ceil() as u32).max(base.loss_ppm),
            ..base
        }
        .bounded()
    }

    pub fn repair_symbols(self, source_symbols: u32) -> u32 {
        let proportional = (u64::from(source_symbols) * u64::from(self.permille)).div_ceil(1_000);
        let for_loss = repair_for_loss(source_symbols, f64::from(self.loss_ppm) / 1e6);
        (proportional as u32).max(self.min_symbols).max(for_loss)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    Subscribe {
        stream_id: u64,
        from_part: Option<u64>,
        repair: RepairPolicy,
        token: Option<u64>,
        /// The client takes the fast lane (`F1`).
        fast_lane: bool,
        /// The client takes the compact lane, with this plan (`C...`).
        compact: Option<CompactPlan>,
    },
    Unsubscribe {
        stream_id: u64,
        token: u64,
    },
    /// The client asks for parts again (compact lane).
    Nack {
        stream_id: u64,
        token: u64,
        parts: Vec<u64>,
    },
    Token {
        stream_id: u64,
        token: u64,
    },
    Subscribed {
        stream_id: u64,
        next_part: u64,
        /// The edge sends the compact lane (`C`).
        compact: bool,
    },
    Waiting {
        stream_id: u64,
        next_part: u64,
    },
    Refused {
        stream_id: u64,
        reason: String,
    },
    Move {
        stream_id: u64,
        reason: String,
    },
}

impl Control {
    pub fn stream_id(&self) -> u64 {
        match self {
            Self::Subscribe { stream_id, .. }
            | Self::Unsubscribe { stream_id, .. }
            | Self::Nack { stream_id, .. }
            | Self::Token { stream_id, .. }
            | Self::Subscribed { stream_id, .. }
            | Self::Waiting { stream_id, .. }
            | Self::Refused { stream_id, .. }
            | Self::Move { stream_id, .. } => *stream_id,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let text = match self {
            Self::Subscribe {
                stream_id,
                from_part,
                repair,
                token,
                fast_lane,
                compact,
            } => format!(
                "{CONTROL_PREFIX}SUB {stream_id} {} {} {} {} {}{}{}",
                from_part.map_or_else(|| "-".to_owned(), |part| part.to_string()),
                repair.permille,
                repair.min_symbols,
                repair.loss_ppm,
                // A first request carries padding, so that the edge's answer
                // (a TOKEN) is never larger than the request.
                token.map_or_else(
                    || format!("- {}", ".".repeat(48)),
                    |token| format!("{token:016x}")
                ),
                if *fast_lane { " F1" } else { "" },
                compact.map_or_else(String::new, |plan| format!(" {}", plan.word())),
            ),
            Self::Unsubscribe { stream_id, token } => {
                format!("{CONTROL_PREFIX}UNSUB {stream_id} {token:016x}")
            }
            Self::Nack {
                stream_id,
                token,
                parts,
            } => {
                let mut text = format!("{CONTROL_PREFIX}NACK {stream_id} {token:016x}");
                for part in parts.iter().take(MAX_NACK_PARTS) {
                    text.push(' ');
                    text.push_str(&part.to_string());
                }
                text
            }
            Self::Token { stream_id, token } => {
                format!("{CONTROL_PREFIX}TOKEN {stream_id} {token:016x}")
            }
            Self::Subscribed {
                stream_id,
                next_part,
                compact,
            } => format!(
                "{CONTROL_PREFIX}OK {stream_id} {next_part}{}",
                if *compact { " C" } else { "" }
            ),
            Self::Waiting {
                stream_id,
                next_part,
            } => format!("{CONTROL_PREFIX}WAIT {stream_id} {next_part}"),
            Self::Refused { stream_id, reason } => {
                format!("{CONTROL_PREFIX}REFUSED {stream_id} {}", word(reason))
            }
            Self::Move { stream_id, reason } => {
                format!("{CONTROL_PREFIX}MOVE {stream_id} {}", word(reason))
            }
        };
        text.into_bytes()
    }

    /// `None` for a datagram that is not a control datagram of this protocol.
    pub fn decode(datagram: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(datagram).ok()?;
        let rest = text.strip_prefix(CONTROL_PREFIX)?;
        let mut words = rest.split_ascii_whitespace();
        let verb = words.next()?;
        let stream_id = words.next()?.parse().ok()?;
        let token = |word: &str| u64::from_str_radix(word, 16).ok();
        let control = match verb {
            "SUB" => {
                let from_part = match words.next()? {
                    "-" => None,
                    part => Some(part.parse().ok()?),
                };
                let permille = words.next()?.parse().ok()?;
                let min_symbols = words.next()?.parse().ok()?;
                let loss_ppm = words.next()?.parse().ok()?;
                let token = match words.next()? {
                    "-" => {
                        // The padding of a first request.
                        if words
                            .clone()
                            .next()
                            .is_some_and(|pad| pad.bytes().all(|b| b == b'.'))
                        {
                            words.next();
                        }
                        None
                    }
                    word => Some(token(word)?),
                };
                // The optional last words of a client that takes the fast
                // lane (`F1`) and the compact lane (`C...`), in this order;
                // a request without them parses as before.
                let fast_lane = words.clone().next() == Some("F1");
                if fast_lane {
                    words.next();
                }
                let compact = words.clone().next().and_then(CompactPlan::parse_word);
                if compact.is_some() {
                    words.next();
                }
                Self::Subscribe {
                    stream_id,
                    from_part,
                    repair: RepairPolicy {
                        permille,
                        min_symbols,
                        loss_ppm,
                    },
                    token,
                    fast_lane,
                    compact,
                }
            }
            "UNSUB" => Self::Unsubscribe {
                stream_id,
                token: token(words.next()?)?,
            },
            "NACK" => {
                let token = token(words.next()?)?;
                let mut parts = Vec::new();
                for word in words.by_ref() {
                    if parts.len() == MAX_NACK_PARTS {
                        return None;
                    }
                    parts.push(word.parse().ok()?);
                }
                if parts.is_empty() {
                    return None;
                }
                Self::Nack {
                    stream_id,
                    token,
                    parts,
                }
            }
            "TOKEN" => Self::Token {
                stream_id,
                token: token(words.next()?)?,
            },
            "OK" => {
                let next_part = words.next()?.parse().ok()?;
                let compact = words.clone().next() == Some("C");
                if compact {
                    words.next();
                }
                Self::Subscribed {
                    stream_id,
                    next_part,
                    compact,
                }
            }
            "WAIT" => Self::Waiting {
                stream_id,
                next_part: words.next()?.parse().ok()?,
            },
            "REFUSED" => Self::Refused {
                stream_id,
                reason: words.next().unwrap_or("refused").to_owned(),
            },
            "MOVE" => Self::Move {
                stream_id,
                reason: words.next().unwrap_or("move").to_owned(),
            },
            _ => return None,
        };
        if words.next().is_some() {
            return None;
        }
        Some(control)
    }
}

fn word(reason: &str) -> String {
    let word: String = reason
        .chars()
        .map(|c| if c.is_ascii_graphic() { c } else { '-' })
        .collect();
    if word.is_empty() {
        "-".to_owned()
    } else {
        word
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Part,
    Init,
    /// The edge never received this part and skipped it.
    Gap,
}

impl ObjectKind {
    fn code(self) -> u8 {
        match self {
            Self::Part => 0,
            Self::Init => 1,
            Self::Gap => 2,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Part),
            1 => Some(Self::Init),
            2 => Some(Self::Gap),
            _ => None,
        }
    }
}

/// What one RaptorQ object carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartObject {
    pub kind: ObjectKind,
    /// Datagrams that the edge sent for this object: source and repair. A
    /// client measures its loss from it. Set by [`encode_object`].
    pub datagrams: u32,
    pub stream_id: u64,
    /// The part number, as in the playlist (`part<n>`). For an init object,
    /// the part that it was sent before.
    pub part: u64,
    /// When the part became available at the edge, in µs since the epoch,
    /// or 0.
    pub available_unix_us: u64,
    /// When the contributor published it, in µs since the epoch, or 0.
    pub published_unix_us: u64,
    /// The bytes of the LL-HLS part (or init segment), unchanged.
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectError(&'static str);

impl fmt::Display for ObjectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ObjectError {}

impl PartObject {
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(OBJECT_HEADER_LEN + self.payload.len());
        out.put_slice(OBJECT_MAGIC);
        out.put_u8(self.kind.code());
        out.put_u8(0);
        out.put_u32(self.datagrams);
        out.put_u32(crc32fast::hash(&self.payload));
        out.put_u64(self.stream_id);
        out.put_u64(self.part);
        out.put_u64(self.available_unix_us);
        out.put_u64(self.published_unix_us);
        out.put_slice(&self.payload);
        out.freeze()
    }

    pub fn decode(object: Bytes) -> Result<Self, ObjectError> {
        if object.len() < OBJECT_HEADER_LEN || &object[..4] != OBJECT_MAGIC {
            return Err(ObjectError("not a Needletail part object"));
        }
        let mut header = &object[4..OBJECT_HEADER_LEN];
        let kind = ObjectKind::from_code(header.get_u8()).ok_or(ObjectError("unknown kind"))?;
        let _flags = header.get_u8();
        let datagrams = header.get_u32();
        let crc = header.get_u32();
        let stream_id = header.get_u64();
        let part = header.get_u64();
        let available_unix_us = header.get_u64();
        let published_unix_us = header.get_u64();
        let payload = object.slice(OBJECT_HEADER_LEN..);
        if crc32fast::hash(&payload) != crc {
            return Err(ObjectError("payload checksum mismatch"));
        }
        Ok(Self {
            kind,
            datagrams,
            stream_id,
            part,
            available_unix_us,
            published_unix_us,
            payload,
        })
    }
}

/// Block id of an object that the edge encodes itself (a part sent from its
/// cache, a gap, an init segment): the part number in its own quarter of the
/// block id space, so that it never shares a block id with the cut-through
/// block of the same part ([`cut_through_block_id`]).
pub fn block_id(kind: ObjectKind, part: u64) -> u32 {
    let low = (part & 0x3fff_ffff) as u32;
    match kind {
        ObjectKind::Init => low | 0x8000_0000,
        ObjectKind::Part | ObjectKind::Gap => low | 0x4000_0000,
    }
}

/// Block id of the cut-through block of a part: the cache-mesh block that
/// carried the part to the edge, forwarded as it arrives.
pub fn cut_through_block_id(part: u64) -> u32 {
    (part & 0x3fff_ffff) as u32
}

/// The bytes of the LL-HLS part, and the init segment if the slot carries
/// one, of a cache slot as the edge stores it: one function with the edge's
/// own slot decoding (see `crate::canonical_slot`).
pub use crate::canonical_slot::slot_media;

/// Block id of a fast-lane block: the contributor's own RaptorQ block of a
/// part, forwarded by the edge as its symbols arrive from the forwarders
/// (RLS1 stripped, block id rewritten, nothing else changed). The fourth
/// quarter of the block id space: init and codec configuration never ride
/// it (they keep the second, on the cache-fed lane), so a completed init
/// block can never make the decoder forget an in-flight media block.
pub fn fast_block_id(part: u64) -> u32 {
    0xC000_0000 | (part & 0x3fff_ffff) as u32
}

/// The quarter of the block id space that a block id is in.
pub const fn block_space(block_id: u32) -> usize {
    (block_id >> 30) as usize
}

/// The quarter of the fast lane.
pub const FAST_SPACE: usize = 3;

/// Rewrap an RQD2 datagram (the one an RLS1 symbol carries) for the fast
/// lane: copied into `out`, its block id replaced by [`fast_block_id`] of
/// `part` and its CRC (if it has one) recomputed. K, T, the transfer length,
/// the packet sequence and the payload id are unchanged, so a client decodes
/// the contributor's own symbols into the canonical envelope. Returns the
/// datagram's length, or `None` when `fec` is not an RQD2 datagram or does
/// not fit `out`.
pub fn rewrap_fast(fec: &[u8], part: u64, out: &mut [u8]) -> Option<usize> {
    const HEADER: usize = raptorq_datagram_fec::HEADER_LEN;
    if fec.len() < HEADER
        || out.len() < fec.len()
        || fec[..4] != raptorq_datagram_fec::DATAGRAM_MAGIC
    {
        return None;
    }
    let out = &mut out[..fec.len()];
    out.copy_from_slice(fec);
    out[8..12].copy_from_slice(&fast_block_id(part).to_le_bytes());
    if out[7] & raptorq_datagram_fec::DATAGRAM_FLAG_PACKET_CRC32 != 0 {
        let crc = raptorq_datagram_fec::packet_crc32(&out[..HEADER - 4], &out[HEADER..]);
        out[HEADER - 4..HEADER].copy_from_slice(&crc.to_le_bytes());
    }
    Some(fec.len())
}

/// [`rewrap_fast`] into a new buffer.
pub fn rewrap_fast_vec(fec: &[u8], part: u64) -> Option<Vec<u8>> {
    let mut out = vec![0_u8; fec.len()];
    rewrap_fast(fec, part, &mut out)?;
    Some(out)
}

/// When the contributor published the part of a cache slot, in µs since
/// the epoch: the `Published` stage timestamp of its media object, if the
/// slot carries one.
pub fn slot_published_unix_us(payload: &Bytes) -> Option<u64> {
    let envelope = if payload.starts_with(b"AVMOBJS1") {
        payload.get(40..)?
    } else if payload.starts_with(&media_object::WIRE_MAGIC) {
        payload.as_ref()
    } else {
        return None;
    };
    let object = media_object::decode(envelope).ok()?;
    object
        .stage_timestamps()
        .iter()
        .find(|timestamp| timestamp.stage() == media_object::Stage::Published)
        .and_then(|timestamp| u64::try_from(timestamp.timestamp().unix_time_ns()).ok())
        .map(|ns| ns / 1_000)
        .filter(|us| *us > 0)
}

/// The part of a cache-mesh chunk frame, as the client of a cut-through
/// block decodes it.
pub fn cut_through_part(frame: &[u8]) -> Option<PartObject> {
    cut_through_object(frame)
}

/// The object of a cut-through block: the cache-mesh chunk frame of a part.
fn cut_through_object(frame: &[u8]) -> Option<PartObject> {
    let (stream_id, slot, payload) = playlists::mesh::chunk_frame_payload(frame)?;
    let (media, _init) = slot_media(&payload, stream_id)?;
    Some(PartObject {
        kind: ObjectKind::Part,
        datagrams: 0,
        stream_id,
        part: slot,
        available_unix_us: 0,
        published_unix_us: slot_published_unix_us(&payload).unwrap_or(0),
        payload: media,
    })
}

/// Source symbols and symbol size of an object of `length` bytes: as few
/// symbols as `max_symbol_size` allows, each just large enough. A 5 ms audio
/// part of 120 bytes is one symbol of 120 bytes, not of 1,200.
pub fn geometry(length: usize, max_symbol_size: u16) -> (u32, u16) {
    let max = usize::from(max_symbol_size).max(MIN_SYMBOL_SIZE);
    let sources = length.div_ceil(max).max(1);
    let size = length.div_ceil(sources).max(MIN_SYMBOL_SIZE);
    (sources as u32, size as u16)
}

/// Encode one object into the datagrams that carry it.
pub fn encode_object(
    object: &PartObject,
    max_symbol_size: u16,
    repair: RepairPolicy,
) -> Result<Vec<Vec<u8>>, raptorq_datagram_fec::DatagramFecError> {
    let length = OBJECT_HEADER_LEN + object.payload.len();
    let (sources, symbol_size) = geometry(length, max_symbol_size);
    let repair_symbols = repair.repair_symbols(sources);
    let bytes = PartObject {
        datagrams: sources.saturating_add(repair_symbols),
        ..object.clone()
    }
    .encode();
    let mut encoder = DatagramFecEncoder::new()
        .with_symbol_size(symbol_size)
        .with_source_symbols(u16::try_from(sources).unwrap_or(u16::MAX))
        .with_initial_block_id(block_id(object.kind, object.part));
    encoder.encode_object_with_repair_symbols(&bytes, repair_symbols)
}

/// Datagrams of one block that a client has seen.
#[derive(Debug, Clone, Copy)]
struct BlockCount {
    received: u32,
    source_symbols: u32,
    /// Known once the block is decoded; 0 for a cut-through block.
    sent: Option<u32>,
    decoded: bool,
    first_seen: Instant,
}

/// The receiving half of a client: datagrams in, objects out.
#[derive(Debug, Default)]
pub struct ObjectReceiver {
    /// One decoder for each quarter of the block id space (cut-through,
    /// edge-encoded parts, init, fast lane): a decoder forgets blocks far
    /// below the last one it decoded, and the spaces must not prune each
    /// other.
    decoders: [DatagramFecDecoder; 4],
    blocks: HashMap<u32, BlockCount>,
    /// Blocks counted already, so that a late datagram does not count one
    /// again.
    settled: HashSet<u32>,
    settled_order: VecDeque<u32>,
    window_received: u64,
    window_sent: u64,
    pub datagrams: u64,
    pub source_datagrams: u64,
    pub repair_datagrams: u64,
    pub objects: u64,
    pub recovered_objects: u64,
    /// Blocks seen and never decoded.
    pub lost_objects: u64,
    pub errors: u64,
    pub lost_datagrams: u64,
    pub sent_datagrams: u64,
    pub flows: u64,
    /// Parts that came on the fast lane, and the bytes of their canonical
    /// envelopes.
    pub fast_parts: u64,
    pub fast_envelope_bytes: u64,
    /// The envelope bytes of the part that the last `push` returned, if it
    /// came on the fast lane.
    pub last_fast_envelope_bytes: Option<usize>,
    /// The parts that the last `push` delivered from compact datagrams of
    /// the edge's fast lane (marked by the edge, see
    /// [`compact::set_fast_lane`]).
    pub last_compact_fast_lane: Vec<u64>,
    /// The source epoch of the last fast-lane part, and how often it changed
    /// (a contributor restart: its part numbers start again).
    pub fast_epoch: Option<u64>,
    pub fast_epoch_changes: u64,
    /// The compact lane's receiver.
    pub compact: CompactReceiver,
    /// The stream of this receiver, for the objects of the compact lane
    /// (whose datagrams do not carry it): see [`Self::expect`].
    pub stream_id: u64,
    /// Bytes of the media datagrams received, with their IPv4 and UDP
    /// headers ([`compact::IP_UDP_HEADER_BYTES`] each): what they cost on
    /// the wire.
    pub wire_bytes: u64,
    /// Decoded parts that another lane (or a copy) had delivered already,
    /// and that `push` did not return again.
    pub duplicate_parts: u64,
    /// Objects that one datagram delivered beyond the one `push` returned
    /// (a window's repair recovers several): see [`Self::next_ready`].
    ready: VecDeque<PartObject>,
}

/// A block is counted for loss once its datagrams have had this long to
/// arrive.
const LOSS_SETTLE: Duration = Duration::from_millis(150);
/// The loss that a client plans for until it has measured its path: a new
/// path may already be lossy.
pub const UNMEASURED_LOSS: f64 = 0.02;
const SETTLED_MEMORY: usize = 8_192;

impl ObjectReceiver {
    pub fn new() -> Self {
        Self {
            flows: 1,
            ..Self::default()
        }
    }

    /// Start receiving from another edge: its symbols of a part differ from
    /// those of the last edge, and must not meet them in one decoder.
    pub fn new_flow(&mut self) {
        self.decoders = Default::default();
        self.blocks.clear();
        self.settled.clear();
        self.settled_order.clear();
        self.compact.new_flow();
        self.flows += 1;
    }

    /// The edge's `OK`: the stream, the part that it sends first (the
    /// reference for the compact lane's 32-bit part numbers) and whether it
    /// sends the compact lane (holes are then asked for with NACK).
    pub fn expect(&mut self, stream_id: u64, next_part: u64, compact: bool) {
        self.stream_id = stream_id;
        self.compact.expect_from(next_part, compact);
        // The short-form parts that came before the OK, read now (their
        // objects wait in `ready`).
        for datagram in self.compact.take_early() {
            if let Some(object) = self.push_compact(&datagram) {
                self.ready.push_back(object);
            }
        }
    }

    /// The next object that the last `push` delivered besides the one it
    /// returned.
    pub fn next_ready(&mut self) -> Option<PartObject> {
        self.ready.pop_front()
    }

    /// Push one datagram and take every object that it delivered.
    pub fn push_all(&mut self, datagram: &[u8]) -> Vec<PartObject> {
        let mut objects: Vec<PartObject> = self.push(datagram).into_iter().collect();
        objects.extend(self.ready.drain(..));
        objects
    }

    /// Parts to ask the edge for again now (compact lane), at most
    /// [`MAX_NACK_PARTS`].
    pub fn nacks_due(&mut self, now: Instant) -> Vec<u64> {
        self.compact.nacks_due(now, MAX_NACK_PARTS)
    }

    /// The compact lane: push one compact datagram.
    fn push_compact(&mut self, datagram: &[u8]) -> Option<PartObject> {
        let now_unix_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_micros() as u64);
        let before = self.compact.stats;
        let parts = self.compact.push(datagram, Instant::now(), now_unix_us);
        let after = self.compact.stats;
        self.errors += after.errors - before.errors;
        self.duplicate_parts += after.duplicates - before.duplicates;
        for part in parts {
            self.objects += 1;
            if part.recovered {
                self.recovered_objects += 1;
            }
            if part.fast_lane && part.kind == compact::Kind::Part {
                self.last_compact_fast_lane.push(part.part);
            }
            // The other lanes' blocks of the part will not complete, and are
            // not loss.
            for other in [
                cut_through_block_id(part.part),
                block_id(ObjectKind::Part, part.part),
                fast_block_id(part.part),
            ] {
                if self.blocks.remove(&other).is_some() {
                    self.settle(other);
                }
            }
            self.ready.push_back(PartObject {
                kind: match part.kind {
                    compact::Kind::Gap => ObjectKind::Gap,
                    _ => ObjectKind::Part,
                },
                datagrams: 0,
                stream_id: self.stream_id,
                part: part.part,
                available_unix_us: 0,
                published_unix_us: part.published_unix_us,
                payload: Bytes::from(part.payload),
            });
        }
        self.ready.pop_front()
    }

    /// Push one media datagram. Returns the object that it completed.
    pub fn push(&mut self, datagram: &[u8]) -> Option<PartObject> {
        self.datagrams += 1;
        self.wire_bytes += (datagram.len() + compact::IP_UDP_HEADER_BYTES) as u64;
        self.last_fast_envelope_bytes = None;
        self.last_compact_fast_lane.clear();
        if compact::is_compact(datagram) || compact::is_short(datagram) {
            return self.push_compact(datagram);
        }
        let header = match DatagramFecHeader::decode(datagram) {
            Ok(header) => header,
            Err(_) => {
                self.errors += 1;
                return None;
            }
        };
        let source_symbol = is_source_symbol(datagram, header.source_symbols);
        if source_symbol {
            self.source_datagrams += 1;
        } else {
            self.repair_datagrams += 1;
        }
        if !self.settled.contains(&header.block_id) {
            let count = self.blocks.entry(header.block_id).or_insert(BlockCount {
                received: 0,
                source_symbols: u32::from(header.source_symbols),
                sent: None,
                decoded: false,
                first_seen: Instant::now(),
            });
            count.received += 1;
        }
        let seen = self
            .blocks
            .get(&header.block_id)
            .map_or(0, |count| count.received);
        let space = block_space(header.block_id);
        match self.decoders[space].push_datagram(datagram) {
            Ok(Some(bytes)) => match decode_any_object(bytes) {
                Ok((object, fast)) => {
                    if let Some(fast) = fast {
                        self.fast_decoded(header.block_id, fast);
                    }
                    self.objects += 1;
                    if let Some(count) = self.blocks.get_mut(&header.block_id) {
                        // A cut-through or fast-lane object does not say how
                        // many datagrams were sent (0): its source symbols and
                        // the repair of the client's own policy.
                        count.sent = Some(object.datagrams);
                        count.decoded = true;
                    }
                    if seen < u32::from(header.source_symbols) || !source_symbol {
                        // Decoded with fewer than all source symbols, or
                        // completed by a repair symbol.
                        self.recovered_objects += 1;
                    }
                    if object.kind == ObjectKind::Part {
                        // The part came on one lane: its blocks on the other
                        // lanes (another parent's block decoded first, or the
                        // edge sent the whole part again) will not complete,
                        // and are not loss.
                        for other in [
                            cut_through_block_id(object.part),
                            block_id(ObjectKind::Part, object.part),
                            fast_block_id(object.part),
                        ] {
                            if other != header.block_id && self.blocks.remove(&other).is_some() {
                                self.settle(other);
                            }
                        }
                    }
                    if matches!(object.kind, ObjectKind::Part | ObjectKind::Gap)
                        && !self
                            .compact
                            .delivered_elsewhere(object.part, Instant::now())
                    {
                        // A part that the compact lane (or another block)
                        // delivered already.
                        self.duplicate_parts += 1;
                        return None;
                    }
                    Some(object)
                }
                Err(_) => {
                    self.errors += 1;
                    None
                }
            },
            Ok(None) => None,
            Err(_) => {
                self.errors += 1;
                None
            }
        }
    }

    /// Remember a block as counted, so that a late datagram of it does not
    /// count again.
    fn settle(&mut self, block: u32) {
        if self.settled.insert(block) {
            self.settled_order.push_back(block);
        }
        if self.settled_order.len() > SETTLED_MEMORY {
            if let Some(old) = self.settled_order.pop_front() {
                self.settled.remove(&old);
            }
        }
    }

    /// A fast-lane block decoded. A new source epoch (the contributor
    /// restarted, and counts its parts from 0 again) starts a new flow: the
    /// old epoch's blocks of the same numbers must not meet the new ones in
    /// a decoder, nor its completed blocks hide them. The block that showed
    /// the new epoch stays completed in the new flow.
    fn fast_decoded(&mut self, block: u32, fast: FastObject) {
        if self.fast_epoch.is_some_and(|epoch| epoch != fast.epoch) {
            self.new_flow();
            self.decoders[FAST_SPACE].expire_block(block);
            self.settle(block);
            self.fast_epoch_changes += 1;
        }
        self.fast_epoch = Some(fast.epoch);
        self.fast_parts += 1;
        self.fast_envelope_bytes += fast.envelope_bytes as u64;
        self.last_fast_envelope_bytes = Some(fast.envelope_bytes);
    }

    /// Count the blocks that have settled, and return the datagram loss
    /// measured since the last call, if any datagram was due. A block that
    /// was never decoded is counted as sent with the repair of `policy`.
    pub fn take_loss(&mut self, policy: RepairPolicy) -> Option<f64> {
        let now = Instant::now();
        let settled: Vec<u32> = self
            .blocks
            .iter()
            .filter(|(_, count)| now.duration_since(count.first_seen) >= LOSS_SETTLE)
            .map(|(block, _)| *block)
            .collect();
        for block in settled {
            let count = self.blocks.remove(&block).unwrap();
            self.settle(block);
            let expected = count.source_symbols + policy.repair_symbols(count.source_symbols);
            let sent = match count.sent {
                Some(sent) if sent > 0 => sent,
                Some(_) => expected,
                None => {
                    if !count.decoded {
                        self.lost_objects += 1;
                    }
                    expected
                }
            };
            let received = count.received.min(sent);
            self.window_received += u64::from(received);
            self.window_sent += u64::from(sent);
            self.sent_datagrams += u64::from(sent);
            self.lost_datagrams += u64::from(sent - received);
        }
        // The compact lane's parts and window repair.
        let (received, sent) = self.compact.take_counts(now);
        self.window_received += received;
        self.window_sent += sent;
        self.sent_datagrams += sent;
        self.lost_datagrams += sent - received;
        if self.window_sent == 0 {
            return None;
        }
        let loss = 1.0 - self.window_received as f64 / self.window_sent as f64;
        self.window_received = 0;
        self.window_sent = 0;
        Some(loss)
    }
}

/// What a fast-lane block carried besides its part.
#[derive(Debug, Clone, Copy)]
struct FastObject {
    epoch: u64,
    envelope_bytes: usize,
}

/// The object of a decoded block: a part object the edge encoded, a
/// cache-mesh chunk frame (cut-through), or a canonical media-object
/// envelope (the fast lane).
fn decode_any_object(bytes: Vec<u8>) -> Result<(PartObject, Option<FastObject>), ObjectError> {
    if bytes.starts_with(OBJECT_MAGIC) {
        return PartObject::decode(Bytes::from(bytes)).map(|object| (object, None));
    }
    if bytes.starts_with(&media_object::WIRE_MAGIC) {
        let (object, fast) = fast_object(Bytes::from(bytes))?;
        return Ok((object, Some(fast)));
    }
    cut_through_object(&bytes)
        .map(|object| (object, None))
        .ok_or(ObjectError(
            "not a part object, a chunk frame or an envelope",
        ))
}

/// The part of a canonical envelope: the slot that every node's commit
/// stores for it (the same function), then the LL-HLS part bytes of that
/// slot (the same function as the edge's part responses).
fn fast_object(envelope: Bytes) -> Result<(PartObject, FastObject), ObjectError> {
    let envelope_bytes = envelope.len();
    let part = crate::canonical_slot::canonical_part_from_envelope(envelope)
        .map_err(|error| ObjectError(error.reason()))?;
    let (media, _init) =
        slot_media(&part.slot, part.stream_id).ok_or(ObjectError("unreadable canonical slot"))?;
    let key = part.object.key();
    Ok((
        PartObject {
            kind: ObjectKind::Part,
            datagrams: 0,
            stream_id: part.stream_id,
            part: key.object(),
            available_unix_us: 0,
            published_unix_us: crate::canonical_slot::object_published_unix_us(&part.object)
                .unwrap_or(0),
            payload: media,
        },
        FastObject {
            epoch: key.epoch(),
            envelope_bytes,
        },
    ))
}

/// Client symbol size of a cut-through block: the largest divisor of the
/// mesh symbol size that fits `max`, so that each mesh source symbol is
/// forwarded at once as a whole number of client symbols. `None` when no
/// divisor of at least 256 bytes fits.
pub fn cut_through_symbol_size(mesh_symbol_size: u16, max: u16) -> Option<u16> {
    let mesh = u32::from(mesh_symbol_size);
    if mesh <= u32::from(max) {
        return Some(mesh_symbol_size);
    }
    (1..=mesh)
        .filter(|parts| mesh % parts == 0)
        .map(|parts| mesh / parts)
        .find(|size| *size <= u32::from(max))
        .filter(|size| *size >= 256)
        .map(|size| size as u16)
}

/// One client source datagram of a cut-through block: source symbol `esi`
/// of a block of `transfer_length` bytes in symbols of `symbol_size`. `data`
/// is the symbol's bytes, padded with zeros to the symbol size as RaptorQ
/// pads the end of a block.
pub fn source_datagram(
    block_id: u32,
    transfer_length: u32,
    packet_sequence: u32,
    symbol_size: u16,
    esi: u32,
    data: &[u8],
) -> Option<Vec<u8>> {
    let source_symbols = u16::try_from(transfer_length.div_ceil(u32::from(symbol_size))).ok()?;
    let mut payload = Vec::with_capacity(4 + usize::from(symbol_size));
    payload.push(0);
    payload.extend_from_slice(&esi.to_be_bytes()[1..]);
    payload.extend_from_slice(&data[..data.len().min(usize::from(symbol_size))]);
    payload.resize(4 + usize::from(symbol_size), 0);
    let header = DatagramFecHeader::raptorq(
        block_id,
        transfer_length,
        packet_sequence,
        source_symbols,
        symbol_size,
        &payload,
    )
    .ok()?;
    let mut datagram = vec![0_u8; raptorq_datagram_fec::HEADER_LEN];
    header.encode(&mut datagram).ok()?;
    datagram.extend_from_slice(&payload);
    Some(datagram)
}

/// Encode a whole block exactly as a cut-through block is cut: `frame` in
/// symbols of `symbol_size`, under `block_id`, with `repair` repair symbols.
/// Its source datagrams carry the same symbols as [`source_datagram`].
pub fn encode_block(
    frame: &[u8],
    block_id: u32,
    symbol_size: u16,
    repair: u32,
) -> Result<Vec<Vec<u8>>, raptorq_datagram_fec::DatagramFecError> {
    let sources = frame.len().div_ceil(usize::from(symbol_size)).max(1);
    DatagramFecEncoder::new()
        .with_symbol_size(symbol_size)
        .with_source_symbols(u16::try_from(sources).unwrap_or(u16::MAX))
        .with_initial_block_id(block_id)
        .encode_object_with_repair_symbols(frame, repair)
}

/// Whether a datagram carries a source symbol: the RaptorQ encoding symbol
/// id (the last three bytes of the payload id after the header) is below
/// the number of source symbols.
fn is_source_symbol(datagram: &[u8], source_symbols: u16) -> bool {
    let header_len = raptorq_datagram_fec::HEADER_LEN;
    match datagram.get(header_len..header_len + 4) {
        Some(id) => {
            let esi = u32::from_be_bytes([0, id[1], id[2], id[3]]);
            esi < u32::from(source_symbols)
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_datagrams_round_trip() {
        for control in [
            Control::Subscribe {
                stream_id: 3001,
                from_part: Some(42),
                repair: RepairPolicy::default(),
                token: Some(0xdead_beef_0123_4567),
                fast_lane: false,
                compact: None,
            },
            Control::Subscribe {
                stream_id: 1,
                from_part: None,
                repair: RepairPolicy {
                    permille: 500,
                    min_symbols: 4,
                    loss_ppm: 52_000,
                },
                token: None,
                fast_lane: false,
                compact: None,
            },
            Control::Subscribe {
                stream_id: 4001,
                from_part: Some(7),
                repair: RepairPolicy::default(),
                token: Some(3),
                fast_lane: true,
                compact: Some(CompactPlan::CLEAN),
            },
            Control::Subscribe {
                stream_id: 4001,
                from_part: None,
                repair: RepairPolicy::default(),
                token: None,
                fast_lane: false,
                compact: Some(CompactPlan {
                    copies: 1,
                    window: 16,
                    window_repair: 2,
                    short_header: true,
                }),
            },
            Control::Nack {
                stream_id: 4001,
                token: 0xfeed,
                parts: vec![12, 13, 900_000_000_001],
            },
            Control::Unsubscribe {
                stream_id: 9,
                token: 7,
            },
            Control::Token {
                stream_id: 9,
                token: u64::MAX,
            },
            Control::Subscribed {
                stream_id: 9,
                next_part: 77,
                compact: false,
            },
            Control::Subscribed {
                stream_id: 9,
                next_part: 78,
                compact: true,
            },
            Control::Waiting {
                stream_id: 9,
                next_part: 78,
            },
            Control::Refused {
                stream_id: 9,
                reason: "no-playlist-slot".into(),
            },
            Control::Move {
                stream_id: 9,
                reason: "draining".into(),
            },
        ] {
            assert_eq!(Control::decode(&control.encode()), Some(control));
        }
        assert_eq!(Control::decode(b"WAVEY-DAW-SUBSCRIBE/2 5"), None);
        assert_eq!(Control::decode(b"NTU/1 SUB 5 - 1"), None);
        assert_eq!(Control::decode(b"NTU/1 SUB 5 - 1 2 3 - x"), None);
        // A NACK names at least one part and at most MAX_NACK_PARTS.
        assert_eq!(Control::decode(b"NTU/1 NACK 5 00ff"), None);
        let many: String = (0..=MAX_NACK_PARTS)
            .map(|part| format!(" {part}"))
            .collect();
        assert_eq!(
            Control::decode(format!("NTU/1 NACK 5 00ff{many}").as_bytes()),
            None
        );
        // An OK from an edge without the compact lane parses as before.
        assert_eq!(
            Control::decode(b"NTU/1 OK 5 9"),
            Some(Control::Subscribed {
                stream_id: 5,
                next_part: 9,
                compact: false
            })
        );
        // The compact word comes after F1, once.
        assert_eq!(
            Control::decode(b"NTU/1 SUB 5 - 0 1 0 00ff C0.32.1 F1"),
            None
        );
        assert_eq!(
            Control::decode(b"NTU/1 SUB 5 - 0 1 0 00ff C0.32.1 C0.32.1"),
            None
        );
    }

    #[test]
    fn the_raptorq_lane_keeps_the_repair_of_before_the_compact_lane() {
        let compact = RepairPolicy::base_for_lane(true, None, None);
        assert_eq!((compact.permille, compact.min_symbols), (0, 1));
        let raptorq = RepairPolicy::base_for_lane(false, None, None);
        assert_eq!((raptorq.permille, raptorq.min_symbols), (250, 3));
        // A one-symbol part on the RaptorQ lane: three repair symbols, as
        // in wave 4, whatever the measured loss adds.
        assert_eq!(raptorq.repair_symbols(1), 3);
        assert_eq!(compact.repair_symbols(1), 1);
        // What the user sets stands on either lane.
        let set = RepairPolicy::base_for_lane(false, Some(100), Some(2));
        assert_eq!((set.permille, set.min_symbols), (100, 2));
        let set = RepairPolicy::base_for_lane(true, Some(250), None);
        assert_eq!((set.permille, set.min_symbols), (250, 1));
    }

    #[test]
    fn a_sub_may_ask_for_the_fast_lane_and_an_old_one_still_parses() {
        for token in [None, Some(0xdead_beef_u64)] {
            for fast_lane in [false, true] {
                let control = Control::Subscribe {
                    stream_id: 4001,
                    from_part: Some(9),
                    repair: RepairPolicy::default(),
                    token,
                    fast_lane,
                    compact: None,
                };
                let encoded = control.encode();
                assert_eq!(encoded.ends_with(b" F1"), fast_lane);
                assert_eq!(Control::decode(&encoded), Some(control));
            }
        }
        // A request of a client from before the fast lane.
        let old = b"NTU/1 SUB 4001 12 250 3 0 00000000deadbeef";
        assert_eq!(
            Control::decode(old),
            Some(Control::Subscribe {
                stream_id: 4001,
                from_part: Some(12),
                repair: RepairPolicy {
                    permille: 250,
                    min_symbols: 3,
                    loss_ppm: 0
                },
                token: Some(0xdead_beef),
                fast_lane: false,
                compact: None,
            })
        );
        let first = format!("NTU/1 SUB 4001 - 250 3 0 - {}", ".".repeat(48));
        assert!(matches!(
            Control::decode(first.as_bytes()),
            Some(Control::Subscribe {
                token: None,
                fast_lane: false,
                ..
            })
        ));
        // Only F1, and only once, after the token.
        assert_eq!(Control::decode(b"NTU/1 SUB 4001 - 250 3 0 00ff F2"), None);
        assert_eq!(
            Control::decode(b"NTU/1 SUB 4001 - 250 3 0 00ff F1 F1"),
            None
        );
        // A first request that is padded still takes F1 after its padding.
        let first = format!("NTU/1 SUB 4001 - 250 3 0 - {} F1", ".".repeat(48));
        assert!(matches!(
            Control::decode(first.as_bytes()),
            Some(Control::Subscribe {
                fast_lane: true,
                ..
            })
        ));
    }

    /// A canonical opaque envelope of `part` in `epoch`, of `len` bytes.
    fn envelope(epoch: u64, part: u64, len: usize) -> Vec<u8> {
        let payload: Vec<u8> = (0..len).map(|i| (i as u64 * 13 + part) as u8).collect();
        let key = media_object::ObjectKey::for_payload(
            "default", "4001", "audio", epoch, 0, part, 1, &payload,
        )
        .unwrap();
        let object =
            media_object::MediaObject::builder(key, media_object::ObjectKind::Media, payload)
                .with_metadata("container", b"opaque".to_vec())
                .with_metadata("payload-format", b"opaque-part-v1".to_vec())
                .build()
                .unwrap();
        media_object::encode(&object).unwrap()
    }

    /// The fast-lane datagrams of an envelope, as a contributor encodes it
    /// (`symbol_size`, with `repair` repair symbols) and an edge rewraps it.
    fn fast_datagrams(envelope: &[u8], part: u64, symbol_size: u16, repair: u32) -> Vec<Vec<u8>> {
        let sources = envelope.len().div_ceil(usize::from(symbol_size)).max(1);
        DatagramFecEncoder::new()
            .with_symbol_size(symbol_size)
            .with_source_symbols(sources as u16)
            .with_initial_block_id(17)
            .encode_object_with_repair_symbols(envelope, repair)
            .unwrap()
            .iter()
            .map(|datagram| rewrap_fast_vec(datagram, part).unwrap())
            .collect()
    }

    #[test]
    fn a_rewrapped_datagram_keeps_everything_but_its_block_id() {
        let datagrams = DatagramFecEncoder::new()
            .with_symbol_size(100)
            .with_source_symbols(3)
            .with_initial_block_id(17)
            .encode_object_with_repair_symbols(&envelope(0, 5, 250), 2)
            .unwrap();
        for original in &datagrams {
            let fast = rewrap_fast_vec(original, 0x4000_0005).unwrap();
            let (a, b) = (
                DatagramFecHeader::decode(original).unwrap(),
                DatagramFecHeader::decode(&fast).unwrap(),
            );
            assert_eq!(b.block_id, fast_block_id(0x4000_0005));
            assert_eq!(b.block_id, 0xC000_0005);
            assert_eq!(block_space(b.block_id), FAST_SPACE);
            assert_eq!(
                (
                    a.transfer_length,
                    a.packet_sequence,
                    a.source_symbols,
                    a.symbol_size
                ),
                (
                    b.transfer_length,
                    b.packet_sequence,
                    b.source_symbols,
                    b.symbol_size
                )
            );
            // The CRC is right for the new block id.
            assert_eq!(b.payload(&fast).unwrap(), a.payload(original).unwrap());
            assert_eq!(fast[32..], original[32..]);
        }
        assert!(rewrap_fast_vec(b"too short", 1).is_none());
        let mut small = [0_u8; 8];
        assert!(rewrap_fast(&datagrams[0], 1, &mut small).is_none());
    }

    #[test]
    fn an_init_block_never_prunes_an_in_flight_fast_lane_block() {
        // Part 500 rides the fast lane in 21 symbols; half of them are in
        // when the inits of a hundred later parts and the cache-fed parts
        // around it complete. Part 500 still decodes.
        let envelope = envelope(0, 500, 20_000);
        let fast = fast_datagrams(&envelope, 500, 1_000, 2);
        let mut receiver = ObjectReceiver::new();
        for datagram in &fast[..10] {
            assert!(receiver.push(datagram).is_none());
        }
        for part in 501..600 {
            let init = PartObject {
                kind: ObjectKind::Init,
                ..object(part, 300)
            };
            assert!(
                encode_object(&init, DEFAULT_SYMBOL_SIZE, RepairPolicy::default())
                    .unwrap()
                    .iter()
                    .any(|datagram| receiver.push(datagram).is_some())
            );
        }
        assert!(encode_object(
            &object(499, 900),
            DEFAULT_SYMBOL_SIZE,
            RepairPolicy::default()
        )
        .unwrap()
        .iter()
        .any(|datagram| receiver.push(datagram).is_some()));
        let decoded = fast[10..]
            .iter()
            .find_map(|datagram| receiver.push(datagram))
            .expect("the fast-lane block completes");
        assert_eq!(decoded.part, 500);
        assert_eq!(receiver.fast_parts, 1);
        assert_eq!(receiver.last_fast_envelope_bytes, Some(envelope.len()));
    }

    #[test]
    fn an_epoch_reset_with_lower_part_numbers_decodes_after_a_new_flow() {
        let mut receiver = ObjectReceiver::new();
        let decode = |receiver: &mut ObjectReceiver, epoch: u64, part: u64| {
            fast_datagrams(&envelope(epoch, part, 300), part, 400, 1)
                .iter()
                .find_map(|datagram| receiver.push(datagram))
        };
        for part in 1_000..1_010 {
            assert_eq!(decode(&mut receiver, 1, part).unwrap().part, part);
        }
        assert_eq!(receiver.fast_epoch, Some(1));
        let flows = receiver.flows;
        // The contributor restarts: epoch 2 counts its parts from 0.
        for part in 0..10 {
            let decoded = decode(&mut receiver, 2, part).expect("the new epoch's part decodes");
            assert_eq!(decoded.part, part);
        }
        assert_eq!(receiver.fast_epoch, Some(2));
        assert_eq!(receiver.fast_epoch_changes, 1);
        assert_eq!(
            receiver.flows,
            flows + 1,
            "the epoch change started a new flow"
        );
        // A late symbol of the part that showed the change does not decode
        // it again.
        let late = fast_datagrams(&envelope(2, 0, 300), 0, 400, 1);
        assert!(late
            .iter()
            .all(|datagram| receiver.push(datagram).is_none()));
        assert_eq!(receiver.fast_parts, 20);
    }

    #[test]
    fn a_token_answer_is_shorter_than_the_request() {
        let request = Control::Subscribe {
            stream_id: 1,
            from_part: None,
            repair: RepairPolicy::default(),
            token: None,
            fast_lane: true,
            compact: Some(CompactPlan::CLEAN),
        };
        let answer = Control::Token {
            stream_id: 1,
            token: u64::MAX,
        };
        assert!(answer.encode().len() <= request.encode().len());
        assert!(answer.encode().len() <= MIN_FIRST_REQUEST_BYTES);
        assert!(request.encode().len() >= MIN_FIRST_REQUEST_BYTES);
    }

    /// The probability that a part of `k` symbols with `r` repair symbols is
    /// lost at loss `p`, with the decode margin of `repair_for_loss`.
    fn part_loss(k: u32, r: u32, p: f64) -> f64 {
        let n = k + r;
        let margin = u32::from(k > 1);
        let mut delivered = 0.0;
        for lost in 0..=(r.saturating_sub(margin)) {
            let mut c = 1.0_f64;
            for i in 0..lost {
                c *= f64::from(n - i) / f64::from(i + 1);
            }
            delivered += c * p.powi(lost as i32) * (1.0 - p).powi((n - lost) as i32);
        }
        1.0 - delivered
    }

    #[test]
    fn repair_keeps_every_part_size_below_the_target() {
        for p in [0.001, 0.01, 0.05, 0.1] {
            for k in [1_u32, 2, 3, 10, 30, 60, 340] {
                let r = repair_for_loss(k, p);
                assert!(
                    part_loss(k, r, p) <= TARGET_PART_LOSS * 1.01,
                    "k {k} p {p} r {r}"
                );
                if r > u32::from(k > 1) {
                    assert!(
                        part_loss(k, r - 1, p) > TARGET_PART_LOSS,
                        "k {k} p {p}: {r} is not least"
                    );
                }
            }
        }
        // The planned loss of a 5 % path covers a part of 30 symbols, which
        // the earlier proportional rule did not.
        let policy = RepairPolicy::for_loss(0.05, RepairPolicy::default());
        assert!(part_loss(30, policy.repair_symbols(30), 0.05) <= TARGET_PART_LOSS);
        assert_eq!(
            RepairPolicy::for_loss(0.0, RepairPolicy::default()),
            RepairPolicy::default()
        );
    }

    #[test]
    fn a_small_part_is_sent_in_a_symbol_of_its_own_size() {
        assert_eq!(geometry(120, 1_200), (1, 120));
        assert_eq!(geometry(1_500, 1_200), (2, 750));
        assert_eq!(geometry(400_000, 1_200), (334, 1_198));
        assert_eq!(geometry(3, 1_200), (1, 16));
    }

    fn object(part: u64, len: usize) -> PartObject {
        PartObject {
            kind: ObjectKind::Part,
            datagrams: 0,
            stream_id: 1,
            part,
            available_unix_us: 7,
            published_unix_us: 5,
            payload: Bytes::from(
                (0..len)
                    .map(|i| (i * 31 + part as usize) as u8)
                    .collect::<Vec<_>>(),
            ),
        }
    }

    #[test]
    fn a_part_is_recovered_bit_for_bit_with_any_loss_within_its_repair() {
        // A 4K video part of 400 kB, and 5 ms audio parts of 80 and 1,440
        // bytes.
        for (part, len) in [(10_u64, 400_000_usize), (11, 80), (12, 1_440)] {
            let original = object(part, len);
            let datagrams =
                encode_object(&original, DEFAULT_SYMBOL_SIZE, RepairPolicy::default()).unwrap();
            let (sources, _) = geometry(OBJECT_HEADER_LEN + len, DEFAULT_SYMBOL_SIZE);
            let repair = datagrams.len() - sources as usize;
            assert_eq!(
                repair as u32,
                RepairPolicy::default().repair_symbols(sources)
            );
            // Lose one datagram fewer than there are repair symbols (a block
            // of more than one symbol is decoded with one to spare), spread
            // over the source symbols.
            let lose = repair - usize::from(sources > 1);
            let stride = (datagrams.len() / lose.max(1)).max(1);
            let mut receiver = ObjectReceiver::new();
            let mut decoded = None;
            for (index, datagram) in datagrams.iter().enumerate() {
                if index % stride == 0 && index / stride < lose {
                    continue;
                }
                if let Some(object) = receiver.push(datagram) {
                    decoded = Some(object);
                }
            }
            let decoded = decoded.expect("object decoded");
            assert_eq!(decoded.datagrams as usize, datagrams.len());
            assert_eq!(
                PartObject {
                    datagrams: 0,
                    ..decoded
                },
                original
            );
        }
    }

    #[test]
    fn symbols_of_one_part_from_two_edges_never_make_a_wrong_object() {
        // Two edges stamp different availability times, so their symbols of
        // one part differ. A decoder fed both either decodes one of them or
        // rejects the result by its checksum; it never yields other bytes.
        let mut a = object(20, 6_000);
        a.available_unix_us = 1;
        let mut b = a.clone();
        b.available_unix_us = 2;
        let policy = RepairPolicy::default();
        let from_a = encode_object(&a, DEFAULT_SYMBOL_SIZE, policy).unwrap();
        let from_b = encode_object(&b, DEFAULT_SYMBOL_SIZE, policy).unwrap();
        let mut receiver = ObjectReceiver::new();
        let mut decoded = None;
        for (index, datagram) in from_a
            .iter()
            .take(3)
            .chain(from_b.iter().skip(3))
            .enumerate()
        {
            let _ = index;
            if let Some(object) = receiver.push(datagram) {
                decoded = Some(object);
            }
        }
        if let Some(decoded) = decoded {
            assert_eq!(decoded.payload, a.payload);
        }
        // The client starts a new flow when it moves, and then decodes the
        // second edge's part.
        let mut receiver = ObjectReceiver::new();
        for datagram in from_a.iter().take(3) {
            receiver.push(datagram);
        }
        receiver.new_flow();
        let decoded = from_b
            .iter()
            .find_map(|datagram| receiver.push(datagram))
            .unwrap();
        assert_eq!(decoded.payload, b.payload);
        assert_eq!(decoded.available_unix_us, 2);
    }

    #[test]
    fn a_receiver_measures_the_loss_of_its_path() {
        let mut receiver = ObjectReceiver::new();
        for part in 0..200_u64 {
            let datagrams = encode_object(
                &object(part, 3_000),
                DEFAULT_SYMBOL_SIZE,
                RepairPolicy::default(),
            )
            .unwrap();
            for (index, datagram) in datagrams.iter().enumerate() {
                // Every tenth datagram of the path is lost.
                if (part as usize * datagrams.len() + index) % 10 == 0 {
                    continue;
                }
                receiver.push(datagram);
            }
        }
        std::thread::sleep(LOSS_SETTLE);
        let loss = receiver.take_loss(RepairPolicy::default()).unwrap();
        assert!((0.08..0.12).contains(&loss), "{loss}");
        assert_eq!(receiver.objects, 200);
        // A late datagram of a counted block is not counted again.
        let late = encode_object(
            &object(0, 3_000),
            DEFAULT_SYMBOL_SIZE,
            RepairPolicy::default(),
        )
        .unwrap();
        receiver.push(&late[0]);
        std::thread::sleep(LOSS_SETTLE);
        assert_eq!(receiver.take_loss(RepairPolicy::default()), None);
        assert_eq!(receiver.lost_objects, 0);
    }

    /// A cache-mesh chunk frame as a parent sends it (playlists `MeshFrame`).
    fn chunk_frame(stream: u64, slot: u64, payload: &[u8]) -> Vec<u8> {
        let node = b"lon-dist-a";
        let mut frame = Vec::new();
        frame.extend_from_slice(b"PLMESH1\0");
        frame.push(2);
        frame.push(2);
        frame.extend_from_slice(&(node.len() as u16).to_be_bytes());
        frame.extend_from_slice(node);
        frame.extend_from_slice(&stream.to_be_bytes());
        frame.extend_from_slice(&slot.to_be_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    fn fmp4_slot_payload(init: &[u8], media: &[u8]) -> Vec<u8> {
        let mut slot = b"AVFMP4S1".to_vec();
        slot.extend_from_slice(&(init.len() as u32).to_be_bytes());
        slot.extend_from_slice(&(media.len() as u32).to_be_bytes());
        slot.extend_from_slice(init);
        slot.extend_from_slice(media);
        slot
    }

    #[test]
    fn the_symbols_cut_from_a_frame_are_the_symbols_the_encoder_makes() {
        let frame = chunk_frame(1, 9, &vec![7_u8; 50_000]);
        for size in [900_u16, 1_000, 1_200] {
            let encoded = encode_block(&frame, 33, size, 4).unwrap();
            let sources = frame.len().div_ceil(usize::from(size));
            for esi in 0..sources {
                let start = esi * usize::from(size);
                let end = (start + usize::from(size)).min(frame.len());
                let cut = source_datagram(
                    33,
                    frame.len() as u32,
                    0,
                    size,
                    esi as u32,
                    &frame[start..end],
                )
                .unwrap();
                assert_eq!(
                    cut[HEADER_LEN_FOR_TESTS..],
                    encoded[esi][HEADER_LEN_FOR_TESTS..],
                    "esi {esi}"
                );
            }
        }
        assert_eq!(cut_through_symbol_size(16_000, 1_200), Some(1_000));
        assert_eq!(cut_through_symbol_size(900, 1_200), Some(900));
        assert_eq!(cut_through_symbol_size(1_801, 1_200), None);
    }

    #[test]
    fn a_cut_through_part_decodes_to_its_ll_hls_bytes_with_loss() {
        // A 4K part in 16 kB mesh symbols, cut into 1,000-byte client
        // symbols as they arrive; a fifth of them lost; the edge's repair
        // after the block decodes completes it.
        let media: Vec<u8> = (0..400_000_u32).map(|i| (i * 7) as u8).collect();
        let frame = chunk_frame(1, 812, &fmp4_slot_payload(b"ftyp-moov", &media));
        let mesh_symbol = 16_000_usize;
        let client = cut_through_symbol_size(mesh_symbol as u16, 1_200).unwrap();
        let block = cut_through_block_id(812);
        let mut receiver = ObjectReceiver::new();
        let mut sent = 0_u32;
        let mut decoded = None;
        for mesh_esi in 0..frame.len().div_ceil(mesh_symbol) {
            let start = mesh_esi * mesh_symbol;
            let symbol = &frame[start..(start + mesh_symbol).min(frame.len())];
            for (index, piece) in symbol.chunks(usize::from(client)).enumerate() {
                let esi = (mesh_esi * mesh_symbol / usize::from(client) + index) as u32;
                let datagram =
                    source_datagram(block, frame.len() as u32, sent, client, esi, piece).unwrap();
                sent += 1;
                if sent % 5 == 0 {
                    continue;
                }
                decoded = decoded.or(receiver.push(&datagram));
            }
        }
        assert!(
            decoded.is_none(),
            "a fifth of the source symbols is missing"
        );
        let sources = frame.len().div_ceil(usize::from(client)) as u32;
        let repair = RepairPolicy::for_loss(0.2, RepairPolicy::default()).repair_symbols(sources);
        let encoded = encode_block(&frame, block, client, repair).unwrap();
        for datagram in &encoded[sources as usize..] {
            decoded = decoded.or(receiver.push(datagram));
        }
        let decoded = decoded.expect("the repair completes the block");
        assert_eq!(decoded.part, 812);
        assert_eq!(decoded.stream_id, 1);
        assert_eq!(decoded.payload.as_ref(), media.as_slice());
    }

    #[test]
    fn slot_media_reads_every_slot_form() {
        let fmp4 = Bytes::from(fmp4_slot_payload(b"init", b"media"));
        assert_eq!(
            slot_media(&fmp4, 1),
            Some((
                Bytes::from_static(b"media"),
                Some(Bytes::from_static(b"init"))
            ))
        );
        let ts = Bytes::from_static(b"\x47raw-ts");
        assert_eq!(slot_media(&ts, 1), Some((ts.clone(), None)));
    }

    #[test]
    fn a_block_of_another_space_does_not_make_the_client_forget_a_cut_through_block() {
        // Part 500 is being cut through (high block ids are far above it);
        // an init (block 0x8000_0000 | 501) and a part sent whole
        // (0x4000_0000 | 499) complete meanwhile. Part 500 still decodes.
        let frame = chunk_frame(1, 500, &fmp4_slot_payload(b"", &vec![9_u8; 20_000]));
        let cut = encode_block(&frame, cut_through_block_id(500), 1_000, 4).unwrap();
        let mut receiver = ObjectReceiver::new();
        for datagram in &cut[..10] {
            assert!(receiver.push(datagram).is_none());
        }
        let init = PartObject {
            kind: ObjectKind::Init,
            ..object(501, 900)
        };
        assert!(
            encode_object(&init, DEFAULT_SYMBOL_SIZE, RepairPolicy::default())
                .unwrap()
                .iter()
                .any(|datagram| receiver.push(datagram).is_some())
        );
        assert!(encode_object(
            &object(499, 900),
            DEFAULT_SYMBOL_SIZE,
            RepairPolicy::default()
        )
        .unwrap()
        .iter()
        .any(|datagram| receiver.push(datagram).is_some()));
        let decoded = cut[10..]
            .iter()
            .find_map(|datagram| receiver.push(datagram));
        assert_eq!(decoded.expect("the cut-through block completes").part, 500);
    }

    const HEADER_LEN_FOR_TESTS: usize = raptorq_datagram_fec::HEADER_LEN;

    #[test]
    fn init_and_part_objects_do_not_share_a_block() {
        assert_ne!(block_id(ObjectKind::Init, 5), block_id(ObjectKind::Part, 5));
        assert_eq!(block_id(ObjectKind::Gap, 5), block_id(ObjectKind::Part, 5));
        assert_ne!(block_id(ObjectKind::Part, 5), cut_through_block_id(5));
    }
}
