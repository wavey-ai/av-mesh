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
//! loss that the client asked for.
//!
//! The protocol has two kinds of datagram.
//!
//! - Control datagrams are one line of ASCII text that starts with `NTU/1`:
//!
//!   | from   | text                                              |
//!   | ------ | ------------------------------------------------- |
//!   | client | `NTU/1 SUB <stream> <from part or -> <repair ‰> <min repair>` |
//!   | client | `NTU/1 UNSUB <stream>`                            |
//!   | edge   | `NTU/1 OK <stream> <first part>`                  |
//!   | edge   | `NTU/1 REFUSED <stream> <reason>`                 |
//!   | edge   | `NTU/1 MOVE <stream> <reason>`                    |
//!
//!   A client repeats `SUB` at least every [`REFRESH`]; a subscription that is
//!   not refreshed for [`SUBSCRIPTION_TTL`] ends. `MOVE` tells the client to
//!   go to the next cache of its assignment (the edge drains, or has lost its
//!   parents); `REFUSED` that this edge cannot take it.
//! - Media datagrams are RaptorQ datagrams of `raptorq-datagram-fec` (they
//!   start with its magic). Each decodes to one [`PartObject`]: a part, the
//!   initialisation segment of an fMP4 stream, or the notice that the edge
//!   skipped a part that it never received (a gap, as LL-HLS has
//!   `EXT-X-GAP`). The block id is the part number, so a client knows which
//!   part a lost block was.
//!
//! One client socket carries one stream: block ids are per stream.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use raptorq_datagram_fec::{DatagramFecDecoder, DatagramFecEncoder, DatagramFecHeader};
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

pub const CONTROL_PREFIX: &str = "NTU/1 ";
/// A client repeats its subscription this often.
pub const REFRESH: Duration = Duration::from_secs(2);
/// A subscription that is not repeated for this long ends.
pub const SUBSCRIPTION_TTL: Duration = Duration::from_secs(10);
/// Symbol size of the media datagrams: a datagram stays under 1,280 bytes
/// with its headers, the smallest IPv6 path MTU.
pub const DEFAULT_SYMBOL_SIZE: u16 = 1_200;
/// Repair asked for by default: a quarter of the source symbols, and at
/// least three, so that a 5 ms audio part of one symbol arrives in four
/// copies and survives 5 % random loss with a probability of 1 - 0.05^4.
pub const DEFAULT_REPAIR_PERMILLE: u32 = 250;
pub const DEFAULT_MIN_REPAIR_SYMBOLS: u32 = 3;
/// The most repair an edge sends for one subscriber.
pub const MAX_REPAIR_PERMILLE: u32 = 1_000;
pub const MAX_MIN_REPAIR_SYMBOLS: u32 = 16;

const OBJECT_MAGIC: &[u8; 4] = b"NTO1";
const OBJECT_HEADER_LEN: usize = 4 + 1 + 1 + 4 + 8 + 8 + 8 + 8;
/// Part loss that the adaptive repair aims below.
pub const TARGET_PART_LOSS: f64 = 1e-7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairPolicy {
    /// Repair symbols per thousand source symbols.
    pub permille: u32,
    /// Repair symbols of every part at least.
    pub min_symbols: u32,
}

impl Default for RepairPolicy {
    fn default() -> Self {
        Self {
            permille: DEFAULT_REPAIR_PERMILLE,
            min_symbols: DEFAULT_MIN_REPAIR_SYMBOLS,
        }
    }
}

impl RepairPolicy {
    pub fn bounded(self) -> Self {
        Self {
            permille: self.permille.min(MAX_REPAIR_PERMILLE),
            min_symbols: self.min_symbols.min(MAX_MIN_REPAIR_SYMBOLS),
        }
    }

    /// The larger of two policies, for a part sent to several subscribers.
    pub fn max(self, other: Self) -> Self {
        Self {
            permille: self.permille.max(other.permille),
            min_symbols: self.min_symbols.max(other.min_symbols),
        }
    }

    /// The repair that keeps the loss of a part below [`TARGET_PART_LOSS`]
    /// on a path that loses the fraction `loss` of datagrams at random, and
    /// never less than `base`.
    ///
    /// The smallest part, one source symbol, is lost when all its datagrams
    /// are: `loss^(1 + min_symbols)`. A large part of `k` source symbols
    /// needs a margin over `k * loss`: three times the loss.
    pub fn for_loss(loss: f64, base: Self) -> Self {
        if !(loss > 0.0) {
            return base.bounded();
        }
        // Measured loss is an estimate: plan for half as much again.
        let loss = (loss * 1.5 + 0.002).min(0.5);
        let mut min_symbols = base.min_symbols;
        while min_symbols < MAX_MIN_REPAIR_SYMBOLS
            && loss.powi(min_symbols as i32 + 1) > TARGET_PART_LOSS
        {
            min_symbols += 1;
        }
        let permille = ((loss * 3_000.0).ceil() as u32).max(base.permille);
        Self {
            permille,
            min_symbols,
        }
        .bounded()
    }

    pub fn repair_symbols(self, source_symbols: u32) -> u32 {
        let proportional = (u64::from(source_symbols) * u64::from(self.permille)).div_ceil(1_000);
        (proportional as u32).max(self.min_symbols)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    Subscribe {
        stream_id: u64,
        from_part: Option<u64>,
        repair: RepairPolicy,
    },
    Unsubscribe {
        stream_id: u64,
    },
    Subscribed {
        stream_id: u64,
        first_part: u64,
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
    pub fn encode(&self) -> Vec<u8> {
        let text = match self {
            Self::Subscribe {
                stream_id,
                from_part,
                repair,
            } => format!(
                "{CONTROL_PREFIX}SUB {stream_id} {} {} {}",
                from_part.map_or_else(|| "-".to_owned(), |part| part.to_string()),
                repair.permille,
                repair.min_symbols
            ),
            Self::Unsubscribe { stream_id } => format!("{CONTROL_PREFIX}UNSUB {stream_id}"),
            Self::Subscribed {
                stream_id,
                first_part,
            } => format!("{CONTROL_PREFIX}OK {stream_id} {first_part}"),
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
        let control = match verb {
            "SUB" => {
                let from_part = match words.next()? {
                    "-" => None,
                    part => Some(part.parse().ok()?),
                };
                let permille = words.next()?.parse().ok()?;
                let min_symbols = words.next()?.parse().ok()?;
                Self::Subscribe {
                    stream_id,
                    from_part,
                    repair: RepairPolicy {
                        permille,
                        min_symbols,
                    },
                }
            }
            "UNSUB" => Self::Unsubscribe { stream_id },
            "OK" => Self::Subscribed {
                stream_id,
                first_part: words.next()?.parse().ok()?,
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
    pub stream_id: u64,
    /// The part number, as in the playlist (`part<n>`). For an init object,
    /// the part that it was sent before.
    pub part: u64,
    /// Datagrams that the edge sent for this object: source and repair. A
    /// client measures its loss from it. Set by [`encode_object`].
    pub datagrams: u32,
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
        let stream_id = header.get_u64();
        let part = header.get_u64();
        let available_unix_us = header.get_u64();
        let published_unix_us = header.get_u64();
        Ok(Self {
            kind,
            datagrams,
            stream_id,
            part,
            available_unix_us,
            published_unix_us,
            payload: object.slice(OBJECT_HEADER_LEN..),
        })
    }
}

/// Block id of a part: its number, modulo 2^32, with the init objects in a
/// separate half of the space so that an init never shares a block id with
/// a part that is still in flight.
pub fn block_id(kind: ObjectKind, part: u64) -> u32 {
    let low = (part & 0x7fff_ffff) as u32;
    match kind {
        ObjectKind::Init => low | 0x8000_0000,
        ObjectKind::Part | ObjectKind::Gap => low,
    }
}

/// Encode one object into the datagrams that carry it.
pub fn encode_object(
    object: &PartObject,
    symbol_size: u16,
    repair: RepairPolicy,
) -> Result<Vec<Vec<u8>>, raptorq_datagram_fec::DatagramFecError> {
    let symbol_size = symbol_size.max(16);
    let length = OBJECT_HEADER_LEN + object.payload.len();
    let source_symbols = length.div_ceil(usize::from(symbol_size)).max(1);
    let repair_symbols = repair.repair_symbols(source_symbols as u32);
    let bytes = PartObject {
        datagrams: (source_symbols as u32).saturating_add(repair_symbols),
        ..object.clone()
    }
    .encode();
    let source_symbols_u16 = u16::try_from(source_symbols).unwrap_or(u16::MAX);
    let mut encoder = DatagramFecEncoder::new()
        .with_symbol_size(symbol_size)
        .with_source_symbols(source_symbols_u16)
        .with_initial_block_id(block_id(object.kind, object.part));
    encoder.encode_object_with_repair_symbols(&bytes, repair_symbols)
}

/// Datagrams of one block that a client has seen.
#[derive(Debug, Clone, Copy)]
struct BlockCount {
    received: u32,
    /// Known once the block is decoded.
    sent: Option<u32>,
    first_seen: Instant,
}

/// The receiving half of a client: datagrams in, objects out.
#[derive(Debug, Default)]
pub struct ObjectReceiver {
    decoder: DatagramFecDecoder,
    blocks: HashMap<u32, BlockCount>,
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
}

/// A block is counted for loss once its datagrams have had this long to
/// arrive.
const LOSS_SETTLE: Duration = Duration::from_millis(500);

impl ObjectReceiver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Push one media datagram. Returns the object that it completed.
    pub fn push(&mut self, datagram: &[u8]) -> Option<PartObject> {
        self.datagrams += 1;
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
        let count = self.blocks.entry(header.block_id).or_insert(BlockCount {
            received: 0,
            sent: None,
            first_seen: Instant::now(),
        });
        count.received += 1;
        let seen = count.received;
        match self.decoder.push_datagram(datagram) {
            Ok(Some(bytes)) => match PartObject::decode(Bytes::from(bytes)) {
                Ok(object) => {
                    self.objects += 1;
                    if let Some(count) = self.blocks.get_mut(&header.block_id) {
                        count.sent = Some(object.datagrams);
                    }
                    if seen < u32::from(header.source_symbols) || !source_symbol {
                        // Decoded with fewer than all source symbols, or
                        // completed by a repair symbol.
                        self.recovered_objects += 1;
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

    /// Count the blocks that have settled, and return the datagram loss
    /// measured since the last call, if any datagram was due.
    pub fn take_loss(&mut self) -> Option<f64> {
        let now = Instant::now();
        let mut settled = Vec::new();
        for (block, count) in &self.blocks {
            if now.duration_since(count.first_seen) >= LOSS_SETTLE {
                settled.push(*block);
            }
        }
        for block in settled {
            let count = self.blocks.remove(&block).unwrap();
            match count.sent {
                Some(sent) => {
                    let received = count.received.min(sent);
                    self.window_received += u64::from(received);
                    self.window_sent += u64::from(sent);
                    self.sent_datagrams += u64::from(sent);
                    self.lost_datagrams += u64::from(sent - received);
                }
                None => self.lost_objects += 1,
            }
        }
        if self.window_sent == 0 {
            return None;
        }
        let loss = 1.0 - self.window_received as f64 / self.window_sent as f64;
        self.window_received = 0;
        self.window_sent = 0;
        Some(loss)
    }
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
            },
            Control::Subscribe {
                stream_id: 1,
                from_part: None,
                repair: RepairPolicy {
                    permille: 500,
                    min_symbols: 4,
                },
            },
            Control::Unsubscribe { stream_id: 9 },
            Control::Subscribed {
                stream_id: 9,
                first_part: 77,
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
        assert_eq!(Control::decode(b"NTU/1 SUB 5 - 1 2 3"), None);
    }

    #[test]
    fn a_one_symbol_part_gets_the_minimum_repair_and_a_large_one_its_share() {
        let policy = RepairPolicy::default();
        assert_eq!(policy.repair_symbols(1), 3);
        assert_eq!(policy.repair_symbols(340), 85);
        assert_eq!(
            RepairPolicy {
                permille: 2_000,
                min_symbols: 99
            }
            .bounded(),
            RepairPolicy {
                permille: MAX_REPAIR_PERMILLE,
                min_symbols: MAX_MIN_REPAIR_SYMBOLS
            }
        );
    }

    fn object(part: u64, len: usize) -> PartObject {
        PartObject {
            kind: ObjectKind::Part,
            datagrams: 0,
            stream_id: 1,
            part,
            available_unix_us: 7,
            published_unix_us: 5,
            payload: Bytes::from((0..len).map(|i| (i * 31 + part as usize) as u8).collect::<Vec<_>>()),
        }
    }

    #[test]
    fn a_part_is_recovered_bit_for_bit_with_any_loss_within_its_repair() {
        // A 4K video part of 400 kB and a 5 ms audio part of 80 bytes.
        for (part, len) in [(10_u64, 400_000_usize), (11, 80)] {
            let original = object(part, len);
            let datagrams =
                encode_object(&original, DEFAULT_SYMBOL_SIZE, RepairPolicy::default()).unwrap();
            let source = len.div_ceil(DEFAULT_SYMBOL_SIZE as usize).max(1);
            let repair = datagrams.len() - source;
            assert_eq!(repair as u32, RepairPolicy::default().repair_symbols(source as u32));
            // Drop as many datagrams as there are repair symbols, spread over
            // the source symbols.
            let mut receiver = ObjectReceiver::new();
            let mut decoded = None;
            for (index, datagram) in datagrams.iter().enumerate() {
                if index % (datagrams.len() / repair).max(1) == 0 && index / (datagrams.len() / repair).max(1) < repair {
                    continue;
                }
                if let Some(object) = receiver.push(datagram) {
                    decoded = Some(object);
                }
            }
            let decoded = decoded.expect("object decoded");
            assert_eq!(decoded.datagrams as usize, datagrams.len());
            assert_eq!(PartObject { datagrams: 0, ..decoded }, original);
            assert_eq!(receiver.recovered_objects, 1);
        }
    }

    #[test]
    fn repair_grows_with_the_measured_loss() {
        let base = RepairPolicy::default();
        assert_eq!(RepairPolicy::for_loss(0.0, base), base);
        let one = RepairPolicy::for_loss(0.01, base);
        let five = RepairPolicy::for_loss(0.05, base);
        assert!(one.min_symbols >= base.min_symbols);
        assert!(five.min_symbols > one.min_symbols);
        // A one-symbol part on a 5 % path is lost with a probability below
        // the target, with the estimate taken at face value.
        assert!(0.05_f64.powi(five.min_symbols as i32 + 1) <= TARGET_PART_LOSS);
        assert!(five.permille >= 150);
    }

    #[test]
    fn a_receiver_measures_the_loss_of_its_path() {
        let mut receiver = ObjectReceiver::new();
        for part in 0..200_u64 {
            let datagrams =
                encode_object(&object(part, 3_000), DEFAULT_SYMBOL_SIZE, RepairPolicy::default())
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
        let loss = receiver.take_loss().unwrap();
        assert!((0.08..0.12).contains(&loss), "{loss}");
        assert_eq!(receiver.objects, 200);
    }

    #[test]
    fn init_and_part_objects_do_not_share_a_block() {
        assert_ne!(block_id(ObjectKind::Init, 5), block_id(ObjectKind::Part, 5));
        assert_eq!(block_id(ObjectKind::Gap, 5), block_id(ObjectKind::Part, 5));
    }
}
