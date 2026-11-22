//! The compact client lane (low-latency wave 5, W1b): a part that fits one
//! datagram goes to a client as that one datagram, without RaptorQ, and is
//! protected by repair across recent parts and by NACK.
//!
//! The RaptorQ lane of `client_udp` sends each part as its own block with
//! its own repair: three repair symbols (and a 32-byte RQD2 header and a
//! 46-byte object header) for every 2.5 ms Opus part made about 4 KB of
//! datagrams for a part of a hundred bytes. On the compact lane:
//!
//! - A part is one datagram of an 11-byte header and the LL-HLS part bytes
//!   ([`encode_part`]): a byte of magic and kind, the flags, the source
//!   generation (see below), and the part number and the publication time
//!   in 32 bits each (a client extends them, see [`extend_u32`]). The UDP
//!   length is the payload's length and the UDP checksum covers the bytes;
//!   only a part recovered from window repair carries its length and a
//!   CRC-32, inside the repair ([`encode_window_repair`]). A gap (a part the
//!   edge never had) is the same header with no payload. With IPv4 and UDP
//!   (28 bytes), a 96-byte Opus part is 135 bytes on the wire, 1.41 times
//!   the part: one datagram per 2.5 ms part cannot come near 1.2 times, the
//!   IP and UDP headers alone are 29 % of it.
//! - The generation byte is a digest of the contributor's source epoch: a
//!   contributor restart counts its part numbers again, and a client that
//!   sees a new generation forgets the old one's parts
//!   ([`CompactReceiver::push`]), so that it NACKs the new parts and does
//!   not drop them as duplicates. 0: unknown.
//! - Per-part copies: a client on a lossy path asks for `copies` more sends
//!   of each part, back to back, so that a part lost on a random-loss path
//!   still arrives at once (its latency stays the latency of the path).
//! - Window repair: every `window` parts (aligned to the part number), the
//!   edge sends `window_repair` repair datagrams over the last `window`
//!   parts ([`encode_window_repair`]): rows of a systematic Cauchy
//!   Reed-Solomon code over GF(256), so any `r` losses among the window's
//!   parts are recovered from `r` repair datagrams. On a clean path one repair
//!   datagram for 32 parts costs about 3 %.
//! - NACK: a client that finds a hole asks the edge for the part (see
//!   `client_udp::Control::Nack`); the edge sends it again from its cache,
//!   marked as a retransmission. A burst that no repair covers is repaired in
//!   a round trip.
//!
//! Which of these a client gets is its [`CompactPlan`], which it sizes from
//! the loss and the burst length that it measures on its own path
//! ([`CompactPlan::for_measured`]): nothing but the window repair on a clean
//! path.
//!
//! The window repair needs the same bytes for a part at the edge and at the
//! client, whatever sent it: the copies count and the retransmission bit
//! (the `flags` byte) are not part of a datagram's canonical form
//! ([`canonical`]) nor of its CRC.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// The first byte of a compact datagram, less its kind (the low two bits):
/// not an ASCII byte (the controls are text) and not `R` (RQD2).
pub const MAGIC: u8 = 0xC0;
const MAGIC_MASK: u8 = 0xFC;
pub const HEADER_LEN: usize = 11;
/// The largest compact datagram: under 1,280 bytes (the smallest IPv6 path
/// MTU) with its IP and UDP headers, as every client datagram is.
pub const MAX_DATAGRAM: usize = 1_200;
/// The largest part that is sent compact.
pub const MAX_PAYLOAD: usize = MAX_DATAGRAM - HEADER_LEN;
pub const MAX_COPIES: u8 = 3;
pub const MIN_WINDOW: u8 = 4;
pub const MAX_WINDOW: u8 = 64;
pub const MAX_WINDOW_REPAIR: u8 = 8;
/// The bytes of IPv4 and UDP headers that every datagram costs on the wire.
pub const IP_UDP_HEADER_BYTES: usize = 28;

const FLAGS_AT: usize = 1;
const GENERATION_AT: usize = 2;
const PART_AT: usize = 3;
const FIELD_AT: usize = 7;
/// The length and CRC-32 that a part carries inside window repair.
const REPAIR_PREFIX: usize = 6;
const FLAG_COPIES: u8 = 0x07;
const FLAG_RETRANSMIT: u8 = 0x08;
/// The edge's fast lane sent the datagram (its class thread, from the
/// symbol as it arrived), not the cache-fed lane: a client tells the lanes
/// apart for its report. Not part of the canonical form, like every flag.
const FLAG_FAST_LANE: u8 = 0x10;
/// Rows of the Cauchy code: the repair of one window has at most this many.
const MAX_ROWS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Part,
    /// The edge never had this part and skipped it.
    Gap,
    /// A row of the repair of a window of parts.
    Repair,
}

impl Kind {
    fn code(self) -> u8 {
        match self {
            Self::Part => 0,
            Self::Gap => 2,
            Self::Repair => 3,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Part),
            2 => Some(Self::Gap),
            3 => Some(Self::Repair),
            _ => None,
        }
    }
}

/// What a client asks of the compact lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompactPlan {
    /// More sends of each part, right after it.
    pub copies: u8,
    /// Parts in a window of repair (aligned to the part number).
    pub window: u8,
    /// Repair datagrams for each window; 0 for none.
    pub window_repair: u8,
    /// Parts in the short form ([`shorten`], wave 7 item 9): a 7-byte
    /// header instead of 11. The client asks for it (`C1.16.2.7`); an edge
    /// without it sends the long form, which a client reads as before.
    pub short_header: bool,
}

/// The measured loss below which a path counts as clean.
pub const CLEAN_LOSS: f64 = 0.002;
/// The share of parts that may need a NACK after the copies: below 1 %, so
/// that the late ones stay out of the 99th percentile.
pub const RESIDUAL_TARGET: f64 = 0.008;

impl Default for CompactPlan {
    fn default() -> Self {
        Self::CLEAN
    }
}

impl CompactPlan {
    /// A clean path: one repair datagram for every 32 parts, NACK for the
    /// rest.
    pub const CLEAN: Self = Self {
        copies: 0,
        window: 32,
        window_repair: 1,
        short_header: false,
    };

    pub fn bounded(self) -> Self {
        Self {
            copies: self.copies.min(MAX_COPIES),
            window: self.window.clamp(MIN_WINDOW, MAX_WINDOW),
            window_repair: self.window_repair.min(MAX_WINDOW_REPAIR),
            short_header: self.short_header,
        }
    }

    /// The plan with parts in the short form, or not.
    #[must_use]
    pub const fn with_short_header(mut self, short_header: bool) -> Self {
        self.short_header = short_header;
        self
    }

    /// The word of a `SUB` that asks for the compact lane: `C1.32.1`
    /// (copies, window, window repair), and `.7` for the short form.
    pub fn word(self) -> String {
        format!(
            "C{}.{}.{}{}",
            self.copies,
            self.window,
            self.window_repair,
            if self.short_header { ".7" } else { "" }
        )
    }

    pub fn parse_word(word: &str) -> Option<Self> {
        let mut numbers = word.strip_prefix('C')?.split('.');
        let mut plan = Self {
            copies: numbers.next()?.parse().ok()?,
            window: numbers.next()?.parse().ok()?,
            window_repair: numbers.next()?.parse().ok()?,
            short_header: false,
        };
        match numbers.next() {
            None => {}
            Some(header) if header.parse::<usize>().ok()? == SHORT_HEADER_LEN => {
                plan.short_header = true;
            }
            Some(_) => return None,
        }
        if numbers.next().is_some() {
            return None;
        }
        Some(plan.bounded())
    }

    /// The plan for a path that loses `loss` of the datagrams, in losses of
    /// `burst` compact datagrams in a row on average (copies included, see
    /// [`CompactReceiver::take_burst`]). Copies keep a random loss from
    /// delaying a part (until fewer than [`RESIDUAL_TARGET`] of the parts
    /// would need a NACK). A part's copies go out right behind it, so a
    /// burst takes them with it: a bursty path gets no copies, and window
    /// repair rows for the burst instead (a burst of `b` parts in a window
    /// is recovered from `b` rows).
    pub fn for_measured(loss: f64, burst: f64) -> Self {
        Self::for_measured_with(loss, burst, true)
    }

    /// [`Self::for_measured`], or with `burst_rows` false the plan of
    /// before (needletail-udp-client `--compact-burst-rows false`): a
    /// bursty path keeps one copy and gets at most two more rows.
    pub fn for_measured_with(loss: f64, burst: f64, burst_rows: bool) -> Self {
        if !(loss >= CLEAN_LOSS) {
            return Self::CLEAN;
        }
        let planned = (loss * 1.5 + 0.002).min(0.3);
        let window = 16_u8;
        if burst_rows && burst >= 2.0 {
            let expected = planned * f64::from(window);
            let rows = 1 + expected.ceil() as u8 + (burst.ceil() as u8).saturating_sub(1).min(4);
            return Self {
                copies: 0,
                window,
                window_repair: rows.min(MAX_WINDOW_REPAIR),
                short_header: false,
            }
            .bounded();
        }
        let mut copies = 0_u8;
        let mut residual = planned;
        while residual > RESIDUAL_TARGET && copies < MAX_COPIES {
            copies += 1;
            residual *= planned;
        }
        let bursty = burst >= 2.0;
        if bursty && copies > 1 {
            copies = 1;
        }
        let expected = residual * f64::from(window);
        let mut window_repair = 1 + expected.ceil() as u8;
        if bursty {
            window_repair += ((burst.ceil() as u8).saturating_sub(1)).min(2);
        }
        Self {
            copies,
            window,
            window_repair: window_repair.min(4),
            short_header: false,
        }
        .bounded()
    }
}

/// The loss estimate that a compact plan follows: an average of the
/// samples (one each 150 ms), the first sample as it is.
pub fn smoothed_loss(previous: Option<f64>, sample: f64) -> f64 {
    match previous {
        Some(previous) => previous * 0.75 + sample * 0.25,
        None => sample,
    }
}

/// `low` (the low 32 bits of a number) extended to the 64-bit number
/// nearest to `reference`.
pub fn extend_u32(reference: u64, low: u32) -> u64 {
    let candidate = (reference & !0xffff_ffff) | u64::from(low);
    let span = 1_u64 << 32;
    let options = [
        candidate.checked_sub(span),
        Some(candidate),
        candidate.checked_add(span),
    ];
    options
        .into_iter()
        .flatten()
        .min_by_key(|value| value.abs_diff(reference))
        .unwrap_or(candidate)
}

/// The generation byte of a source epoch: a digest of all its bytes,
/// never 0 (which a datagram of an unknown epoch carries).
pub fn generation_of_epoch(epoch: u64) -> u8 {
    let folded = epoch
        .to_le_bytes()
        .iter()
        .fold(0_u8, |acc, byte| acc.rotate_left(3) ^ byte);
    if folded == 0 {
        1
    } else {
        folded
    }
}

fn encode(kind: Kind, part_low: u32, field: [u8; 4], payload: &[u8]) -> Vec<u8> {
    let mut datagram = Vec::with_capacity(HEADER_LEN + payload.len());
    datagram.push(MAGIC | kind.code());
    datagram.push(0);
    datagram.push(0);
    datagram.extend_from_slice(&part_low.to_be_bytes());
    datagram.extend_from_slice(&field);
    datagram.extend_from_slice(payload);
    datagram
}

/// The compact datagram of a part, in its canonical form (no copies, not a
/// retransmission, generation 0 until [`set_generation`]); `None` when the
/// part does not fit one datagram.
pub fn encode_part(part: u64, published_unix_us: u64, payload: &[u8]) -> Option<Vec<u8>> {
    if payload.len() > MAX_PAYLOAD {
        return None;
    }
    Some(encode(
        Kind::Part,
        part as u32,
        (published_unix_us as u32).to_be_bytes(),
        payload,
    ))
}

/// The compact datagram of a gap.
pub fn encode_gap(part: u64) -> Vec<u8> {
    encode(Kind::Gap, part as u32, [0; 4], &[])
}

/// Mark a datagram with the source generation of its part (see
/// [`generation_of_epoch`]).
pub fn set_generation(datagram: &mut [u8], generation: u8) {
    if datagram.len() > GENERATION_AT {
        datagram[GENERATION_AT] = generation;
    }
}

/// Mark a datagram with the copies sent of it and as a retransmission (or
/// not); they are not part of its canonical form.
pub fn set_flags(datagram: &mut [u8], copies: u8, retransmit: bool) {
    if datagram.len() > FLAGS_AT {
        datagram[FLAGS_AT] = (copies & FLAG_COPIES) | if retransmit { FLAG_RETRANSMIT } else { 0 };
    }
}

/// Mark a datagram as sent by the edge's fast lane (after [`set_flags`],
/// which clears the mark). Clients before the mark ignore the bit.
pub fn set_fast_lane(datagram: &mut [u8]) {
    if datagram.len() > FLAGS_AT {
        datagram[FLAGS_AT] |= FLAG_FAST_LANE;
    }
}

/// A datagram with its flags cleared: the bytes that the window repair
/// covers.
pub fn canonical(datagram: &[u8]) -> Vec<u8> {
    let mut canonical = datagram.to_vec();
    set_flags(&mut canonical, 0, false);
    canonical
}

pub fn is_compact(datagram: &[u8]) -> bool {
    datagram.len() >= HEADER_LEN && datagram[0] & MAGIC_MASK == MAGIC
}

/// Whether a compact datagram is a gap: a part the edge did not have when
/// it sent it, which the part itself replaces if the edge gets it later.
pub fn is_gap(datagram: &[u8]) -> bool {
    is_compact(datagram) && datagram[0] & !MAGIC_MASK == Kind::Gap.code()
}

// ------------------------------------------------------- the short form

/// The first byte of a part in the short form (wave 7 item 9), less its
/// flags (the low five bits): 0xE0 to 0xFF, which is neither a long compact
/// datagram (0xC0 to 0xC3), a control (text) nor RQD2 (`R`).
pub const SHORT_MAGIC: u8 = 0xE0;
const SHORT_MAGIC_MASK: u8 = 0xE0;
/// The short form's header: the magic and the flags, the generation, the
/// part number's low 16 bits and the publication time's low 24 bits (in
/// microseconds: 16.7 s).
pub const SHORT_HEADER_LEN: usize = 7;
/// What the short form saves on each part.
pub const SHORT_SAVES: usize = HEADER_LEN - SHORT_HEADER_LEN;

/// Every part whose number is a multiple of this keeps the long form, with
/// its 32 bits of publication time: a client learns the high 8 bits of the
/// short form's 24 from it, so its own clock need not agree with the
/// edge's (wave 7 review). At 1 ms parts, one long part every 256 ms
/// (4 bytes more per 256 parts).
pub const LONG_FORM_EVERY: u64 = 256;

/// Whether a part keeps the long form ([`LONG_FORM_EVERY`]). The low 32
/// bits of its number say it.
#[must_use]
pub const fn keeps_long_form(part_low: u64) -> bool {
    part_low % LONG_FORM_EVERY == 0
}

/// Whether a datagram is a part in the short form.
pub fn is_short(datagram: &[u8]) -> bool {
    datagram.len() >= SHORT_HEADER_LEN && datagram[0] & SHORT_MAGIC_MASK == SHORT_MAGIC
}

/// The short form of a part's datagram (marked or not: its flags go with
/// it); `None` for a gap or a repair datagram, which keep the long form,
/// and for a part that keeps it ([`keeps_long_form`]).
pub fn shorten(datagram: &[u8]) -> Option<Vec<u8>> {
    let mut short = Vec::with_capacity(datagram.len() - SHORT_SAVES);
    shorten_into(datagram, &mut short)?;
    Some(short)
}

/// [`shorten`], into `out` (cleared first).
pub fn shorten_into(datagram: &[u8], out: &mut Vec<u8>) -> Option<()> {
    let view = parse(datagram)?;
    if view.kind != Kind::Part || keeps_long_form(u64::from(view.part_low)) {
        return None;
    }
    out.clear();
    out.push(SHORT_MAGIC | (datagram[FLAGS_AT] & !SHORT_MAGIC_MASK));
    out.push(view.generation);
    out.extend_from_slice(&(view.part_low as u16).to_be_bytes());
    out.extend_from_slice(&view.field[1..4]);
    out.extend_from_slice(view.payload);
    Some(())
}

/// The short form, in place in `buffer` (a part's datagram of `len` bytes
/// at its start): the new length, or `None` if it is not a part.
pub fn shorten_in_place(buffer: &mut [u8], len: usize) -> Option<usize> {
    let datagram = buffer.get(..len)?;
    let view = parse(datagram)?;
    if view.kind != Kind::Part || keeps_long_form(u64::from(view.part_low)) {
        return None;
    }
    let flags = datagram[FLAGS_AT] & !SHORT_MAGIC_MASK;
    let generation = view.generation;
    let part = (view.part_low as u16).to_be_bytes();
    let field = view.field;
    buffer.copy_within(HEADER_LEN..len, SHORT_HEADER_LEN);
    buffer[0] = SHORT_MAGIC | flags;
    buffer[1] = generation;
    buffer[2..4].copy_from_slice(&part);
    buffer[4..7].copy_from_slice(&field[1..4]);
    Some(len - SHORT_SAVES)
}

/// `low`, the low `bits` bits of a number, extended to the number nearest
/// to `reference`.
pub fn extend_bits(reference: u64, low: u64, bits: u32) -> u64 {
    let span = 1_u64 << bits;
    let candidate = (reference & !(span - 1)) | (low & (span - 1));
    [
        candidate.checked_sub(span),
        Some(candidate),
        candidate.checked_add(span),
    ]
    .into_iter()
    .flatten()
    .min_by_key(|value| value.abs_diff(reference))
    .unwrap_or(candidate)
}

/// The long form of a part in the short form: its part number extended to
/// the one nearest `reference_part`, its publication time to the one
/// nearest `now_unix_us` (which must be within 8 s of the edge's time: a
/// client takes it from the last long-form part, see
/// [`CompactReceiver::push`]), its flags as they came. The long form is
/// what window repair covers, so a client keeps it.
pub fn lengthen(short: &[u8], reference_part: u64, now_unix_us: u64) -> Option<Vec<u8>> {
    if !is_short(short) {
        return None;
    }
    let part = extend_bits(
        reference_part,
        u64::from(u16::from_be_bytes([short[2], short[3]])),
        16,
    );
    let published = extend_bits(
        now_unix_us,
        u64::from(u32::from_be_bytes([0, short[4], short[5], short[6]])),
        24,
    );
    let mut datagram = encode_part(part, published, &short[SHORT_HEADER_LEN..])?;
    set_generation(&mut datagram, short[1]);
    datagram[FLAGS_AT] = short[0] & !SHORT_MAGIC_MASK;
    Some(datagram)
}

/// A compact datagram, checked.
#[derive(Debug, Clone, Copy)]
pub struct View<'a> {
    pub kind: Kind,
    pub copies: u8,
    pub retransmit: bool,
    /// The edge's fast lane sent it ([`set_fast_lane`]).
    pub fast_lane: bool,
    /// The source generation of the part (0: unknown).
    pub generation: u8,
    pub part_low: u32,
    /// The publication time's low 32 bits (a part), or the window, row and
    /// rows of a repair datagram.
    pub field: [u8; 4],
    pub payload: &'a [u8],
}

/// Parse a compact datagram: its payload is the rest of it. `None` if it
/// is not one.
pub fn parse(datagram: &[u8]) -> Option<View<'_>> {
    if !is_compact(datagram) {
        return None;
    }
    let kind = Kind::from_code(datagram[0] & !MAGIC_MASK)?;
    let flags = datagram[FLAGS_AT];
    Some(View {
        kind,
        copies: flags & FLAG_COPIES,
        retransmit: flags & FLAG_RETRANSMIT != 0,
        fast_lane: flags & FLAG_FAST_LANE != 0,
        generation: datagram[GENERATION_AT],
        part_low: u32::from_be_bytes(datagram[PART_AT..PART_AT + 4].try_into().ok()?),
        field: datagram[FIELD_AT..FIELD_AT + 4].try_into().ok()?,
        payload: &datagram[HEADER_LEN..],
    })
}

/// The symbol that window repair codes for a part: its canonical datagram
/// behind its length and its CRC-32, so that a recovered part (zero padded
/// to the repair's length) is cut to its length and checked.
fn repair_symbol(canonical: &[u8]) -> Vec<u8> {
    let mut symbol = Vec::with_capacity(REPAIR_PREFIX + canonical.len());
    symbol.extend_from_slice(&(canonical.len() as u16).to_be_bytes());
    symbol.extend_from_slice(&crc32fast::hash(canonical).to_be_bytes());
    symbol.extend_from_slice(canonical);
    symbol
}

/// The canonical datagram in a recovered repair symbol, if its length and
/// CRC hold (else empty: not a datagram).
fn from_repair_symbol(symbol: &[u8]) -> Vec<u8> {
    let Some(prefix) = symbol.get(..REPAIR_PREFIX) else {
        return Vec::new();
    };
    let len = usize::from(u16::from_be_bytes([prefix[0], prefix[1]]));
    let stated = u32::from_be_bytes([prefix[2], prefix[3], prefix[4], prefix[5]]);
    match symbol.get(REPAIR_PREFIX..REPAIR_PREFIX + len) {
        Some(canonical) if crc32fast::hash(canonical) == stated => canonical.to_vec(),
        _ => Vec::new(),
    }
}

// ------------------------------------------------------------------ GF(256)

struct Gf {
    exp: [u8; 512],
    log: [u8; 256],
}

impl Gf {
    const fn new() -> Self {
        let mut exp = [0_u8; 512];
        let mut log = [0_u8; 256];
        let mut x: u16 = 1;
        let mut i = 0;
        while i < 255 {
            exp[i] = x as u8;
            log[x as usize] = i as u8;
            x <<= 1;
            if x & 0x100 != 0 {
                x ^= 0x11d;
            }
            i += 1;
        }
        while i < 512 {
            exp[i] = exp[i - 255];
            i += 1;
        }
        Self { exp, log }
    }

    fn mul(&self, a: u8, b: u8) -> u8 {
        if a == 0 || b == 0 {
            0
        } else {
            self.exp[usize::from(self.log[usize::from(a)]) + usize::from(self.log[usize::from(b)])]
        }
    }

    fn inv(&self, a: u8) -> u8 {
        debug_assert_ne!(a, 0);
        self.exp[255 - usize::from(self.log[usize::from(a)])]
    }

    /// `dst += c * src` over GF(256), `src` read as if padded with zeros.
    fn mul_add(&self, dst: &mut [u8], src: &[u8], c: u8) {
        if c == 0 {
            return;
        }
        let len = src.len().min(dst.len());
        if c == 1 {
            for (d, s) in dst[..len].iter_mut().zip(&src[..len]) {
                *d ^= s;
            }
            return;
        }
        let log_c = usize::from(self.log[usize::from(c)]);
        for (d, s) in dst[..len].iter_mut().zip(&src[..len]) {
            if *s != 0 {
                *d ^= self.exp[log_c + usize::from(self.log[usize::from(*s)])];
            }
        }
    }
}

static GF: Gf = Gf::new();

/// The coefficient of the window's `column`-th part in repair row `row`: a
/// Cauchy matrix (1 / (x_row + y_column), x_row = row, y_column = 16 +
/// column), every square submatrix of which is invertible, so that any `r`
/// rows recover any `r` lost parts.
fn coefficient(row: u8, column: usize) -> u8 {
    GF.inv(row ^ (MAX_ROWS + column) as u8)
}

/// The repair datagrams (rows `0..rows`) of the window of parts that starts
/// at `first_part`: `datagrams` are the canonical datagrams of its parts, in
/// order. The rows code each one behind its length and CRC-32.
pub fn encode_window_repair(first_part: u64, datagrams: &[&[u8]], rows: u8) -> Vec<Vec<u8>> {
    let k = datagrams.len();
    let rows = usize::from(rows).min(MAX_ROWS);
    if k == 0 || k > usize::from(MAX_WINDOW) || rows == 0 {
        return Vec::new();
    }
    let symbols: Vec<Vec<u8>> = datagrams
        .iter()
        .map(|datagram| repair_symbol(datagram))
        .collect();
    let t = symbols.iter().map(Vec::len).max().unwrap_or(0);
    // The rows are of the parts' generation.
    let generation = datagrams
        .first()
        .and_then(|datagram| datagram.get(GENERATION_AT))
        .copied()
        .unwrap_or(0);
    (0..rows)
        .map(|row| {
            let mut parity = vec![0_u8; t];
            for (column, symbol) in symbols.iter().enumerate() {
                GF.mul_add(&mut parity, symbol, coefficient(row as u8, column));
            }
            let mut datagram = encode(
                Kind::Repair,
                first_part as u32,
                [k as u8, row as u8, rows as u8, 0],
                &parity,
            );
            set_generation(&mut datagram, generation);
            datagram
        })
        .collect()
}

/// A window repair datagram as sent to a client that gets `rows` rows of
/// the window (the edge encodes the most that any client asks for once):
/// the rows it states, which the client's loss count reads, rewritten.
pub fn with_rows_sent(datagram: &[u8], rows: u8) -> Vec<u8> {
    let mut datagram = datagram.to_vec();
    if is_compact(&datagram) && datagram[0] & !MAGIC_MASK == Kind::Repair.code() {
        datagram[FIELD_AT + 2] = rows;
    }
    datagram
}

/// Recover the missing parts of a window of `k` parts from repair rows of
/// `t` bytes. `known[i]` is the canonical datagram of the window's `i`-th
/// part, if the client has it. Returns each recovered part's index and its
/// canonical datagram (empty if its length or CRC fails: the edge's bytes
/// of a part differ from the client's); `None` if more parts are missing
/// than there are rows (or none is).
pub fn recover_window(
    known: &[Option<&[u8]>],
    t: usize,
    rows: &[(u8, &[u8])],
) -> Option<Vec<(usize, Vec<u8>)>> {
    let symbols: Vec<Option<Vec<u8>>> = known
        .iter()
        .map(|datagram| datagram.map(repair_symbol))
        .collect();
    let known: Vec<Option<&[u8]>> = symbols.iter().map(|symbol| symbol.as_deref()).collect();
    let recovered = recover_symbols(&known, t, rows)?;
    Some(
        recovered
            .into_iter()
            .map(|(index, symbol)| (index, from_repair_symbol(&symbol)))
            .collect(),
    )
}

/// [`recover_window`] on the repair symbols: each recovered symbol zero
/// padded to `t` bytes.
fn recover_symbols(
    known: &[Option<&[u8]>],
    t: usize,
    rows: &[(u8, &[u8])],
) -> Option<Vec<(usize, Vec<u8>)>> {
    let missing: Vec<usize> = known
        .iter()
        .enumerate()
        .filter(|(_, symbol)| symbol.is_none())
        .map(|(index, _)| index)
        .collect();
    let m = missing.len();
    if m == 0 || m > rows.len() || known.len() > usize::from(MAX_WINDOW) {
        return None;
    }
    let rows = &rows[..m];
    // a x = b, where b is each row less the parts the client has.
    let mut a: Vec<Vec<u8>> = rows
        .iter()
        .map(|(row, _)| {
            missing
                .iter()
                .map(|column| coefficient(*row, *column))
                .collect()
        })
        .collect();
    let mut b: Vec<Vec<u8>> = rows
        .iter()
        .map(|(row, parity)| {
            let mut rhs = vec![0_u8; t];
            let len = parity.len().min(t);
            rhs[..len].copy_from_slice(&parity[..len]);
            for (column, symbol) in known.iter().enumerate() {
                if let Some(symbol) = symbol {
                    GF.mul_add(&mut rhs, symbol, coefficient(*row, column));
                }
            }
            rhs
        })
        .collect();
    // Gauss-Jordan: a Cauchy submatrix is never singular.
    for pivot in 0..m {
        let found = (pivot..m).find(|row| a[*row][pivot] != 0)?;
        a.swap(pivot, found);
        b.swap(pivot, found);
        let inverse = GF.inv(a[pivot][pivot]);
        for value in a[pivot].iter_mut() {
            *value = GF.mul(*value, inverse);
        }
        let scaled: Vec<u8> = b[pivot]
            .iter()
            .map(|value| GF.mul(*value, inverse))
            .collect();
        b[pivot] = scaled;
        for row in 0..m {
            if row == pivot || a[row][pivot] == 0 {
                continue;
            }
            let factor = a[row][pivot];
            let pivot_a = a[pivot].clone();
            for (value, p) in a[row].iter_mut().zip(&pivot_a) {
                *value ^= GF.mul(factor, *p);
            }
            let pivot_b = b[pivot].clone();
            GF.mul_add(&mut b[row], &pivot_b, factor);
        }
    }
    Some(missing.into_iter().zip(b).collect())
}

// ------------------------------------------------------------------ receiver

/// A part that a compact datagram, a recovery or a retransmission brought.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactPart {
    pub kind: Kind,
    pub part: u64,
    pub published_unix_us: u64,
    pub payload: Vec<u8>,
    /// It came as a retransmission (an answer to a NACK).
    pub retransmit: bool,
    /// It was recovered from a window's repair.
    pub recovered: bool,
    /// The datagram that brought it came from the edge's fast lane.
    pub fast_lane: bool,
}

/// How long a hole waits before its first NACK: the time a part's
/// cache-fed send may trail its fast-lane neighbours.
pub const NACK_DELAY: Duration = Duration::from_millis(1);
/// The time between the first two NACKs of one part, at least: a NACK is
/// not repeated before its answer can have come (one and a half round
/// trips of the client's path, see [`CompactReceiver::set_rtt`]). It
/// doubles with each NACK after, up to [`MAX_NACK_RETRY`] (or the round
/// trip's wait, if that is longer).
pub const NACK_RETRY: Duration = Duration::from_millis(5);
pub const MAX_NACK_RETRY: Duration = Duration::from_millis(100);
/// The NACK timing of before wave 6: [`NACK_DELAY`], [`NACK_RETRY`] as
/// the floor of the retry, and no NACK while the stream is silent.
///
/// Wave 6 (W4): a client whose parts come on the fast lane sees a hole as
/// soon as the next part arrives (the cache-fed lane no longer trails its
/// neighbours by a millisecond), so the first NACK waits
/// [`FAST_NACK_DELAY`]; a NACK is repeated after one and a half round trips
/// with a floor of [`FAST_NACK_RETRY`]; and when no part arrives for an
/// interval and a margin past the next one's due time (a burst on the
/// client's path takes every datagram), the client asks for the parts due
/// since, and asks again while the silence lasts, each interval at first
/// and then less often (each answer sent into the burst is lost, and the
/// last ones arrive as it ends), for at most [`SILENCE_NACK_FOR`]; the
/// part that ends the silence asks for them all again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NackTiming {
    /// The wait before the first NACK of a hole that a later part showed.
    pub delay: Duration,
    /// The floor of the wait between NACKs of a hole.
    pub retry_floor: Duration,
    /// NACK the parts due while the stream is silent.
    pub silence: bool,
}

impl NackTiming {
    pub const BEFORE: Self = Self {
        delay: NACK_DELAY,
        retry_floor: NACK_RETRY,
        silence: false,
    };
}

impl Default for NackTiming {
    fn default() -> Self {
        Self {
            delay: FAST_NACK_DELAY,
            retry_floor: FAST_NACK_RETRY,
            silence: true,
        }
    }
}

/// The first NACK of a hole on the fast lane (see [`NackTiming`]).
pub const FAST_NACK_DELAY: Duration = Duration::from_micros(250);
/// The floor of the NACK retry on the fast lane (see [`NackTiming`]).
pub const FAST_NACK_RETRY: Duration = Duration::from_millis(2);
/// A silence is NACKed for at most this long: a longer one is a stall of
/// the stream (or of the path) that NACKs every interval would only load
/// the edge with; the parts' holes are then asked for as they show.
pub const SILENCE_NACK_FOR: Duration = Duration::from_millis(50);
/// The parts ahead of the last one that a silence asks for, at most.
pub const SILENCE_NACK_PARTS: u64 = 32;
/// The margin past the next part's due time before a silence is NACKed:
/// at least this, and at least one part interval.
const SILENCE_MARGIN: Duration = Duration::from_micros(500);
/// A shorter gap between two parts' arrivals (parts read together) is no
/// sample of the interval between parts.
const MIN_INTERVAL_SAMPLE: Duration = Duration::from_micros(50);
/// The wait between NACKs during a silence: at least this, and at least
/// one part interval.
const SILENCE_RETRY: Duration = Duration::from_millis(1);
/// The NACKs of one hole during a silence, at most: 0, 1, 3, 5, 9 and 13
/// intervals after it is first asked for. 32 holes of a silence then ask
/// for 192 parts, under the edge's NACK budget of 200 parts in 100 ms.
const SILENCE_NACKS_PER_HOLE: u16 = 6;

/// A hole is given up after this long, or after this many NACKs.
pub const HOLE_GIVE_UP: Duration = Duration::from_millis(1_000);
pub const MAX_NACKS_PER_HOLE: u8 = 12;
/// A jump of more parts than this is a resume elsewhere, not holes.
pub const MAX_HOLE_RUN: u64 = 256;
/// The most gaps that are asked for again at once: a gap is a part that
/// the edge did not have when it was due (its hole outlived the repair
/// horizon); its cache may fill it a moment later, and a NACK then brings
/// the part. Gaps past this many (an outage) are final.
pub const MAX_GAP_HOLES: usize = 64;
/// Parts that the receiver remembers (for windows and duplicates).
const MEMORY: usize = 4_096;
/// A part, or a window, is counted for loss once its datagrams have had
/// this long to arrive.
const SETTLE: Duration = Duration::from_millis(150);

#[derive(Debug, Clone, Copy)]
struct Hole {
    since: Instant,
    next_nack: Instant,
    nacks: u8,
    /// When the last NACK of it went (a retransmission after the first
    /// one measures the round trip).
    last_nack: Option<Instant>,
    /// It was delivered as a gap: the NACK asks for the part in case the
    /// edge has it now.
    gap: bool,
    /// NACKs of it sent during a silence (not counted against
    /// [`MAX_NACKS_PER_HOLE`]; at most [`SILENCE_NACKS_PER_HOLE`]).
    silence_nacks: u16,
}

#[derive(Debug)]
struct WindowState {
    k: u8,
    t: usize,
    rows: Vec<(u8, Vec<u8>)>,
    seen_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct Count {
    received: u8,
    sent: u8,
    at: Instant,
}

/// Counters of a compact receiver.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CompactStats {
    pub datagrams: u64,
    pub parts: u64,
    pub gaps: u64,
    /// Copies and retransmissions of parts already delivered.
    pub duplicates: u64,
    pub retransmits: u64,
    pub repair_datagrams: u64,
    /// Parts recovered from window repair.
    pub recovered: u64,
    pub errors: u64,
    /// Parts asked for again (each NACK of each part).
    pub nacked: u64,
    /// Holes given up.
    pub abandoned: u64,
    /// New source generations (contributor restarts) seen, and datagrams
    /// of an older generation dropped.
    pub generation_changes: u64,
    pub stale_generation: u64,
}

/// The receiving side of the compact lane.
#[derive(Debug, Default)]
pub struct CompactReceiver {
    /// The edge sends this client the compact lane: holes are tracked for
    /// NACK and counted as loss (see [`Self::expect_from`]).
    active: bool,
    reference: Option<u64>,
    known: HashMap<u64, Vec<u8>>,
    known_order: VecDeque<u64>,
    delivered: HashSet<u64>,
    delivered_order: VecDeque<u64>,
    windows: BTreeMap<u64, WindowState>,
    highest: Option<u64>,
    holes: BTreeMap<u64, Hole>,
    counts: BTreeMap<u64, Count>,
    window_counts: HashMap<u64, Count>,
    last_copies: u8,
    run: u64,
    burst_sum: u64,
    burst_runs: u64,
    window_received: u64,
    window_sent: u64,
    /// Parts delivered as gaps: the part itself, if it comes, is delivered
    /// after its gap.
    gapped: BTreeSet<u64>,
    /// The round trip of the client's path to its edge, if measured.
    rtt: Option<Duration>,
    /// When the highest part last advanced, and the interval between
    /// parts (an average of the originals' arrivals).
    highest_at: Option<Instant>,
    interval: Option<Duration>,
    /// The mean deviation of the originals' spacing from the interval (as
    /// RFC 3550's jitter): the silence's margin covers four of it.
    jitter: Duration,
    /// See [`NackTiming`]; `None` is the default.
    timing: Option<NackTiming>,
    /// The `highest_at` whose silence has been noted: the silence's own
    /// holes wake the client after that.
    silence_noted: Option<Instant>,
    /// The source generation of the parts, once a datagram named one, and
    /// the generations before it (their late datagrams are dropped).
    generation: Option<u8>,
    old_generations: VecDeque<u8>,
    /// The publication time of the last long-form part (its 32 bits,
    /// extended by the local clock) and when it arrived: the reference of
    /// the short form's 24 bits, advanced by the time since.
    time_anchor: Option<(u64, Instant)>,
    pub stats: CompactStats,
}

impl CompactReceiver {
    pub fn new() -> Self {
        Self::default()
    }

    /// The part that the edge will send first (its `OK`): the reference
    /// for the 32-bit part numbers until a part arrives.
    pub fn expect_from(&mut self, part: u64, compact: bool) {
        self.reference = Some(part);
        self.active |= compact;
        // Parts that came before this OK (an earlier OK was lost and the
        // subscription asked again): the ones from the part it names up to
        // the highest that came are holes, to ask for (wave 7 gate: under
        // bursts a client whose OK was lost dropped the short parts that
        // came first, and never asked for them again).
        if let Some(highest) = self.highest {
            self.note_holes(part, highest, Instant::now());
        }
    }

    /// Parts `from..to` that have not come are holes (none if the run is
    /// longer than [`MAX_HOLE_RUN`], or the lane is not compact).
    fn note_holes(&mut self, from: u64, to: u64, now: Instant) {
        if !self.active || to <= from || to - from > MAX_HOLE_RUN {
            return;
        }
        let delay = self.nack_timing().delay;
        for hole in from..to {
            if self.delivered.contains(&hole) {
                continue;
            }
            self.holes.entry(hole).or_insert(Hole {
                since: now,
                next_nack: now + delay,
                nacks: 0,
                last_nack: None,
                gap: false,
                silence_nacks: 0,
            });
            self.counts.entry(hole).or_insert(Count {
                received: 0,
                sent: 1 + self.last_copies,
                at: now,
            });
        }
    }

    /// Forget everything of the last edge (or of the last source epoch,
    /// whose part numbers the new one counts again).
    pub fn new_flow(&mut self) {
        self.known.clear();
        self.known_order.clear();
        self.delivered.clear();
        self.delivered_order.clear();
        self.windows.clear();
        self.highest = None;
        self.highest_at = None;
        self.silence_noted = None;
        self.holes.clear();
        self.counts.clear();
        self.window_counts.clear();
        self.gapped.clear();
        self.run = 0;
        self.time_anchor = None;
    }

    /// A round trip of the client's path to its edge (a request and its
    /// answer): NACKs are not repeated sooner than an answer can come.
    pub fn set_rtt(&mut self, sample: Duration) {
        self.rtt = Some(match self.rtt {
            Some(rtt) => (rtt * 7 + sample) / 8,
            None => sample,
        });
    }

    pub fn rtt(&self) -> Option<Duration> {
        self.rtt
    }

    /// The NACK timing (see [`NackTiming`]; the default is wave 6's).
    pub fn set_nack_timing(&mut self, timing: NackTiming) {
        self.timing = Some(timing);
    }

    pub fn nack_timing(&self) -> NackTiming {
        self.timing.unwrap_or_default()
    }

    /// The interval between parts, as measured.
    pub fn part_interval(&self) -> Option<Duration> {
        self.interval
    }

    /// The silence after the highest part past which the next parts are
    /// NACKed: one interval for the next part, and a margin.
    fn silence_after(&self) -> Option<Duration> {
        let interval = self.interval?;
        Some(interval + interval.max(SILENCE_MARGIN).max(self.jitter * 4))
    }

    /// When the stream goes silent, the parts due since the highest are
    /// holes (not loss until a later part shows them): NACKed now, and each
    /// interval while the silence lasts.
    fn note_silence(&mut self, now: Instant) {
        if !self.active || !self.nack_timing().silence {
            return;
        }
        let (Some(highest), Some(at), Some(interval), Some(after)) = (
            self.highest,
            self.highest_at,
            self.interval,
            self.silence_after(),
        ) else {
            return;
        };
        let silent = now.saturating_duration_since(at);
        if silent < after {
            return;
        }
        self.silence_noted = Some(at);
        if silent > SILENCE_NACK_FOR {
            return;
        }
        let due = 1 + ((silent - after).as_micros() / interval.as_micros().max(1)) as u64;
        for part in highest + 1..=highest + due.min(SILENCE_NACK_PARTS) {
            if self.delivered.contains(&part) {
                continue;
            }
            self.holes.entry(part).or_insert(Hole {
                since: now,
                next_nack: now,
                nacks: 0,
                last_nack: None,
                gap: false,
                silence_nacks: 0,
            });
        }
    }

    /// Whether the stream is silent now, for NACKs every interval.
    fn silent(&self, now: Instant) -> Option<Duration> {
        if !self.nack_timing().silence {
            return None;
        }
        let at = self.highest_at?;
        let after = self.silence_after()?;
        let silent = now.saturating_duration_since(at);
        (silent >= after && silent <= SILENCE_NACK_FOR)
            .then(|| self.interval.unwrap_or(SILENCE_RETRY).max(SILENCE_RETRY))
    }

    /// The wait between the first two NACKs of a hole: [`NACK_RETRY`], or
    /// one and a half round trips if that is longer.
    pub fn nack_retry(&self) -> Duration {
        let floor = self.nack_timing().retry_floor;
        self.rtt.map_or(floor, |rtt| floor.max(rtt * 3 / 2))
    }

    /// A part came as a gap: remember it, and ask for the part again a
    /// round trip later (the edge's cache may fill it).
    fn note_gap(&mut self, part: u64, now: Instant) {
        self.gapped.insert(part);
        while self.gapped.len() > MEMORY {
            self.gapped.pop_first();
        }
        let waiting = self.holes.values().filter(|hole| hole.gap).count();
        if self.active && waiting < MAX_GAP_HOLES {
            self.holes.insert(
                part,
                Hole {
                    since: now,
                    next_nack: now + self.nack_retry(),
                    nacks: 0,
                    last_nack: None,
                    gap: true,
                    silence_nacks: 0,
                },
            );
        }
    }

    /// The source generation of a datagram: a new one (the contributor
    /// restarted, and counts its parts again) starts a new flow, so that
    /// the new parts are not taken for the old ones' duplicates and their
    /// holes are NACKed; one of the last few generations is late, and
    /// `false` drops its datagram. 0 (unknown) is always taken.
    fn take_generation(&mut self, generation: u8) -> bool {
        if generation == 0 {
            return true;
        }
        match self.generation {
            Some(current) if current == generation => true,
            _ if self.old_generations.contains(&generation) => false,
            Some(current) => {
                self.old_generations.push_back(current);
                if self.old_generations.len() > 4 {
                    self.old_generations.pop_front();
                }
                self.new_flow();
                self.generation = Some(generation);
                self.stats.generation_changes += 1;
                true
            }
            None => {
                self.generation = Some(generation);
                true
            }
        }
    }

    /// A part came on a RaptorQ lane: it is delivered, and it is no compact
    /// part whose loss counts. `false` if it had been delivered.
    pub fn delivered_elsewhere(&mut self, part: u64, now: Instant) -> bool {
        let first = self.mark_delivered(part, now);
        if first {
            self.counts.remove(&part);
        }
        first
    }

    fn part_number(&self, low: u32) -> u64 {
        extend_u32(self.highest.or(self.reference).unwrap_or(0), low)
    }

    fn remember(&mut self, part: u64, datagram: Vec<u8>) {
        if self.known.insert(part, datagram).is_none() {
            self.known_order.push_back(part);
            if self.known_order.len() > MEMORY {
                if let Some(old) = self.known_order.pop_front() {
                    self.known.remove(&old);
                }
            }
        }
    }

    /// Whether `part` has been delivered (by any lane).
    pub fn is_delivered(&self, part: u64) -> bool {
        self.delivered.contains(&part)
    }

    /// A part was delivered, by this lane or another: holes before it are
    /// noted, and it is no longer one. `false` if it had been delivered.
    pub fn mark_delivered(&mut self, part: u64, now: Instant) -> bool {
        if !self.delivered.insert(part) {
            return false;
        }
        self.delivered_order.push_back(part);
        if self.delivered_order.len() > MEMORY {
            if let Some(old) = self.delivered_order.pop_front() {
                self.delivered.remove(&old);
            }
        }
        self.holes.remove(&part);
        let delay = self.nack_timing().delay;
        match self.highest {
            Some(highest) if part > highest => {
                self.highest_at = Some(now);
                if self.active && part - highest <= MAX_HOLE_RUN {
                    for hole in highest + 1..part {
                        if !self.delivered.contains(&hole) {
                            let entry = self.holes.entry(hole).or_insert(Hole {
                                since: now,
                                next_nack: now + delay,
                                nacks: 0,
                                last_nack: None,
                                gap: false,
                                silence_nacks: 0,
                            });
                            // Asked for during the silence that this part
                            // ends: the answers went into the burst, so it
                            // is asked for again now, as a new hole is.
                            if entry.silence_nacks > 0 {
                                entry.next_nack = entry.next_nack.min(now + delay);
                            }
                            self.counts.entry(hole).or_insert(Count {
                                received: 0,
                                sent: 1 + self.last_copies,
                                at: now,
                            });
                        }
                    }
                }
                self.highest = Some(part);
            }
            Some(_) => {}
            None => {
                self.highest = Some(part);
                self.highest_at = Some(now);
                // The first part after the OK: the parts from the one the OK
                // named up to it were lost on the way.
                if let Some(reference) = self.reference {
                    self.note_holes(reference, part, now);
                }
            }
        }
        true
    }

    /// Push one compact datagram; returns the parts it delivered (itself,
    /// and any that a window's repair recovered with it).
    pub fn push(&mut self, datagram: &[u8], now: Instant, now_unix_us: u64) -> Vec<CompactPart> {
        // A part in the short form is read in its long form, which is what
        // the window repair covers.
        let long;
        let datagram = if is_short(datagram) {
            // Its 16 bits of part number need a reference: the edge's OK
            // or a part. Before either, it is dropped (and asked again).
            let Some(reference) = self.highest.or(self.reference) else {
                self.stats.datagrams += 1;
                self.stats.errors += 1;
                return Vec::new();
            };
            // Its 24 bits of time are extended around the edge's clock as
            // the last long-form part gave it, not the client's own.
            let reference_us = self.time_anchor.map_or(now_unix_us, |(published, at)| {
                published + now.saturating_duration_since(at).as_micros() as u64
            });
            match lengthen(datagram, reference, reference_us) {
                Some(datagram) => {
                    long = datagram;
                    &long
                }
                None => {
                    self.stats.datagrams += 1;
                    self.stats.errors += 1;
                    return Vec::new();
                }
            }
        } else {
            if let Some(view) = parse(datagram) {
                let low = u32::from_be_bytes(view.field);
                if view.kind == Kind::Part && !view.retransmit && low != 0 {
                    self.time_anchor = Some((extend_u32(now_unix_us, low), now));
                }
            }
            datagram
        };
        self.stats.datagrams += 1;
        let Some(view) = parse(datagram) else {
            self.stats.errors += 1;
            return Vec::new();
        };
        if !self.take_generation(view.generation) {
            self.stats.stale_generation += 1;
            return Vec::new();
        }
        self.active = true;
        let part = self.part_number(view.part_low);
        let mut out = Vec::new();
        match view.kind {
            Kind::Repair => {
                self.stats.repair_datagrams += 1;
                let [k, row, rows, _] = view.field;
                if k == 0 || k > MAX_WINDOW || usize::from(row) >= MAX_ROWS {
                    self.stats.errors += 1;
                    return out;
                }
                let count = self.window_counts.entry(part).or_insert(Count {
                    received: 0,
                    sent: rows,
                    at: now,
                });
                count.received = count.received.saturating_add(1);
                let window = self.windows.entry(part).or_insert(WindowState {
                    k,
                    t: view.payload.len(),
                    rows: Vec::new(),
                    seen_at: now,
                });
                if window.k == k
                    && window.t == view.payload.len()
                    && !window.rows.iter().any(|(seen, _)| *seen == row)
                {
                    window.rows.push((row, view.payload.to_vec()));
                }
                self.try_window(part, now, now_unix_us, &mut out);
            }
            Kind::Part | Kind::Gap => {
                if view.retransmit {
                    self.stats.retransmits += 1;
                    // The answer to the only NACK of a hole: a round trip.
                    if let Some(Hole {
                        nacks: 1,
                        last_nack: Some(at),
                        silence_nacks: 0,
                        ..
                    }) = self.holes.get(&part)
                    {
                        let sample = now.saturating_duration_since(*at);
                        self.set_rtt(sample);
                    }
                } else {
                    // The interval between parts, from the originals that
                    // advance the highest part.
                    if let (Some(highest), Some(at)) = (self.highest, self.highest_at) {
                        let sample = (part > highest && part - highest <= MAX_HOLE_RUN)
                            .then(|| now.saturating_duration_since(at) / (part - highest) as u32)
                            // Parts read together say nothing of the interval.
                            .filter(|sample| *sample >= MIN_INTERVAL_SAMPLE);
                        if let Some(sample) = sample {
                            if let Some(interval) = self.interval {
                                let deviation = sample.abs_diff(interval);
                                self.jitter = (self.jitter * 15 + deviation) / 16;
                            }
                            self.interval = Some(match self.interval {
                                Some(interval) => (interval * 7 + sample) / 8,
                                None => sample,
                            });
                        }
                    }
                    self.last_copies = view.copies;
                    let count = self.counts.entry(part).or_insert(Count {
                        received: 0,
                        sent: 1 + view.copies,
                        at: now,
                    });
                    count.received = count.received.saturating_add(1);
                    count.sent = 1 + view.copies;
                }
                let canonical = canonical(&datagram[..HEADER_LEN + view.payload.len()]);
                // The part after its gap: the edge got it late. It is
                // delivered (once), and replaces the gap's bytes.
                if view.kind == Kind::Part && self.gapped.remove(&part) {
                    self.holes.remove(&part);
                    self.known.insert(part, canonical);
                    out.push(self.part_of(part, &view, now_unix_us, false));
                    self.try_windows_with(part, now, now_unix_us, &mut out);
                    return out;
                }
                if view.kind == Kind::Gap && self.delivered.contains(&part) {
                    self.stats.duplicates += 1;
                    return out;
                }
                self.remember(part, canonical);
                if !self.mark_delivered(part, now) {
                    self.stats.duplicates += 1;
                    return out;
                }
                if view.kind == Kind::Gap {
                    self.note_gap(part, now);
                }
                out.push(self.part_of(part, &view, now_unix_us, false));
                self.try_windows_with(part, now, now_unix_us, &mut out);
            }
        }
        out
    }

    fn part_of(
        &mut self,
        part: u64,
        view: &View<'_>,
        now_unix_us: u64,
        recovered: bool,
    ) -> CompactPart {
        let published_unix_us = match view.kind {
            Kind::Part => {
                let low = u32::from_be_bytes(view.field);
                if low == 0 {
                    0
                } else {
                    extend_u32(now_unix_us, low)
                }
            }
            _ => 0,
        };
        match view.kind {
            Kind::Gap => self.stats.gaps += 1,
            _ => self.stats.parts += 1,
        }
        if recovered {
            self.stats.recovered += 1;
        }
        CompactPart {
            kind: view.kind,
            part,
            published_unix_us,
            payload: view.payload.to_vec(),
            retransmit: view.retransmit,
            recovered,
            fast_lane: view.fast_lane && !recovered,
        }
    }

    /// Try the windows that hold `part`.
    fn try_windows_with(
        &mut self,
        part: u64,
        now: Instant,
        now_unix_us: u64,
        out: &mut Vec<CompactPart>,
    ) {
        let firsts: Vec<u64> = self
            .windows
            .range(part.saturating_sub(u64::from(MAX_WINDOW))..=part)
            .filter(|(first, window)| part < **first + u64::from(window.k))
            .map(|(first, _)| *first)
            .collect();
        for first in firsts {
            self.try_window(first, now, now_unix_us, out);
        }
    }

    /// Recover what a window's repair can, and then what the recovered parts
    /// make recoverable in the other windows.
    fn try_window(
        &mut self,
        first: u64,
        now: Instant,
        now_unix_us: u64,
        out: &mut Vec<CompactPart>,
    ) {
        let mut queue = vec![first];
        while let Some(first) = queue.pop() {
            let Some(window) = self.windows.get(&first) else {
                continue;
            };
            let parts: Vec<u64> = (first..first + u64::from(window.k)).collect();
            if parts.iter().all(|part| self.delivered.contains(part)) {
                self.windows.remove(&first);
                continue;
            }
            let known: Vec<Option<&[u8]>> = parts
                .iter()
                .map(|part| self.known.get(part).map(Vec::as_slice))
                .collect();
            let rows: Vec<(u8, &[u8])> = window
                .rows
                .iter()
                .map(|(row, parity)| (*row, parity.as_slice()))
                .collect();
            let Some(recovered) = recover_window(&known, window.t, &rows) else {
                continue;
            };
            self.windows.remove(&first);
            for (index, padded) in recovered {
                let part = first + index as u64;
                let Some(view) = parse(&padded) else {
                    // The edge's bytes of a part differ from the ones the
                    // client has: NACK will fetch it.
                    self.stats.errors += 1;
                    continue;
                };
                if self.part_number(view.part_low) != part {
                    self.stats.errors += 1;
                    continue;
                }
                let datagram = padded[..HEADER_LEN + view.payload.len()].to_vec();
                self.remember(part, datagram);
                if self.mark_delivered(part, now) {
                    if view.kind == Kind::Gap {
                        self.note_gap(part, now);
                    }
                    out.push(self.part_of(part, &view, now_unix_us, true));
                    // Other windows that held it may now be complete enough.
                    queue.extend(
                        self.windows
                            .range(part.saturating_sub(u64::from(MAX_WINDOW))..=part)
                            .filter(|(other, window)| {
                                **other != first && part < **other + u64::from(window.k)
                            })
                            .map(|(other, _)| *other),
                    );
                }
            }
        }
    }

    /// The parts to ask the edge for now: holes whose time has come, at
    /// most `limit`. Holes too old are given up.
    pub fn nacks_due(&mut self, now: Instant, limit: usize) -> Vec<u64> {
        self.note_silence(now);
        let silent = self.silent(now);
        let mut due = Vec::new();
        let mut abandoned = Vec::new();
        let retry = self.nack_retry();
        let most = MAX_NACK_RETRY.max(retry);
        let highest = self.highest;
        for (part, hole) in self.holes.iter_mut() {
            if now.duration_since(hole.since) >= HOLE_GIVE_UP || hole.nacks >= MAX_NACKS_PER_HOLE {
                abandoned.push((*part, hole.gap));
                continue;
            }
            if hole.next_nack <= now && due.len() < limit {
                // While the stream is silent, the holes are asked for again
                // and again (what the edge sends into a burst is lost):
                // each interval at first, then less often, and at most
                // SILENCE_NACKS_PER_HOLE times, so that a long burst does
                // not spend the edge's NACK budget (200 parts in 100 ms);
                // the part that ends the silence asks for them again.
                if let Some(every) = silent {
                    if hole.silence_nacks >= SILENCE_NACKS_PER_HOLE {
                        hole.next_nack = now + SILENCE_NACK_FOR;
                        continue;
                    }
                    hole.silence_nacks += 1;
                    hole.next_nack = now + every * (1 << (hole.silence_nacks / 2).min(3));
                } else {
                    hole.nacks += 1;
                    let backoff = retry.saturating_mul(1 << (hole.nacks - 1).min(8)).min(most);
                    hole.next_nack = now + backoff;
                }
                hole.last_nack = Some(now);
                due.push(*part);
            }
        }
        for (part, gap) in abandoned {
            self.holes.remove(&part);
            // A gap that never filled was delivered as a gap: not lost; nor
            // is a part past the highest that a silence asked for.
            if !gap && highest.is_some_and(|highest| part < highest) {
                self.stats.abandoned += 1;
            }
        }
        self.stats.nacked += due.len() as u64;
        due
    }

    /// When the next NACK is due, if a hole waits, or when the stream will
    /// have been silent long enough to NACK the parts due.
    pub fn next_nack_at(&self) -> Option<Instant> {
        let holes = self.holes.values().map(|hole| hole.next_nack).min();
        let silence =
            (self.active && self.nack_timing().silence && self.silence_noted != self.highest_at)
                .then(|| Some(self.highest_at? + self.silence_after()?))
                .flatten();
        match (holes, silence) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    pub fn holes(&self) -> usize {
        self.holes.len()
    }

    /// Settle the counts of parts and windows older than the settle time:
    /// (datagrams received, datagrams sent) of the originals and copies of
    /// parts and of window repair since the last call. Runs of datagrams
    /// lost in a row are counted as bursts: all the datagrams of a part
    /// lost whole, and the lost ones of a part that came (which end the
    /// run). A copy lost with its part is part of the burst, so a burst
    /// that copies cannot beat reads as one.
    pub fn take_counts(&mut self, now: Instant) -> (u64, u64) {
        let settled: Vec<u64> = self
            .counts
            .iter()
            .take_while(|(_, count)| now.duration_since(count.at) >= SETTLE)
            .map(|(part, _)| *part)
            .collect();
        let (mut received, mut sent) = (0_u64, 0_u64);
        for part in settled {
            let count = self.counts.remove(&part).unwrap();
            let came = count.received.min(count.sent);
            received += u64::from(came);
            sent += u64::from(count.sent);
            self.run += u64::from(count.sent - came);
            if came > 0 && self.run > 0 {
                self.burst_sum += self.run;
                self.burst_runs += 1;
                self.run = 0;
            }
        }
        let windows: Vec<u64> = self
            .window_counts
            .iter()
            .filter(|(_, count)| now.duration_since(count.at) >= SETTLE)
            .map(|(first, _)| *first)
            .collect();
        for first in windows {
            let count = self.window_counts.remove(&first).unwrap();
            received += u64::from(count.received.min(count.sent));
            sent += u64::from(count.sent);
        }
        // Windows whose parts all came, or that are too old to help.
        self.windows
            .retain(|_, window| now.duration_since(window.seen_at) < HOLE_GIVE_UP);
        self.window_received += received;
        self.window_sent += sent;
        (received, sent)
    }

    /// The mean length of the runs of compact datagrams lost in a row since
    /// the last call, if any run ended.
    pub fn take_burst(&mut self) -> Option<f64> {
        if self.burst_runs == 0 {
            return None;
        }
        let mean = self.burst_sum as f64 / self.burst_runs as f64;
        self.burst_sum = 0;
        self.burst_runs = 0;
        Some(mean)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn before() -> CompactReceiver {
        let mut receiver = CompactReceiver::new();
        receiver.set_nack_timing(NackTiming::BEFORE);
        receiver
    }

    /// A receiver of `parts` parts from `first`, one each `interval`, from
    /// `start`; returns when the last one came.
    fn fed(
        receiver: &mut CompactReceiver,
        first: u64,
        parts: u64,
        start: Instant,
        interval: Duration,
    ) -> Instant {
        receiver.expect_from(first, true);
        let mut at = start;
        for part in first..first + parts {
            at = start + interval * (part - first) as u32;
            receiver.push(&encode_part(part, 0, &payload(part, 10)).unwrap(), at, 0);
        }
        at
    }

    #[test]
    fn a_hole_is_asked_for_a_quarter_millisecond_after_a_later_part_shows_it() {
        let mut receiver = CompactReceiver::new();
        assert_eq!(receiver.nack_timing(), NackTiming::default());
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 10, 10, start, ms);
        assert_eq!(receiver.part_interval(), Some(ms));
        // Part 20 is lost; 21 shows it.
        let at = last + ms * 2;
        receiver.push(&encode_part(21, 0, &payload(21, 10)).unwrap(), at, 0);
        assert!(receiver.nacks_due(at, 64).is_empty());
        assert_eq!(receiver.nacks_due(at + FAST_NACK_DELAY, 64), vec![20]);
        // Repeated after the 2 ms floor (no round trip measured), not before.
        assert!(receiver.nacks_due(at + FAST_NACK_DELAY + ms, 64).is_empty());
        // (By then 22 is overdue too, and asked for as a silence.)
        assert!(receiver
            .nacks_due(at + FAST_NACK_DELAY + FAST_NACK_RETRY, 64)
            .contains(&20));
    }

    #[test]
    fn a_silence_asks_for_the_parts_due_each_interval_until_they_come() {
        let mut receiver = CompactReceiver::new();
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 100, 20, start, ms);
        // Part 120 is due at last + 1 ms; it is asked for 2 ms after the
        // last part (one interval and the margin), not sooner.
        assert_eq!(receiver.next_nack_at(), Some(last + ms * 2));
        assert!(receiver
            .nacks_due(last + Duration::from_micros(1_900), 64)
            .is_empty());
        assert_eq!(receiver.nacks_due(last + ms * 2, 64), vec![120]);
        // A millisecond on: 120 again (its answer went into the burst) and
        // 121, now due; then 120 waits two intervals.
        assert!(receiver.next_nack_at().unwrap() <= last + ms * 3);
        assert_eq!(receiver.nacks_due(last + ms * 3, 64), vec![120, 121]);
        assert_eq!(receiver.nacks_due(last + ms * 4, 64), vec![121, 122]);
        // The burst ends: 123 comes; 120-122 are holes now, asked for
        // again after the delay of a new hole.
        let end = last + ms * 4 + Duration::from_micros(300);
        receiver.push(&encode_part(123, 0, &payload(123, 10)).unwrap(), end, 0);
        assert_eq!(receiver.holes(), 3);
        assert!(receiver.nacks_due(end, 64).is_empty());
        assert_eq!(
            receiver.nacks_due(end + FAST_NACK_DELAY, 64),
            vec![120, 121, 122]
        );
        for part in [120_u64, 121, 122] {
            let mut again = encode_part(part, 0, &payload(part, 10)).unwrap();
            set_flags(&mut again, 0, true);
            assert_eq!(receiver.push(&again, end, 0).len(), 1);
        }
        assert_eq!(receiver.holes(), 0);
        // A silence-asked part that no part shows lost is not abandoned
        // as loss when the stream stops.
        assert!(receiver.nacks_due(end + ms * 3, 64).contains(&124));
        receiver.nacks_due(end + HOLE_GIVE_UP + ms * 4, 64);
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.stats.abandoned, 0);
    }

    #[test]
    fn a_long_silence_stops_the_nacks_and_does_not_spin() {
        let mut receiver = CompactReceiver::new();
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 0, 10, start, ms);
        // First seen 60 ms on (a stalled client): nothing is asked for, and
        // no wake-up is due any more.
        assert!(receiver.nacks_due(last + ms * 60, 64).is_empty());
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.next_nack_at(), None);
        // A silence NACKed from its start stops being repeated each
        // interval after SILENCE_NACK_FOR: the holes back off.
        let mut other = CompactReceiver::new();
        let last = fed(&mut other, 0, 10, start, ms);
        let mut sent = 0;
        let mut at = last;
        while at < last + SILENCE_NACK_FOR {
            sent += other.nacks_due(at, 64).len();
            at += Duration::from_micros(250);
        }
        // At most 32 parts, each asked for at most 6 times during the
        // silence: under the edge's NACK budget (200 parts in 100 ms).
        assert!(sent <= 192, "{sent} NACKs during the silence");
        while at < last + ms * 120 {
            other.nacks_due(at, 64);
            at += Duration::from_micros(250);
        }
        assert!(other.holes() > 0);
        let quiet = other.nacks_due(at, 64).len() + other.nacks_due(at + ms, 64).len();
        assert!(quiet <= 32, "{quiet} NACKs a millisecond after the silence");
    }

    #[test]
    fn a_jittery_path_waits_longer_before_it_asks_for_a_silence() {
        let mut receiver = CompactReceiver::new();
        receiver.expect_from(0, true);
        let start = Instant::now();
        // 1 ms parts that arrive 0.9 ms early or late in turn.
        let mut at = start;
        for part in 0..200_u64 {
            let offset = if part % 2 == 0 { 0 } else { 900 };
            at = start + Duration::from_micros(part * 1_000 + offset);
            receiver.push(&encode_part(part, 0, &payload(part, 10)).unwrap(), at, 0);
        }
        let silence = receiver.next_nack_at().unwrap() - at;
        assert!(
            silence > Duration::from_millis(4),
            "a silence is asked for after {silence:?}"
        );
    }

    #[test]
    fn the_timing_of_before_asks_for_nothing_during_a_silence() {
        let mut receiver = before();
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 0, 10, start, ms);
        assert_eq!(receiver.next_nack_at(), None);
        assert!(receiver.nacks_due(last + ms * 5, 64).is_empty());
    }

    fn payload(part: u64, len: usize) -> Vec<u8> {
        (0..len).map(|i| (i as u64 * 7 + part * 13) as u8).collect()
    }

    #[test]
    fn the_fast_lane_mark_is_a_flag_outside_the_canonical_form() {
        let bytes = payload(5, 40);
        let mut datagram = encode_part(5, 1_700_000_000_000_500, &bytes).unwrap();
        set_generation(&mut datagram, 3);
        let plain = datagram.clone();
        set_flags(&mut datagram, 1, false);
        set_fast_lane(&mut datagram);
        let view = parse(&datagram).unwrap();
        assert!(view.fast_lane);
        assert_eq!((view.copies, view.retransmit), (1, false));
        assert_eq!(canonical(&datagram), plain);
        assert!(!parse(&plain).unwrap().fast_lane);
        // A new set_flags (a retransmission from the cache) clears it.
        set_flags(&mut datagram, 0, true);
        assert!(!parse(&datagram).unwrap().fast_lane);
        // The receiver says which parts came marked; a part recovered from
        // window repair is not marked.
        let now = Instant::now();
        let mut receiver = CompactReceiver::new();
        receiver.expect_from(5, true);
        let mut marked = encode_part(5, 1_700_000_000_000_500, &bytes).unwrap();
        set_fast_lane(&mut marked);
        let parts = receiver.push(&marked, now, 1_700_000_000_001_000);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].fast_lane);
        let unmarked = encode_part(6, 1_700_000_000_001_500, &payload(6, 40)).unwrap();
        assert!(!receiver.push(&unmarked, now, 1_700_000_000_002_000)[0].fast_lane);
    }

    /// The short form (wave 7 item 9): 7 bytes of header, the flags kept,
    /// and read back to the long form byte for byte, around the wraps of
    /// its 16-bit part number and 24-bit time.
    #[test]
    fn a_part_in_the_short_form_reads_back_to_its_long_form() {
        let bytes = payload(3, 56);
        for (part, published) in [
            (9_u64, 1_700_000_000_123_456_u64),
            (65_535, 1_700_000_016_777_215),
            (65_537, 1_700_000_016_777_216),
            (1_234_567, 1_763_812_345_678_901),
        ] {
            let mut long = encode_part(part, published, &bytes).unwrap();
            set_generation(&mut long, 9);
            set_flags(&mut long, 2, true);
            set_fast_lane(&mut long);
            let short = shorten(&long).unwrap();
            assert_eq!(short.len(), SHORT_HEADER_LEN + 56);
            assert_eq!(short.len() + SHORT_SAVES, long.len());
            assert!(is_short(&short) && !is_compact(&short));
            let mut in_place = long.clone();
            assert_eq!(
                shorten_in_place(&mut in_place, long.len()),
                Some(short.len())
            );
            assert_eq!(&in_place[..short.len()], short.as_slice());
            // The client's reference is a part or two away, its clock a few
            // milliseconds (or seconds) off.
            for (reference, now) in [
                (part.saturating_sub(2), published + 3_000),
                (part + 1, published - 2_000_000),
                (part + 30_000, published + 7_000_000),
            ] {
                assert_eq!(
                    lengthen(&short, reference, now).unwrap(),
                    long,
                    "part {part}"
                );
            }
        }
        // Gaps and repair keep the long form, and so does every 256th part.
        assert_eq!(shorten(&encode_gap(5)), None);
        for part in [0_u64, 256, 65_536, 1 << 32] {
            let long = encode_part(part, 1_700_000_000_000_000, &bytes).unwrap();
            assert!(keeps_long_form(part));
            assert_eq!(shorten(&long), None, "part {part}");
            let mut in_place = long.clone();
            assert_eq!(shorten_in_place(&mut in_place, long.len()), None);
        }
        // A short part before any reference is dropped; after the OK it is
        // a part.
        let mut receiver = CompactReceiver::new();
        let long = encode_part(70_000, 1_700_000_000_000_000, &bytes).unwrap();
        let short = shorten(&long).unwrap();
        let now = Instant::now();
        assert!(receiver.push(&short, now, 1_700_000_000_001_000).is_empty());
        receiver.expect_from(69_999, true);
        let parts = receiver.push(&short, now, 1_700_000_000_001_000);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part, 70_000);
        assert_eq!(parts[0].payload, bytes);
        assert_eq!(parts[0].published_unix_us, 1_700_000_000_000_000);
    }

    /// Wave 7 gate: the first parts that a client misses are asked for
    /// again: those between the part its OK names and the first that came
    /// (lost on the way), and, when the OK came after parts (an earlier OK
    /// was lost), those between it and the highest that came.
    #[test]
    fn the_first_parts_missed_are_asked_for_again() {
        let bytes = payload(5, 56);
        let start = Instant::now();
        let delay = Duration::from_millis(5);
        // The OK names 700; 700..=702 are lost and 703 comes first.
        let mut receiver = CompactReceiver::new();
        receiver.expect_from(700, true);
        let first = encode_part(703, 1_700_000_000_000_000, &bytes).unwrap();
        assert_eq!(receiver.push(&first, start, 1_700_000_000_001_000).len(), 1);
        assert_eq!(receiver.nacks_due(start + delay, 64), vec![700, 701, 702]);
        // A part (long, as every 256th is) came before the OK, which names
        // an older part: the parts between are asked for.
        let mut receiver = CompactReceiver::new();
        let anchor = encode_part(768, 1_700_000_000_000_000, &bytes).unwrap();
        assert!(shorten(&anchor).is_none());
        receiver.active = true;
        assert_eq!(receiver.push(&anchor, start, 1_700_000_000_001_000).len(), 1);
        receiver.expect_from(760, true);
        assert_eq!(
            receiver.nacks_due(start + delay, 64),
            (760..768).collect::<Vec<_>>()
        );
    }

    /// A listener whose clock is a minute off the edge's still rebuilds the
    /// edge's long form byte for byte: the high bits of the short form's
    /// time come from the last long-form part, not the local clock.
    #[test]
    fn a_short_part_takes_its_time_from_the_last_long_part() {
        let bytes = payload(4, 56);
        let edge_us = 1_763_812_345_678_901_u64;
        let client_off_us = 60_000_000;
        let mut receiver = CompactReceiver::new();
        receiver.expect_from(511, true);
        let start = Instant::now();
        let anchor = encode_part(512, edge_us, &bytes).unwrap();
        assert!(shorten(&anchor).is_none(), "part 512 keeps the long form");
        let parts = receiver.push(&anchor, start, edge_us + client_off_us);
        assert_eq!(parts.len(), 1);
        for index in 1..300_u64 {
            let part = 512 + index;
            let published = edge_us + index * 1_000;
            let long = encode_part(part, published, &bytes).unwrap();
            let datagram = shorten(&long).unwrap_or(long.clone());
            let at = start + Duration::from_millis(index);
            let parts = receiver.push(&datagram, at, published + client_off_us + 500);
            assert_eq!(parts.len(), 1, "part {part}");
            // The long form it keeps (for window repair) is the edge's.
            assert_eq!(
                lengthen(&datagram, part, published).unwrap_or(long.clone()),
                long
            );
            assert_eq!(
                u32::from_be_bytes(receiver.known[&part][7..11].try_into().unwrap())
                    as u64,
                published & 0xffff_ffff,
                "part {part}"
            );
        }
        assert_eq!(receiver.stats.errors, 0);
    }

    #[test]
    fn a_part_is_one_datagram_with_an_eleven_byte_header() {
        assert_eq!(HEADER_LEN, 11);
        let bytes = payload(9, 96);
        let datagram = encode_part(9, 1_700_000_000_123_456, &bytes).unwrap();
        assert_eq!(datagram.len(), HEADER_LEN + 96);
        // A 2.5 ms Opus part of 96 bytes: 135 bytes on the wire, 1.41 times
        // the part (IPv4 and UDP are 28 of them).
        assert_eq!(datagram.len() + IP_UDP_HEADER_BYTES, 135);
        let view = parse(&datagram).unwrap();
        assert_eq!(view.kind, Kind::Part);
        assert_eq!(view.part_low, 9);
        assert_eq!(view.generation, 0);
        assert_eq!(view.payload, bytes.as_slice());
        assert_eq!(
            extend_u32(1_700_000_000_200_000, u32::from_be_bytes(view.field)),
            1_700_000_000_123_456
        );
        // Flags are not in the canonical form; the generation is.
        let mut marked = datagram.clone();
        set_generation(&mut marked, 7);
        let stamped = marked.clone();
        set_flags(&mut marked, 2, true);
        let view = parse(&marked).unwrap();
        assert_eq!(
            (view.copies, view.retransmit, view.generation),
            (2, true, 7)
        );
        assert_eq!(canonical(&marked), stamped);
        // Not a compact datagram: text controls, an RQD2 header.
        assert!(!is_compact(b"SUB 4001 0 C0.32.1 ok"));
        assert!(!is_compact(b"RQD2 and the rest of a header"));
        assert!(is_gap(&encode_gap(3)) && !is_gap(&datagram));
        assert!(encode_part(1, 0, &vec![0; MAX_PAYLOAD + 1]).is_none());
        assert_eq!(
            encode_part(1, 0, &vec![0; MAX_PAYLOAD]).unwrap().len(),
            MAX_DATAGRAM
        );
    }

    #[test]
    fn a_32_bit_number_extends_to_the_nearest() {
        assert_eq!(extend_u32(0, 5), 5);
        assert_eq!(extend_u32(0x1_0000_0005, 7), 0x1_0000_0007);
        assert_eq!(extend_u32(0x1_0000_0002, 0xffff_fffe), 0xffff_fffe);
        assert_eq!(extend_u32(0x1_ffff_fffe, 3), 0x2_0000_0003);
        assert_eq!(
            extend_u32(u64::MAX - 3, (u64::MAX - 1) as u32),
            u64::MAX - 1
        );
    }

    #[test]
    fn a_plan_word_round_trips_and_is_bounded() {
        for plan in [
            CompactPlan::CLEAN,
            CompactPlan {
                copies: 2,
                window: 16,
                window_repair: 3,
                short_header: false,
            },
            CompactPlan::CLEAN.with_short_header(true),
        ] {
            assert_eq!(CompactPlan::parse_word(&plan.word()), Some(plan));
        }
        assert_eq!(
            CompactPlan::CLEAN.with_short_header(true).word(),
            "C0.32.1.7"
        );
        assert_eq!(
            CompactPlan::parse_word("C9.200.40"),
            Some(CompactPlan {
                copies: MAX_COPIES,
                window: MAX_WINDOW,
                window_repair: MAX_WINDOW_REPAIR,
                short_header: false,
            })
        );
        assert_eq!(CompactPlan::parse_word("C1.2.3.11"), None);
        assert_eq!(CompactPlan::parse_word("C1.2.3.7.7"), None);
        assert_eq!(CompactPlan::parse_word("C1.2"), None);
        assert_eq!(CompactPlan::parse_word("F1"), None);
        assert_eq!(CompactPlan::parse_word("C1.2.3.4"), None);
    }

    #[test]
    fn a_clean_path_gets_window_repair_alone_and_a_lossy_one_copies() {
        assert_eq!(CompactPlan::for_measured(0.0, 0.0), CompactPlan::CLEAN);
        assert_eq!(CompactPlan::for_measured(0.001, 1.0), CompactPlan::CLEAN);
        let one = CompactPlan::for_measured(0.01, 1.0);
        assert_eq!(one.copies, 1);
        let five = CompactPlan::for_measured(0.05, 1.0);
        assert!(five.copies >= 1 && five.copies <= 2, "{five:?}");
        // A bursty path: one copy at most, more window repair.
        let bursty = CompactPlan::for_measured(0.05, 5.0);
        assert!(bursty.copies <= 1);
        assert!(
            bursty.window_repair > one.window_repair,
            "{bursty:?} {one:?}"
        );
        for plan in [one, five, bursty] {
            assert_eq!(plan, plan.bounded());
        }
    }

    /// A recovered part whose bytes differ from the edge's (the client holds
    /// other bytes for a part of the window) fails its CRC: it is not
    /// delivered as a part.
    #[test]
    fn a_recovered_part_is_checked_by_its_crc() {
        let symbols: Vec<Vec<u8>> = (0..4_u64)
            .map(|part| encode_part(part, 0, &payload(part, 30)).unwrap())
            .collect();
        let refs: Vec<&[u8]> = symbols.iter().map(Vec::as_slice).collect();
        let repair = encode_window_repair(0, &refs, 1);
        let row = parse(&repair[0]).unwrap();
        let mut other = symbols[1].clone();
        other[HEADER_LEN + 3] ^= 0x40;
        let known = [Some(refs[0]), Some(other.as_slice()), None, Some(refs[3])];
        let recovered = recover_window(&known, row.payload.len(), &[(0, row.payload)]).unwrap();
        assert_eq!(recovered.len(), 1);
        assert!(recovered[0].1.is_empty(), "a wrong part was recovered");
        assert!(parse(&recovered[0].1).is_none());
    }

    /// A contributor restart: the edge stamps the new epoch's generation; the
    /// client forgets the old epoch's parts, NACKs the new epoch's holes and
    /// delivers its parts though they have the old parts' numbers, and drops
    /// the old epoch's late datagrams.
    #[test]
    fn a_new_source_generation_starts_a_new_flow() {
        let start = Instant::now();
        let old = generation_of_epoch(1_700_000_000_000_000);
        let new = generation_of_epoch(1_700_000_100_000_000);
        assert_ne!(old, new);
        assert_ne!(generation_of_epoch(0), 0);
        let stamped = |part: u64, generation: u8| {
            let mut datagram = encode_part(part, 0, &payload(part, 10)).unwrap();
            set_generation(&mut datagram, generation);
            datagram
        };
        let mut receiver = CompactReceiver::new();
        receiver.expect_from(1_000, true);
        for part in 1_000..1_010 {
            assert_eq!(receiver.push(&stamped(part, old), start, 0).len(), 1);
        }
        // The new epoch counts from 0: its parts are delivered, not taken
        // for old ones, and its holes are NACKed.
        assert_eq!(receiver.push(&stamped(0, new), start, 0).len(), 1);
        assert_eq!(receiver.stats.generation_changes, 1);
        assert_eq!(receiver.push(&stamped(2, new), start, 0).len(), 1);
        assert_eq!(receiver.holes(), 1);
        let later = start + Duration::from_millis(5);
        assert_eq!(receiver.nacks_due(later, 16), vec![1]);
        // A late datagram of the old epoch is dropped, and does not switch
        // back.
        assert!(receiver.push(&stamped(1_010, old), later, 0).is_empty());
        assert_eq!(receiver.stats.stale_generation, 1);
        assert_eq!(receiver.push(&stamped(1, new), later, 0).len(), 1);
        assert_eq!(receiver.stats.generation_changes, 1);
        // Generation 0 (not known) is taken in any flow.
        assert_eq!(receiver.push(&stamped(3, 0), later, 0).len(), 1);
    }

    /// Any `r` lost parts of a window are recovered from `r` rows, for every
    /// choice of the lost parts and of the rows that arrived.
    #[test]
    fn any_r_losses_of_a_window_are_recovered_from_r_rows() {
        let k = 8_usize;
        let symbols: Vec<Vec<u8>> = (0..k as u64)
            .map(|part| {
                encode_part(
                    100 + part,
                    77 + part,
                    &payload(part, 40 + part as usize * 9),
                )
                .unwrap()
            })
            .collect();
        let refs: Vec<&[u8]> = symbols.iter().map(Vec::as_slice).collect();
        let repair = encode_window_repair(100, &refs, 4);
        assert_eq!(repair.len(), 4);
        // A client that gets two of the rows is told so.
        let two = with_rows_sent(&repair[1], 2);
        let view = parse(&two).unwrap();
        assert_eq!(view.field, [8, 1, 2, 0]);
        assert_eq!(view.payload, parse(&repair[1]).unwrap().payload);
        let views: Vec<View<'_>> = repair
            .iter()
            .map(|datagram| parse(datagram).unwrap())
            .collect();
        // Each part is coded behind its length and CRC.
        let t = views[0].payload.len();
        assert_eq!(
            t,
            symbols.iter().map(Vec::len).max().unwrap() + REPAIR_PREFIX
        );
        for lost_mask in 1_u32..(1 << k) {
            let lost = lost_mask.count_ones() as usize;
            if lost > 4 {
                continue;
            }
            // The rows that arrived: the last `lost` of them.
            let rows: Vec<(u8, &[u8])> = views[4 - lost..]
                .iter()
                .map(|view| (view.field[1], view.payload))
                .collect();
            let known: Vec<Option<&[u8]>> = (0..k)
                .map(|index| (lost_mask & (1 << index) == 0).then(|| refs[index]))
                .collect();
            let recovered = recover_window(&known, t, &rows).unwrap();
            assert_eq!(recovered.len(), lost);
            for (index, datagram) in recovered {
                // Cut to its length: the part's datagram, byte for byte.
                assert_eq!(datagram, symbols[index]);
                assert_eq!(parse(&datagram).unwrap().part_low, 100 + index as u32);
            }
            // One row fewer than the losses is not enough.
            assert!(recover_window(&known, t, &rows[1..]).is_none());
        }
    }

    #[test]
    fn a_receiver_delivers_each_part_once_and_recovers_a_window() {
        let mut receiver = CompactReceiver::new();
        receiver.expect_from(1_000, true);
        let now = Instant::now();
        let datagrams: Vec<Vec<u8>> = (1_000..1_016_u64)
            .map(|part| encode_part(part, 1_000 + part, &payload(part, 80)).unwrap())
            .collect();
        let refs: Vec<&[u8]> = datagrams.iter().map(Vec::as_slice).collect();
        let repair = encode_window_repair(1_000, &refs, 2);
        let mut delivered = Vec::new();
        for (index, datagram) in datagrams.iter().enumerate() {
            if index == 3 || index == 9 {
                continue;
            }
            delivered.extend(receiver.push(datagram, now, 0));
        }
        assert_eq!(receiver.holes(), 2);
        // A copy is a duplicate.
        let mut copy = datagrams[0].clone();
        set_flags(&mut copy, 1, false);
        assert!(receiver.push(&copy, now, 0).is_empty());
        assert_eq!(receiver.stats.duplicates, 1);
        delivered.extend(receiver.push(&repair[0], now, 0));
        assert_eq!(delivered.len(), 14, "one row is not enough for two holes");
        delivered.extend(receiver.push(&repair[1], now, 0));
        assert_eq!(delivered.len(), 16);
        let recovered: Vec<u64> = delivered
            .iter()
            .filter(|part| part.recovered)
            .map(|part| part.part)
            .collect();
        assert_eq!(recovered, vec![1_003, 1_009]);
        assert_eq!(
            delivered.iter().find(|p| p.part == 1_009).unwrap().payload,
            payload(1_009, 80)
        );
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.stats.recovered, 2);
    }

    #[test]
    fn a_hole_is_asked_for_after_the_delay_and_again_until_given_up() {
        // The timing of before wave 6 (needletail-udp-client --silence-nack
        // false --nack-delay-us 1000 --nack-retry-floor-us 5000).
        let mut receiver = before();
        receiver.expect_from(10, true);
        let start = Instant::now();
        for part in [10_u64, 11, 14] {
            receiver.push(&encode_part(part, 0, &payload(part, 10)).unwrap(), start, 0);
        }
        assert!(
            receiver.nacks_due(start, 64).is_empty(),
            "not before the delay"
        );
        assert_eq!(receiver.nacks_due(start + NACK_DELAY, 64), vec![12, 13]);
        assert!(receiver.nacks_due(start + NACK_DELAY, 64).is_empty());
        assert_eq!(
            receiver.nacks_due(start + NACK_DELAY + NACK_RETRY, 1),
            vec![12],
            "at most the limit"
        );
        // The next NACK of a part waits twice as long.
        assert!(receiver
            .nacks_due(start + NACK_DELAY + NACK_RETRY * 2, 64)
            .contains(&13));
        assert!(receiver
            .nacks_due(start + NACK_DELAY + NACK_RETRY * 2, 64)
            .is_empty());
        assert_eq!(
            receiver.nacks_due(start + NACK_DELAY + NACK_RETRY * 3, 64),
            vec![12]
        );
        // A retransmission fills one hole.
        let mut again = encode_part(13, 0, &payload(13, 10)).unwrap();
        set_flags(&mut again, 0, true);
        let parts = receiver.push(&again, start, 0);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].retransmit);
        assert_eq!(receiver.holes(), 1);
        receiver.nacks_due(start + HOLE_GIVE_UP, 64);
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.stats.abandoned, 1);
        // A jump far ahead is a resume, not holes.
        receiver.push(&encode_part(10_000, 0, &[]).unwrap(), start, 0);
        assert_eq!(receiver.holes(), 0);
    }

    #[test]
    fn a_part_that_comes_after_its_gap_is_delivered_and_the_gap_is_asked_for() {
        // The timing of before wave 6 (needletail-udp-client --silence-nack
        // false --nack-delay-us 1000 --nack-retry-floor-us 5000).
        let mut receiver = before();
        receiver.expect_from(20, true);
        let start = Instant::now();
        receiver.push(&encode_part(20, 0, &payload(20, 10)).unwrap(), start, 0);
        // The edge gave part 21 up (its hole outlived the repair horizon).
        let parts = receiver.push(&encode_gap(21), start, 0);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].kind, Kind::Gap);
        // The gap is asked for again a round trip later, in case the edge's
        // cache has filled it since.
        assert!(receiver.nacks_due(start, 64).is_empty());
        assert_eq!(receiver.nacks_due(start + NACK_RETRY, 64), vec![21]);
        // It had: the part comes, and is delivered after its gap, once.
        let mut late = encode_part(21, 0, &payload(21, 10)).unwrap();
        set_flags(&mut late, 0, true);
        let parts = receiver.push(&late, start + NACK_RETRY * 2, 0);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].kind, Kind::Part);
        assert_eq!(parts[0].payload, payload(21, 10));
        assert!(receiver.push(&late, start + NACK_RETRY * 2, 0).is_empty());
        assert_eq!(receiver.holes(), 0);
        // A gap again for a part delivered is a duplicate.
        assert!(receiver.push(&encode_gap(20), start, 0).is_empty());
        // A gap whose part never comes is not counted as abandoned (it was
        // delivered, as a gap).
        receiver.push(&encode_gap(22), start, 0);
        receiver.nacks_due(start + HOLE_GIVE_UP, 64);
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.stats.abandoned, 0);
    }

    #[test]
    fn nacks_are_not_repeated_before_a_round_trip() {
        // The timing of before wave 6 (needletail-udp-client --silence-nack
        // false --nack-delay-us 1000 --nack-retry-floor-us 5000).
        let mut receiver = before();
        receiver.expect_from(10, true);
        receiver.set_rtt(Duration::from_millis(30));
        assert_eq!(receiver.nack_retry(), Duration::from_millis(45));
        let start = Instant::now();
        for part in [10_u64, 12] {
            receiver.push(&encode_part(part, 0, &payload(part, 10)).unwrap(), start, 0);
        }
        assert_eq!(receiver.nacks_due(start + NACK_DELAY, 64), vec![11]);
        // Not at 5 ms or 15 ms after it: a retransmission takes 30 ms.
        for after in [5, 15, 40] {
            assert!(receiver
                .nacks_due(start + NACK_DELAY + Duration::from_millis(after), 64)
                .is_empty());
        }
        assert_eq!(
            receiver.nacks_due(start + NACK_DELAY + Duration::from_millis(45), 64),
            vec![11]
        );
        // A short path keeps the 5 ms floor.
        let mut near = before();
        near.set_rtt(Duration::from_micros(200));
        assert_eq!(near.nack_retry(), NACK_RETRY);
        // Wave 6's floor is 2 ms.
        let mut fast = CompactReceiver::new();
        fast.set_rtt(Duration::from_micros(200));
        assert_eq!(fast.nack_retry(), FAST_NACK_RETRY);
        let mut far = CompactReceiver::new();
        far.set_rtt(Duration::from_millis(30));
        assert_eq!(far.nack_retry(), Duration::from_millis(45));
        // The answer to a hole's only NACK measures the round trip.
        let mut measured = before();
        measured.expect_from(0, true);
        for part in [0_u64, 2] {
            measured.push(&encode_part(part, 0, &payload(part, 10)).unwrap(), start, 0);
        }
        assert_eq!(measured.nacks_due(start + NACK_DELAY, 64), vec![1]);
        let mut again = encode_part(1, 0, &payload(1, 10)).unwrap();
        set_flags(&mut again, 0, true);
        measured.push(&again, start + NACK_DELAY + Duration::from_millis(20), 0);
        assert_eq!(measured.rtt(), Some(Duration::from_millis(20)));
    }

    #[test]
    fn loss_is_counted_on_originals_and_copies_and_bursts_are_measured() {
        let mut receiver = CompactReceiver::new();
        receiver.expect_from(0, true);
        let start = Instant::now();
        // Parts 0..100 with one copy each; parts 10..13 lost whole with
        // their copies (a burst of six datagrams), part 50's copy lost (a
        // burst of one).
        for part in 0..100_u64 {
            let mut datagram = encode_part(part, 0, &payload(part, 20)).unwrap();
            set_flags(&mut datagram, 1, false);
            if (10..13).contains(&part) {
                continue;
            }
            receiver.push(&datagram, start, 0);
            if part != 50 {
                receiver.push(&datagram, start, 0);
            }
        }
        let (received, sent) = receiver.take_counts(start + SETTLE);
        assert_eq!(sent, 200);
        assert_eq!(received, 200 - 6 - 1);
        assert_eq!(receiver.take_burst(), Some(3.5));
        assert_eq!(receiver.take_burst(), None);
    }

    /// With back-to-back copies, a burst that takes one part takes its
    /// copy: the bursts read in datagrams make a gemodel path bursty (a
    /// count of parts lost whole read about one), and its plan then sends
    /// no copies but window rows for the burst.
    #[test]
    fn a_burst_that_takes_a_part_and_its_copy_reads_as_a_burst() {
        let mut receiver = CompactReceiver::new();
        receiver.expect_from(0, true);
        let start = Instant::now();
        // A burst of four datagrams (parts 10 and 11 and their copies), one
        // of one (part 30's copy: a part that came ends a run) and one of
        // two (part 31 and its copy): 7 / 3.
        let lost = |part: u64, copy: bool| match part {
            10 | 11 | 31 => true,
            30 => copy,
            _ => false,
        };
        for part in 0..60_u64 {
            let mut datagram = encode_part(part, 0, &payload(part, 20)).unwrap();
            set_flags(&mut datagram, 1, false);
            for copy in [false, true] {
                if !lost(part, copy) {
                    receiver.push(&datagram, start, 0);
                }
            }
        }
        receiver.take_counts(start + SETTLE);
        let burst = receiver.take_burst().unwrap();
        assert!((burst - 7.0 / 3.0).abs() < 1e-9, "{burst}");
        let plan = CompactPlan::for_measured(0.048, burst);
        assert_eq!(plan.copies, 0, "{plan:?}");
        assert!(plan.window_repair >= 4, "{plan:?}");
        // Random loss: copies, as before.
        let random = CompactPlan::for_measured(0.01, 1.0);
        assert_eq!(random.copies, 1);
        // The plan of before, for the A/B: one copy and two more rows.
        let before = CompactPlan::for_measured_with(0.048, burst, false);
        assert_eq!((before.copies, before.window_repair), (1, 4), "{before:?}");
    }
}
