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
        Self::for_measured_with(loss, burst, true, RESIDUAL_TARGET)
    }

    /// [`Self::for_measured`], or with `burst_rows` false the plan of
    /// before (needletail-udp-client `--compact-burst-rows false`): a
    /// bursty path keeps one copy and gets at most two more rows. Copies
    /// are added while more than `residual_target` of the parts would need
    /// a NACK ([`RESIDUAL_TARGET`] by default).
    pub fn for_measured_with(
        loss: f64,
        burst: f64,
        burst_rows: bool,
        residual_target: f64,
    ) -> Self {
        if !(loss >= CLEAN_LOSS) {
            return Self::CLEAN;
        }
        let planned = (loss * 1.5 + 0.002).min(0.3);
        let window = 16_u8;
        if burst_rows && burst >= BURSTY {
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
        while residual > residual_target && copies < MAX_COPIES {
            copies += 1;
            residual *= planned;
        }
        let bursty = burst >= BURSTY;
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

/// What a leg's plan is sized from: its smoothed datagram loss, its mean
/// burst (at least 2.0 while the session counts it bursty) and its round
/// trip.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LegLoss {
    pub loss: f64,
    pub burst: f64,
    pub rtt: Option<Duration>,
}

/// The plans of a listener's counted legs (B X3, section 3.3): the repair
/// leg, alone asked for rows, and the other leg. Two datagrams a part is
/// the floor of two paths, and a clean pair of legs asks for nothing more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathPlans {
    pub repair: CompactPlan,
    pub other: Option<CompactPlan>,
}

/// A path whose mean burst is at least this is bursty.
pub const BURSTY: f64 = 2.0;

impl PathPlans {
    /// The plans for the repair leg and, if counted, the other leg.
    /// `joint` is the joint part loss of the legs and its mean run (see
    /// [`CompactReceiver::take_joint`]), `interval` the part interval.
    /// One leg plans as [`CompactPlan::for_measured_with`]. Two legs:
    /// clean (both under [`CLEAN_LOSS`]): `C0.32.0` each. Bursty (a leg's
    /// burst, or a joint loss above 4 p1 p2 + 0.001, the guard against legs
    /// that lose together): rows from the repair leg alone for the joint
    /// loss and its runs. Random loss: copies, one at a time on the leg
    /// with less loss, while the product of the planned losses is above
    /// the residual target; one row when the repair leg's round trip is at
    /// least eight part intervals (a row then comes before a NACK's answer).
    pub fn for_legs(
        repair: LegLoss,
        other: Option<LegLoss>,
        joint: Option<(f64, f64)>,
        interval: Option<Duration>,
        residual_target: f64,
        burst_rows: bool,
    ) -> Self {
        let Some(other) = other else {
            return Self {
                repair: CompactPlan::for_measured_with(
                    repair.loss,
                    repair.burst,
                    burst_rows,
                    residual_target,
                ),
                other: None,
            };
        };
        let plan = |copies: u8, window: u8, window_repair: u8| {
            CompactPlan {
                copies,
                window,
                window_repair,
                short_header: false,
            }
            .bounded()
        };
        let (p1, p2) = (repair.loss, other.loss);
        if !(p1 >= CLEAN_LOSS) && !(p2 >= CLEAN_LOSS) {
            return Self {
                repair: plan(0, 32, 0),
                other: Some(plan(0, 32, 0)),
            };
        }
        let (joint_loss, joint_burst) = joint.unwrap_or((0.0, 1.0));
        let together = joint_loss > 4.0 * p1 * p2 + 0.001;
        if burst_rows && (repair.burst >= BURSTY || other.burst >= BURSTY || together) {
            let planned = (1.5 * joint_loss + 0.002).min(0.3);
            let runs = (joint_burst.ceil() as u8).saturating_sub(1).min(4);
            let rows = (1 + (16.0 * planned).ceil() as u8 + runs).min(MAX_WINDOW_REPAIR);
            return Self {
                repair: plan(0, 16, rows),
                other: Some(plan(0, 16, 0)),
            };
        }
        let planned = |loss: f64| (loss * 1.5 + 0.002).min(0.3);
        let (q1, q2) = (planned(p1), planned(p2));
        let (mut c1, mut c2) = (0_u8, 0_u8);
        let residual = |c1: u8, c2: u8| q1.powi(1 + i32::from(c1)) * q2.powi(1 + i32::from(c2));
        while residual(c1, c2) > residual_target && (c1 < MAX_COPIES || c2 < MAX_COPIES) {
            let first_on_repair = p1 <= p2;
            let on_repair = if first_on_repair {
                c1 < MAX_COPIES
            } else {
                c2 >= MAX_COPIES
            };
            if on_repair {
                c1 += 1;
            } else {
                c2 += 1;
            }
        }
        let far = match (repair.rtt, interval) {
            (Some(rtt), Some(interval)) => rtt >= interval * 8,
            _ => false,
        };
        Self {
            repair: plan(c1, 16, u8::from(far)),
            other: Some(plan(c2, 16, 0)),
        }
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
    let mut datagram = vec![0; HEADER_LEN + payload.len()];
    let len = encode_part_into(part, published_unix_us, payload, &mut datagram)?;
    datagram.truncate(len);
    Some(datagram)
}

/// [`encode_part`] written into `out`, without allocating: its length, or
/// `None` when the part does not fit one datagram or `out`.
pub fn encode_part_into(
    part: u64,
    published_unix_us: u64,
    payload: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let len = HEADER_LEN + payload.len();
    if payload.len() > MAX_PAYLOAD || out.len() < len {
        return None;
    }
    out[0] = MAGIC | Kind::Part.code();
    out[FLAGS_AT] = 0;
    out[GENERATION_AT] = 0;
    out[PART_AT..FIELD_AT].copy_from_slice(&(part as u32).to_be_bytes());
    out[FIELD_AT..HEADER_LEN].copy_from_slice(&(published_unix_us as u32).to_be_bytes());
    out[HEADER_LEN..len].copy_from_slice(payload);
    Some(len)
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

/// A part whose publication time is unknown (0: an edge that took it from
/// a copy without its time) keeps the long form: the short form's 24 bits
/// of 0 would be extended to a time up to 8.4 s off (wave 9 review: parts
/// at an edge taking a new copy reached clients up to 8 s off).
fn unknown_time(view: &View<'_>) -> bool {
    view.field == [0; 4]
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
    if view.kind != Kind::Part || keeps_long_form(u64::from(view.part_low)) || unknown_time(&view) {
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
    if view.kind != Kind::Part || keeps_long_form(u64::from(view.part_low)) || unknown_time(&view) {
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
///
/// Wave 8 (review R6-2..R6-5, S4), each switchable: a silence's NACKs are
/// bounded per silence (`silence_budget` parts) and its holes wait, after
/// the silence's first 50 ms, for a datagram rather than fall into the
/// ordinary schedule; a silence's NACKs are not repeated sooner than the
/// retry (one and a half round trips), nor asked again sooner than that
/// when the silence ends (`rtt_floor`); a gap between two arrivals of more
/// than `interval_cap` intervals is no sample of the interval or the
/// jitter; and a silence is NACKed only `margin_intervals` intervals past
/// the next part's due time, so the 2-5 ms gaps of ordinary delivery ask
/// for nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NackTiming {
    /// The wait before the first NACK of a hole that a later part showed.
    pub delay: Duration,
    /// The floor of the wait between NACKs of a hole.
    pub retry_floor: Duration,
    /// NACK the parts due while the stream is silent.
    pub silence: bool,
    /// The parts that one silence asks for, at most (each NACK of each
    /// part); its holes then wait for a datagram. 0: no bound, and a
    /// silence's holes go on in the ordinary schedule past its first 50 ms
    /// (before wave 8).
    pub silence_budget: u16,
    /// A silence's NACKs of a hole are at least the retry apart, and a hole
    /// that a silence asked for is asked again when the silence ends no
    /// sooner than the retry after its last NACK (`false`: every interval,
    /// and a quarter millisecond after the silence, before wave 8).
    pub rtt_floor: bool,
    /// A gap between two arrivals longer than this many intervals is no
    /// sample of the interval (0: every gap is, before wave 8).
    pub interval_cap: u32,
    /// The margin past the next part's due time before a silence is NACKed,
    /// in intervals (at least [`SILENCE_MARGIN`] and four times the jitter;
    /// 1 before wave 8).
    pub margin_intervals: u32,
    /// The parts a second that the client asks for at most (a token bucket
    /// of a quarter second's worth): an overloaded edge that sends parts
    /// late or not at all is not asked for thousands a second by each
    /// client, which overloads it further (wave 8, the 128-listener lab
    /// collapse). 0: no cap (before wave 8).
    pub rate_cap: u32,
}

impl NackTiming {
    pub const BEFORE: Self = Self {
        delay: NACK_DELAY,
        retry_floor: NACK_RETRY,
        silence: false,
        silence_budget: 0,
        rtt_floor: false,
        interval_cap: 0,
        margin_intervals: 1,
        rate_cap: 0,
    };
    /// Wave 6 and 7's timing (silence NACKs without wave 8's bounds).
    pub const WAVE6: Self = Self {
        delay: FAST_NACK_DELAY,
        retry_floor: FAST_NACK_RETRY,
        silence: true,
        silence_budget: 0,
        rtt_floor: false,
        interval_cap: 0,
        margin_intervals: 1,
        rate_cap: 0,
    };
}

impl Default for NackTiming {
    fn default() -> Self {
        Self {
            delay: FAST_NACK_DELAY,
            retry_floor: FAST_NACK_RETRY,
            silence: true,
            silence_budget: DEFAULT_SILENCE_NACK_BUDGET,
            rtt_floor: true,
            interval_cap: DEFAULT_INTERVAL_CAP,
            margin_intervals: DEFAULT_SILENCE_MARGIN_INTERVALS,
            rate_cap: DEFAULT_NACK_RATE_CAP,
        }
    }
}

/// See [`NackTiming::silence_budget`]: a burst of gemodel 1%/20% is ~5
/// parts, asked for a few times each.
pub const DEFAULT_SILENCE_NACK_BUDGET: u16 = 64;
/// See [`NackTiming::rate_cap`]: a clean path asks for a few parts a
/// second, 5 % loss with copies tens, a burst of 50 ms at 1 ms 50 at once.
pub const DEFAULT_NACK_RATE_CAP: u32 = 200;
/// Short-form parts kept before the `OK`, at most.
const MAX_EARLY: usize = 256;
/// Under the rate cap, the client waits at least this for a token.
const MIN_CAPPED_WAIT: Duration = Duration::from_micros(100);
/// See [`NackTiming::interval_cap`].
pub const DEFAULT_INTERVAL_CAP: u32 = 4;
/// With `interval_cap`: samples shorter than the interval divided by the
/// cap are rejected only after this many were taken.
const INTERVAL_WARMUP_SAMPLES: u32 = 8;
/// With `interval_cap`: this many samples in a row longer than the cap
/// allows seed the interval again (a stall is one such sample).
const INTERVAL_RESEED_AFTER: u32 = 4;
/// See [`NackTiming::margin_intervals`].
pub const DEFAULT_SILENCE_MARGIN_INTERVALS: u32 = 3;

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
pub const SILENCE_MARGIN: Duration = Duration::from_micros(500);
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

/// A hole is given up after this long (by default, see
/// [`CompactReceiver::set_hole_give_up`]; 1 s before B X3), or once it is
/// [`GIVE_UP_PARTS`] behind the newest part.
pub const HOLE_GIVE_UP: Duration = Duration::from_millis(3_000);
/// A hole this many parts behind the newest is given up: the edge's ring
/// (4,096 parts) is about to pass it.
pub const GIVE_UP_PARTS: u64 = 3_072;
/// After this many NACKs of a hole on the ordinary schedule, it is asked
/// for every [`SLOW_NACK`] until it is given up (it was abandoned here
/// before B X3).
pub const MAX_NACKS_PER_HOLE: u8 = 12;
pub const SLOW_NACK: Duration = Duration::from_millis(250);
/// The first NACK of a hole waits for the slower leg's lag at most this
/// long: it may still bring the part.
pub const MAX_LAG_WAIT: Duration = Duration::from_millis(20);
/// A jump of more parts than this is a resume elsewhere, not holes.
pub const MAX_HOLE_RUN: u64 = 256;
/// The most gaps that are asked for again at once: a gap is a part that
/// the edge did not have when it was due (its hole outlived the repair
/// horizon); its cache may fill it a moment later, and a NACK then brings
/// the part. Gaps past this many (an outage) are final.
pub const MAX_GAP_HOLES: usize = 64;
/// Parts that the receiver remembers (for windows and duplicates).
const MEMORY: usize = 4_096;
/// The fewest parts whose bytes a receiver keeps for window repair: a
/// window spans at most [`MAX_WINDOW`] parts.
pub const MIN_KNOWN_PARTS: usize = 2 * MAX_WINDOW as usize;
/// A part, or a window, is counted for loss once its datagrams have had
/// this long to arrive.
const SETTLE: Duration = Duration::from_millis(150);

/// The most legs of one receiver: a listener's subscriptions at different
/// edges, two wanted and a replacement or a hedge each.
pub const MAX_LEGS: usize = 4;
/// A leg whose `OK` names a part this far behind the newest part delivered
/// is stale: its edge handed out an old head (after its admission wait).
pub const STALE_PARTS: u64 = 4_096;
/// Delivered parts whose arrivals are kept, for each leg's lag.
const ARRIVALS: usize = 64;
/// The last delivered parts over which a leg is judged steady or level.
pub const JUDGED_PARTS: usize = 32;
/// A leg that brought at least this many of the judged parts is steady.
pub const STEADY_PARTS: usize = 28;
/// A leg that brought fewer of the judged parts than this has no lag to
/// be compared with: a leg beside it is level once it is steady.
pub const COMPARED_PARTS: usize = 16;
/// A leg is level with another when its median lag is at most this much
/// more than the other's.
pub const LEVEL_MARGIN: Duration = Duration::from_micros(250);

/// One leg of a receiver: the parts that one edge sends one listener.
/// Rows, loss and numbering are the leg's own; delivery is shared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LegId(u8);

impl LegId {
    /// The first leg that a receiver opens.
    pub const FIRST: Self = Self(0);

    pub const fn index(self) -> usize {
        self.0 as usize
    }

    /// The leg in slot `index` (below [`MAX_LEGS`]).
    pub const fn from_index(index: usize) -> Self {
        Self(index as u8)
    }
}

/// What a leg's `OK` means for the session (see
/// [`CompactReceiver::leg_expect`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegStart {
    /// The leg starts at the part its `OK` names.
    Started,
    /// The part its `OK` names is more than [`STALE_PARTS`] behind the
    /// newest part delivered: the leg is failed, nothing of it delivered.
    Stale,
    /// The parts delivered so far all came from this other leg, which the
    /// new leg shows to be more than [`STALE_PARTS`] behind: that leg is
    /// failed, and the session starts again at the new leg's first part.
    Displaces(LegId),
}

/// Counters of one leg.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LegStats {
    /// Compact datagrams from this leg.
    pub datagrams: u64,
    /// Parts that this leg delivered first.
    pub parts_first: u64,
    /// Datagrams of parts delivered already (by this leg or another).
    pub duplicates: u64,
    /// Datagrams of parts below the session's first part: dropped.
    pub before_start: u64,
}

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
    /// It is waiting for the rate cap's tokens, and has been counted in
    /// `nacks_capped` for that wait (once a wait, not once a poll).
    capped: bool,
    /// It was due when no leg could be asked for it: it waits
    /// [`SLOW_NACK`], or until a leg can be asked
    /// ([`CompactReceiver::set_leg_nack`]).
    waiting: bool,
}

#[derive(Debug)]
struct WindowState {
    k: u8,
    t: usize,
    rows: Vec<(u8, Vec<u8>)>,
    seen_at: Instant,
    /// A part recovered from it failed its length, CRC or number: the
    /// window is kept, and counted once.
    failed: bool,
}

#[derive(Debug, Clone, Copy)]
struct Count {
    received: u8,
    sent: u8,
    at: Instant,
}

/// The canonical bytes of a part, and the leg they came from.
#[derive(Debug)]
struct Known {
    leg: LegId,
    bytes: Vec<u8>,
}

/// When a delivered part first arrived, and when each leg brought it (an
/// original or a copy).
#[derive(Debug, Clone, Copy)]
struct Arrival {
    part: u64,
    first: Option<Instant>,
    legs: [Option<Instant>; MAX_LEGS],
}

const NO_ARRIVAL: Arrival = Arrival {
    part: u64::MAX,
    first: None,
    legs: [None; MAX_LEGS],
};

/// Whether a part delivered came as an original or a copy from some leg
/// (else it needed a row or a NACK), for the joint loss.
#[derive(Debug, Clone, Copy)]
struct Joint {
    came: bool,
    at: Instant,
}

/// What the receiver keeps of one leg.
#[derive(Debug, Default)]
struct LegRx {
    open: bool,
    /// The part its `OK` named: the reference of its part numbers until a
    /// part of it comes (kept across a new source generation, as the
    /// reference of before).
    reference: Option<u64>,
    /// The edge's first part for this subscriber (it answers NACKs from
    /// it on): set by the `OK`, cleared by a new source generation.
    first_part: Option<u64>,
    /// The highest part of this leg's own datagrams.
    highest: Option<u64>,
    /// Short-form parts of this leg that came before its `OK`.
    early: Vec<Vec<u8>>,
    windows: BTreeMap<u64, WindowState>,
    counts: BTreeMap<u64, Count>,
    window_counts: HashMap<u64, Count>,
    last_copies: u8,
    run: u64,
    burst_sum: u64,
    burst_runs: u64,
    rtt: Option<Duration>,
    /// The session may NACK on this leg (it holds the edge's token and the
    /// edge serves it the compact lane): see [`CompactReceiver::set_leg_nack`].
    nack: bool,
    /// Its median lag as last measured (each loss check), which a hole's
    /// first NACK waits for.
    lag: Option<Duration>,
    stats: LegStats,
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
    /// Datagrams of parts below the session's first part, dropped.
    pub before_start: u64,
    /// Windows whose recovery failed its check on bytes that another leg
    /// brought: two edges' bytes of a part differ (L1's byte identity).
    pub cross_leg_mismatch: u64,
}

/// The parts to ask each leg's edge for (see
/// [`CompactReceiver::nacks_due`]).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NacksDue {
    parts: [Vec<u64>; MAX_LEGS],
}

impl NacksDue {
    /// The parts to ask `leg`'s edge for.
    pub fn leg(&self, leg: LegId) -> &[u64] {
        &self.parts[leg.index()]
    }

    /// The parts to ask `leg`'s edge for, taken.
    pub fn take(&mut self, leg: LegId) -> Vec<u64> {
        std::mem::take(&mut self.parts[leg.index()])
    }

    pub fn is_empty(&self) -> bool {
        self.parts.iter().all(Vec::is_empty)
    }

    /// The legs with parts to ask for, and their parts.
    pub fn iter(&self) -> impl Iterator<Item = (LegId, &[u64])> {
        self.parts
            .iter()
            .enumerate()
            .filter(|(_, parts)| !parts.is_empty())
            .map(|(index, parts)| (LegId(index as u8), parts.as_slice()))
    }
}

/// The receiving side of the compact lane, for one listener with one leg
/// or several. The first arrival of a part number is delivered; a part's
/// canonical bytes, the holes and the part interval are shared; each leg
/// has its own part numbering, loss, burst and window repair, so that rows
/// from two edges never meet in one decode.
#[derive(Debug, Default)]
pub struct CompactReceiver {
    /// The edge sends this client the compact lane: holes are tracked for
    /// NACK and counted as loss (see [`Self::leg_expect`]).
    active: bool,
    legs: [LegRx; MAX_LEGS],
    known: HashMap<u64, Known>,
    known_order: VecDeque<u64>,
    delivered: HashSet<u64>,
    delivered_order: VecDeque<u64>,
    /// The newest part delivered.
    highest: Option<u64>,
    /// The session's first part (the first delivered, or the part that
    /// its leg's `OK` named before it) and the leg that set it: another
    /// leg's parts below it are dropped (see [`Self::below_floor`]).
    floor: Option<u64>,
    floor_leg: Option<LegId>,
    /// The leg whose bytes of a part replace another leg's (the repair
    /// leg): its rows are coded over its edge's bytes.
    prefer_leg: Option<LegId>,
    holes: BTreeMap<u64, Hole>,
    /// Parts delivered as gaps: the part itself, if it comes, is delivered
    /// after its gap.
    gapped: BTreeSet<u64>,
    /// When the highest part last advanced, and the interval between
    /// parts (an average of the originals' arrivals).
    highest_at: Option<Instant>,
    interval: Option<Duration>,
    /// Samples taken into `interval` since it was last seeded, and the
    /// samples in a row rejected as longer than `interval_cap` intervals
    /// (see [`Self::interval_sample_taken`]).
    interval_samples: u32,
    interval_long_rejects: u32,
    /// The mean deviation of the originals' spacing from the interval (as
    /// RFC 3550's jitter): the silence's margin covers four of it.
    jitter: Duration,
    /// See [`NackTiming`]; `None` is the default.
    timing: Option<NackTiming>,
    /// The `highest_at` whose silence has been noted: the silence's own
    /// holes wake the client after that.
    silence_noted: Option<Instant>,
    /// NACKs of parts sent during the current silence (wave 8 budget).
    silence_spent: u16,
    /// The rate cap's tokens (parts) and when they were last filled.
    nack_tokens: f64,
    nack_tokens_at: Option<Instant>,
    /// NACKs of parts held back by the rate cap.
    pub nacks_capped: u64,
    /// Drop short-form parts that come before their leg's `OK`, as before
    /// wave 8 (wave 8: a subscribe spike delays the `OK` behind the fast
    /// lane's first parts; they were dropped, and not asked for again).
    pub drop_early: bool,
    /// Short-form parts kept until their leg's `OK`.
    pub early_kept: u64,
    /// The source generation of the parts, once a datagram named one, and
    /// the generations before it (their late datagrams are dropped).
    generation: Option<u8>,
    old_generations: VecDeque<u8>,
    /// The publication time of the last long-form part (its 32 bits,
    /// extended by the local clock) and when it arrived: the reference of
    /// the short form's 24 bits, advanced by the time since.
    time_anchor: Option<(u64, Instant)>,
    /// Parts whose bytes are kept for window repair (0: [`MEMORY`]).
    known_parts: usize,
    /// When a hole is given up (`None`: [`HOLE_GIVE_UP`]).
    hole_give_up: Option<Duration>,
    /// The arrivals of the last [`ARRIVALS`] parts delivered, by part
    /// number modulo [`ARRIVALS`] (sized on first use).
    arrivals: Vec<Arrival>,
    /// Parts delivered or missed, until they settle, for the joint loss;
    /// and the runs of parts that no leg brought.
    joint: BTreeMap<u64, Joint>,
    joint_run: u64,
    joint_run_sum: u64,
    joint_runs: u64,
    pub stats: CompactStats,
}

impl CompactReceiver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Keep the bytes of the last `parts` parts for window repair (at
    /// least [`MIN_KNOWN_PARTS`]), not 4,096: a load host that runs
    /// thousands of receivers keeps a few hundred each.
    pub fn set_known_parts(&mut self, parts: usize) {
        self.known_parts = parts.max(MIN_KNOWN_PARTS);
    }

    /// Give a hole up after `after` (`--hole-give-up-ms`).
    pub fn set_hole_give_up(&mut self, after: Duration) {
        self.hole_give_up = Some(after);
    }

    pub fn hole_give_up(&self) -> Duration {
        self.hole_give_up.unwrap_or(HOLE_GIVE_UP)
    }

    /// Whether the session NACKs on `leg` (a leg is open with it on).
    pub fn set_leg_nack(&mut self, leg: LegId, nack: bool) {
        let rx = &mut self.legs[leg.index()];
        let now_asked = nack && rx.open && !rx.nack;
        rx.nack = nack;
        if now_asked {
            self.rearm_waiting();
        }
    }

    /// The holes that were due when no leg could be asked for them are due
    /// now: a leg can be.
    fn rearm_waiting(&mut self) {
        for hole in self.holes.values_mut().filter(|hole| hole.waiting) {
            hole.waiting = false;
            hole.next_nack = hole.next_nack.min(hole.since);
        }
    }

    /// The wait before the first NACK of a hole that a later part showed:
    /// the timing's delay, and the largest lag of a leg (the slower leg may
    /// still bring the part), at most [`MAX_LAG_WAIT`] more.
    fn first_nack_wait(&self) -> Duration {
        let lag = self
            .legs
            .iter()
            .filter(|rx| rx.open)
            .filter_map(|rx| rx.lag)
            .max()
            .unwrap_or(Duration::ZERO);
        self.nack_timing().delay + lag.min(MAX_LAG_WAIT)
    }

    fn known_limit(&self) -> usize {
        if self.known_parts == 0 {
            MEMORY
        } else {
            self.known_parts
        }
    }

    /// A new leg, if fewer than [`MAX_LEGS`] are open.
    pub fn open_leg(&mut self) -> Option<LegId> {
        let index = self.legs.iter().position(|leg| !leg.open)?;
        self.legs[index] = LegRx {
            open: true,
            nack: true,
            ..LegRx::default()
        };
        self.rearm_waiting();
        Some(LegId(index as u8))
    }

    /// Forget a leg: its windows, counts and early parts. What it
    /// delivered stays delivered.
    pub fn close_leg(&mut self, leg: LegId) {
        self.legs[leg.index()] = LegRx::default();
        if self.prefer_leg == Some(leg) {
            self.prefer_leg = None;
        }
        if self.floor_leg == Some(leg) {
            self.floor_leg = None;
        }
    }

    pub fn is_open(&self, leg: LegId) -> bool {
        self.legs[leg.index()].open
    }

    /// The leg whose bytes of a part are kept over another leg's (the
    /// repair leg, whose edge codes its rows over its own bytes).
    pub fn set_prefer_leg(&mut self, leg: Option<LegId>) {
        self.prefer_leg = leg;
    }

    pub fn leg_stats(&self, leg: LegId) -> LegStats {
        self.legs[leg.index()].stats
    }

    /// The highest part of a leg's own datagrams.
    pub fn leg_highest(&self, leg: LegId) -> Option<u64> {
        self.legs[leg.index()].highest
    }

    /// The part that a leg's `OK` named (the edge's first part for it).
    pub fn leg_first_part(&self, leg: LegId) -> Option<u64> {
        self.legs[leg.index()].first_part
    }

    /// The newest part delivered, and the session's first part.
    pub fn newest(&self) -> Option<u64> {
        self.highest
    }

    pub fn floor(&self) -> Option<u64> {
        self.floor
    }

    /// A leg's `OK`: the part that its edge sends it first (the reference
    /// for its 32-bit part numbers until a part of it arrives), and whether
    /// it sends the compact lane (holes are then asked for with NACK).
    pub fn leg_expect(&mut self, leg: LegId, part: u64, compact: bool) -> LegStart {
        if let Some(newest) = self.highest {
            if part.saturating_add(STALE_PARTS) < newest {
                return LegStart::Stale;
            }
            if part > newest.saturating_add(STALE_PARTS) {
                let mut first = self
                    .legs
                    .iter()
                    .enumerate()
                    .filter(|(_, other)| other.open && other.stats.parts_first > 0);
                if let (Some((only, _)), None) = (first.next(), first.next()) {
                    let only = LegId(only as u8);
                    if only != leg {
                        self.reset_delivery();
                        let rx = &mut self.legs[leg.index()];
                        rx.reference = Some(part);
                        rx.first_part = Some(part);
                        self.active |= compact;
                        return LegStart::Displaces(only);
                    }
                }
            }
        }
        let rx = &mut self.legs[leg.index()];
        rx.reference = Some(part);
        rx.first_part = Some(part);
        self.active |= compact;
        // Parts of this leg that came before this OK (an earlier OK was
        // lost and the subscription asked again): the ones from the part
        // it names up to the highest that came are holes, to ask for (wave
        // 7 gate: under bursts a client whose OK was lost dropped the short
        // parts that came first, and never asked for them again).
        if let Some(highest) = rx.highest {
            let now = Instant::now();
            self.leg_losses(leg, part, highest, now);
            if self.floor_leg == Some(leg)
                && self
                    .floor
                    .is_some_and(|floor| part < floor && floor - part <= STALE_PARTS)
            {
                self.floor = Some(part);
            }
            self.note_holes(part, highest, now);
        }
        LegStart::Started
    }

    /// Parts `from..to` that have not been delivered are holes (none if
    /// the run is longer than [`MAX_HOLE_RUN`], or the lane is not
    /// compact, or below the session's first part).
    fn note_holes(&mut self, from: u64, to: u64, now: Instant) {
        if !self.active || to <= from || to - from > MAX_HOLE_RUN {
            return;
        }
        let from = self.floor.map_or(from, |floor| from.max(floor));
        let delay = self.first_nack_wait();
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
                capped: false,
                waiting: false,
            });
            self.joint.entry(hole).or_insert(Joint {
                came: false,
                at: now,
            });
        }
    }

    /// Parts `from..to` that a leg has not brought count as its losses
    /// once they settle (none if the run is longer than [`MAX_HOLE_RUN`],
    /// or the lane is not compact).
    fn leg_losses(&mut self, leg: LegId, from: u64, to: u64, now: Instant) {
        if !self.active || to <= from || to - from > MAX_HOLE_RUN {
            return;
        }
        let rx = &mut self.legs[leg.index()];
        let sent = 1 + rx.last_copies;
        for part in from..to {
            rx.counts.entry(part).or_insert(Count {
                received: 0,
                sent,
                at: now,
            });
        }
    }

    /// A leg brought `part` (an original, a copy or a RaptorQ object): its
    /// highest part moves, and the parts it skipped are its losses.
    fn leg_reached(&mut self, leg: LegId, part: u64, now: Instant) {
        let rx = &self.legs[leg.index()];
        match (rx.highest, rx.first_part) {
            (Some(highest), _) if part > highest => {
                self.leg_losses(leg, highest + 1, part, now);
                self.legs[leg.index()].highest = Some(part);
            }
            (Some(_), _) => {}
            (None, first) => {
                // Its first part: the parts from the one its OK named up
                // to it were lost on the way.
                if let Some(first) = first {
                    self.leg_losses(leg, first, part, now);
                }
                self.legs[leg.index()].highest = Some(part);
            }
        }
    }

    /// The short-form parts of a leg kept until its `OK` (see `early`), to
    /// push now.
    pub fn take_early(&mut self, leg: LegId) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.legs[leg.index()].early)
    }

    /// Forget every part delivered: the session starts again (a stale leg
    /// displaced, or a new source epoch).
    fn reset_delivery(&mut self) {
        self.known.clear();
        self.known_order.clear();
        self.delivered.clear();
        self.delivered_order.clear();
        self.highest = None;
        self.highest_at = None;
        self.floor = None;
        self.floor_leg = None;
        self.silence_noted = None;
        self.silence_spent = 0;
        self.holes.clear();
        self.gapped.clear();
        self.arrivals.clear();
        self.joint.clear();
        self.joint_run = 0;
    }

    /// Forget everything of the last source epoch, whose part numbers the
    /// new one counts again: the parts delivered and every leg's windows,
    /// counts and early parts. The legs stay open, with their round trips.
    pub fn new_flow(&mut self) {
        self.reset_delivery();
        for rx in self.legs.iter_mut().filter(|rx| rx.open) {
            *rx = LegRx {
                open: true,
                reference: rx.reference,
                rtt: rx.rtt,
                nack: rx.nack,
                stats: rx.stats,
                ..LegRx::default()
            };
        }
        self.time_anchor = None;
    }

    /// A round trip of a leg's path to its edge (a request and its
    /// answer): NACKs are not repeated sooner than an answer can come.
    pub fn set_leg_rtt(&mut self, leg: LegId, sample: Duration) {
        let rx = &mut self.legs[leg.index()];
        rx.rtt = Some(match rx.rtt {
            Some(rtt) => (rtt * 7 + sample) / 8,
            None => sample,
        });
    }

    pub fn leg_rtt(&self, leg: LegId) -> Option<Duration> {
        self.legs[leg.index()].rtt
    }

    /// The shortest round trip of an open leg.
    fn shortest_rtt(&self) -> Option<Duration> {
        self.legs
            .iter()
            .filter(|rx| rx.open)
            .filter_map(|rx| rx.rtt)
            .min()
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

    /// Whether `sample` (the spacing of two originals' arrivals, per part)
    /// is a sample of the interval, with wave 8's `interval_cap` (R6-5): a
    /// gap longer than `interval_cap` intervals (a stall) is not, nor,
    /// once [`INTERVAL_WARMUP_SAMPLES`] samples are in, one shorter than
    /// the interval divided by the cap (a burst, such as the backlog after
    /// a stall or a resume's catch-up). The estimate is seeded by the first
    /// sample, which may be short (the first part comes with the `OK`, the
    /// next any time after): [`INTERVAL_RESEED_AFTER`] long samples in a
    /// row seed it again, so a short seed does not reject every later
    /// sample for good (wave 8 review: the estimate could only fall).
    fn interval_sample_taken(&mut self, sample: Duration) -> bool {
        let cap = self.nack_timing().interval_cap;
        let Some(interval) = self.interval.filter(|_| cap != 0) else {
            return true;
        };
        if sample > interval * cap {
            self.interval_long_rejects += 1;
            if self.interval_long_rejects >= INTERVAL_RESEED_AFTER {
                self.interval = None;
                self.jitter = Duration::ZERO;
                self.interval_samples = 0;
                self.interval_long_rejects = 0;
                return true;
            }
            return false;
        }
        self.interval_long_rejects = 0;
        !(self.interval_samples >= INTERVAL_WARMUP_SAMPLES && sample * cap < interval)
    }

    /// The silence after the highest part past which the next parts are
    /// NACKed: one interval for the next part, and a margin.
    fn silence_after(&self) -> Option<Duration> {
        let interval = self.interval?;
        let margin = interval * self.nack_timing().margin_intervals.max(1);
        Some(interval + margin.max(SILENCE_MARGIN).max(self.jitter * 4))
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
                capped: false,
                waiting: false,
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
        let every = self.interval.unwrap_or(SILENCE_RETRY).max(SILENCE_RETRY);
        let every = if self.nack_timing().rtt_floor {
            every.max(self.nack_retry())
        } else {
            every
        };
        (silent >= after && silent <= SILENCE_NACK_FOR).then_some(every)
    }

    /// The wait between the first two NACKs of a hole: [`NACK_RETRY`], or
    /// one and a half round trips of the nearest leg if that is longer.
    pub fn nack_retry(&self) -> Duration {
        let floor = self.nack_timing().retry_floor;
        self.shortest_rtt()
            .map_or(floor, |rtt| floor.max(rtt * 3 / 2))
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
                    capped: false,
                    waiting: false,
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

    /// A part came on a RaptorQ lane of `leg`: it is delivered (unless it
    /// is below the session's first part), and it is no compact part whose
    /// loss counts. `false` if it had been delivered or is dropped.
    pub fn delivered_elsewhere(&mut self, leg: LegId, part: u64, now: Instant) -> bool {
        self.leg_reached(leg, part, now);
        self.legs[leg.index()].counts.remove(&part);
        if self.below_floor(leg, part) {
            return false;
        }
        self.record_leg_arrival(leg, part, now);
        let first = self.mark_delivered(leg, part, now);
        self.joint
            .entry(part)
            .or_insert(Joint {
                came: true,
                at: now,
            })
            .came = true;
        first
    }

    /// A part number from its low 32 bits, extended around the leg's own
    /// last part, else the part its `OK` named, else the newest part
    /// delivered.
    fn part_number(&self, leg: LegId, low: u32) -> u64 {
        let rx = &self.legs[leg.index()];
        extend_u32(
            rx.highest.or(rx.reference).or(self.highest).unwrap_or(0),
            low,
        )
    }

    /// Whether `part` of `leg` is below the session's first part, and so
    /// dropped (counted on the session and on the leg). The leg that set
    /// the first part brings older parts of its own (a resume from a hole,
    /// whose backfill comes after the fast lane's live parts): they lower
    /// the first part, unless they are [`STALE_PARTS`] below it.
    fn below_floor(&mut self, leg: LegId, part: u64) -> bool {
        let Some(floor) = self.floor.filter(|floor| part < *floor) else {
            return false;
        };
        if self.floor_leg == Some(leg) && floor - part <= STALE_PARTS {
            self.floor = Some(part);
            return false;
        }
        self.stats.before_start += 1;
        self.legs[leg.index()].stats.before_start += 1;
        true
    }

    /// Keep a part's canonical bytes for window repair. The bytes of the
    /// preferred leg replace another leg's; no other bytes replace any.
    /// `true` when they replaced different bytes.
    fn remember(&mut self, leg: LegId, part: u64, datagram: Vec<u8>) -> bool {
        if let Some(known) = self.known.get_mut(&part) {
            if known.leg != leg && self.prefer_leg == Some(leg) {
                known.leg = leg;
                if known.bytes != datagram {
                    known.bytes = datagram;
                    return true;
                }
            }
            return false;
        }
        self.known.insert(
            part,
            Known {
                leg,
                bytes: datagram,
            },
        );
        self.known_order.push_back(part);
        if self.known_order.len() > self.known_limit() {
            if let Some(old) = self.known_order.pop_front() {
                self.known.remove(&old);
            }
        }
        false
    }

    /// Whether `part` has been delivered (by any lane).
    pub fn is_delivered(&self, part: u64) -> bool {
        self.delivered.contains(&part)
    }

    /// The arrival record of `part`, made anew when the part is newer
    /// than the one in its place; `None` for a part older than that.
    fn arrival(&mut self, part: u64) -> Option<&mut Arrival> {
        if self.arrivals.is_empty() {
            self.arrivals = vec![NO_ARRIVAL; ARRIVALS];
        }
        let slot = &mut self.arrivals[(part % ARRIVALS as u64) as usize];
        if slot.part == u64::MAX || slot.part < part {
            *slot = Arrival { part, ..NO_ARRIVAL };
        }
        (slot.part == part).then_some(slot)
    }

    /// A leg brought `part` (an original or a copy): its arrival, for the
    /// leg's lag.
    fn record_leg_arrival(&mut self, leg: LegId, part: u64, now: Instant) {
        if let Some(arrival) = self.arrival(part) {
            arrival.legs[leg.index()].get_or_insert(now);
        }
    }

    /// A part was delivered, by `leg` on this lane or another: holes
    /// before it are noted, and it is no longer one. `false` if it had been
    /// delivered.
    pub fn mark_delivered(&mut self, leg: LegId, part: u64, now: Instant) -> bool {
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
        if let Some(arrival) = self.arrival(part) {
            arrival.first.get_or_insert(now);
        }
        self.legs[leg.index()].stats.parts_first += 1;
        if self.floor.is_none() {
            // The session's first part: this one, or the part its leg's OK
            // named when that is before it (those between were lost on the
            // way, or come after it).
            let first = self.legs[leg.index()]
                .first_part
                .filter(|first| *first <= part && part - *first <= STALE_PARTS);
            self.floor = Some(first.unwrap_or(part));
            self.floor_leg = Some(leg);
        }
        let delay = self.first_nack_wait();
        let timing = self.nack_timing();
        let retry = self.nack_retry();
        match self.highest {
            Some(highest) if part > highest => {
                self.highest_at = Some(now);
                self.silence_spent = 0;
                if timing.silence_budget > 0 {
                    // The holes past this part that a silence asked for
                    // wait for the next silence (wave 8, R6-2): not NACKed
                    // between the datagrams of a backlog.
                    let after = self.silence_after().unwrap_or(SILENCE_NACK_FOR);
                    for (_, hole) in self.holes.range_mut(part + 1..) {
                        if !hole.gap {
                            hole.next_nack = now + after;
                            hole.silence_nacks = 0;
                        }
                    }
                }
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
                                capped: false,
                                waiting: false,
                            });
                            // Asked for during the silence that this part
                            // ends: the answers went into the burst, so it
                            // is asked for again now, as a new hole is (no
                            // sooner than the retry after its last NACK,
                            // with the RTT floor: an answer may be on its
                            // way).
                            if entry.silence_nacks > 0 {
                                let at = match (timing.rtt_floor, entry.last_nack) {
                                    (true, Some(last)) => (now + delay).max(last + retry),
                                    _ => now + delay,
                                };
                                entry.next_nack = if timing.silence_budget > 0 {
                                    at
                                } else {
                                    entry.next_nack.min(at)
                                };
                            }
                            self.joint.entry(hole).or_insert(Joint {
                                came: false,
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
                if let Some(first) = self.legs[leg.index()].first_part {
                    self.note_holes(first, part, now);
                }
            }
        }
        true
    }

    /// Push one compact datagram of `leg`; returns the parts it delivered
    /// (itself, and any that a window's repair recovered with it).
    pub fn push(
        &mut self,
        leg: LegId,
        datagram: &[u8],
        now: Instant,
        now_unix_us: u64,
    ) -> Vec<CompactPart> {
        // A part in the short form is read in its long form, which is what
        // the window repair covers.
        let long;
        let mut anchored = true;
        let datagram = if is_short(datagram) {
            // Its 16 bits of part number need a reference of its own leg:
            // the leg's OK or a part. Before either, it waits for the OK.
            let rx = &mut self.legs[leg.index()];
            let Some(reference) = rx.highest.or(rx.reference) else {
                if !self.drop_early && rx.early.len() < MAX_EARLY {
                    rx.early.push(datagram.to_vec());
                    self.early_kept += 1;
                    return Vec::new();
                }
                self.stats.datagrams += 1;
                self.stats.errors += 1;
                return Vec::new();
            };
            // Its 24 bits of time are extended around the edge's clock as
            // the last long-form part gave it. Before any, around the
            // client's own clock, which may be more than 8 s off the
            // source's: its long form may then be wrong, and is not kept
            // for window repair (a wrong part there would also read as a
            // cross-leg mismatch).
            let reference_us = match self.time_anchor {
                Some((published, at)) => {
                    published + now.saturating_duration_since(at).as_micros() as u64
                }
                None => {
                    anchored = false;
                    now_unix_us
                }
            };
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
        self.legs[leg.index()].stats.datagrams += 1;
        let Some(view) = parse(datagram) else {
            self.stats.errors += 1;
            return Vec::new();
        };
        if !self.take_generation(view.generation) {
            self.stats.stale_generation += 1;
            return Vec::new();
        }
        self.active = true;
        let part = self.part_number(leg, view.part_low);
        let mut out = Vec::new();
        match view.kind {
            Kind::Repair => {
                self.stats.repair_datagrams += 1;
                let [k, row, rows, _] = view.field;
                if k == 0 || k > MAX_WINDOW || usize::from(row) >= MAX_ROWS {
                    self.stats.errors += 1;
                    return out;
                }
                let rx = &mut self.legs[leg.index()];
                let count = rx.window_counts.entry(part).or_insert(Count {
                    received: 0,
                    sent: rows,
                    at: now,
                });
                count.received = count.received.saturating_add(1);
                let window = rx.windows.entry(part).or_insert(WindowState {
                    k,
                    t: view.payload.len(),
                    rows: Vec::new(),
                    seen_at: now,
                    failed: false,
                });
                if window.k == k
                    && window.t == view.payload.len()
                    && !window.rows.iter().any(|(seen, _)| *seen == row)
                {
                    window.rows.push((row, view.payload.to_vec()));
                }
                self.try_window(leg, part, now, now_unix_us, &mut out);
            }
            Kind::Part | Kind::Gap => {
                if view.retransmit {
                    self.stats.retransmits += 1;
                    // The answer to the only NACK of a hole: a round trip
                    // of the leg it came from.
                    if let Some(Hole {
                        nacks: 1,
                        last_nack: Some(at),
                        silence_nacks: 0,
                        ..
                    }) = self.holes.get(&part)
                    {
                        let sample = now.saturating_duration_since(*at);
                        self.set_leg_rtt(leg, sample);
                    }
                } else {
                    // The interval between parts, from the originals that
                    // advance the highest part.
                    if let (Some(highest), Some(at)) = (self.highest, self.highest_at) {
                        let sample = (part > highest && part - highest <= MAX_HOLE_RUN)
                            .then(|| now.saturating_duration_since(at) / (part - highest) as u32)
                            // Parts read together say nothing of the interval.
                            .filter(|sample| *sample >= MIN_INTERVAL_SAMPLE)
                            // Nor does a stall, or a burst (wave 8, R6-5).
                            .filter(|sample| self.interval_sample_taken(*sample));
                        if let Some(sample) = sample {
                            if let Some(interval) = self.interval {
                                let deviation = sample.abs_diff(interval);
                                self.jitter = (self.jitter * 15 + deviation) / 16;
                            }
                            self.interval = Some(match self.interval {
                                Some(interval) => (interval * 7 + sample) / 8,
                                None => sample,
                            });
                            self.interval_samples = self.interval_samples.saturating_add(1);
                        }
                    }
                    // The leg's own loss: its parts and their copies.
                    self.leg_reached(leg, part, now);
                    let rx = &mut self.legs[leg.index()];
                    rx.last_copies = view.copies;
                    let count = rx.counts.entry(part).or_insert(Count {
                        received: 0,
                        sent: 1 + view.copies,
                        at: now,
                    });
                    count.received = count.received.saturating_add(1);
                    count.sent = 1 + view.copies;
                }
                if self.below_floor(leg, part) {
                    return out;
                }
                if !view.retransmit {
                    self.record_leg_arrival(leg, part, now);
                    if let Some(joint) = self.joint.get_mut(&part) {
                        joint.came = true;
                    }
                }
                let canonical = canonical(&datagram[..HEADER_LEN + view.payload.len()]);
                // The part after its gap: the edge got it late. It is
                // delivered (once), and replaces the gap's bytes.
                if view.kind == Kind::Part && self.gapped.remove(&part) {
                    self.holes.remove(&part);
                    if anchored {
                        self.known.insert(
                            part,
                            Known {
                                leg,
                                bytes: canonical,
                            },
                        );
                    } else {
                        self.known.remove(&part);
                    }
                    out.push(self.part_of(part, &view, now_unix_us, false));
                    self.try_windows_with(part, now, now_unix_us, &mut out);
                    return out;
                }
                if view.kind == Kind::Gap && self.delivered.contains(&part) {
                    self.stats.duplicates += 1;
                    self.legs[leg.index()].stats.duplicates += 1;
                    return out;
                }
                let replaced = anchored && self.remember(leg, part, canonical);
                if !self.mark_delivered(leg, part, now) {
                    self.stats.duplicates += 1;
                    self.legs[leg.index()].stats.duplicates += 1;
                    if replaced {
                        // The preferred leg's bytes: a window kept after a
                        // failed check may now recover.
                        self.try_windows_with(part, now, now_unix_us, &mut out);
                    }
                    return out;
                }
                self.joint
                    .entry(part)
                    .or_insert(Joint {
                        came: false,
                        at: now,
                    })
                    .came |= !view.retransmit;
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

    /// Try the windows of every leg that hold `part`.
    fn try_windows_with(
        &mut self,
        part: u64,
        now: Instant,
        now_unix_us: u64,
        out: &mut Vec<CompactPart>,
    ) {
        let mut firsts = Vec::new();
        for (index, rx) in self.legs.iter().enumerate() {
            firsts.extend(
                rx.windows
                    .range(part.saturating_sub(u64::from(MAX_WINDOW))..=part)
                    .filter(|(first, window)| part < **first + u64::from(window.k))
                    .map(|(first, _)| (LegId(index as u8), *first)),
            );
        }
        for (leg, first) in firsts {
            self.try_window(leg, first, now, now_unix_us, out);
        }
    }

    /// Recover what a window's repair can, and then what the recovered parts
    /// make recoverable in the other windows (of any leg). A window whose
    /// recovered part fails its check is kept, to be tried again when other
    /// bytes or a NACK's answer come.
    fn try_window(
        &mut self,
        leg: LegId,
        first: u64,
        now: Instant,
        now_unix_us: u64,
        out: &mut Vec<CompactPart>,
    ) {
        let mut queue = vec![(leg, first)];
        while let Some((leg, first)) = queue.pop() {
            let Some(window) = self.legs[leg.index()].windows.get(&first) else {
                continue;
            };
            let parts = first..first + u64::from(window.k);
            if parts.clone().all(|part| self.delivered.contains(&part)) {
                self.legs[leg.index()].windows.remove(&first);
                continue;
            }
            let known: Vec<Option<&[u8]>> = parts
                .clone()
                .map(|part| self.known.get(&part).map(|known| known.bytes.as_slice()))
                .collect();
            let rows: Vec<(u8, &[u8])> = window
                .rows
                .iter()
                .map(|(row, parity)| (*row, parity.as_slice()))
                .collect();
            let Some(recovered) = recover_window(&known, window.t, &rows) else {
                continue;
            };
            let mut checked = true;
            for (index, padded) in recovered {
                let part = first + index as u64;
                // The edge's bytes of a part differ from the ones the
                // client has: the window is kept, and NACK fetches the part.
                let Some(view) = parse(&padded) else {
                    checked = false;
                    continue;
                };
                if self.part_number(leg, view.part_low) != part {
                    checked = false;
                    continue;
                }
                let datagram = padded[..HEADER_LEN + view.payload.len()].to_vec();
                self.remember(leg, part, datagram);
                if self.mark_delivered(leg, part, now) {
                    self.joint.entry(part).or_insert(Joint {
                        came: false,
                        at: now,
                    });
                    if view.kind == Kind::Gap {
                        self.note_gap(part, now);
                    }
                    out.push(self.part_of(part, &view, now_unix_us, true));
                    // Other windows that held it may now be complete enough.
                    for (index, rx) in self.legs.iter().enumerate() {
                        queue.extend(
                            rx.windows
                                .range(part.saturating_sub(u64::from(MAX_WINDOW))..=part)
                                .filter(|(other, window)| {
                                    (index != leg.index() || **other != first)
                                        && part < **other + u64::from(window.k)
                                })
                                .map(|(other, _)| (LegId(index as u8), *other)),
                        );
                    }
                }
            }
            if checked {
                self.legs[leg.index()].windows.remove(&first);
                continue;
            }
            let foreign = (first..first + u64::from(self.legs[leg.index()].windows[&first].k))
                .any(|part| self.known.get(&part).is_some_and(|known| known.leg != leg));
            let window = self.legs[leg.index()]
                .windows
                .get_mut(&first)
                .expect("the window tried");
            if !window.failed {
                window.failed = true;
                self.stats.errors += 1;
                if foreign {
                    self.stats.cross_leg_mismatch += 1;
                }
            }
        }
    }

    /// The legs that may be asked for `part`: open, NACKed on by the
    /// session, and whose edge's first part is at or before it (an edge
    /// answers no NACK of an older part).
    fn eligible(&self, part: u64) -> [bool; MAX_LEGS] {
        let mut eligible = [false; MAX_LEGS];
        for (slot, rx) in eligible.iter_mut().zip(&self.legs) {
            *slot = rx.open && rx.nack && rx.first_part.is_none_or(|first| first <= part);
        }
        eligible
    }

    /// The eligible leg with the shortest round trip (a leg without one
    /// last).
    fn nearest(&self, eligible: &[bool; MAX_LEGS]) -> Option<usize> {
        (0..MAX_LEGS)
            .filter(|index| eligible[*index])
            .min_by_key(|index| self.legs[*index].rtt.unwrap_or(Duration::MAX))
    }

    /// The parts to ask the edges for now, per leg: holes whose time has
    /// come, at most `limit` parts in all (a part asked of two legs counts
    /// once against the rate cap). A hole's first NACK goes to the eligible
    /// leg with the shortest round trip; its later ones to every eligible
    /// leg. After [`MAX_NACKS_PER_HOLE`] a hole is asked for every
    /// [`SLOW_NACK`]; it is given up after [`Self::hole_give_up`] or
    /// [`GIVE_UP_PARTS`] behind the newest part.
    pub fn nacks_due(&mut self, now: Instant, limit: usize) -> NacksDue {
        // The rate cap (wave 8): at most the bucket's tokens of parts.
        let cap = self.nack_timing().rate_cap;
        let asked_limit = limit;
        let limit = if cap == 0 {
            limit
        } else {
            let burst = f64::from(cap) / 4.0;
            let since = self.nack_tokens_at.map_or(burst, |at| {
                now.saturating_duration_since(at).as_secs_f64() * f64::from(cap)
            });
            self.nack_tokens = (self.nack_tokens + since).min(burst);
            self.nack_tokens_at = Some(now);
            let allowed = (self.nack_tokens + 1e-6).floor() as usize;
            limit.min(allowed)
        };
        let capping = limit < asked_limit;
        let mut capped = 0_u64;
        self.note_silence(now);
        let silent = self.silent(now);
        let mut due = NacksDue::default();
        let mut asked = 0_usize;
        let mut abandoned = Vec::new();
        let retry = self.nack_retry();
        let most = MAX_NACK_RETRY.max(retry);
        let highest = self.highest;
        let budget = self.nack_timing().silence_budget;
        let give_up = self.hole_give_up();
        let mut spent = self.silence_spent;
        let parts: Vec<u64> = self.holes.keys().copied().collect();
        for part in parts {
            let eligible = self.eligible(part);
            let nearest = self.nearest(&eligible);
            let hole = self.holes.get_mut(&part).expect("a hole listed");
            if now.duration_since(hole.since) >= give_up
                || highest.is_some_and(|highest| highest.saturating_sub(part) >= GIVE_UP_PARTS)
            {
                abandoned.push((part, hole.gap));
                continue;
            }
            // Wave 8 (R6-3): a hole past the highest part is the silence's
            // own: asked for only during the silence's first 50 ms, within
            // its budget, and then it waits for a datagram (which re-arms
            // it), not the ordinary schedule.
            if budget > 0
                && hole.next_nack <= now
                && highest.is_none_or(|highest| part > highest)
                && !hole.gap
                && (silent.is_none() || spent >= budget)
            {
                // Parked: a datagram re-arms it (as a hole a silence asked
                // for, below the new highest part; for the next silence,
                // above it).
                hole.next_nack = hole.since + give_up;
                hole.silence_nacks = hole.silence_nacks.max(1);
                continue;
            }
            if capping && hole.next_nack <= now && asked >= limit {
                // Past what the rate cap allows: it waits (counted once a
                // wait).
                if !hole.capped {
                    hole.capped = true;
                    capped += 1;
                }
                continue;
            }
            if hole.next_nack > now || asked >= limit {
                continue;
            }
            let Some(nearest) = nearest else {
                // No edge would answer it now (a leg not yet live, or every
                // leg's edge started after it): it waits for a leg that can
                // be asked, or the slow NACK's time, not every retry.
                hole.next_nack = now + SLOW_NACK;
                hole.waiting = true;
                continue;
            };
            hole.waiting = false;
            let first = hole.nacks == 0 && hole.silence_nacks == 0;
            hole.capped = false;
            if silent.is_some() && budget > 0 {
                spent = spent.saturating_add(1);
            }
            // While the stream is silent, the holes are asked for again
            // and again (what the edge sends into a burst is lost): each
            // interval at first, then less often, and at most
            // SILENCE_NACKS_PER_HOLE times, so that a long burst does not
            // spend the edge's NACK budget (200 parts in 100 ms); the part
            // that ends the silence asks for them again.
            if let Some(every) = silent {
                if hole.silence_nacks >= SILENCE_NACKS_PER_HOLE {
                    hole.next_nack = now + SILENCE_NACK_FOR;
                    continue;
                }
                hole.silence_nacks += 1;
                hole.next_nack = now + every * (1 << (hole.silence_nacks / 2).min(3));
            } else {
                hole.nacks = hole.nacks.saturating_add(1);
                hole.next_nack = now
                    + if hole.nacks >= MAX_NACKS_PER_HOLE {
                        SLOW_NACK
                    } else {
                        retry.saturating_mul(1 << (hole.nacks - 1).min(8)).min(most)
                    };
            }
            hole.last_nack = Some(now);
            asked += 1;
            if first {
                due.parts[nearest].push(part);
            } else {
                for (index, eligible) in eligible.iter().enumerate() {
                    if *eligible {
                        due.parts[index].push(part);
                    }
                }
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
        self.silence_spent = spent;
        self.nacks_capped += capped;
        if cap > 0 {
            self.nack_tokens -= asked as f64;
        }
        self.stats.nacked += asked as u64;
        due
    }

    /// When the next NACK is due, if a hole waits, or when the stream will
    /// have been silent long enough to NACK the parts due.
    pub fn next_nack_at(&self) -> Option<Instant> {
        let holes = self.holes.values().map(|hole| hole.next_nack).min();
        // Under the rate cap, not before a token is there (no spin).
        let cap = self.nack_timing().rate_cap;
        let holes = match (holes, self.nack_tokens_at) {
            (Some(at), Some(filled)) if cap > 0 && self.nack_tokens < 1.0 => Some(
                at.max(
                    filled
                        + Duration::from_secs_f64((1.0 - self.nack_tokens) / f64::from(cap))
                            .max(MIN_CAPPED_WAIT),
                ),
            ),
            (holes, _) => holes,
        };
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

    /// The oldest part this receiver still misses (a hole it NACKs).
    pub fn lowest_hole(&self) -> Option<u64> {
        self.holes.keys().next().copied()
    }

    /// Settle a leg's counts of parts and windows older than the settle
    /// time: (datagrams received, datagrams sent) of the originals and
    /// copies of its parts and of its window repair since the last call.
    /// Runs of datagrams lost in a row are counted as bursts: all the
    /// datagrams of a part lost whole, and the lost ones of a part that
    /// came (which end the run). A copy lost with its part is part of the
    /// burst, so a burst that copies cannot beat reads as one.
    pub fn take_counts(&mut self, leg: LegId, now: Instant) -> (u64, u64) {
        let rx = &mut self.legs[leg.index()];
        let settled: Vec<u64> = rx
            .counts
            .iter()
            .take_while(|(_, count)| now.duration_since(count.at) >= SETTLE)
            .map(|(part, _)| *part)
            .collect();
        let (mut received, mut sent) = (0_u64, 0_u64);
        for part in settled {
            let count = rx.counts.remove(&part).unwrap();
            let came = count.received.min(count.sent);
            received += u64::from(came);
            sent += u64::from(count.sent);
            rx.run += u64::from(count.sent - came);
            if came > 0 && rx.run > 0 {
                rx.burst_sum += rx.run;
                rx.burst_runs += 1;
                rx.run = 0;
            }
        }
        let windows: Vec<u64> = rx
            .window_counts
            .iter()
            .filter(|(_, count)| now.duration_since(count.at) >= SETTLE)
            .map(|(first, _)| *first)
            .collect();
        for first in windows {
            let count = rx.window_counts.remove(&first).unwrap();
            received += u64::from(count.received.min(count.sent));
            sent += u64::from(count.sent);
        }
        // Windows whose parts all came, or that are too old to help.
        let give_up = self.hole_give_up.unwrap_or(HOLE_GIVE_UP);
        rx.windows
            .retain(|_, window| now.duration_since(window.seen_at) < give_up);
        let lag = self.leg_lag(leg);
        self.legs[leg.index()].lag = lag;
        (received, sent)
    }

    /// The mean length of the runs of a leg's compact datagrams lost in a
    /// row since the last call, if any run ended.
    pub fn take_burst(&mut self, leg: LegId) -> Option<f64> {
        let rx = &mut self.legs[leg.index()];
        if rx.burst_runs == 0 {
            return None;
        }
        let mean = rx.burst_sum as f64 / rx.burst_runs as f64;
        rx.burst_sum = 0;
        rx.burst_runs = 0;
        Some(mean)
    }

    /// The joint loss since the last call, of the parts settled: the share
    /// of parts that no leg brought as an original or a copy (they needed a
    /// row or a NACK, or never came), and the mean run of such parts if a
    /// run ended. `None` when no part settled.
    pub fn take_joint(&mut self, now: Instant) -> Option<(f64, Option<f64>)> {
        let (mut parts, mut missed) = (0_u64, 0_u64);
        while let Some(entry) = self.joint.first_entry() {
            if now.duration_since(entry.get().at) < SETTLE {
                break;
            }
            let joint = entry.remove();
            parts += 1;
            if joint.came {
                if self.joint_run > 0 {
                    self.joint_run_sum += self.joint_run;
                    self.joint_runs += 1;
                    self.joint_run = 0;
                }
            } else {
                missed += 1;
                self.joint_run += 1;
            }
        }
        if parts == 0 {
            return None;
        }
        let burst =
            (self.joint_runs > 0).then(|| self.joint_run_sum as f64 / self.joint_runs as f64);
        self.joint_run_sum = 0;
        self.joint_runs = 0;
        Some((missed as f64 / parts as f64, burst))
    }

    /// The indices in `arrivals` of the last [`JUDGED_PARTS`] parts
    /// delivered, and how many there are.
    fn judged(&self) -> ([usize; JUDGED_PARTS], usize) {
        let mut indices = [0_usize; JUDGED_PARTS];
        let Some(newest) = self.highest else {
            return (indices, 0);
        };
        let mut candidates = [(0_u64, 0_usize); ARRIVALS];
        let mut count = 0;
        for (index, arrival) in self.arrivals.iter().enumerate() {
            if arrival.first.is_some()
                && arrival.part <= newest
                && arrival.part + ARRIVALS as u64 > newest
            {
                candidates[count] = (arrival.part, index);
                count += 1;
            }
        }
        let candidates = &mut candidates[..count];
        candidates.sort_unstable_by(|a, b| b.0.cmp(&a.0));
        let judged = count.min(JUDGED_PARTS);
        for (slot, (_, index)) in indices.iter_mut().zip(candidates.iter()).take(judged) {
            *slot = *index;
        }
        (indices, judged)
    }

    /// How many of the last [`JUDGED_PARTS`] parts delivered a leg brought,
    /// and its median lag behind their first arrival.
    fn brought(&self, leg: LegId) -> (usize, Option<Duration>) {
        let (indices, judged) = self.judged();
        let mut lags = [Duration::ZERO; JUDGED_PARTS];
        let mut brought = 0;
        for index in &indices[..judged] {
            let arrival = &self.arrivals[*index];
            if let (Some(first), Some(at)) = (arrival.first, arrival.legs[leg.index()]) {
                lags[brought] = at.saturating_duration_since(first);
                brought += 1;
            }
        }
        if brought == 0 {
            return (0, None);
        }
        let lags = &mut lags[..brought];
        lags.sort_unstable();
        (brought, Some(lags[brought / 2]))
    }

    /// A leg's lag behind the first arrival of `part` (0 when it brought it
    /// first), while the part is among the last 64 parts the session knows;
    /// `None` when the leg did not bring it. For the clients' per-leg lag
    /// percentiles (B 2.8 `lag_ms`).
    pub fn leg_lag_of(&self, leg: LegId, part: u64) -> Option<Duration> {
        let arrival = self
            .arrivals
            .get((part % ARRIVALS as u64) as usize)
            .filter(|arrival| arrival.part == part)?;
        Some(arrival.legs[leg.index()]?.saturating_duration_since(arrival.first?))
    }

    /// A leg's median lag behind the first arrival over the last
    /// [`JUDGED_PARTS`] parts delivered that it brought (0 on a part it
    /// brought first).
    pub fn leg_lag(&self, leg: LegId) -> Option<Duration> {
        self.brought(leg).1
    }

    /// A leg brought at least [`STEADY_PARTS`] of the last
    /// [`JUDGED_PARTS`] parts delivered.
    pub fn steady(&self, leg: LegId) -> bool {
        self.brought(leg).0 >= STEADY_PARTS
    }

    /// Leg `new` is level with leg `old`: it is steady, and its median lag
    /// is at most [`LEVEL_MARGIN`] more than `old`'s (or `old` brought
    /// fewer than [`COMPARED_PARTS`] of the parts judged).
    pub fn level(&self, new: LegId, old: LegId) -> bool {
        let (brought, lag) = self.brought(new);
        if brought < STEADY_PARTS {
            return false;
        }
        let (old_brought, old_lag) = self.brought(old);
        if old_brought < COMPARED_PARTS {
            return true;
        }
        match (lag, old_lag) {
            (Some(lag), Some(old_lag)) => lag <= old_lag + LEVEL_MARGIN,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: LegId = LegId::FIRST;

    /// A receiver with its first leg open.
    fn rx() -> CompactReceiver {
        let mut receiver = CompactReceiver::new();
        assert_eq!(receiver.open_leg(), Some(L));
        receiver
    }

    fn before() -> CompactReceiver {
        let mut receiver = rx();
        receiver.set_nack_timing(NackTiming::BEFORE);
        receiver
    }

    /// Wave 6 and 7's timing: silence NACKs without wave 8's bounds.
    fn wave6() -> CompactReceiver {
        let mut receiver = rx();
        receiver.set_nack_timing(NackTiming::WAVE6);
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
        receiver.leg_expect(L, first, true);
        let mut at = start;
        for part in first..first + parts {
            at = start + interval * (part - first) as u32;
            receiver.push(L, &encode_part(part, 0, &payload(part, 10)).unwrap(), at, 0);
        }
        at
    }

    #[test]
    fn a_hole_is_asked_for_a_quarter_millisecond_after_a_later_part_shows_it() {
        let mut receiver = rx();
        assert_eq!(receiver.nack_timing(), NackTiming::default());
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 10, 10, start, ms);
        assert_eq!(receiver.part_interval(), Some(ms));
        // Part 20 is lost; 21 shows it.
        let at = last + ms * 2;
        receiver.push(L, &encode_part(21, 0, &payload(21, 10)).unwrap(), at, 0);
        assert!(receiver.nacks_due(at, 64).take(L).is_empty());
        assert_eq!(
            receiver.nacks_due(at + FAST_NACK_DELAY, 64).take(L),
            vec![20]
        );
        // Repeated after the 2 ms floor (no round trip measured), not before.
        assert!(receiver
            .nacks_due(at + FAST_NACK_DELAY + ms, 64)
            .take(L)
            .is_empty());
        // (By then 22 is overdue too, and asked for as a silence.)
        assert!(receiver
            .nacks_due(at + FAST_NACK_DELAY + FAST_NACK_RETRY, 64)
            .take(L)
            .contains(&20));
    }

    #[test]
    fn a_silence_asks_for_the_parts_due_each_interval_until_they_come() {
        let mut receiver = wave6();
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 100, 20, start, ms);
        // Part 120 is due at last + 1 ms; it is asked for 2 ms after the
        // last part (one interval and the margin), not sooner.
        assert_eq!(receiver.next_nack_at(), Some(last + ms * 2));
        assert!(receiver
            .nacks_due(last + Duration::from_micros(1_900), 64)
            .take(L)
            .is_empty());
        assert_eq!(receiver.nacks_due(last + ms * 2, 64).take(L), vec![120]);
        // A millisecond on: 120 again (its answer went into the burst) and
        // 121, now due; then 120 waits two intervals.
        assert!(receiver.next_nack_at().unwrap() <= last + ms * 3);
        assert_eq!(
            receiver.nacks_due(last + ms * 3, 64).take(L),
            vec![120, 121]
        );
        assert_eq!(
            receiver.nacks_due(last + ms * 4, 64).take(L),
            vec![121, 122]
        );
        // The burst ends: 123 comes; 120-122 are holes now, asked for
        // again after the delay of a new hole.
        let end = last + ms * 4 + Duration::from_micros(300);
        receiver.push(L, &encode_part(123, 0, &payload(123, 10)).unwrap(), end, 0);
        assert_eq!(receiver.holes(), 3);
        assert!(receiver.nacks_due(end, 64).take(L).is_empty());
        assert_eq!(
            receiver.nacks_due(end + FAST_NACK_DELAY, 64).take(L),
            vec![120, 121, 122]
        );
        for part in [120_u64, 121, 122] {
            let mut again = encode_part(part, 0, &payload(part, 10)).unwrap();
            set_flags(&mut again, 0, true);
            assert_eq!(receiver.push(L, &again, end, 0).len(), 1);
        }
        assert_eq!(receiver.holes(), 0);
        // A silence-asked part that no part shows lost is not abandoned
        // as loss when the stream stops.
        assert!(receiver.nacks_due(end + ms * 3, 64).take(L).contains(&124));
        receiver.nacks_due(end + HOLE_GIVE_UP + ms * 4, 64).take(L);
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.stats.abandoned, 0);
    }

    #[test]
    fn a_long_silence_stops_the_nacks_and_does_not_spin() {
        let mut receiver = wave6();
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 0, 10, start, ms);
        // First seen 60 ms on (a stalled client): nothing is asked for, and
        // no wake-up is due any more.
        assert!(receiver.nacks_due(last + ms * 60, 64).take(L).is_empty());
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.next_nack_at(), None);
        // A silence NACKed from its start stops being repeated each
        // interval after SILENCE_NACK_FOR: the holes back off.
        let mut other = wave6();
        let last = fed(&mut other, 0, 10, start, ms);
        let mut sent = 0;
        let mut at = last;
        while at < last + SILENCE_NACK_FOR {
            sent += other.nacks_due(at, 64).take(L).len();
            at += Duration::from_micros(250);
        }
        // At most 32 parts, each asked for at most 6 times during the
        // silence: under the edge's NACK budget (200 parts in 100 ms).
        assert!(sent <= 192, "{sent} NACKs during the silence");
        while at < last + ms * 120 {
            other.nacks_due(at, 64).take(L);
            at += Duration::from_micros(250);
        }
        assert!(other.holes() > 0);
        let quiet =
            other.nacks_due(at, 64).take(L).len() + other.nacks_due(at + ms, 64).take(L).len();
        assert!(quiet <= 32, "{quiet} NACKs a millisecond after the silence");
    }

    #[test]
    fn a_jittery_path_waits_longer_before_it_asks_for_a_silence() {
        let mut receiver = wave6();
        receiver.leg_expect(L, 0, true);
        let start = Instant::now();
        // 1 ms parts that arrive 0.9 ms early or late in turn.
        let mut at = start;
        for part in 0..200_u64 {
            let offset = if part % 2 == 0 { 0 } else { 900 };
            at = start + Duration::from_micros(part * 1_000 + offset);
            receiver.push(L, &encode_part(part, 0, &payload(part, 10)).unwrap(), at, 0);
        }
        let silence = receiver.next_nack_at().unwrap() - at;
        assert!(
            silence > Duration::from_millis(4),
            "a silence is asked for after {silence:?}"
        );
    }

    /// Wave 8 (S4): the 2-4 ms gaps of ordinary delivery on a loaded edge
    /// ask for nothing; a silence is asked for three intervals past the
    /// next part's due time.
    #[test]
    fn a_silence_is_asked_for_only_past_the_wider_margin() {
        let mut receiver = rx();
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 100, 20, start, ms);
        assert_eq!(receiver.next_nack_at(), Some(last + ms * 4));
        assert!(receiver.nacks_due(last + ms * 3, 64).take(L).is_empty());
        // The next part comes 3.5 ms late: nothing was asked for.
        let late = last + Duration::from_micros(3_500);
        receiver.push(L, &encode_part(120, 0, &payload(120, 10)).unwrap(), late, 0);
        assert_eq!(receiver.stats.nacked, 0);
        // Wave 6 asked for it after 2 ms.
        let mut old = wave6();
        let last = fed(&mut old, 100, 20, start, ms);
        assert_eq!(old.nacks_due(last + ms * 2, 64).take(L), vec![120]);
    }

    /// Wave 8 (R6-5): a stall is no sample of the interval: the silence
    /// margin after it is that of the stream's interval.
    #[test]
    fn a_stall_is_no_sample_of_the_interval() {
        let mut receiver = rx();
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 0, 50, start, ms);
        // 300 ms of nothing, then the stream goes on.
        let resumed = last + ms * 300;
        receiver.push(
            L,
            &encode_part(50, 0, &payload(50, 10)).unwrap(),
            resumed,
            0,
        );
        assert_eq!(receiver.part_interval(), Some(ms));
        let mut old = wave6();
        let last = fed(&mut old, 0, 50, start, ms);
        old.push(
            L,
            &encode_part(50, 0, &payload(50, 10)).unwrap(),
            last + ms * 300,
            0,
        );
        assert!(old.part_interval().unwrap() > ms * 10);
    }

    /// Wave 8 review: a short first sample (the second part came 100 us
    /// after the first, which came with the OK) does not hold the estimate
    /// down for good, and a burst of parts (a catch-up after a stall) does
    /// not pull it down; a clean 1 ms stream then asks for nothing.
    #[test]
    fn a_short_seed_or_a_burst_does_not_hold_the_interval_down() {
        let ms = Duration::from_millis(1);
        let start = Instant::now();
        let mut receiver = rx();
        receiver.leg_expect(L, 0, true);
        receiver.push(L, &encode_part(0, 0, &payload(0, 10)).unwrap(), start, 0);
        let mut at = start + Duration::from_micros(100);
        receiver.push(L, &encode_part(1, 0, &payload(1, 10)).unwrap(), at, 0);
        assert_eq!(receiver.part_interval(), Some(Duration::from_micros(100)));
        for part in 2..40 {
            at += ms;
            receiver.push(L, &encode_part(part, 0, &payload(part, 10)).unwrap(), at, 0);
            assert!(receiver.nacks_due(at, 64).take(L).is_empty());
        }
        let interval = receiver.part_interval().unwrap();
        assert!(
            interval > Duration::from_micros(900) && interval < Duration::from_micros(1_100),
            "{interval:?}"
        );
        // A burst of 30 parts 80 us apart, then the stream goes on.
        for part in 40..70 {
            at += Duration::from_micros(80);
            receiver.push(L, &encode_part(part, 0, &payload(part, 10)).unwrap(), at, 0);
        }
        let interval = receiver.part_interval().unwrap();
        assert!(interval > Duration::from_micros(900), "{interval:?}");
        let mut asked = 0;
        for part in 70..200 {
            at += ms;
            receiver.push(L, &encode_part(part, 0, &payload(part, 10)).unwrap(), at, 0);
            asked += receiver.nacks_due(at, 64).take(L).len();
            asked += receiver
                .nacks_due(at + Duration::from_micros(990), 64)
                .take(L)
                .len();
        }
        assert_eq!(asked, 0);
        assert_eq!(receiver.stats.nacked, 0);
    }

    /// Wave 8 review: a hole that waits for the rate cap's tokens is
    /// counted once a wait, not once a poll.
    #[test]
    fn a_capped_hole_is_counted_once_a_wait() {
        let mut receiver = rx();
        let start = Instant::now();
        receiver.leg_expect(L, 0, true);
        receiver.push(L, &encode_part(0, 0, &payload(0, 10)).unwrap(), start, 0);
        // 100 parts lost at once.
        receiver.push(
            L,
            &encode_part(101, 0, &payload(101, 10)).unwrap(),
            start,
            0,
        );
        let at = start + FAST_NACK_DELAY;
        let first = receiver.nacks_due(at, 256).take(L).len();
        assert_eq!(first, DEFAULT_NACK_RATE_CAP as usize / 4);
        for _ in 0..10 {
            assert!(receiver.nacks_due(at, 256).take(L).is_empty());
        }
        assert_eq!(receiver.nacks_capped, 100 - first as u64);
    }

    /// Wave 8 (R6-3): a dead path is not asked for 500 parts a second: one
    /// silence asks for its budget at most, in its first 50 ms, and then
    /// nothing until a datagram comes; the holes do not fall into the
    /// ordinary schedule.
    #[test]
    fn a_silence_spends_its_budget_and_waits_for_a_datagram() {
        let mut receiver = rx();
        receiver.set_leg_rtt(L, Duration::from_millis(1));
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 0, 50, start, ms);
        let mut at = last;
        let mut during = 0;
        while at < last + SILENCE_NACK_FOR {
            during += receiver.nacks_due(at, 64).take(L).len();
            at += Duration::from_micros(250);
        }
        assert!(during > 0);
        assert!(
            during <= usize::from(DEFAULT_SILENCE_NACK_BUDGET),
            "{during} NACKs during the silence"
        );
        let mut after = 0;
        while at < last + ms * 900 {
            after += receiver.nacks_due(at, 64).take(L).len();
            at += Duration::from_micros(250);
        }
        assert_eq!(after, 0, "NACKs after the silence's first 50 ms");
        // No spin: the next wake is not now.
        assert!(receiver.next_nack_at().is_none_or(|next| next > at));
        // Wave 6 went on asking (12 times each, backing off to 100 ms).
        let mut old = wave6();
        let last = fed(&mut old, 0, 50, start, ms);
        let mut at = last;
        let mut total = 0;
        while at < last + ms * 900 {
            total += old.nacks_due(at, 64).take(L).len();
            at += Duration::from_micros(250);
        }
        assert!(
            total > 2 * usize::from(DEFAULT_SILENCE_NACK_BUDGET),
            "{total}"
        );
    }

    /// Wave 8 (R6-2, R6-4): when a stall ends with a backlog, the holes past
    /// the newest part that came are not asked for between its datagrams,
    /// and a hole that the silence asked for is asked again no sooner than
    /// the retry after its last NACK.
    #[test]
    fn a_backlog_after_a_stall_is_not_nacked_between_its_datagrams() {
        let mut receiver = rx();
        // (The rate cap apart: its own test.)
        receiver.set_nack_timing(NackTiming {
            rate_cap: 0,
            ..NackTiming::default()
        });
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 0, 50, start, ms);
        // The silence asks for 50.. while nothing comes for 10 ms.
        let mut at = last;
        let mut asked = BTreeMap::new();
        while at < last + ms * 10 {
            for part in receiver.nacks_due(at, 64).take(L) {
                asked.insert(part, at);
            }
            at += Duration::from_micros(250);
        }
        assert!(asked.contains_key(&50));
        // The backlog: 51 and 52 come (50 is lost), then the reads go on.
        receiver.push(L, &encode_part(51, 0, &payload(51, 10)).unwrap(), at, 0);
        receiver.push(L, &encode_part(52, 0, &payload(52, 10)).unwrap(), at, 0);
        // 50, which the silence asked for, waits the retry after its last
        // NACK (2 ms floor), not the 250 us of a new hole; holes past 52
        // that the silence asked for wait for the next silence: none is
        // due between the backlog's datagrams.
        let last_asked = asked[&50];
        let again = (at + FAST_NACK_DELAY).max(last_asked + receiver.nack_retry());
        let before = receiver
            .nacks_due(again - Duration::from_micros(10), 64)
            .take(L);
        assert!(before.iter().all(|part| *part <= 52), "{before:?}");
        if again > at + FAST_NACK_DELAY {
            assert!(!before.contains(&50));
        }
        let due = receiver.nacks_due(again, 64).take(L);
        assert!(due.contains(&50) || before.contains(&50), "{due:?}");
        assert!(due.iter().all(|part| *part <= 52), "{due:?}");
    }

    /// Wave 8: a client asks for at most its rate cap of parts a second,
    /// however many holes an overloaded edge leaves, and does not spin while
    /// it waits for tokens.
    #[test]
    fn the_nacks_are_capped_at_the_rate_and_do_not_spin() {
        let mut receiver = rx();
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        receiver.leg_expect(L, 0, true);
        // Every other part lost for a second: 500 holes.
        let mut at = start;
        let mut asked = 0;
        let mut wakes = 0;
        for part in (0..1_000_u64).step_by(2) {
            at = start + ms * part as u32;
            receiver.push(L, &encode_part(part, 0, &payload(part, 10)).unwrap(), at, 0);
            let mut now = at;
            while let Some(next) = receiver.next_nack_at().filter(|next| *next <= at + ms) {
                now = next.max(now);
                asked += receiver.nacks_due(now, 64).take(L).len();
                wakes += 1;
                assert!(wakes < 20_000, "the client spins");
            }
        }
        let cap = DEFAULT_NACK_RATE_CAP as usize;
        assert!(
            asked <= cap + cap / 4 + 1,
            "{asked} parts asked for in a second"
        );
        assert!(receiver.nacks_capped > 0);
        let mut free = rx();
        free.set_nack_timing(NackTiming {
            rate_cap: 0,
            ..NackTiming::default()
        });
        free.leg_expect(L, 0, true);
        let mut all = 0;
        for part in (0..1_000_u64).step_by(2) {
            let at = start + ms * part as u32;
            free.push(L, &encode_part(part, 0, &payload(part, 10)).unwrap(), at, 0);
            all += free.nacks_due(at + ms, 64).take(L).len();
        }
        assert!(all > cap, "{all} without the cap");
    }

    #[test]
    fn the_timing_of_before_asks_for_nothing_during_a_silence() {
        let mut receiver = before();
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let last = fed(&mut receiver, 0, 10, start, ms);
        assert_eq!(receiver.next_nack_at(), None);
        assert!(receiver.nacks_due(last + ms * 5, 64).take(L).is_empty());
    }

    #[test]
    fn a_receiver_keeps_the_bytes_of_as_many_parts_as_it_is_told() {
        let now = Instant::now();
        let mut default = rx();
        let mut small = rx();
        small.set_known_parts(200);
        for receiver in [&mut default, &mut small] {
            receiver.leg_expect(L, 0, true);
            for part in 0..5_000_u64 {
                let datagram = encode_part(part, 1, &payload(part, 300)).unwrap();
                assert_eq!(receiver.push(L, &datagram, now, 1).len(), 1);
            }
        }
        assert_eq!(default.known.len(), MEMORY);
        assert_eq!(small.known.len(), 200);
        // Never fewer than a window needs.
        small.set_known_parts(1);
        assert_eq!(small.known_limit(), MIN_KNOWN_PARTS);
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
        let mut receiver = rx();
        receiver.leg_expect(L, 5, true);
        let mut marked = encode_part(5, 1_700_000_000_000_500, &bytes).unwrap();
        set_fast_lane(&mut marked);
        let parts = receiver.push(L, &marked, now, 1_700_000_000_001_000);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].fast_lane);
        let unmarked = encode_part(6, 1_700_000_000_001_500, &payload(6, 40)).unwrap();
        assert!(!receiver.push(L, &unmarked, now, 1_700_000_000_002_000)[0].fast_lane);
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
        // Gaps and repair keep the long form, and so does every 256th part,
        // and a part whose time is unknown (0: the short form would make a
        // time up to 8.4 s off of it).
        assert_eq!(shorten(&encode_gap(5)), None);
        let unknown = encode_part(9, 0, &bytes).unwrap();
        assert_eq!(shorten(&unknown), None);
        let mut in_place = unknown.clone();
        assert_eq!(shorten_in_place(&mut in_place, unknown.len()), None);
        // A time whose low 32 bits are 0 but not 0 itself is rare (one in
        // 2^32 µs) and only keeps the long form too.
        assert!(shorten(&encode_part(9, 1 << 32, &bytes).unwrap()).is_none());
        for part in [0_u64, 256, 65_536, 1 << 32] {
            let long = encode_part(part, 1_700_000_000_000_000, &bytes).unwrap();
            assert!(keeps_long_form(part));
            assert_eq!(shorten(&long), None, "part {part}");
            let mut in_place = long.clone();
            assert_eq!(shorten_in_place(&mut in_place, long.len()), None);
        }
        // A short part before any reference is dropped; after the OK it is
        // a part.
        let mut receiver = rx();
        let long = encode_part(70_000, 1_700_000_000_000_000, &bytes).unwrap();
        let short = shorten(&long).unwrap();
        let now = Instant::now();
        assert!(receiver
            .push(L, &short, now, 1_700_000_000_001_000)
            .is_empty());
        receiver.leg_expect(L, 69_999, true);
        let parts = receiver.push(L, &short, now, 1_700_000_000_001_000);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part, 70_000);
        assert_eq!(parts[0].payload, bytes);
        // No long-form part has given the edge's clock yet: the time is
        // read around the client's, and the long form, which may be wrong,
        // is not kept (B X2, seam A).
        assert_eq!(parts[0].published_unix_us, 1_700_000_000_000_000);
        assert!(!receiver.known.contains_key(&70_000));
        // After one, the short form's time is read.
        let anchor = encode_part(70_144, 1_700_000_000_144_000, &bytes).unwrap();
        assert_eq!(
            receiver.push(L, &anchor, now, 1_700_000_000_145_000).len(),
            1
        );
        let next = shorten(&encode_part(70_145, 1_700_000_000_145_000, &bytes).unwrap()).unwrap();
        let parts = receiver.push(L, &next, now, 1_700_000_000_146_000);
        assert_eq!(parts[0].published_unix_us, 1_700_000_000_145_000);
        assert!(receiver.known.contains_key(&70_145));
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
        let mut receiver = rx();
        receiver.leg_expect(L, 700, true);
        let first = encode_part(703, 1_700_000_000_000_000, &bytes).unwrap();
        assert_eq!(
            receiver.push(L, &first, start, 1_700_000_000_001_000).len(),
            1
        );
        assert_eq!(
            receiver.nacks_due(start + delay, 64).take(L),
            vec![700, 701, 702]
        );
        // A part (long, as every 256th is) came before the OK, which names
        // an older part: the parts between are asked for.
        let mut receiver = rx();
        let anchor = encode_part(768, 1_700_000_000_000_000, &bytes).unwrap();
        assert!(shorten(&anchor).is_none());
        receiver.active = true;
        assert_eq!(
            receiver
                .push(L, &anchor, start, 1_700_000_000_001_000)
                .len(),
            1
        );
        receiver.leg_expect(L, 760, true);
        assert_eq!(
            receiver.nacks_due(start + delay, 64).take(L),
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
        let mut receiver = rx();
        receiver.leg_expect(L, 511, true);
        let start = Instant::now();
        let anchor = encode_part(512, edge_us, &bytes).unwrap();
        assert!(shorten(&anchor).is_none(), "part 512 keeps the long form");
        let parts = receiver.push(L, &anchor, start, edge_us + client_off_us);
        assert_eq!(parts.len(), 1);
        for index in 1..300_u64 {
            let part = 512 + index;
            let published = edge_us + index * 1_000;
            let long = encode_part(part, published, &bytes).unwrap();
            let datagram = shorten(&long).unwrap_or(long.clone());
            let at = start + Duration::from_millis(index);
            let parts = receiver.push(L, &datagram, at, published + client_off_us + 500);
            assert_eq!(parts.len(), 1, "part {part}");
            // The long form it keeps (for window repair) is the edge's.
            assert_eq!(
                lengthen(&datagram, part, published).unwrap_or(long.clone()),
                long
            );
            assert_eq!(
                u32::from_be_bytes(receiver.known[&part].bytes[7..11].try_into().unwrap()) as u64,
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
        let mut receiver = rx();
        receiver.leg_expect(L, 1_000, true);
        for part in 1_000..1_010 {
            assert_eq!(receiver.push(L, &stamped(part, old), start, 0).len(), 1);
        }
        // The new epoch counts from 0: its parts are delivered, not taken
        // for old ones, and its holes are NACKed.
        assert_eq!(receiver.push(L, &stamped(0, new), start, 0).len(), 1);
        assert_eq!(receiver.stats.generation_changes, 1);
        assert_eq!(receiver.push(L, &stamped(2, new), start, 0).len(), 1);
        assert_eq!(receiver.holes(), 1);
        let later = start + Duration::from_millis(5);
        assert_eq!(receiver.nacks_due(later, 16).take(L), vec![1]);
        // A late datagram of the old epoch is dropped, and does not switch
        // back.
        assert!(receiver.push(L, &stamped(1_010, old), later, 0).is_empty());
        assert_eq!(receiver.stats.stale_generation, 1);
        assert_eq!(receiver.push(L, &stamped(1, new), later, 0).len(), 1);
        assert_eq!(receiver.stats.generation_changes, 1);
        // Generation 0 (not known) is taken in any flow.
        assert_eq!(receiver.push(L, &stamped(3, 0), later, 0).len(), 1);
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
        let mut receiver = rx();
        receiver.leg_expect(L, 1_000, true);
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
            delivered.extend(receiver.push(L, datagram, now, 0));
        }
        assert_eq!(receiver.holes(), 2);
        // A copy is a duplicate.
        let mut copy = datagrams[0].clone();
        set_flags(&mut copy, 1, false);
        assert!(receiver.push(L, &copy, now, 0).is_empty());
        assert_eq!(receiver.stats.duplicates, 1);
        delivered.extend(receiver.push(L, &repair[0], now, 0));
        assert_eq!(delivered.len(), 14, "one row is not enough for two holes");
        delivered.extend(receiver.push(L, &repair[1], now, 0));
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
        receiver.leg_expect(L, 10, true);
        let start = Instant::now();
        for part in [10_u64, 11, 14] {
            receiver.push(
                L,
                &encode_part(part, 0, &payload(part, 10)).unwrap(),
                start,
                0,
            );
        }
        assert!(
            receiver.nacks_due(start, 64).take(L).is_empty(),
            "not before the delay"
        );
        assert_eq!(
            receiver.nacks_due(start + NACK_DELAY, 64).take(L),
            vec![12, 13]
        );
        assert!(receiver
            .nacks_due(start + NACK_DELAY, 64)
            .take(L)
            .is_empty());
        assert_eq!(
            receiver
                .nacks_due(start + NACK_DELAY + NACK_RETRY, 1)
                .take(L),
            vec![12],
            "at most the limit"
        );
        // The next NACK of a part waits twice as long.
        assert!(receiver
            .nacks_due(start + NACK_DELAY + NACK_RETRY * 2, 64)
            .take(L)
            .contains(&13));
        assert!(receiver
            .nacks_due(start + NACK_DELAY + NACK_RETRY * 2, 64)
            .take(L)
            .is_empty());
        assert_eq!(
            receiver
                .nacks_due(start + NACK_DELAY + NACK_RETRY * 3, 64)
                .take(L),
            vec![12]
        );
        // A retransmission fills one hole.
        let mut again = encode_part(13, 0, &payload(13, 10)).unwrap();
        set_flags(&mut again, 0, true);
        let parts = receiver.push(L, &again, start, 0);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].retransmit);
        assert_eq!(receiver.holes(), 1);
        receiver.nacks_due(start + HOLE_GIVE_UP, 64).take(L);
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.stats.abandoned, 1);
        // A jump far ahead is a resume, not holes.
        receiver.push(L, &encode_part(10_000, 0, &[]).unwrap(), start, 0);
        assert_eq!(receiver.holes(), 0);
    }

    #[test]
    fn a_part_that_comes_after_its_gap_is_delivered_and_the_gap_is_asked_for() {
        // The timing of before wave 6 (needletail-udp-client --silence-nack
        // false --nack-delay-us 1000 --nack-retry-floor-us 5000).
        let mut receiver = before();
        receiver.leg_expect(L, 20, true);
        let start = Instant::now();
        receiver.push(L, &encode_part(20, 0, &payload(20, 10)).unwrap(), start, 0);
        // The edge gave part 21 up (its hole outlived the repair horizon).
        let parts = receiver.push(L, &encode_gap(21), start, 0);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].kind, Kind::Gap);
        // The gap is asked for again a round trip later, in case the edge's
        // cache has filled it since.
        assert!(receiver.nacks_due(start, 64).take(L).is_empty());
        assert_eq!(receiver.nacks_due(start + NACK_RETRY, 64).take(L), vec![21]);
        // It had: the part comes, and is delivered after its gap, once.
        let mut late = encode_part(21, 0, &payload(21, 10)).unwrap();
        set_flags(&mut late, 0, true);
        let parts = receiver.push(L, &late, start + NACK_RETRY * 2, 0);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].kind, Kind::Part);
        assert_eq!(parts[0].payload, payload(21, 10));
        assert!(receiver
            .push(L, &late, start + NACK_RETRY * 2, 0)
            .is_empty());
        assert_eq!(receiver.holes(), 0);
        // A gap again for a part delivered is a duplicate.
        assert!(receiver.push(L, &encode_gap(20), start, 0).is_empty());
        // A gap whose part never comes is not counted as abandoned (it was
        // delivered, as a gap).
        receiver.push(L, &encode_gap(22), start, 0);
        receiver.nacks_due(start + HOLE_GIVE_UP, 64).take(L);
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.stats.abandoned, 0);
    }

    #[test]
    fn nacks_are_not_repeated_before_a_round_trip() {
        // The timing of before wave 6 (needletail-udp-client --silence-nack
        // false --nack-delay-us 1000 --nack-retry-floor-us 5000).
        let mut receiver = before();
        receiver.leg_expect(L, 10, true);
        receiver.set_leg_rtt(L, Duration::from_millis(30));
        assert_eq!(receiver.nack_retry(), Duration::from_millis(45));
        let start = Instant::now();
        for part in [10_u64, 12] {
            receiver.push(
                L,
                &encode_part(part, 0, &payload(part, 10)).unwrap(),
                start,
                0,
            );
        }
        assert_eq!(receiver.nacks_due(start + NACK_DELAY, 64).take(L), vec![11]);
        // Not at 5 ms or 15 ms after it: a retransmission takes 30 ms.
        for after in [5, 15, 40] {
            assert!(receiver
                .nacks_due(start + NACK_DELAY + Duration::from_millis(after), 64)
                .take(L)
                .is_empty());
        }
        assert_eq!(
            receiver
                .nacks_due(start + NACK_DELAY + Duration::from_millis(45), 64)
                .take(L),
            vec![11]
        );
        // A short path keeps the 5 ms floor.
        let mut near = before();
        near.set_leg_rtt(L, Duration::from_micros(200));
        assert_eq!(near.nack_retry(), NACK_RETRY);
        // Wave 6's floor is 2 ms.
        let mut fast = rx();
        fast.set_leg_rtt(L, Duration::from_micros(200));
        assert_eq!(fast.nack_retry(), FAST_NACK_RETRY);
        let mut far = rx();
        far.set_leg_rtt(L, Duration::from_millis(30));
        assert_eq!(far.nack_retry(), Duration::from_millis(45));
        // The answer to a hole's only NACK measures the round trip.
        let mut measured = before();
        measured.leg_expect(L, 0, true);
        for part in [0_u64, 2] {
            measured.push(
                L,
                &encode_part(part, 0, &payload(part, 10)).unwrap(),
                start,
                0,
            );
        }
        assert_eq!(measured.nacks_due(start + NACK_DELAY, 64).take(L), vec![1]);
        let mut again = encode_part(1, 0, &payload(1, 10)).unwrap();
        set_flags(&mut again, 0, true);
        measured.push(L, &again, start + NACK_DELAY + Duration::from_millis(20), 0);
        assert_eq!(measured.leg_rtt(L), Some(Duration::from_millis(20)));
    }

    #[test]
    fn loss_is_counted_on_originals_and_copies_and_bursts_are_measured() {
        let mut receiver = rx();
        receiver.leg_expect(L, 0, true);
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
            receiver.push(L, &datagram, start, 0);
            if part != 50 {
                receiver.push(L, &datagram, start, 0);
            }
        }
        let (received, sent) = receiver.take_counts(L, start + SETTLE);
        assert_eq!(sent, 200);
        assert_eq!(received, 200 - 6 - 1);
        assert_eq!(receiver.take_burst(L), Some(3.5));
        assert_eq!(receiver.take_burst(L), None);
    }

    /// With back-to-back copies, a burst that takes one part takes its
    /// copy: the bursts read in datagrams make a gemodel path bursty (a
    /// count of parts lost whole read about one), and its plan then sends
    /// no copies but window rows for the burst.
    #[test]
    fn a_burst_that_takes_a_part_and_its_copy_reads_as_a_burst() {
        let mut receiver = rx();
        receiver.leg_expect(L, 0, true);
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
                    receiver.push(L, &datagram, start, 0);
                }
            }
        }
        receiver.take_counts(L, start + SETTLE);
        let burst = receiver.take_burst(L).unwrap();
        assert!((burst - 7.0 / 3.0).abs() < 1e-9, "{burst}");
        let plan = CompactPlan::for_measured(0.048, burst);
        assert_eq!(plan.copies, 0, "{plan:?}");
        assert!(plan.window_repair >= 4, "{plan:?}");
        // Random loss: copies, as before.
        let random = CompactPlan::for_measured(0.01, 1.0);
        assert_eq!(random.copies, 1);
        // The plan of before, for the A/B: one copy and two more rows.
        let before = CompactPlan::for_measured_with(0.048, burst, false, RESIDUAL_TARGET);
        assert_eq!((before.copies, before.window_repair), (1, 4), "{before:?}");
    }

    /// Two legs, both open at `first`.
    fn two_legs(first: u64) -> (CompactReceiver, LegId) {
        let mut receiver = rx();
        let other = receiver.open_leg().unwrap();
        assert_eq!(receiver.leg_expect(L, first, true), LegStart::Started);
        assert_eq!(receiver.leg_expect(other, first, true), LegStart::Started);
        (receiver, other)
    }

    /// B X2: the first arrival of a part number is delivered, the other
    /// leg's is that leg's duplicate; a gap from one edge and the part from
    /// the other is delivered as the part; each leg counts its own
    /// datagrams.
    #[test]
    fn a_part_from_two_legs_is_delivered_once_and_counted_on_each() {
        let (mut receiver, b) = two_legs(100);
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        for part in 100..110_u64 {
            let datagram = encode_part(part, 0, &payload(part, 20)).unwrap();
            let at = start + ms * (part - 100) as u32;
            assert_eq!(receiver.push(L, &datagram, at, 0).len(), 1);
            assert!(receiver
                .push(b, &datagram, at + Duration::from_micros(200), 0)
                .is_empty());
        }
        assert_eq!(receiver.leg_stats(L).parts_first, 10);
        assert_eq!(receiver.leg_stats(L).duplicates, 0);
        assert_eq!(receiver.leg_stats(b).parts_first, 0);
        assert_eq!(receiver.leg_stats(b).duplicates, 10);
        assert_eq!(receiver.stats.parts, 10);
        assert_eq!(receiver.stats.duplicates, 10);
        // A gap from one edge, the part from the other: the part.
        let gap = receiver.push(L, &encode_gap(110), start + ms * 10, 0);
        assert_eq!(gap[0].kind, Kind::Gap);
        let part = receiver.push(
            b,
            &encode_part(110, 0, &payload(110, 20)).unwrap(),
            start + ms * 10,
            0,
        );
        assert_eq!(part.len(), 1);
        assert_eq!(part[0].kind, Kind::Part);
        assert_eq!(receiver.holes(), 0);
        let settled = start + Duration::from_secs(1);
        assert_eq!(receiver.take_counts(L, settled), (11, 11));
        assert_eq!(receiver.take_counts(b, settled), (11, 11));
    }

    /// B X2: the session's first part is the first delivered by any leg;
    /// a slower leg's older parts are counted and dropped, never holes.
    #[test]
    fn a_second_leg_that_starts_lower_delivers_nothing_below_the_first_part() {
        let mut receiver = rx();
        let b = receiver.open_leg().unwrap();
        receiver.leg_expect(L, 1_000, true);
        receiver.leg_expect(b, 998, true);
        let now = Instant::now();
        let part = |part: u64| encode_part(part, 0, &payload(part, 20)).unwrap();
        assert_eq!(receiver.push(L, &part(1_000), now, 0).len(), 1);
        assert_eq!(receiver.floor(), Some(1_000));
        assert!(receiver.push(b, &part(998), now, 0).is_empty());
        assert!(receiver.push(b, &part(999), now, 0).is_empty());
        assert!(receiver.push(b, &part(1_000), now, 0).is_empty());
        assert_eq!(receiver.stats.before_start, 2);
        assert_eq!(receiver.leg_stats(b).before_start, 2);
        assert_eq!(receiver.leg_stats(b).duplicates, 1);
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.push(b, &part(1_001), now, 0).len(), 1);
        assert_eq!(receiver.leg_stats(b).parts_first, 1);
        assert_eq!(receiver.floor(), Some(1_000));
        assert_eq!(receiver.holes(), 0);
        // The leg that set the first part resumes from an older part (its
        // backfill comes after its live parts): delivered, and the first
        // part moves down to it.
        assert_eq!(receiver.push(L, &part(995), now, 0).len(), 1);
        assert_eq!(receiver.floor(), Some(995));
        assert!(receiver.push(b, &part(994), now, 0).is_empty());
        assert_eq!(receiver.leg_stats(b).before_start, 3);
    }

    /// B X2: a leg whose OK names a part 4,096 behind the newest is stale
    /// and the newest does not move; a leg that shows the only leg that
    /// delivered to be that far behind displaces it.
    #[test]
    fn a_stale_first_part_fails_its_leg_and_does_not_move_the_newest() {
        let mut receiver = rx();
        receiver.leg_expect(L, 10_000, true);
        let now = Instant::now();
        for part in 10_000..10_010_u64 {
            receiver.push(
                L,
                &encode_part(part, 0, &payload(part, 10)).unwrap(),
                now,
                0,
            );
        }
        let stale = receiver.open_leg().unwrap();
        assert_eq!(receiver.leg_expect(stale, 5_000, true), LegStart::Stale);
        assert_eq!(receiver.newest(), Some(10_009));
        assert_eq!(receiver.leg_first_part(stale), None);
        receiver.close_leg(stale);
        // A head 4,096 ahead of the only leg that delivered.
        let ahead = receiver.open_leg().unwrap();
        assert_eq!(
            receiver.leg_expect(ahead, 20_000, true),
            LegStart::Displaces(L)
        );
        assert_eq!(receiver.newest(), None);
        assert_eq!(receiver.floor(), None);
        receiver.close_leg(L);
        let parts = receiver.push(
            ahead,
            &encode_part(20_000, 0, &payload(1, 10)).unwrap(),
            now,
            0,
        );
        assert_eq!(parts.len(), 1);
        assert_eq!(receiver.floor(), Some(20_000));
        assert_eq!(receiver.newest(), Some(20_000));
        // With two legs delivering, a far head displaces nobody.
        let (mut receiver, b) = two_legs(100);
        for part in 100..104_u64 {
            let datagram = encode_part(part, 0, &payload(part, 10)).unwrap();
            let leg = if part % 2 == 0 { L } else { b };
            receiver.push(leg, &datagram, now, 0);
        }
        let far = receiver.open_leg().unwrap();
        assert_eq!(receiver.leg_expect(far, 50_000, true), LegStart::Started);
        assert_eq!(receiver.newest(), Some(103));
    }

    /// B X2: the 16 bits of a short-form part are extended around its own
    /// leg's last part or `OK`, never another leg's: before its `OK` a
    /// short part waits. Before any long-form part its long form is not
    /// kept for window repair.
    #[test]
    fn short_parts_are_numbered_from_their_own_leg() {
        let bytes = payload(7, 56);
        let us = 1_700_000_000_000_000_u64;
        let short =
            |part: u64| shorten(&encode_part(part, us + part * 1_000, &bytes).unwrap()).unwrap();
        let now = Instant::now();
        let mut receiver = rx();
        receiver.leg_expect(L, 70_142, true);
        let unanchored = receiver.push(L, &short(70_142), now, us + 70_142_500);
        assert_eq!(unanchored[0].part, 70_142);
        assert_eq!(unanchored[0].published_unix_us, us + 70_142_000);
        assert!(!receiver.known.contains_key(&70_142));
        let anchor = encode_part(70_144, us + 70_144_000, &bytes).unwrap();
        assert_eq!(receiver.push(L, &anchor, now, us + 70_144_500).len(), 1);
        let parts = receiver.push(L, &short(70_145), now, us);
        assert_eq!(parts[0].part, 70_145);
        assert_eq!(parts[0].published_unix_us, us + 70_145_000);
        // A second leg's short part before its OK waits for it, though the
        // session has a newest part.
        let b = receiver.open_leg().unwrap();
        assert!(receiver.push(b, &short(70_146), now, us).is_empty());
        assert_eq!(receiver.early_kept, 1);
        let early = receiver.take_early(b);
        assert_eq!(early.len(), 1);
        receiver.leg_expect(b, 70_146, true);
        let parts = receiver.push(b, &early[0], now, us);
        assert_eq!(parts[0].part, 70_146);
        let parts = receiver.push(b, &short(70_147), now, us);
        assert_eq!(parts[0].part, 70_147);
        assert_eq!(receiver.leg_highest(b), Some(70_147));
        assert_eq!(receiver.leg_highest(L), Some(70_145));
        assert_eq!(receiver.stats.errors, 0);
    }

    /// B X2: each leg's loss is its own datagrams; the joint loss is the
    /// share of parts that no leg brought.
    #[test]
    fn each_leg_measures_its_own_loss_and_the_joint_loss_is_what_no_leg_brought() {
        let (mut receiver, b) = two_legs(0);
        let start = Instant::now();
        for part in 0..100_u64 {
            let datagram = encode_part(part, 0, &payload(part, 20)).unwrap();
            if part % 10 != 3 {
                receiver.push(L, &datagram, start, 0);
            }
            let b_loses = (part % 10 == 3 && part < 50) || part % 10 == 7;
            if !b_loses {
                receiver.push(b, &datagram, start, 0);
            }
        }
        let settled = start + SETTLE;
        assert_eq!(receiver.take_counts(L, settled), (90, 100));
        assert_eq!(receiver.take_counts(b, settled), (85, 100));
        let (joint, burst) = receiver.take_joint(settled).expect("parts settled");
        assert!((joint - 0.05).abs() < 1e-9, "{joint}");
        assert_eq!(burst, Some(1.0));
        assert_eq!(receiver.take_joint(settled), None);
    }

    /// B X2: a repair row enters only the windows of the leg it came from:
    /// one row from each of two edges does not recover two holes.
    #[test]
    fn rows_of_one_leg_never_enter_another_legs_window() {
        let (mut receiver, b) = two_legs(1_000);
        let now = Instant::now();
        let datagrams: Vec<Vec<u8>> = (1_000..1_016_u64)
            .map(|part| encode_part(part, 0, &payload(part, 80)).unwrap())
            .collect();
        let refs: Vec<&[u8]> = datagrams.iter().map(Vec::as_slice).collect();
        let repair = encode_window_repair(1_000, &refs, 2);
        let mut delivered = 0;
        for (index, datagram) in datagrams.iter().enumerate() {
            if index != 3 && index != 9 {
                delivered += receiver.push(L, datagram, now, 0).len();
            }
        }
        delivered += receiver.push(L, &repair[0], now, 0).len();
        delivered += receiver.push(b, &repair[1], now, 0).len();
        assert_eq!(delivered, 14, "rows of two legs met in one decode");
        assert_eq!(receiver.holes(), 2);
        delivered += receiver.push(L, &repair[1], now, 0).len();
        assert_eq!(delivered, 16);
        assert_eq!(receiver.stats.recovered, 2);
    }

    /// B X2: the repair leg's bytes of a part replace another leg's, never
    /// the reverse; a recovery that fails its check on another leg's bytes
    /// counts a cross-leg mismatch, keeps the window, and recovers once the
    /// repair leg's own bytes come.
    #[test]
    fn the_repair_legs_bytes_are_kept_and_a_mismatch_keeps_the_window() {
        let (mut receiver, b) = two_legs(0);
        receiver.set_prefer_leg(Some(L));
        let now = Instant::now();
        let ours: Vec<Vec<u8>> = (0..4_u64)
            .map(|part| encode_part(part, 0, &payload(part, 30)).unwrap())
            .collect();
        let mut theirs = ours[1].clone();
        theirs[HEADER_LEN + 2] ^= 0x55;
        let refs: Vec<&[u8]> = ours.iter().map(Vec::as_slice).collect();
        let rows = encode_window_repair(0, &refs, 1);
        assert_eq!(receiver.push(L, &ours[0], now, 0).len(), 1);
        assert_eq!(receiver.push(b, &theirs, now, 0).len(), 1);
        assert_eq!(receiver.push(L, &ours[3], now, 0).len(), 1);
        assert!(receiver.push(L, &rows[0], now, 0).is_empty());
        assert_eq!(receiver.stats.cross_leg_mismatch, 1);
        assert_eq!(receiver.lowest_hole(), Some(2));
        assert!(receiver.legs[L.index()].windows.contains_key(&0));
        // The row again changes nothing and counts nothing again.
        assert!(receiver.push(L, &rows[0], now, 0).is_empty());
        assert_eq!(receiver.stats.cross_leg_mismatch, 1);
        // The repair leg's own bytes of part 1: they replace the other
        // leg's, and the window recovers part 2.
        let recovered = receiver.push(L, &ours[1], now, 0);
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].part, 2);
        assert!(recovered[0].recovered);
        assert_eq!(receiver.known[&1].bytes, ours[1]);
        // Another leg's bytes never replace the repair leg's.
        let mut other = ours[3].clone();
        other[HEADER_LEN] ^= 0x01;
        assert!(receiver.push(b, &other, now, 0).is_empty());
        assert_eq!(receiver.known[&3].bytes, ours[3]);
        assert_eq!(receiver.holes(), 0);
    }

    /// B X2: a leg is steady when it brought 28 of the last 32 parts, and
    /// level with another when its median lag is at most 250 us more
    /// (steady is enough beside a leg that brought fewer than 16).
    #[test]
    fn a_leg_is_steady_then_level() {
        let (mut receiver, b) = two_legs(0);
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let us = Duration::from_micros(1);
        let feed = |receiver: &mut CompactReceiver,
                    parts: std::ops::Range<u64>,
                    lag: Duration,
                    a: bool,
                    b_brings: &dyn Fn(u64) -> bool| {
            for part in parts {
                let datagram = encode_part(part, 0, &payload(part, 10)).unwrap();
                let at = start + ms * part as u32;
                if a {
                    receiver.push(L, &datagram, at, 0);
                }
                if b_brings(part) {
                    receiver.push(b, &datagram, at + lag, 0);
                }
            }
        };
        feed(&mut receiver, 0..20, us * 100, true, &|_| true);
        assert!(!receiver.steady(b), "20 parts are not 28");
        feed(&mut receiver, 20..40, us * 100, true, &|_| true);
        assert!(receiver.steady(b) && receiver.steady(L));
        assert_eq!(receiver.leg_lag(b), Some(us * 100));
        assert_eq!(receiver.leg_lag(L), Some(Duration::ZERO));
        assert!(receiver.level(b, L));
        // Slower by 400 us: steady, not level; the faster leg is level.
        feed(&mut receiver, 40..80, us * 400, true, &|_| true);
        assert!(receiver.steady(b));
        assert!(!receiver.level(b, L));
        assert!(receiver.level(L, b));
        // Five of the last 32 missing: not steady.
        feed(&mut receiver, 80..100, us * 100, true, &|part| {
            part % 4 != 0
        });
        assert!(!receiver.steady(b));
        assert!(!receiver.level(b, L));
        // The other leg silent: steady is enough.
        feed(&mut receiver, 100..140, us * 100, false, &|_| true);
        assert!(receiver.level(b, L));
        assert!(!receiver.steady(L));
    }

    /// B X2: a new source generation resets every leg's windows, counts and
    /// numbering, and the session's first part.
    #[test]
    fn a_new_generation_resets_every_leg() {
        let (mut receiver, b) = two_legs(1_000);
        let start = Instant::now();
        let old = generation_of_epoch(1_700_000_000_000_000);
        let new = generation_of_epoch(1_700_000_100_000_000);
        let stamped = |part: u64, generation: u8| {
            let mut datagram = encode_part(part, 0, &payload(part, 10)).unwrap();
            set_generation(&mut datagram, generation);
            datagram
        };
        let originals: Vec<Vec<u8>> = (1_000..1_004).map(|part| stamped(part, old)).collect();
        let refs: Vec<&[u8]> = originals.iter().map(Vec::as_slice).collect();
        let rows = encode_window_repair(1_000, &refs, 2);
        for part in 1_000..1_010_u64 {
            if part == 1_002 || part == 1_003 {
                continue;
            }
            receiver.push(L, &stamped(part, old), start, 0);
            if part != 1_005 {
                receiver.push(b, &stamped(part, old), start, 0);
            }
        }
        receiver.push(b, &rows[0], start, 0);
        assert!(!receiver.legs[b.index()].windows.is_empty());
        assert!(!receiver.legs[b.index()].counts.is_empty());
        assert_eq!(receiver.push(L, &stamped(0, new), start, 0).len(), 1);
        assert_eq!(receiver.stats.generation_changes, 1);
        for leg in [L, b] {
            let rx = &receiver.legs[leg.index()];
            assert!(rx.windows.is_empty() && rx.window_counts.is_empty());
            assert_eq!(rx.first_part, None);
            assert!(rx.open);
        }
        assert!(receiver.legs[b.index()].counts.is_empty());
        assert_eq!(receiver.leg_highest(b), None);
        assert_eq!(receiver.floor(), Some(0));
        // The other leg's part of the new generation is a duplicate, not
        // before the start; its old generation's late datagram is dropped.
        assert!(receiver.push(b, &stamped(0, new), start, 0).is_empty());
        assert_eq!(receiver.leg_stats(b).before_start, 0);
        assert!(receiver.push(b, &stamped(1_009, old), start, 0).is_empty());
        assert_eq!(receiver.stats.stale_generation, 1);
        assert_eq!(receiver.push(b, &stamped(1, new), start, 0).len(), 1);
    }

    /// B X3: a hole is not abandoned after twelve NACKs: it is asked for
    /// every 250 ms after them until it is given up, after 3 s or once it
    /// is 3,072 parts behind the newest (the edge's ring then passes it).
    #[test]
    fn a_hole_is_asked_for_until_the_ring_has_passed_it() {
        let mut receiver = before();
        assert_eq!(receiver.hole_give_up(), Duration::from_secs(3));
        receiver.leg_expect(L, 10, true);
        let start = Instant::now();
        for part in [10_u64, 12] {
            receiver.push(
                L,
                &encode_part(part, 0, &payload(part, 10)).unwrap(),
                start,
                0,
            );
        }
        let mut asked: Vec<Instant> = Vec::new();
        let mut at = start;
        while at < start + Duration::from_millis(2_990) {
            at += Duration::from_millis(1);
            if receiver.nacks_due(at, 64).leg(L) == [11] {
                asked.push(at);
            }
        }
        assert!(
            asked.len() > usize::from(MAX_NACKS_PER_HOLE) + 4,
            "{}",
            asked.len()
        );
        for pair in asked[usize::from(MAX_NACKS_PER_HOLE) - 1..].windows(2) {
            assert_eq!(pair[1] - pair[0], SLOW_NACK);
        }
        assert_eq!(receiver.holes(), 1);
        assert!(receiver.nacks_due(start + HOLE_GIVE_UP, 64).is_empty());
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.stats.abandoned, 1);
        // 3,072 parts behind the newest: given up at once.
        let mut receiver = before();
        receiver.leg_expect(L, 0, true);
        receiver.push(L, &encode_part(0, 0, &payload(0, 10)).unwrap(), start, 0);
        for part in 2..3_073_u64 {
            receiver.push(
                L,
                &encode_part(part, 0, &payload(part, 10)).unwrap(),
                start,
                0,
            );
        }
        receiver.nacks_due(start, 64);
        assert_eq!(receiver.lowest_hole(), Some(1));
        receiver.push(
            L,
            &encode_part(3_073, 0, &payload(3, 10)).unwrap(),
            start,
            0,
        );
        receiver.nacks_due(start, 64);
        assert_eq!(receiver.holes(), 0);
        assert_eq!(receiver.stats.abandoned, 1);
    }

    /// B X3: a hole that a later part shows waits for the slower leg's lag
    /// before its first NACK, which goes to the leg with the shortest round
    /// trip; the retries go to every leg whose edge can answer.
    #[test]
    fn a_hole_waits_for_the_slower_leg_and_asks_the_nearer_first() {
        let (mut receiver, b) = two_legs(0);
        receiver.set_nack_timing(NackTiming {
            silence: false,
            ..NackTiming::default()
        });
        receiver.set_leg_rtt(L, Duration::from_millis(10));
        receiver.set_leg_rtt(b, Duration::from_millis(2));
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        let lag = Duration::from_millis(3);
        for part in 0..40_u64 {
            let datagram = encode_part(part, 0, &payload(part, 10)).unwrap();
            let at = start + ms * part as u32;
            receiver.push(L, &datagram, at, 0);
            receiver.push(b, &datagram, at + lag, 0);
        }
        let measured = start + ms * 40;
        receiver.take_counts(L, measured);
        receiver.take_counts(b, measured);
        assert_eq!(receiver.leg_lag(b), Some(lag));
        // A leg whose edge started after the hole is never asked for it.
        let late = receiver.open_leg().unwrap();
        receiver.leg_expect(late, 41, true);
        // Part 40 is lost on both legs; part 41 shows it.
        let shown = start + ms * 41;
        receiver.push(L, &encode_part(41, 0, &payload(41, 10)).unwrap(), shown, 0);
        assert!(receiver.nacks_due(shown + ms, 64).is_empty());
        assert!(receiver.nacks_due(shown + ms * 3, 64).is_empty());
        let first = shown + lag + Duration::from_micros(250);
        let due = receiver.nacks_due(first, 64);
        assert_eq!(due.leg(b), [40]);
        assert!(due.leg(L).is_empty());
        assert_eq!(receiver.nack_retry(), Duration::from_millis(3));
        let again = receiver.nacks_due(first + Duration::from_millis(3), 64);
        assert_eq!(again.leg(b), [40]);
        assert_eq!(again.leg(L), [40]);
        assert!(again.leg(late).is_empty());
        assert_eq!(
            receiver.stats.nacked, 2,
            "a part asked of two legs counts once"
        );
        // A leg the session does not NACK on is not asked.
        receiver.set_leg_nack(L, false);
        let third = receiver.nacks_due(first + Duration::from_millis(9), 64);
        assert_eq!(third.leg(b), [40]);
        assert!(third.leg(L).is_empty());
    }

    /// B X3 review: a hole that no leg can be asked for (no leg NACKs yet,
    /// or every leg's edge started after it) waits the slow NACK, not one
    /// retry, and is asked at once when a leg can be.
    #[test]
    fn a_hole_no_leg_can_be_asked_for_waits_for_a_leg() {
        let (mut receiver, b) = two_legs(0);
        receiver.set_nack_timing(NackTiming {
            silence: false,
            ..NackTiming::default()
        });
        receiver.set_leg_nack(L, false);
        receiver.set_leg_nack(b, false);
        let start = Instant::now();
        let ms = Duration::from_millis(1);
        for part in [0_u64, 1, 3] {
            let datagram = encode_part(part, 0, &payload(part, 10)).unwrap();
            receiver.push(L, &datagram, start + ms * part as u32, 0);
        }
        let due = start + ms * 4;
        assert!(receiver.nacks_due(due, 64).is_empty());
        assert_eq!(receiver.next_nack_at(), Some(due + SLOW_NACK));
        assert!(receiver
            .nacks_due(due + receiver.nack_retry(), 64)
            .is_empty());
        assert_eq!(receiver.stats.nacked, 0);
        // The leg's edge answers: the hole is asked at once.
        let later = due + ms * 10;
        receiver.set_leg_nack(b, true);
        assert!(receiver.next_nack_at().is_some_and(|at| at <= later));
        assert_eq!(receiver.nacks_due(later, 64).leg(b), [2]);
        // Asked once, it follows the ordinary retries again.
        assert!(receiver
            .next_nack_at()
            .is_some_and(|at| at < later + SLOW_NACK));
    }

    fn leg(loss: f64) -> LegLoss {
        LegLoss {
            loss,
            burst: 1.0,
            rtt: Some(Duration::from_millis(2)),
        }
    }

    fn plans(
        repair: LegLoss,
        other: LegLoss,
        joint: Option<(f64, f64)>,
    ) -> (CompactPlan, CompactPlan) {
        let plans = PathPlans::for_legs(
            repair,
            Some(other),
            joint,
            Some(Duration::from_millis(1)),
            RESIDUAL_TARGET,
            true,
        );
        (plans.repair, plans.other.unwrap())
    }

    fn word(plan: CompactPlan) -> String {
        plan.word()
    }

    /// B X3: two clean legs each send every part once and nothing else.
    #[test]
    fn two_legs_plan_parts_alone_when_clean() {
        let (repair, other) = plans(leg(0.0), leg(0.0019), Some((0.0, 1.0)));
        assert_eq!(
            (word(repair), word(other)),
            ("C0.32.0".into(), "C0.32.0".into())
        );
        // Unmeasured joint loss changes nothing when clean.
        let (repair, other) = plans(leg(0.001), leg(0.001), None);
        assert_eq!(
            (word(repair), word(other)),
            ("C0.32.0".into(), "C0.32.0".into())
        );
    }

    /// B X3: with random loss the second leg is the copy: no copy up to
    /// 5.8 % on both legs; above, a copy on the leg with less loss.
    #[test]
    fn two_legs_need_no_copy_at_five_percent() {
        for loss in [0.01, 0.05, 0.058] {
            let (repair, other) = plans(leg(loss), leg(loss), Some((0.0, 1.0)));
            assert_eq!((repair.copies, other.copies), (0, 0), "{loss}");
            assert_eq!((repair.window, repair.window_repair), (16, 0), "{loss}");
            assert_eq!((other.window, other.window_repair), (16, 0), "{loss}");
        }
        let (repair, other) = plans(leg(0.06), leg(0.06), Some((0.0, 1.0)));
        assert_eq!((repair.copies, other.copies), (1, 0));
        // The copy goes on the leg with less loss.
        let (repair, other) = plans(leg(0.12), leg(0.03), Some((0.0, 1.0)));
        assert_eq!((repair.copies, other.copies), (0, 1));
        // The residual target is a parameter.
        let tight = PathPlans::for_legs(
            leg(0.05),
            Some(leg(0.05)),
            Some((0.0, 1.0)),
            Some(Duration::from_millis(1)),
            0.002,
            true,
        );
        assert_eq!(tight.repair.copies, 1);
    }

    /// B X3: the repair leg asks for a row only when its round trip is at
    /// least eight part intervals: a row then comes before a NACK's answer.
    #[test]
    fn a_row_is_planned_only_from_eight_part_intervals_of_round_trip() {
        let with_rtt = |rtt_ms: u64| LegLoss {
            rtt: Some(Duration::from_millis(rtt_ms)),
            ..leg(0.01)
        };
        let (near, _) = plans(with_rtt(7), leg(0.01), Some((0.0, 1.0)));
        assert_eq!(word(near), "C0.16.0");
        let (far, other) = plans(with_rtt(8), leg(0.01), Some((0.0, 1.0)));
        assert_eq!(word(far), "C0.16.1");
        assert_eq!(word(other), "C0.16.0");
        // An unmeasured round trip asks for none.
        let unmeasured = LegLoss {
            rtt: None,
            ..leg(0.01)
        };
        assert_eq!(plans(unmeasured, leg(0.01), None).0.window_repair, 0);
    }

    /// B X3: legs that lose together (their joint loss is far above the
    /// product of their losses), or a bursty leg, get rows from the repair
    /// leg alone, for the joint loss and its runs; no copies.
    #[test]
    fn legs_that_lose_together_get_rows_on_one_leg() {
        let (repair, other) = plans(leg(0.01), leg(0.01), Some((0.01, 1.0)));
        assert_eq!(word(repair), "C0.16.2");
        assert_eq!(word(other), "C0.16.0");
        let bursty = LegLoss {
            burst: 2.5,
            ..leg(0.01)
        };
        let (repair, other) = plans(leg(0.01), bursty, Some((0.001, 3.0)));
        assert_eq!(word(repair), "C0.16.4");
        assert_eq!(word(other), "C0.16.0");
        // At most eight rows.
        let (repair, _) = plans(leg(0.2), leg(0.2), Some((0.2, 9.0)));
        assert_eq!(repair.window_repair, 8);
        // Independent losses are not together: copies or nothing.
        let (repair, _) = plans(leg(0.05), leg(0.05), Some((0.0025, 1.0)));
        assert_eq!(word(repair), "C0.16.0");
    }

    /// B X3: one counted leg plans as before.
    #[test]
    fn one_leg_plans_as_before() {
        for (loss, burst) in [
            (0.0, 1.0),
            (0.01, 1.0),
            (0.05, 1.0),
            (0.048, 7.0 / 3.0),
            (0.2, 3.0),
        ] {
            for burst_rows in [true, false] {
                let plans = PathPlans::for_legs(
                    LegLoss {
                        loss,
                        burst,
                        rtt: Some(Duration::from_millis(50)),
                    },
                    None,
                    None,
                    Some(Duration::from_millis(1)),
                    RESIDUAL_TARGET,
                    burst_rows,
                );
                assert_eq!(plans.other, None);
                assert_eq!(
                    plans.repair,
                    CompactPlan::for_measured_with(loss, burst, burst_rows, RESIDUAL_TARGET)
                );
            }
        }
        assert_eq!(CompactPlan::for_measured(0.0, 1.0), CompactPlan::CLEAN);
        assert_eq!(CompactPlan::for_measured(0.02, 1.0).word(), "C1.16.2");
    }
}
