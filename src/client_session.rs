//! One listener's subscription to a stream, without its sockets: the
//! protocol of `needletail-udp-client` as a state machine, so that the
//! client and the load generator (`needletail-load`, thousands of them on
//! one host) run the same code.
//!
//! The session holds the listener's legs (B, section 3.1): one subscription
//! at one edge each, with that edge's token, round trip, timers, loss and
//! plan. What arrives on any leg goes through one receiver, which delivers
//! the first arrival of each part number. The caller owns the socket and
//! the clock: it passes each datagram that arrived to
//! [`UdpSession::on_datagram`], runs the timers when [`UdpSession::next_wake`]
//! comes, and sends the control datagrams that both hand to its `send`
//! function.
//!
//! Two ways to drive it. The edge-choosing director
//! (`crate::client_director`) holds legs through [`SessionLegs`]: it adds
//! and releases legs, and reads what happened to them as [`LegEvent`]s. A
//! caller that holds one edge and moves itself (the client and the load
//! generator until they run on the director) calls [`UdpSession::begin`]
//! with the next edge when [`UdpSession::poll`] or
//! [`UdpSession::on_datagram`] says that it must move (`MOVE`,
//! `REFUSED`, no answer, silence).
//!
//! The parts come out of [`UdpSession::next_object`] after a datagram, with
//! the lane that they came on in [`UdpSession::receiver`]
//! (`last_fast_envelope_bytes`, `last_compact_fast_lane`), as before.
//!
//! Wave 6 and 7 client behaviour lives here, so that the client and the
//! load generator share it: the NACK timing ([`SessionConfig::nack_timing`]),
//! the short compact header asked for first and dropped when an edge refuses
//! it ([`drops_short_header`]), the compact lane given up only after three
//! unanswered requests and 1.5 s ([`falls_back_from_compact`]), and
//! [`UdpSession::hold`] for the end-of-run drain.

use crate::client_compact::{
    smoothed_loss, smoothed_loss_by, CompactPlan, LegId, LegLoss, LegStart as Expected, LegStats, NackTiming,
    PathPlans, BURSTY, HOLE_GIVE_UP, IP_UDP_HEADER_BYTES, MAX_LEGS, RESIDUAL_TARGET,
};
use crate::client_director::{
    LegEvent, LegFailure, LegKey, LegStart, LegView, SessionLegs, StartFrom,
};
use crate::client_quality::{LatenessHistogram, QualityConfig, QualityJudge};
use crate::client_udp::{
    Control, ObjectKind, ObjectReceiver, PartObject, RepairPolicy, MAX_OTHER_LEGS, REFRESH,
    UNMEASURED_LOSS,
};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// How often the session measures the loss of its path.
pub const LOSS_CHECK: Duration = Duration::from_millis(150);
/// How often a request that the edge has not answered is repeated (at
/// most, with [`SessionConfig::first_retry`]).
pub const UNANSWERED_REFRESH: Duration = Duration::from_millis(300);
/// Wave 9 B: the first repeat of a request that the edge has not answered
/// (see [`SessionConfig::first_retry`]).
pub const DEFAULT_FIRST_RETRY_MS: u64 = 25;
/// An edge that has not answered the compact lane's requests for this long
/// (and three of them) is asked without it.
pub const COMPACT_FALLBACK_AFTER: Duration = Duration::from_millis(1_500);

/// Whether a client that asked for the compact lane and has had no answer
/// (no token, no OK) to `unanswered` requests over `waited` asks without it
/// from now on: three requests and 1.5 s (wave 7 review: a lost ARP request
/// held the first datagrams of a lossy path for a second, and the client
/// left the compact lane, and so the fast lane, for the whole run).
pub fn falls_back_from_compact(
    compact_asked: bool,
    answered: bool,
    unanswered: u32,
    waited: Duration,
) -> bool {
    compact_asked && !answered && unanswered >= 3 && waited >= COMPACT_FALLBACK_AFTER
}

/// Whether an OK (compact or not) to a request with `plan` means the edge
/// refused the short form: it asked for the compact lane in the short form
/// and was refused the lane. An edge older than the short form refuses a
/// plan word with it (`C...7`); the client asks again with the long form.
pub fn drops_short_header(compact: bool, compact_asked: bool, plan: CompactPlan) -> bool {
    !compact && compact_asked && plan.short_header
}

/// Datagrams a part that a compact plan asks the edge for: the part, its
/// copies and its share of the window's rows.
fn plan_cost(plan: CompactPlan) -> f64 {
    1.0 + f64::from(plan.copies) + f64::from(plan.window_repair) / f64::from(plan.window.max(1))
}

/// The silence after which a client moves (wave 8 S2; 1,000 before).
pub const DEFAULT_SILENCE_MS: u64 = 200;
/// See [`SessionConfig::silence_repeat`].
pub const DEFAULT_SILENCE_REPEAT_MS: u64 = 1_000;
/// See [`SessionConfig::token_wait`] (`--token-ms`).
pub const DEFAULT_TOKEN_MS: u64 = 400;
/// See [`SessionConfig::first_double`] (`--first-double-ms`).
pub const DEFAULT_FIRST_DOUBLE_MS: u64 = 3;
/// See [`SessionConfig::hedge_after`] (`--hedge-after-ms`).
pub const DEFAULT_HEDGE_AFTER_MS: u64 = 50;
/// See [`SessionConfig::lone_hedge_after`] (`--lone-hedge-after-ms`).
pub const DEFAULT_LONE_HEDGE_AFTER_MS: u64 = 170;
/// Times a client asks an edge for the compact lane again after falling
/// back from it (see [`SessionConfig::compact_retry`]).
pub const COMPACT_RETRIES: u32 = 2;

/// A plan that asks for less (fewer copies or rows) is taken only this long
/// after the leg's last plan change; one that asks for more goes at once
/// (B X3: the plan flapped under burst loss, hundreds of changes in 200 s,
/// each a `SUB` and a regroup of the edge's fast lane). A rate limit, not a
/// wait for the estimate to want less for this long: that held the copies
/// of every short rise of a 0.2 to 0.5 % loss for a second more and cost
/// 14 to 23 % more datagrams a part than no hold (lead, 24 Nov, after
/// integration 2: less 1 s after the last change). With
/// [`PLAN_SMOOTHING`] at 1/8 the estimate crosses a plan's edge less often,
/// and 0.5 s holds the changes under 50 in 30 s at 0.2 to 0.5 % random loss
/// at the base's cost (av-mesh-fix-3: 1 s and 1/4 cost 11 to 14 % more at
/// 0.3 %; 0.5 s and 1/8, 3 % more at 0.3 %, 32 changes).
pub const PLAN_HOLD: Duration = Duration::from_millis(500);
/// The weight of a new 150 ms loss sample in the estimate that a compact
/// plan follows ([`smoothed_loss_by`]): 1/8, a time constant of about
/// 1.2 s. A loss that rises by more than a few tenths of a percent still
/// asks for copies at its first sample (one sample of 5 % lifts the
/// estimate to 0.6 %); a small rise waits a few samples, as NACK covers.
pub const PLAN_SMOOTHING: f64 = 0.125;
/// A path counted bursty stops being so below this mean burst (it becomes
/// so at [`BURSTY`]).
pub const BURSTY_UNTIL: f64 = 1.5;
/// Loss samples a new session waits for before its plan follows the
/// measured loss: until then it plans for [`UNMEASURED_LOSS`].
pub const PLAN_SAMPLES: u32 = 2;

/// What a client asks for, as `needletail-udp-client`'s flags say.
#[derive(Debug, Clone, Copy)]
pub struct SessionConfig {
    pub stream_id: u64,
    /// Repair of each RaptorQ block, per thousand source symbols, at least
    /// (by default the lane's).
    pub repair_permille: Option<u32>,
    pub min_repair: Option<u32>,
    /// Every leg silent this long, or a leg silent this long beside one that
    /// sends (dead), or without media beside one that delivers (wave 8 S2:
    /// 200 ms; 1 s before).
    pub silence: Duration,
    /// After a move for silence, until the new edge sends a datagram, the
    /// silence that moves again (at least `silence`): a stall of the stream
    /// itself (every edge silent) moves a client once a second, not five
    /// times.
    pub silence_repeat: Duration,
    /// NACKs wait while the caller says datagrams are queued on its socket
    /// ([`UdpSession::queued`], wave 8 R6-2): the backlog after a stall
    /// fills most holes.
    pub nack_after_drain: bool,
    /// An edge that answers a request asked without the compact lane (the
    /// client fell back after its compact requests went unanswered) is
    /// asked for the lane again, twice at most (wave 8: a lost answer
    /// otherwise kept the client on the RaptorQ lane for good).
    pub compact_retry: bool,
    /// Drop the short-form parts that come before the edge's `OK`, as
    /// before wave 8 (`false`: kept and read once the `OK` comes).
    pub drop_early_parts: bool,
    /// Wave 8 local proofs: a move asks the new edge for the oldest part
    /// still missing here (a hole the old edge never repaired, at most
    /// MOVE_HOLE_PARTS back), not only for the parts after the newest; the
    /// new edge serves from the part asked when it holds it (S2). Before
    /// (`false`) the holes were dropped with the old flow: a killed edge's
    /// listeners lost the parts it had not repaired (~2 a listener at 1 ms).
    /// A leg added with [`StartFrom::OldestHole`] follows it too.
    pub move_from_hole: bool,
    /// Move when the edge does not answer the subscription in this time.
    pub answer: Duration,
    /// Keep the repair that was asked for, whatever the measured loss.
    pub fixed_repair: bool,
    pub fast_lane: bool,
    pub compact: bool,
    pub compact_burst_rows: bool,
    /// Ask for parts in the short form (wave 7 item 9, `C...7`): a 7-byte
    /// header instead of 11. An edge that refuses it is asked again with
    /// the long form.
    pub compact_short_header: bool,
    /// When the compact lane's holes are NACKed (wave 6: a quarter
    /// millisecond first, a 2 ms retry floor, NACKs during a silence).
    pub nack_timing: NackTiming,
    /// Parts whose bytes the compact lane keeps for window repair (0: the
    /// receiver's default of 4,096; a load host keeps fewer).
    pub known_parts: usize,
    /// Wave 9 B (time to first audio under loss): a request that the edge
    /// has not answered (the first `SUB`, the `SUB` with the token) is
    /// repeated after this, or three round trips once one is measured, and
    /// then after twice as long each time, up to [`UNANSWERED_REFRESH`]: a
    /// lost request or answer costs a few round trips, not 300 ms. Zero:
    /// every [`UNANSWERED_REFRESH`], as before.
    pub first_retry: Duration,
    /// The share of parts that may need a NACK after the copies
    /// (`--compact-residual-ppm`, [`RESIDUAL_TARGET`] by default).
    pub residual_target: f64,
    /// When a hole is given up (`--hole-give-up-ms`, [`HOLE_GIVE_UP`]).
    pub hole_give_up: Duration,
    /// The token wait of a leg begun with [`UdpSession::begin`]
    /// (`--token-ms`); a leg the director adds carries its own
    /// ([`LegStart::token_wait`]).
    pub token_wait: Duration,
    /// A leg's first tokenless `SUB` and its first `SUB` with the token
    /// are each sent again this soon (zero: not).
    pub first_double: Duration,
    /// A leg with no media this long while another leg delivered a part
    /// first in the last 20 ms is due a hedge at the spare.
    pub hedge_after: Duration,
    /// The only live leg, with no datagram this long, is due a hedge.
    pub lone_hedge_after: Duration,
    /// A plan that asks for less waits this long after the leg's last plan
    /// change ([`PLAN_HOLD`]).
    pub plan_hold: Duration,
    /// The weight of a new loss sample in the estimate a compact plan
    /// follows ([`PLAN_SMOOTHING`]).
    pub plan_smoothing: f64,
    /// Place240 rule 3: a lone leg's parts judged late or missing
    /// ([`crate::client_quality`]).
    pub quality: QualityConfig,
}

/// A move asks for a hole at most this many parts before the newest part
/// (4 s of 1 ms parts; an edge keeps 12 s).
pub const MOVE_HOLE_PARTS: u64 = 4_096;

impl SessionConfig {
    /// The defaults of `needletail-udp-client` for `stream_id`.
    pub fn new(stream_id: u64) -> Self {
        Self {
            stream_id,
            repair_permille: None,
            min_repair: None,
            silence: Duration::from_millis(DEFAULT_SILENCE_MS),
            silence_repeat: Duration::from_millis(DEFAULT_SILENCE_REPEAT_MS),
            nack_after_drain: true,
            compact_retry: true,
            drop_early_parts: false,
            move_from_hole: true,
            answer: Duration::from_millis(5_000),
            fixed_repair: false,
            fast_lane: true,
            compact: true,
            compact_burst_rows: true,
            compact_short_header: true,
            nack_timing: NackTiming::default(),
            known_parts: 0,
            first_retry: Duration::from_millis(DEFAULT_FIRST_RETRY_MS),
            residual_target: RESIDUAL_TARGET,
            hole_give_up: HOLE_GIVE_UP,
            token_wait: Duration::from_millis(DEFAULT_TOKEN_MS),
            first_double: Duration::from_millis(DEFAULT_FIRST_DOUBLE_MS),
            hedge_after: Duration::from_millis(DEFAULT_HEDGE_AFTER_MS),
            lone_hedge_after: Duration::from_millis(DEFAULT_LONE_HEDGE_AFTER_MS),
            plan_hold: PLAN_HOLD,
            plan_smoothing: PLAN_SMOOTHING,
            quality: QualityConfig::default(),
        }
    }

    fn base_for(&self, compact: bool) -> RepairPolicy {
        RepairPolicy::base_for_lane(compact, self.repair_permille, self.min_repair)
    }
}

/// What a datagram was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Received {
    /// Not from a leg's edge, from a leg the session gave up, or before
    /// any edge.
    Ignored,
    /// A control datagram that changes nothing for the caller.
    Control,
    /// A leg's `OK`; `first` when it is the first at that leg.
    Subscribed { next_part: u64, first: bool },
    /// A media datagram: take its objects with [`UdpSession::next_object`].
    Media,
    /// The edge sent `MOVE` or `REFUSED`: move to the next edge.
    Move(String),
}

/// Where a leg is in its life (B 3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Asked, no `TOKEN` yet.
    Asking,
    /// The edge gave its token; no `OK` yet.
    Tokened,
    /// The edge answered `OK`: it serves the leg.
    Live,
    /// The edge sent `MOVE`: it still serves the leg until it is released
    /// or the edge's grace ends.
    Leaving,
    /// The session gave the leg up (or the edge refused it): nothing more
    /// is taken from it, and it stays until it is released.
    Failed,
}

/// One leg: what the session keeps for its subscription at one edge.
#[derive(Debug)]
struct Leg {
    /// The director's name for it.
    key: LegKey,
    /// Its slot, in the session and in the receiver.
    id: LegId,
    /// The edge's node id ("" for a leg begun without one).
    node: String,
    target: SocketAddr,
    state: State,
    /// The reason of the edge's `MOVE`, while leaving.
    leaving: Option<String>,
    /// Why the leg failed, as [`UdpSession::poll`] reports it.
    failure: Option<String>,
    added_at: Instant,
    /// The edge's first `OK`, and when the leg was reported live (its `OK`
    /// and its first media had come).
    ok_at: Option<Instant>,
    live_at: Option<Instant>,
    token: Option<u64>,
    /// When the edge last gave the token (`None` for one handed in by the
    /// director, whose age the session does not know).
    token_at: Option<Instant>,
    /// With no `TOKEN` this long after its first request the leg is passed
    /// over.
    token_wait: Duration,
    /// The part the first requests name (until the edge sends parts).
    from_part: Option<u64>,
    /// The edge has sent a part (or a repair datagram) since the request:
    /// it serves this client, so a repeated request no longer names
    /// `from_part` (an edge that has admitted the client would go back to
    /// it and send those parts again; wave 9 review).
    media_here: bool,
    /// The part its edge's `OK` named, and the first part it brought.
    first_part: Option<u64>,
    first_brought: Option<u64>,
    /// The first request, for the answer wait and the token wait.
    asked_at: Instant,
    /// The first copy of the request now unanswered (its answer measures a
    /// round trip while only the double went), and the requests that the
    /// edge has not answered.
    request_sent_at: Instant,
    unanswered: u32,
    /// Repeats of the request now unanswered (the backoff of
    /// [`SessionConfig::first_retry`]).
    repeats: u32,
    /// When the first request of its kind is sent again (the double).
    double_at: Option<Instant>,
    /// A refresh sent when no other request was outstanding: its `OK` or
    /// `MOVE` measures a round trip.
    refresh_sent_at: Option<Instant>,
    /// When the edge was asked to take the leg back (a `MOVE` within a
    /// round trip of it, before its `OK`, is ignored).
    takeback_at: Option<Instant>,
    rtt: Option<Duration>,
    /// The last datagram of any kind (from when the leg was added, for the
    /// timers), and whether one came at all.
    last_datagram: Instant,
    heard_at: Option<Instant>,
    first_media: Option<Instant>,
    last_media: Option<Instant>,
    /// When a datagram of this leg last delivered a part first.
    last_first: Option<Instant>,
    next_refresh: Instant,
    passed_over: bool,
    hedge_due: bool,
    suspect: bool,
    /// The delivering legs that its last `SUB` named in `o=` (bits by slot).
    others_sent: u8,
    // The compact lane at this edge.
    compact_asked: bool,
    compact_confirmed: bool,
    compact_retries_here: u32,
    /// When the compact lane was asked for again while subscribed, until an
    /// `OK` with it comes: an edge that drops compact requests (one older
    /// than the lane) never answers, and the client asks without it again
    /// after [`COMPACT_FALLBACK_AFTER`] (wave 8 review: the retry undid the
    /// fall-back, every refresh was dropped and the subscription expired).
    compact_retry_at: Option<Instant>,
    // The leg's loss and what it asks for.
    base_repair: RepairPolicy,
    repair: RepairPolicy,
    loss_estimate: Option<f64>,
    compact_estimate: Option<f64>,
    burst_estimate: Option<f64>,
    loss_samples: u32,
    /// The path counts as bursty (from [`BURSTY`] until under
    /// [`BURSTY_UNTIL`]).
    bursty: bool,
    /// When `plan` was last changed (or set at the leg's start): a plan
    /// that asks for less waits until [`PLAN_HOLD`] after it.
    plan_at: Instant,
    plan: CompactPlan,
    datagrams: u64,
    wire_bytes: u64,
    longest_gap: Duration,
    /// The stream's head that the edge's first `WAIT` since the leg's last
    /// media named (`h=`, stuck4k), and when a later `WAIT` first named
    /// another: from then the stream is live at the edge, and the edge
    /// sends none of it.
    wait_heads: Option<(u64, Option<Instant>)>,
    /// Place240: the lateness of the parts this leg delivered.
    lateness: LatenessHistogram,
}

impl Leg {
    fn is_live(&self) -> bool {
        matches!(self.state, State::Live | State::Leaving)
    }

    fn is_failed(&self) -> bool {
        self.state == State::Failed
    }

    /// Counted in the plans: live (its `OK` and media came), not leaving,
    /// not suspect.
    fn is_counted(&self) -> bool {
        self.state == State::Live && self.live_at.is_some() && !self.suspect
    }

    fn bit(&self) -> u8 {
        1 << self.id.index()
    }

    /// Its last media, or when its edge answered (or it was added) if none
    /// came.
    fn media_since(&self) -> Instant {
        self.last_media.or(self.ok_at).unwrap_or(self.added_at)
    }
}

/// How long a leg's unanswered request waits for its answer before it is
/// repeated (see [`SessionConfig::first_retry`]).
fn unanswered_wait(config: &SessionConfig, leg: &Leg) -> Duration {
    let first = config.first_retry;
    if first.is_zero() {
        return UNANSWERED_REFRESH;
    }
    let first = first.max(leg.rtt.map_or(Duration::ZERO, |rtt| rtt * 3));
    first
        .saturating_mul(1 << leg.repeats.min(8))
        .min(UNANSWERED_REFRESH)
}

/// The RaptorQ repair a leg asks for, from its loss.
fn policy_for(config: &SessionConfig, leg: &Leg) -> RepairPolicy {
    if config.fixed_repair {
        leg.base_repair
    } else {
        RepairPolicy::for_loss(
            leg.loss_estimate.unwrap_or(UNMEASURED_LOSS),
            leg.base_repair,
        )
    }
}

/// What a leg measured of the client's path, carried to the next edge by
/// [`UdpSession::begin`].
#[derive(Debug, Clone, Copy)]
struct PathMeasure {
    loss_estimate: Option<f64>,
    compact_estimate: Option<f64>,
    burst_estimate: Option<f64>,
    loss_samples: u32,
    bursty: bool,
    plan: CompactPlan,
}

/// A leg let go: its edge's datagrams are answered with `UNSUB` for a
/// while, and its second `UNSUB` is due.
#[derive(Debug)]
struct Released {
    target: SocketAddr,
    token: Option<u64>,
    until: Instant,
    second_unsub: Option<Instant>,
    answered_at: Option<Instant>,
}

/// What the client's report keeps of one leg (B 2.8): read it before the
/// leg is released.
#[derive(Debug, Clone, PartialEq)]
pub struct LegReport {
    pub node: String,
    pub target: SocketAddr,
    /// The part its edge's `OK` named, else the first part it brought.
    pub first_part: Option<u64>,
    /// Its edge serves the compact lane.
    pub compact: bool,
    pub rtt: Option<Duration>,
    /// Its median lag behind the first arrival (see [`LegView::lag`]).
    pub lag: Option<Duration>,
    /// Its smoothed datagram loss, and the plan it asks for.
    pub loss: Option<f64>,
    pub plan: CompactPlan,
    /// Media datagrams from its edge, their bytes on the wire, and the
    /// longest time between two of them.
    pub datagrams: u64,
    pub wire_bytes: u64,
    pub longest_gap: Duration,
    /// Its compact datagrams, first arrivals, duplicates and parts below
    /// the session's first part.
    pub stats: LegStats,
    /// Place240: the lateness of the parts it delivered (arrival less
    /// publication, less the path's least over 10 s).
    pub lateness: LatenessHistogram,
}

/// One listener's subscription to a stream, held as one or more legs. See
/// the module's documentation.
#[derive(Debug)]
pub struct UdpSession {
    pub config: SessionConfig,
    pub receiver: ObjectReceiver,
    legs: [Option<Leg>; MAX_LEGS],
    released: Vec<Released>,
    events: VecDeque<LegEvent>,
    /// The key of the next leg begun with [`Self::begin`] (counting down,
    /// apart from the director's).
    next_own_key: u32,
    /// The largest loss measured in one check, and the largest mean burst
    /// (any leg).
    pub loss_max: Option<f64>,
    pub burst_max: Option<f64>,
    pub repair_changes: u64,
    pub plan_changes: u64,
    /// Edges that did not answer a request for the compact lane and were
    /// asked without it.
    pub compact_fallbacks: u64,
    /// The short form is asked for (until an edge refuses it), and the
    /// edges that refused it and were asked again with the long form.
    pub short_header: bool,
    pub short_header_dropped: u64,
    pub nack_datagrams: u64,
    /// The part that the first edge said it would send first.
    pub first_subscribed_part: Option<u64>,
    /// Legs begun.
    pub edges: u64,
    /// When the first token and the first `OK` came (time to first audio,
    /// step by step).
    pub first_token_at: Option<Instant>,
    pub first_subscribed_at: Option<Instant>,
    /// While set, the session reports no failure (no answer, silence) and
    /// keeps its legs: the end-of-run drain, which waits for NACK answers.
    pub hold: bool,
    /// Set by the caller before [`Self::poll`]: datagrams are queued on its
    /// socket. NACKs wait for them (with `nack_after_drain`), and no leg is
    /// judged silent, hedged or dead (B 3.1: a client host that stalls
    /// would see every leg silent).
    pub queued: bool,
    /// Since when [`Self::queued`] has been set without a break: NACKs wait
    /// for a queue at most one NACK retry (see [`Self::poll`]).
    queued_since: Option<Instant>,
    /// The compact lane asked for again after a fall-back (see
    /// [`SessionConfig::compact_retry`]), and retries given up.
    pub compact_retries: u64,
    pub compact_retries_unanswered: u64,
    /// Legs failed because their edge's first part was 4,096 behind.
    pub stale_starts: u64,
    /// Legs failed because the receiver refused every datagram they brought
    /// for [`REFUSING_FOR`] (fix 4, F2: a stuck receiver is not silent; and
    /// stuck4k: on the RaptorQ lane, nothing taken for that long).
    pub refusing_legs: u64,
    /// Legs failed because no media came for [`STALLED_FOR`] while their
    /// edge said the stream went on (stuck4k).
    pub stalled_legs: u64,
    /// The joint part loss of the legs (smoothed) and its mean run.
    joint_estimate: Option<f64>,
    joint_burst: Option<f64>,
    /// Every leg went silent and no media came since: the longer silence
    /// applies, and the next report is due then. `silent_for`: the silence
    /// of the last report.
    all_silent: bool,
    next_all_silent: Option<Instant>,
    silent_for: Duration,
    next_loss_check: Instant,
    /// When the timers last ran: a verdict whose time is not after it was
    /// judged then, and [`Self::next_wake`] does not wake for it again.
    polled_at: Instant,
    pending: Option<PartObject>,
    /// The lanes of the last 16 parts a datagram delivered (bit set: a
    /// RaptorQ object), and whether [`LegEvent::ObjectLane`] went.
    raptorq_recent: u16,
    delivered_recent: u8,
    object_lane: bool,
    /// Place240 rule 3: the parts judged for a lone leg, the leg judged
    /// now, the last verdict reported, and the receiver's flows seen.
    pub quality: QualityJudge,
    judged_leg: Option<LegId>,
    degraded_at: Option<Instant>,
    quality_flows: u64,
    /// The slot of the leg whose datagram came last, and when.
    media_from: Option<(usize, Instant)>,
    /// Lone legs reported late or missing parts (place240), and the
    /// edges' distress hints heard.
    pub degraded_late: u64,
    pub degraded_missing: u64,
    pub distress_hints: u64,
}

/// A released leg's second `UNSUB` goes this long after the first.
const SECOND_UNSUB_AFTER: Duration = Duration::from_millis(20);
/// A datagram from a released edge is answered with `UNSUB` for this long,
/// at most once each [`RELEASED_ANSWER_EVERY`].
const RELEASED_FOR: Duration = Duration::from_secs(10);
const RELEASED_ANSWER_EVERY: Duration = Duration::from_millis(100);
/// A leg's token is renewed (a tokenless `SUB` beside its refresh) this
/// long after the edge gave it: a token is valid at least 60 s.
pub const TOKEN_RENEW_AFTER: Duration = Duration::from_secs(45);
/// A leg is hedged only beside a leg that delivered a part first this
/// recently.
const FIRST_ARRIVAL_WINDOW: Duration = Duration::from_millis(20);
/// A leg is suspect when another leg is this many parts ahead of it, and it
/// has had no media for max([`SUSPECT_AFTER`], this many part intervals).
const SUSPECT_PARTS: u64 = 8;
const SUSPECT_AFTER: Duration = Duration::from_millis(20);
/// A live leg whose datagrams the receiver has refused for this long (an
/// old generation, parts below the session's first), while they still
/// come, has failed: it is replaced, and the generations refused are
/// forgotten (fix 4, F2).
pub const REFUSING_FOR: Duration = Duration::from_secs(1);
/// The fewest media datagrams refused in a row that fail a leg (with
/// [`REFUSING_FOR`]): a 20 ms stream sends 50 a second, a 1 ms one 1,000.
pub const REFUSING_MIN_DATAGRAMS: u64 = 32;
/// A live leg that has had no media for this long while its edge's `WAIT`s
/// show the stream's head moving, and no other leg delivers, has failed
/// (stuck4k: an edge waited 40 s for the part after the previous session's
/// last, while the session ran, and its keepalives kept every silence rule
/// from firing). It is replaced, and the generations refused are
/// forgotten.
pub const STALLED_FOR: Duration = Duration::from_secs(1);

impl UdpSession {
    pub fn new(config: SessionConfig, now: Instant) -> Self {
        let mut receiver = ObjectReceiver::new();
        // The session opens its legs itself.
        receiver.close_leg(LegId::FIRST);
        if config.known_parts > 0 {
            receiver.compact.set_known_parts(config.known_parts);
        }
        receiver.compact.set_nack_timing(config.nack_timing);
        receiver.compact.drop_early = config.drop_early_parts;
        receiver.compact.set_hole_give_up(config.hole_give_up);
        Self {
            config,
            receiver,
            legs: Default::default(),
            released: Vec::new(),
            events: VecDeque::new(),
            next_own_key: u32::MAX,
            loss_max: None,
            burst_max: None,
            repair_changes: 0,
            plan_changes: 0,
            compact_fallbacks: 0,
            short_header: config.compact_short_header,
            short_header_dropped: 0,
            nack_datagrams: 0,
            first_subscribed_part: None,
            edges: 0,
            first_token_at: None,
            first_subscribed_at: None,
            hold: false,
            queued: false,
            queued_since: None,
            compact_retries: 0,
            compact_retries_unanswered: 0,
            stale_starts: 0,
            refusing_legs: 0,
            stalled_legs: 0,
            joint_estimate: None,
            joint_burst: None,
            all_silent: false,
            next_all_silent: None,
            silent_for: config.silence,
            next_loss_check: now + LOSS_CHECK,
            polled_at: now,
            pending: None,
            raptorq_recent: 0,
            delivered_recent: 0,
            object_lane: false,
            quality: QualityJudge::new(config.quality),
            judged_leg: None,
            degraded_at: None,
            quality_flows: 1,
            media_from: None,
            degraded_late: 0,
            degraded_missing: 0,
            distress_hints: 0,
        }
    }

    fn legs_iter(&self) -> impl Iterator<Item = &Leg> {
        self.legs.iter().flatten()
    }

    /// The legs not given up.
    fn held(&self) -> impl Iterator<Item = &Leg> {
        self.legs_iter().filter(|leg| !leg.is_failed())
    }

    /// The slot of the leg named `key`.
    fn slot(&self, key: LegKey) -> Option<usize> {
        self.legs
            .iter()
            .position(|leg| leg.as_ref().is_some_and(|leg| leg.key == key))
    }

    fn by_key(&self, key: LegKey) -> Option<&Leg> {
        self.slot(key).and_then(|index| self.legs[index].as_ref())
    }

    /// The leg whose plan and repair the caller reports: the repair leg
    /// (the counted leg live longest), else the first leg held.
    fn main_leg(&self) -> Option<&Leg> {
        self.repair_leg()
            .and_then(|leg| self.legs[leg.index()].as_ref())
            .or_else(|| self.held().next())
    }

    /// The counted leg that has been live longest: it alone is asked for
    /// rows (B 3.3).
    fn repair_leg(&self) -> Option<LegId> {
        self.legs_iter()
            .filter(|leg| leg.is_counted())
            .min_by_key(|leg| (leg.live_at, leg.id))
            .map(|leg| leg.id)
    }

    /// The edge of the main leg.
    pub fn target(&self) -> Option<SocketAddr> {
        self.main_leg().map(|leg| leg.target)
    }

    pub fn token(&self) -> Option<u64> {
        self.main_leg().and_then(|leg| leg.token)
    }

    /// The RaptorQ repair that the main leg asks for.
    pub fn repair(&self) -> RepairPolicy {
        self.main_leg()
            .map_or(self.config.base_for(self.config.compact), |leg| leg.repair)
    }

    /// The compact plan that the main leg asks for.
    pub fn plan(&self) -> CompactPlan {
        self.main_leg()
            .map_or_else(|| self.start_plan(false), |leg| leg.plan)
    }

    /// Whether the main leg asks for the compact lane, and whether its edge
    /// serves it.
    pub fn compact_asked(&self) -> bool {
        self.main_leg()
            .map_or(self.config.compact, |leg| leg.compact_asked)
    }

    pub fn compact_confirmed(&self) -> bool {
        self.main_leg().is_some_and(|leg| leg.compact_confirmed)
    }

    /// Some leg's edge answered `OK`.
    pub fn is_subscribed(&self) -> bool {
        self.legs_iter().any(Leg::is_live)
    }

    /// What the client's report keeps of a leg.
    pub fn leg_report(&self, key: LegKey) -> Option<LegReport> {
        let leg = self.by_key(key)?;
        Some(LegReport {
            node: leg.node.clone(),
            target: leg.target,
            first_part: leg.first_part.or(leg.first_brought),
            compact: leg.compact_confirmed,
            rtt: leg.rtt,
            lag: self.receiver.compact.leg_lag(leg.id),
            loss: leg.loss_estimate,
            plan: leg.plan,
            datagrams: leg.datagrams,
            wire_bytes: leg.wire_bytes,
            longest_gap: leg.longest_gap,
            stats: self.receiver.compact.leg_stats(leg.id),
            lateness: leg.lateness.clone(),
        })
    }

    /// Leg `key`'s lag behind the first arrival of `part`, while the part
    /// is among the last 64 parts (see
    /// [`crate::client_compact::CompactReceiver::leg_lag_of`]).
    pub fn leg_lag_of(&self, key: LegKey, part: u64) -> Option<Duration> {
        let leg = self.by_key(key)?;
        self.receiver.compact.leg_lag_of(leg.id, part)
    }

    /// The plan a new leg starts on: beside another leg `C0.32.0`; alone,
    /// the plan for an unmeasured path (B X3).
    fn start_plan(&self, beside: bool) -> CompactPlan {
        let plan = if beside {
            CompactPlan {
                copies: 0,
                window: 32,
                window_repair: 0,
                short_header: false,
            }
        } else {
            CompactPlan::for_measured_with(
                UNMEASURED_LOSS,
                1.0,
                self.config.compact_burst_rows,
                self.config.residual_target,
            )
        };
        plan.with_short_header(self.short_header)
    }

    /// Whether a leg delivers now: live or leaving, and it brought a
    /// datagram of the stream (a first arrival, a duplicate or a row)
    /// within max(20 ms, 8 part intervals).
    fn delivers(&self, leg: &Leg, now: Instant) -> bool {
        leg.is_live()
            && leg
                .last_media
                .is_some_and(|at| now.saturating_duration_since(at) <= self.suspect_after())
    }

    /// The delivering legs (bits by slot).
    fn delivering_mask(&self, now: Instant) -> u8 {
        self.legs_iter()
            .filter(|leg| self.delivers(leg, now))
            .fold(0, |mask, leg| mask | leg.bit())
    }

    /// The delivering legs other than `leg` that a `SUB` names in `o=`
    /// (bits by slot): those with a node id, at most [`MAX_OTHER_LEGS`].
    fn others_bits(&self, leg: LegId, delivering: u8) -> u8 {
        let mask = delivering & !(1 << leg.index());
        let mut bits = 0;
        let mut named = 0;
        for other in self.legs_iter() {
            if mask & other.bit() != 0 && !other.node.is_empty() && named < MAX_OTHER_LEGS {
                bits |= other.bit();
                named += 1;
            }
        }
        bits
    }

    /// Send a leg's `SUB`: with its token unless `tokenless`, from
    /// `from_part`, with `r=1` if `resume`, and `o=` its delivering
    /// siblings.
    fn send_subscribe(
        &mut self,
        leg: LegId,
        from_part: Option<u64>,
        tokenless: bool,
        resume: bool,
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) {
        let bits = self.others_bits(leg, self.delivering_mask(now));
        let others: Vec<String> = self
            .legs_iter()
            .filter(|other| bits & other.bit() != 0)
            .map(|other| other.node.clone())
            .collect();
        let Some(state) = self.legs[leg.index()].as_mut() else {
            return;
        };
        state.others_sent = bits;
        let datagram = Control::Subscribe {
            stream_id: self.config.stream_id,
            from_part,
            repair: state.repair,
            token: if tokenless { None } else { state.token },
            fast_lane: self.config.fast_lane,
            compact: state.compact_asked.then_some(state.plan),
            others,
            resume,
        }
        .encode();
        send(state.target, &datagram);
    }

    fn unsubscribe(
        stream_id: u64,
        target: SocketAddr,
        token: u64,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) {
        send(target, &Control::Unsubscribe { stream_id, token }.encode());
    }

    /// The part a new leg asks its edge to start from (see [`StartFrom`]).
    fn start_from(&self, from: StartFrom) -> Option<u64> {
        let newest = self.receiver.compact.newest();
        let after = newest.map(|newest| newest + 1);
        match from {
            StartFrom::AfterNewest => after,
            StartFrom::OldestHole => match (self.receiver.compact.lowest_hole(), newest) {
                (Some(hole), Some(newest))
                    if self.config.move_from_hole
                        && hole < newest
                        && newest - hole <= MOVE_HOLE_PARTS =>
                {
                    Some(hole)
                }
                _ => after,
            },
        }
    }

    /// Open the leg `key` at `target` and send its first request (see
    /// [`SessionLegs::add_leg`]).
    #[allow(clippy::too_many_arguments)]
    fn open_leg(
        &mut self,
        key: LegKey,
        node: &str,
        target: SocketAddr,
        from_part: Option<u64>,
        token: Option<u64>,
        token_wait: Duration,
        measured: Option<PathMeasure>,
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) {
        if self.slot(key).is_some() {
            return;
        }
        let beside = self.held().next().is_some();
        let Some(id) = self.receiver.open_leg() else {
            return;
        };
        // The edge's datagrams are this leg's now: no more UNSUB to them.
        self.released.retain(|released| released.target != target);
        // A leg beside another starts on the plan of two legs; so do the
        // legs that have not measured their path yet (a join adds two).
        let start = self.start_plan(beside);
        if beside {
            for leg in self.legs.iter_mut().flatten() {
                if leg.loss_samples < PLAN_SAMPLES && leg.plan != start {
                    leg.plan = start;
                    leg.plan_at = now;
                    leg.next_refresh = now;
                }
            }
        }
        let compact_asked = self.config.compact;
        let base_repair = self.config.base_for(compact_asked);
        let repair = if self.config.fixed_repair {
            base_repair
        } else {
            RepairPolicy::for_loss(UNMEASURED_LOSS, base_repair)
        };
        self.legs[id.index()] = Some(Leg {
            key,
            id,
            node: node.to_owned(),
            target,
            state: if token.is_some() {
                State::Tokened
            } else {
                State::Asking
            },
            leaving: None,
            failure: None,
            added_at: now,
            ok_at: None,
            live_at: None,
            token,
            token_at: None,
            token_wait,
            from_part,
            media_here: false,
            first_part: None,
            first_brought: None,
            asked_at: now,
            request_sent_at: now,
            unanswered: 1,
            repeats: 0,
            double_at: None,
            refresh_sent_at: None,
            takeback_at: None,
            rtt: None,
            last_datagram: now,
            heard_at: None,
            first_media: None,
            last_media: None,
            last_first: None,
            next_refresh: now,
            passed_over: false,
            hedge_due: false,
            suspect: false,
            others_sent: 0,
            compact_asked,
            compact_confirmed: false,
            compact_retries_here: 0,
            compact_retry_at: None,
            base_repair,
            repair,
            loss_estimate: None,
            compact_estimate: None,
            burst_estimate: None,
            loss_samples: 0,
            bursty: false,
            plan_at: now,
            plan: start,
            datagrams: 0,
            wire_bytes: 0,
            longest_gap: Duration::ZERO,
            wait_heads: None,
            lateness: LatenessHistogram::default(),
        });
        if let Some(measured) = measured {
            let config = self.config;
            let leg = self.legs[id.index()].as_mut().expect("the leg opened");
            leg.loss_estimate = measured.loss_estimate;
            leg.compact_estimate = measured.compact_estimate;
            leg.burst_estimate = measured.burst_estimate;
            leg.loss_samples = measured.loss_samples;
            leg.bursty = measured.bursty;
            leg.plan = measured.plan.with_short_header(self.short_header);
            leg.repair = policy_for(&config, leg);
        }
        self.edges += 1;
        self.send_subscribe(id, from_part, token.is_none(), false, now, send);
        let double = self.config.first_double;
        let leg = self.legs[id.index()].as_mut().expect("the leg opened");
        leg.double_at = (!double.is_zero()).then(|| now + double);
        leg.next_refresh = now + unanswered_wait(&self.config, leg);
        self.sync_nack(id);
    }

    /// Let a leg go: `UNSUB` now (and again in 20 ms, when `twice`), and its
    /// edge's datagrams answered with `UNSUB` for a while.
    fn drop_leg(
        &mut self,
        index: usize,
        twice: bool,
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) {
        let Some(state) = self.legs[index].take() else {
            return;
        };
        if let Some(token) = state.token {
            Self::unsubscribe(self.config.stream_id, state.target, token, send);
        }
        self.receiver.close_leg(state.id);
        self.released
            .retain(|released| released.target != state.target);
        self.released.push(Released {
            target: state.target,
            token: state.token,
            until: now + RELEASED_FOR,
            second_unsub: (twice && state.token.is_some()).then(|| now + SECOND_UNSUB_AFTER),
            answered_at: None,
        });
    }

    /// The session gives a leg up (or its edge refused it): nothing more is
    /// taken from it, and `event` reports it. The leg stays, holding its
    /// slot, until it is released.
    fn stop_leg(&mut self, index: usize, event: LegEvent, reason: String) {
        let Some(leg) = self.legs[index].as_mut().filter(|leg| !leg.is_failed()) else {
            return;
        };
        leg.state = State::Failed;
        leg.failure = Some(reason);
        leg.double_at = None;
        let id = leg.id;
        self.receiver.compact.set_leg_nack(id, false);
        self.events.push_back(event);
    }

    /// Whether the receiver NACKs on a leg: its edge gave the token and
    /// serves it the compact lane.
    fn sync_nack(&mut self, leg: LegId) {
        let nack = self.legs[leg.index()].as_ref().is_some_and(|state| {
            state.token.is_some() && state.compact_confirmed && state.is_live()
        });
        self.receiver.compact.set_leg_nack(leg, nack);
    }

    /// Subscribe at `target` from `from_part` (the part after the last one
    /// received, if any), as the one leg of the session: the legs held are
    /// let go first (`UNSUB`, so that they stop sending at once). A move
    /// asks the new edge for the oldest hole the old one left, at most
    /// [`MOVE_HOLE_PARTS`] back (with [`SessionConfig::move_from_hole`]).
    pub fn begin(
        &mut self,
        target: SocketAddr,
        from_part: Option<u64>,
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) {
        let moving = self.legs_iter().next().is_some();
        let from_part = match (from_part, self.receiver.compact.lowest_hole()) {
            (Some(from), Some(hole))
                if self.config.move_from_hole
                    && moving
                    && hole < from
                    && from - hole <= MOVE_HOLE_PARTS =>
            {
                Some(hole)
            }
            (from, _) => from,
        };
        // The new edge is on the same client path: it starts from what the
        // last one measured, as before legs.
        let measured = self.main_leg().map(|leg| PathMeasure {
            loss_estimate: leg.loss_estimate,
            compact_estimate: leg.compact_estimate,
            burst_estimate: leg.burst_estimate,
            loss_samples: leg.loss_samples,
            bursty: leg.bursty,
            plan: leg.plan,
        });
        for index in 0..MAX_LEGS {
            self.drop_leg(index, false, now, send);
        }
        // What the legs let go reported is the caller's to act on now.
        self.events.clear();
        self.pending = None;
        let key = LegKey(self.next_own_key);
        self.next_own_key -= 1;
        let token_wait = self.config.token_wait;
        self.open_leg(
            key, "", target, from_part, None, token_wait, measured, now, send,
        );
    }

    /// Leave every leg's edge (`UNSUB`), at the end of a run. The legs are
    /// kept for the report.
    pub fn leave(&mut self, send: &mut dyn FnMut(SocketAddr, &[u8])) {
        for index in 0..MAX_LEGS {
            if let Some(leg) = self.legs[index].as_mut() {
                if let Some(token) = leg.token.take() {
                    Self::unsubscribe(self.config.stream_id, leg.target, token, send);
                }
                if !leg.is_failed() {
                    leg.state = State::Asking;
                }
            }
            self.receiver
                .compact
                .set_leg_nack(LegId::from_index(index), false);
        }
    }

    /// The timers, for a caller that holds one leg and moves itself: as
    /// [`Self::poll_legs`], and the reason to move if its leg failed or
    /// went silent.
    pub fn poll(
        &mut self,
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) -> Option<String> {
        self.poll_legs(now, send);
        let mut reason = None;
        while let Some(event) = self.events.pop_front() {
            let found = match event {
                LegEvent::Failed { leg, cause } => self
                    .by_key(leg)
                    .and_then(|leg| leg.failure.clone())
                    .unwrap_or_else(|| cause.reason().to_owned()),
                LegEvent::AllSilent => format!("silence-{}ms", self.silent_for.as_millis()),
                _ => continue,
            };
            reason.get_or_insert(found);
        }
        reason
    }

    /// The timers of every leg: requests, refreshes, NACKs, loss checks and
    /// the verdicts of B 3.2. What they found comes out of
    /// [`SessionLegs::next_event`]. Run it when [`Self::next_wake`] comes.
    pub fn poll_legs(&mut self, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8])) {
        let stream_id = self.config.stream_id;
        self.released.retain_mut(|released| {
            if let (Some(at), Some(token)) = (released.second_unsub, released.token) {
                if now >= at {
                    released.second_unsub = None;
                    Self::unsubscribe(stream_id, released.target, token, send);
                }
            }
            now < released.until
        });
        self.judge_legs(now);
        if now >= self.next_loss_check {
            self.next_loss_check = now + LOSS_CHECK;
            self.check_loss(now);
        }
        self.send_nacks(now, send);
        self.refresh_legs(now, send);
        self.polled_at = now;
    }

    /// The silence after which every leg counts as silent (see
    /// [`SessionConfig::silence_repeat`]).
    fn silence(&self) -> Duration {
        if self.all_silent {
            self.config.silence.max(self.config.silence_repeat)
        } else {
            self.config.silence
        }
    }

    /// max(20 ms, 8 part intervals): how long a leg goes without media
    /// before it is suspect, or no longer delivering.
    fn suspect_after(&self) -> Duration {
        let interval = self
            .receiver
            .compact
            .part_interval()
            .unwrap_or(Duration::ZERO);
        SUSPECT_AFTER.max(interval * SUSPECT_PARTS as u32)
    }

    /// The verdicts on each leg (B 3.2): the answer and token waits, and,
    /// on an empty socket, suspect, hedged, dead and every leg silent.
    fn judge_legs(&mut self, now: Instant) {
        let config = self.config;
        let mut failed: Vec<(usize, LegFailure, String)> = Vec::new();
        for (index, leg) in self.legs.iter_mut().enumerate() {
            let Some(leg) = leg
                .as_mut()
                .filter(|leg| !leg.is_live() && !leg.is_failed())
            else {
                continue;
            };
            let waited = now.saturating_duration_since(leg.asked_at);
            if !self.hold && waited > config.answer {
                failed.push((
                    index,
                    LegFailure::NoAnswer,
                    format!("no-answer-{}ms", config.answer.as_millis()),
                ));
            } else if leg.token.is_none() && !leg.passed_over && waited >= leg.token_wait {
                leg.passed_over = true;
                self.events.push_back(LegEvent::NoToken(leg.key));
            }
        }
        if !self.hold && !self.queued {
            let suspect_after = self.suspect_after();
            let live = self.legs_iter().filter(|leg| leg.is_live()).count();
            for index in 0..MAX_LEGS {
                let Some(leg) = self.legs[index].as_ref().filter(|leg| leg.is_live()) else {
                    continue;
                };
                let (id, key) = (leg.id, leg.key);
                let quiet = now.saturating_duration_since(leg.last_datagram);
                let no_media = now.saturating_duration_since(leg.media_since());
                let here = self.receiver.compact.leg_highest(id);
                let (mut other_datagram, mut other_delivers, mut other_first, mut ahead) =
                    (false, false, false, false);
                for other in self
                    .legs_iter()
                    .filter(|other| other.id != id && other.is_live())
                {
                    other_datagram |=
                        now.saturating_duration_since(other.last_datagram) < config.silence;
                    other_delivers |= !other.suspect
                        && other
                            .last_media
                            .is_some_and(|at| now.saturating_duration_since(at) <= suspect_after);
                    other_first |= other.last_first.is_some_and(|at| {
                        now.saturating_duration_since(at) <= FIRST_ARRIVAL_WINDOW
                    });
                    ahead |= match (self.receiver.compact.leg_highest(other.id), here) {
                        (Some(theirs), Some(ours)) => theirs >= ours + SUSPECT_PARTS,
                        (Some(_), None) => true,
                        _ => false,
                    };
                }
                // Refusing: datagrams at the stream's rate, none taken.
                if self.refusing(id, leg.ok_at.unwrap_or(leg.added_at), now) {
                    failed.push((index, LegFailure::Refusing, LegFailure::Refusing.reason().to_owned()));
                    continue;
                }
                // Stalled: the edge says the stream goes on and sends none
                // of it (another leg that delivers judges it sooner).
                if !other_delivers && Self::stalled(leg, now) {
                    failed.push((index, LegFailure::Stalled, LegFailure::Stalled.reason().to_owned()));
                    continue;
                }
                // Dead: silent while another leg sends, or without media
                // while another delivers.
                if quiet >= config.silence && other_datagram {
                    let reason = format!("silence-{}ms", config.silence.as_millis());
                    failed.push((index, LegFailure::Dead, reason));
                    continue;
                }
                if no_media >= config.silence && other_delivers {
                    let reason = format!("behind-{}ms", config.silence.as_millis());
                    failed.push((index, LegFailure::Dead, reason));
                    continue;
                }
                let suspect = !leg.suspect && ahead && no_media >= suspect_after;
                let hedge = !leg.hedge_due
                    && if live > 1 {
                        no_media >= config.hedge_after && other_first
                    } else {
                        quiet >= config.lone_hedge_after
                    };
                let leg = self.legs[index].as_mut().expect("a live leg");
                if suspect {
                    leg.suspect = true;
                }
                if hedge {
                    leg.hedge_due = true;
                    self.events.push_back(LegEvent::HedgeDue(key));
                }
                if suspect {
                    // The plans without it, at once.
                    self.plan_legs(now);
                }
            }
            // Every leg silent: the silence says nothing about any one edge,
            // and nobody is failed.
            let silence = self.silence();
            if live > 0
                && self.next_all_silent.is_none_or(|at| now >= at)
                && self
                    .legs_iter()
                    .filter(|leg| leg.is_live())
                    .all(|leg| now.saturating_duration_since(leg.last_datagram) > silence)
            {
                self.all_silent = true;
                self.silent_for = silence;
                self.next_all_silent = Some(now + config.silence.max(config.silence_repeat));
                self.events.push_back(LegEvent::AllSilent);
            }
        }
        self.judge_quality(now);
        for (index, cause, reason) in failed {
            if matches!(cause, LegFailure::Refusing | LegFailure::Stalled) {
                if cause == LegFailure::Refusing {
                    self.refusing_legs += 1;
                } else {
                    self.stalled_legs += 1;
                }
                // Whatever the receiver took for old, its replacement is
                // judged afresh.
                self.receiver.forget_sessions();
                // With no other leg delivering, what the session delivered
                // says nothing of the stream now (stuck4k: the previous
                // session's last part): its replacement starts at its edge's
                // head, not after that part.
                let failed_id = self.legs[index].as_ref().map(|leg| leg.id);
                let others_deliver = self.legs_iter().any(|other| {
                    Some(other.id) != failed_id
                        && other.is_live()
                        && other
                            .last_first
                            .is_some_and(|at| now.saturating_duration_since(at) < REFUSING_FOR)
                });
                if !others_deliver {
                    self.receiver.new_flow();
                    self.quality.new_flow();
                }
            }
            if let Some(key) = self.legs[index].as_ref().map(|leg| leg.key) {
                self.stop_leg(index, LegEvent::Failed { leg: key, cause }, reason);
            }
        }
    }

    /// Place240 rule 3: the parts of a lone leg (the only live leg not
    /// given up), judged on an empty socket and outside the end-of-run
    /// hold. A verdict is reported once ([`LegEvent::Degraded`]), and again
    /// two windows later if the leg is still the lone one.
    fn judge_quality(&mut self, now: Instant) {
        let verdict = self.quality.judge(now);
        let lone = {
            let mut live = self.legs_iter().filter(|leg| leg.is_live());
            match (live.next(), live.next()) {
                (Some(leg), None) => Some((leg.id, leg.key, leg.state == State::Live)),
                _ => None,
            }
        };
        let Some((id, key, live)) = lone else {
            self.judged_leg = None;
            return;
        };
        if self.judged_leg != Some(id) {
            // A new lone leg: the window starts again.
            self.judged_leg = Some(id);
            self.quality.reset();
            self.degraded_at = None;
            return;
        }
        let Some(verdict) = verdict.filter(|_| live && !self.hold && !self.queued) else {
            return;
        };
        let again = self.config.quality.window * 2;
        if self
            .degraded_at
            .is_some_and(|at| now.saturating_duration_since(at) < again)
        {
            return;
        }
        self.degraded_at = Some(now);
        match verdict {
            crate::client_quality::Verdict::LateParts => self.degraded_late += 1,
            crate::client_quality::Verdict::MissingParts => self.degraded_missing += 1,
        }
        self.events.push_back(LegEvent::Degraded {
            leg: key,
            reason: verdict.reason().to_owned(),
        });
    }

    /// An object that came out of the receiver: its part judged and its
    /// lateness booked to the leg that brought it (per part, not per
    /// datagram).
    fn observe(&mut self, object: &PartObject) {
        if self.receiver.flows != self.quality_flows {
            self.quality_flows = self.receiver.flows;
            self.quality.new_flow();
        }
        let Some((index, at)) = self.media_from else {
            return;
        };
        match object.kind {
            ObjectKind::Part => {
                let arrival_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.as_micros() as u64);
                let late = self
                    .quality
                    .on_part(object.part, object.published_unix_us, arrival_us, at);
                if let (Some(late), Some(leg)) = (late, self.legs[index].as_mut()) {
                    leg.lateness.record(late);
                }
            }
            ObjectKind::Gap => self.quality.on_gap(object.part, at),
            ObjectKind::Init => {}
        }
    }

    /// Whether the receiver has refused every datagram of leg `id` for
    /// [`REFUSING_FOR`] (from its last taken, else from `since`), at least
    /// [`REFUSING_MIN_DATAGRAMS`] of them. (A leg whose datagrams stop is
    /// judged by the silence rules.)
    fn refusing(&self, id: LegId, since: Instant, now: Instant) -> bool {
        let (run, accepted) = self.receiver.compact.leg_refused(id);
        // The RaptorQ lane (stuck4k): its datagrams since a part of it was
        // last taken, whether refused or never whole.
        let (untaken, taken) = self.receiver.leg_untaken(id);
        let last = match (accepted, taken) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        run.saturating_add(untaken) >= REFUSING_MIN_DATAGRAMS
            && now.saturating_duration_since(last.unwrap_or(since)) >= REFUSING_FOR
    }

    /// Whether a live leg has stalled: its edge's `WAIT`s have shown the
    /// stream's head moving for [`STALLED_FOR`], and no media came since
    /// (a source that pauses names one head; one that restarts sends its
    /// new parts within the second).
    fn stalled(leg: &Leg, now: Instant) -> bool {
        leg.wait_heads
            .and_then(|(_, moved)| moved)
            .is_some_and(|moved| now.saturating_duration_since(moved) >= STALLED_FOR)
    }

    /// The loss check of every leg: their repair, and the plans.
    fn check_loss(&mut self, now: Instant) {
        for index in 0..MAX_LEGS {
            let Some(leg) = self.legs[index].as_ref().filter(|leg| !leg.is_failed()) else {
                continue;
            };
            let id = leg.id;
            let policy = leg.repair;
            let Some(loss) = self.receiver.take_loss(id, policy) else {
                continue;
            };
            let burst = self.receiver.compact.take_burst(id);
            self.loss_max = Some(self.loss_max.map_or(loss, |max| max.max(loss)));
            if let Some(burst) = burst {
                self.burst_max = Some(self.burst_max.map_or(burst, |max| max.max(burst)));
            }
            let leg = self.legs[index].as_mut().expect("the leg checked");
            leg.loss_samples = leg.loss_samples.saturating_add(1);
            // Rise at once, fall slowly.
            leg.loss_estimate = Some(match leg.loss_estimate {
                Some(previous) if loss < previous => previous * 0.8 + loss * 0.2,
                _ => loss,
            });
            // The compact plan follows a smoother estimate: NACK covers
            // what a short rise of the loss leaves.
            leg.compact_estimate = Some(smoothed_loss_by(
                leg.compact_estimate,
                loss,
                self.config.plan_smoothing,
            ));
            if let Some(burst) = burst {
                leg.burst_estimate = Some(match leg.burst_estimate {
                    Some(previous) => previous * 0.7 + burst * 0.3,
                    None => burst,
                });
            }
            if !self.config.fixed_repair {
                let wanted = RepairPolicy::for_loss(
                    leg.loss_estimate.unwrap_or(UNMEASURED_LOSS),
                    leg.base_repair,
                );
                if wanted != leg.repair {
                    leg.repair = wanted;
                    self.repair_changes += 1;
                    leg.next_refresh = now;
                }
            }
        }
        if let Some((loss, burst)) = self.receiver.compact.take_joint(now) {
            self.joint_estimate = Some(smoothed_loss(self.joint_estimate, loss));
            if let Some(burst) = burst {
                self.joint_burst = Some(match self.joint_burst {
                    Some(previous) => previous * 0.7 + burst * 0.3,
                    None => burst,
                });
            }
        }
        if !self.config.fixed_repair {
            self.plan_legs(now);
        }
    }

    /// A leg's measures for its plan, with the burst band (B X3).
    fn leg_loss(leg: &mut Leg) -> LegLoss {
        let burst = leg.burst_estimate.unwrap_or(1.0);
        leg.bursty = if leg.bursty {
            burst >= BURSTY_UNTIL
        } else {
            burst >= BURSTY
        };
        LegLoss {
            loss: leg.compact_estimate.unwrap_or(0.0),
            burst: if leg.bursty {
                burst.max(BURSTY)
            } else {
                burst.min(BURSTY_UNTIL)
            },
            rtt: leg.rtt,
        }
    }

    /// The compact plans of the counted legs (B 3.3): one counted leg
    /// plans for its own path once it has [`PLAN_SAMPLES`] samples; two
    /// plan together once both have one. The repair leg's bytes of a part
    /// are the ones the receiver keeps.
    fn plan_legs(&mut self, now: Instant) {
        let repair = self.repair_leg();
        self.receiver.compact.set_prefer_leg(repair);
        let Some(repair) = repair else {
            return;
        };
        let mut counted: Vec<(Option<Instant>, LegId)> = self
            .legs_iter()
            .filter(|leg| leg.is_counted() && leg.id != repair)
            .map(|leg| (leg.live_at, leg.id))
            .collect();
        counted.sort();
        let interval = self.receiver.compact.part_interval();
        let residual = self.config.residual_target;
        let burst_rows = self.config.compact_burst_rows;
        let short = self.short_header;
        let repair_loss = {
            let leg = self.legs[repair.index()].as_mut().expect("the repair leg");
            if counted.is_empty() && leg.loss_samples < PLAN_SAMPLES {
                return;
            }
            if leg.compact_estimate.is_none() {
                return;
            }
            Self::leg_loss(leg)
        };
        let mut others = Vec::with_capacity(counted.len());
        for (_, id) in &counted {
            let leg = self.legs[id.index()].as_mut().expect("a counted leg");
            if leg.compact_estimate.is_none() {
                return;
            }
            others.push((*id, Self::leg_loss(leg)));
        }
        let joint = self
            .joint_estimate
            .map(|loss| (loss, self.joint_burst.unwrap_or(1.0)));
        let plans = PathPlans::for_legs(
            repair_loss,
            others.first().map(|(_, loss)| *loss),
            joint,
            interval,
            residual,
            burst_rows,
        );
        self.offer_plan(repair, plans.repair.with_short_header(short), now);
        if let Some(other) = plans.other {
            for (id, _) in &others {
                self.offer_plan(*id, other.with_short_header(short), now);
            }
        }
    }

    /// Take `wanted` as a leg's plan: at once if it asks for as much or
    /// more; if it asks for less, once [`PLAN_HOLD`] has passed since the
    /// leg's last plan change.
    fn offer_plan(&mut self, leg: LegId, wanted: CompactPlan, now: Instant) {
        let Some(leg) = self.legs[leg.index()].as_mut() else {
            return;
        };
        if wanted == leg.plan {
            return;
        }
        if plan_cost(wanted) < plan_cost(leg.plan)
            && now.saturating_duration_since(leg.plan_at) < self.config.plan_hold
        {
            return;
        }
        leg.plan_at = now;
        leg.plan = wanted;
        leg.next_refresh = now;
        self.plan_changes += 1;
    }

    /// Each leg's refresh, its first request's double, and its compact
    /// lane's fall-back (B 3.1).
    fn refresh_legs(&mut self, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8])) {
        let config = self.config;
        let delivering = self.delivering_mask(now);
        for index in 0..MAX_LEGS {
            let Some(leg) = self.legs[index].as_ref().filter(|leg| !leg.is_failed()) else {
                continue;
            };
            let id = leg.id;
            let others = self.others_bits(id, delivering);
            let leg = self.legs[index].as_mut().expect("a leg");
            // `o=` follows the delivering legs at once.
            if leg.is_live() && others != leg.others_sent {
                leg.next_refresh = leg.next_refresh.min(now);
            }
            if leg.double_at.is_some_and(|at| now >= at) {
                leg.double_at = None;
                let from = leg.from_part.filter(|_| !leg.media_here);
                let tokenless = leg.token.is_none();
                self.send_subscribe(id, from, tokenless, false, now, send);
            }
            let leg = self.legs[index].as_mut().expect("a leg");
            if leg
                .compact_retry_at
                .is_some_and(|at| now.saturating_duration_since(at) >= COMPACT_FALLBACK_AFTER)
            {
                // The retry went unanswered while the subscription without
                // the lane stood: asked without it again, and not retried
                // at this edge.
                leg.compact_retry_at = None;
                leg.compact_retries_here = COMPACT_RETRIES;
                leg.compact_asked = false;
                leg.base_repair = config.base_for(false);
                leg.repair = policy_for(&config, leg);
                leg.next_refresh = now;
                self.compact_retries_unanswered += 1;
            }
            if now < leg.next_refresh {
                continue;
            }
            // An edge that has not answered two requests for the compact
            // lane (an edge older than the lane drops them) is asked
            // without it, on the RaptorQ lane with its repair: for the rest
            // of the run, so only after 1.5 s of silence (a lost ARP request
            // alone holds a path's first datagrams for a second; wave 7
            // review).
            if falls_back_from_compact(
                leg.compact_asked,
                leg.token.is_some() || leg.is_live(),
                leg.unanswered,
                now.saturating_duration_since(leg.asked_at),
            ) {
                leg.compact_asked = false;
                leg.base_repair = config.base_for(false);
                leg.repair = policy_for(&config, leg);
                self.compact_fallbacks += 1;
            }
            // Before the edge answers, keep asking from the same part
            // (until it sends parts: it has admitted the client).
            let live = leg.is_live();
            let from = if live || leg.media_here {
                None
            } else {
                leg.from_part
            };
            // A refresh with nothing else outstanding measures a round trip
            // by its answer; one sent while another is unanswered gives none.
            leg.refresh_sent_at = (live && leg.refresh_sent_at.is_none()).then_some(now);
            if !live {
                if leg.token.is_none() {
                    leg.unanswered += 1;
                }
                leg.repeats = leg.repeats.saturating_add(1);
            }
            // The token is renewed while it is 45 s old or more (or of an
            // age unknown, as the spare's handed to a hedge), at every
            // refresh until the edge's TOKEN comes: a lost renewal is sent
            // again 2 s later, not 45 s later.
            let renew = live
                && leg.token.is_some()
                && leg
                    .token_at
                    .is_none_or(|at| now.saturating_duration_since(at) >= TOKEN_RENEW_AFTER);
            leg.next_refresh = now
                + if live {
                    REFRESH
                } else {
                    unanswered_wait(&config, leg)
                };
            self.send_subscribe(id, from, false, false, now, send);
            if renew {
                // The token is renewed before the edge forgets it: a
                // tokenless SUB, which the edge answers with TOKEN and
                // nothing else.
                self.send_subscribe(id, None, true, false, now, send);
            }
        }
    }

    /// The NACKs due, each to its legs' edges, while the socket is empty
    /// (or no longer than one NACK retry: a stream that keeps the socket
    /// busy, 4K at 25k datagrams a second, never empties it; wave 8
    /// review).
    fn send_nacks(&mut self, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8])) {
        let queued = self.queued && self.config.nack_after_drain;
        let drained = if queued {
            let since = *self.queued_since.get_or_insert(now);
            now.saturating_duration_since(since) >= self.receiver.compact.nack_retry()
        } else {
            self.queued_since = None;
            true
        };
        if !drained || !self.held().any(|leg| leg.compact_confirmed) {
            return;
        }
        let mut due = self.receiver.nacks_due(now);
        for leg in self.legs.iter().flatten() {
            let parts = due.take(leg.id);
            let Some(token) = leg.token.filter(|_| !parts.is_empty()) else {
                continue;
            };
            self.nack_datagrams += 1;
            send(
                leg.target,
                &Control::Nack {
                    stream_id: self.config.stream_id,
                    token,
                    parts,
                }
                .encode(),
            );
        }
    }

    /// When [`Self::poll_legs`] wants to run next.
    pub fn next_wake(&self) -> Instant {
        let mut wake = self.next_loss_check;
        // A verdict's time that the last poll judged already is not woken
        // for again (no spin while, say, every leg stays silent).
        let polled = self.polled_at;
        let verdict = |wake: &mut Instant, at: Instant| {
            if at > polled {
                *wake = (*wake).min(at);
            }
        };
        let ms = Duration::from_millis(1);
        for released in &self.released {
            if let Some(at) = released.second_unsub {
                wake = wake.min(at);
            }
        }
        let judging = !self.hold && !self.queued;
        let suspect_after = self.suspect_after();
        let live = self.legs_iter().filter(|leg| leg.is_live()).count();
        let mut last_datagram: Option<Instant> = None;
        for leg in self.held() {
            wake = wake.min(leg.next_refresh);
            if let Some(at) = leg.double_at {
                wake = wake.min(at);
            }
            if !leg.is_live() {
                // A held session reports no failure: no wake for one.
                if !self.hold {
                    verdict(&mut wake, leg.asked_at + self.config.answer + ms);
                }
                if leg.token.is_none() && !leg.passed_over {
                    verdict(&mut wake, leg.asked_at + leg.token_wait);
                }
                continue;
            }
            last_datagram =
                Some(last_datagram.map_or(leg.last_datagram, |at| at.max(leg.last_datagram)));
            if !judging {
                continue;
            }
            let media = leg.media_since();
            if let Some((_, Some(moved))) = leg.wait_heads {
                verdict(&mut wake, moved + STALLED_FOR + ms);
            }
            if live > 1 {
                verdict(&mut wake, leg.last_datagram + self.config.silence);
                verdict(&mut wake, media + self.config.silence);
                if !leg.hedge_due {
                    verdict(&mut wake, media + self.config.hedge_after);
                }
                if !leg.suspect {
                    verdict(&mut wake, media + suspect_after);
                }
            } else if !leg.hedge_due {
                verdict(&mut wake, leg.last_datagram + self.config.lone_hedge_after);
            }
        }
        if let Some(at) = self.quality.next_deadline().filter(|_| judging) {
            verdict(&mut wake, at);
        }
        if let Some(last) = last_datagram.filter(|_| judging) {
            let at = (last + self.silence() + ms).max(self.next_all_silent.unwrap_or(last));
            verdict(&mut wake, at);
        }
        if self.held().any(|leg| leg.compact_confirmed) {
            if let Some(at) = self.receiver.compact.next_nack_at() {
                wake = wake.min(at);
            }
        }
        wake
    }

    /// Which leg a datagram from `peer` is for: the leg whose edge is
    /// exactly `peer`, else (unless `peer` is an edge let go) the only leg
    /// at that address (B 3.1).
    fn attribute(&self, peer: SocketAddr, now: Instant) -> Option<usize> {
        if let Some(index) = self
            .legs
            .iter()
            .position(|leg| leg.as_ref().is_some_and(|leg| leg.target == peer))
        {
            return Some(index);
        }
        if self
            .released
            .iter()
            .any(|released| released.target == peer && now < released.until)
        {
            return None;
        }
        let mut same_ip = self.legs_iter().filter(|leg| leg.target.ip() == peer.ip());
        match (same_ip.next(), same_ip.next()) {
            (Some(leg), None) => Some(leg.id.index()),
            _ => None,
        }
    }

    /// A datagram from `peer`, received at `now`.
    pub fn on_datagram(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) -> Received {
        let Some(index) = self.attribute(peer, now) else {
            // An edge let go in the last 10 s that still sends: UNSUB again.
            let exact = self.released.iter().any(|released| released.target == peer);
            if let Some(released) = self.released.iter_mut().find(|released| {
                (released.target == peer || (!exact && released.target.ip() == peer.ip()))
                    && now < released.until
            }) {
                if let Some(token) = released.token.filter(|_| {
                    released
                        .answered_at
                        .is_none_or(|at| now.saturating_duration_since(at) >= RELEASED_ANSWER_EVERY)
                }) {
                    released.answered_at = Some(now);
                    Self::unsubscribe(self.config.stream_id, released.target, token, send);
                }
            }
            return Received::Ignored;
        };
        let leg = self.legs[index].as_mut().expect("an attributed leg");
        if leg.is_failed() {
            // Given up: it stays only until the director releases it.
            return Received::Ignored;
        }
        let (id, key) = (leg.id, leg.key);
        leg.last_datagram = now;
        leg.heard_at = Some(now);
        let Some(control) = Control::decode(datagram) else {
            return self.on_media(index, datagram, now);
        };
        match control {
            Control::Token { token: given, .. } => {
                self.first_token_at.get_or_insert(now);
                leg.token_at = Some(now);
                if leg.token == Some(given) {
                    return Received::Control;
                }
                // A request and its answer: the round trip that the NACKs
                // wait for, from the request's first copy. Not when the
                // request was repeated (Karn): the answer may be to an
                // earlier copy (wave 9 review).
                if leg.token.is_none() && leg.repeats == 0 {
                    let rtt = now.saturating_duration_since(leg.request_sent_at);
                    leg.rtt = Some(rtt);
                    self.receiver.compact.set_leg_rtt(id, rtt);
                }
                let first = leg.token.is_none();
                leg.token = Some(given);
                if leg.state == State::Asking {
                    leg.state = State::Tokened;
                }
                if first {
                    self.events.push_back(LegEvent::Tokened(key));
                }
                let leg = self.legs[index].as_mut().expect("an attributed leg");
                let from = leg.from_part.filter(|_| !leg.media_here);
                let live = leg.is_live();
                self.send_subscribe(id, from, false, false, now, send);
                // The request with the token is a new request: its own
                // repeats, from the first wait, and its own double.
                if !live {
                    let leg = self.legs[index].as_mut().expect("an attributed leg");
                    leg.request_sent_at = now;
                    leg.repeats = 0;
                    leg.double_at = (first && !self.config.first_double.is_zero())
                        .then(|| now + self.config.first_double);
                    if !self.config.first_retry.is_zero() {
                        leg.next_refresh = now + unanswered_wait(&self.config, leg);
                    }
                }
                self.sync_nack(id);
                Received::Control
            }
            Control::Subscribed {
                next_part, compact, ..
            } => self.on_subscribed(index, next_part, compact, now),
            Control::Move { reason, .. } => {
                // A MOVE within a round trip of a take-back (the first
                // retry's wait while the leg has none), before its OK,
                // answers the SUB before it.
                let window = leg
                    .rtt
                    .unwrap_or_else(|| unanswered_wait(&self.config, leg));
                if leg
                    .takeback_at
                    .is_some_and(|at| now.saturating_duration_since(at) <= window)
                {
                    return Received::Control;
                }
                leg.takeback_at = None;
                // A leaving leg's edge repeats its MOVE every 250 ms on its
                // own: a MOVE then is no answer to the refresh, and gives no
                // round trip.
                if leg.leaving.is_some() {
                    leg.refresh_sent_at = None;
                } else {
                    self.sample_refresh(index, now);
                }
                let leg = self.legs[index].as_mut().expect("an attributed leg");
                if !leg.is_live() {
                    // The edge refuses a leg it never served.
                    let refused = LegEvent::Refused {
                        leg: key,
                        reason: reason.clone(),
                    };
                    self.stop_leg(index, refused, reason.clone());
                    return Received::Move(reason);
                }
                if leg.leaving.as_deref() != Some(reason.as_str()) {
                    leg.state = State::Leaving;
                    leg.leaving = Some(reason.clone());
                    self.events.push_back(LegEvent::Moved {
                        leg: key,
                        reason: reason.clone(),
                    });
                }
                Received::Move(reason)
            }
            Control::Refused { reason, .. } => {
                let refused = LegEvent::Refused {
                    leg: key,
                    reason: reason.clone(),
                };
                self.stop_leg(index, refused, reason.clone());
                Received::Move(reason)
            }
            Control::Waiting {
                head: Some(head), ..
            } => {
                if leg.is_live() {
                    leg.wait_heads = Some(match leg.wait_heads {
                        None => (head, None),
                        Some((first, None)) if first != head => (first, Some(now)),
                        Some(heads) => heads,
                    });
                }
                Received::Control
            }
            _ => Received::Control,
        }
    }

    /// A refresh answered: a round trip, if nothing else was outstanding.
    fn sample_refresh(&mut self, index: usize, now: Instant) {
        let Some(state) = self.legs[index].as_mut() else {
            return;
        };
        if let Some(sent) = state.refresh_sent_at.take() {
            let rtt = now.saturating_duration_since(sent);
            state.rtt = Some(state.rtt.map_or(rtt, |previous| (previous * 7 + rtt) / 8));
            self.receiver.compact.set_leg_rtt(state.id, rtt);
        }
    }

    /// The leg's `OK` and its first media have both come: it is live.
    fn report_live(&mut self, index: usize, now: Instant) {
        let Some(leg) = self.legs[index]
            .as_mut()
            .filter(|leg| leg.is_live() && leg.live_at.is_none() && leg.media_here)
        else {
            return;
        };
        leg.live_at = Some(now);
        self.events.push_back(LegEvent::Live {
            leg: leg.key,
            compact: leg.compact_confirmed,
        });
    }

    fn on_subscribed(
        &mut self,
        index: usize,
        next_part: u64,
        compact: bool,
        now: Instant,
    ) -> Received {
        let leg = self.legs[index].as_mut().expect("an attributed leg");
        let (id, key) = (leg.id, leg.key);
        // An edge older than the short form refuses a plan word with it
        // (C...7) and serves the RaptorQ lane: asked again at once with the
        // long form (wave 7 review). An edge that refuses the lane for
        // another reason refuses that too.
        if drops_short_header(compact, leg.compact_asked, leg.plan) {
            self.short_header = false;
            self.short_header_dropped += 1;
            for leg in self.legs.iter_mut().flatten() {
                leg.plan = leg.plan.with_short_header(false);
            }
            let leg = self.legs[index].as_mut().expect("an attributed leg");
            leg.next_refresh = now;
            return Received::Control;
        }
        let first = !leg.is_live() && leg.first_part.is_none();
        if leg.takeback_at.take().is_some() && leg.state == State::Leaving {
            leg.state = State::Live;
            leg.leaving = None;
            self.events.push_back(LegEvent::TakenBack(key));
        }
        if !first {
            self.sample_refresh(index, now);
        }
        let leg = self.legs[index].as_mut().expect("an attributed leg");
        if first {
            // The first answer to the SUB with the token: a round trip, if
            // only the double went.
            if leg.repeats == 0 && leg.token.is_some() {
                let rtt = now.saturating_duration_since(leg.request_sent_at);
                leg.rtt = Some(leg.rtt.map_or(rtt, |previous| (previous * 7 + rtt) / 8));
                self.receiver.compact.set_leg_rtt(id, rtt);
            }
            let leg = self.legs[index].as_mut().expect("an attributed leg");
            leg.double_at = None;
            match self
                .receiver
                .expect(id, self.config.stream_id, next_part, compact)
            {
                Expected::Started => {}
                Expected::Stale => {
                    self.stale_starts += 1;
                    let stale = LegEvent::Failed {
                        leg: key,
                        cause: LegFailure::Stale,
                    };
                    self.stop_leg(index, stale, LegFailure::Stale.reason().to_owned());
                    return Received::Control;
                }
                Expected::Displaces(stale) => {
                    self.stale_starts += 1;
                    if let Some(stale_key) = self.legs[stale.index()].as_ref().map(|leg| leg.key) {
                        let event = LegEvent::Failed {
                            leg: stale_key,
                            cause: LegFailure::Stale,
                        };
                        let reason = LegFailure::Stale.reason().to_owned();
                        self.stop_leg(stale.index(), event, reason);
                    }
                }
            }
            self.first_subscribed_at.get_or_insert(now);
            self.first_subscribed_part.get_or_insert(next_part);
            let leg = self.legs[index].as_mut().expect("an attributed leg");
            leg.first_part = Some(next_part);
            leg.state = State::Live;
            leg.ok_at = Some(now);
        }
        let leg = self.legs[index].as_mut().expect("an attributed leg");
        if !leg.is_live() {
            leg.state = State::Live;
            leg.ok_at.get_or_insert(now);
        }
        leg.compact_confirmed = compact;
        if compact {
            leg.compact_retry_at = None;
        }
        leg.next_refresh = now + REFRESH;
        // An edge that does not serve the compact lane (OK without C): the
        // RaptorQ lane's repair, at once.
        let wanted_base = self.config.base_for(compact);
        if wanted_base != leg.base_repair {
            leg.base_repair = wanted_base;
            leg.repair = policy_for(&self.config, leg);
            leg.next_refresh = now;
        }
        // Answered without the lane that the client gave up for want of an
        // answer: the edge hears it, so the lane is asked for again (at
        // most COMPACT_RETRIES times at this edge).
        let leg = self.legs[index].as_mut().expect("an attributed leg");
        if !compact
            && !leg.compact_asked
            && self.config.compact
            && self.config.compact_retry
            && self.compact_fallbacks > 0
            && leg.compact_retries_here < COMPACT_RETRIES
        {
            leg.compact_retries_here += 1;
            self.compact_retries += 1;
            leg.compact_retry_at = Some(now);
            leg.compact_asked = true;
            leg.base_repair = self.config.base_for(true);
            leg.repair = policy_for(&self.config, leg);
            leg.next_refresh = now;
        }
        self.sync_nack(id);
        // Parts that came before the OK: it is live now.
        self.report_live(index, now);
        Received::Subscribed { next_part, first }
    }

    fn on_media(&mut self, index: usize, datagram: &[u8], now: Instant) -> Received {
        self.media_from = Some((index, now));
        let leg = self.legs[index].as_mut().expect("an attributed leg");
        let id = leg.id;
        if let Some(last) = leg.last_media {
            leg.longest_gap = leg.longest_gap.max(now.saturating_duration_since(last));
        }
        leg.first_media.get_or_insert(now);
        leg.last_media = Some(now);
        leg.media_here = true;
        leg.hedge_due = false;
        leg.wait_heads = None;
        leg.datagrams += 1;
        leg.wire_bytes += (datagram.len() + IP_UDP_HEADER_BYTES) as u64;
        self.all_silent = false;
        self.next_all_silent = None;
        let first_before = self.receiver.compact.leg_stats(id).parts_first;
        let raptorq = !is_compact_datagram(datagram);
        self.pending = self.receiver.push(id, datagram);
        let delivered = self.pending.is_some()
            || self.receiver.compact.leg_stats(id).parts_first > first_before;
        let leg = self.legs[index].as_mut().expect("an attributed leg");
        if leg.first_brought.is_none() {
            leg.first_brought = self.receiver.compact.leg_highest(id);
        }
        if delivered {
            leg.last_first = Some(now);
            self.raptorq_recent = (self.raptorq_recent << 1) | u16::from(raptorq);
            self.delivered_recent = self.delivered_recent.saturating_add(1).min(16);
            if !self.object_lane && self.mostly_raptorq() {
                self.object_lane = true;
                self.events.push_back(LegEvent::ObjectLane);
            }
        }
        let leg = self.legs[index].as_mut().expect("an attributed leg");
        if leg.suspect {
            let newest = self.receiver.compact.newest();
            let here = self.receiver.compact.leg_highest(id);
            if here.is_some_and(|here| newest.is_none_or(|newest| here + SUSPECT_PARTS > newest)) {
                leg.suspect = false;
                self.plan_legs(now);
            }
        }
        self.report_live(index, now);
        Received::Media
    }

    /// 8 of the last 16 parts delivered came as RaptorQ objects: a stream
    /// over the two-path bit rate (B 3.9).
    fn mostly_raptorq(&self) -> bool {
        self.delivered_recent >= 16 && self.raptorq_recent.count_ones() >= 8
    }

    /// The objects that the last media datagram delivered, one at a time.
    pub fn next_object(&mut self) -> Option<PartObject> {
        let object = self.pending.take().or_else(|| self.receiver.next_ready())?;
        self.observe(&object);
        Some(object)
    }

    /// Whether [`Self::next_object`] has objects now: after an `OK`, the
    /// parts that came before it (take them at once, not after the next
    /// media datagram).
    pub fn has_objects(&self) -> bool {
        self.pending.is_some() || self.receiver.has_ready()
    }
}

/// A datagram of the compact lane (long or short form).
fn is_compact_datagram(datagram: &[u8]) -> bool {
    crate::client_compact::is_compact(datagram) || crate::client_compact::is_short(datagram)
}

impl SessionLegs for UdpSession {
    fn add_leg(
        &mut self,
        leg: LegKey,
        node: &str,
        addr: SocketAddr,
        start: LegStart,
        now: Instant,
        send: &mut dyn FnMut(SocketAddr, &[u8]),
    ) {
        let from = self.start_from(start.from);
        self.open_leg(
            leg,
            node,
            addr,
            from,
            start.token,
            start.token_wait,
            None,
            now,
            send,
        );
    }

    fn release_leg(&mut self, leg: LegKey, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8])) {
        if let Some(index) = self.slot(leg) {
            self.drop_leg(index, true, now, send);
        }
    }

    fn takeback_leg(&mut self, leg: LegKey, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8])) {
        let from = self.receiver.compact.newest().map(|newest| newest + 1);
        let Some(index) = self.slot(leg) else {
            return;
        };
        let state = self.legs[index].as_mut().expect("a slot held");
        state.takeback_at = Some(now);
        state.refresh_sent_at = None;
        let id = state.id;
        self.send_subscribe(id, from, false, false, now, send);
    }

    fn reask_leg(&mut self, leg: LegKey, now: Instant, send: &mut dyn FnMut(SocketAddr, &[u8])) {
        let from = self
            .receiver
            .compact
            .lowest_hole()
            .or_else(|| self.receiver.compact.newest().map(|newest| newest + 1));
        let Some(index) = self.slot(leg) else {
            return;
        };
        let state = self.legs[index].as_mut().expect("a slot held");
        state.refresh_sent_at = None;
        let id = state.id;
        self.send_subscribe(id, from, false, from.is_some(), now, send);
    }

    fn leg(&self, leg: LegKey, now: Instant) -> Option<LegView> {
        let leg = self.by_key(leg)?;
        Some(LegView {
            delivering: self.delivers(leg, now),
            steady: self.receiver.compact.steady(leg.id),
            first_part: leg.first_part.or(leg.first_brought),
            first_media: leg.first_media,
            last_media: leg.last_media,
            last_datagram: leg.heard_at,
            lag: self.receiver.compact.leg_lag(leg.id),
        })
    }

    fn level(&self, new: LegKey, old: LegKey) -> bool {
        match (self.by_key(new), self.by_key(old)) {
            (Some(new), Some(old)) => self.receiver.compact.level(new.id, old.id),
            _ => false,
        }
    }

    fn oldest_hole(&self) -> Option<u64> {
        self.receiver.compact.lowest_hole()
    }

    fn next_event(&mut self) -> Option<LegEvent> {
        self.events.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_udp::{encode_object, ObjectKind};
    use bytes::Bytes;

    fn edge() -> SocketAddr {
        "127.0.0.1:19446".parse().unwrap()
    }

    /// The session's only leg.
    fn only(session: &UdpSession) -> &Leg {
        let mut legs = session.legs.iter().flatten();
        let leg = legs.next().expect("a leg");
        assert!(legs.next().is_none(), "one leg");
        leg
    }

    fn only_mut(session: &mut UdpSession) -> &mut Leg {
        session.legs.iter_mut().flatten().next().expect("a leg")
    }

    /// The defaults without the doubled first requests (B X5), for the
    /// tests of the repeats' schedule.
    fn undoubled(stream_id: u64) -> SessionConfig {
        SessionConfig {
            first_double: Duration::ZERO,
            ..SessionConfig::new(stream_id)
        }
    }

    /// Wave 9 B: an unanswered request is repeated after 25 ms, then after
    /// twice as long each time up to 300 ms; the request with the token
    /// starts its own repeats, after three round trips at least. With
    /// `first_retry` zero, every 300 ms as before.
    #[test]
    fn an_unanswered_request_is_repeated_after_a_few_round_trips() {
        let now = Instant::now();
        let mut session = UdpSession::new(undoubled(4001), now);
        let mut sent: Vec<(Instant, Vec<u8>)> = Vec::new();
        session.begin(edge(), None, now, &mut |_, datagram| {
            sent.push((now, datagram.to_vec()))
        });
        let mut at = now;
        while at < now + Duration::from_millis(1_000) {
            at = session.next_wake();
            session.poll(at, &mut |_, datagram| sent.push((at, datagram.to_vec())));
        }
        let gaps: Vec<u64> = sent
            .windows(2)
            .map(|pair| pair[1].0.duration_since(pair[0].0).as_millis() as u64)
            .collect();
        assert_eq!(&gaps[..5], &[25, 50, 100, 200, 300]);
        // The token comes 4 ms after the last request: the request with
        // it is repeated 25 ms later (three round trips are 12 ms), then 50.
        let last = sent.last().unwrap().0;
        let token = Control::Token {
            stream_id: 4001,
            token: 77,
        }
        .encode();
        let answered = last + Duration::from_millis(4);
        let mut with_token: Vec<Instant> = Vec::new();
        session.on_datagram(edge(), &token, answered, &mut |_, _| {
            with_token.push(answered)
        });
        assert_eq!(with_token.len(), 1);
        while with_token.len() < 3 {
            let wake = session.next_wake();
            assert!(wake < answered + Duration::from_millis(200));
            session.poll(wake, &mut |_, datagram| {
                assert!(matches!(
                    Control::decode(datagram),
                    Some(Control::Subscribe {
                        token: Some(77),
                        ..
                    })
                ));
                with_token.push(wake)
            });
        }
        assert_eq!(with_token[1] - with_token[0], Duration::from_millis(25));
        assert_eq!(with_token[2] - with_token[1], Duration::from_millis(50));
        // A slower edge: three round trips.
        let mut session = UdpSession::new(undoubled(4001), now);
        session.begin(edge(), None, now, &mut |_, _| {});
        let answered = now + Duration::from_millis(40);
        session.on_datagram(edge(), &token, answered, &mut |_, _| {});
        assert_eq!(
            only(&session).next_refresh - answered,
            Duration::from_millis(120)
        );
        // Before wave 9: every 300 ms.
        let config = SessionConfig {
            first_retry: Duration::ZERO,
            ..undoubled(4001)
        };
        let mut session = UdpSession::new(config, now);
        session.begin(edge(), None, now, &mut |_, _| {});
        assert_eq!(only(&session).next_refresh - now, UNANSWERED_REFRESH);
        session.poll(now + UNANSWERED_REFRESH, &mut |_, _| {});
        assert_eq!(only(&session).next_refresh - now, UNANSWERED_REFRESH * 2);
    }

    /// Wave 9 review: an answer to a repeated request gives no round trip
    /// (Karn), and once the edge sends parts a repeated request no longer
    /// names the part to resume from (the edge would go back to it).
    #[test]
    fn a_repeated_request_gives_no_round_trip_and_stops_naming_its_part() {
        let now = Instant::now();
        let mut session = UdpSession::new(undoubled(4001), now);
        session.begin(edge(), Some(500), now, &mut |_, _| {});
        // The first request is repeated at 25 ms; the token comes at 45 ms
        // (an answer to either: 45 or 20 ms).
        let repeat = session.next_wake();
        assert_eq!(repeat - now, Duration::from_millis(25));
        session.poll(repeat, &mut |_, _| {});
        let token = Control::Token {
            stream_id: 4001,
            token: 9,
        }
        .encode();
        let answered = now + Duration::from_millis(45);
        let mut sent: Vec<Vec<u8>> = Vec::new();
        session.on_datagram(edge(), &token, answered, &mut |_, datagram| {
            sent.push(datagram.to_vec())
        });
        assert_eq!(only(&session).rtt, None);
        // So the request with the token waits the first retry, not 3 x 20.
        assert_eq!(
            only(&session).next_refresh - answered,
            Duration::from_millis(25)
        );
        match Control::decode(&sent[0]) {
            Some(Control::Subscribe { from_part, .. }) => assert_eq!(from_part, Some(500)),
            other => panic!("{other:?}"),
        }
        // Unrepeated, the answer is a sample.
        let mut fresh = UdpSession::new(SessionConfig::new(4001), now);
        fresh.begin(edge(), None, now, &mut |_, _| {});
        fresh.on_datagram(edge(), &token, answered, &mut |_, _| {});
        assert_eq!(only(&fresh).rtt, Some(Duration::from_millis(45)));
        // A part comes before the OK (the OK is lost): the next repeat
        // names no part.
        let published = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let part = crate::client_compact::encode_part(500, published, b"pcm").unwrap();
        let at = answered + Duration::from_millis(2);
        assert_eq!(
            session.on_datagram(edge(), &part, at, &mut |_, _| {}),
            Received::Media
        );
        let wake = only(&session).next_refresh;
        let mut repeated: Vec<Vec<u8>> = Vec::new();
        session.poll(wake, &mut |_, datagram| repeated.push(datagram.to_vec()));
        let sub = repeated
            .iter()
            .find_map(|datagram| match Control::decode(datagram) {
                Some(Control::Subscribe {
                    from_part, token, ..
                }) => Some((from_part, token)),
                _ => None,
            })
            .expect("a repeated SUB");
        assert_eq!(sub, (None, Some(9)));
    }

    #[test]
    fn the_handshake_subscribes_with_the_token_and_delivers_parts() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(4001), now);
        let mut sent: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
        let mut send = |to: SocketAddr, datagram: &[u8]| sent.push((to, datagram.to_vec()));
        session.begin(edge(), Some(10), now, &mut send);
        let token = Control::Token {
            stream_id: 4001,
            token: 77,
        }
        .encode();
        assert_eq!(
            session.on_datagram(edge(), &token, now, &mut send),
            Received::Control
        );
        let ok = Control::Subscribed {
            stream_id: 4001,
            next_part: 10,
            compact: true,
        }
        .encode();
        assert_eq!(
            session.on_datagram(edge(), &ok, now, &mut send),
            Received::Subscribed {
                next_part: 10,
                first: true
            }
        );
        assert!(session.is_subscribed());
        assert!(session.compact_confirmed());
        // The compact header carries the publication time relative to the
        // receiver's clock: a time of now.
        let published = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
            - 500;
        let part = crate::client_compact::encode_part(10, published, b"pcm").unwrap();
        assert_eq!(
            session.on_datagram(edge(), &part, now, &mut send),
            Received::Media
        );
        let object = session.next_object().expect("the part");
        assert_eq!(object.part, 10);
        assert_eq!(object.published_unix_us, published);
        assert_eq!(&object.payload[..], b"pcm");
        assert!(session.next_object().is_none());
        // A datagram from elsewhere is not the edge's.
        let other: SocketAddr = "127.0.0.2:19446".parse().unwrap();
        assert_eq!(
            session.on_datagram(other, &part, now, &mut send),
            Received::Ignored
        );
        drop(send);
        // SUB without a token, SUB with it.
        assert_eq!(sent.len(), 2);
        match Control::decode(&sent[1].1) {
            Some(Control::Subscribe {
                token, from_part, ..
            }) => {
                assert_eq!(token, Some(77));
                assert_eq!(from_part, Some(10));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_silent_edge_and_a_move_are_reported_and_the_next_edge_is_left_cleanly() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut sent: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
        let mut send = |to: SocketAddr, datagram: &[u8]| sent.push((to, datagram.to_vec()));
        session.begin(edge(), None, now, &mut send);
        // No answer within --answer-ms.
        assert!(session
            .poll(now + Duration::from_millis(100), &mut send)
            .is_none());
        let reason = session
            .poll(now + Duration::from_millis(5_001), &mut send)
            .expect("move");
        assert_eq!(reason, "no-answer-5000ms");
        // The next edge: answered, then MOVE.
        let next: SocketAddr = "127.0.0.3:19446".parse().unwrap();
        session.begin(next, Some(5), now, &mut send);
        let token = Control::Token {
            stream_id: 1,
            token: 9,
        }
        .encode();
        session.on_datagram(next, &token, now, &mut send);
        let moved = Control::Move {
            stream_id: 1,
            reason: "drain".into(),
        }
        .encode();
        assert_eq!(
            session.on_datagram(next, &moved, now, &mut send),
            Received::Move("drain".into())
        );
        session.begin(edge(), Some(5), now, &mut send);
        drop(send);
        // The edge left with its token got an UNSUB.
        assert!(sent.iter().any(|(to, datagram)| *to == next
            && matches!(
                Control::decode(datagram),
                Some(Control::Unsubscribe { token: 9, .. })
            )));
        assert_eq!(session.edges, 3);
        // Each edge is a leg of its own, with its own decoders: a move is
        // no new flow (B X4, X5).
        assert_eq!(session.receiver.flows, 1);
    }

    /// A session subscribed at `edge()` with a part received at `at`.
    fn subscribed(config: SessionConfig, at: Instant) -> UdpSession {
        let mut session = UdpSession::new(config, at);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.begin(edge(), None, at, &mut send);
        let token = Control::Token {
            stream_id: config.stream_id,
            token: 5,
        }
        .encode();
        session.on_datagram(edge(), &token, at, &mut send);
        let ok = Control::Subscribed {
            stream_id: config.stream_id,
            next_part: 10,
            compact: true,
        }
        .encode();
        session.on_datagram(edge(), &ok, at, &mut send);
        let part = crate::client_compact::encode_part(10, 0, b"pcm").unwrap();
        session.on_datagram(edge(), &part, at, &mut send);
        session
    }

    /// Wave 8 S2: a client moves after 200 ms of silence; an edge it moved
    /// to that is silent too (the stream itself stalls) moves it again only
    /// after the second's silence, until a datagram comes.
    #[test]
    fn a_client_moves_on_a_short_silence_and_then_on_the_long_one() {
        let now = Instant::now();
        let config = SessionConfig::new(1);
        assert_eq!(config.silence, Duration::from_millis(200));
        let mut session = subscribed(config, now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        assert!(session
            .poll(now + Duration::from_millis(150), &mut send)
            .is_none());
        let reason = session
            .poll(now + Duration::from_millis(201), &mut send)
            .expect("move");
        assert_eq!(reason, "silence-200ms");
        // The next edge answers but sends nothing.
        let moved_at = now + Duration::from_millis(201);
        let next: SocketAddr = "127.0.0.3:19446".parse().unwrap();
        session.begin(next, Some(11), moved_at, &mut send);
        let token = Control::Token {
            stream_id: 1,
            token: 6,
        }
        .encode();
        session.on_datagram(next, &token, moved_at, &mut send);
        let ok = Control::Subscribed {
            stream_id: 1,
            next_part: 11,
            compact: true,
        }
        .encode();
        session.on_datagram(next, &ok, moved_at, &mut send);
        assert!(session
            .poll(moved_at + Duration::from_millis(500), &mut send)
            .is_none());
        assert_eq!(
            session
                .poll(moved_at + Duration::from_millis(1_001), &mut send)
                .as_deref(),
            Some("silence-1000ms")
        );
        // A datagram from the edge in use brings the short silence back.
        let mut session = subscribed(config, now);
        session.poll(now + Duration::from_millis(201), &mut send);
        session.begin(next, Some(11), moved_at, &mut send);
        session.on_datagram(next, &token, moved_at, &mut send);
        session.on_datagram(next, &ok, moved_at, &mut send);
        let part = crate::client_compact::encode_part(11, 0, b"pcm").unwrap();
        session.on_datagram(next, &part, moved_at, &mut send);
        assert_eq!(
            session
                .poll(moved_at + Duration::from_millis(201), &mut send)
                .as_deref(),
            Some("silence-200ms")
        );
    }

    /// Wave 8: short-form parts that come before the OK (a subscribe spike
    /// delays the OK behind the fast lane's first parts) are kept and read
    /// once the OK names the first part, not dropped.
    #[test]
    fn parts_before_the_ok_are_kept_until_it_comes() {
        for drop_early_parts in [false, true] {
            let now = Instant::now();
            let config = SessionConfig {
                drop_early_parts,
                ..SessionConfig::new(3)
            };
            let mut session = UdpSession::new(config, now);
            let mut send = |_: SocketAddr, _: &[u8]| {};
            session.begin(edge(), None, now, &mut send);
            let token = Control::Token {
                stream_id: 3,
                token: 4,
            }
            .encode();
            session.on_datagram(edge(), &token, now, &mut send);
            // A known publication time: a part without one keeps the long
            // form (wave 9 review).
            let long =
                crate::client_compact::encode_part(70_000, 1_700_000_000_000_000, b"pcm").unwrap();
            let short = crate::client_compact::shorten(&long).expect("short form");
            assert_eq!(
                session.on_datagram(edge(), &short, now, &mut send),
                Received::Media
            );
            assert!(session.next_object().is_none());
            let ok = Control::Subscribed {
                stream_id: 3,
                next_part: 70_000,
                compact: true,
            }
            .encode();
            session.on_datagram(edge(), &ok, now, &mut send);
            let got = session.next_object().map(|object| object.part);
            if drop_early_parts {
                assert_eq!(got, None);
            } else {
                assert_eq!(got, Some(70_000));
            }
        }
    }

    /// Wave 8 local proofs: a listener of a killed edge moves with a hole
    /// the edge never repaired; the new edge is asked from that hole, not
    /// from the part after the newest (with the flag off, as before).
    #[test]
    fn a_move_asks_the_new_edge_for_the_hole_the_old_edge_left() {
        for move_from_hole in [true, false] {
            let now = Instant::now();
            let config = SessionConfig {
                move_from_hole,
                ..SessionConfig::new(3)
            };
            let mut session = UdpSession::new(config, now);
            let mut send = |_: SocketAddr, _: &[u8]| {};
            session.begin(edge(), None, now, &mut send);
            let token = Control::Token {
                stream_id: 3,
                token: 4,
            }
            .encode();
            session.on_datagram(edge(), &token, now, &mut send);
            let ok = Control::Subscribed {
                stream_id: 3,
                next_part: 100,
                compact: true,
            }
            .encode();
            session.on_datagram(edge(), &ok, now, &mut send);
            for part in [100_u64, 101, 103, 104] {
                let long = crate::client_compact::encode_part(part, 0, b"pcm").unwrap();
                session.on_datagram(edge(), &long, now, &mut send);
            }
            while session.next_object().is_some() {}
            assert_eq!(session.receiver.compact.lowest_hole(), Some(102));
            let other: SocketAddr = "127.0.0.2:9000".parse().unwrap();
            session.begin(other, Some(105), now, &mut send);
            let expected = if move_from_hole { Some(102) } else { Some(105) };
            assert_eq!(
                only(&session).from_part,
                expected,
                "move_from_hole {move_from_hole}"
            );
            // A first subscription is not a move: the part asked is kept.
            let mut fresh = UdpSession::new(SessionConfig::new(3), now);
            fresh.begin(edge(), Some(105), now, &mut send);
            assert_eq!(only(&fresh).from_part, Some(105));
        }
    }

    /// Wave 8 review: several parts before the OK come out in the order
    /// they came, at the OK (not after the next media datagram).
    #[test]
    fn parts_before_the_ok_come_out_in_order_at_the_ok() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(3), now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.begin(edge(), None, now, &mut send);
        let token = Control::Token {
            stream_id: 3,
            token: 4,
        }
        .encode();
        session.on_datagram(edge(), &token, now, &mut send);
        for part in 70_000..70_004_u64 {
            let long =
                crate::client_compact::encode_part(part, 1_700_000_000_000_000, b"pcm").unwrap();
            let short = crate::client_compact::shorten(&long).expect("short form");
            session.on_datagram(edge(), &short, now, &mut send);
        }
        assert!(!session.has_objects());
        let ok = Control::Subscribed {
            stream_id: 3,
            next_part: 70_000,
            compact: true,
        }
        .encode();
        assert!(matches!(
            session.on_datagram(edge(), &ok, now, &mut send),
            Received::Subscribed { first: true, .. }
        ));
        assert!(session.has_objects());
        let mut parts = Vec::new();
        while let Some(object) = session.next_object() {
            parts.push(object.part);
        }
        assert_eq!(parts, vec![70_000, 70_001, 70_002, 70_003]);
        // The next part follows them.
        let next = crate::client_compact::encode_part(70_004, 0, b"pcm").unwrap();
        session.on_datagram(edge(), &next, now, &mut send);
        assert_eq!(
            session.next_object().map(|object| object.part),
            Some(70_004)
        );
    }

    /// Wave 8 R6-2: no NACK while the caller says datagrams are queued.
    #[test]
    fn nacks_wait_while_datagrams_are_queued() {
        let now = Instant::now();
        let mut session = subscribed(SessionConfig::new(1), now);
        let mut sent = 0;
        let mut send = |_: SocketAddr, datagram: &[u8]| {
            if matches!(Control::decode(datagram), Some(Control::Nack { .. })) {
                sent += 1;
            }
        };
        // Part 12 shows 11 lost.
        let part = crate::client_compact::encode_part(12, 0, b"pcm").unwrap();
        session.on_datagram(edge(), &part, now, &mut send);
        let at = now + Duration::from_millis(1);
        session.queued = true;
        session.poll(at, &mut send);
        session.queued = false;
        session.poll(at, &mut send);
        drop(send);
        assert_eq!(sent, 1);
    }

    /// Wave 8 review: a socket that never empties (a 4K stream) holds the
    /// NACKs back for one NACK retry at most, not until the holes are
    /// given up.
    #[test]
    fn a_socket_that_stays_busy_holds_nacks_back_for_a_retry_at_most() {
        let now = Instant::now();
        let mut session = subscribed(SessionConfig::new(1), now);
        let sent = std::cell::Cell::new(0);
        let mut send = |_: SocketAddr, datagram: &[u8]| {
            if matches!(Control::decode(datagram), Some(Control::Nack { .. })) {
                sent.set(sent.get() + 1);
            }
        };
        let part = crate::client_compact::encode_part(12, 0, b"pcm").unwrap();
        session.on_datagram(edge(), &part, now, &mut send);
        let mut at = now;
        let mut first = None;
        while at < now + Duration::from_millis(20) {
            at += Duration::from_micros(100);
            session.queued = true;
            session.poll(at, &mut send);
            if first.is_none() && sent.get() > 0 {
                first = Some(at);
            }
        }
        let first = first.expect("a NACK while the socket stayed busy");
        assert!(
            first <= now + Duration::from_micros(2_400),
            "first NACK {:?} after the hole",
            first - now
        );
        // A queue that empties now and then: the wait starts again.
        let mut session = subscribed(SessionConfig::new(1), now);
        let mut nacks = 0;
        let mut send = |_: SocketAddr, datagram: &[u8]| {
            if matches!(Control::decode(datagram), Some(Control::Nack { .. })) {
                nacks += 1;
            }
        };
        session.on_datagram(edge(), &part, now, &mut send);
        session.queued = true;
        session.poll(now + Duration::from_millis(1), &mut send);
        session.queued = false;
        session.poll(now + Duration::from_millis(1), &mut send);
        drop(send);
        assert_eq!(nacks, 1);
    }

    /// Wave 8: a client that gave up the compact lane for want of an answer
    /// asks for it again once the edge answers (twice at most).
    #[test]
    fn the_compact_lane_is_asked_for_again_when_the_edge_answers() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let sent: std::cell::RefCell<Vec<Vec<u8>>> = Default::default();
        let mut send = |_: SocketAddr, datagram: &[u8]| sent.borrow_mut().push(datagram.to_vec());
        session.begin(edge(), None, now, &mut send);
        let mut at = now;
        while session.compact_asked() {
            at += UNANSWERED_REFRESH;
            session.poll(at, &mut send);
        }
        assert_eq!(session.compact_fallbacks, 1);
        // The edge answers the request without the lane.
        let token = Control::Token {
            stream_id: 1,
            token: 8,
        }
        .encode();
        session.on_datagram(edge(), &token, at, &mut send);
        let ok = Control::Subscribed {
            stream_id: 1,
            next_part: 10,
            compact: false,
        }
        .encode();
        session.on_datagram(edge(), &ok, at, &mut send);
        assert!(session.compact_asked());
        assert_eq!(session.compact_retries, 1);
        session.poll(at, &mut send);
        match Control::decode(sent.borrow().last().unwrap()) {
            Some(Control::Subscribe { compact, .. }) => assert!(compact.is_some()),
            other => panic!("{other:?}"),
        }
        // An edge that drops compact requests never answers the retry: the
        // client asks without the lane again within 1.5 s, before its
        // subscription could expire, and does not retry at this edge.
        let mut later = at;
        let wait = Control::Waiting {
            stream_id: 1,
            next_part: 10,
            head: None,
        }
        .encode();
        while session.compact_asked() {
            later += Duration::from_millis(100);
            session.on_datagram(edge(), &wait, later, &mut send);
            assert!(session.poll(later, &mut send).is_none());
            assert!(later < at + COMPACT_FALLBACK_AFTER + Duration::from_millis(200));
        }
        assert_eq!(session.compact_retries_unanswered, 1);
        match Control::decode(sent.borrow().last().unwrap()) {
            Some(Control::Subscribe { compact, .. }) => assert!(compact.is_none()),
            other => panic!("{other:?}"),
        }
        session.on_datagram(edge(), &ok, later, &mut send);
        assert!(!session.compact_asked());
        assert_eq!(session.compact_retries, 1);
        // Without the retry, the RaptorQ lane stays.
        let config = SessionConfig {
            compact_retry: false,
            ..SessionConfig::new(1)
        };
        let mut session = UdpSession::new(config, now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.begin(edge(), None, now, &mut send);
        let mut at = now;
        while session.compact_asked() {
            at += UNANSWERED_REFRESH;
            session.poll(at, &mut send);
        }
        session.on_datagram(edge(), &token, at, &mut send);
        session.on_datagram(edge(), &ok, at, &mut send);
        assert!(!session.compact_asked());
    }

    #[test]
    fn an_edge_that_ignores_the_compact_lane_is_asked_without_it() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut sent: Vec<Vec<u8>> = Vec::new();
        let mut send = |_: SocketAddr, datagram: &[u8]| sent.push(datagram.to_vec());
        session.begin(edge(), None, now, &mut send);
        let mut at = now;
        // Three requests unanswered is not enough before 1.5 s (wave 7
        // review: a lost ARP request holds a path's first datagram ~1 s).
        for _ in 0..3 {
            at += UNANSWERED_REFRESH;
            assert!(session.poll(at, &mut send).is_none());
        }
        assert!(session.compact_asked());
        while at < now + COMPACT_FALLBACK_AFTER {
            at += UNANSWERED_REFRESH;
            assert!(session.poll(at, &mut send).is_none());
        }
        assert!(!session.compact_asked());
        assert_eq!(session.compact_fallbacks, 1);
        drop(send);
        match Control::decode(sent.last().unwrap()) {
            Some(Control::Subscribe { compact, .. }) => assert!(compact.is_none()),
            other => panic!("{other:?}"),
        }
    }

    /// Wave 7 review: the compact lane is given up only after three
    /// unanswered requests and 1.5 s, and an edge that refuses the short
    /// form is asked again with the long form (once: the plan then has none).
    #[test]
    fn the_compact_lane_is_given_up_late_and_the_short_form_first() {
        let second = Duration::from_millis(600);
        assert!(!falls_back_from_compact(true, false, 3, second));
        assert!(!falls_back_from_compact(
            true,
            false,
            2,
            Duration::from_secs(2)
        ));
        assert!(falls_back_from_compact(
            true,
            false,
            3,
            COMPACT_FALLBACK_AFTER
        ));
        assert!(!falls_back_from_compact(
            true,
            true,
            9,
            Duration::from_secs(9)
        ));
        assert!(!falls_back_from_compact(
            false,
            false,
            9,
            Duration::from_secs(9)
        ));
        let short = CompactPlan::CLEAN.with_short_header(true);
        assert!(drops_short_header(false, true, short));
        assert!(!drops_short_header(true, true, short));
        assert!(!drops_short_header(false, false, short));
        assert!(!drops_short_header(
            false,
            true,
            short.with_short_header(false)
        ));
    }

    /// An edge that answers a short-form request without the compact lane
    /// is asked again at once with the long form, and the session stays
    /// unsubscribed until that answer.
    #[test]
    fn a_refused_short_form_is_asked_again_in_the_long_form() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(5), now);
        assert!(session.plan().short_header);
        let mut sent: Vec<Vec<u8>> = Vec::new();
        let mut send = |_: SocketAddr, datagram: &[u8]| sent.push(datagram.to_vec());
        session.begin(edge(), Some(3), now, &mut send);
        let token = Control::Token {
            stream_id: 5,
            token: 1,
        }
        .encode();
        session.on_datagram(edge(), &token, now, &mut send);
        assert_eq!(session.first_token_at, Some(now));
        let refused = Control::Subscribed {
            stream_id: 5,
            next_part: 3,
            compact: false,
        }
        .encode();
        assert_eq!(
            session.on_datagram(edge(), &refused, now, &mut send),
            Received::Control
        );
        assert!(!session.is_subscribed());
        assert!(!session.short_header);
        assert_eq!(session.short_header_dropped, 1);
        assert!(session.next_wake() <= now);
        assert!(session.poll(now, &mut send).is_none());
        drop(send);
        match Control::decode(sent.last().unwrap()) {
            Some(Control::Subscribe { compact, .. }) => {
                let plan = compact.expect("still the compact lane");
                assert!(!plan.short_header);
            }
            other => panic!("{other:?}"),
        }
        // The long-form refusal is a real one: the RaptorQ lane.
        let mut send = |_: SocketAddr, _: &[u8]| {};
        assert_eq!(
            session.on_datagram(edge(), &refused, now, &mut send),
            Received::Subscribed {
                next_part: 3,
                first: true
            }
        );
        assert!(!session.compact_confirmed());
        assert_eq!(session.short_header_dropped, 1);
        assert_eq!(session.first_subscribed_at, Some(now));
    }

    /// The end-of-run drain: a held session reports no silence and does
    /// not wake for one, but still NACKs and refreshes.
    #[test]
    fn a_held_session_reports_no_silence() {
        let now = Instant::now();
        let mut config = SessionConfig::new(2);
        config.nack_timing = NackTiming::BEFORE;
        let mut session = UdpSession::new(config, now);
        assert_eq!(session.receiver.compact.nack_timing(), NackTiming::BEFORE);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.begin(edge(), None, now, &mut send);
        let ok = Control::Subscribed {
            stream_id: 2,
            next_part: 0,
            compact: true,
        }
        .encode();
        session.on_datagram(edge(), &ok, now, &mut send);
        let late = now + Duration::from_secs(2);
        session.hold = true;
        assert!(session.poll(late, &mut send).is_none());
        assert!(session.next_wake() > late);
        session.hold = false;
        assert_eq!(
            session.poll(late, &mut send),
            Some(format!("silence-{DEFAULT_SILENCE_MS}ms"))
        );
    }

    #[test]
    fn a_raptorq_part_decodes_through_the_session() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(7), now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.begin(edge(), None, now, &mut send);
        let object = PartObject {
            kind: ObjectKind::Part,
            datagrams: 0,
            stream_id: 7,
            part: 3,
            available_unix_us: 5,
            published_unix_us: 4,
            generation: 0,
            payload: Bytes::from_static(&[9_u8; 2_000]),
        };
        let datagrams = encode_object(&object, 1_200, RepairPolicy::default()).unwrap();
        let mut got = Vec::new();
        for datagram in &datagrams {
            if session.on_datagram(edge(), datagram, now, &mut send) == Received::Media {
                while let Some(object) = session.next_object() {
                    got.push(object);
                }
            }
        }
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].part, 3);
        assert_eq!(got[0].payload.len(), 2_000);
    }

    /// The processor time of one compact part through the session (no
    /// socket): what a load host's listener costs above the kernel.
    /// `cargo test --release --lib decode_cost -- --ignored --nocapture`
    #[test]
    #[ignore = "a measurement"]
    fn decode_cost_of_a_compact_part() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.begin(edge(), None, now, &mut send);
        let token = Control::Token {
            stream_id: 1,
            token: 3,
        }
        .encode();
        session.on_datagram(edge(), &token, now, &mut send);
        let ok = Control::Subscribed {
            stream_id: 1,
            next_part: 0,
            compact: true,
        }
        .encode();
        session.on_datagram(edge(), &ok, now, &mut send);
        let payload = vec![7_u8; 304];
        let published = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let parts = 200_000_u64;
        let datagrams: Vec<Vec<u8>> = (0..parts)
            .map(|part| crate::client_compact::encode_part(part, published, &payload).unwrap())
            .collect();
        let started = Instant::now();
        let mut delivered = 0_u64;
        for (index, datagram) in datagrams.iter().enumerate() {
            let at = now + Duration::from_micros(index as u64 * 1_000);
            session.on_datagram(edge(), datagram, at, &mut send);
            while let Some(object) = session.next_object() {
                delivered += object.payload.len() as u64 / 304;
            }
            if index % 150 == 0 {
                session.poll(at, &mut send);
            }
        }
        let elapsed = started.elapsed();
        assert_eq!(delivered, parts);
        println!(
            "{:.3} us a compact part ({} parts in {:?})",
            elapsed.as_secs_f64() * 1e6 / parts as f64,
            parts,
            elapsed
        );
    }

    /// B X3: a new one-leg session plans for 2 % loss (`C1.16.2`, as the
    /// RaptorQ repair plans for an unmeasured path) until its second loss
    /// sample, and then follows the measure.
    #[test]
    fn a_new_one_leg_session_plans_for_two_percent() {
        let now = Instant::now();
        let mut session = subscribed(SessionConfig::new(1), now);
        assert_eq!(session.plan().word(), "C1.16.2.7");
        let leg = only_mut(&mut session);
        leg.compact_estimate = Some(0.0);
        leg.loss_samples = 1;
        session.plan_legs(now);
        assert_eq!(session.plan().word(), "C1.16.2.7");
        only_mut(&mut session).loss_samples = 2;
        session.plan_legs(now);
        // Less: after the hold.
        assert_eq!(session.plan().word(), "C1.16.2.7");
        session.plan_legs(now + PLAN_HOLD);
        assert_eq!(session.plan().word(), "C0.32.1.7");
        assert_eq!(session.plan_changes, 1);
        // The long form when the short form is off.
        let config = SessionConfig {
            compact_short_header: false,
            ..SessionConfig::new(1)
        };
        assert_eq!(UdpSession::new(config, now).plan().word(), "C1.16.2");
    }

    /// B X3 as the lead changed it after integration 2: a plan that asks for
    /// more goes at once; one that asks for less [`PLAN_HOLD`] after the
    /// leg's last plan change, however briefly the estimate has wanted
    /// less; a path counts as bursty from a mean burst of 2.0 until it falls
    /// below 1.5.
    #[test]
    fn a_plan_that_asks_for_less_waits_for_the_hold_after_the_last_change() {
        let now = Instant::now();
        let mut session = subscribed(SessionConfig::new(1), now);
        only_mut(&mut session).loss_samples = 2;
        let hold = PLAN_HOLD.as_millis() as u64;
        assert!(hold > 200);
        let ms = |ms: u64| now + Duration::from_millis(ms);
        let plan = |session: &mut UdpSession, loss: f64, burst: Option<f64>, at: Instant| {
            let leg = only_mut(session);
            leg.compact_estimate = Some(loss);
            leg.burst_estimate = burst;
            session.plan_legs(at);
            session.plan().word()
        };
        assert_eq!(plan(&mut session, 0.05, None, ms(0)), "C1.16.2.7");
        assert_eq!(
            plan(&mut session, 0.10, None, ms(150)),
            "C2.16.2.7",
            "more goes at once"
        );
        assert_eq!(only(&session).next_refresh, ms(150));
        assert_eq!(plan(&mut session, 0.05, None, ms(160)), "C2.16.2.7");
        // The estimate wants the plan again: the wait does not start over,
        // it runs from the change.
        plan(&mut session, 0.10, None, ms(100 + hold));
        assert_eq!(plan(&mut session, 0.05, None, ms(140 + hold)), "C2.16.2.7");
        assert_eq!(
            plan(&mut session, 0.05, None, ms(150 + hold)),
            "C1.16.2.7",
            "less the hold after the change, though wanted only since just before"
        );
        assert_eq!(session.plan_changes, 2);
        // More right after a change still goes at once; less again the
        // hold after that.
        assert_eq!(plan(&mut session, 0.10, None, ms(200 + hold)), "C2.16.2.7");
        assert_eq!(plan(&mut session, 0.05, None, ms(190 + 2 * hold)), "C2.16.2.7");
        assert_eq!(plan(&mut session, 0.05, None, ms(200 + 2 * hold)), "C1.16.2.7");
        assert_eq!(session.plan_changes, 4);
        // The burst band: bursty at 2.0 (rows instead of a copy, which asks
        // for less: after the hold), still at 1.7, not at 1.4 (a copy
        // again, more: at once).
        assert_eq!(plan(&mut session, 0.05, Some(2.0), ms(300 + 2 * hold)), "C1.16.2.7");
        assert_eq!(plan(&mut session, 0.05, Some(2.0), ms(200 + 3 * hold)), "C0.16.4.7");
        assert_eq!(plan(&mut session, 0.05, Some(1.7), ms(400 + 3 * hold)), "C0.16.4.7");
        assert_eq!(plan(&mut session, 0.05, Some(1.4), ms(500 + 3 * hold)), "C1.16.2.7");
    }

    fn edge_b() -> SocketAddr {
        "127.0.0.2:19446".parse().unwrap()
    }

    fn edge_c() -> SocketAddr {
        "127.0.0.4:19446".parse().unwrap()
    }

    fn part(part: u64) -> Vec<u8> {
        crate::client_compact::encode_part(part, 0, b"pcm").unwrap()
    }

    fn token(token: u64) -> Vec<u8> {
        Control::Token {
            stream_id: 1,
            token,
        }
        .encode()
    }

    fn ok(next_part: u64) -> Vec<u8> {
        Control::Subscribed {
            stream_id: 1,
            next_part,
            compact: true,
        }
        .encode()
    }

    type Sent = Vec<(SocketAddr, Control)>;

    fn sender(sent: &mut Sent) -> impl FnMut(SocketAddr, &[u8]) + '_ {
        move |to, datagram| sent.push((to, Control::decode(datagram).expect("a control")))
    }

    const A: LegKey = LegKey(1);
    const B: LegKey = LegKey(2);
    const C: LegKey = LegKey(3);

    /// How the director starts a leg beside a delivering one, without a
    /// token.
    fn start() -> LegStart {
        LegStart {
            from: StartFrom::AfterNewest,
            token: None,
            token_wait: Duration::from_millis(DEFAULT_TOKEN_MS),
        }
    }

    fn events(session: &mut UdpSession) -> Vec<LegEvent> {
        std::iter::from_fn(|| session.next_event()).collect()
    }

    fn held(session: &UdpSession, key: LegKey) -> &Leg {
        session.by_key(key).expect("the leg is held")
    }

    fn held_mut(session: &mut UdpSession, key: LegKey) -> &mut Leg {
        let index = session.slot(key).expect("the leg is held");
        session.legs[index].as_mut().unwrap()
    }

    fn view(session: &UdpSession, key: LegKey, now: Instant) -> LegView {
        session.leg(key, now).expect("the leg is held")
    }

    /// A session whose legs `A` (node "a" at `edge()`, token 5) and `B`
    /// (node "b" at `edge_b()`, token 6) are answered from part `first`; no
    /// media yet.
    fn two_legs(config: SessionConfig, first: u64, now: Instant) -> UdpSession {
        let mut session = UdpSession::new(config, now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.add_leg(A, "a", edge(), start(), now, &mut send);
        session.add_leg(B, "b", edge_b(), start(), now, &mut send);
        for (target, given) in [(edge(), 5), (edge_b(), 6)] {
            session.on_datagram(target, &token(given), now, &mut send);
            session.on_datagram(target, &ok(first), now, &mut send);
        }
        events(&mut session);
        session
    }

    /// B X5: two legs are asked and subscribed each at its own edge, each
    /// is live at its first media after its `OK`, each part comes out once
    /// whichever leg brings it first, and each leg's refresh names the
    /// other delivering leg in `o=`.
    #[test]
    fn two_legs_subscribe_and_each_part_comes_out_once() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut sent = Sent::new();
        session.add_leg(A, "a", edge(), start(), now, &mut sender(&mut sent));
        session.add_leg(B, "b", edge_b(), start(), now, &mut sender(&mut sent));
        assert!(matches!(
            &sent[..],
            [
                (first, Control::Subscribe { token: None, .. }),
                (second, Control::Subscribe { token: None, .. })
            ] if *first == edge() && *second == edge_b()
        ));
        sent.clear();
        for (target, given) in [(edge(), 5), (edge_b(), 6)] {
            session.on_datagram(target, &token(given), now, &mut sender(&mut sent));
            assert_eq!(
                session.on_datagram(target, &ok(100), now, &mut sender(&mut sent)),
                Received::Subscribed {
                    next_part: 100,
                    first: true
                }
            );
        }
        assert!(sent.iter().any(|(to, control)| *to == edge()
            && matches!(control, Control::Subscribe { token: Some(5), .. })));
        assert!(sent.iter().any(|(to, control)| *to == edge_b()
            && matches!(control, Control::Subscribe { token: Some(6), .. })));
        assert_eq!(
            events(&mut session),
            vec![LegEvent::Tokened(A), LegEvent::Tokened(B)]
        );
        let mut delivered = Vec::new();
        let ms = Duration::from_millis(1);
        for number in 100..110_u64 {
            let at = now + ms * (number - 99) as u32;
            // a first on even parts, b first on odd ones.
            let order = if number % 2 == 0 {
                [edge(), edge_b()]
            } else {
                [edge_b(), edge()]
            };
            for target in order {
                session.on_datagram(target, &part(number), at, &mut sender(&mut sent));
                while let Some(object) = session.next_object() {
                    delivered.push(object.part);
                }
            }
        }
        assert_eq!(
            events(&mut session),
            vec![
                LegEvent::Live {
                    leg: A,
                    compact: true
                },
                LegEvent::Live {
                    leg: B,
                    compact: true
                }
            ]
        );
        assert_eq!(delivered, (100..110).collect::<Vec<_>>());
        let report = session.leg_report(A).unwrap();
        assert_eq!(report.stats.parts_first, 5);
        assert_eq!(session.leg_report(B).unwrap().stats.parts_first, 5);
        assert_eq!(report.datagrams, 10);
        assert_eq!(report.first_part, Some(100));
        let at = now + ms * 11;
        let a = view(&session, A, at);
        assert!(a.delivering && view(&session, B, at).delivering);
        assert_eq!(a.first_part, Some(100));
        assert_eq!(a.last_media, Some(now + ms * 10));
        // Not delivering once no media came for 20 ms.
        assert!(!view(&session, A, at + ms * 30).delivering);
        // Each leg's next SUB names the other in o=, at once.
        sent.clear();
        session.poll_legs(at, &mut sender(&mut sent));
        let others = |target: SocketAddr| {
            sent.iter().find_map(|(to, control)| match control {
                Control::Subscribe { others, token, .. } if *to == target && token.is_some() => {
                    Some(others.clone())
                }
                _ => None,
            })
        };
        assert_eq!(others(edge()), Some(vec!["b".to_owned()]));
        assert_eq!(others(edge_b()), Some(vec!["a".to_owned()]));
        // And not again until something changes.
        sent.clear();
        session.poll_legs(at + ms, &mut sender(&mut sent));
        assert!(sent.is_empty(), "{sent:?}");
    }

    /// B X5 review: a leg is live (and counted in the plans) once its
    /// edge's `OK` and its first media have come, in either order.
    #[test]
    fn a_leg_is_live_at_its_ok_and_its_first_media() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.add_leg(A, "a", edge(), start(), now, &mut send);
        session.on_datagram(edge(), &token(5), now, &mut send);
        session.on_datagram(edge(), &ok(100), now, &mut send);
        assert_eq!(events(&mut session), vec![LegEvent::Tokened(A)]);
        assert_eq!(session.repair_leg(), None, "an OK alone is not live");
        session.on_datagram(edge(), &part(100), now, &mut send);
        assert_eq!(
            events(&mut session),
            vec![LegEvent::Live {
                leg: A,
                compact: true
            }]
        );
        assert_eq!(session.repair_leg(), Some(held(&session, A).id));
        // A part before the OK (the OK came late): live at the OK.
        session.add_leg(B, "b", edge_b(), start(), now, &mut send);
        session.on_datagram(edge_b(), &token(6), now, &mut send);
        session.on_datagram(edge_b(), &part(101), now, &mut send);
        assert_eq!(events(&mut session), vec![LegEvent::Tokened(B)]);
        session.on_datagram(edge_b(), &ok(101), now, &mut send);
        assert_eq!(
            events(&mut session),
            vec![LegEvent::Live {
                leg: B,
                compact: true
            }]
        );
        // Once only.
        session.on_datagram(edge_b(), &part(102), now, &mut send);
        assert_eq!(events(&mut session), vec![]);
    }

    /// B X5: a datagram goes to the leg whose edge is exactly its source,
    /// else to the only leg at that address; one from an edge let go in the
    /// last 10 s is answered with `UNSUB`, at most every 100 ms.
    #[test]
    fn a_datagram_is_attributed_by_address_and_port() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        let first: SocketAddr = "127.0.0.1:19446".parse().unwrap();
        let second: SocketAddr = "127.0.0.1:19447".parse().unwrap();
        session.add_leg(A, "a", first, start(), now, &mut send);
        session.add_leg(B, "b", second, start(), now, &mut send);
        session.on_datagram(second, &token(6), now, &mut send);
        assert_eq!(held(&session, B).token, Some(6));
        assert_eq!(held(&session, A).token, None);
        // Another port of that host: neither leg's.
        let stranger: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        assert_eq!(
            session.on_datagram(stranger, &token(9), now, &mut send),
            Received::Ignored
        );
        // The only leg at an address takes its datagrams from any port.
        session.add_leg(
            C,
            "c",
            "127.0.0.3:19446".parse().unwrap(),
            start(),
            now,
            &mut send,
        );
        session.on_datagram("127.0.0.3:5000".parse().unwrap(), &token(7), now, &mut send);
        assert_eq!(held(&session, C).token, Some(7));
        // b is let go: UNSUB twice, 20 ms apart; its edge's datagrams are
        // answered with UNSUB, at most every 100 ms, for 10 s.
        let mut sent = Sent::new();
        let unsubs = |sent: &Sent| {
            sent.iter()
                .filter(|(to, control)| {
                    *to == second && matches!(control, Control::Unsubscribe { token: 6, .. })
                })
                .count()
        };
        session.release_leg(B, now, &mut sender(&mut sent));
        assert_eq!(unsubs(&sent), 1);
        session.poll_legs(now + Duration::from_millis(19), &mut sender(&mut sent));
        assert_eq!(unsubs(&sent), 1);
        session.poll_legs(now + Duration::from_millis(20), &mut sender(&mut sent));
        assert_eq!(unsubs(&sent), 2);
        assert!(sent
            .iter()
            .all(|(to, _)| *to != second || unsubs(&sent) == 2));
        sent.clear();
        let later = now + Duration::from_millis(30);
        for (offset, answered) in [(0, 1), (50, 1), (100, 2)] {
            let at = later + Duration::from_millis(offset);
            assert_eq!(
                session.on_datagram(second, &part(100), at, &mut sender(&mut sent)),
                Received::Ignored
            );
            assert_eq!(sent.len(), answered, "at {offset} ms");
        }
        sent.clear();
        session.on_datagram(
            second,
            &part(100),
            now + Duration::from_secs(11),
            &mut sender(&mut sent),
        );
        assert!(sent.is_empty());
        assert!(session.leg(B, now).is_none());
    }

    /// B X5 review: a leg added at an edge released less than 10 s ago (a
    /// hedge back at the spare) ends that edge's `UNSUB`s: the second
    /// `UNSUB` of the release does not go, and its datagrams are the new
    /// leg's.
    #[test]
    fn a_leg_added_where_one_was_released_takes_its_datagrams() {
        let now = Instant::now();
        let mut session = two_legs(SessionConfig::new(1), 100, now);
        let mut sent = Sent::new();
        session.release_leg(A, now, &mut sender(&mut sent));
        assert_eq!(sent.len(), 1, "the first UNSUB");
        let hedge = LegStart {
            token: Some(5),
            ..start()
        };
        let ms = Duration::from_millis(1);
        session.add_leg(C, "a", edge(), hedge, now + ms * 5, &mut sender(&mut sent));
        sent.clear();
        session.poll_legs(now + ms * 25, &mut sender(&mut sent));
        assert!(
            !sent
                .iter()
                .any(|(_, control)| matches!(control, Control::Unsubscribe { .. })),
            "{sent:?}"
        );
        assert_eq!(
            session.on_datagram(edge(), &ok(101), now + ms * 26, &mut sender(&mut sent)),
            Received::Subscribed {
                next_part: 101,
                first: true
            }
        );
        assert_eq!(
            session.on_datagram(edge(), &part(101), now + ms * 26, &mut sender(&mut sent)),
            Received::Media
        );
        assert_eq!(session.leg_report(C).unwrap().datagrams, 1);
        assert!(sent
            .iter()
            .all(|(_, control)| !matches!(control, Control::Unsubscribe { .. })));
    }

    /// B X5: a leg with no media beside a leg that delivers is suspect at
    /// max(20 ms, 8 parts), due a hedge at 50 ms, and dead at 200 ms; the
    /// other leg is never judged. The dead leg delivers nothing more and
    /// stays until the director releases it.
    #[test]
    fn a_leg_without_media_beside_a_delivering_leg_is_hedged_then_dead() {
        let now = Instant::now();
        let mut session = two_legs(SessionConfig::new(1), 100, now);
        let ms = Duration::from_millis(1);
        let mut sent = Sent::new();
        for number in 100..120_u64 {
            let at = now + ms * (number - 100) as u32;
            session.on_datagram(edge(), &part(number), at, &mut sender(&mut sent));
            session.on_datagram(edge_b(), &part(number), at, &mut sender(&mut sent));
        }
        events(&mut session);
        // b stops at part 119 (t = 19 ms); a goes on, a part a ms.
        let stopped = now + ms * 19;
        let mut seen: Vec<(u32, LegEvent)> = Vec::new();
        let mut suspect_at = None;
        for step in 1..=200_u32 {
            let at = stopped + ms * step;
            session.on_datagram(
                edge(),
                &part(119 + u64::from(step)),
                at,
                &mut sender(&mut sent),
            );
            session.poll_legs(at, &mut sender(&mut sent));
            if suspect_at.is_none() && held(&session, B).suspect {
                suspect_at = Some(step);
            }
            seen.extend(events(&mut session).into_iter().map(|event| (step, event)));
            if seen
                .iter()
                .any(|(_, event)| matches!(event, LegEvent::Failed { .. }))
            {
                break;
            }
        }
        assert_eq!(suspect_at, Some(20));
        assert_eq!(
            seen,
            vec![
                (50, LegEvent::HedgeDue(B)),
                (
                    200,
                    LegEvent::Failed {
                        leg: B,
                        cause: LegFailure::Dead
                    }
                ),
            ]
        );
        assert_eq!(held(&session, B).failure.as_deref(), Some("silence-200ms"));
        // Kept until released; its datagrams are not taken.
        while session.next_object().is_some() {}
        let at = stopped + ms * 201;
        assert!(session.leg(B, at).is_some_and(|view| !view.delivering));
        assert_eq!(
            session.on_datagram(edge_b(), &part(500), at, &mut sender(&mut sent)),
            Received::Ignored
        );
        assert!(session.next_object().is_none());
        assert!(!sent
            .iter()
            .any(|(_, control)| matches!(control, Control::Unsubscribe { .. })));
        session.release_leg(B, at, &mut sender(&mut sent));
        assert!(sent.iter().any(|(to, control)| *to == edge_b()
            && matches!(control, Control::Unsubscribe { token: 6, .. })));
        assert!(session.leg(B, at).is_none());
        assert!(view(&session, A, at).delivering);
        assert_eq!(held(&session, A).state, State::Live);
    }

    /// Fix 4, F2 in process: a listener subscribes just as a source
    /// session starts, on one leg and on two. The new session's first parts
    /// come before, between and after each leg's `OK`, in the short form on
    /// the fast lane (kept until the `OK`) or the long form on the cache-fed
    /// lane, and one edge's `OK` part carries the previous session's
    /// generation (its cache had not recorded the new epoch yet, as in
    /// lp2c10-b-kill and -h-null). Every order of those datagrams is run.
    /// In each, the session ends on the new session on every leg: the
    /// stream's later parts all come out, no leg refuses its datagrams, and
    /// the datagrams refused on the way are bounded.
    #[test]
    fn a_join_at_a_session_start_converges_whatever_the_order() {
        use crate::client_compact::{
            encode_part, generation_of_epoch, set_fast_lane, set_generation, shorten,
            GENERATION_CHALLENGE,
        };
        #[derive(Debug, Clone, Copy)]
        enum Step {
            Ok(usize),
            New(usize, u64),
            Stale(usize, u64),
        }
        let old = generation_of_epoch(1_795_521_024_001_000);
        let new = generation_of_epoch(1_795_521_472_001_000);
        let datagram = |part: u64, generation: u8, fast: bool| {
            let mut long = encode_part(part, 1_795_521_472_001_000 + part * 1_000, b"pcm").unwrap();
            set_generation(&mut long, generation);
            if fast {
                set_fast_lane(&mut long);
                shorten(&long).unwrap_or(long)
            } else {
                long
            }
        };
        /// Every order of `steps` (Heap's algorithm).
        fn orders(steps: &mut Vec<Step>, k: usize, out: &mut Vec<Vec<Step>>) {
            if k <= 1 {
                out.push(steps.clone());
                return;
            }
            for i in 0..k {
                orders(steps, k - 1, out);
                let j = if k % 2 == 0 { i } else { 0 };
                steps.swap(j, k - 1);
            }
        }
        use std::collections::BTreeSet;
        const TAIL: u64 = 64;
        let mut runs = 0;
        for legs in [1_usize, 2] {
            for stale_leg in 0..legs {
                for fast in [true, false] {
                    let mut steps = vec![Step::New(0, 100), Step::Stale(stale_leg, 99), Step::New(0, 101)];
                    for leg in 0..legs {
                        steps.push(Step::Ok(leg));
                    }
                    if legs == 2 {
                        steps.push(Step::New(1, 100));
                        steps.push(Step::New(1, 101));
                    }
                    let mut all = Vec::new();
                    let k = steps.len();
                    orders(&mut steps, k, &mut all);
                    for order in all {
                        runs += 1;
                        let now = Instant::now();
                        let mut session = UdpSession::new(SessionConfig::new(1), now);
                        let mut send = |_: SocketAddr, _: &[u8]| {};
                        let targets = [edge(), edge_b()];
                        for (leg, key) in [A, B].into_iter().enumerate().take(legs) {
                            session.add_leg(key, ["a", "b"][leg], targets[leg], start(), now, &mut send);
                            session.on_datagram(targets[leg], &token(5 + leg as u64), now, &mut send);
                        }
                        let mut delivered: BTreeSet<u64> = BTreeSet::new();
                        let mut take = |session: &mut UdpSession| {
                            while let Some(object) = session.next_object() {
                                delivered.insert(object.part);
                            }
                        };
                        let step_at = Duration::from_micros(100);
                        for (index, step) in order.iter().enumerate() {
                            let at = now + step_at * index as u32;
                            match *step {
                                Step::Ok(leg) => {
                                    let named = if leg == stale_leg { 99 } else { 100 };
                                    session.on_datagram(targets[leg], &ok(named), at, &mut send);
                                }
                                Step::New(leg, part) => {
                                    session.on_datagram(targets[leg], &datagram(part, new, fast), at, &mut send);
                                }
                                Step::Stale(leg, part) => {
                                    session.on_datagram(targets[leg], &datagram(part, old, false), at, &mut send);
                                }
                            }
                            take(&mut session);
                        }
                        // Then the stream, a part a millisecond on every leg.
                        for (index, part) in (102..102 + TAIL).enumerate() {
                            let at = now + Duration::from_millis(1 + index as u64);
                            for target in targets.iter().take(legs) {
                                session.on_datagram(*target, &datagram(part, new, fast), at, &mut send);
                                take(&mut session);
                            }
                            session.poll_legs(at, &mut send);
                        }
                        let tail_from = 102 + TAIL - 40;
                        let missing: Vec<u64> =
                            (tail_from..102 + TAIL).filter(|part| !delivered.contains(part)).collect();
                        assert!(missing.is_empty(), "{legs} legs, stale on {stale_leg}, fast {fast}, {order:?}: missing {missing:?}");
                        for key in [A, B].into_iter().take(legs) {
                            let id = held(&session, key).id;
                            assert_eq!(
                                session.receiver.compact.leg_refused(id).0,
                                0,
                                "{legs} legs, stale on {stale_leg}, fast {fast}, {order:?}: {key:?} refuses"
                            );
                        }
                        let stats = session.receiver.compact.stats;
                        assert!(
                            stats.stale_generation <= 2 * u64::from(GENERATION_CHALLENGE),
                            "{order:?}: {stats:?}"
                        );
                        assert_eq!(session.refusing_legs, 0);
                    }
                }
            }
        }
        assert_eq!(runs, 2 * 24 + 4 * 5_040);
    }

    /// Fix 4, F2: a leg whose datagrams the receiver refuses for a second
    /// at the stream's rate fails (`refused-1s`), counted, and the
    /// generations refused are forgotten; the leg beside it delivers on.
    /// Here edge B stamps another generation than A for good (on its
    /// cache-fed lane): the challenger never wins while A's datagrams come
    /// between B's.
    #[test]
    fn a_leg_that_refuses_everything_for_a_second_fails() {
        use crate::client_compact::{encode_part, set_fast_lane, set_generation};
        let now = Instant::now();
        let mut session = two_legs(SessionConfig::new(1), 100, now);
        let stamped = |part: u64, generation: u8, fast: bool| {
            let mut datagram = encode_part(part, 0, b"pcm").unwrap();
            set_generation(&mut datagram, generation);
            if fast {
                set_fast_lane(&mut datagram);
            }
            datagram
        };
        let mut send = |_: SocketAddr, _: &[u8]| {};
        let ms = Duration::from_millis(1);
        let mut failed_at = None;
        for step in 0..1_200_u32 {
            let at = now + ms * step;
            let part = 100 + u64::from(step);
            session.on_datagram(edge(), &stamped(part, 9, true), at, &mut send);
            session.on_datagram(edge_b(), &stamped(part, 7, false), at, &mut send);
            while session.next_object().is_some() {}
            session.poll_legs(at, &mut send);
            let failed = events(&mut session).into_iter().find(|event| {
                matches!(event, LegEvent::Failed { leg: B, cause: LegFailure::Refusing })
            });
            if failed.is_some() {
                failed_at = Some(step);
                break;
            }
            assert!(held(&session, A).failure.is_none());
        }
        // A second after B's OK (its datagrams never taken), or about.
        let failed_at = failed_at.unwrap_or_else(|| {
            panic!(
                "B failed for refusing: {:?} {:?} {:?}",
                session.receiver.compact.leg_refused(held(&session, B).id),
                session.receiver.compact.part_interval(),
                held(&session, B).state
            )
        });
        assert!((1_000..1_010).contains(&failed_at), "{failed_at}");
        assert_eq!(session.refusing_legs, 1);
        assert_eq!(held(&session, B).failure.as_deref(), Some("refused-1s"));
        assert!(session.receiver.compact.leg_stats(held(&session, B).id).stale_generation >= 1_000);
        assert_eq!(session.receiver.compact.leg_refused(held(&session, A).id).0, 0);
    }

    /// A 4K part's RaptorQ datagrams as an edge's cache-fed lane sends
    /// them: `generation` stamped from its slot's epoch.
    fn object_datagrams(part: u64, generation: u8, len: usize) -> Vec<Vec<u8>> {
        let object = PartObject {
            kind: ObjectKind::Part,
            datagrams: 0,
            stream_id: 1,
            part,
            available_unix_us: 0,
            published_unix_us: 0,
            generation,
            payload: Bytes::from(vec![part as u8; len]),
        };
        encode_object(&object, 1_200, RepairPolicy::default()).unwrap()
    }

    fn wait(next_part: u64, head: Option<u64>) -> Vec<u8> {
        Control::Waiting {
            stream_id: 1,
            next_part,
            head,
        }
        .encode()
    }

    /// One 4K leg, live on a part.
    fn lone_4k_leg(now: Instant) -> UdpSession {
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.add_leg(A, "syd-edge-2", edge(), start(), now, &mut send);
        session.on_datagram(edge(), &token(5), now, &mut send);
        session.on_datagram(edge(), &ok(1_233), now, &mut send);
        let mut parts = Vec::new();
        for datagram in object_datagrams(1_233, 7, 12_000) {
            session.on_datagram(edge(), &datagram, now, &mut send);
            parts.extend(std::iter::from_fn(|| session.next_object()).map(|object| object.part));
        }
        assert_eq!(parts, vec![1_233]);
        events(&mut session);
        session
    }

    /// Stuck4k, syd-edge-2's sequence: a lone 4K leg had the previous
    /// session's last part (1233) and then only `WAIT 1234` keepalives,
    /// every 80 ms, for 40 s, which kept every silence rule from firing.
    /// Now the `WAIT`s carry the stream's head (`h=`): while it names one
    /// head (the source paused) nothing fails; once it moves and no media
    /// comes for a second, the leg fails `stalled-1s`, counted, and the
    /// session starts afresh (its replacement asks for the edge's head, not
    /// for the part after 1233).
    #[test]
    fn a_lone_leg_whose_edge_waits_while_the_stream_goes_on_fails() {
        let now = Instant::now();
        let mut session = lone_4k_leg(now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        let ms = Duration::from_millis(1);
        let mut failed_at = None;
        for step in 1..=40_u32 {
            let at = now + ms * (80 * step);
            // The stale head for 400 ms, then the new session's, moving.
            let head = if step <= 5 { 1_233 } else { 190 + u64::from(step) };
            session.on_datagram(edge(), &wait(1_234, Some(head)), at, &mut send);
            session.poll_legs(at, &mut send);
            if events(&mut session).contains(&LegEvent::Failed {
                leg: A,
                cause: LegFailure::Stalled,
            }) {
                failed_at = Some(80 * step);
                break;
            }
        }
        // The head moved at 480 ms: failed a second later.
        let failed_at = failed_at.expect("the leg fails");
        assert!((1_480..1_600).contains(&failed_at), "{failed_at}");
        assert_eq!(held(&session, A).failure.as_deref(), Some("stalled-1s"));
        assert_eq!(session.stalled_legs, 1);
        assert_eq!(session.refusing_legs, 0);
        assert_eq!(session.receiver.compact.newest(), None);
        assert_eq!(session.start_from(StartFrom::AfterNewest), None);
    }

    /// Stuck4k: `WAIT`s that name one head (a source that paused), or none
    /// (an edge older than `h=`), fail no leg however long they last.
    #[test]
    fn a_lone_leg_whose_edge_waits_on_a_quiet_stream_is_kept() {
        for head in [Some(1_233), None] {
            let now = Instant::now();
            let mut session = lone_4k_leg(now);
            let mut send = |_: SocketAddr, _: &[u8]| {};
            for step in 1..=60_u32 {
                let at = now + Duration::from_millis(80 * u64::from(step));
                session.on_datagram(edge(), &wait(1_234, head), at, &mut send);
                session.poll_legs(at, &mut send);
                let failed: Vec<LegEvent> = events(&mut session)
                    .into_iter()
                    .filter(|event| matches!(event, LegEvent::Failed { .. }))
                    .collect();
                assert!(failed.is_empty(), "{head:?} at step {step}: {failed:?}");
            }
            assert_eq!(session.stalled_legs, 0);
        }
    }

    /// Stuck4k: a 4K part's repair symbols that come after it decoded (60
    /// of them here) are no datagrams refused: a source that pauses after
    /// it fails no leg.
    #[test]
    fn the_repair_after_a_part_is_not_refused_when_the_source_pauses() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.add_leg(A, "syd-edge-2", edge(), start(), now, &mut send);
        session.on_datagram(edge(), &token(5), now, &mut send);
        session.on_datagram(edge(), &ok(197), now, &mut send);
        let object = PartObject {
            kind: ObjectKind::Part,
            datagrams: 0,
            stream_id: 1,
            part: 197,
            available_unix_us: 0,
            published_unix_us: 0,
            generation: 7,
            payload: Bytes::from(vec![7_u8; 60_000]),
        };
        let repair = RepairPolicy {
            permille: 1_200,
            min_symbols: 60,
            loss_ppm: 0,
        };
        let datagrams = encode_object(&object, 1_200, repair).unwrap();
        assert!(datagrams.len() >= 110, "{}", datagrams.len());
        let mut parts = 0;
        for datagram in &datagrams {
            session.on_datagram(edge(), datagram, now, &mut send);
            parts += std::iter::from_fn(|| session.next_object()).count();
        }
        assert_eq!(parts, 1);
        for step in 1..=40_u32 {
            let at = now + Duration::from_millis(80 * u64::from(step));
            session.on_datagram(edge(), &wait(198, Some(197)), at, &mut send);
            session.poll_legs(at, &mut send);
            assert!(
                !events(&mut session)
                    .iter()
                    .any(|event| matches!(event, LegEvent::Failed { .. })),
                "step {step}"
            );
        }
        assert_eq!(session.refusing_legs, 0);
    }

    /// Stuck4k, a client held at a rate above what its edge sends it: a 4K
    /// leg receives part of each part's datagrams, never enough to decode
    /// one. It received datagrams and took nothing: after a second it fails
    /// `refused-1s` (the RaptorQ lane's rule beside the compact lane's),
    /// counted, and the session starts afresh.
    #[test]
    fn a_raptorq_leg_that_takes_nothing_for_a_second_fails() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.add_leg(A, "syd-edge-2", edge(), start(), now, &mut send);
        session.on_datagram(edge(), &token(5), now, &mut send);
        session.on_datagram(edge(), &ok(197), now, &mut send);
        let ms = Duration::from_millis(1);
        let mut failed_at = None;
        for step in 0..40_u32 {
            let at = now + ms * (42 * step);
            // All but three of each part's sources and repair.
            let datagrams = object_datagrams(197 + u64::from(step), 7, 12_000);
            for datagram in datagrams.iter().take(datagrams.len().saturating_sub(3)) {
                session.on_datagram(edge(), datagram, at, &mut send);
            }
            assert!(session.next_object().is_none());
            session.poll_legs(at, &mut send);
            if events(&mut session).contains(&LegEvent::Failed {
                leg: A,
                cause: LegFailure::Refusing,
            }) {
                failed_at = Some(42 * step);
                break;
            }
        }
        let failed_at = failed_at.expect("the leg fails");
        assert!((1_000..1_100).contains(&failed_at), "{failed_at}");
        assert_eq!(held(&session, A).failure.as_deref(), Some("refused-1s"));
        assert_eq!(session.refusing_legs, 1);
        assert_eq!(session.stalled_legs, 0);
    }

    /// Stuck4k with two legs: one edge sends the new session, the other the
    /// previous one's parts (its cache's stale copies), both for good. The
    /// session holds the session established first; the other leg's objects
    /// are refused, counted by reason, and it fails `refused-1s`; the leg
    /// that delivers is untouched and the session is not restarted.
    #[test]
    fn a_leg_of_another_session_beside_a_delivering_leg_is_refused_and_fails() {
        let now = Instant::now();
        let mut session = two_legs(SessionConfig::new(1), 197, now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        let ms = Duration::from_millis(1);
        let mut failed_at = None;
        let mut delivered = Vec::new();
        for step in 0..40_u32 {
            let at = now + ms * (42 * step);
            for datagram in object_datagrams(197 + u64::from(step), 9, 3_000) {
                session.on_datagram(edge(), &datagram, at, &mut send);
                while let Some(object) = session.next_object() {
                    delivered.push(object.part);
                }
            }
            for datagram in object_datagrams(1_145 + u64::from(step), 7, 3_000) {
                session.on_datagram(edge_b(), &datagram, at, &mut send);
                while let Some(object) = session.next_object() {
                    delivered.push(object.part);
                }
            }
            session.poll_legs(at, &mut send);
            if events(&mut session).contains(&LegEvent::Failed {
                leg: B,
                cause: LegFailure::Refusing,
            }) {
                failed_at = Some(42 * step);
                break;
            }
            assert!(held(&session, A).failure.is_none());
        }
        let failed_at = failed_at.expect("b fails");
        assert!((1_000..1_100).contains(&failed_at), "{failed_at}");
        assert!(delivered.iter().all(|part| *part < 1_000), "{delivered:?}");
        assert_eq!(delivered.len() as u32, failed_at / 42 + 1);
        assert!(session.receiver.raptorq_refused.stale_generation >= 24);
        assert_eq!(session.refusing_legs, 1);
        assert!(session.receiver.compact.newest().is_some());
    }

    /// B X5 review: a failed leg keeps its slot until the director
    /// releases it: a leg added meanwhile (the hedge taking its place) gets
    /// another, and the release lets only the failed leg go.
    #[test]
    fn a_failed_leg_keeps_its_place_until_it_is_released() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut sent = Sent::new();
        session.add_leg(A, "a", edge(), start(), now, &mut sender(&mut sent));
        session.add_leg(B, "b", edge_b(), start(), now, &mut sender(&mut sent));
        session.on_datagram(edge(), &token(5), now, &mut sender(&mut sent));
        session.on_datagram(edge(), &ok(100), now, &mut sender(&mut sent));
        session.on_datagram(edge(), &part(100), now, &mut sender(&mut sent));
        // b's edge gives a token and never answers the SUB with it.
        session.on_datagram(edge_b(), &token(6), now, &mut sender(&mut sent));
        events(&mut session);
        let late = now + Duration::from_millis(5_001);
        session.hold = true;
        session.poll_legs(late, &mut sender(&mut sent));
        assert_eq!(events(&mut session), vec![], "held: no verdict");
        session.hold = false;
        session.poll_legs(late, &mut sender(&mut sent));
        assert!(events(&mut session).contains(&LegEvent::Failed {
            leg: B,
            cause: LegFailure::NoAnswer
        }));
        let b_slot = held(&session, B).id;
        // The hedge, added before the failed leg is released.
        let hedge = LegStart {
            token: Some(7),
            ..start()
        };
        sent.clear();
        session.add_leg(C, "c", edge_c(), hedge, late, &mut sender(&mut sent));
        assert_ne!(held(&session, C).id, b_slot);
        assert!(matches!(
            &sent[..],
            [(to, Control::Subscribe { token: Some(7), from_part: Some(101), .. })] if *to == edge_c()
        ));
        sent.clear();
        session.release_leg(B, late, &mut sender(&mut sent));
        assert!(matches!(
            &sent[..],
            [(to, Control::Unsubscribe { token: 6, .. })] if *to == edge_b()
        ));
        assert!(session.leg(B, late).is_none());
        assert!(session.leg(C, late).is_some());
        assert_eq!(
            session.on_datagram(edge_c(), &ok(101), late, &mut sender(&mut sent)),
            Received::Subscribed {
                next_part: 101,
                first: true
            }
        );
    }

    /// B X5 review: an edge that answers a leg it never served with `MOVE`,
    /// or with `REFUSED`, refuses it: the leg is given up until released.
    #[test]
    fn a_move_before_the_ok_is_a_refusal() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.add_leg(A, "a", edge(), start(), now, &mut send);
        session.add_leg(B, "b", edge_b(), start(), now, &mut send);
        session.on_datagram(edge(), &token(5), now, &mut send);
        let moved = Control::Move {
            stream_id: 1,
            reason: "stale-copy".into(),
        }
        .encode();
        assert_eq!(
            session.on_datagram(edge(), &moved, now, &mut send),
            Received::Move("stale-copy".into())
        );
        let refused = Control::Refused {
            stream_id: 1,
            reason: "edge-busy".into(),
        }
        .encode();
        assert_eq!(
            session.on_datagram(edge_b(), &refused, now, &mut send),
            Received::Move("edge-busy".into())
        );
        assert_eq!(
            events(&mut session),
            vec![
                LegEvent::Tokened(A),
                LegEvent::Refused {
                    leg: A,
                    reason: "stale-copy".into()
                },
                LegEvent::Refused {
                    leg: B,
                    reason: "edge-busy".into()
                }
            ]
        );
        assert!(held(&session, A).is_failed() && held(&session, B).is_failed());
        assert_eq!(
            session.on_datagram(edge(), &ok(100), now, &mut send),
            Received::Ignored
        );
    }

    /// B X5: a leg asks its edge to start after the newest part when
    /// another leg delivers, or from the oldest hole (at most 4,096 back).
    #[test]
    fn a_new_leg_starts_after_the_newest_or_from_the_oldest_hole() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut sent = Sent::new();
        session.add_leg(A, "a", edge(), start(), now, &mut sender(&mut sent));
        assert!(matches!(
            &sent[..],
            [(
                _,
                Control::Subscribe {
                    from_part: None,
                    ..
                }
            )]
        ));
        session.on_datagram(edge(), &token(5), now, &mut sender(&mut sent));
        session.on_datagram(edge(), &ok(100), now, &mut sender(&mut sent));
        for number in [100_u64, 101, 103] {
            session.on_datagram(edge(), &part(number), now, &mut sender(&mut sent));
        }
        sent.clear();
        session.add_leg(B, "b", edge_b(), start(), now, &mut sender(&mut sent));
        let hole = LegStart {
            from: StartFrom::OldestHole,
            ..start()
        };
        session.add_leg(C, "c", edge_c(), hole, now, &mut sender(&mut sent));
        let from = |target: SocketAddr| {
            sent.iter().find_map(|(to, control)| match control {
                Control::Subscribe { from_part, .. } if *to == target => Some(*from_part),
                _ => None,
            })
        };
        assert_eq!(from(edge_b()), Some(Some(104)));
        assert_eq!(from(edge_c()), Some(Some(102)));
    }

    /// B X5: while the caller says datagrams are queued, no leg is judged
    /// silent, hedged or dead: the backlog is read first.
    #[test]
    fn no_verdict_while_datagrams_are_queued() {
        let now = Instant::now();
        let mut session = two_legs(SessionConfig::new(1), 100, now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        for target in [edge(), edge_b()] {
            session.on_datagram(target, &part(100), now, &mut send);
        }
        events(&mut session);
        // The host stalls 300 ms with both legs' datagrams queued.
        let stall = now + Duration::from_millis(300);
        session.queued = true;
        session.poll_legs(stall, &mut send);
        assert_eq!(events(&mut session), vec![]);
        // The backlog, read.
        for number in 101..400_u64 {
            for target in [edge(), edge_b()] {
                session.on_datagram(target, &part(number), stall, &mut send);
            }
        }
        session.queued = false;
        session.poll_legs(stall, &mut send);
        assert_eq!(events(&mut session), vec![]);
        assert!(view(&session, A, stall).delivering && view(&session, B, stall).delivering);
        // One leg alone: no hedge while queued either.
        let mut alone = subscribed(SessionConfig::new(1), now);
        events(&mut alone);
        alone.queued = true;
        alone.poll_legs(stall, &mut send);
        assert_eq!(events(&mut alone), vec![]);
    }

    /// B X5: when no leg has sent a datagram for 200 ms nobody is failed
    /// (the silence says nothing about any one edge); each leg is asked
    /// again from the oldest hole with `r=1`; the next report comes a
    /// second later.
    #[test]
    fn every_leg_silent_fails_nobody_and_asks_again_from_the_hole() {
        let now = Instant::now();
        let config = SessionConfig {
            nack_timing: NackTiming::BEFORE,
            ..SessionConfig::new(1)
        };
        let mut session = two_legs(config, 100, now);
        let mut sent = Sent::new();
        for number in [100_u64, 101, 102, 104, 105] {
            for target in [edge(), edge_b()] {
                session.on_datagram(target, &part(number), now, &mut sender(&mut sent));
            }
        }
        events(&mut session);
        assert_eq!(session.oldest_hole(), Some(103));
        let mut silences = Vec::new();
        let mut at = now;
        while at < now + Duration::from_millis(1_300) {
            at = session.next_wake().max(at + Duration::from_micros(100));
            session.poll_legs(at, &mut sender(&mut sent));
            for event in events(&mut session) {
                assert!(
                    matches!(event, LegEvent::AllSilent | LegEvent::HedgeDue(_)),
                    "{event:?}"
                );
                if event == LegEvent::AllSilent {
                    silences.push(at - now);
                }
            }
            if silences.len() == 1 {
                assert_eq!(session.silent_for, Duration::from_millis(200));
            }
        }
        assert!(session.leg(A, at).is_some() && session.leg(B, at).is_some());
        assert!(!held(&session, A).is_failed() && !held(&session, B).is_failed());
        assert_eq!(silences.len(), 2, "{silences:?}");
        assert!(
            silences[0] > Duration::from_millis(200) && silences[0] < Duration::from_millis(210)
        );
        assert!(silences[1] >= silences[0] + Duration::from_millis(1_000));
        sent.clear();
        for leg in [A, B] {
            session.reask_leg(leg, at, &mut sender(&mut sent));
        }
        for (target, given) in [(edge(), 5), (edge_b(), 6)] {
            assert!(sent.iter().any(|(to, control)| *to == target
                && matches!(
                    control,
                    Control::Subscribe { from_part: Some(103), resume: true, token: Some(t), .. } if *t == given
                )));
        }
        // A media datagram ends the silence: the short silence again.
        session.on_datagram(edge(), &part(106), at, &mut sender(&mut sent));
        session.poll_legs(at + Duration::from_millis(150), &mut sender(&mut sent));
        assert!(!events(&mut session).contains(&LegEvent::AllSilent));
    }

    /// B X5: a leg with no `TOKEN` in its own token wait is passed over
    /// (once) and goes on asking; if its edge answers it is used; with no
    /// answer in the answer wait it fails.
    #[test]
    fn a_leg_without_a_token_is_passed_over_and_used_if_it_answers() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(1), now);
        let mut sent = Sent::new();
        let from_hole = LegStart {
            from: StartFrom::OldestHole,
            ..start()
        };
        session.add_leg(A, "a", edge(), from_hole, now, &mut sender(&mut sent));
        // A far entry: four round trips of 200 ms.
        let far = LegStart {
            token_wait: Duration::from_millis(800),
            ..from_hole
        };
        session.add_leg(B, "b", edge_b(), far, now, &mut sender(&mut sent));
        session.poll_legs(now + Duration::from_millis(399), &mut sender(&mut sent));
        assert_eq!(events(&mut session), vec![]);
        session.poll_legs(now + Duration::from_millis(400), &mut sender(&mut sent));
        assert_eq!(events(&mut session), vec![LegEvent::NoToken(A)]);
        session.poll_legs(now + Duration::from_millis(799), &mut sender(&mut sent));
        assert_eq!(events(&mut session), vec![]);
        session.poll_legs(now + Duration::from_millis(800), &mut sender(&mut sent));
        assert_eq!(events(&mut session), vec![LegEvent::NoToken(B)]);
        sent.clear();
        let mut at = now + Duration::from_millis(800);
        while at < now + Duration::from_millis(1_300) {
            at = session.next_wake();
            session.poll_legs(at, &mut sender(&mut sent));
        }
        assert!(sent.iter().any(|(to, control)| *to == edge()
            && matches!(
                control,
                Control::Subscribe {
                    token: None,
                    from_part: None,
                    ..
                }
            )));
        assert!(events(&mut session).is_empty(), "passed over once");
        // a's edge answers late: the leg is used.
        session.on_datagram(edge(), &token(5), at, &mut sender(&mut sent));
        session.on_datagram(edge(), &ok(7), at, &mut sender(&mut sent));
        session.on_datagram(edge(), &part(7), at, &mut sender(&mut sent));
        assert_eq!(
            events(&mut session),
            vec![
                LegEvent::Tokened(A),
                LegEvent::Live {
                    leg: A,
                    compact: true
                }
            ]
        );
        // b's never does: failed at the answer wait.
        session.hold = true;
        session.poll_legs(now + Duration::from_millis(5_001), &mut sender(&mut sent));
        assert!(!events(&mut session).contains(&LegEvent::Failed {
            leg: B,
            cause: LegFailure::NoAnswer
        }));
        session.hold = false;
        session.poll_legs(now + Duration::from_millis(5_001), &mut sender(&mut sent));
        assert!(events(&mut session).contains(&LegEvent::Failed {
            leg: B,
            cause: LegFailure::NoAnswer
        }));
        assert_eq!(
            held(&session, B).failure.as_deref(),
            Some("no-answer-5000ms")
        );
    }

    /// B X5 (2.6): 45 s after its token a leg's refresh is joined by a
    /// tokenless `SUB`; the edge's new token is taken and used, so that no
    /// `NACK` or `UNSUB` goes with a token the edge forgot.
    #[test]
    fn a_token_is_renewed_before_the_edge_forgets_it() {
        let now = Instant::now();
        let mut session = subscribed(SessionConfig::new(1), now);
        session.hold = true;
        let mut sent: Vec<(Instant, Control)> = Vec::new();
        let mut at = now;
        while at < now + Duration::from_secs(50) {
            at = session.next_wake();
            let before = sent.len();
            session.poll(at, &mut |_, datagram| {
                sent.push((at, Control::decode(datagram).unwrap()))
            });
            // The edge answers a renewal with the token it holds.
            if sent[before..]
                .iter()
                .any(|(_, control)| matches!(control, Control::Subscribe { token: None, .. }))
            {
                session.on_datagram(edge(), &token(5), at, &mut |_, _| {});
            }
        }
        let tokenless: Vec<Instant> = sent
            .iter()
            .filter(|(_, control)| matches!(control, Control::Subscribe { token: None, .. }))
            .map(|(at, _)| *at)
            .collect();
        assert_eq!(tokenless.len(), 1, "one renewal in 50 s");
        assert!(tokenless[0] >= now + TOKEN_RENEW_AFTER);
        assert!(tokenless[0] < now + TOKEN_RENEW_AFTER + REFRESH + Duration::from_millis(1));
        assert!(sent.iter().any(|(when, control)| *when == tokenless[0]
            && matches!(control, Control::Subscribe { token: Some(5), .. })));
        // The edge's keys turned over: a new token, used at once, and for
        // the NACKs and UNSUB that follow.
        let mut answer: Vec<Control> = Vec::new();
        session.on_datagram(edge(), &token(99), at, &mut |_, datagram| {
            answer.push(Control::decode(datagram).unwrap())
        });
        assert!(matches!(
            &answer[..],
            [Control::Subscribe {
                token: Some(99),
                ..
            }]
        ));
        assert_eq!(session.token(), Some(99));
        let mut left: Vec<Control> = Vec::new();
        session.leave(&mut |_, datagram| left.push(Control::decode(datagram).unwrap()));
        assert!(matches!(
            &left[..],
            [Control::Unsubscribe { token: 99, .. }]
        ));
    }

    /// B X5 review (2.6): a renewal whose `TOKEN` is lost is sent again at
    /// every refresh until one comes, so a leg never holds a token past its
    /// 60 s; a token handed in by the director (the spare's, of an age the
    /// session does not know) is renewed at the leg's first refresh.
    #[test]
    fn a_lost_renewal_is_sent_again_at_the_next_refresh() {
        let now = Instant::now();
        let mut session = subscribed(SessionConfig::new(1), now);
        session.hold = true;
        let tokenless = |session: &mut UdpSession, from: Instant, until: Instant| {
            let mut renewals = Vec::new();
            let mut at = from;
            while at < until {
                at = session.next_wake();
                session.poll(at, &mut |_, datagram| {
                    if matches!(
                        Control::decode(datagram),
                        Some(Control::Subscribe { token: None, .. })
                    ) {
                        renewals.push(at - now);
                    }
                });
            }
            renewals
        };
        let s = Duration::from_secs;
        // Every renewal is lost: one at each refresh from 45 s on.
        let lost = tokenless(&mut session, now, now + s(51));
        assert_eq!(lost.len(), 3, "{lost:?}");
        assert!(lost[0] >= TOKEN_RENEW_AFTER && lost[0] < TOKEN_RENEW_AFTER + REFRESH);
        assert_eq!(lost[1] - lost[0], REFRESH);
        assert_eq!(lost[2] - lost[1], REFRESH);
        // The edge answers: no renewal for another 45 s.
        let answered = now + lost[2] + Duration::from_millis(5);
        session.on_datagram(edge(), &token(5), answered, &mut |_, _| {});
        assert!(tokenless(&mut session, answered, answered + s(44)).is_empty());
        // A hedge with the spare's token: renewed at its first refresh.
        let mut hedged = UdpSession::new(SessionConfig::new(1), now);
        hedged.hold = true;
        let spare = LegStart {
            token: Some(7),
            ..start()
        };
        hedged.add_leg(A, "a", edge(), spare, now, &mut |_, _| {});
        hedged.on_datagram(edge(), &ok(100), now, &mut |_, _| {});
        hedged.on_datagram(edge(), &part(100), now, &mut |_, _| {});
        let first = tokenless(&mut hedged, now, now + REFRESH);
        assert_eq!(first, vec![REFRESH]);
        hedged.on_datagram(edge(), &token(7), now + REFRESH, &mut |_, _| {});
        assert!(tokenless(&mut hedged, now + REFRESH, now + s(40)).is_empty());
    }

    /// B X5: the answer to a request sent again only by the 3 ms double
    /// measures the round trip from the first copy; a request repeated by
    /// the retry gives none (Karn), and the first refresh gives it then.
    #[test]
    fn a_doubled_request_still_gives_a_round_trip() {
        for rtt_ms in [2_u64, 80, 500] {
            let rtt = Duration::from_millis(rtt_ms);
            let now = Instant::now();
            let mut session = UdpSession::new(SessionConfig::new(1), now);
            let mut send = |_: SocketAddr, _: &[u8]| {};
            session.add_leg(A, "a", edge(), start(), now, &mut send);
            // The edge answers each request one round trip after its first
            // copy; the client runs its timers meanwhile.
            let until = |session: &mut UdpSession, end: Instant| {
                let mut at = now;
                while session.next_wake() < end {
                    at = session.next_wake().max(at);
                    session.poll_legs(at, &mut |_, _| {});
                }
            };
            until(&mut session, now + rtt);
            session.on_datagram(edge(), &token(5), now + rtt, &mut send);
            until(&mut session, now + rtt * 2);
            session.on_datagram(edge(), &ok(10), now + rtt * 2, &mut send);
            if rtt_ms < 25 {
                // Only the double went: a sample of each exchange.
                assert_eq!(held(&session, A).rtt, Some(rtt), "{rtt_ms} ms");
                continue;
            }
            assert_eq!(held(&session, A).rtt, None, "{rtt_ms} ms: repeated");
            // The first refresh, answered a round trip later.
            let refresh = only(&session).next_refresh;
            until(&mut session, refresh + Duration::from_micros(1));
            session.on_datagram(edge(), &ok(10), refresh + rtt, &mut send);
            assert_eq!(held(&session, A).rtt, Some(rtt), "{rtt_ms} ms");
        }
    }

    /// B X5: a refresh sent with no other request outstanding measures the
    /// round trip by its `OK` (or `MOVE`); one sent while another is
    /// unanswered gives none.
    #[test]
    fn a_refresh_gives_a_round_trip() {
        let now = Instant::now();
        let mut session = subscribed(SessionConfig::new(1), now);
        session.hold = true;
        let mut send = |_: SocketAddr, _: &[u8]| {};
        only_mut(&mut session).rtt = None;
        let refresh = only(&session).next_refresh;
        session.poll_legs(refresh, &mut send);
        session.on_datagram(
            edge(),
            &ok(10),
            refresh + Duration::from_millis(30),
            &mut send,
        );
        assert_eq!(only(&session).rtt, Some(Duration::from_millis(30)));
        // A refresh unanswered, then another: no sample from the answer.
        let first = only(&session).next_refresh;
        session.poll_legs(first, &mut send);
        let second = only(&session).next_refresh;
        session.poll_legs(second, &mut send);
        session.on_datagram(
            edge(),
            &ok(10),
            second + Duration::from_millis(5),
            &mut send,
        );
        assert_eq!(only(&session).rtt, Some(Duration::from_millis(30)));
        // A MOVE answers a refresh too.
        let third = only(&session).next_refresh;
        session.poll_legs(third, &mut send);
        let moved = Control::Move {
            stream_id: 1,
            reason: "draining".into(),
        }
        .encode();
        session.on_datagram(edge(), &moved, third + Duration::from_millis(30), &mut send);
        assert_eq!(only(&session).rtt, Some(Duration::from_millis(30)));
        assert!(session
            .receiver
            .compact
            .leg_rtt(only(&session).id)
            .is_some());
    }

    /// B X5 review: the `MOVE` a leaving leg's edge repeats on its own is
    /// taken neither as the answer to a refresh (no round trip from it) nor,
    /// within the first retry's wait of a take-back by a leg with no round
    /// trip yet, as a new `MOVE`.
    #[test]
    fn a_move_reminder_gives_no_round_trip_and_keeps_a_take_back() {
        let now = Instant::now();
        let mut session = two_legs(SessionConfig::new(1), 100, now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        let moved = Control::Move {
            stream_id: 1,
            reason: "rebalance".into(),
        }
        .encode();
        let ms = Duration::from_millis(1);
        session.on_datagram(edge(), &moved, now, &mut send);
        held_mut(&mut session, A).rtt = Some(ms * 40);
        // A refresh of the leaving leg, and the edge's reminder 5 ms later.
        let refresh = held(&session, A).next_refresh;
        session.poll_legs(refresh, &mut send);
        session.on_datagram(edge(), &moved, refresh + ms * 5, &mut send);
        assert_eq!(held(&session, A).rtt, Some(ms * 40));
        // A take-back by a leg with no round trip: a reminder 10 ms later is
        // the answer to the SUB before it; its OK takes the leg back.
        held_mut(&mut session, A).rtt = None;
        let back = refresh + ms * 100;
        session.takeback_leg(A, back, &mut send);
        session.on_datagram(edge(), &moved, back + ms * 10, &mut send);
        session.on_datagram(edge(), &ok(200), back + ms * 15, &mut send);
        assert!(events(&mut session).contains(&LegEvent::TakenBack(A)));
        assert_eq!(held(&session, A).state, State::Live);
        // A MOVE past the wait is a new MOVE: its OK takes nothing back.
        session.on_datagram(edge(), &moved, back + ms * 200, &mut send);
        let later = back + ms * 300;
        session.takeback_leg(A, later, &mut send);
        session.on_datagram(
            edge(),
            &moved,
            later + Duration::from_millis(DEFAULT_FIRST_RETRY_MS + 5),
            &mut send,
        );
        session.on_datagram(edge(), &ok(200), later + ms * 40, &mut send);
        let seen = events(&mut session);
        assert!(!seen.contains(&LegEvent::TakenBack(A)), "{seen:?}");
        assert_eq!(held(&session, A).state, State::Leaving);
    }

    /// B X5: a hole's first NACK goes to the leg with the shortest round
    /// trip, each with its own token; its later NACKs to every leg that
    /// brings the stream.
    #[test]
    fn a_nack_goes_to_every_leg_that_sent_the_part() {
        let config = SessionConfig {
            nack_timing: NackTiming {
                silence: false,
                ..NackTiming::default()
            },
            ..SessionConfig::new(1)
        };
        let mut session = two_legs(config, 100, Instant::now());
        for (leg, rtt) in [(A, 10), (B, 2)] {
            let rtt = Duration::from_millis(rtt);
            held_mut(&mut session, leg).rtt = Some(rtt);
            let id = held(&session, leg).id;
            session.receiver.compact.set_leg_rtt(id, rtt);
        }
        let mut send = |_: SocketAddr, _: &[u8]| {};
        let now = Instant::now();
        for number in [100_u64, 101, 103] {
            for target in [edge(), edge_b()] {
                session.on_datagram(target, &part(number), now, &mut send);
            }
        }
        let mut sent = Sent::new();
        let nacks = |sent: &Sent| -> Vec<(SocketAddr, u64, Vec<u64>)> {
            sent.iter()
                .filter_map(|(to, control)| match control {
                    Control::Nack { token, parts, .. } => Some((*to, *token, parts.clone())),
                    _ => None,
                })
                .collect()
        };
        // The receiver keeps its holes on the real clock: the first poll
        // is taken from it, after the pushes, past the first NACK's wait.
        let first = Instant::now() + Duration::from_millis(1);
        session.poll_legs(first, &mut sender(&mut sent));
        assert_eq!(nacks(&sent), vec![(edge_b(), 6, vec![102])]);
        sent.clear();
        // The retry: 2 ms (the floor; b's round trip is well under it).
        session.poll_legs(first + Duration::from_millis(1), &mut sender(&mut sent));
        assert!(nacks(&sent).is_empty(), "not before the retry");
        session.poll_legs(first + Duration::from_millis(2), &mut sender(&mut sent));
        assert_eq!(
            nacks(&sent),
            vec![(edge(), 5, vec![102]), (edge_b(), 6, vec![102])]
        );
        assert_eq!(session.nack_datagrams, 3);
    }

    /// B X5: a leg that falls 8 parts behind with no media for 20 ms is
    /// suspect, and the plans are made without it at once: the other leg,
    /// alone, asks for its one-path plan in a SUB sent in the same poll.
    #[test]
    fn a_suspect_leg_changes_the_plan_at_once() {
        let now = Instant::now();
        let mut session = two_legs(SessionConfig::new(1), 100, now);
        assert_eq!(only_plan(&session, A), "C0.32.0.7");
        assert_eq!(only_plan(&session, B), "C0.32.0.7");
        for leg in [A, B] {
            let state = held_mut(&mut session, leg);
            state.compact_estimate = Some(0.0);
            state.loss_samples = PLAN_SAMPLES;
        }
        let ms = Duration::from_millis(1);
        let mut sent = Sent::new();
        for number in 100..110_u64 {
            let at = now + ms * (number - 100) as u32;
            for target in [edge(), edge_b()] {
                session.on_datagram(target, &part(number), at, &mut sender(&mut sent));
            }
        }
        session.plan_legs(now + ms * 9);
        assert_eq!(only_plan(&session, A), "C0.32.0.7");
        let mut suspect_at = None;
        for step in 1..=30_u32 {
            let at = now + ms * (9 + step);
            sent.clear();
            session.on_datagram(
                edge(),
                &part(109 + u64::from(step)),
                at,
                &mut sender(&mut sent),
            );
            session.poll_legs(at, &mut sender(&mut sent));
            if held(&session, B).suspect {
                suspect_at = Some(step);
                break;
            }
        }
        assert_eq!(suspect_at, Some(20));
        assert_eq!(only_plan(&session, A), "C0.32.1.7");
        assert!(sent.iter().any(|(to, control)| *to == edge()
            && matches!(control, Control::Subscribe { compact: Some(plan), .. } if plan.word() == "C0.32.1.7")));
    }

    fn only_plan(session: &UdpSession, leg: LegKey) -> String {
        held(session, leg).plan.word()
    }

    /// B X5: a leg whose edge sent `MOVE` is leaving and still receives
    /// (its parts delivered, its NACKs sent) until the director releases
    /// it; a new leg level with it is seen to be; released, it gets
    /// `UNSUB` twice and its datagrams are ignored. A take-back makes it
    /// live again.
    #[test]
    fn a_moved_leg_receives_until_it_is_released() {
        let config = SessionConfig {
            nack_timing: NackTiming {
                silence: false,
                ..NackTiming::default()
            },
            ..SessionConfig::new(1)
        };
        let mut session = two_legs(config, 100, Instant::now());
        let mut sent = Sent::new();
        let moved = Control::Move {
            stream_id: 1,
            reason: "draining".into(),
        }
        .encode();
        assert_eq!(
            session.on_datagram(edge(), &moved, Instant::now(), &mut sender(&mut sent)),
            Received::Move("draining".into())
        );
        assert_eq!(
            events(&mut session),
            vec![LegEvent::Moved {
                leg: A,
                reason: "draining".into()
            }]
        );
        assert_eq!(held(&session, A).state, State::Leaving);
        assert_eq!(held(&session, A).leaving.as_deref(), Some("draining"));
        // It still delivers: a's parts come first, b's a little later. The
        // receiver times arrivals on the real clock, so the test does too.
        let mut first_from_a = 0;
        for number in 100..140_u64 {
            if number == 120 {
                continue;
            }
            let at = Instant::now();
            session.on_datagram(edge(), &part(number), at, &mut sender(&mut sent));
            if session.next_object().is_some() {
                first_from_a += 1;
            }
            session.on_datagram(edge_b(), &part(number), at, &mut sender(&mut sent));
            while session.next_object().is_some() {}
            session.poll_legs(at, &mut sender(&mut sent));
        }
        assert_eq!(first_from_a, 39);
        assert!(session.level(B, A));
        assert!(!session.level(A, C), "no such leg");
        // Its NACKs still go (the hole of part 120, asked of a first: both
        // round trips are 0, and a is the first leg).
        let due = Instant::now() + Duration::from_millis(30);
        session.poll_legs(due, &mut sender(&mut sent));
        assert!(sent.iter().any(
            |(to, control)| *to == edge() && matches!(control, Control::Nack { token: 5, .. })
        ));
        // A refresh still goes to it.
        sent.clear();
        let at = due + REFRESH;
        session.poll_legs(at, &mut sender(&mut sent));
        assert!(sent.iter().any(|(to, control)| *to == edge()
            && matches!(control, Control::Subscribe { token: Some(5), .. })));
        // A take-back: the edge's OK makes it live again.
        let ms = Duration::from_millis(1);
        session.takeback_leg(A, at, &mut sender(&mut sent));
        session.on_datagram(edge(), &ok(140), at + ms, &mut sender(&mut sent));
        assert!(events(&mut session).contains(&LegEvent::TakenBack(A)));
        assert_eq!(held(&session, A).state, State::Live);
        // Released: UNSUB now and 20 ms later; its datagrams no longer
        // deliver.
        sent.clear();
        session.release_leg(A, at, &mut sender(&mut sent));
        session.poll_legs(at + ms * 20, &mut sender(&mut sent));
        let unsubs = sent
            .iter()
            .filter(|(to, control)| {
                *to == edge() && matches!(control, Control::Unsubscribe { token: 5, .. })
            })
            .count();
        assert_eq!(unsubs, 2);
        session.on_datagram(edge(), &part(141), at + ms * 21, &mut sender(&mut sent));
        assert!(session.next_object().is_none());
        assert!(session.leg(A, at).is_none() && session.leg(B, at).is_some());
    }

    /// B X5 (3.9): once 8 of the last 16 parts delivered came as RaptorQ
    /// objects, the director is told, once.
    #[test]
    fn a_stream_of_objects_is_reported_once() {
        let now = Instant::now();
        let mut session = UdpSession::new(SessionConfig::new(7), now);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        session.add_leg(A, "a", edge(), start(), now, &mut send);
        let mut reported = Vec::new();
        for number in 1..=20_u64 {
            let object = PartObject {
                kind: ObjectKind::Part,
                datagrams: 0,
                stream_id: 7,
                part: number,
                available_unix_us: 5,
                published_unix_us: 4,
                generation: 0,
                payload: Bytes::from_static(&[9_u8; 2_000]),
            };
            for datagram in encode_object(&object, 1_200, RepairPolicy::default()).unwrap() {
                session.on_datagram(edge(), &datagram, now, &mut send);
                while session.next_object().is_some() {}
            }
            if events(&mut session).contains(&LegEvent::ObjectLane) {
                reported.push(number);
            }
        }
        assert_eq!(reported, vec![16]);
    }

    /// A part's datagrams, published at `published_unix_us`.
    fn published_datagrams(part: u64, len: usize, published_unix_us: u64) -> Vec<Vec<u8>> {
        let object = PartObject {
            kind: ObjectKind::Part,
            datagrams: 0,
            stream_id: 1,
            part,
            available_unix_us: 0,
            published_unix_us,
            generation: 7,
            payload: Bytes::from(vec![part as u8; len]),
        };
        encode_object(&object, 1_200, RepairPolicy::default()).unwrap()
    }

    fn unix_us() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    /// Feed a lone 4K leg parts 1234.. at 24 a second, each `late(index)`
    /// after its publication (`None`: never sent), polling as a client
    /// does; the events and the index of the first `Degraded`.
    fn feed_lone_leg(
        late: impl Fn(usize) -> Option<u64>,
        count: usize,
    ) -> (UdpSession, Vec<LegEvent>, Option<usize>) {
        let t0 = Instant::now();
        let mut session = lone_4k_leg(t0);
        let mut send = |_: SocketAddr, _: &[u8]| {};
        let mut all = Vec::new();
        let mut first = None;
        for index in 0..count {
            let now = t0 + Duration::from_micros(41_667 * (index as u64 + 1));
            if let Some(late_us) = late(index) {
                let published = unix_us().saturating_sub(late_us);
                for datagram in published_datagrams(1_234 + index as u64, 4_000, published) {
                    session.on_datagram(edge(), &datagram, now, &mut send);
                    while session.next_object().is_some() {}
                }
            }
            session.poll_legs(now, &mut send);
            for event in events(&mut session) {
                if matches!(event, LegEvent::Degraded { .. }) {
                    first.get_or_insert(index);
                }
                all.push(event);
            }
        }
        (session, all, first)
    }

    /// Place240 rule 3 (cmpd8: a 4K viewer's parts came 3 to 5.7 s late
    /// on one leg and no rule fired): a lone leg whose parts come late is
    /// reported `late-parts` within the window, once, and is not failed:
    /// it keeps delivering until the director has its replacement.
    #[test]
    fn a_lone_leg_whose_parts_come_late_is_degraded_and_keeps_delivering() {
        let (session, events, first) =
            feed_lone_leg(|index| Some(if index < 48 { 30_000 } else { 2_030_000 }), 96);
        let degraded: Vec<&LegEvent> = events
            .iter()
            .filter(|event| matches!(event, LegEvent::Degraded { .. }))
            .collect();
        assert_eq!(
            degraded,
            vec![&LegEvent::Degraded {
                leg: A,
                reason: "late-parts".into()
            }],
            "{events:?}"
        );
        assert!(first.is_some_and(|at| (49..=50).contains(&at)), "{first:?}");
        assert!(!events.iter().any(|event| matches!(event, LegEvent::Failed { .. })));
        assert_eq!(session.degraded_late, 1);
        let report = session.leg_report(A).unwrap();
        assert!(report.lateness.max_us >= 1_900_000, "{:?}", report.lateness);
        // On time throughout: nothing.
        let (_, events, first) = feed_lone_leg(|_| Some(30_000), 96);
        assert_eq!(first, None, "{events:?}");
    }

    /// Place240 rule 3: a lone leg that skips parts at their playout
    /// deadline is reported `missing-parts`.
    #[test]
    fn a_lone_leg_that_misses_parts_is_degraded() {
        let (session, events, first) =
            feed_lone_leg(|index| (index % 10 != 5 || index < 24).then_some(30_000), 120);
        assert!(
            events.contains(&LegEvent::Degraded {
                leg: A,
                reason: "missing-parts".into()
            }),
            "{events:?}"
        );
        // The second hole passes its deadline 0.5 s after the next part.
        assert!(first.is_some_and(|at| at <= 50), "{first:?}");
        assert!(session.quality.missing_total >= 2);
    }

}
